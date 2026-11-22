/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! The splittable DoFn that reads the Arrow responses of one stream into rows.

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beam::coders::DefaultCoder;
use beam::metrics::{Lineage, Metrics};
use beam::schema::{BeamRow, RowElement};
use beam::transforms::sdf::{ProcessContinuation, RestrictionTracker, SplittableDoFn};
use beam::transforms::{Outputs, ProcessContext};
use beam_gcp_core::retry::{push_back, status_name};
use beam_io_arrow::arrow_array::RecordBatch;
use beam_io_arrow::arrow_buffer::Buffer;
use beam_io_arrow::arrow_ipc::reader::StreamDecoder;
use beam_io_arrow::{BeamRowCodec, RowCodec, SchemaRowCodec};
use bytes::Bytes;
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model::ReadRowsResponse;
use tokio::sync::mpsc;

use super::restriction::{StreamRestriction, StreamTracker};
use crate::bigquery::METRICS;
use crate::bigquery::clients::read_client;
use crate::bigquery::schema::{batch_to_beam_rows, beam_batch};
use crate::bigquery::table::api_request;
use crate::runtime::client_runtime;
use beam::internals::{AbortOnDrop, recv_blocking};
use beam::retry::{Backoff, Decision, RetryWaits};

/// Responses that a stream reader fetches ahead of the DoFn.
const PREFETCH_RESPONSES: usize = 4;

/// Reads of a stream after failed `ReadRows` calls: 4 retries from the next offset, waits of
/// 0.5 to 4 seconds.
const READ_RETRY: Backoff = Backoff {
    attempts: 5,
    initial: Duration::from_millis(500),
    max: Duration::from_secs(4),
};

/// The longest wait for the next `ReadRows` response before the reader reconnects.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(120);

/// One message of the task that reads a stream.
#[expect(
    clippy::large_enum_variant,
    reason = "each message but the last is a response, so a box only adds an allocation"
)]
enum Fetched {
    /// The next response of the stream.
    Response(ReadRowsResponse),
    /// The stream has no more rows. If the channel closes before this message, the task
    /// stopped early.
    End,
}

/// Starts a task that reads `stream` from `offset` and sends each response. After a failed
/// call, the task reads again from the offset after the last response that it sent.
fn fetch_responses(
    client: Option<Read>,
    stream: String,
    offset: i64,
    waits: Arc<RetryWaits>,
) -> io::Result<(mpsc::Receiver<Result<Fetched, String>>, AbortOnDrop)> {
    let (sender, receiver) = mpsc::channel(PREFETCH_RESPONSES);
    let request = api_request(&stream, "ReadRows");
    let task = client_runtime()?.spawn(async move {
        let mut next_offset = offset;
        let mut failures = 0;
        loop {
            let client = match &client {
                Some(client) => Ok(client.clone()),
                None => read_client().await,
            };
            let started = Instant::now();
            // Only server push-back counts as throttling.
            let (error, decision, status) = match client {
                Ok(client) => {
                    match client
                        .read_rows()
                        .set_read_stream(stream.clone())
                        .set_offset(next_offset)
                        .send()
                        .await
                    {
                        Ok(mut responses) => loop {
                            match tokio::time::timeout(RESPONSE_TIMEOUT, responses.next()).await {
                                Err(_) => {
                                    let error =
                                        format!("no response for {} s", RESPONSE_TIMEOUT.as_secs());
                                    break (error, Decision::Backoff, Some("deadline_exceeded"));
                                }
                                Ok(None) => {
                                    request.record("ok", started.elapsed());
                                    let _ = sender.send(Ok(Fetched::End)).await;
                                    return;
                                }
                                Ok(Some(Ok(response))) => {
                                    failures = 0;
                                    next_offset += response.row_count;
                                    if sender.send(Ok(Fetched::Response(response))).await.is_err() {
                                        return;
                                    }
                                }
                                Ok(Some(Err(e))) => {
                                    break (
                                        e.to_string(),
                                        read_decision(&e),
                                        Some(status_name(&e)),
                                    );
                                }
                            }
                        },
                        Err(e) => (e.to_string(), read_decision(&e), Some(status_name(&e))),
                    }
                }
                Err(e) => (e.to_string(), Decision::Backoff, None),
            };
            if let Some(status) = status {
                request.record(status, started.elapsed());
            }
            if !READ_RETRY.allows_retry(failures) {
                let _ = sender.send(Err(error)).await;
                return;
            }
            let wait = decision.wait(READ_RETRY, failures).unwrap_or_default();
            tokio::time::sleep(wait).await;
            waits.add(decision, wait);
            failures += 1;
        }
    });
    Ok((receiver, AbortOnDrop(task)))
}

/// Retries every failed read. A server push-back counts as throttling.
fn read_decision(error: &google_cloud_gax::error::Error) -> Decision {
    push_back(error).unwrap_or(Decision::Backoff)
}

/// Decodes the Arrow record batches of one stream.
struct BatchDecoder {
    decoder: StreamDecoder,
}

impl BatchDecoder {
    fn new(schema: &[u8]) -> beam::Result<Self> {
        let mut decoder = StreamDecoder::new();
        let mut buffer = Buffer::from(schema.to_vec());
        while !buffer.is_empty() {
            decoder.decode(&mut buffer).map_err(beam::Error::msg)?;
        }
        Ok(Self { decoder })
    }

    fn decode(&mut self, bytes: Bytes) -> beam::Result<Vec<RecordBatch>> {
        let mut buffer = Buffer::from(bytes);
        let mut batches = Vec::new();
        while !buffer.is_empty() {
            if let Some(batch) = self.decoder.decode(&mut buffer).map_err(beam::Error::msg)? {
                batches.push(batch);
            }
        }
        Ok(batches)
    }
}

/// How a stream reader converts and emits the rows of a record batch.
pub(super) trait BatchOutput: Clone + Send + Sync + 'static {
    type Out: Outputs;

    /// Emits the rows of `batch`. `first_row` is the stream and the offset of the first row.
    fn emit_batch(
        &self,
        batch: &RecordBatch,
        first_row: (&str, i64),
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result;
}

impl<T: BeamRow + DefaultCoder> BatchOutput for BeamRowCodec<T> {
    type Out = T;

    fn emit_batch(
        &self,
        batch: &RecordBatch,
        _first_row: (&str, i64),
        ctx: &mut ProcessContext<'_, T>,
    ) -> beam::Result {
        batch_to_beam_rows::<T>(batch)?
            .into_iter()
            .try_for_each(|value| ctx.emit(value))
    }
}

impl BatchOutput for SchemaRowCodec {
    type Out = RowElement;

    fn emit_batch(
        &self,
        batch: &RecordBatch,
        _first_row: (&str, i64),
        ctx: &mut ProcessContext<'_, RowElement>,
    ) -> beam::Result {
        let batch = beam_batch(batch)?;
        self.decode_batch(&batch)
            .map_err(beam::Error::msg)?
            .into_iter()
            .try_for_each(|row| ctx.emit(row))
    }
}

/// Reads one stream of a session, from the start offset of its restriction. Emits the rows of
/// each batch with `output`. A row that does not convert is in a claimed range, so the
/// restriction counts it as read.
#[derive(Clone)]
pub(super) struct ReadStreamFn<O> {
    output: O,
    client: Option<Read>,
    lineage_table: Option<[String; 3]>,
}

impl<O> ReadStreamFn<O> {
    pub(super) fn new(output: O, client: Option<Read>, lineage_table: Option<[String; 3]>) -> Self {
        Self {
            output,
            client,
            lineage_table,
        }
    }
}

impl<O: BatchOutput> SplittableDoFn for ReadStreamFn<O> {
    type In = (StreamRestriction, Vec<u8>);
    type Out = O::Out;
    type Restriction = StreamRestriction;
    type Tracker = StreamTracker;

    fn initial_restriction(&self, (restriction, _): &Self::In) -> beam::Result<StreamRestriction> {
        Ok(restriction.clone())
    }

    /// The estimate of the bytes left, so runners and autoscalers see the backlog.
    fn restriction_size(&self, _element: &Self::In, restriction: &StreamRestriction) -> f64 {
        restriction.bytes_after(restriction.start).max(1.0)
    }

    fn create_tracker(&self, restriction: &StreamRestriction) -> StreamTracker {
        StreamTracker::new(restriction.clone()).with_client(self.client.clone())
    }

    fn process_element(
        &mut self,
        (_, schema): Self::In,
        tracker: &StreamTracker,
        ctx: &mut ProcessContext<'_, O::Out>,
    ) -> beam::Result<ProcessContinuation> {
        if let Some(segments) = &self.lineage_table {
            Lineage::sources().add(METRICS, segments.iter().map(String::as_str));
        }
        let mut decoder = BatchDecoder::new(&schema)?;
        let restriction = tracker.current_restriction();
        let (mut stream, mut offset) = (restriction.stream, restriction.start);
        let waits = Arc::new(RetryWaits::default());
        let (mut responses, mut task) = fetch_responses(
            self.client.clone(),
            stream.clone(),
            offset,
            Arc::clone(&waits),
        )?;
        let mut emitted = false;
        loop {
            let received = match responses.try_recv() {
                Ok(item) => Some(Some(item)),
                Err(mpsc::error::TryRecvError::Disconnected) => Some(None),
                Err(mpsc::error::TryRecvError::Empty) => {
                    if emitted {
                        ctx.flush_output()?;
                        emitted = false;
                    }
                    recv_blocking(&mut responses)
                }
            };
            waits.record(METRICS);
            let response = match received {
                Some(Some(Ok(Fetched::Response(response)))) => response,
                Some(Some(Ok(Fetched::End))) => break,
                Some(Some(Err(e))) => {
                    return Err(beam::Error::msg(format!(
                        "Failed to read BigQuery stream '{stream}': {e}"
                    )));
                }
                Some(None) => {
                    return Err(beam::Error::msg(format!(
                        "The read task of BigQuery stream '{stream}' stopped before the end \
                         of the stream"
                    )));
                }
                None => return Err(beam::Error::msg("The async I/O runtime cannot start")),
            };
            if let Some(progress) = response.stats.as_ref().and_then(|s| s.progress.as_ref()) {
                tracker.set_fraction(progress.at_response_end);
            }
            if response.row_count == 0 {
                continue;
            }
            let end = offset + response.row_count;
            if !tracker.try_claim(&(stream.clone(), end)) {
                // After a server split, the same rows continue in the primary stream.
                let current = tracker.current_restriction();
                if current.stream == stream {
                    return Ok(ProcessContinuation::Stop);
                }
                stream = current.stream;
                offset = tracker.claimed_until();
                (responses, task) = fetch_responses(
                    self.client.clone(),
                    stream.clone(),
                    offset,
                    Arc::clone(&waits),
                )?;
                continue;
            }
            // A clone of `Bytes` shares the buffer and copies no row.
            let bytes = response
                .arrow_record_batch()
                .map(|batch| batch.serialized_record_batch.clone())
                .unwrap_or_default();
            let byte_len = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
            let row_count = response.row_count;
            decoder
                .decode(bytes)?
                .iter()
                .try_fold(offset, |first, batch| {
                    self.output.emit_batch(batch, (&stream, first), ctx)?;
                    Ok::<_, beam::Error>(first + batch.num_rows() as i64)
                })?;
            emitted = true;
            Metrics::counter(METRICS, "rows_read").inc_by(row_count);
            Metrics::counter(METRICS, "bytes_read").inc_by(byte_len);
            offset = end;
        }
        drop(task);
        waits.record(METRICS);
        tracker.finish(offset);
        Ok(ProcessContinuation::Stop)
    }
}
