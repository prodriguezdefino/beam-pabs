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
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use arrow_io::RowOutput;
use arrow_io::arrow_array::RecordBatch;
use arrow_io::arrow_buffer::Buffer;
use arrow_io::arrow_ipc::reader::StreamDecoder;
use beam::schema::{RowElement, Schema};
use beam::transforms::sdf::{ProcessContinuation, RestrictionTracker, SplittableDoFn};
use beam::transforms::{Outputs, ProcessContext};
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model::ReadRowsResponse;
use tokio::sync::mpsc;

use super::restriction::{StreamRestriction, StreamTracker};
use super::session::read_client;
use crate::bigquery::storage::schema::{batch_to_encoded_rows, batch_to_rows};
use crate::runtime::{client_runtime, recv_blocking};

/// Responses that a stream reader fetches ahead of the DoFn.
const PREFETCH_RESPONSES: usize = 4;

/// Attempts to read a stream again from the next offset after a failed `ReadRows` call.
const READ_ATTEMPTS: u32 = 5;

/// The longest wait for the next `ReadRows` response before the reader reconnects.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(120);

/// Aborts the task that fetches responses when the DoFn stops reading.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Starts a task that reads `stream` from `offset` and sends each response. After a failed
/// call, the task reads again from the offset after the last response that it sent.
fn fetch_responses(
    client: Option<Read>,
    stream: String,
    offset: i64,
) -> io::Result<(
    mpsc::Receiver<Result<ReadRowsResponse, String>>,
    AbortOnDrop,
)> {
    let (sender, receiver) = mpsc::channel(PREFETCH_RESPONSES);
    let task = client_runtime()?.spawn(async move {
        let mut next_offset = offset;
        let mut failures = 0;
        loop {
            let client = match &client {
                Some(client) => Ok(client.clone()),
                None => read_client().await,
            };
            let error = match client {
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
                                    break format!(
                                        "no response for {} s",
                                        RESPONSE_TIMEOUT.as_secs()
                                    );
                                }
                                Ok(None) => return,
                                Ok(Some(Ok(response))) => {
                                    failures = 0;
                                    next_offset += response.row_count;
                                    if sender.send(Ok(response)).await.is_err() {
                                        return;
                                    }
                                }
                                Ok(Some(Err(e))) => break e.to_string(),
                            }
                        },
                        Err(e) => e.to_string(),
                    }
                }
                Err(e) => e.to_string(),
            };
            failures += 1;
            if failures >= READ_ATTEMPTS {
                let _ = sender.send(Err(error)).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(250 << failures)).await;
        }
    });
    Ok((receiver, AbortOnDrop(task)))
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

    fn decode(&mut self, bytes: Vec<u8>) -> beam::Result<Vec<RecordBatch>> {
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

/// The elements that a stream reader emits for the rows of a record batch.
pub(super) trait StreamOutput: Outputs + Sized {
    /// Emits the rows of `batch` as rows of `schema` in the form that `output` names.
    /// `first_row` is the stream and the offset of the first row of `batch`.
    fn emit_batch(
        batch: &RecordBatch,
        schema: &Arc<Schema>,
        output: RowOutput,
        first_row: (&str, i64),
        ctx: &mut ProcessContext<'_, Self>,
    ) -> beam::Result;
}

/// Fails on the first row that does not convert, with no work for each row.
impl StreamOutput for RowElement {
    fn emit_batch(
        batch: &RecordBatch,
        schema: &Arc<Schema>,
        output: RowOutput,
        _first_row: (&str, i64),
        ctx: &mut ProcessContext<'_, Self>,
    ) -> beam::Result {
        match output {
            RowOutput::Encoded => batch_to_encoded_rows(batch, schema)?
                .into_iter()
                .try_for_each(|row| ctx.emit(RowElement::Encoded(row))),
            RowOutput::Values => batch_to_rows(batch, schema)?
                .into_iter()
                .try_for_each(|row| ctx.emit(RowElement::Row(row))),
        }
    }
}

/// Reads one stream of a session, from the start offset of its restriction. Emits the rows of
/// each batch as `O`. A row that does not convert is in a claimed range, so the restriction
/// counts it as read.
pub(super) struct ReadStreamFn<O> {
    schema: Arc<Schema>,
    client: Option<Read>,
    rows: RowOutput,
    output: PhantomData<fn() -> O>,
}

impl<O> ReadStreamFn<O> {
    pub(super) fn new(schema: Arc<Schema>, client: Option<Read>, rows: RowOutput) -> Self {
        Self {
            schema,
            client,
            rows,
            output: PhantomData,
        }
    }
}

impl<O> Clone for ReadStreamFn<O> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.schema), self.client.clone(), self.rows)
    }
}

impl<O: StreamOutput> SplittableDoFn for ReadStreamFn<O> {
    type In = (StreamRestriction, Vec<u8>);
    type Out = O;
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
        ctx: &mut ProcessContext<'_, O>,
    ) -> beam::Result<ProcessContinuation> {
        let mut decoder = BatchDecoder::new(&schema)?;
        let restriction = tracker.current_restriction();
        let (mut stream, mut offset) = (restriction.stream, restriction.start);
        let (mut responses, mut task) =
            fetch_responses(self.client.clone(), stream.clone(), offset)?;
        loop {
            let response = match recv_blocking(&mut responses) {
                Some(Some(response)) => response,
                Some(None) => break,
                None => return Err(beam::Error::msg("The BigQuery reader thread panicked")),
            }
            .map_err(|e| {
                beam::Error::msg(format!("Failed to read BigQuery stream '{stream}': {e}"))
            })?;
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
                (responses, task) = fetch_responses(self.client.clone(), stream.clone(), offset)?;
                continue;
            }
            let bytes = response
                .arrow_record_batch()
                .map(|batch| batch.serialized_record_batch.to_vec())
                .unwrap_or_default();
            decoder
                .decode(bytes)?
                .iter()
                .try_fold(offset, |first, batch| {
                    O::emit_batch(batch, &self.schema, self.rows, (&stream, first), ctx)?;
                    Ok::<_, beam::Error>(first + batch.num_rows() as i64)
                })?;
            offset = end;
        }
        drop(task);
        tracker.finish(offset);
        Ok(ProcessContinuation::Stop)
    }
}
