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

//! The exactly-once writer of a bounded input: each bundle appends to its own pending
//! stream, and a last step commits the streams of the table together.

use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::{BeamIterable, DefaultCoder};
use beam::schema::{Row, Schema};
use beam::transforms::{BundleWindows, DoFn, FinishBundleContext, ProcessContext};
use beam::values::{AnySideInput, SingletonView};
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::write::PendingWriter;
use google_cloud_bigquery::write::format::Arrow;
use tokio::task::JoinError;

use beam_io_arrow::EncodedBatch;

use super::append::{AppendStats, Failure, append_at, append_at_stats};
use super::buffer::{RowBuffer, in_flight_limit};
use super::client::{APPEND_RETRY, CALL_RETRY, open_decision, transient_decision, write_client};
use super::commit::commit_streams;
use super::encode::{Encoded, Encoder, WriteCodec};
use super::encoded_batch::Request;
use super::projection::Selection;
use super::table::Destination;
use super::{GLOBAL, emit_global, finish_timestamp};
use crate::bigquery::METRICS;
use crate::bigquery::table::{api_request, record_sink_lineage};
use crate::runtime::{block_on_async, client_runtime};
use beam::internals::InFlightWindow;
use beam::metrics::Metrics;
use beam::retry::with_throttled_retries;
use beam_gcp_core::retry::status_name;

/// The table and the stream to commit, or a rejected row.
type Out = Result<(String, String), Row>;

/// How many times a failed append opens a new connection before the bundle fails, as the
/// Java SDK retries an append that fails for another reason than quota.
const MAX_RECONNECTS: u32 = 5;

/// Stops draining planned appends when one fails or finishes out of order.
enum SettleStop {
    Recover(Request, i64, Result<(), Failure>),
    Fatal(beam::Error),
}

/// The pending stream of the current bundle.
struct Bundle {
    stream: Arc<PendingWriter<Arrow>>,
    planned: InFlightWindow<(Request, i64), (Result<(), Failure>, AppendStats)>,
    /// The offset of the next append that the bundle sends.
    next: i64,
    /// The rows that BigQuery has, in order, from the start of the stream.
    written: i64,
}

/// Converts the rows of each bundle into Arrow batches, appends them to a new pending stream at
/// offsets that follow each other, and finalizes the stream in `finish_bundle`. Emits the table
/// and the stream to the commit step, and the rows that do not convert or that BigQuery rejects
/// to the error output.
///
/// The writer has no shuffle before it. A retried bundle converts its rows again and writes
/// them to a new stream. The stream of the failed attempt is never emitted, so the commit step
/// does not commit it and BigQuery drops its rows.
///
/// The appends of a bundle run at the same time. If one fails, every later append fails too,
/// because its offset is after the end of the stream. The writer then waits for all appends,
/// and writes the failed batches again in order.
pub(super) struct WritePendingFn<C, T> {
    destination: SingletonView<Destination>,
    codec: C,
    selection: Arc<Selection>,
    errors: Arc<Schema>,
    connect: Connect,
    encoder: Option<Arc<(Encoder, String)>>,
    rows: RowBuffer<T>,
    bundle: Option<Bundle>,
    windows: BundleWindows,
    _marker: PhantomData<fn(T)>,
}

/// How the writer opens and reopens the connection of a pending stream.
#[derive(Clone)]
struct Connect {
    client: Option<Arc<Write>>,
    append_timeout: Duration,
}

impl Connect {
    /// Opens a writer for a new pending stream of `encoder`'s table, or for `stream` when set.
    fn writer(
        &self,
        encoder: &(Encoder, String),
        stream: Option<&str>,
    ) -> beam::Result<PendingWriter<Arrow>> {
        let (client, schema, table) = (
            self.client.clone(),
            encoder.0.writer_schema(),
            encoder.1.clone(),
        );
        let stream = stream.map(str::to_string);
        block_on_async(async move {
            let client = write_client(client).await?;
            with_throttled_retries(APPEND_RETRY, METRICS, open_decision, || {
                let builder = match &stream {
                    Some(stream) => client.attach_to_stream(stream),
                    None => client.create_stream(&table),
                };
                builder.build_arrow(schema.clone())
            })
            .await
            .map_err(io::Error::other)
        })
        .map_err(|e| beam::Error::from(e).context("Cannot open a pending stream"))
    }

    /// Replaces the writer of `bundle` with a writer on a new connection to the same stream.
    fn reconnect(&self, bundle: &mut Bundle, encoder: &(Encoder, String)) -> beam::Result {
        let stream = bundle.stream.write_stream().to_string();
        tracing::warn!("Opening a new connection to pending stream '{stream}'");
        bundle.stream = Arc::new(self.writer(encoder, Some(&stream))?);
        Ok(())
    }
}

impl<C, T> WritePendingFn<C, T> {
    pub(super) fn new(
        destination: SingletonView<Destination>,
        codec: C,
        selection: Arc<Selection>,
        errors: Arc<Schema>,
        client: Option<Arc<Write>>,
        append_timeout: Duration,
    ) -> Self {
        Self {
            destination,
            codec,
            selection,
            errors,
            connect: Connect {
                client,
                append_timeout,
            },
            encoder: None,
            rows: RowBuffer::default(),
            bundle: None,
            windows: BundleWindows::new(),
            _marker: PhantomData,
        }
    }

    /// The encoder and the table resource, from the side input on the first element.
    fn encoder(&mut self, ctx: &ProcessContext<'_, Out>) -> beam::Result<Arc<(Encoder, String)>> {
        if let Some(encoder) = &self.encoder {
            return Ok(Arc::clone(encoder));
        }
        let destination = ctx.side_input(&self.destination)?;
        let encoder = Encoder::new(&self.selection, Arc::clone(&self.errors), &destination)?;
        let encoder = Arc::new((encoder, destination.table));
        self.encoder = Some(Arc::clone(&encoder));
        Ok(encoder)
    }

    fn open(&self, encoder: &(Encoder, String)) -> beam::Result<Bundle> {
        let stream = Arc::new(self.connect.writer(encoder, None)?);
        record_sink_lineage(&encoder.1);
        Ok(Bundle {
            stream,
            planned: InFlightWindow::from_limit(in_flight_limit()),
            next: 0,
            written: 0,
        })
    }

    /// Waits for appends from the oldest, while the in-flight limit is full, or for all of them
    /// if `all` is set.
    fn settle(
        connect: &Connect,
        bundle: &mut Bundle,
        encoder: &(Encoder, String),
        all: bool,
        rejected: &mut Vec<Row>,
    ) -> beam::Result {
        let written = &mut bundle.written;
        let settled = if all {
            bundle
                .planned
                .drain_all(|meta, result| settle_entry(written, meta, result))
        } else {
            bundle
                .planned
                .drain_while_full(|meta, result| settle_entry(written, meta, result))
        };
        match settled {
            Ok(()) => Ok(()),
            Err(SettleStop::Fatal(err)) => {
                bundle.planned.clear();
                Err(err)
            }
            Err(SettleStop::Recover(request, offset, result)) => Self::recover(
                connect,
                (request, offset, result),
                bundle,
                encoder,
                rejected,
            ),
        }
    }

    /// Waits for every append in flight, then writes the failed batches again in order at the
    /// end of the stream. No append runs while the batches are written again. A failure that
    /// needs a new connection reopens the stream first, as the Java SDK does.
    fn recover(
        connect: &Connect,
        first: (Request, i64, Result<(), Failure>),
        bundle: &mut Bundle,
        encoder: &(Encoder, String),
        rejected: &mut Vec<Row>,
    ) -> beam::Result {
        let results = std::iter::once(Ok(first))
            .chain(
                bundle
                    .planned
                    .take_all()
                    .into_iter()
                    .map(|((request, offset), result)| {
                        Ok((request, offset, finish_append(result)?))
                    }),
            )
            .collect::<beam::Result<Vec<_>>>()?;
        if results
            .iter()
            .any(|(_, _, result)| result.as_ref().is_err_and(Failure::needs_new_connection))
        {
            connect.reconnect(bundle, encoder)?;
        }
        results
            .into_iter()
            .try_for_each(|(request, offset, result)| match result {
                Ok(()) if offset == bundle.written => {
                    request.record_written();
                    bundle.written += request.count;
                    Ok(())
                }
                Ok(()) => Err(beam::Error::msg(format!(
                    "An append at offset {offset} succeeded after a failed append at {}",
                    bundle.written
                ))),
                Err(failure) => Self::rewrite(connect, request, failure, bundle, encoder, rejected),
            })?;
        bundle.next = bundle.written;
        Ok(())
    }

    /// Appends `request` at the end of the stream. Rejected rows go to the error output, and
    /// the other rows are appended again. A failure that needs a new connection reopens the
    /// stream, at most [`MAX_RECONNECTS`] times.
    fn rewrite(
        connect: &Connect,
        request: Request,
        failure: Failure,
        bundle: &mut Bundle,
        encoder: &(Encoder, String),
        rejected: &mut Vec<Row>,
    ) -> beam::Result {
        let mut pending = VecDeque::from([(request, Some(failure))]);
        let mut reconnects = 0;
        while let Some((request, failure)) = pending.pop_front() {
            let result = match failure {
                Some(failure @ Failure::Rows(_)) => Err(failure),
                failure => {
                    if failure.is_some() {
                        Metrics::counter(METRICS, "append_retries").inc();
                    }
                    let (stream, rows, offset) = (
                        Arc::clone(&bundle.stream),
                        request.rows.clone(),
                        bundle.written,
                    );
                    // The failed call may have written the rows, so `Exists` counts as success.
                    let retried = failure.as_ref().is_some_and(Failure::needs_new_connection);
                    let timeout = connect.append_timeout;
                    let appends = api_request(&encoder.1, "AppendRows");
                    block_on_async(async move {
                        Ok(append_at(
                            |rows, offset| stream.append(rows).set_offset(offset).send(),
                            &appends,
                            &rows,
                            offset,
                            retried,
                            timeout,
                        )
                        .await)
                    })?
                }
            };
            match result {
                Ok(()) => {
                    request.record_written();
                    bundle.written += request.count;
                }
                Err(Failure::Rows(rows)) if !rows.is_empty() => {
                    let Encoded { batches, errors } = encoder.0.split_rejected(&request, rows)?;
                    rejected.extend(errors);
                    batches
                        .into_iter()
                        .rev()
                        .for_each(|batch| pending.push_front((Request::from(batch), None)));
                }
                Err(failure) if failure.needs_new_connection() && reconnects < MAX_RECONNECTS => {
                    reconnects += 1;
                    connect.reconnect(bundle, encoder)?;
                    pending.push_front((request, Some(failure)));
                }
                Err(failure) => {
                    return Err(beam::Error::msg(format!(
                        "An append to a pending stream failed: {}",
                        failure.message()
                    )));
                }
            }
        }
        Ok(())
    }
}

impl<C: WriteCodec<T>, T: 'static> WritePendingFn<C, T> {
    /// Converts the buffered elements and appends their batches. Rows that do not convert or
    /// that BigQuery rejects go to `rejected`.
    fn send(&mut self, encoder: &Arc<(Encoder, String)>, rejected: &mut Vec<Row>) -> beam::Result {
        if self.rows.is_empty() {
            return Ok(());
        }
        let Encoded { batches, errors } = match encoder.0.encode(&self.codec, self.rows.rows()) {
            Ok(encoded) => {
                self.rows.finish_batch(&encoded.batches);
                encoded
            }
            Err(err) => {
                self.rows.clear();
                return Err(err);
            }
        };
        rejected.extend(errors);
        batches
            .into_iter()
            .try_for_each(|batch| self.append(batch, encoder, rejected))
    }

    /// Sends `batch` at the next offset of the stream of the bundle, which it opens on the
    /// first batch.
    fn append(
        &mut self,
        batch: EncodedBatch,
        encoder: &Arc<(Encoder, String)>,
        rejected: &mut Vec<Row>,
    ) -> beam::Result {
        let mut bundle = match self.bundle.take() {
            Some(bundle) => bundle,
            None => self.open(encoder)?,
        };
        let request = Request::from(batch);
        let count = request.count;
        let bytes = request.rows.serialized_record_batch.len();
        let (stream, rows, offset) = (
            Arc::clone(&bundle.stream),
            request.rows.clone(),
            bundle.next,
        );
        let timeout = self.connect.append_timeout;
        let appends = api_request(&encoder.1, "AppendRows");
        let handle = client_runtime()?.handle();
        bundle
            .planned
            .spawn(handle, (request, offset), bytes, async move {
                append_at_stats(
                    |rows, offset| stream.append(rows).set_offset(offset).send(),
                    &appends,
                    &rows,
                    offset,
                    false,
                    timeout,
                )
                .await
            });
        bundle.next += count;
        let result = Self::settle(&self.connect, &mut bundle, encoder, false, rejected);
        self.bundle = Some(bundle);
        result
    }
}

fn settle_entry(
    written: &mut i64,
    (request, offset): (Request, i64),
    result: Result<(Result<(), Failure>, AppendStats), JoinError>,
) -> Result<(), SettleStop> {
    let outcome = finish_append(result).map_err(SettleStop::Fatal)?;
    match outcome {
        Ok(()) if offset == *written => {
            request.record_written();
            *written += request.count;
            Ok(())
        }
        result => Err(SettleStop::Recover(request, offset, result)),
    }
}

fn finish_append(
    result: Result<(Result<(), Failure>, AppendStats), JoinError>,
) -> beam::Result<Result<(), Failure>> {
    let (outcome, stats) = result.map_err(io::Error::other)?;
    stats.record();
    Ok(outcome)
}

/// Each copy starts with no stream, an empty buffer and its own encoder.
impl<C: Clone, T> Clone for WritePendingFn<C, T> {
    fn clone(&self) -> Self {
        Self::new(
            self.destination.clone(),
            self.codec.clone(),
            Arc::clone(&self.selection),
            Arc::clone(&self.errors),
            self.connect.client.clone(),
            self.connect.append_timeout,
        )
    }
}

impl<C: WriteCodec<T>, T: DefaultCoder> DoFn for WritePendingFn<C, T> {
    type In = T;
    type Out = Out;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.destination]
    }

    fn process_element(&mut self, row: T, ctx: &mut ProcessContext<'_, Out>) -> beam::Result {
        let encoder = self.encoder(ctx)?;
        self.windows.add(GLOBAL, ctx.timestamp());
        let bytes = self.codec.estimated_bytes(&row);
        if !self.rows.push(row, bytes) {
            return Ok(());
        }
        let mut rejected = Vec::new();
        let sent = self.send(&encoder, &mut rejected);
        rejected
            .into_iter()
            .try_for_each(|row| ctx.emit(Err(row)))?;
        sent
    }

    /// The write rewindows its input into the global window, so the outputs go there too.
    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Out>) -> beam::Result {
        let timestamp = finish_timestamp(&mut self.windows);
        let Some(encoder) = self.encoder.clone() else {
            return Ok(());
        };
        let mut rejected = Vec::new();
        let settled = self.send(&encoder, &mut rejected).and_then(|()| {
            let Some(mut bundle) = self.bundle.take() else {
                return Ok(None);
            };
            Self::settle(&self.connect, &mut bundle, &encoder, true, &mut rejected)?;
            Ok(Some(bundle))
        });
        rejected
            .into_iter()
            .try_for_each(|row| emit_global(ctx, Err(row), timestamp))?;
        let Some(bundle) = settled? else {
            return Ok(());
        };
        let stream = Arc::clone(&bundle.stream);
        let finalizes = api_request(&encoder.1, "FinalizeWriteStream");
        let finalized = block_on_async(async move {
            with_throttled_retries(CALL_RETRY, METRICS, transient_decision, || {
                finalizes.time(stream.finalize(), status_name)
            })
            .await
            .map_err(io::Error::other)
        })
        .map_err(|e| beam::Error::from(e).context("Cannot finalize a pending stream"))?;
        if finalized.row_count != bundle.written {
            return Err(beam::Error::msg(format!(
                "The pending stream has {} rows after finalize, but the bundle wrote {}",
                finalized.row_count, bundle.written
            )));
        }
        let stream = bundle.stream.write_stream().to_string();
        emit_global(ctx, Ok((encoder.1.clone(), stream)), timestamp)
    }

    fn teardown(&mut self) -> beam::Result {
        self.bundle = None;
        Ok(())
    }
}

/// Commits the finalized pending streams of one table in one `BatchCommitWriteStreams` call,
/// so the rows of all bundles become visible together. A retry finds the streams committed
/// and succeeds.
#[derive(Clone)]
pub(super) struct CommitFn;

impl DoFn for CommitFn {
    type In = (String, BeamIterable<String>);
    type Out = i32;

    fn process_element(
        &mut self,
        (table, streams): (String, BeamIterable<String>),
        _ctx: &mut ProcessContext<'_, i32>,
    ) -> beam::Result {
        let streams = streams
            .try_into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| beam::Error::msg(format!("Cannot read the streams of '{table}': {e}")))?;
        let count = streams.len();
        let parent = table.clone();
        block_on_async(async move { commit_streams(&parent, streams).await }).map_err(|e| {
            beam::Error::from(e).context(format!("Cannot commit {count} streams to '{table}'"))
        })
    }
}
