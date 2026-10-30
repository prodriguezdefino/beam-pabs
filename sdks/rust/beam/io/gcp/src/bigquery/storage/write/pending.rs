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
use std::sync::Arc;
use std::time::Duration;

use beam::coders::BeamIterable;
use beam::schema::{Row, RowElement, Schema};
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};
use beam::values::{AnySideInput, SingletonView};
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::error::WriterBuilderError;
use google_cloud_bigquery::write::PendingWriter;
use google_cloud_bigquery::write::format::Arrow;
use tokio::task::JoinHandle;

use super::append::{Failure, append_at};
use super::buffer::{InFlightLimit, RowBuffer};
use super::client::{APPEND_RETRY, CALL_RETRY, is_transient, with_retries, write_client};
use super::commit::commit_streams;
use super::emit_global;
use super::encode::{Encoded, Encoder};
use super::encoded_batch::{EncodedBatch, Request};
use super::projection::Selection;
use super::table::Destination;
use crate::runtime::{block_on_async, client_runtime};

/// The table and the stream to commit, or a rejected row.
type Out = Result<(String, String), Row>;

/// How many times a failed append opens a new connection before the bundle fails, as the
/// Java SDK retries an append that fails for another reason than quota.
const MAX_RECONNECTS: u32 = 5;

/// An append that the bundle sent at a planned offset.
struct Planned {
    request: Request,
    offset: i64,
    task: JoinHandle<Result<(), Failure>>,
}

/// The pending stream of the current bundle.
struct Bundle {
    stream: Arc<PendingWriter<Arrow>>,
    planned: VecDeque<Planned>,
    /// The offset of the next append that the bundle sends.
    next: i64,
    /// The rows that BigQuery has, in order, from the start of the stream.
    written: i64,
    limit: InFlightLimit,
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
pub(super) struct WritePendingFn {
    destination: SingletonView<Destination>,
    selection: Arc<Selection>,
    errors: Arc<Schema>,
    connect: Connect,
    encoder: Option<Arc<(Encoder, String)>>,
    rows: RowBuffer,
    bundle: Option<Bundle>,
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
            with_retries(
                APPEND_RETRY,
                |e: &WriterBuilderError| match e {
                    WriterBuilderError::Rpc { source, .. } => {
                        is_transient(source) || source.http_status_code() == Some(404)
                    }
                    _ => false,
                },
                || {
                    let builder = match &stream {
                        Some(stream) => client.attach_to_stream(stream),
                        None => client.create_stream(&table),
                    };
                    builder.build_arrow(schema.clone())
                },
            )
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

impl WritePendingFn {
    pub(super) fn new(
        destination: SingletonView<Destination>,
        selection: Arc<Selection>,
        errors: Arc<Schema>,
        client: Option<Arc<Write>>,
        append_timeout: Duration,
    ) -> Self {
        Self {
            destination,
            selection,
            errors,
            connect: Connect {
                client,
                append_timeout,
            },
            encoder: None,
            rows: RowBuffer::default(),
            bundle: None,
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
        Ok(Bundle {
            stream: Arc::new(self.connect.writer(encoder, None)?),
            planned: VecDeque::new(),
            next: 0,
            written: 0,
            limit: InFlightLimit::default(),
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
        while (all && !bundle.planned.is_empty()) || bundle.limit.is_full() {
            let Some(Planned {
                request,
                offset,
                task,
            }) = bundle.planned.pop_front()
            else {
                break;
            };
            bundle.limit.remove(&request);
            match join(task)? {
                Ok(()) if offset == bundle.written => bundle.written += request.count,
                result => Self::recover(
                    connect,
                    (request, offset, result),
                    bundle,
                    encoder,
                    rejected,
                )?,
            }
        }
        Ok(())
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
        bundle.limit = InFlightLimit::default();
        let results = std::iter::once(Ok(first))
            .chain(std::mem::take(&mut bundle.planned).into_iter().map(
                |Planned {
                     request,
                     offset,
                     task,
                 }| Ok((request, offset, join(task)?)),
            ))
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
                    let (stream, rows, offset) = (
                        Arc::clone(&bundle.stream),
                        request.rows.clone(),
                        bundle.written,
                    );
                    // The failed call may have written the rows, so `Exists` counts as success.
                    let retried = failure.as_ref().is_some_and(Failure::needs_new_connection);
                    let timeout = connect.append_timeout;
                    block_on_async(async move {
                        Ok(append_at(
                            |rows, offset| stream.append(rows).set_offset(offset).send(),
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
                Ok(()) => bundle.written += request.count,
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

impl WritePendingFn {
    /// Converts the buffered rows and appends their batches. The rows that do not convert or
    /// that BigQuery rejects go to `rejected`.
    fn send(&mut self, encoder: &Arc<(Encoder, String)>, rejected: &mut Vec<Row>) -> beam::Result {
        let rows = self.rows.take();
        if rows.is_empty() {
            return Ok(());
        }
        let Encoded { batches, errors } = encoder.0.encode(&rows)?;
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
        let (stream, rows, offset) = (
            Arc::clone(&bundle.stream),
            request.rows.clone(),
            bundle.next,
        );
        let timeout = self.connect.append_timeout;
        let task = client_runtime()?.spawn(async move {
            append_at(
                |rows, offset| stream.append(rows).set_offset(offset).send(),
                &rows,
                offset,
                false,
                timeout,
            )
            .await
        });
        bundle.next += request.count;
        bundle.limit.add(&request);
        bundle.planned.push_back(Planned {
            request,
            offset,
            task,
        });
        let result = Self::settle(&self.connect, &mut bundle, encoder, false, rejected);
        self.bundle = Some(bundle);
        result
    }
}

fn join(task: JoinHandle<Result<(), Failure>>) -> beam::Result<Result<(), Failure>> {
    Ok(block_on_async(async move {
        task.await.map_err(io::Error::other)
    })?)
}

/// Each copy starts with no stream, an empty buffer and its own encoder.
impl Clone for WritePendingFn {
    fn clone(&self) -> Self {
        Self::new(
            self.destination.clone(),
            Arc::clone(&self.selection),
            Arc::clone(&self.errors),
            self.connect.client.clone(),
            self.connect.append_timeout,
        )
    }
}

impl DoFn for WritePendingFn {
    type In = RowElement;
    type Out = Out;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.destination]
    }

    fn process_element(
        &mut self,
        row: RowElement,
        ctx: &mut ProcessContext<'_, Out>,
    ) -> beam::Result {
        let encoder = self.encoder(ctx)?;
        if !self.rows.push(row) {
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
            .try_for_each(|row| emit_global(ctx, Err(row)))?;
        let Some(bundle) = settled? else {
            return Ok(());
        };
        let stream = Arc::clone(&bundle.stream);
        let finalized = block_on_async(async move {
            with_retries(CALL_RETRY, is_transient, || stream.finalize())
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
        emit_global(ctx, Ok((encoder.1.clone(), stream)))
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
