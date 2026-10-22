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

//! The at-least-once writer: each bundle appends its rows to the default stream of the table.

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use beam::schema::{Row, RowElement, Schema};
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};
use beam::values::{AnySideInput, SingletonView};
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::error::{AppendError, WriterBuilderError};
use google_cloud_bigquery::model_ext::AppendResponse;
use google_cloud_bigquery::write::DefaultWriter;
use google_cloud_bigquery::write::format::Arrow;
use tokio::task::JoinHandle;

use super::append::Failure;
use super::buffer::{InFlightLimit, RowBuffer};
use super::client::{APPEND_RETRY, is_transient, with_retries, write_client};
use super::emit_global;
use super::encode::{Encoded, Encoder};
use super::encoded_batch::Request;
use super::projection::Selection;
use super::table::Destination;
use crate::runtime::{block_on_async, client_runtime};

/// The encoder and the default stream writer of one DoFn copy, which its bundles share.
struct Writer {
    encoder: Encoder,
    stream: Arc<DefaultWriter<Arrow>>,
}

/// An append that the bundle sent, with its rows for the case that BigQuery rejects some.
struct InFlight {
    request: Request,
    task: JoinHandle<Result<AppendResponse, AppendError>>,
}

/// Converts the rows of each bundle into Arrow batches and appends them to the default stream,
/// with no shuffle. The appends of a bundle run at the same time, and `finish_bundle` waits
/// for all of them, so the bundle succeeds only when BigQuery has every row. Emits the rows
/// that BigQuery rejects.
pub(super) struct WriteDefaultFn {
    destination: SingletonView<Destination>,
    selection: Arc<Selection>,
    errors: Arc<Schema>,
    client: Option<Arc<Write>>,
    append_timeout: Duration,
    writer: Option<Arc<Writer>>,
    rows: RowBuffer,
    in_flight: VecDeque<InFlight>,
    limit: InFlightLimit,
}

impl WriteDefaultFn {
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
            client,
            append_timeout,
            writer: None,
            rows: RowBuffer::default(),
            in_flight: VecDeque::new(),
            limit: InFlightLimit::default(),
        }
    }

    fn writer(&mut self, ctx: &ProcessContext<'_, Row>) -> beam::Result<Arc<Writer>> {
        if let Some(writer) = &self.writer {
            return Ok(Arc::clone(writer));
        }
        let destination = ctx.side_input(&self.destination)?;
        let encoder = Encoder::new(&self.selection, Arc::clone(&self.errors), &destination)?;
        let (client, schema, table, timeout) = (
            self.client.clone(),
            encoder.writer_schema(),
            destination.table,
            self.append_timeout,
        );
        let stream = block_on_async(async move {
            let client = write_client(client).await?;
            // A new table can answer NOT_FOUND for a short time, so the open is retried.
            with_retries(
                APPEND_RETRY,
                |e: &WriterBuilderError| match e {
                    WriterBuilderError::Rpc { source, .. } => {
                        is_transient(source) || source.http_status_code() == Some(404)
                    }
                    _ => false,
                },
                || {
                    client
                        .open_default_stream(&table)
                        .with_multiplexing(true)
                        // A call with no answer in time goes to a new connection.
                        .with_attempt_timeout(timeout)
                        .build_arrow(schema.clone())
                },
            )
            .await
            .map_err(io::Error::other)
        })
        .map_err(|e| beam::Error::from(e).context("Cannot open the default stream"))?;
        let writer = Arc::new(Writer {
            encoder,
            stream: Arc::new(stream),
        });
        self.writer = Some(Arc::clone(&writer));
        Ok(writer)
    }

    /// Converts the buffered rows and sends their appends. Rows that do not convert or that
    /// BigQuery rejects go to `rejected`.
    fn send(&mut self, rejected: &mut Vec<Row>) -> beam::Result {
        let Some(writer) = self.writer.clone() else {
            return Ok(());
        };
        let rows = self.rows.take();
        if rows.is_empty() {
            return Ok(());
        }
        let Encoded { batches, errors } = writer.encoder.encode(&rows)?;
        rejected.extend(errors);
        batches.into_iter().try_for_each(|batch| {
            let sent = spawn(&writer.stream, Request::from(batch))?;
            self.limit.add(&sent.request);
            self.in_flight.push_back(sent);
            while self.limit.is_full() {
                self.settle_oldest(&writer, rejected)?;
            }
            Ok(())
        })
    }

    /// Waits for the oldest append. Rows that BigQuery rejects go to `rejected`, and the other
    /// rows of the request are appended again.
    fn settle_oldest(&mut self, writer: &Writer, rejected: &mut Vec<Row>) -> beam::Result {
        let Some(InFlight { request, task }) = self.in_flight.pop_front() else {
            return Ok(());
        };
        self.limit.remove(&request);
        let mut pending = VecDeque::from([(request, Some(task))]);
        while let Some((request, task)) = pending.pop_front() {
            let task = match task {
                Some(task) => task,
                None => spawn(&writer.stream, request.clone())?.task,
            };
            let result = block_on_async(async move { task.await.map_err(io::Error::other) })?;
            match result.map_err(Failure::from) {
                Ok(_) => {}
                Err(Failure::Rows(rows)) if !rows.is_empty() => {
                    let Encoded { batches, errors } =
                        writer.encoder.split_rejected(&request, rows)?;
                    rejected.extend(errors);
                    batches
                        .into_iter()
                        .rev()
                        .for_each(|batch| pending.push_front((Request::from(batch), None)));
                }
                Err(failure) => {
                    return Err(beam::Error::msg(format!(
                        "An append to the default stream failed: {}",
                        failure.message()
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Sends one append on the client runtime. The default stream writer retries transient
/// failures itself.
fn spawn(stream: &Arc<DefaultWriter<Arrow>>, request: Request) -> io::Result<InFlight> {
    let stream = Arc::clone(stream);
    let rows = request.rows.clone();
    let task = client_runtime()?.spawn(async move { stream.append(rows).send().await });
    Ok(InFlight { request, task })
}

/// Each copy opens its own writer on the shared connections, and starts with empty buffers.
impl Clone for WriteDefaultFn {
    fn clone(&self) -> Self {
        Self::new(
            self.destination.clone(),
            Arc::clone(&self.selection),
            Arc::clone(&self.errors),
            self.client.clone(),
            self.append_timeout,
        )
    }
}

impl DoFn for WriteDefaultFn {
    type In = RowElement;
    type Out = Row;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.destination]
    }

    fn process_element(
        &mut self,
        row: RowElement,
        ctx: &mut ProcessContext<'_, Row>,
    ) -> beam::Result {
        self.writer(ctx)?;
        if !self.rows.push(row) {
            return Ok(());
        }
        let mut rejected = Vec::new();
        let sent = self.send(&mut rejected);
        ctx.emit_all(rejected)?;
        sent
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Row>) -> beam::Result {
        let mut rejected = Vec::new();
        let settled = self.send(&mut rejected).and_then(|()| {
            let Some(writer) = self.writer.clone() else {
                return Ok(());
            };
            while !self.in_flight.is_empty() {
                self.settle_oldest(&writer, &mut rejected)?;
            }
            Ok(())
        });
        rejected
            .into_iter()
            .try_for_each(|row| emit_global(ctx, row))?;
        settled
    }
}
