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
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::internals::InFlightWindow;
use beam::metrics::ApiRequest;
use beam::retry::{RetryWaits, with_throttled_retries};
use beam::schema::{Row, Schema};
use beam::transforms::{BundleWindows, DoFn, FinishBundleContext, ProcessContext};
use beam::values::{AnySideInput, SingletonView};
use beam_gcp_core::retry::{RETRY_TIME_LIMIT, observe_retries};
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::error::AppendError;
use google_cloud_bigquery::model::ArrowRecordBatch;
use google_cloud_bigquery::model_ext::AppendResponse;
use google_cloud_bigquery::write::DefaultWriter;
use google_cloud_bigquery::write::format::Arrow;
use google_cloud_bigquery::write::retry_policy::RetryableErrors;
use google_cloud_gax::exponential_backoff::ExponentialBackoff;
use google_cloud_gax::retry_policy::RetryPolicyExt as _;
use tokio::task::JoinError;

use super::append::{Failure, append_status};
use super::buffer::{RowBuffer, in_flight_limit};
use super::client::{APPEND_RETRY, open_decision, write_client};
use super::encode::{Encoded, Encoder, WriteCodec};
use super::encoded_batch::Request;
use super::projection::Selection;
use super::table::Destination;
use super::{GLOBAL, emit_global, finish_timestamp};
use crate::bigquery::METRICS;
use crate::bigquery::table::{api_request, record_sink_lineage};
use crate::runtime::{block_on_async, client_runtime};

/// The encoder and the default stream writer of one DoFn copy, which its bundles share.
struct Writer {
    encoder: Encoder,
    table: String,
    stream: Arc<DefaultWriter<Arrow>>,
    waits: Arc<RetryWaits>,
}

/// Converts the elements of each bundle into Arrow batches and appends them to the default
/// stream, with no shuffle. The appends of a bundle run at the same time, and `finish_bundle`
/// waits for all of them, so the bundle succeeds only when BigQuery has every row. Emits the
/// rows that BigQuery rejects.
pub(super) struct WriteDefaultFn<C, T> {
    destination: SingletonView<Destination>,
    codec: C,
    selection: Arc<Selection>,
    errors: Arc<Schema>,
    client: Option<Arc<Write>>,
    append_timeout: Duration,
    writer: Option<Arc<Writer>>,
    rows: RowBuffer<T>,
    in_flight: InFlightWindow<Request, Result<AppendResponse, AppendError>>,
    windows: BundleWindows,
    wrote_in_bundle: bool,
    _marker: PhantomData<fn(T)>,
}

impl<C, T> WriteDefaultFn<C, T> {
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
            client,
            append_timeout,
            writer: None,
            rows: RowBuffer::default(),
            in_flight: InFlightWindow::from_limit(in_flight_limit()),
            windows: BundleWindows::new(),
            wrote_in_bundle: false,
            _marker: PhantomData,
        }
    }
}

impl<C: WriteCodec<T>, T: 'static> WriteDefaultFn<C, T> {
    fn writer(&mut self, ctx: &ProcessContext<'_, Row>) -> beam::Result<Arc<Writer>> {
        if let Some(writer) = &self.writer {
            return Ok(Arc::clone(writer));
        }
        let destination = ctx.side_input(&self.destination)?;
        let encoder = Encoder::new(&self.selection, Arc::clone(&self.errors), &destination)?;
        let table = destination.table;
        let schema = encoder.writer_schema();
        let waits = Arc::new(RetryWaits::default());
        let stream_result = block_on_async(async {
            let client = write_client(self.client.clone()).await?;
            // This loop owns the retries of the open, as the writer's policy retries only the
            // appends. A new table can answer NOT_FOUND for a short time.
            with_throttled_retries(APPEND_RETRY, METRICS, open_decision, || {
                let (retry, backoff) = observe_retries(
                    RetryableErrors.with_time_limit(RETRY_TIME_LIMIT),
                    ExponentialBackoff::default(),
                    Arc::clone(&waits),
                );
                client
                    .open_default_stream(&table)
                    .with_multiplexing(true)
                    .with_retry_policy(retry)
                    .with_backoff_policy(backoff)
                    .with_attempt_timeout(self.append_timeout)
                    .build_arrow(schema.clone())
            })
            .await
            .map_err(io::Error::other)
        });
        waits.record(METRICS);
        let stream = stream_result
            .map_err(|e| beam::Error::from(e).context("Cannot open the default stream"))?;
        let writer = Arc::new(Writer {
            encoder,
            table,
            stream: Arc::new(stream),
            waits,
        });
        self.writer = Some(Arc::clone(&writer));
        Ok(writer)
    }

    /// Converts the buffered elements and sends their appends. Rows that do not convert or
    /// that BigQuery rejects go to `rejected`.
    fn send(&mut self, rejected: &mut Vec<Row>) -> beam::Result {
        let Some(writer) = self.writer.clone() else {
            return Ok(());
        };
        if self.rows.is_empty() {
            return Ok(());
        }
        let Encoded { batches, errors } = match writer.encoder.encode(&self.codec, self.rows.rows())
        {
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
        if batches.is_empty() {
            return Ok(());
        }
        let handle = client_runtime()?.handle();
        let drain = batches.into_iter().try_for_each(|batch| {
            let request = Request::from(batch);
            let bytes = request.rows.serialized_record_batch.len();
            let stream = Arc::clone(&writer.stream);
            let rows = request.rows.clone();
            let appends = api_request(&writer.table, "AppendRows");
            self.in_flight
                .spawn(handle, request, bytes, send_append(stream, rows, appends));
            self.in_flight.drain_while_full(|request, result| {
                settle_append(&writer, &request, result, rejected)
            })
        });
        if drain.is_err() {
            self.in_flight.clear();
        }
        drain
    }
}

/// Sends one append request. The default stream writer retries transient failures itself, so
/// the metrics count one call for all attempts.
async fn send_append(
    stream: Arc<DefaultWriter<Arrow>>,
    rows: ArrowRecordBatch,
    appends: ApiRequest,
) -> Result<AppendResponse, AppendError> {
    appends
        .time(stream.append(rows).send(), append_status)
        .await
}

/// Records an accepted append or splits rejected rows into follow-up requests.
fn handle_append_outcome(
    writer: &Writer,
    request: &Request,
    outcome: Result<AppendResponse, AppendError>,
    rejected: &mut Vec<Row>,
) -> beam::Result<Vec<Request>> {
    match outcome.map_err(Failure::from) {
        Ok(_) => {
            request.record_written();
            Ok(Vec::new())
        }
        Err(Failure::Rows(rows)) if !rows.is_empty() => {
            let Encoded { batches, errors } = writer.encoder.split_rejected(request, rows)?;
            rejected.extend(errors);
            Ok(batches.into_iter().map(Request::from).collect())
        }
        Err(failure) => Err(beam::Error::msg(format!(
            "An append to the default stream failed: {}",
            failure.message()
        ))),
    }
}

/// Settles one completed append. Rows that BigQuery rejects go to `rejected`, and the other
/// rows of the request are appended again.
fn settle_append(
    writer: &Writer,
    request: &Request,
    result: Result<Result<AppendResponse, AppendError>, JoinError>,
    rejected: &mut Vec<Row>,
) -> beam::Result {
    writer.waits.record(METRICS);
    let retry_batches =
        handle_append_outcome(writer, request, result.map_err(io::Error::other)?, rejected)?;
    if retry_batches.is_empty() {
        return Ok(());
    }
    let mut pending: VecDeque<Request> = retry_batches.into();
    while let Some(request) = pending.pop_front() {
        let stream = Arc::clone(&writer.stream);
        let rows = request.rows.clone();
        let appends = api_request(&writer.table, "AppendRows");
        let outcome = block_on_async(async move { Ok(send_append(stream, rows, appends).await) });
        writer.waits.record(METRICS);
        let next = handle_append_outcome(writer, &request, outcome?, rejected)?;
        next.into_iter()
            .rev()
            .for_each(|batch| pending.push_front(batch));
    }
    Ok(())
}

/// Each copy opens its own writer on the shared connections, and starts with empty buffers.
impl<C: Clone, T> Clone for WriteDefaultFn<C, T> {
    fn clone(&self) -> Self {
        Self::new(
            self.destination.clone(),
            self.codec.clone(),
            Arc::clone(&self.selection),
            Arc::clone(&self.errors),
            self.client.clone(),
            self.append_timeout,
        )
    }
}

impl<C: WriteCodec<T>, T: DefaultCoder> DoFn for WriteDefaultFn<C, T> {
    type In = T;
    type Out = Row;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.destination]
    }

    fn process_element(&mut self, row: T, ctx: &mut ProcessContext<'_, Row>) -> beam::Result {
        self.writer(ctx)?;
        self.wrote_in_bundle = true;
        self.windows.add(GLOBAL, ctx.timestamp());
        let bytes = self.codec.estimated_bytes(&row);
        if !self.rows.push(row, bytes) {
            return Ok(());
        }
        let mut rejected = Vec::new();
        let sent = self.send(&mut rejected);
        ctx.emit_all(rejected)?;
        sent
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Row>) -> beam::Result {
        let wrote = std::mem::take(&mut self.wrote_in_bundle);
        let mut rejected = Vec::new();
        let settled = self.send(&mut rejected).and_then(|()| {
            let Some(writer) = self.writer.clone() else {
                return Ok(());
            };
            self.in_flight.drain_all(|request, result| {
                settle_append(&writer, &request, result, &mut rejected)
            })?;
            if wrote {
                record_sink_lineage(&writer.table);
            }
            Ok(())
        });
        if settled.is_err() {
            self.in_flight.clear();
        }
        let timestamp = finish_timestamp(&mut self.windows);
        rejected
            .into_iter()
            .try_for_each(|row| emit_global(ctx, row, timestamp))?;
        settled
    }

    fn teardown(&mut self) -> beam::Result {
        self.in_flight.clear();
        Ok(())
    }
}
