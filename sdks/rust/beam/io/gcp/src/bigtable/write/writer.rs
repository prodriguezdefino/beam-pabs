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

//! [`WriteMutationsFn`] `DoFn` with concurrent in-flight `MutateRows` tasks, partial-failure
//! retries, and error-output routing.

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;

use beam::Instant;
use beam::schema::{Row, RowElement, Schema};
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};
use beam::windowing::{BoundedWindow, GlobalWindow};
use tokio::task::JoinHandle;

use super::batch::{BatchBuffer, BatchOutcome, BatchRequest, InFlightLimit, partition_outcomes};
use super::decode::{MutationInputKind, PendingEntry, decode_mutation_row};
use super::options::WriteOptions;
use crate::bigtable::client::{BigtableClient, RetryPolicy, RoutingCookies, is_transient};
use crate::bigtable::proto::{MutateRowsRequest, MutateRowsResponse, Status};
use crate::bigtable::retry::wait_before_retry;
use crate::runtime::{block_on_async, client_runtime};

struct InFlightBatch {
    bytes: usize,
    task: JoinHandle<Result<Vec<(PendingEntry, Status)>, String>>,
}

/// `DoFn` that batches incoming [`RowElement`]s per bundle, sends concurrent `MutateRows`
/// RPCs on [`client_runtime`], retries transient partial row failures, and emits permanently
/// rejected rows with [`error_schema`](super::error_schema).
pub struct WriteMutationsFn {
    pub options: WriteOptions,
    pub kind: MutationInputKind,
    pub input_schema: Arc<Schema>,
    pub err_schema: Arc<Schema>,
    pub client: Option<Arc<BigtableClient>>,
    buffer: BatchBuffer,
    in_flight: VecDeque<InFlightBatch>,
    limit: InFlightLimit,
}

impl Clone for WriteMutationsFn {
    fn clone(&self) -> Self {
        Self::new(
            self.options.clone(),
            self.kind,
            Arc::clone(&self.input_schema),
            Arc::clone(&self.err_schema),
            self.client.clone(),
        )
    }
}

impl WriteMutationsFn {
    pub fn new(
        options: WriteOptions,
        kind: MutationInputKind,
        input_schema: Arc<Schema>,
        err_schema: Arc<Schema>,
        client: Option<Arc<BigtableClient>>,
    ) -> Self {
        let buffer = BatchBuffer::new(options.max_elements_per_batch, options.max_bytes_per_batch);
        let limit = InFlightLimit::new(options.max_in_flight_requests, options.max_in_flight_bytes);
        Self {
            options,
            kind,
            input_schema,
            err_schema,
            client,
            buffer,
            in_flight: VecDeque::new(),
            limit,
        }
    }

    fn ensure_client(&mut self) -> beam::Result<Arc<BigtableClient>> {
        if let Some(client) = &self.client {
            return Ok(Arc::clone(client));
        }
        let endpoint = self.options.endpoint.clone();
        let created =
            block_on_async(async move { BigtableClient::connect(endpoint.as_deref()).await })?;
        let arc = Arc::new(created);
        self.client = Some(Arc::clone(&arc));
        Ok(arc)
    }

    fn spawn_batch(&mut self, batch: BatchRequest, rejected: &mut Vec<Row>) -> beam::Result {
        let client = self.ensure_client()?;
        let options = self.options.clone();
        let bytes = batch.total_bytes;
        let entries = batch.entries;

        let task = client_runtime()?
            .spawn(async move { mutate_batch_with_retry(client, options, entries).await });

        self.limit.add(bytes);
        self.in_flight.push_back(InFlightBatch { bytes, task });

        while self.limit.is_full() {
            self.settle_oldest(rejected)?;
        }
        Ok(())
    }

    fn settle_oldest(&mut self, rejected: &mut Vec<Row>) -> beam::Result {
        let Some(InFlightBatch { bytes, task }) = self.in_flight.pop_front() else {
            return Ok(());
        };
        self.limit.remove(bytes);
        let outcome = block_on_async(async move {
            task.await
                .map_err(|e| io::Error::other(format!("Bigtable MutateRows task panicked: {e}")))
        })?;
        match outcome {
            Ok(rejected_entries) => {
                for (entry, status) in rejected_entries {
                    let msg = format!(
                        "Bigtable rejected mutation with code {}: {}",
                        status.code, status.message
                    );
                    let err_row =
                        entry.into_error_row(&self.input_schema, &self.err_schema, msg)?;
                    rejected.push(err_row);
                }
                Ok(())
            }
            Err(fatal_err) => Err(beam::Error::msg(fatal_err)),
        }
    }
}

impl DoFn for WriteMutationsFn {
    type In = RowElement;
    type Out = Row;

    fn process_element(
        &mut self,
        element: RowElement,
        ctx: &mut ProcessContext<'_, Row>,
    ) -> beam::Result {
        let decoded =
            decode_mutation_row(element, self.kind, &self.input_schema, &self.err_schema)?;
        let pending = match decoded {
            Ok(entry) => entry,
            Err(err_row) => {
                return ctx.emit(err_row);
            }
        };

        let mut flushed = self.buffer.push(pending);
        if flushed.is_none() && self.buffer.is_ready() {
            flushed = self.buffer.flush();
        }

        if let Some(batch) = flushed {
            let mut rejected = Vec::new();
            self.spawn_batch(batch, &mut rejected)?;
            for err_row in rejected {
                ctx.emit(err_row)?;
            }
        }
        Ok(())
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Row>) -> beam::Result {
        let mut rejected = Vec::new();
        if let Some(batch) = self.buffer.flush() {
            self.spawn_batch(batch, &mut rejected)?;
        }
        while !self.in_flight.is_empty() {
            self.settle_oldest(&mut rejected)?;
        }
        for err_row in rejected {
            ctx.emit(
                err_row,
                BoundedWindow::Global(GlobalWindow),
                Instant::END_OF_GLOBAL_WINDOW,
            )?;
        }
        Ok(())
    }
}

async fn mutate_batch_with_retry(
    client: Arc<BigtableClient>,
    options: WriteOptions,
    mut remaining: Vec<PendingEntry>,
) -> Result<Vec<(PendingEntry, Status)>, String> {
    let retry_policy = RetryPolicy::default();
    let app_profile = options.app_profile_id.as_deref();
    let routing_header = options.target.routing_header(app_profile);
    let mut cookies = RoutingCookies::default();
    let mut all_rejected: Vec<(PendingEntry, Status)> = Vec::new();
    let mut attempt = 0u32;
    let resource = options.target.resource_name();

    while !remaining.is_empty() {
        // The request takes its entries, and a retry sends the failed entries again, so each
        // attempt copies them. Keys, qualifiers and values are `Bytes`: the copy shares their
        // buffers and allocates only the family names.
        let mut request = MutateRowsRequest {
            entries: remaining.iter().map(|p| p.entry.clone()).collect(),
            ..MutateRowsRequest::default()
        };
        options.target.apply_to_mutate(&mut request, app_profile);

        let stream_res = tokio::time::timeout(
            options.attempt_timeout,
            client.mutate_rows(request, &routing_header, &mut cookies),
        )
        .await;

        let mut stream = match stream_res {
            Ok(Ok(stream)) => stream,
            Ok(Err(status)) if attempt < retry_policy.attempts && is_transient(status.code()) => {
                let cause = status.to_string();
                wait_before_retry(
                    retry_policy,
                    attempt,
                    "MutateRows",
                    &resource,
                    &cause,
                    Some(&status),
                )
                .await;
                attempt += 1;
                continue;
            }
            Ok(Err(status)) => {
                return Err(format!(
                    "Bigtable MutateRows failed on {}: {}",
                    options.target.resource_name(),
                    status
                ));
            }
            Err(_) if attempt < retry_policy.attempts => {
                let cause = "connect timed out";
                wait_before_retry(retry_policy, attempt, "MutateRows", &resource, cause, None)
                    .await;
                attempt += 1;
                continue;
            }
            Err(_) => {
                return Err(format!(
                    "Bigtable MutateRows connect timed out on {}",
                    options.target.resource_name()
                ));
            }
        };

        let mut responses: Vec<MutateRowsResponse> = Vec::new();
        let mut stream_status_err: Option<tonic::Status> = None;

        loop {
            match tokio::time::timeout(options.attempt_timeout, stream.message()).await {
                Ok(Ok(Some(resp))) => responses.push(resp),
                Ok(Ok(None)) => break,
                Ok(Err(status)) => {
                    cookies.extract(status.metadata());
                    stream_status_err = Some(status);
                    break;
                }
                Err(_) => {
                    stream_status_err = Some(tonic::Status::deadline_exceeded(
                        "MutateRows stream timed out",
                    ));
                    break;
                }
            }
        }

        if let Some(status) = &stream_status_err
            && !is_transient(status.code())
            && responses.is_empty()
        {
            return Err(format!(
                "Bigtable MutateRows stream failed on {}: {}",
                options.target.resource_name(),
                status
            ));
        }

        match partition_outcomes(remaining, responses, options.enable_flow_control) {
            BatchOutcome::AllSucceeded => return Ok(all_rejected),
            BatchOutcome::Partial {
                retryable,
                rejected,
                server_backoff,
            } => {
                all_rejected.extend(rejected);
                if retryable.is_empty() {
                    return Ok(all_rejected);
                }
                if attempt >= retry_policy.attempts {
                    let msg = match stream_status_err {
                        Some(s) => s.to_string(),
                        None => "transient per-row MutateRows error".to_string(),
                    };
                    return Err(format!(
                        "Bigtable MutateRows exhausted {} retries for {} entries on {}: {}",
                        retry_policy.attempts,
                        retryable.len(),
                        options.target.resource_name(),
                        msg
                    ));
                }
                // The flow-control delay of `RateLimitInfo` is a floor for the retry wait.
                let floor = server_backoff.unwrap_or_default();
                let status = stream_status_err.as_ref();
                let wait = retry_policy.delay(attempt, status).max(floor);
                tracing::warn!(
                    "Retrying Bigtable MutateRows for {} entries on {resource} (attempt {} of {}) in {} ms",
                    retryable.len(),
                    attempt + 1,
                    retry_policy.attempts,
                    wait.as_millis()
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
                remaining = retryable;
            }
        }
    }

    Ok(all_rejected)
}
