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
use beam::internals::InFlightLimit;
use beam::metrics::{Lineage, Metrics};
use beam::retry::{Decision, RetryWaits};
use beam::schema::{Row, RowElement, Schema};
use beam::transforms::{BundleWindows, DoFn, FinishBundleContext, ProcessContext};
use beam::windowing::{BoundedWindow, GlobalWindow};
use tokio::task::JoinHandle;

use super::batch::{BatchBuffer, BatchOutcome, BatchRequest, partition_outcomes};
use super::decode::{MutationInputKind, PendingEntry, decode_mutation_row};
use super::options::WriteOptions;
use crate::bigtable::METRICS;
use crate::bigtable::client::{BigtableClient, RoutingCookies};
use crate::bigtable::proto::{MutateRowsRequest, MutateRowsResponse, Status};
use crate::bigtable::retry::{DEFAULT_RETRY, decision, wait_before_retry};
use crate::runtime::{block_on_async, client_runtime};

/// The window of every element of the write: it rewindows its input into the global window.
const GLOBAL: BoundedWindow = BoundedWindow::Global(GlobalWindow);

#[derive(Default)]
struct BatchStats {
    mutations_written: u64,
    bytes_written: u64,
    write_retries: u64,
    waits: RetryWaits,
}

impl BatchStats {
    fn record(self) {
        if self.mutations_written > 0 {
            Metrics::counter(METRICS, "mutations_written")
                .inc_by(i64::try_from(self.mutations_written).unwrap_or(i64::MAX));
        }
        if self.bytes_written > 0 {
            Metrics::counter(METRICS, "bytes_written")
                .inc_by(i64::try_from(self.bytes_written).unwrap_or(i64::MAX));
        }
        if self.write_retries > 0 {
            Metrics::counter(METRICS, "write_retries")
                .inc_by(i64::try_from(self.write_retries).unwrap_or(i64::MAX));
        }
        self.waits.record(METRICS);
    }
}

type MutateResult = (Result<Vec<(PendingEntry, Status)>, String>, BatchStats);

struct InFlightBatch {
    bytes: usize,
    task: JoinHandle<MutateResult>,
}

/// `DoFn` that batches incoming [`RowElement`]s per bundle, sends concurrent `MutateRows`
/// RPCs on [`client_runtime`], retries transient partial row failures, and emits permanently
/// rejected rows with [`error_schema`](super::error_schema).
pub struct WriteMutationsFn {
    pub options: Arc<WriteOptions>,
    pub kind: MutationInputKind,
    pub input_schema: Arc<Schema>,
    pub err_schema: Arc<Schema>,
    pub client: Option<Arc<BigtableClient>>,
    buffer: BatchBuffer,
    in_flight: VecDeque<InFlightBatch>,
    limit: InFlightLimit,
    windows: BundleWindows,
    wrote_in_bundle: bool,
}

impl Clone for WriteMutationsFn {
    fn clone(&self) -> Self {
        Self::new(
            Arc::clone(&self.options),
            self.kind,
            Arc::clone(&self.input_schema),
            Arc::clone(&self.err_schema),
            self.client.clone(),
        )
    }
}

impl WriteMutationsFn {
    pub fn new(
        options: Arc<WriteOptions>,
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
            windows: BundleWindows::new(),
            wrote_in_bundle: false,
        }
    }

    fn ensure_client(&mut self) -> beam::Result<Arc<BigtableClient>> {
        if let Some(client) = &self.client {
            return Ok(Arc::clone(client));
        }
        let created = block_on_async(BigtableClient::connect(self.options.endpoint.as_deref()))?;
        let arc = Arc::new(created);
        self.client = Some(Arc::clone(&arc));
        Ok(arc)
    }

    fn spawn_batch(&mut self, batch: BatchRequest, rejected: &mut Vec<Row>) -> beam::Result {
        let client = self.ensure_client()?;
        let options = Arc::clone(&self.options);
        let bytes = batch.total_bytes;
        let entries = batch.entries;

        let task = client_runtime()?
            .spawn(async move { mutate_batch_with_retry(client, &options, entries).await });

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
        let (outcome, stats) = block_on_async(async move {
            task.await
                .map_err(|e| io::Error::other(format!("Bigtable MutateRows task panicked: {e}")))
        })?;
        stats.record();
        match outcome {
            Ok(rejected_entries) => {
                for (entry, status) in rejected_entries {
                    let msg = format!(
                        "Bigtable rejected mutation with code {}: {}",
                        status.code, status.message
                    );
                    let err_row = entry.into_error_row(
                        self.kind,
                        &self.input_schema,
                        &self.err_schema,
                        msg,
                    )?;
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
        self.wrote_in_bundle = true;
        self.windows.add(GLOBAL, ctx.timestamp());
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
        let wrote = std::mem::take(&mut self.wrote_in_bundle);
        let mut rejected = Vec::new();
        if let Some(batch) = self.buffer.flush() {
            self.spawn_batch(batch, &mut rejected)?;
        }
        while !self.in_flight.is_empty() {
            self.settle_oldest(&mut rejected)?;
        }
        if wrote {
            Lineage::sinks().add(METRICS, self.options.target.lineage_segments());
        }
        let timestamp = self
            .windows
            .take(&GLOBAL)
            .unwrap_or(Instant::END_OF_GLOBAL_WINDOW);
        rejected
            .into_iter()
            .try_for_each(|err_row| ctx.emit(err_row, GLOBAL, timestamp))
    }
}

async fn mutate_batch_with_retry(
    client: Arc<BigtableClient>,
    options: &WriteOptions,
    mut remaining: Vec<PendingEntry>,
) -> MutateResult {
    let retry_policy = DEFAULT_RETRY;
    let app_profile = options.app_profile_id.as_deref();
    let routing_header = options.target.routing_header(app_profile);
    let mut cookies = RoutingCookies::default();
    let mut all_rejected: Vec<(PendingEntry, Status)> = Vec::new();
    let mut attempt = 0u32;
    let resource = options.target.resource_name();
    let mut stats = BatchStats::default();

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
            Ok(Err(status))
                if retry_policy.allows_retry(attempt) && decision(&status) != Decision::Stop =>
            {
                let cause = status.to_string();
                wait_before_retry(
                    retry_policy,
                    attempt,
                    decision(&status),
                    &stats.waits,
                    "MutateRows",
                    &resource,
                    &cause,
                )
                .await;
                stats.write_retries += 1;
                attempt += 1;
                continue;
            }
            Ok(Err(status)) => {
                return (
                    Err(format!(
                        "Bigtable MutateRows failed on {}: {}",
                        options.target.resource_name(),
                        status
                    )),
                    stats,
                );
            }
            Err(_) if retry_policy.allows_retry(attempt) => {
                wait_before_retry(
                    retry_policy,
                    attempt,
                    Decision::Backoff,
                    &stats.waits,
                    "MutateRows",
                    &resource,
                    "connect timed out",
                )
                .await;
                stats.write_retries += 1;
                attempt += 1;
                continue;
            }
            Err(_) => {
                return (
                    Err(format!(
                        "Bigtable MutateRows connect timed out on {}",
                        options.target.resource_name()
                    )),
                    stats,
                );
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
            && decision(status) == Decision::Stop
            && responses.is_empty()
        {
            return (
                Err(format!(
                    "Bigtable MutateRows stream failed on {}: {}",
                    options.target.resource_name(),
                    status
                )),
                stats,
            );
        }

        let attempt_count = remaining.len();
        let attempt_bytes: u64 = remaining.iter().map(|e| e.byte_size as u64).sum();
        match partition_outcomes(remaining, responses, options.enable_flow_control) {
            BatchOutcome::AllSucceeded => {
                stats.mutations_written += attempt_count as u64;
                stats.bytes_written += attempt_bytes;
                return (Ok(all_rejected), stats);
            }
            BatchOutcome::Partial {
                retryable,
                rejected,
                server_backoff,
                throttled,
            } => {
                let retry_bytes: u64 = retryable.iter().map(|e| e.byte_size as u64).sum();
                let rej_bytes: u64 = rejected.iter().map(|(e, _)| e.byte_size as u64).sum();
                let succeeded = attempt_count.saturating_sub(retryable.len() + rejected.len());
                stats.mutations_written += succeeded as u64;
                stats.bytes_written += attempt_bytes.saturating_sub(retry_bytes + rej_bytes);
                all_rejected.extend(rejected);
                if retryable.is_empty() {
                    return (Ok(all_rejected), stats);
                }
                if !retry_policy.allows_retry(attempt) {
                    let msg = match stream_status_err {
                        Some(s) => s.to_string(),
                        None => "transient per-row MutateRows error".to_string(),
                    };
                    return (
                        Err(format!(
                            "Bigtable MutateRows exhausted {} retries for {} entries on {}: {}",
                            retry_policy.retries(),
                            retryable.len(),
                            options.target.resource_name(),
                            msg
                        )),
                        stats,
                    );
                }
                // The flow-control delay of `RateLimitInfo` is a floor for the retry wait. It and
                // a `RESOURCE_EXHAUSTED` entry are server push-back.
                let retry = match (stream_status_err.as_ref().map(decision), throttled) {
                    (Some(retry @ (Decision::Throttled | Decision::Wait(_))), _) => retry,
                    (_, true) => Decision::Throttled,
                    (Some(Decision::Stop | Decision::Backoff) | None, false) => Decision::Backoff,
                };
                let retry = match (server_backoff, retry.wait(retry_policy, attempt)) {
                    (Some(floor), Some(wait)) => Decision::Wait(wait.max(floor)),
                    (_, _) => retry,
                };
                let entries = retryable.len();
                let cause = format!("{entries} entries failed");
                wait_before_retry(
                    retry_policy,
                    attempt,
                    retry,
                    &stats.waits,
                    "MutateRows",
                    &resource,
                    &cause,
                )
                .await;
                stats.write_retries += 1;
                attempt += 1;
                remaining = retryable;
            }
        }
    }

    (Ok(all_rejected), stats)
}
