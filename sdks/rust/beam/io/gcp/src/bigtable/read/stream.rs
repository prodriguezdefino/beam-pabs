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

//! Background `ReadRows` stream task of one key range, with retries from the last committed
//! key.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Notify, mpsc};

use super::assembler::{RowAssembler, ScanEvent};
use super::options::ReadOptions;
use crate::bigtable::client::{BigtableClient, RetryPolicy, RoutingCookies, is_transient};
use crate::bigtable::key::{is_valid_range, next_key};
use crate::bigtable::proto::{ReadRowsRequest, RowFilter, RowRange, RowRangeExt as _, RowSet};
use crate::bigtable::retry::wait_before_retry;
use crate::bigtable::target::ReadTarget;
use crate::runtime::client_runtime;

const PREFETCH_BATCHES: usize = 4;

/// Aborts the stream task when the reader drops it.
pub(super) struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One message from the stream task to the reader.
pub(super) enum StreamBatch {
    Events(Vec<ScanEvent>),
    SplitNotified,
    /// A permanent error. `last_key` is the last committed key, or empty before the first.
    Error {
        last_key: Bytes,
        message: String,
    },
}

fn build_read_request(
    target: &ReadTarget,
    app_profile: Option<&str>,
    start_key: &[u8],
    end_key: &[u8],
    filter: Option<RowFilter>,
) -> (ReadRowsRequest, String) {
    let mut req = ReadRowsRequest {
        rows: Some(RowSet {
            row_keys: Vec::new(),
            row_ranges: vec![RowRange::half_open(
                Bytes::copy_from_slice(start_key),
                Bytes::copy_from_slice(end_key),
            )],
        }),
        filter,
        ..ReadRowsRequest::default()
    };
    target.apply_to_read(&mut req, app_profile);
    (req, target.routing_header(app_profile))
}

/// Starts the `ReadRows` stream of `[initial_start, initial_end)` on the client runtime.
pub(super) fn spawn_range_reader(
    existing_client: Option<Arc<BigtableClient>>,
    options: ReadOptions,
    initial_start: Vec<u8>,
    initial_end: Vec<u8>,
    split_notify: Arc<Notify>,
) -> io::Result<(mpsc::Receiver<StreamBatch>, AbortOnDrop)> {
    let filter = options.decoded_filter().map_err(io::Error::other)?;
    let (tx, rx) = mpsc::channel(PREFETCH_BATCHES);
    let task = client_runtime()?.spawn(async move {
        let client = match existing_client {
            Some(c) => c,
            None => match BigtableClient::connect(options.endpoint.as_deref()).await {
                Ok(c) => Arc::new(c),
                Err(err) => {
                    let _ = tx
                        .send(StreamBatch::Error {
                            last_key: Bytes::new(),
                            message: err.to_string(),
                        })
                        .await;
                    return;
                }
            },
        };
        let retry_policy = RetryPolicy::default();
        let app_profile = options.app_profile_id.as_deref();
        let mut cookies = RoutingCookies::default();
        let mut last_committed_key: Option<Bytes> = None;
        let mut attempt = 0u32;
        let resource = options.target.resource_name();

        loop {
            let resume_start = match &last_committed_key {
                Some(last) => next_key(last),
                None => initial_start.clone(),
            };
            if !is_valid_range(&resume_start, &initial_end) {
                return;
            }

            let (req, routing_header) = build_read_request(
                &options.target,
                app_profile,
                &resume_start,
                &initial_end,
                filter.clone(),
            );
            let connect_res = tokio::time::timeout(
                options.attempt_timeout,
                client.read_rows(req, &routing_header, &mut cookies),
            )
            .await;

            let mut stream = match connect_res {
                Ok(Ok(stream)) => stream,
                Ok(Err(status))
                    if attempt < retry_policy.attempts && is_transient(status.code()) =>
                {
                    let cause = status.to_string();
                    wait_before_retry(retry_policy, attempt, "ReadRows", &resource, &cause, Some(&status))
                        .await;
                    attempt += 1;
                    continue;
                }
                Ok(Err(status)) => {
                    let _ = tx
                        .send(StreamBatch::Error {
                            last_key: last_committed_key.unwrap_or_default(),
                            message: status.to_string(),
                        })
                        .await;
                    return;
                }
                Err(_) if attempt < retry_policy.attempts => {
                    let cause = "connect timed out";
                    wait_before_retry(retry_policy, attempt, "ReadRows", &resource, cause, None).await;
                    attempt += 1;
                    continue;
                }
                Err(_) => {
                    let _ = tx
                        .send(StreamBatch::Error {
                            last_key: last_committed_key.unwrap_or_default(),
                            message: "ReadRows connect timed out".to_string(),
                        })
                        .await;
                    return;
                }
            };

            let mut assembler = RowAssembler::new();
            let stream_err: Option<(bool, String, Option<tonic::Status>)> = loop {
                tokio::select! {
                    biased;
                    _ = split_notify.notified() => {
                        if tx.send(StreamBatch::SplitNotified).await.is_err() {
                            return;
                        }
                    }
                    msg = tokio::time::timeout(options.attempt_timeout, stream.message()) => {
                        match msg {
                            Ok(Ok(Some(response))) => {
                                attempt = 0;
                                match assembler.feed(response) {
                                    Ok(events) => {
                                        if let Some(last) = events.last() {
                                            let key = match last {
                                                ScanEvent::Row(r) => r.key.clone(),
                                                ScanEvent::Scanned(k) => k.clone(),
                                            };
                                            last_committed_key = Some(key);
                                        }
                                        if !events.is_empty()
                                            && tx.send(StreamBatch::Events(events)).await.is_err()
                                        {
                                            return;
                                        }
                                    }
                                    Err(err) => break Some((false, err, None)),
                                }
                            }
                            Ok(Ok(None)) => {
                                if let Err(err) = assembler.finish_stream() {
                                    break Some((false, err, None));
                                }
                                return;
                            }
                            Ok(Err(status)) => {
                                cookies.extract(status.metadata());
                                let message = status.to_string();
                                break Some((is_transient(status.code()), message, Some(status)));
                            }
                            Err(_) => {
                                break Some((true, "ReadRows stream message timed out".to_string(), None));
                            }
                        }
                    }
                }
            };

            if let Some((retryable, message, status)) = stream_err {
                if retryable && attempt < retry_policy.attempts {
                    let status = status.as_ref();
                    wait_before_retry(retry_policy, attempt, "ReadRows", &resource, &message, status)
                        .await;
                    attempt += 1;
                    continue;
                }
                let _ = tx
                    .send(StreamBatch::Error {
                        last_key: last_committed_key.unwrap_or_default(),
                        message,
                    })
                    .await;
                return;
            }
        }
    });
    Ok((rx, AbortOnDrop(task)))
}
