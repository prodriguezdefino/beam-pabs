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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::Duration;

use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::grpc;
use model::fn_execution::{
    ProcessBundleDescriptor, StateAppendRequest, StateClearRequest, StateGetRequest, StateKey,
    StateRequest, StateResponse, beam_fn_state_client::BeamFnStateClient, state_key, state_request,
    state_response,
};

/// How long one state request may wait for the runner's reply before the bundle fails.
const STATE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(600);

type Reply = Result<StateResponse, String>;

/// Shared clients by endpoint and worker id.
type ClientsByEndpoint = HashMap<(String, String), Arc<StateClient>>;

/// One instruction's view of the runner's state service.
#[derive(Clone)]
pub struct StateChannel {
    client: Arc<StateClient>,
    instruction_id: String,
}

impl StateChannel {
    /// A channel for `instruction_id` over the shared client for `endpoint_url`.
    pub fn new(instruction_id: String, endpoint_url: String, worker_id: String) -> Self {
        Self {
            client: StateClient::shared(&endpoint_url, &worker_id),
            instruction_id,
        }
    }

    /// A channel to the state endpoint of `descriptor`, or `None` if it has none.
    pub fn from_descriptor(
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        worker_id: &str,
    ) -> Option<Self> {
        descriptor
            .state_api_service_descriptor
            .as_ref()
            .map(|d| d.url.clone())
            .filter(|u| !u.is_empty())
            .map(|url| Self::new(instruction_id.to_string(), url, worker_id.to_string()))
    }

    /// Fetches all bytes for the given state key and concatenates the pages.
    pub fn get(&self, state_key: StateKey) -> Result<Vec<u8>, String> {
        self.stream_pages(state_key)?
            .try_fold(Vec::new(), |mut all, page| {
                all.extend_from_slice(&page?);
                Ok(all)
            })
    }

    /// Appends encoded element bytes to the given state key.
    pub fn append(&self, state_key: StateKey, data: Vec<u8>) -> Result<(), String> {
        self.call(
            state_key,
            state_request::Request::Append(StateAppendRequest { data }),
        )
        .map(|_| ())
    }

    /// Clears all elements from the given state key.
    pub fn clear(&self, state_key: StateKey) -> Result<(), String> {
        self.call(
            state_key,
            state_request::Request::Clear(StateClearRequest {}),
        )
        .map(|_| ())
    }

    /// Opens an iterator over the pages of `state_key` that fetches each page on demand.
    pub fn stream_pages(&self, state_key: StateKey) -> Result<beam::coders::PageStream, String> {
        Ok(Box::new(StatePageIterator {
            channel: self.clone(),
            state_key,
            continuation_token: Some(Vec::new()),
        }))
    }

    /// One page of `state_key` from `continuation_token` (empty for the first page), and the
    /// token for the next page if there is one.
    fn fetch_page(
        &self,
        state_key: &StateKey,
        continuation_token: Vec<u8>,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), String> {
        let first_page = continuation_token.is_empty();
        let request = state_request::Request::Get(StateGetRequest { continuation_token });
        let (id, response) = self.client.call(StateRequest {
            instruction_id: self.instruction_id.clone(),
            state_key: Some(state_key.clone()),
            request: Some(request),
            ..Default::default()
        })?;
        get_page(&id, first_page, response.response)
    }

    fn call(&self, state_key: StateKey, request: state_request::Request) -> Reply {
        self.client
            .call(StateRequest {
                instruction_id: self.instruction_id.clone(),
                state_key: Some(state_key),
                request: Some(request),
                ..Default::default()
            })
            .map(|(_, response)| response)
    }
}

impl beam::coders::StateStreamReader for StateChannel {
    fn stream_runner_pages(&self, token: &[u8]) -> Result<beam::coders::PageStream, String> {
        tracing::debug!(
            "StateChannel: stream_runner_pages opening channel for runner continuation token (len={})",
            token.len()
        );
        let state_key = StateKey {
            r#type: Some(state_key::Type::Runner(state_key::Runner {
                key: token.to_vec(),
            })),
        };
        self.stream_pages(state_key)
    }
}

/// Reads a state key one page at a time, fetching each page only when it is requested.
struct StatePageIterator {
    channel: StateChannel,
    state_key: StateKey,
    /// Token of the next page; `None` after the last page or an error.
    continuation_token: Option<Vec<u8>>,
}

impl Iterator for StatePageIterator {
    type Item = Result<Vec<u8>, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let token = self.continuation_token.take()?;
        Some(
            self.channel
                .fetch_page(&self.state_key, token)
                .map(|(page, next)| {
                    self.continuation_token = next;
                    page
                }),
        )
    }
}

/// Requests awaiting their responses, by request id.
#[derive(Default)]
struct Waiters {
    pending: HashMap<String, mpsc::SyncSender<Reply>>,
    /// Why the stream ended; all later requests fail with it.
    failed: Option<String>,
}

impl Waiters {
    /// Fails every waiting request and every later one.
    fn fail(&mut self, reason: String) {
        self.pending.drain().for_each(|(_, waiter)| {
            let _ = waiter.try_send(Err(reason.clone()));
        });
        self.failed = Some(reason);
    }
}

/// One bidirectional `State` stream per endpoint, shared and pipelined by all bundles: a
/// reader task routes each response to the request with its id, so bundles neither open
/// connections nor wait for each other's calls.
struct StateClient {
    requests: tokio::sync::mpsc::UnboundedSender<StateRequest>,
    waiters: Arc<Mutex<Waiters>>,
    next_id: AtomicU64,
}

impl StateClient {
    /// The live client for `endpoint` and `worker_id`; connects a new one if there is none or
    /// the last one's stream ended.
    fn shared(endpoint: &str, worker_id: &str) -> Arc<Self> {
        static CLIENTS: OnceLock<Mutex<ClientsByEndpoint>> = OnceLock::new();
        let key = (endpoint.to_string(), worker_id.to_string());
        let Ok(mut clients) = CLIENTS.get_or_init(Default::default).lock() else {
            return Arc::new(Self::connect(endpoint, worker_id));
        };
        clients
            .get(&key)
            .filter(|client| client.is_live())
            .cloned()
            .unwrap_or_else(|| {
                let client = Arc::new(Self::connect(endpoint, worker_id));
                clients.insert(key, client.clone());
                client
            })
    }

    fn is_live(&self) -> bool {
        self.waiters.lock().is_ok_and(|w| w.failed.is_none())
    }

    /// Opens the stream on the state runtime. Requests sent before it is open are queued.
    fn connect(endpoint: &str, worker_id: &str) -> Self {
        let (requests, outbound) = tokio::sync::mpsc::unbounded_channel();
        let waiters = Arc::new(Mutex::new(Waiters::default()));
        let fail = {
            let waiters = waiters.clone();
            move |reason: String| {
                tracing::error!("{reason}");
                if let Ok(mut w) = waiters.lock() {
                    w.fail(reason);
                }
            }
        };
        let (endpoint, worker_id) = (endpoint.to_string(), worker_id.to_string());
        let route = waiters.clone();
        let stream = async move {
            let channel = grpc::channel(&endpoint).await.map_err(|e| {
                format!("Failed to connect to the state endpoint '{endpoint}': {e}")
            })?;
            let mut request = tonic::Request::new(UnboundedReceiverStream::new(outbound));
            if !worker_id.is_empty() {
                grpc::attach_worker_id(&mut request, &worker_id);
            }
            let mut responses = BeamFnStateClient::new(channel)
                .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES)
                .state(request)
                .await
                .map_err(|e| format!("State stream to '{endpoint}' rejected: {e}"))?
                .into_inner();
            let reason = loop {
                match responses.message().await {
                    Ok(Some(response)) => {
                        let waiter = route
                            .lock()
                            .ok()
                            .and_then(|mut w| w.pending.remove(&response.id));
                        match waiter {
                            Some(waiter) => {
                                let _ = waiter.try_send(Ok(response));
                            }
                            None => tracing::warn!(
                                "Dropping state response '{}' that no request is waiting for",
                                response.id
                            ),
                        }
                    }
                    Ok(None) => break format!("State stream to '{endpoint}' closed by runner"),
                    Err(e) => break format!("State stream to '{endpoint}' failed: {e}"),
                }
            };
            Err::<(), String>(reason)
        };
        match state_runtime() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(reason) = stream.await {
                        fail(reason);
                    }
                });
            }
            Err(e) => fail(format!("State runtime unavailable: {e}")),
        }
        Self {
            requests,
            waiters,
            next_id: AtomicU64::new(1),
        }
    }

    /// Sends `request` with a new id and returns the id and the response.
    fn call(&self, request: StateRequest) -> Result<(String, StateResponse), String> {
        let id = format!("state_{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        {
            let mut waiters = self.waiters.lock().map_err(|e| e.to_string())?;
            if let Some(reason) = &waiters.failed {
                return Err(reason.clone());
            }
            waiters.pending.insert(id.clone(), reply_tx);
        }
        self.requests
            .send(StateRequest {
                id: id.clone(),
                ..request
            })
            .map_err(|_| "State gRPC request stream closed".to_string())?;
        let reply = wait_for(&reply_rx).inspect_err(|_| {
            if let Ok(mut w) = self.waiters.lock() {
                w.pending.remove(&id);
            }
        })?;
        match reply {
            res if !res.error.is_empty() => Err(format!("Runner state error: {}", res.error)),
            res => Ok((id, res)),
        }
    }
}

/// The runtime that drives all state streams. Callers block for replies in synchronous
/// element code, possibly on a single-threaded runtime, so the streams run elsewhere.
fn state_runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    static RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("beam-state")
                .enable_all()
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(Clone::clone)
}

/// Waits up to [`STATE_RESPONSE_TIMEOUT`] for a reply. Bundles run on a tokio worker thread,
/// so on the multi-threaded runtime the wait moves that thread's other tasks (the data and
/// control loops) to other threads instead of starving them.
fn wait_for(reply_rx: &mpsc::Receiver<Reply>) -> Reply {
    let wait = || {
        reply_rx
            .recv_timeout(STATE_RESPONSE_TIMEOUT)
            .map_err(|e| match e {
                mpsc::RecvTimeoutError::Timeout => {
                    format!("No state response within {STATE_RESPONSE_TIMEOUT:?}")
                }
                mpsc::RecvTimeoutError::Disconnected => "State stream ended".to_string(),
            })?
    };
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(wait),
        _ => wait(),
    }
}

/// Interprets the body of a reply to a Get request as one page of state.
///
/// Runners such as Dataflow reply to a read of empty state (an empty iterable side input, an
/// unset key) with an unset `response`. That means "empty" only on the first page: on a
/// continuation page it would look like a final page and truncate the iterable, so it is a
/// protocol violation there, like any reply other than a Get.
fn get_page(
    req_id: &str,
    first_page: bool,
    response: Option<state_response::Response>,
) -> Result<(Vec<u8>, Option<Vec<u8>>), String> {
    match response {
        Some(state_response::Response::Get(get)) => {
            let next = (!get.continuation_token.is_empty()).then_some(get.continuation_token);
            Ok((get.data, next))
        }
        None if first_page => Ok((Vec::new(), None)),
        None => Err(format!(
            "Empty response to continuation of state request '{req_id}'; the iterable would be \
             truncated"
        )),
        other => Err(format!(
            "Expected a Get response for state request '{req_id}', got: {other:?}"
        )),
    }
}
