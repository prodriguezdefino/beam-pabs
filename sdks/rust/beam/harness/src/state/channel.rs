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
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

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

/// The state streams of one worker, one per endpoint. Bundles share them. A stream that
/// ended is replaced at the next request for a channel to its endpoint.
pub struct StateClients {
    worker_id: String,
    clients: Mutex<HashMap<String, Arc<StateClient>>>,
    /// Starts at the first stream, so a worker without state has no state thread.
    runtime: OnceLock<Result<Arc<StateRuntime>, String>>,
}

impl StateClients {
    /// Clients that send `worker_id` to the runner, so it can match each stream with the
    /// worker of the control stream.
    pub fn new(worker_id: String) -> Self {
        Self {
            worker_id,
            clients: Mutex::default(),
            runtime: OnceLock::new(),
        }
    }

    /// A channel for `instruction_id` to the state endpoint of `descriptor`, or `None` if
    /// the descriptor has none.
    pub fn channel(
        &self,
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
    ) -> Option<StateChannel> {
        descriptor
            .state_api_service_descriptor
            .as_ref()
            .map(|d| d.url.as_str())
            .filter(|url| !url.is_empty())
            .map(|url| self.channel_to(instruction_id, url))
    }

    /// A channel for `instruction_id` to the state service at `endpoint`.
    pub fn channel_to(&self, instruction_id: &str, endpoint: &str) -> StateChannel {
        StateChannel {
            client: self.client(endpoint),
            instruction_id: instruction_id.to_string(),
        }
    }

    /// The live client for `endpoint`. Connects a new one if there is none or the stream of
    /// the last one ended.
    fn client(&self, endpoint: &str) -> Arc<StateClient> {
        let mut clients = self.clients.lock().unwrap_or_else(PoisonError::into_inner);
        match clients.get(endpoint).filter(|client| client.is_live()) {
            Some(client) => Arc::clone(client),
            None => {
                let client = Arc::new(StateClient::connect(
                    endpoint,
                    &self.worker_id,
                    self.runtime(),
                ));
                clients.insert(endpoint.to_string(), Arc::clone(&client));
                client
            }
        }
    }

    fn runtime(&self) -> Result<Arc<StateRuntime>, String> {
        self.runtime.get_or_init(StateRuntime::start).clone()
    }
}

/// The runtime that drives the state streams. Element code waits for replies on its own
/// thread, so the streams run on another thread.
struct StateRuntime(Option<tokio::runtime::Runtime>);

impl StateRuntime {
    fn start() -> Result<Arc<Self>, String> {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("beam-state")
            .enable_all()
            .build()
            .map(|runtime| Arc::new(Self(Some(runtime))))
            .map_err(|e| e.to_string())
    }

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        if let Some(runtime) = &self.0 {
            runtime.spawn(task);
        }
    }
}

impl Drop for StateRuntime {
    /// Stops the runtime without waiting, so the last owner can drop it in async code.
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// One instruction's view of the runner's state service.
#[derive(Clone)]
pub struct StateChannel {
    client: Arc<StateClient>,
    instruction_id: String,
}

impl StateChannel {
    /// Fetches all bytes for the given state key and concatenates the pages.
    pub fn get(&self, state_key: StateKey) -> Result<Vec<u8>, String> {
        self.stream_pages(state_key)
            .map_err(|e| e.to_string())?
            .try_fold(Vec::new(), |mut all, page| {
                all.extend_from_slice(&page.map_err(|e| e.to_string())?);
                Ok(all)
            })
    }

    /// Sends an append of `data` to `state_key` and returns without waiting for the reply.
    /// The runner applies the requests of one stream in order.
    pub fn start_append(
        &self,
        state_key: StateKey,
        data: Vec<u8>,
    ) -> Result<PendingReply<'_>, String> {
        self.start(
            state_key,
            state_request::Request::Append(StateAppendRequest { data }),
        )
    }

    /// Sends a clear of `state_key` and returns without waiting for the reply.
    pub fn start_clear(&self, state_key: StateKey) -> Result<PendingReply<'_>, String> {
        self.start(
            state_key,
            state_request::Request::Clear(StateClearRequest {}),
        )
    }

    fn start(
        &self,
        state_key: StateKey,
        request: state_request::Request,
    ) -> Result<PendingReply<'_>, String> {
        self.client.send(StateRequest {
            instruction_id: self.instruction_id.clone(),
            state_key: Some(state_key),
            request: Some(request),
            ..Default::default()
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
    pub fn stream_pages(
        &self,
        state_key: StateKey,
    ) -> Result<beam::coders::PageStream, beam::coders::CoderError> {
        Ok(Box::new(StatePageIterator {
            channel: self.clone(),
            state_key,
            continuation_token: Some(Vec::new()),
        }))
    }

    /// One page of `state_key` from `continuation_token` (empty for the first page), and the
    /// token for the next page if there is one.
    pub(crate) fn fetch_page(
        &self,
        state_key: &StateKey,
        continuation_token: Vec<u8>,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), String> {
        let first_page = continuation_token.is_empty();
        let request = state_request::Request::Get(StateGetRequest { continuation_token });
        let response = self.client.call(StateRequest {
            instruction_id: self.instruction_id.clone(),
            state_key: Some(state_key.clone()),
            request: Some(request),
            ..Default::default()
        })?;
        get_page(&response.id, first_page, response.response)
    }

    fn call(&self, state_key: StateKey, request: state_request::Request) -> Reply {
        self.client.call(StateRequest {
            instruction_id: self.instruction_id.clone(),
            state_key: Some(state_key),
            request: Some(request),
            ..Default::default()
        })
    }
}

impl beam::coders::StateStreamReader for StateChannel {
    fn stream_runner_pages(
        &self,
        token: &[u8],
    ) -> Result<beam::coders::PageStream, beam::coders::CoderError> {
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
    type Item = Result<Vec<u8>, beam::coders::CoderError>;

    fn next(&mut self) -> Option<Self::Item> {
        let token = self.continuation_token.take()?;
        Some(
            self.channel
                .fetch_page(&self.state_key, token)
                .map(|(page, next)| {
                    self.continuation_token = next;
                    page
                })
                .map_err(beam::coders::CoderError::StateStream),
        )
    }
}

/// Prefix of the request ids; the number after it is the key of [`Waiters::pending`].
const REQUEST_ID_PREFIX: &str = "state_";

/// The reply to one request. The reader task fills it and wakes the thread that sent the
/// request.
struct ReplySlot {
    reply: Mutex<Option<Reply>>,
    ready: AtomicBool,
    waiter: Thread,
}

impl ReplySlot {
    fn fill(&self, reply: Reply) {
        if let Ok(mut slot) = self.reply.lock() {
            *slot = Some(reply);
        }
        self.ready.store(true, Ordering::Release);
        self.waiter.unpark();
    }

    fn take(&self) -> Option<Reply> {
        self.ready
            .load(Ordering::Acquire)
            .then(|| self.reply.lock().ok()?.take())
            .flatten()
    }
}

/// Requests awaiting their responses, by request number.
#[derive(Default)]
struct Waiters {
    pending: HashMap<u64, Arc<ReplySlot>>,
    /// Why the stream ended; all later requests fail with it.
    failed: Option<String>,
}

impl Waiters {
    /// Fails every waiting request and every later one.
    fn fail(&mut self, reason: String) {
        self.pending
            .drain()
            .for_each(|(_, slot)| slot.fill(Err(reason.clone())));
        self.failed = Some(reason);
    }
}

/// One bidirectional `State` stream runs per endpoint, shared across all bundles.
/// A reader task routes each response to the request with its id.
/// Bundles neither open connections nor block on each other's calls.
struct StateClient {
    requests: tokio::sync::mpsc::UnboundedSender<StateRequest>,
    waiters: Arc<Mutex<Waiters>>,
    next_id: AtomicU64,
    /// Keeps the runtime of the stream while a channel uses the client.
    _runtime: Option<Arc<StateRuntime>>,
}

impl StateClient {
    fn is_live(&self) -> bool {
        self.waiters.lock().is_ok_and(|w| w.failed.is_none())
    }

    /// Opens the stream on `runtime`. Requests sent before it is open are queued.
    fn connect(
        endpoint: &str,
        worker_id: &str,
        runtime: Result<Arc<StateRuntime>, String>,
    ) -> Self {
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
                        let slot = response
                            .id
                            .strip_prefix(REQUEST_ID_PREFIX)
                            .and_then(|number| number.parse().ok())
                            .and_then(|number| route.lock().ok()?.pending.remove(&number));
                        match slot {
                            Some(slot) => slot.fill(Ok(response)),
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
        let runtime = match runtime {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(reason) = stream.await {
                        fail(reason);
                    }
                });
                Some(runtime)
            }
            Err(e) => {
                fail(format!("State runtime unavailable: {e}"));
                None
            }
        };
        Self {
            requests,
            waiters,
            next_id: AtomicU64::new(1),
            _runtime: runtime,
        }
    }

    /// Sends `request` with a new id and returns the response.
    fn call(&self, request: StateRequest) -> Reply {
        self.send(request)?.wait()
    }

    /// Sends `request` with a new id. The reply arrives in the returned [`PendingReply`].
    fn send(&self, request: StateRequest) -> Result<PendingReply<'_>, String> {
        let number = self.next_id.fetch_add(1, Ordering::Relaxed);
        let slot = Arc::new(ReplySlot {
            reply: Mutex::new(None),
            ready: AtomicBool::new(false),
            waiter: thread::current(),
        });
        {
            let mut waiters = self.waiters.lock().map_err(|e| e.to_string())?;
            if let Some(reason) = &waiters.failed {
                return Err(reason.clone());
            }
            waiters.pending.insert(number, Arc::clone(&slot));
        }
        self.requests
            .send(StateRequest {
                id: format!("{REQUEST_ID_PREFIX}{number}"),
                ..request
            })
            .map_err(|_| "State gRPC request stream closed".to_string())?;
        Ok(PendingReply {
            client: self,
            number,
            slot,
            same_thread: PhantomData,
        })
    }
}

/// A sent state request whose reply has not been read. Only the thread that sent the request
/// is woken by the reply, so the type is not `Send`.
#[must_use = "a state request fails silently unless its reply is read"]
pub struct PendingReply<'a> {
    client: &'a StateClient,
    number: u64,
    slot: Arc<ReplySlot>,
    same_thread: PhantomData<*const ()>,
}

impl PendingReply<'_> {
    /// Waits for the reply. A reply with an error is an `Err`.
    fn wait(self) -> Reply {
        let reply = wait_for(&self.slot).inspect_err(|_| {
            if let Ok(mut w) = self.client.waiters.lock() {
                w.pending.remove(&self.number);
            }
        })?;
        match reply {
            res if !res.error.is_empty() => Err(format!("Runner state error: {}", res.error)),
            res => Ok(res),
        }
    }

    /// Waits for the reply of a mutation.
    pub fn finish(self) -> Result<(), String> {
        self.wait().map(|_| ())
    }
}

/// Waits up to [`STATE_RESPONSE_TIMEOUT`] for the reply in `slot`. The thread does not spin:
/// a spin costs a sequential caller more CPU than the wakeup that it saves. The wait blocks
/// the thread; the streams run on their own runtime, so they continue.
fn wait_for(slot: &ReplySlot) -> Reply {
    let deadline = Instant::now() + STATE_RESPONSE_TIMEOUT;
    loop {
        if let Some(reply) = slot.take() {
            return reply;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(format!(
                "No state response within {STATE_RESPONSE_TIMEOUT:?}"
            ));
        }
        thread::park_timeout(left);
    }
}

/// Interprets the body of a reply to a Get request as one page of state.
///
/// Runners such as Dataflow reply to a read of empty state (an empty iterable side
/// input, an unset key) with an unset `response`. That means "empty" only on the first
/// page. On a continuation page it looks like a final page and truncates the iterable.
/// Thus an unset response on continuation pages violates protocol, as does any
/// non-Get reply.
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
