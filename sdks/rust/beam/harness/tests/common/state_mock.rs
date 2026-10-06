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

//! A strict in-process `BeamFnState` server for harness tests.
//!
//! It stores and records every request under its *full* `StateKey`, so a harness bug that
//! builds the wrong key hits the wrong cell and the test sees it. It can also page GETs,
//! fail chosen requests, record instruction ids and `worker_id` metadata, end a stream
//! unanswered, or hold a reply until released.

#![expect(
    clippy::unwrap_used,
    reason = "test fixture: a poisoned lock or malformed token is a test failure"
)]

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use model::fn_execution::beam_fn_state_server::{BeamFnState, BeamFnStateServer};
use model::fn_execution::{
    StateAppendResponse, StateClearResponse, StateGetResponse, StateKey, StateRequest,
    StateResponse, state_key, state_request, state_response,
};

/// Every field of a `StateKey`, in a hashable form.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FullKey {
    Bag {
        transform_id: String,
        state_id: String,
        window: Vec<u8>,
        key: Vec<u8>,
    },
    Multimap {
        transform_id: String,
        state_id: String,
        window: Vec<u8>,
        key: Vec<u8>,
        map_key: Vec<u8>,
    },
    MultimapKeys {
        transform_id: String,
        state_id: String,
        window: Vec<u8>,
        key: Vec<u8>,
    },
    IterableSideInput {
        transform_id: String,
        side_input_id: String,
        window: Vec<u8>,
    },
    MultimapSideInput {
        transform_id: String,
        side_input_id: String,
        window: Vec<u8>,
        key: Vec<u8>,
    },
    Runner {
        key: Vec<u8>,
    },
    Unsupported(String),
}

impl FullKey {
    pub fn bag(transform_id: &str, state_id: &str, window: &[u8], key: &[u8]) -> Self {
        Self::Bag {
            transform_id: transform_id.to_string(),
            state_id: state_id.to_string(),
            window: window.to_vec(),
            key: key.to_vec(),
        }
    }

    pub fn multimap(
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Self {
        Self::Multimap {
            transform_id: transform_id.to_string(),
            state_id: state_id.to_string(),
            window: window.to_vec(),
            key: key.to_vec(),
            map_key: map_key.to_vec(),
        }
    }

    pub fn multimap_keys(transform_id: &str, state_id: &str, window: &[u8], key: &[u8]) -> Self {
        Self::MultimapKeys {
            transform_id: transform_id.to_string(),
            state_id: state_id.to_string(),
            window: window.to_vec(),
            key: key.to_vec(),
        }
    }

    pub fn iterable_side_input(transform_id: &str, side_input_id: &str, window: &[u8]) -> Self {
        Self::IterableSideInput {
            transform_id: transform_id.to_string(),
            side_input_id: side_input_id.to_string(),
            window: window.to_vec(),
        }
    }

    pub fn multimap_side_input(
        transform_id: &str,
        side_input_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Self {
        Self::MultimapSideInput {
            transform_id: transform_id.to_string(),
            side_input_id: side_input_id.to_string(),
            window: window.to_vec(),
            key: key.to_vec(),
        }
    }

    fn from_proto(key: StateKey) -> Self {
        use state_key::Type;
        match key.r#type {
            Some(Type::BagUserState(k)) => Self::Bag {
                transform_id: k.transform_id,
                state_id: k.user_state_id,
                window: k.window,
                key: k.key,
            },
            Some(Type::MultimapUserState(k)) => Self::Multimap {
                transform_id: k.transform_id,
                state_id: k.user_state_id,
                window: k.window,
                key: k.key,
                map_key: k.map_key,
            },
            Some(Type::MultimapKeysUserState(k)) => Self::MultimapKeys {
                transform_id: k.transform_id,
                state_id: k.user_state_id,
                window: k.window,
                key: k.key,
            },
            Some(Type::IterableSideInput(k)) => Self::IterableSideInput {
                transform_id: k.transform_id,
                side_input_id: k.side_input_id,
                window: k.window,
            },
            Some(Type::MultimapSideInput(k)) => Self::MultimapSideInput {
                transform_id: k.transform_id,
                side_input_id: k.side_input_id,
                window: k.window,
                key: k.key,
            },
            Some(Type::Runner(k)) => Self::Runner { key: k.key },
            other => Self::Unsupported(format!("{other:?}")),
        }
    }

    /// For a `Multimap` entry, the `MultimapKeys` key of the map it belongs to.
    fn owning_map(&self) -> Option<(Self, Vec<u8>)> {
        match self {
            Self::Multimap {
                transform_id,
                state_id,
                window,
                key,
                map_key,
            } => Some((
                Self::multimap_keys(transform_id, state_id, window, key),
                map_key.clone(),
            )),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Get,
    Append,
    Clear,
}

/// One request the server received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Call {
    pub op: Op,
    pub key: FullKey,
    /// The GET continuation token (empty for a first page and for non-GETs).
    pub token: Vec<u8>,
    /// The APPEND payload (empty otherwise).
    pub data: Vec<u8>,
}

impl Call {
    pub fn get(key: FullKey) -> Self {
        Self::get_page(key, b"")
    }

    pub fn get_page(key: FullKey, token: &[u8]) -> Self {
        Self {
            op: Op::Get,
            key,
            token: token.to_vec(),
            data: Vec::new(),
        }
    }

    pub fn append(key: FullKey, data: &[u8]) -> Self {
        Self {
            op: Op::Append,
            key,
            token: Vec::new(),
            data: data.to_vec(),
        }
    }

    pub fn clear(key: FullKey) -> Self {
        Self {
            op: Op::Clear,
            key,
            token: Vec::new(),
            data: Vec::new(),
        }
    }
}

struct ErrorRule {
    op: Op,
    token: Option<Vec<u8>>,
    message: String,
}

struct ResponseRule {
    op: Op,
    token: Option<Vec<u8>>,
    response: Option<state_response::Response>,
}

/// What the stream does with the next request for `op` instead of answering it at once.
enum StreamRule {
    /// Serves and records the request, then ends the response stream without replying.
    Close { op: Op },
    /// Signals `held`, then waits for `release` (or its sender's drop) before serving.
    Hold {
        op: Op,
        held: std::sync::mpsc::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    },
}

impl StreamRule {
    fn op(&self) -> Op {
        match self {
            Self::Close { op } | Self::Hold { op, .. } => *op,
        }
    }
}

/// A reply the server is holding back; see [`StrictStateBackend::hold_next`]. Dropping it
/// releases the reply, so a failing test cannot leave the request parked.
pub struct HeldReply {
    held: std::sync::mpsc::Receiver<()>,
    release: Option<tokio::sync::oneshot::Sender<()>>,
}

impl HeldReply {
    /// Blocks until the server holds the reply; panics if no request arrives within `timeout`.
    pub fn wait_held(&self, timeout: Duration) {
        self.held
            .recv_timeout(timeout)
            .expect("the held request should reach the state server");
    }

    /// Lets the server answer the held request. Idempotent.
    pub fn release(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for HeldReply {
    fn drop(&mut self) {
        self.release();
    }
}

/// Storage, request log and failure plan shared with the server.
#[derive(Default)]
pub struct StrictStateBackend {
    storage: Mutex<HashMap<FullKey, Vec<u8>>>,
    calls: Mutex<Vec<Call>>,
    /// Bytes per GET page; 0 returns everything in one page.
    page_size: AtomicUsize,
    errors: Mutex<Vec<ErrorRule>>,
    responses: Mutex<Vec<ResponseRule>>,
    stream_rules: Mutex<Vec<StreamRule>>,
    /// The `instruction_id` of every request recorded in `calls`, in the same order.
    instruction_ids: Mutex<Vec<String>>,
    /// The `worker_id` metadata of every state stream opened, in order.
    stream_worker_ids: Mutex<Vec<Option<String>>>,
}

impl StrictStateBackend {
    /// Stores `data` under `key`, as if a previous bundle had committed it.
    pub fn preload(&self, key: FullKey, data: &[u8]) {
        self.storage.lock().unwrap().insert(key, data.to_vec());
    }

    /// The bytes currently stored under exactly `key`.
    pub fn stored(&self, key: &FullKey) -> Option<Vec<u8>> {
        self.storage.lock().unwrap().get(key).cloned()
    }

    /// Splits GET responses into pages of at most `bytes` bytes.
    pub fn set_page_size(&self, bytes: usize) {
        self.page_size.store(bytes, Ordering::SeqCst);
    }

    /// Fails the next `op` request (only the `token` page, if given) with `message` as error.
    pub fn fail_next(&self, op: Op, token: Option<&[u8]>, message: &str) {
        self.errors.lock().unwrap().push(ErrorRule {
            op,
            token: token.map(<[u8]>::to_vec),
            message: message.to_string(),
        });
    }

    /// Answers the next `op` request (only the `token` page, if given) with `response`
    /// instead of serving it. `None` sends neither a result nor an error.
    pub fn respond_next(
        &self,
        op: Op,
        token: Option<&[u8]>,
        response: Option<state_response::Response>,
    ) {
        self.responses.lock().unwrap().push(ResponseRule {
            op,
            token: token.map(<[u8]>::to_vec),
            response,
        });
    }

    /// Every request received so far, in order.
    pub fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    pub fn take_calls(&self) -> Vec<Call> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }

    /// The `instruction_id` of every request received so far, in order. Unlike
    /// [`Self::take_calls`], nothing clears it.
    pub fn instruction_ids(&self) -> Vec<String> {
        self.instruction_ids.lock().unwrap().clone()
    }

    /// The `worker_id` metadata of every state stream opened so far, in order.
    pub fn stream_worker_ids(&self) -> Vec<Option<String>> {
        self.stream_worker_ids.lock().unwrap().clone()
    }

    /// How many state streams have been opened so far.
    pub fn streams_opened(&self) -> usize {
        self.stream_worker_ids.lock().unwrap().len()
    }

    /// Serves and records the next `op` request, then ends its stream without replying.
    pub fn close_stream_on_next(&self, op: Op) {
        self.stream_rules
            .lock()
            .unwrap()
            .push(StreamRule::Close { op });
    }

    /// Holds the reply to the next `op` request until the returned handle is released
    /// or dropped. Later requests on the same stream queue behind it.
    pub fn hold_next(&self, op: Op) -> HeldReply {
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        self.stream_rules.lock().unwrap().push(StreamRule::Hold {
            op,
            held: held_tx,
            release: release_rx,
        });
        HeldReply {
            held: held_rx,
            release: Some(release_tx),
        }
    }

    fn take_stream_rule(&self, req: &StateRequest) -> Option<StreamRule> {
        let op = match req.request {
            Some(state_request::Request::Get(_)) => Op::Get,
            Some(state_request::Request::Append(_)) => Op::Append,
            Some(state_request::Request::Clear(_)) => Op::Clear,
            _ => return None,
        };
        let mut rules = self.stream_rules.lock().unwrap();
        let pos = rules.iter().position(|r| r.op() == op)?;
        Some(rules.remove(pos))
    }

    fn planned_error(&self, op: Op, token: &[u8]) -> Option<String> {
        let mut errors = self.errors.lock().unwrap();
        let pos = errors
            .iter()
            .position(|r| r.op == op && r.token.as_deref().is_none_or(|t| t == token))?;
        Some(errors.remove(pos).message)
    }

    fn planned_response(&self, op: Op, token: &[u8]) -> Option<Option<state_response::Response>> {
        let mut responses = self.responses.lock().unwrap();
        let pos = responses
            .iter()
            .position(|r| r.op == op && r.token.as_deref().is_none_or(|t| t == token))?;
        Some(responses.remove(pos).response)
    }

    /// The full value of `key`. A map's key set is the concatenation of its map keys,
    /// which are already encoded with the key coder.
    fn value_of(&self, key: &FullKey) -> Vec<u8> {
        let storage = self.storage.lock().unwrap();
        if matches!(key, FullKey::MultimapKeys { .. }) {
            let map_keys: BTreeSet<Vec<u8>> = storage
                .keys()
                .filter_map(FullKey::owning_map)
                .filter(|(owner, _)| owner == key)
                .map(|(_, map_key)| map_key)
                .collect();
            map_keys.into_iter().flatten().collect()
        } else {
            storage.get(key).cloned().unwrap_or_default()
        }
    }

    fn handle(&self, req: StateRequest) -> StateResponse {
        let key = FullKey::from_proto(req.state_key.unwrap_or_default());
        let (op, token, data) = match req.request {
            Some(state_request::Request::Get(get)) => (Op::Get, get.continuation_token, vec![]),
            Some(state_request::Request::Append(append)) => (Op::Append, vec![], append.data),
            Some(state_request::Request::Clear(_)) => (Op::Clear, vec![], vec![]),
            other => {
                return StateResponse {
                    id: req.id,
                    error: format!("unsupported request {other:?}"),
                    response: None,
                    ..Default::default()
                };
            }
        };
        self.instruction_ids
            .lock()
            .unwrap()
            .push(req.instruction_id.clone());
        self.calls.lock().unwrap().push(Call {
            op,
            key: key.clone(),
            token: token.clone(),
            data: data.clone(),
        });

        if let Some(message) = self.planned_error(op, &token) {
            return StateResponse {
                id: req.id,
                error: message,
                response: None,
                ..Default::default()
            };
        }
        if let Some(response) = self.planned_response(op, &token) {
            return StateResponse {
                id: req.id,
                error: String::new(),
                response,
                ..Default::default()
            };
        }

        let response = match op {
            Op::Get => {
                let value = self.value_of(&key);
                let start: usize = if token.is_empty() {
                    0
                } else {
                    String::from_utf8(token).unwrap().parse().unwrap()
                };
                let page_size = self.page_size.load(Ordering::SeqCst);
                let end = if page_size == 0 {
                    value.len()
                } else {
                    (start + page_size).min(value.len())
                };
                let continuation_token = if end < value.len() {
                    end.to_string().into_bytes()
                } else {
                    Vec::new()
                };
                state_response::Response::Get(StateGetResponse {
                    data: value[start..end].to_vec(),
                    continuation_token,
                })
            }
            Op::Append => {
                self.storage
                    .lock()
                    .unwrap()
                    .entry(key)
                    .or_default()
                    .extend_from_slice(&data);
                state_response::Response::Append(StateAppendResponse {})
            }
            Op::Clear => {
                let mut storage = self.storage.lock().unwrap();
                if matches!(key, FullKey::MultimapKeys { .. }) {
                    storage.retain(|k, _| k.owning_map().is_none_or(|(owner, _)| owner != key));
                } else {
                    storage.remove(&key);
                }
                state_response::Response::Clear(StateClearResponse {})
            }
        };
        StateResponse {
            id: req.id,
            error: String::new(),
            response: Some(response),
            ..Default::default()
        }
    }
}

struct StrictStateService {
    backend: Arc<StrictStateBackend>,
}

#[tonic::async_trait]
impl BeamFnState for StrictStateService {
    type StateStream = Pin<Box<dyn Stream<Item = Result<StateResponse, Status>> + Send + 'static>>;

    async fn state(
        &self,
        request: Request<Streaming<StateRequest>>,
    ) -> Result<Response<Self::StateStream>, Status> {
        let worker_id = request
            .metadata()
            .get("worker_id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        self.backend
            .stream_worker_ids
            .lock()
            .unwrap()
            .push(worker_id);
        let mut inbound = request.into_inner();
        let (out_tx, out_rx) = mpsc::channel(100);
        let backend = Arc::clone(&self.backend);
        tokio::spawn(async move {
            loop {
                let req = match inbound.message().await {
                    Ok(Some(req)) => req,
                    Ok(None) => break,
                    Err(status) => {
                        eprintln!("strict state server: inbound stream failed: {status}");
                        break;
                    }
                };
                match backend.take_stream_rule(&req) {
                    Some(StreamRule::Close { .. }) => {
                        backend.handle(req);
                        // Dropping `out_tx` ends the response stream unanswered.
                        break;
                    }
                    Some(StreamRule::Hold { held, release, .. }) => {
                        let _ = held.send(());
                        let _ = release.await;
                    }
                    None => {}
                }
                let _ = out_tx.send(Ok(backend.handle(req))).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(out_rx))))
    }
}

/// Handle to the background server. Dropping it stops the server and joins its thread,
/// so the listening socket does not outlive the test.
pub struct StrictStateServer {
    pub backend: Arc<StrictStateBackend>,
    endpoint: String,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl StrictStateServer {
    pub fn start() -> Self {
        let backend = Arc::new(StrictStateBackend::default());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let served = Arc::clone(&backend);

        let thread = std::thread::Builder::new()
            .name("strict-state-server".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("mock state server runtime");
                rt.block_on(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
                    let addr = listener.local_addr().expect("local address");
                    ready_tx
                        .send(format!("http://{addr}"))
                        .expect("caller waiting");
                    tokio::spawn(async move {
                        let _ = tonic::transport::Server::builder()
                            .add_service(BeamFnStateServer::new(StrictStateService {
                                backend: served,
                            }))
                            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                                listener,
                            ))
                            .await;
                    });
                    let _ = shutdown_rx.await;
                });
                // Dropping the runtime closes the listener without draining client connections.
                drop(rt);
            })
            .expect("spawn mock state server thread");

        let endpoint = ready_rx.recv().expect("mock state server endpoint");
        Self {
            backend,
            endpoint,
            shutdown_tx: Some(shutdown_tx),
            thread: Some(thread),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for StrictStateServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
