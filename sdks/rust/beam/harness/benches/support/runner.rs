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

//! An in-process runner for the benchmarks: control, data, state and logging services over
//! gRPC on one local port, and a [`Worker`] connected to them. It plays the part of the
//! runner side of Java's `ProcessBundleBenchmark` (`SdkHarnessClient` and the Grpc*Service
//! servers): it sends bundles, feeds their input and waits for their output and response.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

use beam::internals::TransformFn;
use beam::options::HarnessOptions;
use harness::worker::Worker;
use model::fn_execution::beam_fn_control_server::{BeamFnControl, BeamFnControlServer};
use model::fn_execution::beam_fn_data_server::{BeamFnData, BeamFnDataServer};
use model::fn_execution::beam_fn_logging_server::{BeamFnLogging, BeamFnLoggingServer};
use model::fn_execution::beam_fn_state_server::{BeamFnState, BeamFnStateServer};
use model::fn_execution::process_bundle_request::{CacheToken, cache_token};
use model::fn_execution::{
    Elements, GetProcessBundleDescriptorRequest, InstructionRequest, InstructionResponse,
    LogControl, ProcessBundleDescriptor, ProcessBundleRequest, RegisterRequest,
    StateAppendResponse, StateClearResponse, StateGetResponse, StateRequest, StateResponse,
    elements, instruction_request, log_entry, state_key, state_request, state_response,
};
use model::pipeline::ApiServiceDescriptor;

type Replies<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The bytes the worker sent for one bundle, complete once its sink reports `is_last`.
type OutputWaiters = Arc<Mutex<HashMap<String, (Vec<u8>, Option<oneshot::Sender<Vec<u8>>>)>>>;

/// Runner-side state shared by the gRPC services.
#[derive(Clone)]
struct Services {
    /// Requests for the worker. Taken by the first control stream.
    requests: Arc<Mutex<Option<mpsc::Receiver<InstructionRequest>>>>,
    responses: Arc<Mutex<HashMap<String, oneshot::Sender<InstructionResponse>>>>,
    /// The sender of the worker's data stream, once it connects.
    data: watch::Sender<Option<mpsc::Sender<Result<Elements, Status>>>>,
    outputs: OutputWaiters,
    /// Bag user state by user key, as Java's `InMemoryBagUserStateHandler`.
    bags: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

#[tonic::async_trait]
impl BeamFnControl for Services {
    type ControlStream = Replies<InstructionRequest>;

    async fn control(
        &self,
        request: Request<Streaming<InstructionResponse>>,
    ) -> Result<Response<Self::ControlStream>, Status> {
        let requests = self
            .requests
            .lock()
            .expect("requests lock")
            .take()
            .ok_or_else(|| Status::already_exists("one control stream only"))?;
        let mut responses = request.into_inner();
        let waiting = Arc::clone(&self.responses);
        tokio::spawn(async move {
            while let Ok(Some(response)) = responses.message().await {
                let waiter = waiting
                    .lock()
                    .expect("responses lock")
                    .remove(&response.instruction_id);
                if let Some(waiter) = waiter {
                    // The benchmark may have stopped waiting.
                    let _ = waiter.send(response);
                }
            }
        });
        let stream = tokio_stream::StreamExt::map(ReceiverStream::new(requests), Ok);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_process_bundle_descriptor(
        &self,
        _request: Request<GetProcessBundleDescriptorRequest>,
    ) -> Result<Response<ProcessBundleDescriptor>, Status> {
        Err(Status::unimplemented("descriptors are registered up front"))
    }
}

#[tonic::async_trait]
impl BeamFnData for Services {
    type DataStream = Replies<Elements>;

    async fn data(
        &self,
        request: Request<Streaming<Elements>>,
    ) -> Result<Response<Self::DataStream>, Status> {
        let mut inbound = request.into_inner();
        let outputs = Arc::clone(&self.outputs);
        tokio::spawn(async move {
            while let Ok(Some(elements)) = inbound.message().await {
                let mut outputs = outputs.lock().expect("outputs lock");
                for data in elements.data {
                    let Some((bytes, waiter)) = outputs.get_mut(&data.instruction_id) else {
                        continue;
                    };
                    bytes.extend_from_slice(&data.data);
                    if data.is_last
                        && let Some(waiter) = waiter.take()
                    {
                        let _ = waiter.send(std::mem::take(bytes));
                    }
                }
            }
        });
        let (tx, rx) = mpsc::channel(64);
        self.data.send_replace(Some(tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[tonic::async_trait]
impl BeamFnState for Services {
    type StateStream = Replies<StateResponse>;

    async fn state(
        &self,
        request: Request<Streaming<StateRequest>>,
    ) -> Result<Response<Self::StateStream>, Status> {
        let mut requests = request.into_inner();
        let bags = Arc::clone(&self.bags);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            while let Ok(Some(request)) = requests.message().await {
                let response = handle_state(&bags, request);
                if tx.send(Ok(response)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Answers a bag user state request from memory. GET returns the whole bag in one page.
fn handle_state(bags: &Mutex<HashMap<Vec<u8>, Vec<u8>>>, request: StateRequest) -> StateResponse {
    let key = match request.state_key.and_then(|k| k.r#type) {
        Some(state_key::Type::BagUserState(bag)) => bag.key,
        other => {
            return StateResponse {
                id: request.id,
                error: format!("unsupported state key {other:?}"),
                ..Default::default()
            };
        }
    };
    let mut bags = bags.lock().expect("bags lock");
    let response = match request.request {
        Some(state_request::Request::Get(_)) => state_response::Response::Get(StateGetResponse {
            data: bags.get(&key).cloned().unwrap_or_default(),
            ..Default::default()
        }),
        Some(state_request::Request::Append(append)) => {
            bags.entry(key).or_default().extend(append.data);
            state_response::Response::Append(StateAppendResponse {})
        }
        Some(state_request::Request::Clear(_)) => {
            bags.remove(&key);
            state_response::Response::Clear(StateClearResponse {})
        }
        other => {
            return StateResponse {
                id: request.id,
                error: format!("unsupported state request {other:?}"),
                ..Default::default()
            };
        }
    };
    StateResponse {
        id: request.id,
        response: Some(response),
        ..Default::default()
    }
}

#[tonic::async_trait]
impl BeamFnLogging for Services {
    type LoggingStream = Replies<LogControl>;

    async fn logging(
        &self,
        request: Request<Streaming<log_entry::List>>,
    ) -> Result<Response<Self::LoggingStream>, Status> {
        let mut entries = request.into_inner();
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            while let Ok(Some(_)) = entries.message().await {}
            drop(tx);
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// A runner serving a worker that runs in the same process.
pub struct BenchRunner {
    endpoint: String,
    requests: mpsc::Sender<InstructionRequest>,
    services: Services,
    next_instruction: AtomicUsize,
}

impl BenchRunner {
    /// Starts the services and a worker with `handlers`. Call from a Tokio runtime.
    pub async fn start(handlers: HashMap<String, TransformFn>) -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local port");
        let endpoint = listener.local_addr().expect("local address").to_string();
        let (requests, requests_rx) = mpsc::channel(1024);
        let services = Services {
            requests: Arc::new(Mutex::new(Some(requests_rx))),
            responses: Arc::default(),
            data: watch::channel(None).0,
            outputs: Arc::default(),
            bags: Arc::default(),
        };
        let served = services.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(BeamFnControlServer::new(served.clone()))
                .add_service(BeamFnDataServer::new(served.clone()))
                .add_service(BeamFnStateServer::new(served.clone()))
                .add_service(BeamFnLoggingServer::new(served))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("benchmark runner services");
        });
        let worker = Worker::new(HarnessOptions {
            id: Some("benchmark_worker".to_string()),
            control_endpoint: Some(endpoint.clone()),
            logging_endpoint: Some(endpoint.clone()),
            ..Default::default()
        })
        .with_handlers(handlers);
        tokio::spawn(async move { worker.run().await.expect("benchmark worker") });
        Arc::new(Self {
            endpoint,
            requests,
            services,
            next_instruction: AtomicUsize::new(0),
        })
    }

    /// The services' address, for the data, state and logging ports of descriptors.
    pub fn api_service_descriptor(&self) -> ApiServiceDescriptor {
        ApiServiceDescriptor {
            url: self.endpoint.clone(),
            ..Default::default()
        }
    }

    fn next_id(&self, prefix: &str) -> String {
        format!(
            "{prefix}-{}",
            self.next_instruction.fetch_add(1, Ordering::Relaxed)
        )
    }

    async fn instruct(
        &self,
        instruction_id: String,
        request: instruction_request::Request,
    ) -> InstructionResponse {
        let (tx, rx) = oneshot::channel();
        self.services
            .responses
            .lock()
            .expect("responses lock")
            .insert(instruction_id.clone(), tx);
        self.requests
            .send(InstructionRequest {
                instruction_id,
                request: Some(request),
            })
            .await
            .expect("the worker reads its control stream");
        rx.await.expect("the worker answers every instruction")
    }

    /// Registers `descriptors` with the worker.
    pub async fn register(&self, descriptors: Vec<ProcessBundleDescriptor>) {
        let response = self
            .instruct(
                self.next_id("register"),
                instruction_request::Request::Register(RegisterRequest {
                    process_bundle_descriptor: descriptors,
                }),
            )
            .await;
        assert!(response.error.is_empty(), "register: {}", response.error);
    }

    /// Runs one bundle of `descriptor_id`: sends `input` to `source`, waits for the response
    /// and for the end of the output, and returns the output bytes. With `cache_state`, the
    /// request carries a user state cache token, so the worker may cache state across bundles.
    pub async fn process_bundle(
        &self,
        descriptor_id: &str,
        source: &str,
        input: Vec<u8>,
        cache_state: bool,
    ) -> Vec<u8> {
        let instruction_id = self.next_id("bundle");
        let (output_tx, output_rx) = oneshot::channel();
        self.services
            .outputs
            .lock()
            .expect("outputs lock")
            .insert(instruction_id.clone(), (Vec::new(), Some(output_tx)));
        let cache_tokens = if cache_state {
            vec![CacheToken {
                token: b"cacheMe".to_vec(),
                r#type: Some(cache_token::Type::UserState(cache_token::UserState {})),
            }]
        } else {
            Vec::new()
        };
        let request = instruction_request::Request::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: descriptor_id.to_string(),
            cache_tokens,
            ..Default::default()
        });
        let feed = async {
            let mut data = self.services.data.subscribe();
            let sender = data
                .wait_for(Option::is_some)
                .await
                .expect("the data stream connects")
                .clone()
                .expect("a data stream sender");
            sender
                .send(Ok(Elements {
                    data: vec![elements::Data {
                        instruction_id: instruction_id.clone(),
                        transform_id: source.to_string(),
                        data: input,
                        is_last: true,
                    }],
                    timers: Vec::new(),
                }))
                .await
                .expect("the worker reads its data stream");
        };
        let (response, ()) = tokio::join!(self.instruct(instruction_id.clone(), request), feed);
        assert!(response.error.is_empty(), "bundle: {}", response.error);
        let output = output_rx.await.expect("the bundle's output ends");
        self.services
            .outputs
            .lock()
            .expect("outputs lock")
            .remove(&instruction_id);
        output
    }
}
