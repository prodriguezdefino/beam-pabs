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

//! End-to-end tests of `Worker::run` against an in-process mock runner: endpoint
//! resolution, the worker identity it presents, connection retries, and shutdown when the
//! runner hangs up. Also `beam::runners::run` in worker mode, which selects `WorkerRunner`.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::{Notify, mpsc};
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Code, Request, Response, Status, Streaming};

use beam::options::{HarnessOptions, PipelineOptions};
use beam::pipeline::Pipeline;
use beam::runners::{PipelineResult, RunnerError};
use harness::bundle_processor::{ExecutionSampler, set_element_processing_timeout};
use harness::worker::{Worker, WorkerError};
use model::fn_execution::beam_fn_control_server::{BeamFnControl, BeamFnControlServer};
use model::fn_execution::beam_fn_data_server::{BeamFnData, BeamFnDataServer};
use model::fn_execution::beam_fn_logging_server::{BeamFnLogging, BeamFnLoggingServer};
use model::fn_execution::provision_service_server::{ProvisionService, ProvisionServiceServer};
use model::fn_execution::{
    Elements, GetProcessBundleDescriptorRequest, GetProvisionInfoRequest, GetProvisionInfoResponse,
    InstructionRequest, InstructionResponse, LogControl, ProcessBundleDescriptor, ProvisionInfo,
    RegisterRequest, instruction_request, log_entry,
};
use model::pipeline::ApiServiceDescriptor;

mod common;
use common::{STAGE_ID, identity_handlers, linear_descriptor, within};

/// Id of the `Register` instruction the mock runner sends.
const REGISTER_ID: &str = "register-1";

/// A server-streaming reply.
type Replies<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Runner-side senders of streams the mock keeps open until the server stops.
type OpenStreams<T> = Arc<Mutex<Vec<mpsc::Sender<Result<T, Status>>>>>;

/// What the mock runner saw, shared between the services and the test.
#[derive(Clone, Default)]
struct Recorder {
    /// `worker_id` metadata of each call, per service.
    provision_ids: Arc<Mutex<Vec<String>>>,
    control_ids: Arc<Mutex<Vec<String>>>,
    data_ids: Arc<Mutex<Vec<String>>>,
    logging_ids: Arc<Mutex<Vec<String>>>,
    /// When each logging call arrived.
    logging_calls: Arc<Mutex<Vec<Instant>>>,
    /// The worker's answer to the `Register` instruction.
    register_response: Arc<Mutex<Option<InstructionResponse>>>,
    /// Signalled when a logging stream opens.
    logging_connected: Arc<Notify>,
}

impl Recorder {
    fn ids(list: &Mutex<Vec<String>>) -> Vec<String> {
        list.lock().expect("recorder lock").clone()
    }
}

/// An in-process runner serving provision, control, data and logging. Control sends one
/// `Register` with `register`, awaits its response, then hangs up unless `hold_control` is set.
#[derive(Clone, Default)]
struct MockRunner {
    rec: Recorder,
    provision_info: ProvisionInfo,
    register: RegisterRequest,
    /// How many more data calls to reject as unavailable.
    data_rejections: Arc<AtomicUsize>,
    /// Rejects every logging call as unavailable.
    reject_logging: bool,
    /// Keeps the control stream open after `Register`, so the worker keeps running.
    hold_control: bool,
    open_control: OpenStreams<InstructionRequest>,
    open_data: OpenStreams<Elements>,
    open_logging: OpenStreams<LogControl>,
}

fn record_worker_id<T>(list: &Mutex<Vec<String>>, request: &Request<T>) {
    let id = request
        .metadata()
        .get("worker_id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<missing>")
        .to_string();
    list.lock().expect("recorder lock").push(id);
}

#[tonic::async_trait]
impl ProvisionService for MockRunner {
    async fn get_provision_info(
        &self,
        request: Request<GetProvisionInfoRequest>,
    ) -> Result<Response<GetProvisionInfoResponse>, Status> {
        record_worker_id(&self.rec.provision_ids, &request);
        Ok(Response::new(GetProvisionInfoResponse {
            info: Some(self.provision_info.clone()),
        }))
    }
}

#[tonic::async_trait]
impl BeamFnControl for MockRunner {
    type ControlStream = Replies<InstructionRequest>;

    async fn control(
        &self,
        request: Request<Streaming<InstructionResponse>>,
    ) -> Result<Response<Self::ControlStream>, Status> {
        record_worker_id(&self.rec.control_ids, &request);
        let mut responses = request.into_inner();
        let register_response = Arc::clone(&self.rec.register_response);
        let register = self.register.clone();
        let (hold_control, open_control) = (self.hold_control, Arc::clone(&self.open_control));
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let register = InstructionRequest {
                instruction_id: REGISTER_ID.to_string(),
                request: Some(instruction_request::Request::Register(register)),
            };
            if tx.send(Ok(register)).await.is_err() {
                return;
            }
            if let Ok(Some(response)) = responses.message().await {
                *register_response.lock().expect("recorder lock") = Some(response);
            }
            if hold_control {
                open_control.lock().expect("open control lock").push(tx);
            }
            // Otherwise dropping `tx` here ends the stream: the runner hangs up.
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_process_bundle_descriptor(
        &self,
        _request: Request<GetProcessBundleDescriptorRequest>,
    ) -> Result<Response<ProcessBundleDescriptor>, Status> {
        Err(Status::unimplemented(
            "the mock runner registers no descriptors",
        ))
    }
}

#[tonic::async_trait]
impl BeamFnData for MockRunner {
    type DataStream = Replies<Elements>;

    async fn data(
        &self,
        request: Request<Streaming<Elements>>,
    ) -> Result<Response<Self::DataStream>, Status> {
        record_worker_id(&self.rec.data_ids, &request);
        let rejected = self
            .data_rejections
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if rejected {
            return Err(Status::unavailable("the mock runner is not ready"));
        }
        let mut inbound = request.into_inner();
        tokio::spawn(async move { while let Ok(Some(_)) = inbound.message().await {} });
        let (tx, rx) = mpsc::channel(1);
        self.open_data.lock().expect("open data lock").push(tx);
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[tonic::async_trait]
impl BeamFnLogging for MockRunner {
    type LoggingStream = Replies<LogControl>;

    async fn logging(
        &self,
        request: Request<Streaming<log_entry::List>>,
    ) -> Result<Response<Self::LoggingStream>, Status> {
        record_worker_id(&self.rec.logging_ids, &request);
        self.rec
            .logging_calls
            .lock()
            .expect("recorder lock")
            .push(Instant::now());
        if self.reject_logging {
            return Err(Status::unavailable("the mock runner rejects logging"));
        }
        let mut inbound = request.into_inner();
        tokio::spawn(async move { while let Ok(Some(_)) = inbound.message().await {} });
        let (tx, rx) = mpsc::channel(1);
        self.open_logging
            .lock()
            .expect("open logging lock")
            .push(tx);
        self.rec.logging_connected.notify_one();
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Binds a local port, returning the listener and its `host:port` address.
async fn listen() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("should bind local port");
    let addr = listener
        .local_addr()
        .expect("should get address")
        .to_string();
    (listener, addr)
}

/// Serves every mock service of `runner` on `listener`.
async fn run_server(listener: TcpListener, runner: MockRunner) {
    tonic::transport::Server::builder()
        .add_service(ProvisionServiceServer::new(runner.clone()))
        .add_service(BeamFnControlServer::new(runner.clone()))
        .add_service(BeamFnDataServer::new(runner.clone()))
        .add_service(BeamFnLoggingServer::new(runner))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .expect("mock runner failed");
}

/// Serves every mock service of `runner` on `listener` until the task is aborted.
fn serve(listener: TcpListener, runner: MockRunner) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_server(listener, runner))
}

fn descriptor(addr: &str) -> Option<ApiServiceDescriptor> {
    Some(ApiServiceDescriptor {
        url: addr.to_string(),
        ..Default::default()
    })
}

fn worker(args: HarnessOptions) -> Worker {
    Worker::with_handlers(args, identity_handlers(&[STAGE_ID]))
}

/// The worker must fail before contacting the runner.
#[tokio::test]
async fn a_worker_without_an_id_is_rejected() {
    let (listener, addr) = listen().await;
    let runner = MockRunner::default();
    let rec = runner.rec.clone();
    let server = serve(listener, runner);

    let result = within(
        "the worker to reject its arguments",
        worker(HarnessOptions {
            control_endpoint: Some(addr),
            ..Default::default()
        })
        .run(),
    )
    .await;

    match result {
        Err(WorkerError::Status(status)) => {
            assert_eq!(status.code(), Code::InvalidArgument);
            assert!(status.message().contains("--id"), "{}", status.message());
        }
        other => panic!("expected an invalid-argument error, got {other:?}"),
    }
    assert!(Recorder::ids(&rec.control_ids).is_empty());
    server.abort();
}

/// The worker sends its id on every stream, and `--logging_endpoint` overrides provisioning.
#[tokio::test]
async fn endpoints_are_resolved_from_provisioning_when_no_control_endpoint_is_given() {
    let (runner_listener, runner_addr) = listen().await;
    let (flag_logging_listener, flag_logging_addr) = listen().await;

    let runner = MockRunner {
        provision_info: ProvisionInfo {
            control_endpoint: descriptor(&runner_addr),
            logging_endpoint: descriptor(&runner_addr),
            ..Default::default()
        },
        ..Default::default()
    };
    let rec = runner.rec.clone();
    let flag_logging = MockRunner::default();
    let flag_logging_rec = flag_logging.rec.clone();
    let servers = [
        serve(runner_listener, runner),
        serve(flag_logging_listener, flag_logging),
    ];

    let result = within(
        "the worker to run until the runner hangs up",
        worker(HarnessOptions {
            id: Some("worker-provisioned".to_string()),
            provision_endpoint: Some(runner_addr),
            logging_endpoint: Some(flag_logging_addr),
            ..Default::default()
        })
        .run(),
    )
    .await;
    assert!(result.is_ok(), "{result:?}");

    let expected = vec!["worker-provisioned".to_string()];
    assert_eq!(Recorder::ids(&rec.provision_ids), expected);
    assert_eq!(Recorder::ids(&rec.control_ids), expected);
    assert_eq!(Recorder::ids(&rec.data_ids), expected);

    within(
        "the logging stream to the flag's endpoint",
        flag_logging_rec.logging_connected.notified(),
    )
    .await;
    assert_eq!(Recorder::ids(&flag_logging_rec.logging_ids), expected);
    assert!(
        Recorder::ids(&rec.logging_ids).is_empty(),
        "the provisioned logging endpoint is overridden by the flag"
    );

    servers.iter().for_each(tokio::task::JoinHandle::abort);
}

/// The worker answers what it was sent, then `run` returns `Ok` instead of hanging.
#[tokio::test]
async fn a_closed_control_stream_shuts_the_worker_down_cleanly() {
    let (listener, addr) = listen().await;
    let runner = MockRunner {
        register: RegisterRequest {
            process_bundle_descriptor: vec![
                linear_descriptor("desc_a"),
                linear_descriptor("desc_b"),
            ],
        },
        ..Default::default()
    };
    let rec = runner.rec.clone();
    let server = serve(listener, runner);

    let worker = worker(HarnessOptions {
        id: Some("worker-closing".to_string()),
        control_endpoint: Some(addr),
        ..Default::default()
    });
    let result = within(
        "the worker to shut down after the control stream closes",
        worker.run(),
    )
    .await;
    assert!(result.is_ok(), "{result:?}");

    let response = rec
        .register_response
        .lock()
        .expect("recorder lock")
        .clone()
        .expect("the worker answers the Register instruction");
    assert_eq!(response.instruction_id, REGISTER_ID);
    assert!(response.error.is_empty(), "{}", response.error);
    assert_eq!(
        Recorder::ids(&rec.control_ids),
        vec!["worker-closing".to_string()]
    );
    assert_eq!(worker.metrics().descriptors_count(), 2);
    server.abort();
}

#[tokio::test]
async fn endpoints_that_come_up_late_are_still_reached() {
    // Hold the port bound so no other process can take it; listen once the runner starts.
    let socket = TcpSocket::new_v4().expect("should create a socket");
    socket
        .bind(([127, 0, 0, 1], 0).into())
        .expect("should bind local port");
    let addr = socket.local_addr().expect("should get address").to_string();
    let runner = MockRunner {
        data_rejections: Arc::new(AtomicUsize::new(2)),
        ..Default::default()
    };
    let rec = runner.rec.clone();
    let server = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let listener = socket
            .listen(1024)
            .expect("should listen on the bound port");
        run_server(listener, runner).await;
    });

    let result = within(
        "the worker to reach the late runner",
        worker(HarnessOptions {
            id: Some("worker-late".to_string()),
            control_endpoint: Some(addr),
            ..Default::default()
        })
        .run(),
    )
    .await;
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(Recorder::ids(&rec.control_ids).len(), 1);
    assert_eq!(
        Recorder::ids(&rec.data_ids).len(),
        3,
        "two rejected data calls, then one accepted"
    );
    server.abort();
}

/// A malformed endpoint fails to dial without I/O, so only the retry sleeps advance the
/// paused clock. A refused connection would let it auto-advance to the connect timeout.
#[tokio::test(start_paused = true)]
async fn control_dial_failures_are_retried_60_times_500ms_apart() {
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(120),
        worker(HarnessOptions {
            id: Some("worker-unreachable".to_string()),
            control_endpoint: Some("not a valid endpoint".to_string()),
            ..Default::default()
        })
        .run(),
    )
    .await
    .expect("the worker gives up instead of retrying forever");
    assert!(matches!(result, Err(WorkerError::Dial(_))), "{result:?}");
    assert_eq!(started.elapsed(), Duration::from_secs(30));
}

/// Runs on the real clock (about 5s): paused time would jump to gRPC timers during I/O.
#[tokio::test]
async fn a_data_stream_the_runner_keeps_rejecting_fails_after_20_retries() {
    let (listener, addr) = listen().await;
    let runner = MockRunner {
        data_rejections: Arc::new(AtomicUsize::new(usize::MAX)),
        ..Default::default()
    };
    let rec = runner.rec.clone();
    let server = serve(listener, runner);

    let result = tokio::time::timeout(
        Duration::from_secs(20),
        worker(HarnessOptions {
            id: Some("worker-rejected".to_string()),
            control_endpoint: Some(addr),
            ..Default::default()
        })
        .run(),
    )
    .await
    .expect("the worker gives up instead of retrying forever");
    match result {
        Err(WorkerError::Status(status)) => assert_eq!(status.code(), Code::Unavailable),
        other => panic!("expected the runner's rejection, got {other:?}"),
    }
    assert_eq!(
        Recorder::ids(&rec.data_ids).len(),
        21,
        "the first call and 20 retries"
    );
    server.abort();
}

#[tokio::test]
async fn a_rejected_logging_stream_reconnects_with_a_doubling_backoff() {
    let (listener, addr) = listen().await;
    let (logging_listener, logging_addr) = listen().await;
    let logging = MockRunner {
        reject_logging: true,
        ..Default::default()
    };
    let logging_rec = logging.rec.clone();
    let control = MockRunner {
        hold_control: true,
        ..Default::default()
    };
    let servers = [serve(listener, control), serve(logging_listener, logging)];

    // The worker keeps running, so it keeps its logging client. The process-wide logging
    // handle keeps only the first client set in the process, which may belong to another
    // test's worker.
    let worker = worker(HarnessOptions {
        id: Some("worker-logging".to_string()),
        control_endpoint: Some(addr),
        logging_endpoint: Some(logging_addr),
        ..Default::default()
    });
    let running = tokio::spawn(async move { worker.run().await });

    let calls = within("three logging calls", async {
        loop {
            let calls = logging_rec
                .logging_calls
                .lock()
                .expect("recorder lock")
                .clone();
            if calls.len() >= 3 {
                break calls;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let (first, second) = (calls[1] - calls[0], calls[2] - calls[1]);
    assert!(first >= Duration::from_millis(500), "first wait {first:?}");
    assert!(
        second >= Duration::from_secs(1),
        "second wait {second:?} should double"
    );
    running.abort();
    servers.iter().for_each(tokio::task::JoinHandle::abort);
}

/// Runs an empty pipeline through `beam::runners::run` as a worker with `flags`.
async fn run_as_worker(flags: &[&str]) -> Result<PipelineResult, RunnerError> {
    let args = ["pipeline", "--worker"].iter().chain(flags).copied();
    let options = PipelineOptions::try_parse_from(args).expect("valid command line");
    beam::runners::run(&Pipeline::new(), &options).await
}

#[tokio::test]
async fn worker_flags_serve_the_pipeline_and_report_done_with_the_worker_id() {
    let (listener, addr) = listen().await;
    let runner = MockRunner::default();
    let rec = runner.rec.clone();
    let server = serve(listener, runner);

    let result = within(
        "the worker runner to finish",
        run_as_worker(&["--id=worker-runner", &format!("--control_endpoint={addr}")]),
    )
    .await
    .expect("a clean shutdown succeeds");

    assert_eq!(result, PipelineResult::new("worker-runner", "DONE"));
    assert_eq!(
        Recorder::ids(&rec.control_ids),
        vec!["worker-runner".to_string()]
    );
    assert!(tracing::dispatcher::has_been_set(), "init_logging must run");
    server.abort();
}

#[tokio::test]
async fn a_worker_failure_is_an_execution_error_wrapping_the_worker_error() {
    let (listener, addr) = listen().await;
    let server = serve(listener, MockRunner::default());

    let result = within(
        "the worker runner to fail",
        run_as_worker(&[&format!("--control_endpoint={addr}")]),
    )
    .await;

    let Err(RunnerError::Execution(source)) = result else {
        panic!("expected an execution error, got {result:?}");
    };
    match source.downcast_ref::<WorkerError>() {
        Some(WorkerError::Status(status)) => assert_eq!(status.code(), Code::InvalidArgument),
        other => panic!("expected the worker's invalid-argument error, got {other:?}"),
    }
    server.abort();
}

/// Set in a child's environment to make it act out the timeout-wiring test.
const TIMEOUT_CHILD_ENV: &str = "BEAM_WORKER_RUNNER_TIMEOUT_CHILD";

/// The timeout is set once per process and fires by exiting it, so this runs in a child.
/// The child's own later 200ms timeout must lose to the 1 minute from the flag, so a
/// 3s stuck element (past the 2s a terminating worker waits) leaves it running.
#[test]
fn the_element_processing_timeout_flag_is_applied_before_serving() {
    const TEST: &str = "the_element_processing_timeout_flag_is_applied_before_serving";
    if std::env::var_os(TIMEOUT_CHILD_ENV).is_some() {
        tokio::runtime::Runtime::new()
            .expect("tokio runtime")
            .block_on(async {
                let (listener, addr) = listen().await;
                let server = serve(listener, MockRunner::default());
                let flags = [
                    "--id=worker-timeout",
                    &format!("--control_endpoint={addr}"),
                    "--element_processing_timeout_minutes=1",
                ];
                within("the worker runner to finish", run_as_worker(&flags))
                    .await
                    .expect("a clean shutdown succeeds");
                server.abort();
            });
        set_element_processing_timeout(Duration::from_millis(200));
        let sampler = ExecutionSampler::new("inst_stuck", &["stuck".to_string()]);
        let previous = sampler.enter(0);
        std::thread::sleep(Duration::from_secs(3));
        sampler.exit(previous);
        return;
    }

    let exe = std::env::current_exe().expect("path of the running test binary");
    let mut child = std::process::Command::new(exe)
        .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
        .env(TIMEOUT_CHILD_ENV, "1")
        .spawn()
        .expect("spawn the child test process");
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            break status;
        }
        if Instant::now() >= deadline {
            // Best effort: the child may exit between the poll and the kill.
            let _ = child.kill();
            panic!("child still running after 30s; killed it");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(
        status.success(),
        "the 1 minute timeout must win, got {status}"
    );
}
