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

//! Two workers that run one after the other in one process each send logs to their own
//! runner. The global logging handle is process-global, so the case runs in a child process.

use std::future::IntoFuture;
use std::pin::Pin;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::options::HarnessOptions;
use beam_harness::logging::{global_handle, init_logging};
use beam_harness::worker::Worker;
use beam_model::fn_execution::beam_fn_control_server::{BeamFnControl, BeamFnControlServer};
use beam_model::fn_execution::beam_fn_data_server::{BeamFnData, BeamFnDataServer};
use beam_model::fn_execution::beam_fn_logging_server::{BeamFnLogging, BeamFnLoggingServer};
use beam_model::fn_execution::{
    Elements, GetProcessBundleDescriptorRequest, InstructionRequest, InstructionResponse,
    LogControl, ProcessBundleDescriptor, log_entry,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

const CHILD_ENV: &str = "BEAM_WORKER_LOGGING_TEST_CHILD";
const TEST: &str = "worker_logging_test::a_second_worker_sends_its_logs_to_its_own_runner";
const WAIT: Duration = Duration::from_secs(10);
/// The message each worker logs through `tracing` when its control stream ends.
const SHUTDOWN: &str = "Control stream terminated";
/// The test logs this while no worker runs.
const BETWEEN: &str = "logged between the workers";

type Replies<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Runner-side senders of the data streams that stay open until the server stops.
type OpenStreams = Arc<Mutex<Vec<mpsc::Sender<Result<Elements, Status>>>>>;

async fn within<F: IntoFuture>(what: &str, fut: F) -> F::Output {
    tokio::time::timeout(WAIT, fut)
        .await
        .unwrap_or_else(|_| panic!("timed out after {WAIT:?} waiting for {what}"))
}

/// A runner that hangs up the control stream at once and records the log messages it gets.
#[derive(Clone, Default)]
struct MockRunner {
    messages: Arc<Mutex<Vec<String>>>,
    /// Keeps the data streams open until the server stops.
    open_data: OpenStreams,
}

impl MockRunner {
    fn count(&self, text: &str) -> usize {
        self.messages
            .lock()
            .expect("messages lock")
            .iter()
            .filter(|message| message.contains(text))
            .count()
    }
}

#[tonic::async_trait]
impl BeamFnControl for MockRunner {
    type ControlStream = Replies<InstructionRequest>;

    async fn control(
        &self,
        _request: Request<Streaming<InstructionResponse>>,
    ) -> Result<Response<Self::ControlStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn get_process_bundle_descriptor(
        &self,
        _request: Request<GetProcessBundleDescriptorRequest>,
    ) -> Result<Response<ProcessBundleDescriptor>, Status> {
        Err(Status::unimplemented("the mock runner has no descriptors"))
    }
}

#[tonic::async_trait]
impl BeamFnData for MockRunner {
    type DataStream = Replies<Elements>;

    async fn data(
        &self,
        _request: Request<Streaming<Elements>>,
    ) -> Result<Response<Self::DataStream>, Status> {
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
        let mut inbound = request.into_inner();
        let messages = Arc::clone(&self.messages);
        tokio::spawn(async move {
            while let Ok(Some(batch)) = inbound.message().await {
                messages
                    .lock()
                    .expect("messages lock")
                    .extend(batch.log_entries.into_iter().map(|entry| entry.message));
            }
        });
        Ok(Response::new(Box::pin(tokio_stream::pending())))
    }
}

/// Serves `runner` on a local port and returns the port's `host:port` address.
async fn serve(runner: MockRunner) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a local port");
    let addr = listener.local_addr().expect("local address").to_string();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(BeamFnControlServer::new(runner.clone()))
            .add_service(BeamFnDataServer::new(runner.clone()))
            .add_service(BeamFnLoggingServer::new(runner))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    addr
}

/// Runs one worker against the runner at `addr` until the runner hangs up.
async fn run_worker(id: &str, addr: &str) {
    let worker = Worker::new(HarnessOptions {
        id: Some(id.to_string()),
        control_endpoint: Some(addr.to_string()),
        logging_endpoint: Some(addr.to_string()),
        ..Default::default()
    });
    within("the worker to stop", worker.run())
        .await
        .expect("the worker stops cleanly");
}

/// Waits until `runner` got `count` messages that contain `text`.
async fn wait_for(runner: &MockRunner, text: &str, count: usize) {
    within(text, async {
        while runner.count(text) < count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

async fn act() {
    init_logging();
    let first = MockRunner::default();
    let second = MockRunner::default();
    let first_addr = serve(first.clone()).await;
    let second_addr = serve(second.clone()).await;

    run_worker("worker-1", &first_addr).await;
    assert!(
        !global_handle().has_client(),
        "a stopped worker clears its client"
    );
    tracing::info!("{BETWEEN}");
    run_worker("worker-2", &second_addr).await;
    assert!(!global_handle().has_client());

    wait_for(&first, SHUTDOWN, 1).await;
    wait_for(&second, SHUTDOWN, 1).await;
    wait_for(&second, BETWEEN, 1).await;
    // Wait for late entries before the counts.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        first.count(SHUTDOWN),
        1,
        "the second worker logs to its own runner"
    );
    assert_eq!(first.count(BETWEEN), 0);
    assert_eq!(second.count(SHUTDOWN), 1);
}

#[tokio::test]
async fn a_second_worker_sends_its_logs_to_its_own_runner() {
    if std::env::var_os(CHILD_ENV).is_some() {
        act().await;
        return;
    }

    let exe = std::env::current_exe().expect("path of the running test binary");
    let out = Command::new(exe)
        .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .env_remove("RUST_LOG")
        .output()
        .expect("run the child test process");
    assert!(
        out.status.success(),
        "child failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
