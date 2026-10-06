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

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures unwrap; a failure is a test failure"
)]

//! Integration and unit tests for `BeamFnWorkerStatus` reporting.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use harness::status::{WorkerMetrics, WorkerStatusHandler, format_status_info};
use model::fn_execution::beam_fn_worker_status_server::{
    BeamFnWorkerStatus, BeamFnWorkerStatusServer,
};
use model::fn_execution::{WorkerStatusRequest, WorkerStatusResponse};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

mod common;
use common::within;

/// The runner's half of a status stream.
type RequestSender = mpsc::Sender<Result<WorkerStatusRequest, Status>>;

#[test]
fn test_worker_metrics_accounting() {
    let metrics = WorkerMetrics::new();
    assert_eq!(metrics.active_bundles(), 0);
    assert_eq!(metrics.total_bundles(), 0);
    assert_eq!(metrics.descriptors_count(), 0);

    metrics.set_descriptors_count(5);
    assert_eq!(metrics.descriptors_count(), 5);

    metrics.record_bundle_start();
    metrics.record_bundle_start();
    assert_eq!(metrics.active_bundles(), 2);
    assert_eq!(metrics.total_bundles(), 0);

    metrics.record_bundle_finish();
    assert_eq!(metrics.active_bundles(), 1);
    assert_eq!(metrics.total_bundles(), 1);

    metrics.record_bundle_finish();
    assert_eq!(metrics.active_bundles(), 0);
    assert_eq!(metrics.total_bundles(), 2);
}

#[derive(Clone)]
struct MockStatusService {
    requests_to_send: Vec<String>,
    received_responses: Arc<Mutex<Vec<WorkerStatusResponse>>>,
    received_worker_id: Arc<Mutex<Option<String>>>,
}

#[tonic::async_trait]
impl BeamFnWorkerStatus for MockStatusService {
    type WorkerStatusStream =
        Pin<Box<dyn Stream<Item = Result<WorkerStatusRequest, Status>> + Send>>;

    async fn worker_status(
        &self,
        request: Request<Streaming<WorkerStatusResponse>>,
    ) -> Result<Response<Self::WorkerStatusStream>, Status> {
        if let Some(val) = request.metadata().get("worker_id")
            && let Ok(str_val) = val.to_str()
        {
            *self.received_worker_id.lock().unwrap() = Some(str_val.to_string());
        }

        let mut in_stream = request.into_inner();
        let responses = Arc::clone(&self.received_responses);

        tokio::spawn(async move {
            while let Ok(Some(resp)) = in_stream.message().await {
                responses.lock().unwrap().push(resp);
            }
        });

        let (tx, rx) = mpsc::channel(4);
        for id in &self.requests_to_send {
            let _ = tx.send(Ok(WorkerStatusRequest { id: id.clone() })).await;
        }

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[tokio::test]
async fn test_worker_status_grpc_lifecycle() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("should bind local port");
    let addr = listener.local_addr().expect("should get address");

    let received_responses = Arc::new(Mutex::new(Vec::new()));
    let received_worker_id = Arc::new(Mutex::new(None));

    let service = MockStatusService {
        requests_to_send: vec!["req-status-42".to_string()],
        received_responses: Arc::clone(&received_responses),
        received_worker_id: Arc::clone(&received_worker_id),
    };

    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(BeamFnWorkerStatusServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("Server failed");
    });

    let metrics = WorkerMetrics::new();
    metrics.set_descriptors_count(4);
    metrics.record_bundle_start();
    metrics.record_bundle_start();
    metrics.record_bundle_finish();

    let endpoint = format!("http://{addr}");
    let handler = WorkerStatusHandler::connect(&endpoint, "test-worker-node-1", metrics.clone());

    // Wait until the mock runner receives a response.
    let mut retries = 0;
    while retries < 20 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if !received_responses.lock().unwrap().is_empty() {
            break;
        }
        retries += 1;
    }

    let responses = received_responses.lock().unwrap().clone();
    assert_eq!(responses.len(), 1, "Expected 1 WorkerStatusResponse");
    let resp = &responses[0];
    assert_eq!(resp.id, "req-status-42");
    assert!(resp.error.is_empty());
    assert!(resp.status_info.contains("Memory Usage"));
    assert!(resp.status_info.contains("Active Bundles: 1"));
    assert!(resp.status_info.contains("Total Bundles Processed: 1"));
    assert!(resp.status_info.contains("Registered Descriptors: 4"));

    let worker_id = received_worker_id.lock().unwrap().clone();
    assert_eq!(worker_id, Some("test-worker-node-1".to_string()));

    handler.stop();
    server_handle.abort();
}

/// Linux reports `/proc` figures; other platforms report that they are unavailable.
#[test]
fn status_report_includes_memory_usage_details() {
    let info = format_status_info(Instant::now(), &WorkerMetrics::new());

    let mut lines = info.lines();
    lines
        .find(|l| *l == "============Memory Usage============")
        .expect("memory section header");
    let first = lines.next().unwrap_or_default();
    assert!(
        !first.is_empty() && !first.starts_with("===="),
        "the memory section is empty: {info}"
    );
    if cfg!(target_os = "linux") {
        assert!(
            first.starts_with("Vm") || first.starts_with("Threads:"),
            "{first}"
        );
    }
}

/// A status service that keeps its stream open and signals when the worker closes it.
#[derive(Clone)]
struct HangUpProbe {
    connected: Arc<tokio::sync::Notify>,
    closed: Arc<tokio::sync::Notify>,
    /// Holds the runner's half open, so only the worker can end the stream.
    open_streams: Arc<Mutex<Vec<RequestSender>>>,
}

#[tonic::async_trait]
impl BeamFnWorkerStatus for HangUpProbe {
    type WorkerStatusStream =
        Pin<Box<dyn Stream<Item = Result<WorkerStatusRequest, Status>> + Send>>;

    async fn worker_status(
        &self,
        request: Request<Streaming<WorkerStatusResponse>>,
    ) -> Result<Response<Self::WorkerStatusStream>, Status> {
        let mut in_stream = request.into_inner();
        let closed = Arc::clone(&self.closed);
        tokio::spawn(async move {
            while let Ok(Some(_)) = in_stream.message().await {}
            closed.notify_one();
        });

        let (tx, rx) = mpsc::channel(1);
        self.open_streams
            .lock()
            .expect("open streams lock")
            .push(tx);
        self.connected.notify_one();
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Starts a [`HangUpProbe`] server and returns once a status handler's stream to it is open.
async fn connected_handler() -> (
    WorkerStatusHandler,
    HangUpProbe,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("should bind local port");
    let addr = listener.local_addr().expect("should get address");
    let probe = HangUpProbe {
        connected: Arc::default(),
        closed: Arc::default(),
        open_streams: Arc::default(),
    };
    let service = probe.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(BeamFnWorkerStatusServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("Server failed");
    });

    let handler =
        WorkerStatusHandler::connect(&format!("http://{addr}"), "worker", WorkerMetrics::new());
    within("the status stream to open", probe.connected.notified()).await;
    (handler, probe, server)
}

/// `stop` ends the status stream even while the handler itself is still alive.
#[tokio::test]
async fn stopping_or_dropping_the_status_handler_closes_its_stream() {
    for stop in [true, false] {
        let (handler, probe, server) = connected_handler().await;
        if stop {
            handler.stop();
        } else {
            drop(handler);
        }
        within(
            "the status handler to close its stream",
            probe.closed.notified(),
        )
        .await;
        server.abort();
    }
}
