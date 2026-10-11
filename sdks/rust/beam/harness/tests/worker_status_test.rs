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
use common::{DescriptorBuilder, Gate, SINK_ID, TestBundle, within};

use beam::coders::{Coder, Context, StringUtf8Coder, WindowedValue, WindowedValueCoder};
use beam::internals::{ElementSink, PendingFinalizer, TransformFn};
use harness::bundle_processor::BundleProcessor;
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::instruction_request::Request as InstRequest;
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleDescriptor, ProcessBundleRequest, RegisterRequest,
};
use std::collections::HashMap;

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
    let handler = WorkerStatusHandler::connect(
        &endpoint,
        "test-worker-node-1",
        metrics.clone(),
        std::sync::Weak::new(),
    );

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
    let info = format_status_info(Instant::now(), &WorkerMetrics::new(), None);

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

#[test]
fn status_report_counts_the_expired_finalization_callbacks() {
    let processor = TestBundle::empty().processor;
    processor.store_finalization(
        "expired",
        vec![PendingFinalizer {
            deadline: Instant::now() - std::time::Duration::from_secs(1),
            callback: Box::new(|| Ok(())),
        }],
    );
    assert_eq!(processor.sweep_expired_finalizations(), 1);

    let status = format_status_info(Instant::now(), &WorkerMetrics::new(), Some(&processor));
    assert!(
        status.contains("Expired Finalization Callbacks: 1"),
        "{status}"
    );
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

    let handler = WorkerStatusHandler::connect(
        &format!("http://{addr}"),
        "worker",
        WorkerMetrics::new(),
        std::sync::Weak::new(),
    );
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

const GATE_ID: &str = "gate_transform";

fn create_status_test_descriptor(descriptor_id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(descriptor_id)
        .stage(GATE_ID, "pcoll_input", "pcoll_gated")
        .sink(SINK_ID, "pcoll_gated")
        .build()
}

/// Active bundle state is included in status reports while blocked, and removed once complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_bundle_status_reports_transform_and_lull_until_finished() {
    let released = Gate::new();
    let _release_on_exit = released.open_on_drop();
    let handler_gate = Arc::clone(&released);

    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(
        GATE_ID.to_string(),
        Arc::new(move |in_bytes: &[u8], sink: &mut dyn ElementSink| {
            handler_gate.wait("the test to release the gated handler");
            sink.push(in_bytes.to_vec())
        }),
    );

    let TestBundle {
        processor: bundle_processor,
        data_manager,
        ..
    } = TestBundle::new(handlers);
    let metrics = WorkerMetrics::new();
    let control_client = ControlClient::new(bundle_processor.clone()).with_metrics(metrics.clone());

    let descriptor_id = "desc_status_test_1";
    let pbd = create_status_test_descriptor(descriptor_id);

    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_reg".to_string(),
            request: Some(InstRequest::Register(RegisterRequest {
                process_bundle_descriptor: vec![pbd],
            })),
        })
        .await;

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    let wv = WindowedValue::global("elem_1".to_string(), 1700000000);
    coder.encode(&wv, &mut payload, Context::Nested).unwrap();

    let instruction_id = "inst_status_blocked_42";

    data_manager
        .handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: instruction_id.to_string(),
                transform_id: "source_transform".to_string(),
                data: payload,
                is_last: true,
            }],
            timers: Vec::new(),
        })
        .await;

    let cc_clone = control_client.clone();
    let inst_id_copy = instruction_id.to_string();
    let desc_id_copy = descriptor_id.to_string();
    let bundle_handle = tokio::spawn(async move {
        cc_clone
            .handle_instruction(InstructionRequest {
                instruction_id: inst_id_copy,
                request: Some(InstRequest::ProcessBundle(ProcessBundleRequest {
                    process_bundle_descriptor_id: desc_id_copy,
                    data_stream_id: "data_stream_status_test".to_string(),
                    cache_tokens: Vec::new(),
                    elements: None,
                    has_no_state: false,
                    only_bundle_for_keys: false,
                })),
            })
            .await
    });

    // Wait until the handler starts and blocks on the gate.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let status = format_status_info(Instant::now(), &metrics, Some(&bundle_processor));
    assert!(
        status.contains(instruction_id),
        "status report must contain instruction id '{instruction_id}':\n{status}"
    );
    assert!(
        status.contains(GATE_ID),
        "status report must contain running transform id '{GATE_ID}':\n{status}"
    );
    assert!(
        status.contains("Lull:"),
        "status report must contain lull field:\n{status}"
    );
    assert!(
        status.contains(descriptor_id),
        "status report must contain descriptor id '{descriptor_id}':\n{status}"
    );
    assert!(
        status.contains("reading input"),
        "status report must indicate reading input phase:\n{status}"
    );

    // Release the gate so the bundle ends.
    released.open();
    let bundle_resp = bundle_handle.await.expect("bundle execution task");
    assert_eq!(bundle_resp.instruction_id, instruction_id);

    // After the bundle ends the line is gone.
    let status_after = format_status_info(Instant::now(), &metrics, Some(&bundle_processor));
    assert!(
        !status_after.contains(instruction_id),
        "finished bundle must be removed from status report:\n{status_after}"
    );
}

/// Active bundles in status reports are sorted by longest lull first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_bundles_ordered_by_longest_lull_first() {
    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);

    let gate_1 = Gate::new();
    let _release_1 = gate_1.open_on_drop();
    let gate_1_clone = Arc::clone(&gate_1);

    let gate_2 = Gate::new();
    let _release_2 = gate_2.open_on_drop();
    let gate_2_clone = Arc::clone(&gate_2);

    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(
        "gate_1".to_string(),
        Arc::new(move |in_bytes: &[u8], sink: &mut dyn ElementSink| {
            gate_1_clone.wait("gate 1 release");
            sink.push(in_bytes.to_vec())
        }),
    );
    handlers.insert(
        "gate_2".to_string(),
        Arc::new(move |in_bytes: &[u8], sink: &mut dyn ElementSink| {
            gate_2_clone.wait("gate 2 release");
            sink.push(in_bytes.to_vec())
        }),
    );

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let metrics = WorkerMetrics::new();
    let control_client = ControlClient::new(bundle_processor.clone()).with_metrics(metrics.clone());

    let desc_1 = DescriptorBuilder::new("desc_1")
        .stage("gate_1", "pcoll_input", "pcoll_gated_1")
        .sink("sink_1", "pcoll_gated_1")
        .build();
    let desc_2 = DescriptorBuilder::new("desc_2")
        .stage("gate_2", "pcoll_input", "pcoll_gated_2")
        .sink("sink_2", "pcoll_gated_2")
        .build();

    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_both".to_string(),
            request: Some(InstRequest::Register(RegisterRequest {
                process_bundle_descriptor: vec![desc_1, desc_2],
            })),
        })
        .await;

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    let wv = WindowedValue::global("elem".to_string(), 1700000000);
    coder.encode(&wv, &mut payload, Context::Nested).unwrap();

    // Start bundle 1 first.
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: "inst_first_older".to_string(),
                transform_id: "source_transform".to_string(),
                data: payload.clone(),
                is_last: true,
            }],
            timers: Vec::new(),
        })
        .await;

    let cc1 = control_client.clone();
    let handle_1 = tokio::spawn(async move {
        cc1.handle_instruction(InstructionRequest {
            instruction_id: "inst_first_older".to_string(),
            request: Some(InstRequest::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_1".to_string(),
                data_stream_id: "stream_1".to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
            })),
        })
        .await
    });

    // Wait so bundle 1 accumulates more lull time than bundle 2.
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

    // Start bundle 2.
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: "inst_second_newer".to_string(),
                transform_id: "source_transform".to_string(),
                data: payload,
                is_last: true,
            }],
            timers: Vec::new(),
        })
        .await;

    let cc2 = control_client.clone();
    let handle_2 = tokio::spawn(async move {
        cc2.handle_instruction(InstructionRequest {
            instruction_id: "inst_second_newer".to_string(),
            request: Some(InstRequest::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_2".to_string(),
                data_stream_id: "stream_2".to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
            })),
        })
        .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let status = format_status_info(Instant::now(), &metrics, Some(&bundle_processor));
    assert!(status.contains("inst_first_older"), "{status}");
    assert!(status.contains("inst_second_newer"), "{status}");

    let pos_older = status.find("inst_first_older").expect("older bundle pos");
    let pos_newer = status.find("inst_second_newer").expect("newer bundle pos");
    assert!(
        pos_older < pos_newer,
        "older bundle with longer lull must appear first:\n{status}"
    );

    gate_1.open();
    gate_2.open();
    handle_1.await.expect("bundle 1 task");
    handle_2.await.expect("bundle 2 task");

    let status_after = format_status_info(Instant::now(), &metrics, Some(&bundle_processor));
    assert!(!status_after.contains("inst_first_older"));
    assert!(!status_after.contains("inst_second_newer"));
}
