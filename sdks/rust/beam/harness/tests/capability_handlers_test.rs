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

//! Conformance tests that verify declared protocols and transforms map to harness handlers.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use beam::coders::{
    Coder, Context, DefaultCoder, ElementMetadata, StringUtf8Coder, WindowedValue,
    WindowedValueCoder,
};
use beam::internals::{ElementSink, SplittableDoFnHandler};
use beam::metrics::URN_DATA_CHANNEL_READ_INDEX;
use beam::pipeline::{
    PROTOCOL_CONTROL_REQUEST_ELEMENTS_EMBEDDING, PROTOCOL_DATA_SAMPLING, PROTOCOL_ELEMENT_METADATA,
    PROTOCOL_MULTI_CORE_BUNDLE_PROCESSING, PROTOCOL_MULTIMAP_STATE, PROTOCOL_NAMED_DATA_STREAMS,
    PROTOCOL_ORDERED_LIST_STATE, PROTOCOL_SDK_CONSUMING_RECEIVED_DATA, PROTOCOL_STATE_CACHING,
    PROTOCOL_WORKER_STATUS, URN_SDF_TRUNCATE_SIZED_RESTRICTIONS, URN_TO_STRING,
    standard_capabilities,
};
use beam::transforms::ProcessContext;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, ProcessContinuation, SplittableDoFn};
use harness::bundle_processor::{BundleProcessor, TransformFn, lookup_handler, resolve_handler};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, HarnessMonitoringInfosRequest, InstructionRequest, MonitoringInfosMetadataRequest,
    ProcessBundleDescriptor, ProcessBundleProgressRequest, ProcessBundleRequest, RegisterRequest,
    elements, instruction_request::Request, instruction_response::Response,
};
use model::pipeline as proto_pipeline;
use prost::Message;
use tokio::sync::mpsc;

mod common;
use common::{
    CODER_RAW, DescriptorBuilder, Gate, SINK_ID, SOURCE_ID, STAGE_ID, bytes_coder, run_bundle,
};

type CheckFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
type LiveCheckFn =
    Box<dyn for<'a> Fn(&'a ControlClient, &'a DataManager) -> CheckFuture<'a> + Send + Sync>;

enum CapabilityRow {
    /// A live behavioural check executed directly against the harness in this test.
    LiveCheck(LiveCheckFn),
    /// Covered by existing dedicated integration tests whose coverage is made explicit.
    CoveredByTests(&'static [&'static str]),
}

#[derive(Clone)]
struct TruncateTestSdf;

impl SplittableDoFn for TruncateTestSdf {
    type In = String;
    type Out = String;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, _element: &Self::In) -> Self::Restriction {
        OffsetRange::new(0, 10)
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        _tracker: &Self::Tracker,
        _ctx: &mut ProcessContext<Self::Out>,
    ) -> Result<ProcessContinuation, beam::Error> {
        Ok(ProcessContinuation::stop())
    }

    fn restriction_size(&self, _element: &Self::In, restriction: &Self::Restriction) -> f64 {
        (restriction.end - restriction.start) as f64
    }

    fn truncate_restriction(
        &self,
        element: &Self::In,
        restriction: &Self::Restriction,
    ) -> Option<Self::Restriction> {
        if element == "keep" {
            Some(OffsetRange::new(restriction.start, restriction.start + 5))
        } else {
            None
        }
    }
}

fn truncate_transform_spec(key: &str) -> proto_pipeline::FunctionSpec {
    let payload = proto_pipeline::ParDoPayload {
        do_fn: Some(proto_pipeline::FunctionSpec {
            urn: "beam:dofn:rust:v1".to_string(),
            payload: key.as_bytes().to_vec(),
        }),
        ..Default::default()
    }
    .encode_to_vec();

    proto_pipeline::FunctionSpec {
        urn: URN_SDF_TRUNCATE_SIZED_RESTRICTIONS.to_string(),
        payload,
    }
}

/// Runs two bundles concurrently on one ControlClient and ensures both reach their handler.
async fn check_multi_core_bundle_processing() {
    let barrier = Arc::new(Barrier::new(2));
    let (data_out_tx, _data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);

    let b1 = barrier.clone();
    let handler1: TransformFn = Arc::new(move |_elem: &[u8], _sink: &mut dyn ElementSink| {
        b1.wait();
        Ok(())
    });
    let b2 = barrier.clone();
    let handler2: TransformFn = Arc::new(move |_elem: &[u8], _sink: &mut dyn ElementSink| {
        b2.wait();
        Ok(())
    });

    let handlers = HashMap::from([
        ("stage_sync1".to_string(), handler1),
        ("stage_sync2".to_string(), handler2),
    ]);
    let control_client = Arc::new(ControlClient::new(Arc::new(
        BundleProcessor::with_handlers(data_manager.clone(), handlers),
    )));

    let desc1 = DescriptorBuilder::new("desc_mc1")
        .with_coders(CODER_RAW, bytes_coder())
        .stage("stage_sync1", "pcoll_in1", "pcoll_out1")
        .sink("sink1", "pcoll_out1")
        .build();
    let desc2 = DescriptorBuilder::new("desc_mc2")
        .with_coders(CODER_RAW, bytes_coder())
        .stage("stage_sync2", "pcoll_in2", "pcoll_out2")
        .sink("sink2", "pcoll_out2")
        .build();

    let reg_req = InstructionRequest {
        instruction_id: "reg_mc".to_string(),
        request: Some(Request::Register(RegisterRequest {
            process_bundle_descriptor: vec![desc1, desc2],
        })),
    };
    let reg_resp = control_client.handle_instruction(reg_req).await;
    assert!(
        reg_resp.error.is_empty(),
        "registration failed: {}",
        reg_resp.error
    );

    let dm1 = data_manager.clone();
    tokio::spawn(async move {
        dm1.handle_inbound_elements(Elements {
            data: vec![
                elements::Data {
                    instruction_id: "inst_mc1".to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: b"x".to_vec(),
                    is_last: false,
                },
                elements::Data {
                    instruction_id: "inst_mc1".to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: Vec::new(),
                    is_last: true,
                },
            ],
            timers: Vec::new(),
        })
        .await;
    });

    let dm2 = data_manager.clone();
    tokio::spawn(async move {
        dm2.handle_inbound_elements(Elements {
            data: vec![
                elements::Data {
                    instruction_id: "inst_mc2".to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: b"y".to_vec(),
                    is_last: false,
                },
                elements::Data {
                    instruction_id: "inst_mc2".to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: Vec::new(),
                    is_last: true,
                },
            ],
            timers: Vec::new(),
        })
        .await;
    });

    let cc1 = control_client.clone();
    let task1 = tokio::spawn(async move {
        cc1.handle_instruction(InstructionRequest {
            instruction_id: "inst_mc1".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_mc1".to_string(),
                ..Default::default()
            })),
        })
        .await
    });

    let cc2 = control_client.clone();
    let task2 = tokio::spawn(async move {
        cc2.handle_instruction(InstructionRequest {
            instruction_id: "inst_mc2".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_mc2".to_string(),
                ..Default::default()
            })),
        })
        .await
    });

    let (res1, res2) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::try_join!(task1, task2).expect("both bundles join")
    })
    .await
    .expect("both bundles must execute concurrently on one ControlClient without deadlock");

    assert!(res1.error.is_empty(), "bundle 1 failed: {}", res1.error);
    assert!(res2.error.is_empty(), "bundle 2 failed: {}", res2.error);
}

/// Runs a bundle holding an element in a gate, checks progress read index, and resolves short ids.
async fn check_progress_and_short_ids(check_short_ids: bool) {
    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);

    let gate = Gate::new();
    let _release_on_exit = gate.open_on_drop();
    let handler_gate = Arc::clone(&gate);

    let handlers: HashMap<String, TransformFn> = HashMap::from([(
        "gated_transform".to_string(),
        Arc::new(move |in_bytes: &[u8], sink: &mut dyn ElementSink| {
            handler_gate.wait("test to release gate");
            sink.push(in_bytes.to_vec())
        }) as TransformFn,
    )]);

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor);

    let descriptor = DescriptorBuilder::new("desc_prog_test")
        .stage("gated_transform", "pcoll_input", "pcoll_out")
        .sink(SINK_ID, "pcoll_out")
        .build();

    let reg_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_reg_prog".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;
    assert!(
        reg_resp.error.is_empty(),
        "registration failed: {}",
        reg_resp.error
    );

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    let wv = WindowedValue::global("elem_1".to_string(), 1000);
    coder
        .encode(&wv, &mut payload, Context::Nested)
        .expect("encoding windowed value succeeds");

    let instruction_id = "inst_bundle_prog_active";
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![
                elements::Data {
                    instruction_id: instruction_id.to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: payload,
                    is_last: false,
                },
                elements::Data {
                    instruction_id: instruction_id.to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: Vec::new(),
                    is_last: true,
                },
            ],
            timers: Vec::new(),
        })
        .await;

    let cc = control_client.clone();
    let bundle_handle = tokio::spawn(async move {
        cc.handle_instruction(InstructionRequest {
            instruction_id: instruction_id.to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_prog_test".to_string(),
                ..Default::default()
            })),
        })
        .await
    });

    // Wait until the bundle is in-flight in the handler
    tokio::time::sleep(Duration::from_millis(50)).await;

    let progress_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "req_progress".to_string(),
            request: Some(Request::ProcessBundleProgress(
                ProcessBundleProgressRequest {
                    instruction_id: instruction_id.to_string(),
                },
            )),
        })
        .await;

    let Some(Response::ProcessBundleProgress(prog)) = progress_resp.response else {
        panic!("expected ProcessBundleProgressResponse");
    };

    let has_read_index = prog
        .monitoring_infos
        .iter()
        .any(|info| info.urn == URN_DATA_CHANNEL_READ_INDEX);
    assert!(
        has_read_index,
        "data_channel_read_index must be present in monitoring_infos of running bundle"
    );

    if check_short_ids {
        let short_ids: Vec<String> = prog.monitoring_data.keys().cloned().collect();
        assert!(
            !short_ids.is_empty(),
            "running bundle must report monitoring_data with short id keys"
        );

        let meta_resp = control_client
            .handle_instruction(InstructionRequest {
                instruction_id: "req_short_id_meta".to_string(),
                request: Some(Request::MonitoringInfos(MonitoringInfosMetadataRequest {
                    monitoring_info_id: short_ids.clone(),
                })),
            })
            .await;

        let Some(Response::MonitoringInfos(info_meta)) = meta_resp.response else {
            panic!("expected MonitoringInfosMetadataResponse");
        };

        for short_id in &short_ids {
            assert!(
                info_meta.monitoring_info.contains_key(short_id),
                "metadata response must resolve short id '{short_id}'"
            );
        }
    }

    gate.open();
    let bundle_resp = bundle_handle.await.expect("bundle completes");
    assert!(bundle_resp.error.is_empty());
}

/// Runs a bundle with draining element metadata and verifies output retains it.
async fn check_element_metadata_propagation() {
    let descriptor = DescriptorBuilder::new("desc_element_metadata")
        .stage(STAGE_ID, "pcoll_input", "pcoll_out")
        .sink(SINK_ID, "pcoll_out")
        .build();

    let identity_handler: TransformFn =
        Arc::new(|elem: &[u8], sink: &mut dyn ElementSink| sink.push(elem.to_vec()));
    let handlers = HashMap::from([(STAGE_ID.to_string(), identity_handler)]);

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    let wv = WindowedValue::global("meta_val".to_string(), 12345).with_metadata(ElementMetadata {
        drain: beam::coders::CausedByDrain::CausedByDrain,
        ..Default::default()
    });
    coder
        .encode(&wv, &mut payload, Context::Nested)
        .expect("encoding windowed value succeeds");

    let run = run_bundle(handlers, descriptor, vec![payload]).await;
    let _resp = run.bundle_response();

    let out_bytes = run.sink_bytes.get(SINK_ID).expect("sink output exists");
    let mut cursor = std::io::Cursor::new(out_bytes);
    let decoded_wv: WindowedValue<String> = coder
        .decode(&mut cursor, Context::Nested)
        .expect("decoding output succeeds");

    assert_eq!(decoded_wv.value, "meta_val");
    assert!(
        decoded_wv.metadata.is_draining(),
        "element metadata must be preserved through the harness onto sink outputs"
    );
}

/// Builds the mapping from capability URN to harness check or verified test references.
fn capability_check_table() -> HashMap<&'static str, CapabilityRow> {
    let mut table: HashMap<&'static str, CapabilityRow> = HashMap::new();

    table.insert(
        "beam:protocol:progress_reporting:v1",
        CapabilityRow::LiveCheck(Box::new(|_client, _dm| {
            Box::pin(check_progress_and_short_ids(false))
        })),
    );

    table.insert(
        "beam:protocol:harness_monitoring_infos:v1",
        CapabilityRow::LiveCheck(Box::new(|client, _dm| {
            Box::pin(async move {
                let req = InstructionRequest {
                    instruction_id: "check_harness_monitoring_infos".to_string(),
                    request: Some(Request::HarnessMonitoringInfos(
                        HarnessMonitoringInfosRequest {},
                    )),
                };
                let resp = client.handle_instruction(req).await;
                assert!(
                    resp.error.is_empty(),
                    "harness monitoring infos request failed: {}",
                    resp.error
                );
            })
        })),
    );

    table.insert(
        "beam:protocol:monitoring_info_short_ids:v1",
        CapabilityRow::LiveCheck(Box::new(|_client, _dm| {
            Box::pin(check_progress_and_short_ids(true))
        })),
    );

    table.insert(
        PROTOCOL_WORKER_STATUS,
        CapabilityRow::LiveCheck(Box::new(|_client, _dm| {
            Box::pin(async move {
                let metrics = harness::status::WorkerMetrics::new();
                metrics.record_bundle_start();
                metrics.record_bundle_finish();
                let status_string =
                    harness::status::format_status_info(std::time::Instant::now(), &metrics, None);
                assert!(
                    !status_string.is_empty(),
                    "worker status report must produce a non-empty status string"
                );
            })
        })),
    );

    table.insert(
        PROTOCOL_MULTI_CORE_BUNDLE_PROCESSING,
        CapabilityRow::LiveCheck(Box::new(|_client, _dm| {
            Box::pin(check_multi_core_bundle_processing())
        })),
    );

    table.insert(
        PROTOCOL_ELEMENT_METADATA,
        CapabilityRow::LiveCheck(Box::new(|_client, _dm| {
            Box::pin(check_element_metadata_propagation())
        })),
    );

    table.insert(
        PROTOCOL_NAMED_DATA_STREAMS,
        CapabilityRow::CoveredByTests(&[
            "data_plane_test::test_bundle_execution_with_named_data_stream",
        ]),
    );

    table.insert(
        PROTOCOL_SDK_CONSUMING_RECEIVED_DATA,
        CapabilityRow::CoveredByTests(&[
            "bundle_progress_test::test_process_bundle_progress_live_reporting",
            "bundle_progress_test::a_bundle_that_waits_for_data_is_not_consuming",
        ]),
    );

    table.insert(
        PROTOCOL_MULTIMAP_STATE,
        CapabilityRow::CoveredByTests(&[
            "user_state_test::test_bundle_map_state_deferred_commit_lifecycle",
        ]),
    );

    table.insert(
        PROTOCOL_DATA_SAMPLING,
        CapabilityRow::CoveredByTests(&[
            "data_sampling_test::test_data_sampling_samples_elements_and_exceptions",
        ]),
    );

    table.insert(
        PROTOCOL_CONTROL_REQUEST_ELEMENTS_EMBEDDING,
        CapabilityRow::CoveredByTests(&[
            "elements_embedding_test::test_bundle_execution_with_embedded_elements_in_and_out",
        ]),
    );

    table.insert(
        PROTOCOL_ORDERED_LIST_STATE,
        CapabilityRow::CoveredByTests(&[
            "state_protocol_test::ordered_list_state_add_and_read_sorted",
        ]),
    );

    table.insert(
        PROTOCOL_STATE_CACHING,
        CapabilityRow::CoveredByTests(&[
            "state_cache_test::user_state_is_read_once_while_the_token_stays",
            "state_cache_test::a_new_token_reads_user_state_again",
            "state_cache_test::side_input_pages_are_shared_while_the_token_stays",
        ]),
    );

    table.insert(
        URN_TO_STRING,
        CapabilityRow::LiveCheck(Box::new(|_client, _dm| {
            Box::pin(async move {
                let transform = proto_pipeline::PTransform {
                    unique_name: "test_to_string".to_string(),
                    spec: Some(proto_pipeline::FunctionSpec {
                        urn: URN_TO_STRING.to_string(),
                        payload: Vec::new(),
                    }),
                    ..Default::default()
                };
                let descriptor = ProcessBundleDescriptor::default();
                let handler = resolve_handler(&HashMap::new(), &transform, &descriptor);
                assert!(
                    handler.is_some(),
                    "harness handler registry must resolve URN_TO_STRING"
                );
            })
        })),
    );

    table.insert(
        URN_SDF_TRUNCATE_SIZED_RESTRICTIONS,
        CapabilityRow::LiveCheck(Box::new(|_client, _dm| {
            Box::pin(async move {
                let sdf_handler: TransformFn =
                    Arc::new(SplittableDoFnHandler::new(TruncateTestSdf));
                let handlers = HashMap::from([("my_sdf".to_string(), sdf_handler)]);
                let transform = proto_pipeline::PTransform {
                    unique_name: "test_truncate".to_string(),
                    spec: Some(truncate_transform_spec("my_sdf")),
                    ..Default::default()
                };
                let handler = lookup_handler(&handlers, &transform);
                assert!(
                    handler.is_some(),
                    "harness handler registry must resolve URN_SDF_TRUNCATE_SIZED_RESTRICTIONS"
                );
            })
        })),
    );

    table
}

/// Walks all declared protocols and transforms and verifies harness checks or coverage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_declared_protocols_and_transforms_have_harness_handlers() {
    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let bundle_processor = Arc::new(BundleProcessor::new(data_manager.clone()));
    let control_client = ControlClient::new(bundle_processor);

    let table = capability_check_table();

    for capability in standard_capabilities() {
        if !capability.starts_with("beam:protocol:") && !capability.starts_with("beam:transform:") {
            continue;
        }

        let row = table.get(capability.as_str()).unwrap_or_else(|| {
            panic!(
                "Declared capability '{capability}' has no entry in the capability handler table. \
                 Add a handler check to capability_handlers_test.rs."
            );
        });

        match row {
            CapabilityRow::LiveCheck(check) => {
                check(&control_client, &data_manager).await;
            }
            CapabilityRow::CoveredByTests(test_names) => {
                assert!(
                    !test_names.is_empty(),
                    "Covered capability '{capability}' must reference at least one test"
                );
            }
        }
    }
}

/// Runs a bundle through an SDF truncate transform and checks re-computed restriction sizes.
#[tokio::test]
async fn test_sdf_truncate_sized_restrictions_bundle_execution() {
    let mut descriptor = DescriptorBuilder::new("desc_truncate")
        .with_coders(CODER_RAW, bytes_coder())
        .stage("my_sdf", "pcoll_input", "pcoll_out")
        .sink(SINK_ID, "pcoll_out")
        .build();

    descriptor.transforms.get_mut("my_sdf").unwrap().spec = Some(truncate_transform_spec("my_sdf"));

    let sdf_handler: TransformFn = Arc::new(SplittableDoFnHandler::new(TruncateTestSdf));
    let handlers = HashMap::from([("my_sdf".to_string(), sdf_handler)]);

    let elem_keep = (("keep".to_string(), OffsetRange::new(0, 10)), 10.0_f64)
        .encode()
        .expect("encoding keep element succeeds");
    let elem_drop = (("drop".to_string(), OffsetRange::new(0, 10)), 10.0_f64)
        .encode()
        .expect("encoding drop element succeeds");

    let run = run_bundle(handlers, descriptor, vec![elem_keep, elem_drop]).await;
    let _resp = run.bundle_response();

    let out_bytes = run.sink_bytes.get(SINK_ID).expect("sink output exists");
    let decoded: ((String, OffsetRange), f64) = <((String, OffsetRange), f64)>::decode(out_bytes)
        .expect("decoding output element succeeds");

    assert_eq!(decoded.0.0, "keep", "element value must be 'keep'");
    assert_eq!(
        decoded.0.1,
        OffsetRange::new(0, 5),
        "restriction must be truncated to [0, 5)"
    );
    assert_eq!(decoded.1, 5.0, "size must be re-computed to 5.0");

    let expected_encoded = (("keep".to_string(), OffsetRange::new(0, 5)), 5.0_f64)
        .encode()
        .expect("encoding expected element succeeds");
    assert_eq!(
        out_bytes.as_slice(),
        expected_encoded.as_slice(),
        "dropped element must produce no output"
    );
}
