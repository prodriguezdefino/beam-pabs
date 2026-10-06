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

//! Integration test verifying live ProcessBundleProgressRequest metrics reporting.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

use beam::coders::{
    Coder, Context, StringUtf8Coder, VarIntCoder, WindowedValue, WindowedValueCoder,
};
use beam::internals::ElementSink;
use beam::metrics::URN_DATA_CHANNEL_READ_INDEX;
use harness::bundle_processor::{BundleProcessor, TransformFn};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleDescriptor, ProcessBundleProgressRequest,
    ProcessBundleRequest, RegisterRequest, instruction_request::Request,
    instruction_response::Response,
};

mod common;
use common::{DescriptorBuilder, Gate, SINK_ID, element_counts, within};

const GATE_ID: &str = "gate_transform";

fn create_progress_test_descriptor(descriptor_id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(descriptor_id)
        .stage(GATE_ID, "pcoll_input", "pcoll_gated")
        .sink(SINK_ID, "pcoll_gated")
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_process_bundle_progress_live_reporting() {
    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);

    // A latch, not a one-shot: the handler runs per element and blocks to hold the bundle open.
    let released = Gate::new();
    // Opens the latch even if an assertion fails, so shutdown never hangs.
    let _release_on_exit = released.open_on_drop();
    let handler_gate = Arc::clone(&released);

    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(
        "gate_transform".to_string(),
        Arc::new(move |in_bytes: &[u8], sink: &mut dyn ElementSink| {
            handler_gate.wait("the test to release the gated handler");
            sink.push(in_bytes.to_vec())
        }),
    );

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor);

    let descriptor_id = "desc_progress_test";
    let pbd = create_progress_test_descriptor(descriptor_id);

    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_reg".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![pbd],
            })),
        })
        .await;

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    for i in 0..5 {
        let wv = WindowedValue::global(format!("elem_{i}"), 1700000000);
        // Nested, not WholeStream: elements share this buffer and need length prefixes.
        coder.encode(&wv, &mut payload, Context::Nested).unwrap();
    }

    let instruction_id = "inst_bundle_progress_1";

    data_manager
        .handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: instruction_id.to_string(),
                transform_id: "source_transform".to_string(),
                data: payload,
                is_last: false,
            }],
            timers: Vec::new(),
        })
        .await;

    let cc_clone = control_client.clone();
    let bundle_handle = tokio::spawn(async move {
        cc_clone
            .handle_instruction(InstructionRequest {
                instruction_id: instruction_id.to_string(),
                request: Some(Request::ProcessBundle(ProcessBundleRequest {
                    process_bundle_descriptor_id: descriptor_id.to_string(),
                    data_stream_id: "data_stream_progress".to_string(),
                    cache_tokens: Vec::new(),
                    elements: None,
                    has_no_state: false,
                    only_bundle_for_keys: false,
                })),
            })
            .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let progress_req = InstructionRequest {
        instruction_id: "inst_progress_query_1".to_string(),
        request: Some(Request::ProcessBundleProgress(
            ProcessBundleProgressRequest {
                instruction_id: instruction_id.to_string(),
            },
        )),
    };

    let progress_resp = control_client.handle_instruction(progress_req).await;
    assert_eq!(progress_resp.instruction_id, "inst_progress_query_1");

    match progress_resp.response {
        Some(Response::ProcessBundleProgress(prog)) => {
            assert!(
                !prog.monitoring_infos.is_empty() || !prog.monitoring_data.is_empty(),
                "Progress response must report active monitoring infos or monitoring data"
            );
            let read_index_info = prog
                .monitoring_infos
                .iter()
                .find(|info| info.urn == URN_DATA_CHANNEL_READ_INDEX)
                .unwrap_or_else(|| {
                    panic!(
                        "Expected data_channel_read_index in monitoring_infos: {:?}",
                        prog.monitoring_infos
                    )
                });

            // The gate holds element 0, so the in-flight index must be 0; an index that
            // saturates at the bundle end would tell the runner nothing.
            let read_index =
                VarIntCoder::decode_varint(&mut Cursor::new(&read_index_info.payload)).unwrap();
            assert_eq!(
                read_index, 0,
                "Expected the in-flight element index 0 while the first element is held"
            );
        }
        other => panic!("Expected ProcessBundleProgress response, got {other:?}"),
    }

    // Open the latch so the remaining elements flow through.
    released.open();

    data_manager
        .handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: instruction_id.to_string(),
                transform_id: "source_transform".to_string(),
                data: Vec::new(),
                is_last: true,
            }],
            timers: Vec::new(),
        })
        .await;

    let bundle_resp = within("the bundle response", bundle_handle).await.unwrap();
    assert!(
        bundle_resp.error.is_empty(),
        "Bundle failed: {}",
        bundle_resp.error
    );

    // The final report switches convention: one past the last element read, so a count.
    // Runners compare it against what they sent and reject the bundle on a mismatch.
    let Some(Response::ProcessBundle(pb_resp)) = bundle_resp.response.clone() else {
        panic!("Expected ProcessBundleResponse");
    };
    let final_read_index_info = pb_resp
        .monitoring_infos
        .iter()
        .find(|info| info.urn == URN_DATA_CHANNEL_READ_INDEX)
        .expect("data_channel_read_index must be present in the final response");
    let final_read_index =
        VarIntCoder::decode_varint(&mut Cursor::new(&final_read_index_info.payload)).unwrap();
    assert_eq!(
        final_read_index, 5,
        "Expected the final read index to equal the 5 elements processed"
    );

    let post_progress_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_progress_query_2".to_string(),
            request: Some(Request::ProcessBundleProgress(
                ProcessBundleProgressRequest {
                    instruction_id: instruction_id.to_string(),
                },
            )),
        })
        .await;

    match post_progress_resp.response {
        Some(Response::ProcessBundleProgress(prog)) => {
            assert!(
                prog.monitoring_infos.is_empty() && prog.monitoring_data.is_empty(),
                "Post-completion progress must be empty since bundle is unregistered"
            );
        }
        other => panic!("Expected ProcessBundleProgress response, got {other:?}"),
    }
}

/// The first element's run time; longer than the harness's 100ms progress sync interval.
const SLOWER_THAN_SYNC_INTERVAL: std::time::Duration = std::time::Duration::from_millis(150);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progress_reports_element_counts_once_the_sync_interval_has_passed() {
    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);

    let second_element_released = Gate::new();
    let _release_on_exit = second_element_released.open_on_drop();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler: TransformFn = Arc::new({
        let gate = Arc::clone(&second_element_released);
        let calls = Arc::clone(&calls);
        move |in_bytes: &[u8], sink: &mut dyn ElementSink| {
            match calls.fetch_add(1, Ordering::SeqCst) {
                0 => std::thread::sleep(SLOWER_THAN_SYNC_INTERVAL),
                _ => gate.wait("the test to release the second element"),
            }
            sink.push(in_bytes.to_vec())
        }
    });
    let control_client = ControlClient::new(Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        HashMap::from([(GATE_ID.to_string(), handler)]),
    )));

    let descriptor_id = "desc_progress_counts";
    let registered = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_progress_counts".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![create_progress_test_descriptor(descriptor_id)],
            })),
        })
        .await;
    assert!(registered.error.is_empty(), "{}", registered.error);

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    for word in ["first", "second"] {
        coder
            .encode(
                &WindowedValue::global(word.to_string(), 0),
                &mut payload,
                Context::Nested,
            )
            .unwrap();
    }
    let instruction_id = "inst_progress_counts";
    let element_data = |data: Vec<u8>, is_last: bool| Elements {
        data: vec![model::fn_execution::elements::Data {
            instruction_id: instruction_id.to_string(),
            transform_id: "source_transform".to_string(),
            data,
            is_last,
        }],
        timers: Vec::new(),
    };
    data_manager
        .handle_inbound_elements(element_data(payload, false))
        .await;

    let bundle = tokio::spawn({
        let control_client = control_client.clone();
        async move {
            control_client
                .handle_instruction(InstructionRequest {
                    instruction_id: instruction_id.to_string(),
                    request: Some(Request::ProcessBundle(ProcessBundleRequest {
                        process_bundle_descriptor_id: descriptor_id.to_string(),
                        ..Default::default()
                    })),
                })
                .await
        }
    });

    // The second element is held at the gate, so any counts come from the first.
    let counts = within("element counts in a progress report", async {
        loop {
            let resp = control_client
                .handle_instruction(InstructionRequest {
                    instruction_id: "progress_poll".to_string(),
                    request: Some(Request::ProcessBundleProgress(
                        ProcessBundleProgressRequest {
                            instruction_id: instruction_id.to_string(),
                        },
                    )),
                })
                .await;
            let Some(Response::ProcessBundleProgress(progress)) = resp.response else {
                panic!("expected a progress response, got {:?}", resp.response);
            };
            let counts = element_counts(&progress.monitoring_infos);
            if !counts.is_empty() {
                break counts;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(
        counts,
        HashMap::from([
            ("pcoll_input".to_string(), 1),
            ("pcoll_gated".to_string(), 1)
        ])
    );

    second_element_released.open();
    data_manager
        .handle_inbound_elements(element_data(Vec::new(), true))
        .await;
    let resp = within("the bundle response", bundle).await.unwrap();
    assert!(resp.error.is_empty(), "bundle failed: {}", resp.error);
}
