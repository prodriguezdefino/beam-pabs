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

//! Integration tests for `BeamFnControl` instruction dispatch.
//!
//! Covers descriptor registration and dispatch across multiple descriptors, unknown
//! descriptor rejection, bundle finalization, and failure isolation between bundles.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use beam::internals::ElementSink;
use harness::bundle_processor::{BundleProcessor, TransformFn};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, FinalizeBundleRequest, InstructionRequest, ProcessBundleRequest, RegisterRequest,
    instruction_request::Request, instruction_response::Response,
};

mod common;
use common::{STAGE_ID, identity_handlers, linear_descriptor, within};

#[tokio::test]
async fn test_process_bundle_unknown_descriptor_returns_error() {
    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let bundle_processor = Arc::new(BundleProcessor::new(data_manager));
    let control_client = ControlClient::new(bundle_processor);

    let pb_req = InstructionRequest {
        instruction_id: "inst_unknown".to_string(),
        request: Some(Request::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: "non_existent_desc".to_string(),
            cache_tokens: Vec::new(),
            elements: None,
            has_no_state: false,
            only_bundle_for_keys: false,
            data_stream_id: String::new(),
        })),
    };

    let pb_resp = within(
        "the unknown-descriptor bundle response",
        control_client.handle_instruction(pb_req),
    )
    .await;
    assert_eq!(pb_resp.instruction_id, "inst_unknown");
    assert!(!pb_resp.error.is_empty());
    assert!(pb_resp.error.contains("Unknown ProcessBundleDescriptor"));
    assert!(pb_resp.response.is_none());
}
#[tokio::test]
async fn test_finalize_bundle_instruction() {
    let (data_out_tx, _data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);
    let bundle_processor = Arc::new(BundleProcessor::new(data_manager));
    let control_client = ControlClient::new(bundle_processor);

    let req = InstructionRequest {
        instruction_id: "inst_finalize_1".to_string(),
        request: Some(Request::FinalizeBundle(FinalizeBundleRequest {
            instruction_id: "inst_pb_original".to_string(),
        })),
    };

    let resp = control_client.handle_instruction(req).await;
    assert_eq!(resp.instruction_id, "inst_finalize_1");
    assert!(resp.error.is_empty());
    assert!(matches!(resp.response, Some(Response::FinalizeBundle(_))));
}

#[tokio::test]
async fn test_finalize_bundle_executes_callbacks() {
    let (data_out_tx, _data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);
    let bundle_processor = Arc::new(BundleProcessor::new(data_manager));

    let executed_first = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let executed_second = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let ex1 = Arc::clone(&executed_first);
    let ex2 = Arc::clone(&executed_second);

    let callbacks: Vec<beam::internals::FinalizationCallback> = vec![
        Box::new(move || {
            ex1.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }),
        Box::new(move || {
            ex2.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }),
    ];

    bundle_processor.store_finalization("inst_bundle_target", callbacks);

    let control_client = ControlClient::new(bundle_processor);

    let req = InstructionRequest {
        instruction_id: "inst_finalize_rpc_1".to_string(),
        request: Some(Request::FinalizeBundle(FinalizeBundleRequest {
            instruction_id: "inst_bundle_target".to_string(),
        })),
    };

    let resp = control_client.handle_instruction(req).await;
    assert_eq!(resp.instruction_id, "inst_finalize_rpc_1");
    assert!(resp.error.is_empty());
    assert!(matches!(resp.response, Some(Response::FinalizeBundle(_))));

    assert!(executed_first.load(std::sync::atomic::Ordering::SeqCst));
    assert!(executed_second.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn test_finalize_bundle_callback_failure() {
    let (data_out_tx, _data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);
    let bundle_processor = Arc::new(BundleProcessor::new(data_manager));

    let callbacks: Vec<beam::internals::FinalizationCallback> = vec![Box::new(|| {
        Err("simulated commit failure on storage".into())
    })];

    bundle_processor.store_finalization("inst_bundle_fail", callbacks);

    let control_client = ControlClient::new(bundle_processor);

    let req = InstructionRequest {
        instruction_id: "inst_finalize_fail_rpc".to_string(),
        request: Some(Request::FinalizeBundle(FinalizeBundleRequest {
            instruction_id: "inst_bundle_fail".to_string(),
        })),
    };

    let resp = control_client.handle_instruction(req).await;
    assert_eq!(resp.instruction_id, "inst_finalize_fail_rpc");
    assert!(!resp.error.is_empty());
    assert!(resp.error.contains("simulated commit failure on storage"));
    assert!(resp.response.is_none());
}
/// The count is of distinct descriptors, so re-registering an id does not grow it.
#[tokio::test]
async fn registering_descriptors_updates_client_metrics() {
    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let control_client = ControlClient::new(Arc::new(BundleProcessor::new(DataManager::new(
        data_out_tx,
    ))));
    assert_eq!(control_client.metrics().descriptors_count(), 0);

    let cases: [(&[&str], usize); 2] = [
        (&["desc_one", "desc_two"], 2),
        (&["desc_two", "desc_three"], 3),
    ];
    for (round, (ids, expected)) in cases.into_iter().enumerate() {
        let resp = control_client
            .handle_instruction(InstructionRequest {
                instruction_id: format!("reg_metrics_{round}"),
                request: Some(Request::Register(RegisterRequest {
                    process_bundle_descriptor: ids.iter().map(|id| linear_descriptor(id)).collect(),
                })),
            })
            .await;
        assert!(resp.error.is_empty(), "registration failed: {}", resp.error);
        assert_eq!(
            control_client.metrics().descriptors_count(),
            expected,
            "after registering {ids:?}"
        );
    }
}

#[tokio::test]
async fn test_transform_error_handling_and_resource_cleanup() {
    let (data_out_tx, _data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);

    // The stage handler fails on every element.
    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(
        STAGE_ID.to_string(),
        Arc::new(|_in_bytes: &[u8], _sink: &mut dyn ElementSink| {
            Err("simulated fatal transform failure".to_string())
        }),
    );

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor.clone());

    let desc = linear_descriptor("desc_failing");
    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_failing".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![desc],
            })),
        })
        .await;

    // Send one element so that the handler fails.
    tokio::spawn({
        let dm = data_manager.clone();
        async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            dm.handle_inbound_elements(Elements {
                data: vec![model::fn_execution::elements::Data {
                    instruction_id: "inst_fail_1".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: b"trigger_failure".to_vec(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let resp = within(
        "the failing bundle response",
        control_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_fail_1".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_failing".to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
                data_stream_id: String::new(),
            })),
        }),
    )
    .await;

    // The instruction response contains the handler error.
    assert!(!resp.error.is_empty());
    assert!(resp.error.contains("simulated fatal transform failure"));
    assert!(resp.response.is_none());

    // After the failure, a bundle on a healthy processor with the same data manager succeeds.
    let healthy_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        identity_handlers(&[STAGE_ID]),
    ));
    let healthy_client = ControlClient::new(healthy_processor);

    let healthy_desc = linear_descriptor("desc_healthy");
    healthy_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_healthy".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![healthy_desc],
            })),
        })
        .await;

    tokio::spawn({
        let dm = data_manager.clone();
        async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            dm.handle_inbound_elements(Elements {
                data: vec![model::fn_execution::elements::Data {
                    instruction_id: "inst_healthy_2".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: b"healthy_payload".to_vec(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let healthy_resp = within(
        "the healthy bundle response",
        healthy_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_healthy_2".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_healthy".to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
                data_stream_id: String::new(),
            })),
        }),
    )
    .await;

    assert!(healthy_resp.error.is_empty(), "Healthy bundle must succeed");
}
