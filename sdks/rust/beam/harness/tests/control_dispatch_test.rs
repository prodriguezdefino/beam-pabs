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

use beam::internals::{ElementSink, TransformFn};
use harness::bundle_processor::BundleProcessor;
use harness::control::ControlClient;
use model::fn_execution::{
    Elements, FinalizeBundleRequest, InstructionRequest, ProcessBundleRequest, RegisterRequest,
    instruction_request::Request, instruction_response::Response,
};
use std::collections::HashMap;
use std::sync::Arc;

mod common;
use common::{STAGE_ID, TestBundle, identity_handlers, linear_descriptor, within};

#[tokio::test]
async fn test_process_bundle_unknown_descriptor_returns_error() {
    let TestBundle { control_client, .. } = TestBundle::empty();

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
    let TestBundle { control_client, .. } = TestBundle::empty();

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

/// A callback that expires in one minute.
fn pending(
    callback: impl FnOnce() -> beam::Result + Send + 'static,
) -> beam::internals::PendingFinalizer {
    beam::internals::PendingFinalizer {
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
        callback: Box::new(callback),
    }
}

#[tokio::test]
async fn test_finalize_bundle_executes_callbacks() {
    let TestBundle {
        processor: bundle_processor,
        control_client,
        ..
    } = TestBundle::empty();

    let executed_first = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let executed_second = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let ex1 = Arc::clone(&executed_first);
    let ex2 = Arc::clone(&executed_second);

    let callbacks = vec![
        pending(move || {
            ex1.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }),
        pending(move || {
            ex2.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }),
    ];

    bundle_processor.store_finalization("inst_bundle_target", callbacks);

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
    let TestBundle {
        processor: bundle_processor,
        control_client,
        ..
    } = TestBundle::empty();

    let callbacks = vec![pending(
        || Err("simulated commit failure on storage".into()),
    )];

    bundle_processor.store_finalization("inst_bundle_fail", callbacks);

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
    let TestBundle { control_client, .. } = TestBundle::empty();
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
    // The stage handler fails on every element.
    let handlers = HashMap::from([(
        STAGE_ID.to_string(),
        Arc::new(|_in_bytes: &[u8], _sink: &mut dyn ElementSink| {
            Err("simulated fatal transform failure".to_string())
        }) as TransformFn,
    )]);

    let TestBundle {
        control_client,
        data_manager,
        data_out_rx: _data_out_rx,
        ..
    } = TestBundle::new(handlers);

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

#[tokio::test]
async fn a_bundle_that_panics_fails_with_an_error_and_is_unregistered() {
    let panics: TransformFn = Arc::new(|_: &[u8], _: &mut dyn ElementSink| panic!("boom"));
    let TestBundle {
        processor: bundle_processor,
        control_client,
        data_manager,
        data_out_rx: _data_out_rx,
        ..
    } = TestBundle::new(HashMap::from([(STAGE_ID.to_string(), panics)]));
    let register = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "register".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![linear_descriptor("panics")],
            })),
        })
        .await;
    assert!(register.error.is_empty(), "{}", register.error);

    let bundle = control_client.handle_instruction_isolated(InstructionRequest {
        instruction_id: "bundle".to_string(),
        request: Some(Request::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: "panics".to_string(),
            ..Default::default()
        })),
    });
    let feed = data_manager.handle_inbound_elements(Elements {
        data: vec![model::fn_execution::elements::Data {
            instruction_id: "bundle".to_string(),
            transform_id: common::SOURCE_ID.to_string(),
            data: vec![1, 2, 3],
            is_last: true,
        }],
        timers: Vec::new(),
    });
    let (response, ()) = within("the response of a bundle that panics", async {
        tokio::join!(bundle, feed)
    })
    .await;

    assert_eq!(response.instruction_id, "bundle");
    assert!(
        response.error.contains("panicked: boom"),
        "{}",
        response.error
    );
    assert!(bundle_processor.active_bundle_reports().is_empty());
}
