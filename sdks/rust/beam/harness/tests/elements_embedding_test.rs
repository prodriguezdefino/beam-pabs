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

use std::collections::HashMap;
use std::sync::Arc;

use beam::internals::{ElementSink, TransformFn};
use beam::pipeline::PROTOCOL_CONTROL_RESPONSE_ELEMENTS_EMBEDDING;
use harness::provisioning::RUNNER_CAPABILITIES_ENV;
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleRequest, RegisterRequest, elements,
    instruction_request::Request, instruction_response::Response,
};

mod common;
use common::{CODER_RAW, DescriptorBuilder, SINK_ID, TestBundle, bytes_coder};

#[tokio::test]
async fn test_bundle_execution_with_embedded_elements() {
    // The runner declares response embedding.
    // SAFETY: This binary contains exactly one #[tokio::test]. We set the environment variable
    // once here before any test operations or asynchronous tasks run, ensuring no concurrent
    // environment reads or writes can occur.
    unsafe {
        std::env::set_var(
            RUNNER_CAPABILITIES_ENV,
            PROTOCOL_CONTROL_RESPONSE_ELEMENTS_EMBEDDING,
        );
    }

    run_scenario_embedded_elements_in_and_out().await;
    run_scenario_many_embedded_input_chunks().await;
    run_scenario_bundle_without_outputs_leaves_embedded_elements_unset().await;
}

async fn run_scenario_embedded_elements_in_and_out() {
    let handler: TransformFn = Arc::new(|elem: &[u8], sink: &mut dyn ElementSink| {
        let mut out = elem.to_vec();
        out.extend_from_slice(b"_processed");
        sink.push(out)
    });

    let handlers = HashMap::from([("stage_embed".to_string(), handler)]);
    let TestBundle {
        control_client,
        mut data_out_rx,
        ..
    } = TestBundle::new(handlers);

    let descriptor = DescriptorBuilder::new("desc_embed")
        .with_coders(CODER_RAW, bytes_coder())
        .stage("stage_embed", "pcoll_input", "pcoll_out")
        .sink(SINK_ID, "pcoll_out")
        .build();

    let reg_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_embed".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor.clone()],
            })),
        })
        .await;
    assert!(reg_resp.error.is_empty(), "registration must succeed");

    // Provide embedded input elements directly in ProcessBundleRequest
    let embedded_input = Elements {
        data: vec![elements::Data {
            instruction_id: "bundle_embed_1".to_string(),
            transform_id: "source_transform".to_string(),
            data: b"hello".to_vec(),
            is_last: true,
        }],
        timers: Vec::new(),
    };

    let pb_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "bundle_embed_1".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_embed".to_string(),
                data_stream_id: "".to_string(),
                elements: Some(embedded_input),
                ..Default::default()
            })),
        })
        .await;

    assert!(
        pb_resp.error.is_empty(),
        "bundle execution must succeed: {}",
        pb_resp.error
    );

    let pb_response = match pb_resp.response {
        Some(Response::ProcessBundle(resp)) => resp,
        other => panic!("expected ProcessBundle response, got {other:?}"),
    };

    // The output should be embedded in ProcessBundleResponse.elements
    let embedded_out = pb_response
        .elements
        .expect("ProcessBundleResponse must have embedded elements");

    assert!(
        !embedded_out.data.is_empty(),
        "embedded output data must not be empty"
    );

    let output_data = embedded_out
        .data
        .iter()
        .find(|d| d.transform_id == SINK_ID && !d.data.is_empty())
        .expect("output element for sink must be present");

    assert_eq!(
        output_data.data.as_slice(),
        b"hello_processed",
        "output element must match expected processed value"
    );

    // No output should have been sent over the gRPC data stream
    assert!(
        data_out_rx.try_recv().is_err(),
        "no messages should be sent over gRPC data stream when embedding is used"
    );
}

async fn run_scenario_many_embedded_input_chunks() {
    let handler: TransformFn =
        Arc::new(|elem: &[u8], sink: &mut dyn ElementSink| sink.push(elem.to_vec()));

    let handlers = HashMap::from([("stage_many".to_string(), handler)]);
    let TestBundle { control_client, .. } = TestBundle::new(handlers);

    let descriptor = DescriptorBuilder::new("desc_many")
        .with_coders(CODER_RAW, bytes_coder())
        .stage("stage_many", "pcoll_in", "pcoll_out")
        .sink(SINK_ID, "pcoll_out")
        .build();

    let reg_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_many".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;
    assert!(reg_resp.error.is_empty(), "registration must succeed");

    // Send embedded chunks past INBOUND_QUEUE_CHUNKS to verify no backpressure deadlock.
    let mut data_chunks = Vec::with_capacity(150);
    for i in 0..149 {
        data_chunks.push(elements::Data {
            instruction_id: "bundle_many_1".to_string(),
            transform_id: "source_transform".to_string(),
            data: format!("chunk_{i}").into_bytes(),
            is_last: false,
        });
    }
    data_chunks.push(elements::Data {
        instruction_id: "bundle_many_1".to_string(),
        transform_id: "source_transform".to_string(),
        data: b"chunk_149".to_vec(),
        is_last: true,
    });

    let embedded_input = Elements {
        data: data_chunks,
        timers: Vec::new(),
    };

    let pb_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "bundle_many_1".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_many".to_string(),
                data_stream_id: "".to_string(),
                elements: Some(embedded_input),
                ..Default::default()
            })),
        })
        .await;

    assert!(
        pb_resp.error.is_empty(),
        "bundle with 150 embedded chunks must succeed without deadlock: {}",
        pb_resp.error
    );
}

async fn run_scenario_bundle_without_outputs_leaves_embedded_elements_unset() {
    // A terminal transform: it consumes its input and emits nothing.
    let handler: TransformFn = Arc::new(|_elem: &[u8], _sink: &mut dyn ElementSink| Ok(()));
    let handlers = HashMap::from([("stage_terminal".to_string(), handler)]);
    let TestBundle {
        control_client,
        mut data_out_rx,
        ..
    } = TestBundle::new(handlers);

    let descriptor = DescriptorBuilder::new("desc_terminal")
        .with_coders(CODER_RAW, bytes_coder())
        .stage("stage_terminal", "pcoll_input", "pcoll_unconsumed")
        .build_terminal();

    let reg_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_terminal".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;
    assert!(reg_resp.error.is_empty(), "registration must succeed");

    let pb_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "bundle_terminal_1".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_terminal".to_string(),
                elements: Some(Elements {
                    data: vec![elements::Data {
                        instruction_id: "bundle_terminal_1".to_string(),
                        transform_id: "source_transform".to_string(),
                        data: b"hello".to_vec(),
                        is_last: true,
                    }],
                    timers: Vec::new(),
                }),
                ..Default::default()
            })),
        })
        .await;
    assert!(
        pb_resp.error.is_empty(),
        "bundle execution must succeed: {}",
        pb_resp.error
    );

    let Some(Response::ProcessBundle(pb_response)) = pb_resp.response else {
        panic!("expected a ProcessBundle response");
    };
    assert_eq!(
        pb_response.elements, None,
        "a bundle without outbound endpoints must not set embedded elements"
    );
    assert!(
        data_out_rx.try_recv().is_err(),
        "a bundle without outbound endpoints sends nothing on the data stream"
    );
}
