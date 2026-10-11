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

//! End-to-end channel splits on a plain (non-splittable) DoFn.
//!
//! `bundle_split_test.rs` covers the split arithmetic. These tests check that the executor
//! enforces an agreed split: it stops claiming at the first residual, residual elements
//! never reach the sink, and the final read index equals `first_residual_element`.
#![expect(clippy::unwrap_used, reason = "integration test helpers")]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use beam::coders::{
    Coder, Context, StringUtf8Coder, VarIntCoder, WindowedValue, WindowedValueCoder,
};
use beam::internals::ElementSink;
use beam::metrics::URN_DATA_CHANNEL_READ_INDEX;
use harness::bundle_processor::{BundleProcessor, TransformFn};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::process_bundle_split_request::DesiredSplit;
use model::fn_execution::process_bundle_split_response::ChannelSplit;
use model::fn_execution::{
    Elements, InstructionRequest, InstructionResponse, ProcessBundleRequest, ProcessBundleResponse,
    ProcessBundleSplitRequest, ProcessBundleSplitResponse, RegisterRequest,
    instruction_request::Request, instruction_response::Response,
};

mod common;
use common::{Gate, OpenOnDrop, SINK_ID, SOURCE_ID, STAGE_ID, windowed_linear_descriptor, within};

const ELEMENTS: usize = 10;

/// A bundle over `ELEMENTS` windowed strings whose stage records each element and blocks in
/// element 0 until the test opens `release`. `_release_on_drop` unblocks it on panic.
struct Fixture {
    control: ControlClient,
    data_out: mpsc::Receiver<Elements>,
    seen: Arc<Mutex<Vec<String>>>,
    release: Arc<Gate>,
    bundle: tokio::task::JoinHandle<InstructionResponse>,
    _release_on_drop: OpenOnDrop,
}

const INSTRUCTION: &str = "inst_channel_split";

async fn start_gated_bundle(descriptor_id: &str) -> Fixture {
    let (data_out_tx, data_out) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let entered = Gate::new();
    let release = Gate::new();
    let release_on_drop = release.open_on_drop();

    let handler: TransformFn = {
        let (seen, entered, release) = (seen.clone(), entered.clone(), release.clone());
        Arc::new(move |bytes: &[u8], sink: &mut dyn ElementSink| {
            let value = StringUtf8Coder
                .decode(&mut Cursor::new(bytes), Context::Nested)
                .map_err(|e| e.to_string())?;
            let first = {
                let mut seen = seen.lock().unwrap();
                seen.push(value);
                seen.len() == 1
            };
            if first {
                entered.open();
                release.wait("the test to release element 0");
            }
            sink.push(bytes.to_vec())
        })
    };
    let handlers = HashMap::from([(STAGE_ID.to_string(), handler)]);
    let processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control = ControlClient::new(processor);

    within(
        "registration",
        control.handle_instruction(InstructionRequest {
            instruction_id: "reg".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![windowed_linear_descriptor(descriptor_id)],
            })),
        }),
    )
    .await;

    // All elements arrive in one chunk, so a stop at the boundary is the executor's decision.
    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    (0..ELEMENTS).for_each(|i| {
        coder
            .encode(
                &WindowedValue::global(format!("e{i}"), 1_700_000_000),
                &mut payload,
                Context::Nested,
            )
            .unwrap();
    });
    within(
        "the inbound data to be accepted",
        data_manager.handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: INSTRUCTION.to_string(),
                transform_id: SOURCE_ID.to_string(),
                data: payload,
                is_last: true,
            }],
            timers: Vec::new(),
        }),
    )
    .await;

    let bundle = tokio::spawn({
        let control = control.clone();
        let descriptor_id = descriptor_id.to_string();
        async move {
            control
                .handle_instruction(InstructionRequest {
                    instruction_id: INSTRUCTION.to_string(),
                    request: Some(Request::ProcessBundle(ProcessBundleRequest {
                        process_bundle_descriptor_id: descriptor_id,
                        ..Default::default()
                    })),
                })
                .await
        }
    });

    // Block until element 0 is in flight, so the split is taken against index 0.
    within(
        "element 0 to reach the stage",
        tokio::task::spawn_blocking(move || entered.wait("element 0 to reach the stage")),
    )
    .await
    .unwrap();

    Fixture {
        control,
        data_out,
        seen,
        release,
        bundle,
        _release_on_drop: release_on_drop,
    }
}

async fn split(
    control: &ControlClient,
    transform_id: &str,
    desired: DesiredSplit,
) -> ProcessBundleSplitResponse {
    let resp = within(
        "the split response",
        control.handle_instruction(InstructionRequest {
            instruction_id: "split".to_string(),
            request: Some(Request::ProcessBundleSplit(ProcessBundleSplitRequest {
                instruction_id: INSTRUCTION.to_string(),
                desired_splits: HashMap::from([(transform_id.to_string(), desired)]),
            })),
        }),
    )
    .await;
    assert!(resp.error.is_empty(), "split failed: {}", resp.error);
    match resp.response {
        Some(Response::ProcessBundleSplit(split)) => split,
        other => panic!("expected a ProcessBundleSplit response, got {other:?}"),
    }
}

fn desired(fraction: f64, estimated: i64) -> DesiredSplit {
    DesiredSplit {
        fraction_of_remainder: fraction,
        allowed_split_points: vec![],
        estimated_input_elements: estimated,
    }
}

async fn finish(fixture: Fixture) -> (ProcessBundleResponse, Vec<String>, Vec<String>) {
    let Fixture {
        mut data_out,
        seen,
        release,
        bundle,
        ..
    } = fixture;
    release.open();
    let resp = within("the bundle response", bundle).await.unwrap();
    assert!(resp.error.is_empty(), "bundle failed: {}", resp.error);
    let Some(Response::ProcessBundle(pb)) = resp.response else {
        panic!("expected a ProcessBundle response");
    };

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut sink_bytes = Vec::new();
    let mut closed = false;
    while let Ok(elements) = data_out.try_recv() {
        for data in elements.data {
            assert_eq!(data.instruction_id, INSTRUCTION);
            assert_eq!(data.transform_id, SINK_ID);
            assert!(!closed, "data after the sink's end-of-stream");
            closed |= data.is_last;
            sink_bytes.extend_from_slice(&data.data);
        }
    }
    assert!(closed, "the sink must still be closed after a split");

    let mut cursor = Cursor::new(sink_bytes.as_slice());
    let mut emitted = Vec::new();
    while (cursor.position() as usize) < sink_bytes.len() {
        emitted.push(coder.decode(&mut cursor, Context::Nested).unwrap().value);
    }
    let seen = seen.lock().unwrap().clone();
    (pb, seen, emitted)
}

fn final_read_index(pb: &ProcessBundleResponse) -> i64 {
    let info = pb
        .monitoring_infos
        .iter()
        .find(|info| info.urn == URN_DATA_CHANNEL_READ_INDEX)
        .expect("final response carries data_channel_read_index");
    assert_eq!(
        info.labels.get("PTRANSFORM").map(String::as_str),
        Some(SOURCE_ID)
    );
    VarIntCoder::decode_varint(&mut Cursor::new(&info.payload)).unwrap()
}

fn names(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(|i| format!("e{i}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_agreed_channel_split_stops_emission_at_the_first_residual() {
    // Element 0 is in flight (progress 0.5), so 9.5 of the 10 elements remain.
    let cases = [
        // Keeping 30% puts the boundary at 0 + round(0.5 + 2.85) = 3.
        ("desc_channel_split", 0.3, 3),
        // A checkpoint keeps only the element in flight.
        ("desc_channel_checkpoint", 0.0, 1),
    ];
    for (descriptor_id, fraction, first_residual) in cases {
        let fixture = start_gated_bundle(descriptor_id).await;

        let response = split(
            &fixture.control,
            SOURCE_ID,
            desired(fraction, ELEMENTS as i64),
        )
        .await;
        assert_eq!(
            response,
            ProcessBundleSplitResponse {
                primary_roots: vec![],
                residual_roots: vec![],
                channel_splits: vec![ChannelSplit {
                    transform_id: SOURCE_ID.to_string(),
                    last_primary_element: first_residual - 1,
                    first_residual_element: first_residual,
                }],
            },
            "{descriptor_id}"
        );

        // A second request cannot move the boundary outwards: keeping 90% of what remains
        // before the granted boundary lands back on it, which is no split.
        let again = split(&fixture.control, SOURCE_ID, desired(0.9, ELEMENTS as i64)).await;
        assert_eq!(
            again,
            ProcessBundleSplitResponse::default(),
            "{descriptor_id}"
        );

        let (pb, seen, emitted) = finish(fixture).await;

        let primary = names(0..first_residual as usize);
        assert_eq!(seen, primary, "residual elements must never be processed");
        assert_eq!(emitted, primary, "residual elements must never be emitted");
        assert_eq!(
            final_read_index(&pb),
            first_residual,
            "the final read index must equal first_residual_element"
        );
        assert!(
            pb.residual_roots.is_empty(),
            "a channel split has no residual roots"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_split_snaps_to_an_allowed_point_and_is_enforced_there() {
    let fixture = start_gated_bundle("desc_channel_allowed").await;

    // Unconstrained this would land at 3; the runner only permits 5 and 8.
    let response = split(
        &fixture.control,
        SOURCE_ID,
        DesiredSplit {
            fraction_of_remainder: 0.3,
            allowed_split_points: vec![5, 8],
            estimated_input_elements: ELEMENTS as i64,
        },
    )
    .await;
    assert_eq!(
        response.channel_splits,
        vec![ChannelSplit {
            transform_id: SOURCE_ID.to_string(),
            last_primary_element: 4,
            first_residual_element: 5,
        }]
    );

    let (pb, seen, emitted) = finish(fixture).await;
    assert_eq!(seen, names(0..5));
    assert_eq!(emitted, names(0..5));
    assert_eq!(final_read_index(&pb), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_split_naming_another_transform_is_declined_and_nothing_is_lost() {
    let fixture = start_gated_bundle("desc_channel_unknown").await;

    let response = split(&fixture.control, "not_the_source", desired(0.0, 10)).await;
    assert_eq!(response, ProcessBundleSplitResponse::default());

    let (pb, seen, emitted) = finish(fixture).await;
    assert_eq!(seen, names(0..ELEMENTS));
    assert_eq!(emitted, names(0..ELEMENTS));
    assert_eq!(final_read_index(&pb), ELEMENTS as i64);
}

#[tokio::test]
async fn a_split_for_an_unknown_instruction_is_empty() {
    let (tx, _rx) = mpsc::channel::<Elements>(64);
    let control = ControlClient::new(Arc::new(BundleProcessor::new(DataManager::new(tx))));
    assert_eq!(
        split(&control, SOURCE_ID, desired(0.5, 10)).await,
        ProcessBundleSplitResponse::default()
    );
}
