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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use beam::coders::{
    Coder, Context, StringUtf8Coder, VarIntCoder, WindowedValue, WindowedValueCoder,
};
use beam::internals::{BundleHandler, ElementSink, HandlerContext, HandlerInstance, TransformFn};
use beam::metrics::URN_DATA_CHANNEL_READ_INDEX;
use beam_harness::control::ControlClient;
use beam_model::fn_execution::process_bundle_split_request::DesiredSplit;
use beam_model::fn_execution::process_bundle_split_response::ChannelSplit;
use beam_model::fn_execution::{
    Elements, InstructionRequest, InstructionResponse, ProcessBundleRequest, ProcessBundleResponse,
    ProcessBundleSplitRequest, ProcessBundleSplitResponse, RegisterRequest,
    instruction_request::Request, instruction_response::Response,
};

use crate::common;
use common::{
    Gate, OpenOnDrop, SINK_ID, SOURCE_ID, STAGE_ID, TestBundle, windowed_linear_descriptor, within,
};

const ELEMENTS: usize = 10;

/// A bundle over `ELEMENTS` windowed strings whose stage records each element and blocks
/// until the test opens `release`. `_release_on_drop` unblocks it on panic.
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
    let seen = Arc::new(Mutex::new(Vec::new()));
    let entered = Gate::new();
    let release = Gate::new();

    let handler: TransformFn = {
        let (seen, entered, release) = (
            Arc::clone(&seen),
            Arc::clone(&entered),
            Arc::clone(&release),
        );
        Arc::new(move |bytes: &[u8], sink: &mut dyn ElementSink| {
            let value = StringUtf8Coder
                .decode(&mut &bytes[..], Context::Nested)
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
    start_bundle(descriptor_id, handler, seen, entered, release).await
}

/// A stage that records each element and blocks in `finish_bundle` until the test opens
/// `release`.
#[derive(Clone)]
struct GatedFinish {
    seen: Arc<Mutex<Vec<String>>>,
    entered: Arc<Gate>,
    release: Arc<Gate>,
}

impl BundleHandler for GatedFinish {
    fn process(&mut self, bytes: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let value = StringUtf8Coder
            .decode(&mut &bytes[..], Context::Nested)
            .map_err(|e| e.to_string())?;
        self.seen.lock().unwrap().push(value);
        ctx.sink.push(bytes.to_vec())
    }

    fn finish_bundle(&mut self, _ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        self.entered.open();
        self.release.wait("the test to release finish_bundle");
        Ok(())
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

/// Starts a bundle of `handler` over `ELEMENTS` windowed strings, and waits until the handler
/// opens `entered`.
async fn start_bundle(
    descriptor_id: &str,
    handler: TransformFn,
    seen: Arc<Mutex<Vec<String>>>,
    entered: Arc<Gate>,
    release: Arc<Gate>,
) -> Fixture {
    start_bundle_with_count(descriptor_id, ELEMENTS, handler, seen, entered, release).await
}

/// Starts a bundle of `handler` over `count` windowed strings, and waits until the handler
/// opens `entered`.
async fn start_bundle_with_count(
    descriptor_id: &str,
    count: usize,
    handler: TransformFn,
    seen: Arc<Mutex<Vec<String>>>,
    entered: Arc<Gate>,
    release: Arc<Gate>,
) -> Fixture {
    start_bundle_in_chunks(descriptor_id, count, None, handler, seen, entered, release).await
}

/// Starts a bundle of `handler` over `count` windowed strings, and waits until the handler
/// opens `entered`. With `cut_element`, the data comes in two chunks, and the first chunk
/// ends in the middle of that element.
async fn start_bundle_in_chunks(
    descriptor_id: &str,
    count: usize,
    cut_element: Option<usize>,
    handler: TransformFn,
    seen: Arc<Mutex<Vec<String>>>,
    entered: Arc<Gate>,
    release: Arc<Gate>,
) -> Fixture {
    let release_on_drop = release.open_on_drop();
    let handlers = HashMap::from([(STAGE_ID.to_string(), handler)]);
    let TestBundle {
        data_out_rx: data_out,
        data_manager,
        control_client: control,
        ..
    } = TestBundle::new(handlers);

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

    // Without a cut, all elements arrive in one chunk, so a stop at the boundary is the
    // executor's decision.
    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    let mut cut = None;
    (0..count).for_each(|i| {
        let start = payload.len();
        coder
            .encode(
                &WindowedValue::global(format!("e{i}"), 1_700_000_000),
                &mut payload,
                Context::Nested,
            )
            .unwrap();
        if cut_element == Some(i) {
            cut = Some(start + (payload.len() - start) / 2);
        }
    });
    let tail = payload.split_off(cut.unwrap_or(payload.len()));
    let chunks = [(payload, tail.is_empty()), (tail, true)];
    for (data, is_last) in chunks.into_iter().filter(|(data, _)| !data.is_empty()) {
        within(
            "the inbound data to be accepted",
            data_manager.handle_inbound_elements(Elements {
                data: vec![beam_model::fn_execution::elements::Data {
                    instruction_id: INSTRUCTION.to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data,
                    is_last,
                }],
                timers: Vec::new(),
            }),
        )
        .await;
    }

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

    // Block until the stage waits for `release`, so the split is taken against a known index.
    within(
        "the stage to block",
        tokio::task::spawn_blocking(move || entered.wait("the stage to block")),
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

    let mut cursor = sink_bytes.as_slice();
    let mut emitted = Vec::new();
    while !cursor.is_empty() {
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
    VarIntCoder::decode_varint(&mut &info.payload[..]).unwrap()
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

/// The channel closes when the input ends. A split request that arrives while the finish
/// handlers run gets no split, and the final read index is the count of elements.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_split_request_after_the_last_element_is_declined() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let entered = Gate::new();
    let release = Gate::new();
    let handler: TransformFn = Arc::new(GatedFinish {
        seen: Arc::clone(&seen),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let fixture = start_bundle("desc_channel_finishing", handler, seen, entered, release).await;

    let response = split(&fixture.control, SOURCE_ID, desired(0.0, ELEMENTS as i64)).await;
    assert_eq!(response, ProcessBundleSplitResponse::default());

    let (pb, seen, emitted) = finish(fixture).await;
    assert_eq!(seen, names(0..ELEMENTS));
    assert_eq!(emitted, names(0..ELEMENTS));
    assert_eq!(final_read_index(&pb), ELEMENTS as i64);
}

#[tokio::test]
async fn a_split_for_an_unknown_instruction_is_empty() {
    let TestBundle {
        control_client: control,
        ..
    } = TestBundle::empty();
    assert_eq!(
        split(&control, SOURCE_ID, desired(0.5, 10)).await,
        ProcessBundleSplitResponse::default()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declined_channel_split_resumes_claiming_and_a_later_split_enforces_its_boundary() {
    // A declined split on the source clears the slow-path flag so all elements still run.
    let fixture = start_gated_bundle("desc_channel_declined_then_complete").await;
    let declined = DesiredSplit {
        fraction_of_remainder: 0.5,
        allowed_split_points: vec![0],
        estimated_input_elements: ELEMENTS as i64,
    };
    assert_eq!(
        split(&fixture.control, SOURCE_ID, declined.clone()).await,
        ProcessBundleSplitResponse::default()
    );
    let (pb, seen, emitted) = finish(fixture).await;
    assert_eq!(seen, names(0..ELEMENTS));
    assert_eq!(emitted, names(0..ELEMENTS));
    assert_eq!(final_read_index(&pb), ELEMENTS as i64);

    // A granted split after a declined split stops at the exact boundary.
    let fixture = start_gated_bundle("desc_channel_declined_then_granted").await;
    assert_eq!(
        split(&fixture.control, SOURCE_ID, declined).await,
        ProcessBundleSplitResponse::default()
    );
    let granted = split(&fixture.control, SOURCE_ID, desired(0.3, ELEMENTS as i64)).await;
    assert_eq!(
        granted.channel_splits,
        vec![ChannelSplit {
            transform_id: SOURCE_ID.to_string(),
            last_primary_element: 2,
            first_residual_element: 3,
        }]
    );
    let (pb, seen, emitted) = finish(fixture).await;
    assert_eq!(seen, names(0..3));
    assert_eq!(emitted, names(0..3));
    assert_eq!(final_read_index(&pb), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_splits_while_claiming_never_cross_the_granted_boundary() {
    const MANY: usize = 1_000;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let entered = Gate::new();
    let release = Gate::new();
    // Open `release` upfront so elements run freely while split requests race with claims.
    release.open();

    let handler: TransformFn = {
        let (seen, entered) = (Arc::clone(&seen), Arc::clone(&entered));
        Arc::new(move |bytes: &[u8], sink: &mut dyn ElementSink| {
            let value = StringUtf8Coder
                .decode(&mut &bytes[..], Context::Nested)
                .map_err(|e| e.to_string())?;
            let count = {
                let mut seen = seen.lock().unwrap();
                seen.push(value);
                seen.len()
            };
            if count == 1 {
                entered.open();
            } else if count.is_multiple_of(8) {
                std::thread::yield_now();
            }
            sink.push(bytes.to_vec())
        })
    };
    let fixture = start_bundle_with_count(
        "desc_channel_concurrent_split",
        MANY,
        handler,
        seen,
        entered,
        release,
    )
    .await;

    let declined = DesiredSplit {
        fraction_of_remainder: 0.5,
        allowed_split_points: vec![0],
        estimated_input_elements: MANY as i64,
    };
    for _ in 0..8 {
        assert_eq!(
            split(&fixture.control, SOURCE_ID, declined.clone()).await,
            ProcessBundleSplitResponse::default()
        );
    }
    let granted = split(&fixture.control, SOURCE_ID, desired(0.5, MANY as i64)).await;
    let expected_count = match granted.channel_splits.as_slice() {
        [] => MANY,
        [channel] => {
            assert_eq!(channel.transform_id, SOURCE_ID);
            assert_eq!(
                channel.last_primary_element + 1,
                channel.first_residual_element
            );
            assert!(
                (1..=MANY as i64).contains(&channel.first_residual_element),
                "unexpected split boundary: {channel:?}"
            );
            channel.first_residual_element as usize
        }
        other => panic!("expected at most one channel split, got {other:?}"),
    };

    let (pb, seen, emitted) = finish(fixture).await;
    let expected = names(0..expected_count);
    assert_eq!(seen, expected);
    assert_eq!(emitted, expected);
    assert_eq!(final_read_index(&pb), expected_count as i64);
}
