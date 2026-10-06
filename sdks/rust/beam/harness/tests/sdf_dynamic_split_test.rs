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

//! Runner-initiated splits of an in-flight Splittable DoFn restriction.
//!
//! Each test holds one SDF element after it claims positions 0..20 of [0, 100), sends a
//! split, then lets it finish. The assertions pin the primary/residual restrictions returned
//! to the runner *and* the positions the DoFn emitted. One test gates the second of two
//! elements, to split at a nonzero channel index.

#![expect(clippy::unwrap_used, reason = "integration test helpers")]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

use beam::coders::{
    Coder, Context, DefaultCoder, StringUtf8Coder, VarIntCoder, WindowedValue, WindowedValueCoder,
};
use beam::internals::{BundleHandler, TransformFn};
use beam::internals::{HandlerContext, SdfDynamicSplitter};
use beam::metrics::{
    LABEL_PTRANSFORM, TYPE_PROGRESS, URN_DATA_CHANNEL_READ_INDEX, URN_WORK_COMPLETED,
    URN_WORK_REMAINING,
};
use beam::transforms::ProcessContext;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, ProcessContinuation, SplittableDoFn};
use harness::bundle_processor::BundleProcessor;
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::process_bundle_split_request::DesiredSplit;
use model::fn_execution::process_bundle_split_response::ChannelSplit;
use model::fn_execution::{
    BundleApplication, DelayedBundleApplication, Elements, InstructionRequest,
    ProcessBundleDescriptor, ProcessBundleProgressRequest, ProcessBundleRequest,
    ProcessBundleResponse, ProcessBundleSplitRequest, ProcessBundleSplitResponse, RegisterRequest,
    instruction_request::Request, instruction_response::Response,
};
use model::pipeline::MonitoringInfo;

mod common;
use common::{DescriptorBuilder, Gate, SINK_ID, SOURCE_ID, STAGE_ID, within};

/// The element value the splitter reports in its split applications.
const SPLIT_VALUE: &str = "element_input";
/// Positions claimed before the split request is sent.
const CLAIMED_BEFORE_SPLIT: i64 = 20;
const RESTRICTION: OffsetRange = OffsetRange { start: 0, end: 100 };
const TIMESTAMP: i64 = 1_700_000_000;

#[derive(Clone)]
struct TestSplittableFn;

impl SplittableDoFn for TestSplittableFn {
    type In = String;
    type Out = i64;
    type Restriction = OffsetRange;
    type Position = i64;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, _element: &Self::In) -> Self::Restriction {
        RESTRICTION
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn restriction_size(&self, _element: &Self::In, restriction: &Self::Restriction) -> f64 {
        restriction.size()
    }

    fn process_element(
        &self,
        _element: Self::In,
        _tracker: &Self::Tracker,
        _ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result<ProcessContinuation> {
        // `GatedSdfHandler` drives the tracker; this only sizes restrictions for the splitter.
        Err("not invoked by these tests".into())
    }
}

/// Runs an SDF element behind gates and emits each claimed position as a UTF-8 string, so the
/// test sees where the primary stopped. Only element `gated_element` (0-based) pauses; copies
/// share the gates and the element count.
#[derive(Clone)]
struct GatedSdfHandler {
    func: Arc<TestSplittableFn>,
    gate: Arc<Gate>,
    started: Arc<Gate>,
    elements_seen: Arc<AtomicUsize>,
    gated_element: usize,
}

fn emit_position(ctx: &mut HandlerContext<'_>, pos: i64) -> Result<(), String> {
    let mut bytes = Vec::new();
    StringUtf8Coder
        .encode(&pos.to_string(), &mut bytes, Context::Nested)
        .map_err(|e| e.to_string())?;
    let header = ctx.header().clone();
    ctx.sink.push_windowed(&header, bytes)
}

impl BundleHandler for GatedSdfHandler {
    fn instantiate(&self) -> beam::internals::HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, _element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let tracker = Arc::new(self.func.create_tracker(&RESTRICTION));

        if self.elements_seen.fetch_add(1, Ordering::SeqCst) != self.gated_element {
            let mut pos = RESTRICTION.start;
            while tracker.try_claim(&pos) {
                emit_position(ctx, pos)?;
                pos += 1;
            }
            return Ok(());
        }

        for pos in 0..CLAIMED_BEFORE_SPLIT {
            assert!(tracker.try_claim(&pos));
            emit_position(ctx, pos)?;
        }

        let splitter = Arc::new(SdfDynamicSplitter::new(
            ctx.transform_id().to_string(),
            self.func.clone(),
            tracker.clone(),
            SPLIT_VALUE.to_string(),
            ctx.header().clone(),
        ));
        let _guard = ctx.register_dynamic_split(splitter);

        self.started.open();
        self.gate.wait("the test to send its split");

        // Finish whatever the split left in the primary.
        let mut pos = CLAIMED_BEFORE_SPLIT;
        while tracker.try_claim(&pos) {
            emit_position(ctx, pos)?;
            pos += 1;
        }
        Ok(())
    }
}

fn create_sdf_test_descriptor(descriptor_id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(descriptor_id)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build()
}

/// What one gated SDF bundle produced.
struct Outcome {
    split: ProcessBundleSplitResponse,
    bundle: ProcessBundleResponse,
    /// Positions the DoFn emitted, decoded from the sink.
    emitted: Vec<i64>,
    /// The encoded windowed-value header of the input element.
    header: Vec<u8>,
    /// The monitoring infos of a progress report while the gated element was paused.
    progress: Vec<MonitoringInfo>,
}

/// Runs one SDF element, sends `desired` for `split_target` once 20 positions are
/// claimed, then lets the element run to completion.
async fn run_with_split(split_target: &str, desired: DesiredSplit) -> Outcome {
    run_gated_on(1, 0, split_target, desired).await
}

/// Like [`run_with_split`], but runs `elements` elements and gates only `gated_element`.
async fn run_gated_on(
    elements: usize,
    gated_element: usize,
    split_target: &str,
    desired: DesiredSplit,
) -> Outcome {
    let (data_out_tx, mut data_out_rx) = mpsc::channel::<Elements>(256);
    let data_manager = DataManager::new(data_out_tx);

    let gate = Gate::new();
    // Keeps a failed assertion from leaving the handler parked.
    let _release_on_drop = gate.open_on_drop();
    let started = Gate::new();
    let handler: TransformFn = Arc::new(GatedSdfHandler {
        func: Arc::new(TestSplittableFn),
        gate: Arc::clone(&gate),
        started: Arc::clone(&started),
        elements_seen: Arc::new(AtomicUsize::new(0)),
        gated_element,
    });
    let handlers = HashMap::from([(STAGE_ID.to_string(), handler)]);
    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor);

    let descriptor_id = "desc_sdf_split_test";
    within(
        "registration",
        control_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_reg".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![create_sdf_test_descriptor(descriptor_id)],
            })),
        }),
    )
    .await;

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut element = Vec::new();
    coder
        .encode(
            &WindowedValue::global("hello".to_string(), TIMESTAMP),
            &mut element,
            Context::Nested,
        )
        .unwrap();
    let mut value_bytes = Vec::new();
    StringUtf8Coder
        .encode(&"hello".to_string(), &mut value_bytes, Context::Nested)
        .unwrap();
    assert!(element.ends_with(&value_bytes));
    let header = element[..element.len() - value_bytes.len()].to_vec();
    let payload = element.repeat(elements);

    let instruction_id = "inst_sdf_split_1";
    within(
        "the inbound data to be accepted",
        data_manager.handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: instruction_id.to_string(),
                transform_id: SOURCE_ID.to_string(),
                data: payload,
                is_last: true,
            }],
            timers: Vec::new(),
        }),
    )
    .await;

    let bundle_handle = tokio::spawn({
        let control_client = control_client.clone();
        async move {
            control_client
                .handle_instruction(InstructionRequest {
                    instruction_id: instruction_id.to_string(),
                    request: Some(Request::ProcessBundle(ProcessBundleRequest {
                        process_bundle_descriptor_id: descriptor_id.to_string(),
                        data_stream_id: "data_stream_sdf".to_string(),
                        ..Default::default()
                    })),
                })
                .await
        }
    });

    within(
        "the gated element to pause mid-restriction",
        tokio::task::spawn_blocking(move || {
            started.wait("the gated element to pause mid-restriction");
        }),
    )
    .await
    .unwrap();

    let progress_resp = within(
        "the progress response",
        control_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_progress_query".to_string(),
            request: Some(Request::ProcessBundleProgress(
                ProcessBundleProgressRequest {
                    instruction_id: instruction_id.to_string(),
                },
            )),
        }),
    )
    .await;
    let progress = match progress_resp.response {
        Some(Response::ProcessBundleProgress(progress)) => progress.monitoring_infos,
        other => panic!("Expected ProcessBundleProgress response, got {other:?}"),
    };

    let split_resp = within(
        "the split response",
        control_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_split_query".to_string(),
            request: Some(Request::ProcessBundleSplit(ProcessBundleSplitRequest {
                instruction_id: instruction_id.to_string(),
                desired_splits: HashMap::from([(split_target.to_string(), desired)]),
            })),
        }),
    )
    .await;
    let split = match split_resp.response {
        Some(Response::ProcessBundleSplit(split)) => split,
        other => panic!("Expected ProcessBundleSplit response, got {other:?}"),
    };

    gate.open();

    let bundle_resp = within("the bundle response", bundle_handle).await.unwrap();
    assert!(
        bundle_resp.error.is_empty(),
        "Bundle failed: {}",
        bundle_resp.error
    );
    let Some(Response::ProcessBundle(bundle)) = bundle_resp.response else {
        panic!("Expected ProcessBundle response");
    };

    let mut sink_bytes = Vec::new();
    while let Ok(elements) = data_out_rx.try_recv() {
        for data in elements.data {
            assert_eq!(data.transform_id, SINK_ID);
            sink_bytes.extend_from_slice(&data.data);
        }
    }
    let mut cursor = Cursor::new(sink_bytes.as_slice());
    let mut emitted = Vec::new();
    while (cursor.position() as usize) < sink_bytes.len() {
        let wv = coder.decode(&mut cursor, Context::Nested).unwrap();
        assert_eq!(wv.timestamp_millis, TIMESTAMP);
        emitted.push(wv.value.parse().unwrap());
    }

    Outcome {
        split,
        bundle,
        emitted,
        header,
        progress,
    }
}

/// The `BundleApplication` the splitter must produce for `restriction`.
fn application(header: &[u8], restriction: OffsetRange) -> BundleApplication {
    let mut element = header.to_vec();
    element.extend(
        ((SPLIT_VALUE.to_string(), restriction), restriction.size())
            .encode()
            .unwrap(),
    );
    BundleApplication {
        transform_id: STAGE_ID.to_string(),
        input_id: "in".to_string(),
        element,
        output_watermarks: HashMap::new(),
        is_bounded: model::pipeline::is_bounded::Enum::Bounded as i32,
    }
}

fn residual(header: &[u8], restriction: OffsetRange) -> DelayedBundleApplication {
    DelayedBundleApplication {
        application: Some(application(header, restriction)),
        requested_time_delay: None,
    }
}

fn final_read_index(bundle: &ProcessBundleResponse) -> i64 {
    let info = bundle
        .monitoring_infos
        .iter()
        .find(|info| info.urn == URN_DATA_CHANNEL_READ_INDEX)
        .expect("data_channel_read_index in the final response");
    VarIntCoder::decode_varint(&mut Cursor::new(&info.payload)).unwrap()
}

/// The single value of a `beam:metrics:progress:v1` payload for `urn`.
fn progress_value(infos: &[MonitoringInfo], urn: &str) -> f64 {
    let info = infos
        .iter()
        .find(|info| {
            info.urn == urn && info.labels.get(LABEL_PTRANSFORM) == Some(&STAGE_ID.to_string())
        })
        .unwrap_or_else(|| panic!("no {urn} for the SDF stage in {infos:?}"));
    assert_eq!(info.r#type, TYPE_PROGRESS);
    assert_eq!(
        info.payload[..4],
        1_i32.to_be_bytes(),
        "an iterable of one double"
    );
    f64::from_be_bytes(info.payload[4..12].try_into().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progress_reports_the_work_left_in_the_sdf_element() {
    let outcome = run_with_split(
        STAGE_ID,
        DesiredSplit {
            fraction_of_remainder: 0.5,
            allowed_split_points: vec![],
            estimated_input_elements: 0,
        },
    )
    .await;
    // The element paused after it claimed positions 0 to 19 of [0, 100).
    let completed = progress_value(&outcome.progress, URN_WORK_COMPLETED);
    let remaining = progress_value(&outcome.progress, URN_WORK_REMAINING);
    assert_eq!((completed, remaining), (20.0, 80.0));
    assert!(
        !outcome
            .bundle
            .monitoring_infos
            .iter()
            .any(|info| info.urn == URN_WORK_REMAINING),
        "the final response has no element in progress"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_bundle_processor_direct_sdf_dynamic_split() {
    // Split addressed to the SDF transform itself, after it claimed position 19.
    let cases = [
        // 81 positions remain; keeping half rounds up to 41, so the split is at 19 + 41.
        (0.5, 60),
        // A checkpoint keeps only what was claimed.
        (0.0, 20),
    ];
    for (fraction, split_at) in cases {
        let outcome = run_with_split(
            STAGE_ID,
            DesiredSplit {
                fraction_of_remainder: fraction,
                allowed_split_points: vec![],
                estimated_input_elements: 0,
            },
        )
        .await;

        assert_eq!(
            outcome.split,
            ProcessBundleSplitResponse {
                primary_roots: vec![application(&outcome.header, OffsetRange::new(0, split_at))],
                residual_roots: vec![residual(&outcome.header, OffsetRange::new(split_at, 100))],
                channel_splits: vec![],
            },
            "fraction {fraction}"
        );
        assert_eq!(
            outcome.emitted,
            (0..split_at).collect::<Vec<_>>(),
            "the primary must stop at the split point and emit nothing from the residual"
        );
        assert!(outcome.bundle.residual_roots.is_empty());
        assert_eq!(final_read_index(&outcome.bundle), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_in_flight_sdf_split_respects_allowed_split_points() {
    // The runner allows a split only before element 0, so the element in flight cannot be
    // divided and no split is made: the whole restriction is processed.
    let outcome = run_with_split(
        SOURCE_ID,
        DesiredSplit {
            fraction_of_remainder: 0.5,
            allowed_split_points: vec![0],
            estimated_input_elements: 1,
        },
    )
    .await;

    assert_eq!(outcome.split, ProcessBundleSplitResponse::default());
    assert_eq!(outcome.emitted, (0..100).collect::<Vec<_>>());
    assert_eq!(final_read_index(&outcome.bundle), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_in_flight_sdf_split_with_an_understated_estimate_still_divides_the_element() {
    // With element 0 in flight, the SDK assumes at least 1 element, giving [0, 60) / [60, 100).
    let outcome = run_with_split(
        SOURCE_ID,
        DesiredSplit {
            fraction_of_remainder: 0.5,
            allowed_split_points: vec![],
            estimated_input_elements: 0,
        },
    )
    .await;

    assert_eq!(
        outcome.split,
        ProcessBundleSplitResponse {
            primary_roots: vec![application(&outcome.header, OffsetRange::new(0, 60))],
            residual_roots: vec![residual(&outcome.header, OffsetRange::new(60, 100))],
            channel_splits: vec![ChannelSplit {
                transform_id: SOURCE_ID.to_string(),
                last_primary_element: -1,
                first_residual_element: 1,
            }],
        }
    );
    assert_eq!(outcome.emitted, (0..60).collect::<Vec<_>>());
    assert_eq!(final_read_index(&outcome.bundle), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_in_flight_sdf_split_of_a_later_element_divides_only_its_remainder() {
    // Element 0 is done and element 1 is 20% done, so half the remainder ends at 60.
    let outcome = run_gated_on(
        2,
        1,
        SOURCE_ID,
        DesiredSplit {
            fraction_of_remainder: 0.5,
            allowed_split_points: vec![],
            estimated_input_elements: 2,
        },
    )
    .await;

    assert_eq!(
        outcome.split,
        ProcessBundleSplitResponse {
            primary_roots: vec![application(&outcome.header, OffsetRange::new(0, 60))],
            residual_roots: vec![residual(&outcome.header, OffsetRange::new(60, 100))],
            channel_splits: vec![ChannelSplit {
                transform_id: SOURCE_ID.to_string(),
                last_primary_element: 0,
                first_residual_element: 2,
            }],
        }
    );
    assert_eq!(
        outcome.emitted,
        (0..100).chain(0..60).collect::<Vec<_>>(),
        "element 0 runs whole; element 1 stops at the split"
    );
    assert_eq!(final_read_index(&outcome.bundle), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_in_flight_sdf_split_is_allowed_when_points_on_both_sides_are_allowed() {
    // Points 0 and 1 bracket the element in flight, so it can be divided.
    let outcome = run_with_split(
        SOURCE_ID,
        DesiredSplit {
            fraction_of_remainder: 0.5,
            allowed_split_points: vec![0, 1],
            estimated_input_elements: 1,
        },
    )
    .await;

    assert_eq!(
        outcome.split,
        ProcessBundleSplitResponse {
            primary_roots: vec![application(&outcome.header, OffsetRange::new(0, 60))],
            residual_roots: vec![residual(&outcome.header, OffsetRange::new(60, 100))],
            channel_splits: vec![ChannelSplit {
                transform_id: SOURCE_ID.to_string(),
                last_primary_element: -1,
                first_residual_element: 1,
            }],
        }
    );
    assert_eq!(outcome.emitted, (0..60).collect::<Vec<_>>());
    assert_eq!(final_read_index(&outcome.bundle), 1);
}
