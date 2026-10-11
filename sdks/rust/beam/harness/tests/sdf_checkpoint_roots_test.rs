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

//! The self-checkpoint of a splittable DoFn in a descriptor with the transform ids and the
//! input and output names of a runner such as Dataflow. The residual root names the runner's
//! transform, main input and outputs, and holds the watermark of the tracker.

use std::collections::HashMap;
use std::sync::Arc;

use beam::Instant;
use beam::coders::{
    Coder, Context, DefaultCoder, StringUtf8Coder, URN_DOUBLE, URN_GLOBAL_WINDOW, URN_KV,
    URN_STRING_UTF8, URN_VARINT, URN_WINDOWED_VALUE, WindowedValue, WindowedValueCoder,
};
use beam::internals::{SplittableDoFnHandler, TransformFn};
use beam::pipeline::URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS;
use beam::transforms::ProcessContext;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    WatermarkedTracker,
};
use beam::values::IsBounded;
use beam::windowing::ManualWatermarkEstimator;
use harness::bundle_processor::{URN_DATA_SINK, URN_DATA_SOURCE};
use model::fn_execution::{
    BundleApplication, DelayedBundleApplication, ProcessBundleDescriptor, RemoteGrpcPort,
};
use model::pipeline::{Coder as CoderProto, FunctionSpec, PTransform, ParDoPayload};
use prost::Message;

mod common;
use common::run_bundle_from;

/// The ids and names that the runner gives. None of them is a name of the SDK.
const SOURCE: &str = "fn/read/ref_PCollection_PCollection_12:0";
const READ: &str = "ref_AppliedPTransform_ReadPartitions-ProcessSizedElementsAndRestrictions_17";
const SINK: &str = "fn/write/ref_PCollection_PCollection_13:0";
const MAIN_INPUT: &str = "i0";
const OUTPUT: &str = "out0";
/// The handler key in the `do_fn` payload.
const HANDLER_KEY: &str = "ReadPartitions";

/// The timestamp of the input element, as after an impulse.
const INPUT_TIMESTAMP: i64 = -9_223_372_036_854_775;
/// The timestamp of the record that the reader emits, and its watermark.
const RECORD_TIMESTAMP: i64 = 1_760_000_000_123;
const START: i64 = 40;

/// Reads one position for each call, emits it at [`RECORD_TIMESTAMP`], moves the watermark
/// to that timestamp and checkpoints.
#[derive(Clone)]
struct OnePositionReader;

type Tracker = WatermarkedTracker<OffsetRangeTracker, ManualWatermarkEstimator>;

impl SplittableDoFn for OnePositionReader {
    type In = String;
    type Out = String;
    type Restriction = OffsetRange;
    type Tracker = Tracker;

    fn boundedness(&self) -> IsBounded {
        IsBounded::Unbounded
    }

    fn initial_restriction(&self, _element: &String) -> beam::Result<OffsetRange> {
        Ok(OffsetRange::new(START, i64::MAX))
    }

    fn restriction_size(&self, _element: &String, _restriction: &OffsetRange) -> f64 {
        7.0
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> Tracker {
        WatermarkedTracker::new(
            OffsetRangeTracker::new(*restriction),
            ManualWatermarkEstimator::default(),
        )
        // An unbounded tracker lets `check_done` skip the unclaimed rest of the restriction.
        .with_boundedness(IsBounded::Unbounded)
    }

    fn process_element(
        &mut self,
        element: String,
        tracker: &Tracker,
        ctx: &mut ProcessContext<'_, String>,
    ) -> beam::Result<ProcessContinuation> {
        let position = tracker.current_restriction().start;
        if !tracker.try_claim(&position) {
            return Ok(ProcessContinuation::Stop);
        }
        let timestamp = Instant::from_millis(RECORD_TIMESTAMP);
        ctx.output(format!("{element}@{position}"))
            .at(timestamp)
            .emit()?;
        tracker.estimator.set_watermark(timestamp);
        Ok(ProcessContinuation::Resume)
    }
}

fn coder(urn: &str, components: &[&str]) -> CoderProto {
    CoderProto {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: components.iter().map(ToString::to_string).collect(),
    }
}

fn data_port(urn: &str, coder_id: &str) -> Option<FunctionSpec> {
    let port = RemoteGrpcPort {
        api_service_descriptor: None,
        coder_id: coder_id.to_string(),
    };
    Some(FunctionSpec {
        urn: urn.to_string(),
        payload: port.encode_to_vec(),
    })
}

fn transform(
    spec: Option<FunctionSpec>,
    inputs: &[(&str, &str)],
    outputs: &[(&str, &str)],
) -> PTransform {
    let names = |ports: &[(&str, &str)]| {
        ports
            .iter()
            .map(|(name, pcollection)| (name.to_string(), pcollection.to_string()))
            .collect()
    };
    PTransform {
        spec,
        inputs: names(inputs),
        outputs: names(outputs),
        ..Default::default()
    }
}

/// The source sends `KV<KV<element, restriction>, size>` to the splittable transform, which
/// writes strings to the sink.
fn descriptor() -> ProcessBundleDescriptor {
    let payload = ParDoPayload {
        do_fn: Some(FunctionSpec {
            urn: "beam:dofn:rust:v1".to_string(),
            payload: HANDLER_KEY.as_bytes().to_vec(),
        }),
        restriction_coder_id: "c_restriction".to_string(),
        ..Default::default()
    };
    let read = transform(
        Some(FunctionSpec {
            urn: URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS.to_string(),
            payload: payload.encode_to_vec(),
        }),
        &[(MAIN_INPUT, "pc_sized")],
        &[(OUTPUT, "pc_out")],
    );
    ProcessBundleDescriptor {
        id: "dataflow_like".to_string(),
        transforms: HashMap::from([
            (
                SOURCE.to_string(),
                transform(
                    data_port(URN_DATA_SOURCE, "c_wv_sized"),
                    &[],
                    &[("out", "pc_sized")],
                ),
            ),
            (READ.to_string(), read),
            (
                SINK.to_string(),
                transform(
                    data_port(URN_DATA_SINK, "c_wv_string"),
                    &[("in", "pc_out")],
                    &[],
                ),
            ),
        ]),
        coders: HashMap::from([
            ("c_string".to_string(), coder(URN_STRING_UTF8, &[])),
            ("c_varint".to_string(), coder(URN_VARINT, &[])),
            ("c_double".to_string(), coder(URN_DOUBLE, &[])),
            (
                "c_range".to_string(),
                coder(URN_KV, &["c_varint", "c_varint"]),
            ),
            ("c_bytes".to_string(), coder(beam::coders::URN_BYTES, &[])),
            // The SDK keeps the watermark estimator state with each restriction.
            (
                "c_restriction".to_string(),
                coder(URN_KV, &["c_range", "c_bytes"]),
            ),
            (
                "c_pair".to_string(),
                coder(URN_KV, &["c_string", "c_restriction"]),
            ),
            (
                "c_sized".to_string(),
                coder(URN_KV, &["c_pair", "c_double"]),
            ),
            ("c_gw".to_string(), coder(URN_GLOBAL_WINDOW, &[])),
            (
                "c_wv_sized".to_string(),
                coder(URN_WINDOWED_VALUE, &["c_sized", "c_gw"]),
            ),
            (
                "c_wv_string".to_string(),
                coder(URN_WINDOWED_VALUE, &["c_string", "c_gw"]),
            ),
        ]),
        ..Default::default()
    }
}

/// The encoded windowed value header of an element at [`INPUT_TIMESTAMP`] in the global
/// window.
fn header() -> Vec<u8> {
    let value = "partition".to_string();
    let mut element = Vec::new();
    WindowedValueCoder::new(StringUtf8Coder)
        .encode(
            &WindowedValue::global(value.clone(), INPUT_TIMESTAMP),
            &mut element,
            Context::Nested,
        )
        .expect("the windowed value encodes");
    let mut value_bytes = Vec::new();
    StringUtf8Coder
        .encode(&value, &mut value_bytes, Context::Nested)
        .expect("the value encodes");
    element[..element.len() - value_bytes.len()].to_vec()
}

/// A sized restriction from `start`, with the encoded watermark estimator `state`: empty
/// for a restriction that no estimator processed yet.
fn sized(start: i64, size: f64, state: Vec<u8>) -> Vec<u8> {
    (
        (
            "partition".to_string(),
            (OffsetRange::new(start, i64::MAX), state),
        ),
        size,
    )
        .encode()
        .expect("the sized restriction encodes")
}

#[tokio::test]
async fn a_checkpoint_names_the_runner_transform_input_and_outputs_and_holds_the_watermark() {
    let handler: TransformFn = Arc::new(SplittableDoFnHandler::new(OnePositionReader));
    let run = run_bundle_from(
        SOURCE,
        HashMap::from([(HANDLER_KEY.to_string(), handler)]),
        descriptor(),
        vec![[header(), sized(START, 7.0, Vec::new())].concat()],
    )
    .await;

    let watermark_state = Instant::from_millis(RECORD_TIMESTAMP)
        .encode()
        .expect("the watermark encodes");
    let expected = DelayedBundleApplication {
        application: Some(BundleApplication {
            transform_id: READ.to_string(),
            input_id: MAIN_INPUT.to_string(),
            // The residual keeps the watermark that the estimator reached.
            element: [header(), sized(START + 1, 7.0, watermark_state)].concat(),
            output_watermarks: HashMap::from([(
                OUTPUT.to_string(),
                Instant::from_millis(RECORD_TIMESTAMP).into(),
            )]),
            ..BundleApplication::default()
        }),
        requested_time_delay: None,
    };
    assert_eq!(run.bundle_response().residual_roots, vec![expected]);

    let mut sink = run.sink_bytes[SINK].as_slice();
    let records: Vec<(String, i64)> = std::iter::from_fn(|| {
        (!sink.is_empty()).then(|| {
            WindowedValueCoder::new(StringUtf8Coder)
                .decode(&mut sink, Context::Nested)
                .expect("the sink element decodes")
        })
    })
    .map(|record| (record.value, record.timestamp_millis))
    .collect();
    assert_eq!(records, [("partition@40".to_string(), RECORD_TIMESTAMP)]);
}
