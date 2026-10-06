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

//! Integration tests for the `GenerateSequence` splittable `DoFn` transform.
#![expect(clippy::unwrap_used, reason = "test helpers")]

use beam::transforms::display_data::HasDisplayData;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::internals::TransformFn;
use beam::internals::{HandlerContext, ResidualApplication, ResidualCollector};
use beam::pipeline::Pipeline;
use beam::pipeline::constants::{
    URN_REQUIREMENT_SPLITTABLE_DOFN, URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
    URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS,
};
use beam::prelude::*;
use beam::transforms::display_data::DisplayDataBuilder;
use beam::transforms::sdf::OffsetRange;

#[test]
fn test_generate_sequence_bounded_config() {
    let seq = GenerateSequence::new("GenerateSequence", 10)
        .with_end(100)
        .with_split_size(500);
    assert_eq!(seq.start(), 10);
    assert_eq!(seq.end(), Some(100));
    assert!(!seq.is_unbounded());
    assert!(seq.rate().is_none());
    assert!(seq.max_read_time().is_none());

    let mut builder = DisplayDataBuilder::new();
    seq.populate_display_data(&mut builder);
    let items = builder.build();
    assert!(items.iter().any(|i| i.key == "start" && i.value == "10"));
    assert!(items.iter().any(|i| i.key == "end" && i.value == "100"));
    assert!(
        items
            .iter()
            .any(|i| i.key == "split_size" && i.value == "500")
    );
}

#[test]
fn test_generate_sequence_unbounded_config_with_rate() {
    let seq = GenerateSequence::new("GenerateSequence", 0)
        .with_rate(100, Duration::from_secs(1))
        .with_max_read_time(Duration::from_secs(60));

    assert_eq!(seq.start(), 0);
    assert_eq!(seq.end(), None);
    assert!(seq.is_unbounded());
    assert_eq!(seq.rate(), Some((100, Duration::from_secs(1))));
    assert_eq!(seq.max_read_time(), Some(Duration::from_secs(60)));

    let mut builder = DisplayDataBuilder::new();
    seq.populate_display_data(&mut builder);
    let items = builder.build();
    assert!(items.iter().any(|i| i.key == "start" && i.value == "0"));
    assert!(
        items
            .iter()
            .any(|i| i.key == "end" && i.value == "unbounded")
    );
    assert!(
        items
            .iter()
            .any(|i| i.key == "rate_elements" && i.value == "100")
    );
    assert!(
        items
            .iter()
            .any(|i| i.key == "rate_period_ms" && i.value == "1000")
    );
    assert!(
        items
            .iter()
            .any(|i| i.key == "max_read_time_ms" && i.value == "60000")
    );
}

/// Returns the `Generate` SDF handler of the single `GenerateSequence` in `p`.
fn generate_handler(p: &Pipeline) -> (String, TransformFn) {
    p.transform_handlers()
        .into_iter()
        .find(|(k, _)| k.contains("Generate"))
        .expect("Generate handler must be registered")
}

/// Runs `ProcessSizedElementsAndRestrictions` on `restriction`.
///
/// Returns emitted values and residual applications.
fn process_sized(p: &Pipeline, restriction: OffsetRange) -> (Vec<i64>, Vec<ResidualApplication>) {
    let (handler_id, handler) = generate_handler(p);
    let stage = handler
        .stage_handler(URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS)
        .expect("ProcessSizedElements stage handler must exist");
    let residuals = Arc::new(ResidualCollector::new());
    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink)
        .with_transform_id(&handler_id)
        .with_residual_collector(Some(&residuals));
    let element = ((Vec::<u8>::new(), restriction), 1.0).encode().unwrap();
    stage
        .instantiate()
        .process(&element, &mut ctx)
        .expect("Process must succeed");
    let values = sink.iter().map(|b| i64::decode(b).unwrap()).collect();
    (values, residuals.drain())
}

#[test]
fn test_generate_sequence_builder_to_converts_to_bounded() {
    // A bounded sequence splits into initial chunks configured by `with_split_size`.
    let p = Pipeline::new();
    let pcoll = p.apply(
        GenerateSequence::new("GenerateSequence", 5)
            .with_end(25)
            .with_split_size(8),
    );
    assert_eq!(
        p.lock().components.pcollections[pcoll.id()].is_bounded,
        model::pipeline::is_bounded::Enum::Bounded as i32
    );

    let (_, handler) = generate_handler(&p);
    let stage = handler
        .stage_handler(URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS)
        .expect("SplitAndSize stage handler must exist");
    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink);
    let element = (Vec::<u8>::new(), OffsetRange::new(5, 25))
        .encode()
        .unwrap();
    stage.instantiate().process(&element, &mut ctx).unwrap();

    let splits: Vec<(i64, i64, f64)> = sink
        .iter()
        .map(|b| {
            let ((_, r), size) = <((Vec<u8>, OffsetRange), f64)>::decode(b).unwrap();
            (r.start, r.end, size)
        })
        .collect();
    assert_eq!(splits, [(5, 13, 8.0), (13, 21, 8.0), (21, 25, 4.0)]);
}

#[test]
fn test_generate_sequence_period_shorthand() {
    // `with_period(d)` emits one element per `d`. The SDF defers the second element by about `d`.
    let p = Pipeline::new();
    let _ = p.apply(
        GenerateSequence::new("GenerateSequence", 0)
            .with_end(10)
            .with_period(Duration::from_secs(4)),
    );
    let (values, residuals) = process_sized(&p, OffsetRange::new(0, 10));
    assert_eq!(values, [0]);
    assert_eq!(residuals.len(), 1);
    let delay = residuals[0].delay.expect("resume delay");
    assert!(
        delay > Duration::from_secs(2) && delay <= Duration::from_secs(4),
        "{delay:?}"
    );
}

#[test]
fn test_generate_sequence_bounded_pipeline_expansion() {
    let p = Pipeline::new();
    let pcoll = p.apply(GenerateSequence::new("GenerateSequence", 1).with_end(100));

    let is_bounded = p.lock().components.pcollections[pcoll.id()].is_bounded;
    assert_eq!(
        is_bounded,
        model::pipeline::is_bounded::Enum::Bounded as i32
    );

    let proto = p.to_proto();
    assert!(
        proto
            .requirements
            .contains(&URN_REQUIREMENT_SPLITTABLE_DOFN.to_string()),
        "Pipeline must require splittable DoFn"
    );
}

#[test]
fn test_generate_sequence_unbounded_pipeline_expansion() {
    let p = Pipeline::new();
    let pcoll =
        p.apply(GenerateSequence::new("GenerateSequence", 0).with_rate(10, Duration::from_secs(1)));

    let is_bounded = p.lock().components.pcollections[pcoll.id()].is_bounded;
    assert_eq!(
        is_bounded,
        model::pipeline::is_bounded::Enum::Unbounded as i32
    );

    let proto = p.to_proto();
    assert!(
        proto
            .requirements
            .contains(&URN_REQUIREMENT_SPLITTABLE_DOFN.to_string()),
        "Pipeline must require splittable DoFn"
    );
}

#[test]
fn test_generate_sequence_bounded_execution_via_handler() {
    let p = Pipeline::new();
    let _pcoll = p.apply(GenerateSequence::new("GenerateSequence", 10).with_end(20));

    let handler = p
        .transform_handlers()
        .into_iter()
        .find_map(|(k, v)| {
            if k.contains("Generate") {
                Some(v)
            } else {
                None
            }
        })
        .expect("Generate handler must be registered");

    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink);

    let impulse_element = Vec::<u8>::new().encode().unwrap();
    handler
        .instantiate()
        .process(&impulse_element, &mut ctx)
        .expect("Handler process must succeed");

    let values: Vec<i64> = sink
        .iter()
        .map(|bytes| i64::decode(bytes).expect("Output must decode as i64"))
        .collect();

    assert_eq!(values, (10..20).collect::<Vec<i64>>());
}

#[test]
fn test_generate_sequence_rate_limiting_yields_residual_with_delay() {
    // Configure one element per 10 seconds.
    // The first element emits immediately.
    // The second element yields a resume delay near 10 seconds.
    let p = Pipeline::new();
    let _pcoll = p.apply(
        GenerateSequence::new("GenerateSequence", 100)
            .with_end(110)
            .with_rate(1, Duration::from_secs(10)),
    );

    let (handler_id, direct_handler) = p
        .transform_handlers()
        .into_iter()
        .find(|(k, _)| k.contains("Generate"))
        .expect("Generate handler must be registered");

    let stage_handler = direct_handler
        .stage_handler(URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS)
        .expect("ProcessSizedElements stage handler must exist");

    let residual_collector = Arc::new(ResidualCollector::new());
    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink)
        .with_transform_id(&handler_id)
        .with_residual_collector(Some(&residual_collector));

    // Sized element: ((impulse_payload, restriction), size).
    let restriction = OffsetRange::new(100, 110);
    let sized_element = ((Vec::<u8>::new(), restriction), 1.0)
        .encode()
        .expect("Encoding sized element must succeed");

    stage_handler
        .instantiate()
        .process(&sized_element, &mut ctx)
        .expect("Process must succeed");

    assert_eq!(sink.len(), 1);
    let first_val = i64::decode(&sink[0]).unwrap();
    assert_eq!(first_val, 100);

    let residuals = residual_collector.drain();
    assert_eq!(residuals.len(), 1);
    let r = &residuals[0];
    assert!(
        r.delay.is_some(),
        "Residual must contain a requested resume delay"
    );
    let delay = r.delay.unwrap();
    // Verify requested resume delay is about 10 seconds.
    assert!(
        delay >= Duration::from_secs(8) && delay <= Duration::from_secs(11),
        "Expected delay near 10s, got: {delay:?}"
    );
    assert!(r.output_watermarks.contains_key("out"));

    // Residual restriction begins at the next offset.
    let ((_, residual_range), _) =
        <((Vec<u8>, OffsetRange), f64)>::decode(&r.element).expect("Residual must decode");
    assert_eq!(residual_range.start, 101);
    assert_eq!(residual_range.end, 110);
}

#[test]
fn test_generate_sequence_max_read_time_stops() {
    let p = Pipeline::new();
    let _pcoll = p.apply(
        GenerateSequence::new("GenerateSequence", 0)
            .with_rate(1, Duration::from_secs(5))
            .with_max_read_time(Duration::from_millis(0)),
    );

    let direct_handler = p
        .transform_handlers()
        .into_iter()
        .find_map(|(k, v)| {
            if k.contains("Generate") {
                Some(v)
            } else {
                None
            }
        })
        .expect("Generate handler must be registered");

    let stage_handler = direct_handler
        .stage_handler(URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS)
        .expect("ProcessSizedElements stage handler must exist");

    let residual_collector = Arc::new(ResidualCollector::new());
    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink).with_residual_collector(Some(&residual_collector));

    let restriction = OffsetRange::new(0, i64::MAX);
    let sized_element = ((Vec::<u8>::new(), restriction), 1.0)
        .encode()
        .expect("Encoding sized element must succeed");

    stage_handler
        .instantiate()
        .process(&sized_element, &mut ctx)
        .expect("Process must succeed");

    // With zero max read time, execution stops immediately without emissions or residuals.
    let residuals = residual_collector.drain();
    assert_eq!(residuals.len(), 0);
    assert!(
        sink.is_empty(),
        "no element may be emitted after the deadline"
    );
}

#[test]
fn generate_sequence_before_max_read_time_keeps_going() {
    // When the deadline is unexpired, emit the current element and reschedule remaining range.
    let p = Pipeline::new();
    let _ = p.apply(
        GenerateSequence::new("GenerateSequence", 0)
            .with_rate(1, Duration::from_secs(5))
            .with_max_read_time(Duration::from_secs(3600)),
    );
    let (values, residuals) = process_sized(&p, OffsetRange::new(0, i64::MAX));
    assert_eq!(values, [0]);
    assert_eq!(residuals.len(), 1);
    let ((_, rest), _) = <((Vec<u8>, OffsetRange), f64)>::decode(&residuals[0].element).unwrap();
    assert_eq!((rest.start, rest.end), (1, i64::MAX));
}

#[test]
fn generate_sequence_clock_starts_when_the_pipeline_executes() {
    // Simulate pipeline construction delay exceeding `max_read_time`.
    let p = Pipeline::new();
    let _ = p.apply(
        GenerateSequence::new("GenerateSequence", 0)
            .with_rate(1000, Duration::from_secs(1))
            .with_max_read_time(Duration::from_millis(200)),
    );
    std::thread::sleep(Duration::from_millis(300));

    // The clock begins at execution start, allowing elements to emit.
    let (values, _) = process_sized(&p, OffsetRange::new(0, i64::MAX));
    assert!(
        !values.is_empty() && values[0] == 0,
        "max_read_time already expired before the first element: {values:?}"
    );
}
