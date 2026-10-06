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

use beam::transforms::sdf::RestrictionTracker;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::{GlobalWindow, IntervalWindow, URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW};
use beam::pipeline::constants::{
    URN_WINDOW_FN_FIXED_WINDOWS, URN_WINDOW_FN_GLOBAL_WINDOWS, URN_WINDOW_FN_SESSION_WINDOWS,
    URN_WINDOW_FN_SLIDING_WINDOWS, URN_WINDOW_INTO,
};
use beam::prelude::*;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, WatermarkedTracker};
use beam::windowing::{
    AccumulationMode, BoundedWindow, ClosingBehavior, FixedWindows, GlobalWindows,
    ManualWatermarkEstimator, OnTimeBehavior, OutputTime, Sessions, SlidingWindows,
    TimestampObservingWatermarkEstimator, Trigger, WallTimeWatermarkEstimator, WatermarkEstimator,
    WindowInto, WindowingStrategy, decode_window_fn, watermark_from_proto, watermark_to_proto,
};

#[test]
fn test_global_windows() {
    let fn_global = GlobalWindows;
    assert_eq!(fn_global.urn(), URN_WINDOW_FN_GLOBAL_WINDOWS);
    assert_eq!(fn_global.window_coder_urn(), URN_GLOBAL_WINDOW);
    assert!(fn_global.assigns_to_one_window());

    let assigned = fn_global.assign_windows(123456);
    assert_eq!(assigned, vec![BoundedWindow::Global(GlobalWindow)]);
    assert_eq!(
        assigned[0].max_timestamp(),
        BoundedWindow::TIMESTAMP_MAX_VALUE
    );

    let spec = model::pipeline::FunctionSpec {
        urn: fn_global.urn().to_string(),
        payload: fn_global.payload(),
    };
    let decoded = decode_window_fn(&spec).expect("Should decode GlobalWindows");
    assert_eq!(decoded.urn(), URN_WINDOW_FN_GLOBAL_WINDOWS);
}

#[test]
fn interval_windows_are_half_open() {
    let window = IntervalWindow::new(10, 20);
    for (timestamp, inside) in [(9, false), (10, true), (19, true), (20, false)] {
        assert_eq!(window.contains(timestamp), inside, "{timestamp}");
    }

    let cases = [
        (IntervalWindow::new(0, 10), false),  // touches the start
        (IntervalWindow::new(20, 30), false), // touches the end
        (IntervalWindow::new(0, 5), false),   // disjoint before
        (IntervalWindow::new(25, 30), false), // disjoint after
        (IntervalWindow::new(0, 11), true),   // overlaps the first millisecond
        (IntervalWindow::new(19, 30), true),  // overlaps the last millisecond
        (IntervalWindow::new(12, 18), true),  // contained
        (IntervalWindow::new(0, 30), true),   // containing
        (IntervalWindow::new(10, 20), true),  // identical
    ];
    for (other, overlap) in cases {
        assert_eq!(window.intersects(&other), overlap, "{other:?}");
        assert_eq!(other.intersects(&window), overlap, "{other:?} (reversed)");
    }
}

#[test]
fn test_fixed_windows() {
    let fixed = FixedWindows::of(Duration::from_secs(10));
    assert_eq!(fixed.urn(), URN_WINDOW_FN_FIXED_WINDOWS);
    assert_eq!(fixed.window_coder_urn(), URN_INTERVAL_WINDOW);
    assert!(fixed.assigns_to_one_window());

    // Element at 5s -> [0s, 10s)
    let w1 = fixed.assign_windows(5_000);
    assert_eq!(
        w1,
        vec![BoundedWindow::Interval(IntervalWindow::new(0, 10_000))]
    );
    assert_eq!(w1[0].max_timestamp(), 9_999);

    // Element at 10s -> [10s, 20s)
    let w2 = fixed.assign_windows(10_000);
    assert_eq!(
        w2,
        vec![BoundedWindow::Interval(IntervalWindow::new(10_000, 20_000))]
    );

    // Negative timestamp: -5s -> [-10s, 0s)
    let w3 = fixed.assign_windows(-5_000);
    assert_eq!(
        w3,
        vec![BoundedWindow::Interval(IntervalWindow::new(-10_000, 0))]
    );

    // Fixed window with offset: 10s size with 2s offset
    let with_offset = FixedWindows::of(Duration::from_secs(10)).with_offset(Duration::from_secs(2));
    // Element at 5s -> [2s, 12s)
    let w_off = with_offset.assign_windows(5_000);
    assert_eq!(
        w_off,
        vec![BoundedWindow::Interval(IntervalWindow::new(2_000, 12_000))]
    );

    // Serialization / Deserialization
    let spec = model::pipeline::FunctionSpec {
        urn: with_offset.urn().to_string(),
        payload: with_offset.payload(),
    };
    let decoded = decode_window_fn(&spec).expect("Should decode FixedWindows");
    assert_eq!(decoded.urn(), URN_WINDOW_FN_FIXED_WINDOWS);
    assert_eq!(
        decoded.assign_windows(5_000),
        vec![BoundedWindow::Interval(IntervalWindow::new(2_000, 12_000))]
    );
}

#[test]
fn test_sliding_windows() {
    // 10s window every 5s
    let sliding = SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5));
    assert_eq!(sliding.urn(), URN_WINDOW_FN_SLIDING_WINDOWS);
    assert_eq!(sliding.window_coder_urn(), URN_INTERVAL_WINDOW);
    assert!(!sliding.assigns_to_one_window());

    // Element at 7s falls into [0s, 10s) and [5s, 15s)
    let assigned = sliding.assign_windows(7_000);
    assert_eq!(
        assigned,
        vec![
            BoundedWindow::Interval(IntervalWindow::new(5_000, 15_000)),
            BoundedWindow::Interval(IntervalWindow::new(0, 10_000)),
        ]
    );

    // Proto roundtrip
    let spec = model::pipeline::FunctionSpec {
        urn: sliding.urn().to_string(),
        payload: sliding.payload(),
    };
    let decoded = decode_window_fn(&spec).expect("Should decode SlidingWindows");
    assert_eq!(decoded.urn(), URN_WINDOW_FN_SLIDING_WINDOWS);
    assert_eq!(decoded.assign_windows(7_000), assigned);
}

#[test]
fn test_session_windows() {
    let sessions = Sessions::with_gap_duration(Duration::from_secs(5));
    assert_eq!(sessions.urn(), URN_WINDOW_FN_SESSION_WINDOWS);
    assert_eq!(sessions.window_coder_urn(), URN_INTERVAL_WINDOW);
    assert_eq!(
        sessions.merge_status(),
        model::pipeline::merge_status::Enum::NeedsMerge
    );

    // Element at 1000ms -> [1000ms, 6000ms)
    let assigned = sessions.assign_windows(1_000);
    assert_eq!(
        assigned,
        vec![BoundedWindow::Interval(IntervalWindow::new(1_000, 6_000))]
    );

    // Proto roundtrip
    let spec = model::pipeline::FunctionSpec {
        urn: sessions.urn().to_string(),
        payload: sessions.payload(),
    };
    let decoded = decode_window_fn(&spec).expect("Should decode Sessions");
    assert_eq!(decoded.urn(), URN_WINDOW_FN_SESSION_WINDOWS);
    assert_eq!(decoded.assign_windows(1_000), assigned);
}

#[test]
fn test_windowing_strategy_proto_roundtrip() {
    let strategy = WindowingStrategy {
        window_fn: Arc::new(FixedWindows::of(Duration::from_secs(30))),
        trigger: Trigger::repeatedly(Trigger::after_count(5)),
        accumulation_mode: AccumulationMode::Accumulating,
        output_time: OutputTime::LatestInPane,
        closing_behavior: ClosingBehavior::EmitAlways,
        allowed_lateness: Duration::from_secs(120),
        on_time_behavior: OnTimeBehavior::FireAlways,
    };

    let proto_strategy = strategy.to_proto("coder_1", "env_1");
    assert_eq!(proto_strategy.window_coder_id, "coder_1");
    assert_eq!(proto_strategy.environment_id, "env_1");
    assert_eq!(proto_strategy.allowed_lateness, 120_000);

    let roundtrip = WindowingStrategy::from_proto(&proto_strategy)
        .expect("Should deserialize WindowingStrategy");
    assert_eq!(roundtrip.accumulation_mode, AccumulationMode::Accumulating);
    assert_eq!(roundtrip.output_time, OutputTime::LatestInPane);
    assert_eq!(roundtrip.closing_behavior, ClosingBehavior::EmitAlways);
    assert_eq!(roundtrip.on_time_behavior, OnTimeBehavior::FireAlways);
    assert_eq!(roundtrip.allowed_lateness, Duration::from_secs(120));
    assert_eq!(
        roundtrip.trigger,
        Trigger::repeatedly(Trigger::after_count(5))
    );
    assert_eq!(roundtrip.window_fn.urn(), URN_WINDOW_FN_FIXED_WINDOWS);
}

#[test]
fn test_watermark_estimators() {
    // ManualWatermarkEstimator
    let manual = ManualWatermarkEstimator::new(1_000);
    assert_eq!(manual.current_watermark(), 1_000);
    manual.set_watermark(5_000);
    assert_eq!(manual.current_watermark(), 5_000);

    // TimestampObservingWatermarkEstimator
    let observing = TimestampObservingWatermarkEstimator::new(0);
    assert_eq!(observing.current_watermark(), 0);
    observing.observe_timestamp(10_000);
    assert_eq!(observing.current_watermark(), 10_000);
    // Monotonicity: older timestamps do not decrease the watermark.
    observing.observe_timestamp(5_000);
    assert_eq!(observing.current_watermark(), 10_000);
    observing.observe_timestamp(15_000);
    assert_eq!(observing.current_watermark(), 15_000);

    // WallTimeWatermarkEstimator reports current wall time.
    let now_millis = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    let wall = WallTimeWatermarkEstimator::new();
    let before = now_millis();
    let reading = wall.current_watermark();
    let after = now_millis();
    assert!(
        (before..=after).contains(&reading),
        "{before} <= {reading} <= {after}"
    );

    // Protobuf representation requires seconds and non-negative nanoseconds.
    for (millis, seconds, nanos) in [
        (1_234_567, 1_234, 567_000_000),
        (0, 0, 0),
        (-1, -1, 999_000_000),
        (-1_500, -2, 500_000_000),
    ] {
        let ts = watermark_to_proto(millis);
        assert_eq!((ts.seconds, ts.nanos), (seconds, nanos), "{millis}ms");
        assert_eq!(watermark_from_proto(&ts), millis, "{millis}ms round trip");
    }
}

#[test]
fn test_watermarked_tracker() {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, 100));
    let estimator = ManualWatermarkEstimator::new(50_000);
    let watermarked = WatermarkedTracker::new(tracker, estimator).with_bounded(false);

    assert!(!watermarked.is_bounded());
    assert_eq!(watermarked.current_watermark(), Some(50_000));
    assert!(watermarked.try_claim(&0));
    assert_eq!(watermarked.current_restriction(), OffsetRange::new(0, 100));

    watermarked.estimator.set_watermark(75_000);
    assert_eq!(watermarked.current_watermark(), Some(75_000));

    // Bounded by default; checkpoint and check_done delegate to the inner tracker.
    let bounded = WatermarkedTracker::new(
        OffsetRangeTracker::new(OffsetRange::new(0, 4)),
        ManualWatermarkEstimator::new(0),
    );
    assert!(bounded.is_bounded());
    assert!(bounded.try_claim(&0));
    assert!(bounded.check_done().is_err(), "offsets 1..4 are unclaimed");
    assert_eq!(bounded.try_checkpoint(), Some(OffsetRange::new(1, 4)));
    assert!(bounded.check_done().is_ok());
}

#[test]
fn test_window_into_pipeline_expansion() {
    let pipeline = Pipeline::default();
    let pbegin = pipeline.begin();

    let pcoll = pbegin.apply(Create::new(
        "Create",
        vec!["hello".to_string(), "world".to_string()],
    ));
    let windowed = pcoll.apply(
        WindowInto::new("WindowInto", FixedWindows::of(Duration::from_secs(60)))
            .triggering(Trigger::after_count(2))
            .accumulating_fired_panes(),
    );

    assert!(!windowed.id().is_empty());
    let ws_id = windowed.windowing_strategy_id();
    assert!(!ws_id.is_empty());

    let inner = pipeline.lock();
    let strategy_proto = inner
        .components
        .windowing_strategies
        .get(&ws_id)
        .expect("Strategy proto should be registered");

    assert_eq!(
        strategy_proto.window_fn.as_ref().unwrap().urn,
        URN_WINDOW_FN_FIXED_WINDOWS
    );
    assert_eq!(
        strategy_proto.accumulation_mode,
        model::pipeline::accumulation_mode::Enum::Accumulating as i32
    );

    // Confirm WindowInto transform is present in components
    let window_transform = inner
        .components
        .transforms
        .values()
        .find(|t| t.spec.as_ref().is_some_and(|s| s.urn == URN_WINDOW_INTO))
        .expect("WindowInto transform must be present in graph");
    assert_eq!(
        window_transform.inputs.get("in").unwrap(),
        &pcoll.id().to_string()
    );
    assert_eq!(
        window_transform.outputs.get("out").unwrap(),
        &windowed.id().to_string()
    );
}

#[test]
fn multi_window_header_explodes_into_single_windows_for_dofn() {
    use beam::coders::{DefaultCoder, PaneInfo, WindowedHeader};
    use beam::internals::{BundleHandler, DoFnHandler, ElementSink, HandlerContext, TypedElement};
    use beam::windowing::WindowFn;

    struct WindowRecorder(Vec<(IntervalWindow, i64)>);

    impl ElementSink for WindowRecorder {
        fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
            Ok(())
        }

        fn push_windowed(
            &mut self,
            header: &WindowedHeader,
            element: Vec<u8>,
        ) -> std::result::Result<(), String> {
            let sink = &mut Vec::new();
            let ctx = ProcessContext::<i64>::with_context(sink, None, header);
            self.0.push((
                ctx.interval_window().expect("single interval window"),
                i64::decode(&element).expect("decode value"),
            ));
            Ok(())
        }
    }

    #[derive(Clone)]
    struct ObserveWindowFn;

    impl DoFn for ObserveWindowFn {
        type In = i64;
        type Out = i64;

        fn process_element(&mut self, v: i64, ctx: &mut ProcessContext<'_, i64>) -> Result {
            let win = ctx.interval_window().expect("interval window");
            assert_eq!(ctx.header().window_count(), 1);
            ctx.emit(v + win.start_millis)
        }
    }

    let sliding = SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5));
    let windows = sliding.assign_windows_encoded(7_000);
    let header = WindowedHeader::new(7_000, &windows, PaneInfo::NO_FIRING);
    assert_eq!(header.window_count(), 2);

    // Bytes path, and the by-value path used when an upstream operator is fused.
    let run = |by_value: bool| {
        let mut recorder = WindowRecorder(Vec::new());
        let mut handler = DoFnHandler::new(ObserveWindowFn);
        let mut hctx = HandlerContext::new(&mut recorder).with_header(&header);
        let result = match by_value {
            false => handler.process(&42i64.encode().expect("encode"), &mut hctx),
            true => handler.process_value(TypedElement::new(&mut Some(42i64)), &mut hctx),
        };
        result.map(|()| recorder.0)
    };

    let expected = vec![
        (IntervalWindow::new(5_000, 15_000), 5_042),
        (IntervalWindow::new(0, 10_000), 42),
    ];
    assert_eq!(run(false), Ok(expected.clone()), "bytes path");
    assert_eq!(run(true), Ok(expected), "by-value path");
}

#[test]
fn header_explode_splits_interval_windows_and_rejects_other_encodings() {
    use beam::coders::{PaneInfo, WindowedHeader};
    use beam::windowing::WindowFn;
    use std::borrow::Cow;

    let pane = PaneInfo::NO_FIRING;
    let global = WindowedHeader::global(7_000, pane);
    let unchanged: Vec<_> = global.explode().unwrap().collect();
    assert!(matches!(unchanged.as_slice(), [Cow::Borrowed(h)] if *h == &global));

    let sliding = SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5));
    let windows = sliding.assign_windows_encoded(7_000);
    let split: Vec<_> = WindowedHeader::new(7_000, &windows, pane)
        .explode()
        .unwrap()
        .map(Cow::into_owned)
        .collect();
    let expected: Vec<_> = windows
        .iter()
        .map(|w| WindowedHeader::new(7_000, std::slice::from_ref(w), pane))
        .collect();
    assert_eq!(split, expected);

    // Many windows that are not interval windows cannot be told apart.
    let opaque = WindowedHeader::new(7_000, &[vec![1, 2, 3], vec![4, 5, 6]], pane);
    assert!(opaque.explode().is_err());
}
