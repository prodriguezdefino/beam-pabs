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
#![expect(clippy::unwrap_used, reason = "test helpers")]

use beam::coders::{Coder, Context, DefaultCoder, URN_KV};
use beam::internals::*;
use beam::prelude::*;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionError, RestrictionTracker,
    SplittableDoFn, SplittableParDo, WatermarkedTracker,
};
use beam::transforms::{ProcessContext, Windowed};
use beam::values::IsBounded;
use beam::windowing::{GlobalWindow, ManualWatermarkEstimator};

#[test]
fn test_offset_range_properties() {
    let range = OffsetRange::new(10, 50);
    assert_eq!(range.start, 10);
    assert_eq!(range.end, 50);
    assert_eq!(range.size(), 40.0);
    assert!(!range.is_empty());
    assert!(range.contains(10));
    assert!(range.contains(49));
    assert!(!range.contains(50));
    assert!(!range.contains(9));

    let empty = OffsetRange::new(50, 50);
    assert!(empty.is_empty());
    assert_eq!(empty.size(), 0.0);

    let inverted = OffsetRange::new(50, 10);
    assert!(inverted.is_empty());
    assert_eq!(inverted.size(), 0.0);
}

#[test]
fn test_offset_range_even_splits() {
    let range = OffsetRange::new(0, 10);

    let splits = range.even_splits(2);
    assert_eq!(
        splits,
        vec![OffsetRange::new(0, 5), OffsetRange::new(5, 10)]
    );

    let splits = range.even_splits(3);
    assert_eq!(
        splits,
        vec![
            OffsetRange::new(0, 3),
            OffsetRange::new(3, 6),
            OffsetRange::new(6, 10)
        ]
    );

    // Verify union of even splits equals original without gaps
    let range = OffsetRange::new(7, 103);
    let splits = range.even_splits(5);
    assert_eq!(splits.first().unwrap().start, 7);
    assert_eq!(splits.last().unwrap().end, 103);
    for window in splits.windows(2) {
        assert_eq!(window[0].end, window[1].start);
    }

    // An empty range comes back whole, even one whose size would overflow.
    let inverted = OffsetRange::new(i64::MAX, i64::MIN);
    assert_eq!(inverted.even_splits(2), vec![inverted]);
}

#[test]
fn test_offset_range_sized_splits() {
    let range = OffsetRange::new(0, 24);
    let splits = range.sized_splits(10);
    assert_eq!(
        splits,
        vec![
            OffsetRange::new(0, 10),
            OffsetRange::new(10, 20),
            OffsetRange::new(20, 24)
        ]
    );

    let splits = range.sized_splits(50);
    assert_eq!(splits, vec![OffsetRange::new(0, 24)]);

    // A range that divides evenly gets no trailing empty split.
    assert_eq!(
        OffsetRange::new(0, 20).sized_splits(10),
        vec![OffsetRange::new(0, 10), OffsetRange::new(10, 20)]
    );

    // A non-positive size returns the range whole. `i64::MIN` comes first: stepping by
    // it overflows at once, where stepping by 0 would loop forever.
    for size in [i64::MIN, 0] {
        assert_eq!(range.sized_splits(size), vec![range], "size {size}");
    }
}

#[test]
fn test_offset_range_coder_roundtrip() {
    let original = OffsetRange::new(1024, 2048);
    let encoded = original.encode().expect("Encoding must succeed");
    let decoded = OffsetRange::decode(&encoded).expect("Decoding must succeed");
    assert_eq!(original, decoded);

    // The standalone coder declares and writes the same KV<varint, varint> encoding.
    let coder = OffsetRange::coder();
    assert_eq!(coder.urn(), URN_KV);
    let mut written = Vec::new();
    coder
        .encode(&original, &mut written, Context::Nested)
        .expect("Encoding must succeed");
    assert_eq!(written, (1024i64, 2048i64).encode().unwrap());
    assert_eq!(
        coder
            .decode(&mut written.as_slice(), Context::Nested)
            .unwrap(),
        original
    );
}

#[test]
fn test_tracker_claim_progression() {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, 5));

    assert_eq!(tracker.last_claimed(), None);
    assert!(tracker.try_claim(&0));
    assert_eq!(tracker.last_claimed(), Some(0));
    assert!(tracker.try_claim(&1));
    assert!(tracker.try_claim(&2));
    assert!(tracker.try_claim(&3));
    assert!(tracker.try_claim(&4));
    assert_eq!(tracker.last_claimed(), Some(4));

    // Claiming beyond range ends work
    assert!(!tracker.try_claim(&5));
    assert!(tracker.check_done().is_ok());
}

#[test]
fn test_tracker_out_of_bounds_claim() {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(10, 20));
    assert!(!tracker.try_claim(&5));
    assert!(matches!(
        tracker.error(),
        Some(RestrictionError::OutOfBounds(_))
    ));
    assert!(tracker.check_done().is_err());
}

#[test]
fn test_tracker_non_monotonic_claim() {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, 10));
    assert!(tracker.try_claim(&2));
    assert!(!tracker.try_claim(&1));
    assert!(matches!(
        tracker.error(),
        Some(RestrictionError::NonMonotonicClaim(_, _))
    ));
}

#[test]
fn test_tracker_incomplete_work_fails_check_done() {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, 10));
    assert!(tracker.try_claim(&0));
    assert!(tracker.try_claim(&1));
    // Stopped at 1 without attempting up to 9
    let err = tracker.check_done().unwrap_err();
    assert!(matches!(err, RestrictionError::IncompleteWork(_)));

    let empty_tracker = OffsetRangeTracker::new(OffsetRange::new(0, 0));
    assert!(empty_tracker.check_done().is_ok());

    // Claiming the last offset completes the work; no failed claim past the end is needed.
    let exact = OffsetRangeTracker::new(OffsetRange::new(0, 3));
    (0..3).for_each(|i| assert!(exact.try_claim(&i)));
    assert!(exact.check_done().is_ok());
}

#[test]
fn test_tracker_progress_reporting() {
    // A non-zero start checks that progress is measured from it.
    let tracker = OffsetRangeTracker::new(OffsetRange::new(10, 20));

    // Initially
    let p = tracker.current_progress();
    assert_eq!(p.work_completed, 0.0);
    assert_eq!(p.work_remaining, 10.0);
    assert_eq!(p.fraction_completed(), 0.0);

    // After claiming 4 elements (10, 11, 12, 13)
    assert!(tracker.try_claim(&10));
    assert!(tracker.try_claim(&11));
    assert!(tracker.try_claim(&12));
    assert!(tracker.try_claim(&13));

    let p = tracker.current_progress();
    assert_eq!(p.work_completed, 4.0);
    assert_eq!(p.work_remaining, 6.0);
    assert_eq!(p.fraction_completed(), 0.4);
}

#[test]
fn test_tracker_dynamic_split_before_processing() {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, 10));

    // Split at 50% remainder before claiming
    let (primary, residual) = tracker
        .try_split(0.5)
        .expect("Split before processing must succeed");
    assert_eq!(primary, OffsetRange::new(0, 5));
    assert_eq!(residual, OffsetRange::new(5, 10));
    assert_eq!(tracker.current_restriction(), OffsetRange::new(0, 5));

    // A split that would leave the residual empty is refused.
    assert_eq!(tracker.try_split(1.0), None);
    assert_eq!(tracker.current_restriction(), OffsetRange::new(0, 5));

    // Process primary
    for i in 0..5 {
        assert!(tracker.try_claim(&i));
    }
    assert!(!tracker.try_claim(&5));
    assert!(tracker.check_done().is_ok());
}

#[test]
fn test_tracker_dynamic_split_mid_stream() {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, 10));

    assert!(tracker.try_claim(&0));
    assert!(tracker.try_claim(&1));
    assert!(tracker.try_claim(&2));

    // Split remainder: work remaining is [3, 10) (7 units).
    // fraction 0.5 of 7 is 3.5 -> ceil(3.5) = 4.
    // split point is 2 + 4 = 6.
    let (primary, residual) = tracker
        .try_split(0.5)
        .expect("Mid-stream split must succeed");
    assert_eq!(primary, OffsetRange::new(0, 6));
    assert_eq!(residual, OffsetRange::new(6, 10));

    assert!(tracker.try_claim(&3));
    assert!(tracker.try_claim(&4));
    assert!(tracker.try_claim(&5));
    assert!(!tracker.try_claim(&6));
    assert!(tracker.check_done().is_ok());
}

#[test]
fn test_tracker_checkpoint_split_fraction_zero() {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, 10));

    assert!(tracker.try_claim(&0));
    assert!(tracker.try_claim(&1));

    // Self-checkpointing split at fraction 0.0: should stop as soon as possible
    let (primary, residual) = tracker
        .try_split(0.0)
        .expect("Checkpoint split must succeed");
    assert_eq!(primary, OffsetRange::new(0, 2));
    assert_eq!(residual, OffsetRange::new(2, 10));

    // Immediate stop
    assert!(!tracker.try_claim(&2));
    assert!(tracker.check_done().is_ok());
}

#[derive(Clone)]
struct StreamingTestSdf;

impl SplittableDoFn for StreamingTestSdf {
    type In = String;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = WatermarkedTracker<OffsetRangeTracker, ManualWatermarkEstimator>;

    fn boundedness(&self) -> IsBounded {
        IsBounded::Unbounded
    }

    fn initial_restriction(&self, _element: &Self::In) -> Self::Restriction {
        OffsetRange::new(0, 100)
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        let tracker = OffsetRangeTracker::new(*restriction);
        let estimator = ManualWatermarkEstimator::new(Instant::from_millis(42_000));
        WatermarkedTracker::new(tracker, estimator).with_boundedness(IsBounded::Unbounded)
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        tracker: &Self::Tracker,
        out: &mut ProcessContext<Self::Out>,
    ) -> Result<ProcessContinuation> {
        tracker.try_claim(&0);
        out.emit(100)?;
        tracker
            .estimator
            .set_watermark(Instant::from_millis(55_000));
        Ok(ProcessContinuation::resume())
    }
}

#[test]
fn test_sdf_watermark_propagation_to_residual() {
    use beam::internals::BundleHandler;
    use beam::internals::{ResidualCollector, SdfProcessSizedElementsHandler};
    use std::sync::Arc;

    let mut handler = SdfProcessSizedElementsHandler::new(StreamingTestSdf);

    let residual_collector = Arc::new(ResidualCollector::new());
    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink)
        .with_transform_id("transform-sdf-1")
        .with_residual_collector(Some(&residual_collector));

    let sized_element = (("stream_elem".to_string(), OffsetRange::new(0, 100)), 100.0)
        .encode()
        .unwrap();

    handler
        .process(&sized_element, &mut ctx)
        .expect("process should succeed");

    let residuals = residual_collector.drain();
    assert_eq!(residuals.len(), 1);
    let r = &residuals[0];
    assert_eq!(r.transform_id, "transform-sdf-1");
    assert_eq!(r.boundedness, IsBounded::Unbounded);
    assert_eq!(
        r.output_watermarks.get("out"),
        Some(&Instant::from_millis(55_000))
    );
    // The residual resumes right after the one claimed offset.
    let ((elem, rest), _) = <((String, OffsetRange), f64)>::decode(&r.element).unwrap();
    assert_eq!(elem, "stream_elem");
    assert_eq!(rest, OffsetRange::new(1, 100));
    assert_eq!(r.delay, None);
    let emitted: Vec<i64> = sink.iter().map(|b| i64::decode(b).unwrap()).collect();
    assert_eq!(emitted, [100]);
}

#[derive(Clone)]
struct BoundedRangeSdf;

impl SplittableDoFn for BoundedRangeSdf {
    type In = String;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn boundedness(&self) -> IsBounded {
        IsBounded::Bounded
    }

    fn initial_restriction(&self, _element: &Self::In) -> Self::Restriction {
        OffsetRange::new(0, 10)
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        tracker: &Self::Tracker,
        out: &mut ProcessContext<Self::Out>,
    ) -> Result<ProcessContinuation> {
        let mut i = tracker.current_restriction().start;
        while tracker.try_claim(&i) {
            out.emit(i)?;
            i += 1;
        }
        Ok(ProcessContinuation::stop())
    }
}

/// An SDF with a side input: the side input values depend on the window, so each window of
/// an element is processed on its own.
#[derive(Clone)]
struct SideInputBoundedRangeSdf {
    view: SingletonView<i64>,
}

impl SplittableDoFn for SideInputBoundedRangeSdf {
    type In = String;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn boundedness(&self) -> IsBounded {
        IsBounded::Bounded
    }

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.view]
    }

    fn initial_restriction(&self, _element: &Self::In) -> Self::Restriction {
        OffsetRange::new(0, 10)
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        tracker: &Self::Tracker,
        out: &mut ProcessContext<Self::Out>,
    ) -> Result<ProcessContinuation> {
        let mut i = tracker.current_restriction().start;
        while tracker.try_claim(&i) {
            out.emit(i)?;
            i += 1;
        }
        Ok(ProcessContinuation::stop())
    }
}

#[test]
fn sdf_processes_a_multi_window_element_compressed_by_default_and_per_window_with_side_inputs() {
    use beam::coders::{PaneInfo, WindowedHeader};
    use beam::internals::{BundleHandler, ElementSink, SdfProcessSizedElementsHandler};
    use beam::windowing::{SlidingWindows, WindowFn};
    use std::time::Duration;

    #[derive(Default)]
    struct WindowedSink(Vec<(Vec<u8>, i64)>);

    impl ElementSink for WindowedSink {
        fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
            Err("expected a windowed element".to_string())
        }

        fn push_windowed(
            &mut self,
            header: &WindowedHeader,
            element: Vec<u8>,
        ) -> std::result::Result<(), String> {
            let value = i64::decode(&element).map_err(|e| e.to_string())?;
            self.0.push((header.window_bytes().to_vec(), value));
            Ok(())
        }
    }

    let sliding = SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5));
    let windows = sliding.assign_windows_encoded(Instant::from_millis(7_000));
    let header = WindowedHeader::new(7_000, &windows, PaneInfo::NO_FIRING);
    let sized = (("e".to_string(), OffsetRange::new(0, 2)), 2.0)
        .encode()
        .unwrap();

    // Window-oblivious SDF (default) processes the element once with the multi-window header intact.
    let mut oblivious_handler = SdfProcessSizedElementsHandler::new(BoundedRangeSdf);
    let mut oblivious_sink = WindowedSink::default();
    let mut ctx = HandlerContext::new(&mut oblivious_sink).with_header(&header);
    oblivious_handler
        .process(&sized, &mut ctx)
        .expect("process");
    drop(ctx);
    assert_eq!(
        oblivious_sink.0,
        vec![
            (header.window_bytes().to_vec(), 0),
            (header.window_bytes().to_vec(), 1),
        ]
    );

    // An SDF with side inputs splits the multi-window header and runs once per window.
    let view = Pipeline::new()
        .apply(Create::new("Side", vec![1_i64]))
        .as_singleton();
    let mut observing_handler =
        SdfProcessSizedElementsHandler::new(SideInputBoundedRangeSdf { view });
    let mut observing_sink = WindowedSink::default();
    let mut ctx = HandlerContext::new(&mut observing_sink).with_header(&header);
    observing_handler
        .process(&sized, &mut ctx)
        .expect("process");
    drop(ctx);

    let expected: Vec<_> = windows
        .iter()
        .flat_map(|w| [(w.clone(), 0), (w.clone(), 1)])
        .collect();
    assert_eq!(observing_sink.0, expected);
}

/// Takes `Windowed<String>`: the restriction depends on the window (one offset in the window
/// that starts at 0, two otherwise), and each output is the window start plus the offset.
#[derive(Clone)]
struct WindowAwareSdf;

impl SplittableDoFn for WindowAwareSdf {
    type In = Windowed<String>;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, input: &Self::In) -> Self::Restriction {
        let start = input.window.interval().map_or(0, |w| w.start.as_millis());
        OffsetRange::new(0, if start == 0 { 1 } else { 2 })
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        input: Self::In,
        tracker: &Self::Tracker,
        out: &mut ProcessContext<Self::Out>,
    ) -> Result<ProcessContinuation> {
        let start = input.window.interval().map_or(0, |w| w.start.as_millis());
        let mut i = tracker.current_restriction().start;
        while tracker.try_claim(&i) {
            out.emit(start + i)?;
            i += 1;
        }
        Ok(ProcessContinuation::stop())
    }
}

#[test]
fn a_window_observing_sdf_runs_every_stage_once_per_window() {
    use beam::coders::{PaneInfo, WindowedHeader};
    use beam::internals::{
        BundleHandler, ElementSink, SdfPairWithRestrictionHandler, SdfProcessSizedElementsHandler,
    };
    use beam::windowing::{SlidingWindows, WindowFn};
    use std::time::Duration;

    #[derive(Default)]
    struct Recorder(Vec<(WindowedHeader, Vec<u8>)>);

    impl ElementSink for Recorder {
        fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
            Err("a window-observing stage pushes under a single-window header".to_string())
        }

        fn push_windowed(
            &mut self,
            header: &WindowedHeader,
            element: Vec<u8>,
        ) -> std::result::Result<(), String> {
            self.0.push((header.clone(), element));
            Ok(())
        }
    }

    let sliding = SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5));
    let header = WindowedHeader::new(
        7_000,
        &sliding.assign_windows_encoded(Instant::from_millis(7_000)),
        PaneInfo::NO_FIRING,
    );
    let window_start = |h: &WindowedHeader| {
        h.single_window()
            .unwrap()
            .interval()
            .unwrap()
            .start
            .as_millis()
    };

    // Pair stage: one (element, restriction) per window, sized from that window.
    let mut pair = SdfPairWithRestrictionHandler {
        func: WindowAwareSdf,
    };
    let mut sink = Recorder::default();
    let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
    pair.process(&"e".to_string().encode().unwrap(), &mut ctx)
        .expect("pair");
    drop(ctx);
    let mut paired: Vec<_> = sink
        .0
        .iter()
        .map(|(h, bytes)| {
            let (_, r) = <(String, OffsetRange)>::decode(bytes).unwrap();
            (window_start(h), r.end)
        })
        .collect();
    paired.sort();
    assert_eq!(paired, vec![(0, 1), (5_000, 2)]);

    // Process stage: each window runs on its own and sees its window.
    let sized = (("e".to_string(), OffsetRange::new(0, 1)), 1.0)
        .encode()
        .unwrap();
    let mut process = SdfProcessSizedElementsHandler::new(WindowAwareSdf);
    let mut sink = Recorder::default();
    let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
    process.process(&sized, &mut ctx).expect("process");
    drop(ctx);
    let mut outputs: Vec<_> = sink
        .0
        .iter()
        .map(|(h, bytes)| (window_start(h), i64::decode(bytes).unwrap()))
        .collect();
    outputs.sort();
    assert_eq!(outputs, vec![(0, 0), (5_000, 5_000)]);
}

#[derive(Clone)]
struct BoundedRangeSdfWithSide {
    factor_view: SingletonView<i32>,
}

impl SplittableDoFn for BoundedRangeSdfWithSide {
    type In = String;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn boundedness(&self) -> IsBounded {
        IsBounded::Bounded
    }

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.factor_view]
    }

    fn initial_restriction(&self, _element: &Self::In) -> Self::Restriction {
        OffsetRange::new(0, 10)
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        tracker: &Self::Tracker,
        out: &mut ProcessContext<Self::Out>,
    ) -> Result<ProcessContinuation> {
        let factor = i64::from(out.side_input(&self.factor_view)?);
        let mut i = tracker.current_restriction().start;
        while tracker.try_claim(&i) {
            out.emit(i * factor)?;
            i += 1;
        }
        Ok(ProcessContinuation::stop())
    }
}

#[test]
fn test_splittable_pardo_with_side_inputs_graph_and_proto() {
    let p = Pipeline::new();
    let main_col = p.apply(Create::new("Main", vec!["test".to_string()]));
    let factor_view = p.apply(Create::new("Factor", vec![5_i32])).as_singleton();

    let out = main_col.apply(SplittableParDo::new(
        "BoundedSdfWithSide",
        BoundedRangeSdfWithSide {
            factor_view: factor_view.clone(),
        },
    ));
    assert!(!out.id().is_empty());

    let proto = p.to_proto();
    let components = proto.components.as_ref().unwrap();
    let transform = components
        .transforms
        .values()
        .find(|t| t.unique_name == "BoundedSdfWithSide")
        .expect("BoundedSdfWithSide transform must exist");

    assert_eq!(transform.inputs.len(), 2);
    assert!(transform.inputs.contains_key("in"));
    assert_eq!(
        transform.inputs.get(factor_view.tag()).map(String::as_str),
        Some(factor_view.pcollection_id())
    );

    let side_tags = extract_side_input_tags(transform);
    assert!(side_tags.contains(factor_view.tag()));
}

#[test]
fn test_sdf_truncate_restriction_bounded_and_unbounded() {
    use beam::internals::BundleHandler;
    use beam::internals::SplittableDoFnHandler;
    use beam::pipeline::constants::URN_SDF_TRUNCATE_SIZED_RESTRICTIONS;

    // Bounded SDF defaults to retaining the restriction during drain.
    let bounded_handler = SplittableDoFnHandler::new(BoundedRangeSdf);
    let truncate_bounded = bounded_handler
        .stage_handler(URN_SDF_TRUNCATE_SIZED_RESTRICTIONS)
        .expect("truncate handler must be available for bounded SDF");

    let mut sink_bounded = Vec::<Vec<u8>>::new();
    let mut ctx_bounded = HandlerContext::new(&mut sink_bounded);
    let sized_bounded = (("element".to_string(), OffsetRange::new(0, 10)), 10.0)
        .encode()
        .unwrap();

    truncate_bounded
        .instantiate()
        .process(&sized_bounded, &mut ctx_bounded)
        .expect("truncate process should succeed");

    assert_eq!(sink_bounded.len(), 1);
    let decoded_bounded: ((String, OffsetRange), f64) =
        <((String, OffsetRange), f64)>::decode(&sink_bounded[0]).unwrap();
    assert_eq!(decoded_bounded.0.1, OffsetRange::new(0, 10));

    // Unbounded SDF defaults to dropping the restriction (returning `None`) during drain.
    let streaming_handler = SplittableDoFnHandler::new(StreamingTestSdf);
    let truncate_streaming = streaming_handler
        .stage_handler(URN_SDF_TRUNCATE_SIZED_RESTRICTIONS)
        .expect("truncate handler must be available for streaming SDF");

    let mut sink_streaming = Vec::<Vec<u8>>::new();
    let mut ctx_streaming = HandlerContext::new(&mut sink_streaming);
    let sized_streaming = (("stream".to_string(), OffsetRange::new(0, 100)), 100.0)
        .encode()
        .unwrap();

    truncate_streaming
        .instantiate()
        .process(&sized_streaming, &mut ctx_streaming)
        .expect("truncate process should succeed");

    assert_eq!(
        sink_streaming.len(),
        0,
        "Unbounded SDF must drop restriction during drain"
    );
}

/// Every way a split can interleave with a claim loop leaves the claimed offsets in the
/// primary and hands out only unclaimed ones in the residual.
///
/// `try_claim` and `try_split` each run under the tracker's lock, so any concurrent run
/// is equivalent to some sequential order. This test enumerates all of them: a split
/// after each possible claim count, at fractions from checkpoint (0) to all (1).
#[test]
fn a_split_at_any_point_of_a_claim_loop_partitions_the_range() {
    const END: i64 = 16;
    for claims_before_split in 0..=END {
        for fraction in [0.0, 0.25, 0.5, 0.99, 1.0] {
            let case = format!("split after {claims_before_split} claims at {fraction}");
            let tracker = OffsetRangeTracker::new(OffsetRange::new(0, END));
            for pos in 0..claims_before_split {
                assert!(tracker.try_claim(&pos), "{case}: claim {pos}");
            }
            assert_eq!(
                tracker.last_attempted(),
                (claims_before_split > 0).then(|| claims_before_split - 1),
                "{case}"
            );

            let split = tracker.try_split(fraction);

            let mut claimed = (0..claims_before_split).collect::<Vec<_>>();
            let mut pos = claims_before_split;
            while tracker.try_claim(&pos) {
                claimed.push(pos);
                pos += 1;
            }
            assert!(tracker.check_done().is_ok(), "{case}");

            let primary_end = match split {
                Some((primary, residual)) => {
                    assert_eq!(primary, OffsetRange::new(0, residual.start), "{case}");
                    assert_eq!(residual.end, END, "{case}");
                    assert!(
                        claims_before_split <= residual.start,
                        "{case}: residual {residual:?} overlaps a claimed offset"
                    );
                    residual.start
                }
                None => END,
            };
            assert_eq!(
                tracker.current_restriction(),
                OffsetRange::new(0, primary_end),
                "{case}"
            );
            assert_eq!(claimed, (0..primary_end).collect::<Vec<_>>(), "{case}");
        }
    }
}

/// One round of a claimer thread racing a splitter on real threads. Returns the last
/// offset claimed and the residual handed back, if any.
fn race_claim_against_split(end: i64) -> (Option<i64>, Option<OffsetRange>) {
    use std::sync::{Arc, Barrier};

    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, end));
    let barrier = Arc::new(Barrier::new(2));
    let claimer = {
        let tracker = tracker.clone();
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            let mut pos = 0;
            while tracker.try_claim(&pos) {
                pos += 1;
            }
        })
    };
    barrier.wait();
    let mut residual = None;
    // Bounded by the claimer, not by tracker state, so a broken tracker cannot hang it.
    while residual.is_none() && !claimer.is_finished() {
        residual = tracker.try_split(0.0).map(|(_, residual)| residual);
    }
    claimer.join().unwrap();
    (tracker.last_claimed(), residual)
}

/// Smoke test that the lock really serialises claims and splits across threads; the
/// interleavings themselves are covered deterministically above.
#[test]
fn claimed_offsets_never_overlap_a_concurrent_split_residual() {
    for round in 0..200 {
        let (last_claimed, residual) = race_claim_against_split(64);
        if let (Some(claimed), Some(residual)) = (last_claimed, residual) {
            assert!(
                claimed < residual.start,
                "round {round}: offset {claimed} was claimed but also given to residual {residual:?}"
            );
        }
    }
}

/// Counts the elements of a bundle in a field and emits the count at the end of the bundle.
/// Each element yields once with `resume()` before it finishes, so the handler checkpoints
/// and processes the residual. Copies share `calls`, the lifecycle record.
#[derive(Clone)]
struct CountingSdf {
    seen: i64,
    calls: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl CountingSdf {
    fn record(&self, call: &'static str) {
        if let Ok(mut calls) = self.calls.lock() {
            calls.push(call);
        }
    }
}

impl SplittableDoFn for CountingSdf {
    type In = String;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn setup(&mut self) -> Result {
        self.record("setup");
        Ok(())
    }

    fn start_bundle(&mut self) -> Result {
        self.record("start_bundle");
        self.seen = 0;
        Ok(())
    }

    fn initial_restriction(&self, _element: &String) -> OffsetRange {
        OffsetRange::new(0, 2)
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        _element: String,
        tracker: &OffsetRangeTracker,
        _ctx: &mut ProcessContext<'_, i64>,
    ) -> Result<ProcessContinuation> {
        // Claims one position per call, so the first call leaves work and resumes.
        let position = tracker.current_restriction().start;
        if !tracker.try_claim(&position) {
            return Ok(ProcessContinuation::stop());
        }
        if position == 0 {
            return Ok(ProcessContinuation::resume());
        }
        self.seen += 1;
        Ok(ProcessContinuation::stop())
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, i64>) -> Result {
        self.record("finish_bundle");
        ctx.emit(
            self.seen,
            BoundedWindow::Global(GlobalWindow),
            Instant::END_OF_GLOBAL_WINDOW,
        )
    }

    fn teardown(&mut self) -> Result {
        self.record("teardown");
        Ok(())
    }
}

#[test]
fn the_unexpanded_sdf_handler_runs_the_lifecycle_on_its_own_copy_and_resumes_residuals() {
    use beam::internals::{BundleHandler, SplittableDoFnHandler};

    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler = SplittableDoFnHandler::new(CountingSdf {
        seen: 0,
        calls: std::sync::Arc::clone(&calls),
    });
    let mut instance = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();

    instance.setup().expect("setup");
    instance.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink);
        for element in ["a", "b", "c"] {
            let encoded = element.to_string().encode().expect("encode");
            instance.process(&encoded, &mut ctx).expect("process");
        }
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    instance.teardown().expect("teardown");

    // Each element resumed once and finished on its residual; the count lives in the
    // instance across the bundle.
    let emitted: Vec<i64> = sink
        .iter()
        .map(|b| i64::decode(b).expect("decode"))
        .collect();
    assert_eq!(emitted, [3]);
    assert_eq!(
        *calls.lock().expect("calls"),
        ["setup", "start_bundle", "finish_bundle", "teardown"]
    );
}
