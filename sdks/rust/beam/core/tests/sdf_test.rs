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
use beam::transforms::ProcessContext;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionError, RestrictionTracker,
    SplittableDoFn, SplittableParDo, WatermarkedTracker,
};
use beam::windowing::ManualWatermarkEstimator;

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
    type Position = i64;
    type Tracker = WatermarkedTracker<OffsetRangeTracker, ManualWatermarkEstimator>;

    fn is_bounded(&self) -> bool {
        false
    }

    fn initial_restriction(&self, _element: &Self::In) -> Self::Restriction {
        OffsetRange::new(0, 100)
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        let tracker = OffsetRangeTracker::new(*restriction);
        let estimator = ManualWatermarkEstimator::new(42_000);
        WatermarkedTracker::new(tracker, estimator).with_bounded(false)
    }

    fn process_element(
        &self,
        _element: Self::In,
        tracker: &Self::Tracker,
        out: &mut ProcessContext<Self::Out>,
    ) -> Result<ProcessContinuation> {
        tracker.try_claim(&0);
        out.emit(100)?;
        tracker.estimator.set_watermark(55_000);
        Ok(ProcessContinuation::resume())
    }
}

#[test]
fn test_sdf_watermark_propagation_to_residual() {
    use beam::internals::BundleHandler;
    use beam::internals::{ResidualCollector, SdfProcessSizedElementsHandler};
    use std::sync::Arc;

    let mut handler = SdfProcessSizedElementsHandler {
        func: Arc::new(StreamingTestSdf),
    };

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
    assert!(!r.is_bounded);
    assert_eq!(r.output_watermarks.get("out"), Some(&55_000));
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
    type Position = i64;
    type Tracker = OffsetRangeTracker;

    fn is_bounded(&self) -> bool {
        true
    }

    fn initial_restriction(&self, _element: &Self::In) -> Self::Restriction {
        OffsetRange::new(0, 10)
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &self,
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
fn sdf_processes_a_multi_window_element_once_per_window() {
    use beam::coders::{PaneInfo, WindowedHeader};
    use beam::internals::{BundleHandler, ElementSink, SdfProcessSizedElementsHandler};
    use beam::windowing::{SlidingWindows, WindowFn};
    use std::sync::Arc;
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
    let windows = sliding.assign_windows_encoded(7_000);
    let header = WindowedHeader::new(7_000, &windows, PaneInfo::NO_FIRING);
    let mut handler = SdfProcessSizedElementsHandler {
        func: Arc::new(BoundedRangeSdf),
    };
    let mut sink = WindowedSink::default();
    let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
    let sized = (("e".to_string(), OffsetRange::new(0, 2)), 2.0)
        .encode()
        .unwrap();
    handler.process(&sized, &mut ctx).expect("process");
    drop(ctx);

    let expected: Vec<_> = windows
        .iter()
        .flat_map(|w| [(w.clone(), 0), (w.clone(), 1)])
        .collect();
    assert_eq!(sink.0, expected);
}

#[test]
fn test_splittable_pardo_with_side_inputs_graph_and_proto() {
    let p = Pipeline::new();
    let main_col = p.apply(Create::new("Main", vec!["test".to_string()]));
    let factor_view = p.apply(Create::new("Factor", vec![5_i32])).as_singleton();

    let out = main_col.apply(
        SplittableParDo::new("BoundedSdfWithSide", BoundedRangeSdf).with_side_input(&factor_view),
    );
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
    use std::sync::Arc;

    // Bounded SDF defaults to retaining the restriction during drain.
    let bounded_handler = SplittableDoFnHandler {
        func: Arc::new(BoundedRangeSdf),
    };
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
    let streaming_handler = SplittableDoFnHandler {
        func: Arc::new(StreamingTestSdf),
    };
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
