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

use std::sync::Arc;

use apache_beam_core::coders::{DefaultCoder, PaneInfo, WindowedHeader};
use apache_beam_core::internals::{DynamicSplitHandler, DynamicSplitRegistrar, SdfDynamicSplitter};
use apache_beam_core::transforms::ProcessContext;
use apache_beam_core::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, SplittableDoFn,
};

type Sized = ((String, OffsetRange), f64);

#[derive(Clone)]
struct TestRangeSdf;

impl SplittableDoFn for TestRangeSdf {
    type In = String;
    type Out = i64;
    type Restriction = OffsetRange;
    type Position = i64;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, _element: &Self::In) -> Self::Restriction {
        OffsetRange::new(0, 100)
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
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result<ProcessContinuation> {
        let mut pos = tracker.current_restriction().start;
        while tracker.try_claim(&pos) {
            ctx.emit(pos)?;
            pos += 1;
        }
        Ok(ProcessContinuation::stop())
    }
}

#[test]
fn test_dynamic_split_registrar_and_guard_lifecycle() {
    let registrar = DynamicSplitRegistrar::new();
    assert!(registrar.current_handler().is_none());

    let func = Arc::new(TestRangeSdf);
    let tracker = Arc::new(OffsetRangeTracker::new(OffsetRange::new(0, 100)));
    let splitter = Arc::new(SdfDynamicSplitter::new(
        "test-sdf-transform",
        func,
        tracker,
        "input".to_string(),
        WindowedHeader::EMPTY.clone(),
    ));

    {
        let guard = registrar.register(splitter);
        assert!(registrar.current_handler().is_some());
        let current = registrar.current_handler().unwrap();
        assert_eq!(current.transform_id(), "test-sdf-transform");
        drop(guard);
    }

    // After guard is dropped, handler is automatically unregistered.
    assert!(registrar.current_handler().is_none());
}

#[test]
fn test_sdf_dynamic_splitter_splits_restriction_and_updates_tracker() {
    let func = Arc::new(TestRangeSdf);
    let tracker = Arc::new(OffsetRangeTracker::new(OffsetRange::new(0, 100)));

    // Claim initial 20 elements.
    for pos in 0..20 {
        assert!(tracker.try_claim(&pos));
    }

    let splitter = SdfDynamicSplitter::new(
        "test-sdf-transform",
        func,
        tracker.clone(),
        "input".to_string(),
        WindowedHeader::EMPTY.clone(),
    );

    let progress = splitter.current_progress();
    assert!((progress.fraction_completed() - 0.2).abs() < 1e-4);

    // Split remaining range [20, 100) at 50% fraction -> primary [0, 60), residual [60, 100).
    let split_res = splitter.try_split(0.5).expect("Split should succeed");

    assert_eq!(split_res.primary.transform_id, "test-sdf-transform");
    assert_eq!(split_res.primary.input_id, "in");
    let delayed_app = split_res.residual.clone();
    let residual_app = delayed_app
        .application
        .expect("Residual application must exist");
    assert_eq!(residual_app.transform_id, "test-sdf-transform");
    assert_eq!(residual_app.input_id, "in");
    assert_eq!(delayed_app.requested_time_delay, None);

    // Both halves carry the element with its sized restriction, and no header.
    assert_eq!(
        decode_sized(&split_res.primary.element),
        (("input".to_string(), OffsetRange::new(0, 60)), 60.0)
    );
    assert_eq!(
        decode_sized(&residual_app.element),
        (("input".to_string(), OffsetRange::new(60, 100)), 40.0)
    );

    // Tracker restriction shrinks to [0, 60).
    assert_eq!(tracker.current_restriction(), OffsetRange::new(0, 60));

    for pos in 20..60 {
        assert!(tracker.try_claim(&pos));
    }

    // Position 60 belongs to the residual and must fail try_claim.
    assert!(!tracker.try_claim(&60));

    // Restriction check_done succeeds when [0, 60) is fully claimed.
    assert!(tracker.check_done().is_ok());
}

fn decode_sized(bytes: &[u8]) -> Sized {
    Sized::decode(bytes).expect("element must decode as ((String, OffsetRange), f64)")
}

#[test]
fn dynamic_split_prefixes_both_halves_with_the_windowed_header() {
    let tracker = Arc::new(OffsetRangeTracker::new(OffsetRange::new(10, 20)));
    assert!(tracker.try_claim(&10));
    let header = WindowedHeader::global(1_234, PaneInfo::NO_FIRING);
    let splitter = SdfDynamicSplitter::new(
        "sdf",
        Arc::new(TestRangeSdf),
        tracker.clone(),
        "elem".to_string(),
        header.clone(),
    );

    let split = splitter.try_split(0.0).expect("split at the next offset");
    let residual = split.residual.application.expect("residual application");
    let h = header.as_bytes();
    assert!(!h.is_empty());
    for (element, expected) in [
        (
            &split.primary.element,
            (("elem".to_string(), OffsetRange::new(10, 11)), 1.0),
        ),
        (
            &residual.element,
            (("elem".to_string(), OffsetRange::new(11, 20)), 9.0),
        ),
    ] {
        assert_eq!(&element[..h.len()], h, "header prefix");
        assert_eq!(decode_sized(&element[h.len()..]), expected);
    }
    // An offset tracker has no watermark, and bounded-ness follows the tracker.
    assert!(residual.output_watermarks.is_empty());
    assert_eq!(
        residual.is_bounded,
        model::pipeline::is_bounded::Enum::Bounded as i32
    );
    assert_eq!(tracker.current_restriction(), OffsetRange::new(10, 11));
}

#[test]
fn dynamic_split_after_everything_is_claimed_returns_none() {
    let tracker = Arc::new(OffsetRangeTracker::new(OffsetRange::new(0, 3)));
    for pos in 0..3 {
        assert!(tracker.try_claim(&pos));
    }
    let splitter = SdfDynamicSplitter::new(
        "sdf",
        Arc::new(TestRangeSdf),
        tracker.clone(),
        "elem".to_string(),
        WindowedHeader::EMPTY.clone(),
    );
    assert!(splitter.try_split(0.5).is_none());
    assert_eq!(tracker.current_restriction(), OffsetRange::new(0, 3));
}

fn splitter_named(name: &str) -> Arc<dyn DynamicSplitHandler> {
    Arc::new(SdfDynamicSplitter::new(
        name,
        Arc::new(TestRangeSdf),
        Arc::new(OffsetRangeTracker::new(OffsetRange::new(0, 10))),
        "elem".to_string(),
        WindowedHeader::EMPTY.clone(),
    ))
}

fn current_id(registrar: &DynamicSplitRegistrar) -> Option<String> {
    registrar
        .current_handler()
        .map(|h| h.transform_id().to_string())
}

#[test]
fn dropping_an_outdated_guard_keeps_the_newer_handler() {
    let registrar = DynamicSplitRegistrar::new();
    let first = registrar.register(splitter_named("first"));
    let second = registrar.register(splitter_named("second"));
    assert_eq!(current_id(&registrar).as_deref(), Some("second"));

    // Dropping an inactive registration guard preserves the active handler.
    drop(first);
    assert_eq!(current_id(&registrar).as_deref(), Some("second"));

    drop(second);
    assert_eq!(current_id(&registrar), None);
}

#[test]
fn dropping_guards_in_registration_order_clears_the_registrar() {
    let registrar = DynamicSplitRegistrar::new();
    let first = registrar.register(splitter_named("first"));
    let second = registrar.register(splitter_named("second"));
    drop(second);
    assert_eq!(current_id(&registrar), None);
    drop(first);
    assert_eq!(current_id(&registrar), None);

    // Explicit unregister clears whatever is registered.
    let _guard = registrar.register(splitter_named("third"));
    registrar.unregister();
    assert_eq!(current_id(&registrar), None);
}
