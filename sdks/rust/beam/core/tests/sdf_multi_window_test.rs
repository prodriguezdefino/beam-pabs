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

//! A window-observing SDF over a multi-window element: a checkpoint or a dynamic split in the
//! first window hands the rest of that window and the remaining windows to the runner.

use std::sync::{Arc, Mutex};

use beam::coders::{DefaultCoder, PaneInfo, WindowedHeader};
use beam::internals::{
    BundleHandler, DynamicSplitRegistrar, DynamicSplitResult, ElementSink, HandlerContext,
    ResidualCollector, SdfProcessSizedElementsHandler,
};
use beam::prelude::*;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    WatermarkedTracker,
};
use beam::windowing::ManualWatermarkEstimator;

const TIMESTAMP_MILLIS: i64 = 5_000;

fn window(i: i64) -> IntervalWindow {
    IntervalWindow::new(
        Instant::from_millis(i * 10_000),
        Instant::from_millis((i + 1) * 10_000),
    )
}

fn encoded_window(i: i64) -> Vec<u8> {
    let mut bytes = Vec::new();
    BoundedWindow::Interval(window(i))
        .encode(&mut bytes)
        .expect("encoding into a Vec cannot fail");
    bytes
}

/// The header of the input under windows `windows`.
fn header(windows: &[i64]) -> WindowedHeader {
    let windows: Vec<_> = windows.iter().map(|&i| encoded_window(i)).collect();
    WindowedHeader::new(TIMESTAMP_MILLIS, &windows, PaneInfo::NO_FIRING)
}

/// The sized input element: `(("e", [0, 10)), 10.0)`.
fn sized_element() -> Vec<u8> {
    (
        ("e".to_string(), (OffsetRange::new(0, 10), Vec::<u8>::new())),
        10.0,
    )
        .encode()
        .expect("encode the input")
}

/// Records the window of each output.
#[derive(Default)]
struct WindowRecorder(Vec<(BoundedWindow, i64)>);

impl ElementSink for WindowRecorder {
    fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
        Err("a window-observing SDF pushes under a single-window header".to_string())
    }

    fn push_windowed(
        &mut self,
        header: &WindowedHeader,
        element: Vec<u8>,
    ) -> std::result::Result<(), String> {
        let window = header.single_window().map_err(|e| e.to_string())?;
        let value = i64::decode(&element).map_err(|e| e.to_string())?;
        self.0.push((window, value));
        Ok(())
    }
}

/// Splits `residual` into its windowed-value header bytes and its sized restriction.
fn split_residual(residual: &[u8], expected_header: &WindowedHeader) -> (String, OffsetRange) {
    let header = expected_header.as_bytes();
    assert!(
        residual.starts_with(header),
        "the residual starts with the header of its windows"
    );
    let ((value, (restriction, _)), _size) =
        <((String, (OffsetRange, Vec<u8>)), f64)>::decode(&residual[header.len()..])
            .expect("decode");
    (value, restriction)
}

/// Claims one offset per window, then checkpoints.
#[derive(Clone)]
struct CheckpointingSdf;

impl SplittableDoFn for CheckpointingSdf {
    type In = Windowed<String>;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, _element: &Windowed<String>) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, 10))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        element: Windowed<String>,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, i64>,
    ) -> Result<ProcessContinuation> {
        let start = tracker.current_restriction().start;
        if tracker.try_claim(&start) {
            let window_start = element.window.interval().expect("interval").start;
            ctx.emit(window_start.as_millis() + start)?;
        }
        Ok(ProcessContinuation::Resume)
    }
}

#[test]
fn a_checkpoint_in_the_first_window_defers_the_rest_and_the_remaining_windows() {
    let residuals = Arc::new(ResidualCollector::new());
    let input_header = header(&[0, 1, 2]);
    let mut sink = WindowRecorder::default();
    let mut handler = SdfProcessSizedElementsHandler::new(CheckpointingSdf);
    {
        let mut ctx = HandlerContext::new(&mut sink)
            .with_transform_id("sdf")
            .with_header(&input_header)
            .with_residual_collector(Some(&residuals));
        handler
            .process(&sized_element(), &mut ctx)
            .expect("process");
    }

    assert_eq!(
        sink.0,
        vec![(BoundedWindow::Interval(window(0)), 0)],
        "only the first window is processed"
    );

    let residuals = residuals.drain();
    assert_eq!(residuals.len(), 2);
    assert!(residuals.iter().all(|r| r.transform_id == "sdf"));
    // The rest of the first window.
    assert_eq!(
        split_residual(&residuals[0].element, &header(&[0])),
        ("e".to_string(), OffsetRange::new(1, 10))
    );
    // The windows not processed yet, with the initial restriction.
    assert_eq!(
        split_residual(&residuals[1].element, &header(&[1, 2])),
        ("e".to_string(), OffsetRange::new(0, 10))
    );
}

/// Claims the first offset, then asks the active splitter for a split that keeps nothing more,
/// as a runner would during processing.
#[derive(Clone)]
struct SelfSplittingSdf {
    registrar: DynamicSplitRegistrar,
    splits: Arc<Mutex<Vec<DynamicSplitResult>>>,
}

impl SplittableDoFn for SelfSplittingSdf {
    type In = Windowed<String>;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, _element: &Windowed<String>) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, 10))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        element: Windowed<String>,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, i64>,
    ) -> Result<ProcessContinuation> {
        let window_start = element.window.interval().expect("interval").start;
        let mut offset = tracker.current_restriction().start;
        while tracker.try_claim(&offset) {
            ctx.emit(window_start.as_millis() + offset)?;
            if offset == 0 {
                let splitter = self.registrar.current_handler().expect("active splitter");
                let split = splitter.try_split(0.0).expect("split");
                self.splits.lock().expect("splits lock").push(split);
            }
            offset += 1;
        }
        Ok(ProcessContinuation::Stop)
    }
}

#[test]
fn a_dynamic_split_in_the_first_window_returns_two_residuals_and_stops_after_it() {
    let registrar = DynamicSplitRegistrar::new();
    let splits = Arc::new(Mutex::new(Vec::new()));
    let sdf = SelfSplittingSdf {
        registrar: registrar.clone(),
        splits: Arc::clone(&splits),
    };
    let registrar = Arc::new(registrar);
    let input_header = header(&[0, 1, 2]);
    let mut sink = WindowRecorder::default();
    let mut handler = SdfProcessSizedElementsHandler::new(sdf);
    {
        let mut ctx = HandlerContext::new(&mut sink)
            .with_transform_id("sdf")
            .with_header(&input_header)
            .with_dynamic_split_registrar(Some(&registrar));
        handler
            .process(&sized_element(), &mut ctx)
            .expect("process");
    }

    assert_eq!(
        sink.0,
        vec![(BoundedWindow::Interval(window(0)), 0)],
        "processing stops after the first window"
    );
    assert!(
        registrar.current_handler().is_none(),
        "the splitter is unregistered"
    );

    let splits = splits.lock().unwrap();
    let [split] = splits.as_slice() else {
        panic!("expected one split, got {}", splits.len());
    };
    let primary = split.primary.clone();
    assert_eq!(primary.transform_id, "sdf");
    assert_eq!(
        split_residual(&primary.element, &header(&[0])),
        ("e".to_string(), OffsetRange::new(0, 1))
    );

    let roots: Vec<_> = split.residuals.iter().map(|r| r.element.clone()).collect();
    assert_eq!(roots.len(), 2, "the rest of window 1, and windows 2 and 3");
    assert_eq!(
        split_residual(&roots[0], &header(&[0])),
        ("e".to_string(), OffsetRange::new(1, 10))
    );
    assert_eq!(
        split_residual(&roots[1], &header(&[1, 2])),
        ("e".to_string(), OffsetRange::new(0, 10))
    );
}

/// Starts at a 42 s watermark, claims one offset, moves the watermark to 55 s, then checkpoints.
#[derive(Clone)]
struct WatermarkingSdf;

impl SplittableDoFn for WatermarkingSdf {
    type In = Windowed<String>;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = WatermarkedTracker<OffsetRangeTracker, ManualWatermarkEstimator>;

    fn initial_restriction(&self, _element: &Windowed<String>) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, 10))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> Self::Tracker {
        let estimator = ManualWatermarkEstimator::new(Instant::from_millis(42_000));
        WatermarkedTracker::new(OffsetRangeTracker::new(*restriction), estimator)
    }

    fn process_element(
        &mut self,
        _element: Windowed<String>,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, i64>,
    ) -> Result<ProcessContinuation> {
        if tracker.try_claim(&0) {
            ctx.emit(0)?;
        }
        tracker
            .estimator
            .set_watermark(Instant::from_millis(55_000));
        Ok(ProcessContinuation::Resume)
    }
}

#[test]
fn the_remaining_windows_keep_the_watermark_from_before_the_element_ran() {
    let residuals = Arc::new(ResidualCollector::new());
    let input_header = header(&[0, 1]);
    let mut sink = WindowRecorder::default();
    let mut handler = SdfProcessSizedElementsHandler::new(WatermarkingSdf);
    {
        let mut ctx = HandlerContext::new(&mut sink)
            .with_transform_id("sdf")
            .with_header(&input_header)
            .with_residual_collector(Some(&residuals));
        handler
            .process(&sized_element(), &mut ctx)
            .expect("process");
    }

    let watermarks: Vec<_> = residuals
        .drain()
        .into_iter()
        .map(|r| r.output_watermark)
        .collect();
    // The rest of window 0 resumes at the current watermark. Window 1 has not started, so
    // it can still output at the watermark from before the element ran.
    assert_eq!(
        watermarks,
        vec![
            Some(Instant::from_millis(55_000)),
            Some(Instant::from_millis(42_000))
        ]
    );
}
