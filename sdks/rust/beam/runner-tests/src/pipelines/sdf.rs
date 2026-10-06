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

//! Splittable `DoFn`s: self-checkpointing, watermark estimation, side inputs, tagged
//! outputs, lifecycle order and late data.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, TestStream, passert};
use beam::transforms::FinishBundleContext;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    SplittableParDo, WatermarkedTracker,
};
use beam::windowing::{GlobalWindow, TimestampObservingWatermarkEstimator};

use crate::dofns::{AssignTimestampDoFn, CallOrder};

/// Emits `(key, offset)` for `0..count` in batches of `batch_size`, resuming after each
/// full batch.
#[derive(Clone)]
pub struct SelfCheckpointingSdf {
    pub batch_size: i64,
}

impl SplittableDoFn for SelfCheckpointingSdf {
    type In = (String, i64);
    type Out = (String, i64);
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, (_, count): &Self::In) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, *count))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        (key, _): Self::In,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result<ProcessContinuation> {
        let start = tracker.current_restriction().start;
        let claimed: Vec<i64> = (start..start + self.batch_size)
            .take_while(|offset| tracker.try_claim(offset))
            .collect();
        let emitted = claimed.len() as i64;
        claimed
            .into_iter()
            .try_for_each(|offset| ctx.emit((key.clone(), offset)))?;
        if emitted == self.batch_size {
            Ok(ProcessContinuation::Resume)
        } else {
            Ok(ProcessContinuation::Stop)
        }
    }
}

/// Checks that a self-checkpointing `SplittableDoFn` emits every offset once across
/// resumed continuations.
pub fn build_sdf_self_checkpoint(p: &TestPipeline) {
    let pairs = p
        .apply(
            "Create",
            Create::new(vec![("a".to_string(), 5i64), ("b".to_string(), 4i64)]),
        )
        .apply(
            "SelfCheckpointingSdf",
            SplittableParDo::new(SelfCheckpointingSdf { batch_size: 2 }),
        );
    let expected: Vec<(String, i64)> = (0..5)
        .map(|i| ("a".to_string(), i))
        .chain((0..4).map(|i| ("b".to_string(), i)))
        .collect();
    passert::that("AssertCheckpointed", &pairs).contains_in_any_order(expected);
}

/// Emits four timestamped `(key, value)` pairs in two checkpoints while advancing a
/// [`TimestampObservingWatermarkEstimator`].
#[derive(Clone)]
pub struct TimestampedSdf;

impl SplittableDoFn for TimestampedSdf {
    type In = String;
    type Out = (String, i64);
    type Restriction = OffsetRange;
    type Tracker = WatermarkedTracker<OffsetRangeTracker, TimestampObservingWatermarkEstimator>;

    fn initial_restriction(&self, _key: &Self::In) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, 4))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> Self::Tracker {
        WatermarkedTracker::new(
            OffsetRangeTracker::new(*restriction),
            TimestampObservingWatermarkEstimator::default(),
        )
    }

    fn process_element(
        &mut self,
        key: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result<ProcessContinuation> {
        let start = tracker.current_restriction().start;
        let claimed: Vec<i64> = (start..start + 2)
            .take_while(|i| tracker.try_claim(i))
            .collect();
        let emitted = claimed.len();
        for i in claimed {
            let ts = Instant::from_millis((i + 1) * 4_000);
            tracker.estimator.observe_timestamp(ts);
            ctx.output((key.clone(), (i + 1) * 10)).at(ts).emit()?;
        }
        if emitted == 2 {
            Ok(ProcessContinuation::Resume)
        } else {
            Ok(ProcessContinuation::Stop)
        }
    }
}

/// Checks that a `SplittableDoFn` with a watermark estimator advances the output
/// watermark across checkpoints and closes fixed windows.
pub fn build_sdf_watermark_estimator(p: &TestPipeline) {
    let sums = p
        .apply("Create", Create::new(vec!["k".to_string()]))
        .apply("TimestampedSdf", SplittableParDo::new(TimestampedSdf))
        .apply(
            "WindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .combine_per_key("SumPerWindow", Sum::new());

    passert::that_windowed("AssertWindowSums", &sums).contains_in_any_order([
        (
            ("k".to_string(), 30),
            IntervalWindow::new(Instant::EPOCH, Instant::from_millis(10_000)),
        ),
        (
            ("k".to_string(), 70),
            IntervalWindow::new(Instant::from_millis(10_000), Instant::from_millis(20_000)),
        ),
    ]);
}

/// Claims up to `size` offsets from the start of the restriction of `tracker`. The
/// continuation is `Resume` if it claimed all of them, so the rest goes to a checkpoint.
fn claim_batch(tracker: &OffsetRangeTracker, size: i64) -> (Vec<i64>, ProcessContinuation) {
    let start = tracker.current_restriction().start;
    let claimed: Vec<i64> = (start..start + size)
        .take_while(|offset| tracker.try_claim(offset))
        .collect();
    let continuation = if claimed.len() as i64 == size {
        ProcessContinuation::Resume
    } else {
        ProcessContinuation::Stop
    };
    (claimed, continuation)
}

/// Emits `(key, offset * factor)` for `0..count`, with `factor` from a singleton side
/// input. Claims `batch_size` offsets per call and resumes after each full batch.
#[derive(Clone)]
pub struct ScaledRangeSdf {
    pub factor: SingletonView<i64>,
    pub batch_size: i64,
}

impl SplittableDoFn for ScaledRangeSdf {
    type In = (String, i64);
    type Out = (String, i64);
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, (_, count): &Self::In) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, *count))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.factor]
    }

    fn process_element(
        &mut self,
        (key, _): Self::In,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result<ProcessContinuation> {
        let factor = ctx.side_input(&self.factor)?;
        let (claimed, continuation) = claim_batch(tracker, self.batch_size);
        claimed
            .into_iter()
            .try_for_each(|offset| ctx.emit((key.clone(), offset * factor)))?;
        Ok(continuation)
    }
}

/// Validates a `SplittableDoFn` that reads a singleton side input.
///
/// Java: `SplittableDoFnTest.testSideInputBounded`, `testSideInputUnbounded`.
pub fn build_sdf_side_input(p: &TestPipeline) {
    let factor = p.apply("Factor", Create::new(vec![10i64])).as_singleton();
    let scaled = p
        .apply(
            "Counts",
            Create::new(vec![("a".to_string(), 3i64), ("b".to_string(), 2i64)]),
        )
        .apply(
            "ScaledRange",
            SplittableParDo::new(ScaledRangeSdf {
                factor,
                batch_size: 100,
            }),
        );

    passert::that("AssertScaled", &scaled).contains_in_any_order([
        ("a".to_string(), 0),
        ("a".to_string(), 10),
        ("a".to_string(), 20),
        ("b".to_string(), 0),
        ("b".to_string(), 10),
    ]);
}

/// Applies [`ScaledRangeSdf`] to two keys in two fixed windows, with a factor of 10 in the
/// first window and 20 in the second, and checks the output of each window.
fn check_sdf_windowed_side_input(p: &TestPipeline, batch_size: i64) {
    let fixed = || WindowInto::new(FixedWindows::of(Duration::from_secs(10)));
    let factor = p
        .apply(
            "Factors",
            Create::new(vec![
                ("factor".to_string(), (10i64, 1_000i64)),
                ("factor".to_string(), (20, 11_000)),
            ]),
        )
        .par_do("TimestampFactors", AssignTimestampDoFn)
        .apply("WindowFactors", fixed())
        .map("FactorValue", |(_, v): (String, i64)| v)
        .as_singleton();
    let scaled = p
        .apply(
            "Counts",
            Create::new(vec![
                ("a".to_string(), (3i64, 2_000i64)),
                ("b".to_string(), (2, 12_000)),
            ]),
        )
        .par_do("TimestampCounts", AssignTimestampDoFn)
        .apply("WindowCounts", fixed())
        .apply(
            "ScaledRange",
            SplittableParDo::new(ScaledRangeSdf { factor, batch_size }),
        );

    let first = IntervalWindow::new(Instant::EPOCH, Instant::from_millis(10_000));
    let second = IntervalWindow::new(Instant::from_millis(10_000), Instant::from_millis(20_000));
    passert::that_windowed("AssertScaled", &scaled).contains_in_any_order([
        (("a".to_string(), 0), first),
        (("a".to_string(), 10), first),
        (("a".to_string(), 20), first),
        (("b".to_string(), 0), second),
        (("b".to_string(), 20), second),
    ]);
}

/// Validates a `SplittableDoFn` that reads the side input of the window of each element.
///
/// Java: `SplittableDoFnTest.testWindowedSideInputBounded`,
/// `testWindowedSideInputUnbounded`.
pub fn build_sdf_windowed_side_input(p: &TestPipeline) {
    check_sdf_windowed_side_input(p, 100);
}

/// Validates that a `SplittableDoFn` reads the side input of the same window after each
/// checkpoint.
///
/// Java: `SplittableDoFnTest.testWindowedSideInputWithCheckpointsBounded`,
/// `testWindowedSideInputWithCheckpointsUnbounded`.
pub fn build_sdf_windowed_side_input_checkpoints(p: &TestPipeline) {
    check_sdf_windowed_side_input(p, 1);
}

/// The outputs of [`TaggedRangeSdf`].
#[derive(Outputs)]
pub enum RangeOutputs {
    /// `(key, offset)` for each claimed offset.
    Main((String, i64)),
    /// `additional:<key>:<offset>` for each claimed offset.
    Additional(String),
}

/// Emits `(key, offset)` to the main output and `additional:key:offset` to the tagged
/// output for `0..count`, two offsets per call.
#[derive(Clone)]
pub struct TaggedRangeSdf;

impl SplittableDoFn for TaggedRangeSdf {
    type In = (String, i64);
    type Out = RangeOutputs;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, (_, count): &Self::In) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, *count))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        (key, _): Self::In,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result<ProcessContinuation> {
        let (claimed, continuation) = claim_batch(tracker, 2);
        claimed.into_iter().try_for_each(|offset| {
            ctx.emit(RangeOutputs::Additional(format!(
                "additional:{key}:{offset}"
            )))?;
            ctx.emit(RangeOutputs::Main((key.clone(), offset)))
        })?;
        Ok(continuation)
    }
}

/// Validates a `SplittableDoFn` with a main and a tagged output, across checkpoints.
///
/// Java: `SplittableDoFnTest.testAdditionalOutputBounded`, `testAdditionalOutputUnbounded`.
pub fn build_sdf_tagged_outputs(p: &TestPipeline) {
    let RangeOutputsPCollections { main, additional } = p
        .apply(
            "Counts",
            Create::new(vec![("a".to_string(), 3i64), ("b".to_string(), 1i64)]),
        )
        .apply("TaggedRange", SplittableParDo::new(TaggedRangeSdf));

    passert::that("AssertMain", &main).contains_in_any_order([
        ("a".to_string(), 0),
        ("a".to_string(), 1),
        ("a".to_string(), 2),
        ("b".to_string(), 0),
    ]);
    passert::that("AssertAdditional", &additional).contains_in_any_order(
        [
            "additional:a:0",
            "additional:a:1",
            "additional:a:2",
            "additional:b:0",
        ]
        .map(String::from),
    );
}

/// Fails the bundle if a lifecycle method runs out of order. Emits `process:word:offset`
/// for each character offset of a word, two per call, and `finish:n` per bundle, `n`
/// being the number of offsets that the bundle claimed.
#[derive(Clone, Default)]
pub struct LifecycleSdf {
    calls: CallOrder,
}

impl SplittableDoFn for LifecycleSdf {
    type In = String;
    type Out = String;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, word: &String) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, word.len() as i64))
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn setup(&mut self) -> Result {
        self.calls.setup()
    }

    fn start_bundle(&mut self) -> Result {
        self.calls.start_bundle()
    }

    fn process_element(
        &mut self,
        word: String,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, String>,
    ) -> Result<ProcessContinuation> {
        let (claimed, continuation) = claim_batch(tracker, 2);
        claimed.into_iter().try_for_each(|offset| {
            self.calls.process(&format!("process_element({word})"))?;
            ctx.emit(format!("process:{word}:{offset}"))
        })?;
        Ok(continuation)
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, String>) -> Result {
        let claimed = self.calls.finish_bundle()?;
        // The pipeline is globally windowed.
        ctx.emit(
            format!("finish:{claimed}"),
            BoundedWindow::Global(GlobalWindow),
            Instant::END_OF_GLOBAL_WINDOW,
        )
    }
}

/// Validates the lifecycle order of a `SplittableDoFn` across checkpoints: `setup`, then
/// `start_bundle`, `process_element` and `finish_bundle` for each bundle.
///
/// Java: `SplittableDoFnTest.testLifecycleMethodsBounded`, `testLifecycleMethodsUnbounded`.
pub fn build_sdf_lifecycle(p: &TestPipeline) {
    let out = p
        .apply(
            "Words",
            Create::new(vec!["ab".to_string(), "cde".to_string()]),
        )
        .apply("Lifecycle", SplittableParDo::new(LifecycleSdf::default()));

    passert::that("AssertLifecycle", &out).satisfies(|lines: &[String]| {
        let mut processed: Vec<&str> = lines
            .iter()
            .filter_map(|line| line.strip_prefix("process:"))
            .collect();
        processed.sort_unstable();
        if processed != ["ab:0", "ab:1", "cde:0", "cde:1", "cde:2"] {
            return Err(format!("each offset must be processed once: {lines:?}").into());
        }
        let finished: Vec<usize> = lines
            .iter()
            .filter_map(|line| line.strip_prefix("finish:"))
            .map(|n| n.parse().map_err(|e| format!("bad finish line {n}: {e}")))
            .collect::<std::result::Result<_, _>>()?;
        if finished.is_empty() || finished.iter().sum::<usize>() != processed.len() {
            return Err(format!("finish_bundle must count every claimed offset: {lines:?}").into());
        }
        if processed.len() + finished.len() != lines.len() {
            return Err(format!("unexpected output: {lines:?}").into());
        }
        Ok(())
    });
}

/// Validates that a `SplittableDoFn` processes late data within the allowed lateness,
/// and that the outputs keep the window of the late element. Prism only.
///
/// Java: `SplittableDoFnTest.testLateData`.
pub fn build_sdf_late_data(p: &TestPipeline) {
    let pairs = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([(
                    ("on_time".to_string(), 2i64),
                    Instant::from_millis(1_000),
                )])
                .advance_watermark_to(Instant::from_millis(15_000))
                .add_timestamped_elements([(
                    ("late".to_string(), 3i64),
                    Instant::from_millis(2_000),
                )])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
                .with_allowed_lateness(Duration::from_secs(60)),
        )
        .apply(
            "SelfCheckpointingSdf",
            SplittableParDo::new(SelfCheckpointingSdf { batch_size: 1 }),
        );

    let window = IntervalWindow::new(Instant::EPOCH, Instant::from_millis(10_000));
    passert::that_windowed("AssertLateOutputs", &pairs).contains_in_any_order([
        (("on_time".to_string(), 0), window),
        (("on_time".to_string(), 1), window),
        (("late".to_string(), 0), window),
        (("late".to_string(), 1), window),
        (("late".to_string(), 2), window),
    ]);
}
