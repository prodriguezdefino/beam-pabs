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

//! Triggers: early, on-time and late panes, and the accumulation modes.
//!
//! A batch source delivers everything at once and cannot show early firings. These
//! validators drive arrival with a [`TestStream`] and assert on each pane separately.

use std::time::Duration;

use beam::coders::{DefaultCoder, GlobalWindow, Timing};
use beam::prelude::*;
use beam::testing::{TestPipeline, TestStream, passert};
use beam::windowing::{ClosingBehavior, OnTimeBehavior, OutputTime};

use super::core_transforms::sorted_groups;
use crate::dofns::TimeSortedDeltaDoFn;

/// The one window every element here falls into.
const WINDOW: IntervalWindow = IntervalWindow {
    start: Instant::EPOCH,
    end: Instant::from_millis(10_000),
};

fn keyed(value: i64, timestamp: i64) -> ((String, i64), Instant) {
    (("k".to_string(), value), Instant::from_millis(timestamp))
}

fn fixed_windows() -> WindowInto<FixedWindows> {
    WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
}

/// Validates a repeated count trigger in discarding mode: every two elements fire a pane
/// holding exactly those two, before the window closes.
pub fn build_trigger_repeated_count_discarding(p: &TestPipeline) {
    let panes = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([keyed(1, 1_000), keyed(2, 2_000)])
                .advance_watermark_to(Instant::from_millis(5_000))
                .add_timestamped_elements([keyed(3, 3_000), keyed(4, 4_000)])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            fixed_windows()
                .with_trigger(Trigger::repeatedly(Trigger::after_count(2)))
                .with_accumulation_mode(AccumulationMode::Discarding),
        )
        .group_by_key("Group");
    let panes = sorted_groups(&panes, "SortPanes");

    passert::that("EarlyPanes", &panes)
        .in_early_panes(WINDOW)
        .contains_in_any_order([("k".to_string(), vec![1, 2]), ("k".to_string(), vec![3, 4])]);
    // No other pane fired, in particular no on-time pane repeating discarded elements.
    passert::that("WholeWindow", &panes)
        .in_window(WINDOW)
        .has_count(2);
}

/// Validates early firings with accumulating panes: each early pane includes all the
/// elements seen so far, and the on-time pane the full total.
pub fn build_trigger_early_firings_accumulating(p: &TestPipeline) {
    let sums = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([keyed(10, 1_000), keyed(20, 2_000)])
                .advance_watermark_to(Instant::from_millis(5_000))
                .add_timestamped_elements([keyed(30, 3_000), keyed(40, 4_000)])
                .advance_watermark_to(Instant::from_millis(6_000))
                // Too few to fire early; only the on-time pane picks it up.
                .add_timestamped_elements([keyed(50, 4_500)])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            fixed_windows()
                .with_trigger(
                    Trigger::after_end_of_window().with_early_firings(Trigger::after_count(2)),
                )
                .with_accumulation_mode(AccumulationMode::Accumulating),
        )
        .group_by_key("Group")
        .map("Sum", |(k, v): (String, BeamIterable<i64>)| {
            (
                k,
                v.into_iter()
                    .sum::<Result<i64>>()
                    .expect("group values decode"),
            )
        });

    passert::that("EarlyPanes", &sums)
        .in_early_panes(WINDOW)
        .contains_in_any_order([("k".to_string(), 30), ("k".to_string(), 100)]);
    passert::that("OnTimePane", &sums)
        .in_on_time_pane(WINDOW)
        .contains_in_any_order([("k".to_string(), 150)]);
    passert::that("FinalPane", &sums)
        .in_final_pane(WINDOW)
        .contains_in_any_order([("k".to_string(), 150)]);
}

/// Validates late firings within the allowed lateness, in discarding mode.
///
/// The late trigger needs three elements, but only two late ones arrive. They are emitted
/// together in one pane when the window expires at the end of the stream.
pub fn build_trigger_late_firings_discarding(p: &TestPipeline) {
    let panes = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([keyed(1, 1_000)])
                .advance_watermark_to(Instant::from_millis(20_000))
                // Behind the watermark, but within the allowed lateness.
                .add_timestamped_elements([keyed(2, 2_000)])
                .advance_watermark_to(Instant::from_millis(21_000))
                .add_timestamped_elements([keyed(3, 3_000)])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            fixed_windows()
                .with_trigger(
                    Trigger::after_end_of_window().with_late_firings(Trigger::after_count(3)),
                )
                .with_accumulation_mode(AccumulationMode::Discarding)
                .with_allowed_lateness(Duration::from_secs(3_600)),
        )
        .group_by_key("Group");
    let panes = sorted_groups(&panes, "SortPanes");

    passert::that("OnTimePane", &panes)
        .in_on_time_pane(WINDOW)
        .contains_in_any_order([("k".to_string(), vec![1])]);
    passert::that("LatePanes", &panes)
        .in_late_panes(WINDOW)
        .contains_in_any_order([("k".to_string(), vec![2, 3])]);
    passert::that("NoEarlyPanes", &panes)
        .in_early_panes(WINDOW)
        .empty();
}

#[derive(State)]
struct TestStreamSumState {
    sum: ValueState<i64>,
}

#[derive(Timers)]
enum TestStreamTimer {
    #[timer(event_time)]
    EarlyCheck,
    #[timer(event_time)]
    WindowEnd,
}

#[derive(Clone)]
struct TestStreamStatefulDoFn;

impl StatefulDoFn for TestStreamStatefulDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, String);
    type State = TestStreamSumState;
    type Timer = TestStreamTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let next = state.sum.read()?.unwrap_or(0) + value;
        state.sum.write(next)?;
        if ctx.timestamp().as_millis() < 5_000 {
            ctx.timer(TestStreamTimer::EarlyCheck)
                .at(Instant::from_millis(4_000))?;
        }
        ctx.timer(TestStreamTimer::WindowEnd)
            .at(Instant::from_millis(9_999))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let total = state.sum.read()?.unwrap_or(0);
        match timer {
            TestStreamTimer::EarlyCheck => ctx.emit((key.clone(), format!("early:{total}"))),
            TestStreamTimer::WindowEnd => {
                ctx.emit((key.clone(), format!("final:{total}")))?;
                state.sum.clear()
            }
        }
    }
}

/// Validates windowed user state and event-time timers with [`TestStream`].
pub fn build_test_stream_state_and_timers(p: &TestPipeline) {
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([keyed(10, 1_000), keyed(20, 2_000)])
                .advance_watermark_to(Instant::from_millis(5_000))
                .add_timestamped_elements([keyed(5, 6_000)])
                .advance_watermark_to_infinity(),
        )
        .apply("FixedWindows", fixed_windows())
        .apply("StatefulDoFn", ParDo::new(TestStreamStatefulDoFn));

    passert::that_windowed("AssertWindowedOut", &out).contains_in_any_order([
        (("k".to_string(), "early:30".to_string()), WINDOW),
        (("k".to_string(), "final:35".to_string()), WINDOW),
    ]);
}

/// Builds the pane metadata pipeline of [`build_test_stream_pane_info`] and
/// [`build_test_stream_pane_info_accumulating`] with `mode`.
fn pane_info_panes(p: &TestPipeline, mode: AccumulationMode) -> PCollection<(i64, String)> {
    p.apply(
        "TestStream",
        TestStream::new()
            .add_timestamped_elements([keyed(1, 1_000), keyed(2, 2_000)])
            .advance_watermark_to(Instant::from_millis(5_000))
            .add_timestamped_elements([keyed(4, 3_000)])
            .advance_watermark_to(Instant::from_millis(20_000))
            .add_timestamped_elements([keyed(8, 4_000)])
            .advance_watermark_to_infinity(),
    )
    .apply(
        "FixedWindows",
        fixed_windows()
            .with_trigger(
                Trigger::after_end_of_window()
                    .with_early_firings(Trigger::after_count(2))
                    .with_late_firings(Trigger::after_count(1)),
            )
            .with_accumulation_mode(mode)
            .with_allowed_lateness(Duration::from_secs(60)),
    )
    .group_by_key("Group")
    .map("Sum", |(_k, v): (String, BeamIterable<i64>)| {
        v.into_iter()
            .sum::<Result<i64>>()
            .expect("group values decode")
    })
    .par_do_fn("InspectPane", |sum: i64, ctx| {
        let pane = ctx.pane();
        ctx.emit((
            sum,
            format!(
                "{:?}:first={}:index={:?}:non_spec={:?}",
                pane.timing(),
                pane.is_first(),
                pane.index(),
                pane.on_time_index(),
            ),
        ))
    })
}

/// Validates pane metadata (`Timing`, `is_first`, `index` and `on_time_index`) across
/// early, on-time and late trigger firings in discarding mode.
pub fn build_test_stream_pane_info(p: &TestPipeline) {
    let panes = pane_info_panes(p, AccumulationMode::Discarding);

    passert::that("AssertPanes", &panes)
        .in_window(WINDOW)
        .contains_in_any_order([
            (
                3,
                "Early:first=true:index=Some(0):non_spec=None".to_string(),
            ),
            (
                4,
                "OnTime:first=false:index=Some(1):non_spec=Some(0)".to_string(),
            ),
            (
                8,
                "Late:first=false:index=Some(2):non_spec=Some(1)".to_string(),
            ),
        ]);
}

/// Validates pane metadata (`Timing`, `is_first`, `index` and `on_time_index`) across
/// early, on-time and late trigger firings in accumulating mode.
pub fn build_test_stream_pane_info_accumulating(p: &TestPipeline) {
    let panes = pane_info_panes(p, AccumulationMode::Accumulating);

    passert::that("AssertPanes", &panes)
        .in_window(WINDOW)
        .contains_in_any_order([
            (
                3,
                "Early:first=true:index=Some(0):non_spec=None".to_string(),
            ),
            (
                7,
                "OnTime:first=false:index=Some(1):non_spec=Some(0)".to_string(),
            ),
            (
                15,
                "Late:first=false:index=Some(2):non_spec=Some(1)".to_string(),
            ),
        ]);
}

// Late data, processing-time triggers, chained stages and time-sorted input.

/// Groups `input` under one key and gives back its values, in the pane of each firing.
fn grouped_values<T: DefaultCoder>(input: &PCollection<T>, name: &str) -> PCollection<T> {
    input
        .map(format!("{name}Key"), |value: T| (1i64, value))
        .group_by_key(format!("{name}Group"))
        .par_do_fn(
            format!("{name}Values"),
            |(_key, values): (i64, BeamIterable<T>), ctx| {
                values.into_iter().try_for_each(|value| ctx.emit(value?))
            },
        )
}

/// Checks that every value of a pane is in `range`.
fn all_in<T: PartialOrd + std::fmt::Debug>(
    values: &[T],
    range: std::ops::RangeInclusive<T>,
) -> Result {
    if values.iter().all(|value| range.contains(value)) {
        Ok(())
    } else {
        Err(format!("every value must be in {range:?}: {values:?}").into())
    }
}

/// Validates late panes in accumulating mode: late data within the allowed lateness joins
/// the final pane, and droppable data never shows up. Java: `TestStreamTest.testLateDataAccumulating`.
pub fn build_trigger_late_firings_accumulating(p: &TestPipeline) {
    let minutes = |m: u64| Duration::from_secs(60 * m);
    let at_zero = |values: &[i64]| -> Vec<(i64, Instant)> {
        values
            .iter()
            .map(|value| (*value, Instant::EPOCH))
            .collect()
    };
    let windowed = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements(at_zero(&[1, 2, 3]))
                .advance_watermark_to(Instant::EPOCH + minutes(6))
                // Late, but within the allowed lateness.
                .add_timestamped_elements(at_zero(&[4, 5]))
                .advance_watermark_to(Instant::EPOCH + minutes(20))
                // Droppable: the window expired.
                .add_timestamped_elements(at_zero(&[-1, -2, -3]))
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            WindowInto::new(FixedWindows::of(minutes(5)))
                .with_trigger(
                    Trigger::after_end_of_window()
                        .with_early_firings(Trigger::after_processing_time(minutes(2)))
                        .with_late_firings(Trigger::after_count(1)),
                )
                .with_accumulation_mode(AccumulationMode::Accumulating)
                .with_allowed_lateness(minutes(5))
                .with_closing_behavior(ClosingBehavior::EmitAlways),
        );
    let triggered = grouped_values(&windowed, "Triggered");
    let count = windowed.apply("Count", CountGlobally::new().without_defaults());
    let sum = windowed.apply("Sum", CombineGlobally::new(Sum::new()).without_defaults());

    let window = IntervalWindow::new(Instant::EPOCH, Instant::EPOCH + minutes(5));
    passert::that("FinalPane", &triggered)
        .in_final_pane(window)
        .contains_in_any_order([1, 2, 3, 4, 5]);
    passert::that("OnTimePane", &triggered)
        .in_on_time_pane(window)
        .contains_in_any_order([1, 2, 3]);
    passert::that("Counts", &count)
        .in_window(window)
        .satisfies(|counts: &[i64]| all_in(counts, 3..=5));
    passert::that("Sums", &sum)
        .in_window(window)
        .satisfies(|sums: &[i64]| all_in(sums, 6..=15));
}

/// Validates that a first element that is droppable does not open its window, and that
/// the on-time element after it is kept. Java: `TestStreamTest.testFirstElementLate`.
pub fn build_test_stream_first_element_late(p: &TestPipeline) {
    let late_at = Instant::from_millis(-1_000_000);
    let windowed = p
        .apply(
            "TestStream",
            TestStream::new()
                .advance_watermark_to(Instant::EPOCH)
                .add_timestamped_elements([("late".to_string(), late_at)])
                .add_timestamped_elements([("onTime".to_string(), Instant::from_millis(100))])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            WindowInto::new(FixedWindows::of(Duration::from_secs(1)))
                .with_trigger(Trigger::Default)
                .with_accumulation_mode(AccumulationMode::Discarding)
                .with_allowed_lateness(Duration::from_secs(5)),
        );
    let values = grouped_values(&windowed, "Grouped");

    passert::that("LateWindow", &values)
        .in_window(IntervalWindow::new(late_at, Instant::from_millis(-999_000)))
        .empty();
    passert::that("OnTimeWindow", &values)
        .in_window(IntervalWindow::new(
            Instant::EPOCH,
            Instant::from_millis(1_000),
        ))
        .contains_in_any_order(["onTime".to_string()]);
}

/// Validates elements at the last timestamp of the global window, in the fixed window that
/// holds it. Java: `TestStreamTest.testElementsAtAlmostPositiveInfinity`.
pub fn build_test_stream_elements_at_end_of_global_window(p: &TestPipeline) {
    let size = Duration::from_secs(6 * 3_600);
    let end = Instant::END_OF_GLOBAL_WINDOW;
    let windowed = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([("foo".to_string(), end), ("bar".to_string(), end)])
                .advance_watermark_to_infinity(),
        )
        .apply("FixedWindows", WindowInto::new(FixedWindows::of(size)));
    let values = grouped_values(&windowed, "Grouped");

    let window = FixedWindows::of(size).assign_windows(end)[0]
        .interval()
        .expect("fixed windows are interval windows");
    passert::that("AssertLastWindow", &values)
        .in_window(window)
        .contains_in_any_order(["foo", "bar"].map(String::from));
}

/// Validates that a watermark one millisecond past [`Instant::END_OF_GLOBAL_WINDOW`] fires
/// the on-time pane of the global window. An element that comes after it is droppable.
pub fn build_test_stream_watermark_past_end_of_global_window(p: &TestPipeline) {
    let at = |value: &str| (value.to_string(), Instant::EPOCH);
    let values = p.apply(
        "TestStream",
        TestStream::new()
            .add_timestamped_elements([at("onTime")])
            .advance_watermark_to(Instant::END_OF_GLOBAL_WINDOW + Duration::from_millis(1))
            .add_timestamped_elements([at("droppable")])
            .advance_watermark_to_infinity(),
    );
    let panes = grouped_values(&values, "Grouped").par_do_fn("Timing", |value: String, ctx| {
        let timing = format!("{:?}", ctx.pane().timing());
        ctx.emit((value, timing))
    });
    passert::that("AssertOnTimePane", &panes)
        .contains_in_any_order([("onTime".to_string(), "OnTime".to_string())]);
}

/// Validates early panes from a processing-time trigger: each processing-time advance
/// fires one accumulated early pane. Java: `TestStreamTest.testEarlyPanesOfWindow`.
pub fn build_trigger_processing_time_early_panes(p: &TestPipeline) {
    let minutes = |m: u64| Duration::from_secs(60 * m);
    let sums = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([(1i64, Instant::from_millis(1_000))])
                .advance_processing_time(minutes(6))
                .add_timestamped_elements([(2i64, Instant::from_millis(2_000))])
                .advance_processing_time(minutes(6))
                .add_timestamped_elements([(3i64, Instant::from_millis(3_000))])
                .advance_processing_time(minutes(6))
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            WindowInto::new(FixedWindows::of(minutes(30)))
                .with_trigger(
                    Trigger::after_end_of_window()
                        .with_early_firings(Trigger::after_processing_time(minutes(5))),
                )
                .with_accumulation_mode(AccumulationMode::Accumulating)
                .with_allowed_lateness(Duration::ZERO),
        )
        .map("Key", |value: i64| ("key".to_string(), value))
        .sum_per_key("Sum");

    let window = IntervalWindow::new(Instant::EPOCH, Instant::EPOCH + minutes(30));
    passert::that("EarlyPanes", &sums)
        .in_early_panes(window)
        .contains_in_any_order([1, 3, 6].map(|sum| ("key".to_string(), sum)));
    passert::that("OnTimePane", &sums)
        .in_on_time_pane(window)
        .contains_in_any_order([("key".to_string(), 6)]);
}

/// The values that [`MultiStageSeenDoFn`] saw for its key.
#[derive(State)]
struct SeenState {
    seen: ValueState<String>,
}

#[derive(Timers)]
enum MultiStageTimer {
    #[timer(event_time, id = "emit")]
    Emit,
}

/// Joins the values of a key in arrival order, and emits them from a timer at 100 s.
#[derive(Clone)]
struct MultiStageSeenDoFn;

impl StatefulDoFn for MultiStageSeenDoFn {
    type Key = String;
    type Value = String;
    type Out = String;
    type State = SeenState;
    type Timer = MultiStageTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let seen = match state.seen.read()? {
            Some(seen) => format!("{seen},{value}"),
            None => value,
        };
        state.seen.write(seen)?;
        ctx.timer(MultiStageTimer::Emit)
            .at(Instant::from_millis(100_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        _key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            MultiStageTimer::Emit => ctx.emit(state.seen.read()?.unwrap_or_default()),
        }
    }
}

/// Validates that the watermark orders the stages of a pipeline that reads a TestStream:
/// a stateful DoFn sees each value before the grouped copy that the watermark releases.
/// Java: `TestStreamTest.testMultiStage`.
pub fn build_test_stream_multi_stage(p: &TestPipeline) {
    let input = p.apply(
        "TestStream",
        TestStream::new()
            .add_elements(["before".to_string()])
            .advance_watermark_to(Instant::EPOCH)
            .add_timestamped_elements([("after".to_string(), Instant::from_millis(10_000))])
            .advance_watermark_to_infinity(),
    );
    let grouped = input
        .apply(
            "FixedWindows",
            WindowInto::new(FixedWindows::of(Duration::from_secs(1))),
        )
        .map("SelfKey", |value: String| (value.clone(), value))
        .group_by_key("Group")
        .map("Keys", |(key, _values): (String, BeamIterable<String>)| {
            key.to_uppercase()
        })
        .apply("Rewindow", WindowInto::new(GlobalWindows));
    let result = input
        .flatten("Flatten", &[&grouped])
        .map("Key", |value: String| ("key".to_string(), value))
        .apply("Seen", ParDo::new(MultiStageSeenDoFn));

    passert::that("AssertSeen", &result)
        .contains_in_any_order(["before,BEFORE,after,AFTER".to_string()]);
}

/// Keeps the elements of a bundle and emits them in `finish_bundle`, with their timestamps.
#[derive(Clone, Default)]
struct BufferUntilFinishBundleDoFn {
    buffer: Vec<(String, Instant)>,
}

impl DoFn for BufferUntilFinishBundleDoFn {
    type In = String;
    type Out = String;

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        self.buffer.push((element, ctx.timestamp()));
        Ok(())
    }

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, Self::Out>) -> Result {
        std::mem::take(&mut self.buffer)
            .into_iter()
            .try_for_each(|(element, timestamp)| {
                out.emit(element, BoundedWindow::Global(GlobalWindow), timestamp)
            })
    }
}

/// Validates that the runner does not advance the watermark inside a bundle: outputs of
/// `finish_bundle` at the timestamps of their inputs are never late, so none is dropped.
/// Java: `ParDoTest.testWatermarkUpdateMidBundle`.
pub fn build_test_stream_watermark_advance_inside_bundle(p: &TestPipeline) {
    const BUNDLES: i64 = 100;
    let stream = (0..BUNDLES).fold(
        TestStream::new().advance_watermark_to(Instant::EPOCH),
        |stream, round| {
            stream
                .add_timestamped_elements(
                    (0..=round).map(|value| (value.to_string(), Instant::from_millis(round))),
                )
                .advance_watermark_to(Instant::from_millis(round + 1))
        },
    );
    let total = p
        .apply("TestStream", stream.advance_watermark_to_infinity())
        .apply("Buffer", ParDo::new(BufferUntilFinishBundleDoFn::default()))
        .apply(
            "MilliWindows",
            WindowInto::new(FixedWindows::of(Duration::from_millis(1))),
        )
        .apply("Count", CountGlobally::new().without_defaults())
        .apply(
            "GlobalWindow",
            WindowInto::new(GlobalWindows)
                .with_trigger(Trigger::after_end_of_window())
                .with_allowed_lateness(Duration::ZERO)
                .with_accumulation_mode(AccumulationMode::Discarding),
        )
        .apply("Sum", CombineGlobally::new(Sum::new()).without_defaults());

    passert::that("AssertTotal", &total).contains_in_any_order([(BUNDLES * BUNDLES + BUNDLES) / 2]);
}

/// Validates a repeated processing-time trigger with accumulating panes: a timer that fires
/// after the window expired adds no pane. Java:
/// `GroupByKeyTest.testCombiningAccumulatingProcessingTime`.
pub fn build_trigger_processing_time_accumulating(p: &TestPipeline) {
    let sums = p
        .apply(
            "TestStream",
            TestStream::new()
                .advance_watermark_to(Instant::EPOCH)
                .add_timestamped_elements([
                    (2i64, Instant::from_millis(2)),
                    (5i64, Instant::from_millis(5)),
                ])
                .advance_watermark_to(Instant::from_millis(100))
                .advance_processing_time(Duration::from_millis(10))
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            WindowInto::new(FixedWindows::of(Duration::from_millis(100)))
                .with_output_time(OutputTime::EarliestInPane)
                .with_accumulation_mode(AccumulationMode::Accumulating)
                .with_allowed_lateness(Duration::ZERO)
                .with_trigger(Trigger::repeatedly(Trigger::after_processing_time(
                    Duration::from_millis(10),
                ))),
        )
        .apply("Sum", CombineGlobally::new(Sum::new()).without_defaults());

    passert::that("AssertSums", &sums).contains_in_any_order([7]);
}

/// Validates that a processing-time pane flows through a second combine as an early pane,
/// under the continuation trigger that the first combine sets on its output. Java:
/// `GroupByKeyTest.testAfterProcessingTimeContinuationTriggerEarly`.
pub fn build_trigger_processing_time_continuation(p: &TestPipeline) {
    let first_sums = p
        .apply(
            "TestStream",
            TestStream::new()
                .advance_watermark_to(Instant::EPOCH)
                .add_elements([42i64])
                .advance_processing_time(Duration::from_millis(10))
                .advance_processing_time(Duration::from_millis(500))
                .advance_watermark_to_infinity(),
        )
        .apply(
            "GlobalWindow",
            WindowInto::new(GlobalWindows)
                .with_trigger(Trigger::repeatedly(Trigger::after_processing_time(
                    Duration::from_millis(1),
                )))
                .with_accumulation_mode(AccumulationMode::Accumulating)
                .with_on_time_behavior(OnTimeBehavior::FireIfNonempty)
                .with_allowed_lateness(Duration::ZERO),
        )
        .apply(
            "TriggeredSum",
            CombineGlobally::new(Sum::new()).without_defaults(),
        );
    let second_sums = first_sums.apply(
        "SecondTriggeredSum",
        CombineGlobally::new(Sum::new()).without_defaults(),
    );

    passert::that("FirstEarlySums", &early_panes(&first_sums, "FirstEarly"))
        .contains_in_any_order([42]);
    passert::that("SecondEarlySums", &early_panes(&second_sums, "SecondEarly"))
        .contains_in_any_order([42]);
}

/// The values of `input` that early panes emitted.
fn early_panes(input: &PCollection<i64>, name: &str) -> PCollection<i64> {
    input.par_do_fn(name, |sum: i64, ctx| match ctx.pane().timing() {
        Timing::Early => ctx.emit(sum),
        Timing::OnTime | Timing::Late | Timing::Unknown => Ok(()),
    })
}

/// Validates a global combine with a default value under a repeated count trigger. Every
/// pane of the combine reaches the output with its pane info, and the default does not.
/// Java: `CombineTest.testGlobalCombineWithDefaultsAndTriggers`.
///
/// The number of early panes is up to the runner. Their sums grow with the pane index, and
/// only the on-time pane is the last one, with the full sum.
pub fn build_global_combine_with_defaults_and_trigger(p: &TestPipeline) {
    let sums = p
        .apply("Create", Create::new(vec![1i64, 1]))
        .apply(
            "GlobalWindow",
            WindowInto::new(GlobalWindows)
                .with_trigger(Trigger::repeatedly(Trigger::after_count(1)))
                .with_accumulation_mode(AccumulationMode::Accumulating)
                .with_allowed_lateness(Duration::ZERO)
                .with_closing_behavior(ClosingBehavior::EmitAlways),
        )
        .sum_globally("Sum");
    let formatted = sums.par_do_fn("FormatPane", |sum: i64, ctx| {
        let is_last = ctx.pane().is_last();
        ctx.emit(format!("{sum}: {is_last}"))
    });
    let panes = sums.par_do_fn("PaneInfo", |sum: i64, ctx| {
        let pane = ctx.pane();
        let index = pane.index().map_or(-1, |index| index as i64);
        ctx.emit((
            (index, format!("{:?}", pane.timing())),
            (pane.is_last(), sum),
        ))
    });

    passert::that("AssertLastPane", &formatted).satisfies(|panes: &[String]| {
        if panes.iter().any(|pane| pane == "2: true") {
            Ok(())
        } else {
            Err(format!("no last pane with the sum 2: {panes:?}").into())
        }
    });
    passert::that("AssertPanes", &panes).satisfies(check_triggered_sum_panes);
}

/// One pane of [`build_global_combine_with_defaults_and_trigger`]: the index and the timing,
/// then whether it is the last pane and its sum.
type SumPane = ((i64, String), (bool, i64));

/// Checks the panes of a triggered global sum of `[1, 1]` in accumulating mode.
fn check_triggered_sum_panes(panes: &[SumPane]) -> Result {
    let mut sorted = panes.to_vec();
    sorted.sort();
    let Some((last, early)) = sorted.split_last() else {
        return Err("the combine emitted no pane".into());
    };
    let indexes_follow = sorted
        .iter()
        .enumerate()
        .all(|(position, ((index, _), _))| *index == position as i64);
    let last_is_full = *last == ((early.len() as i64, "OnTime".to_string()), (true, 2));
    let early_grow = early.iter().all(|((_, timing), (is_last, sum))| {
        timing == "Early" && !is_last && (1..=2).contains(sum)
    }) && early.windows(2).all(|pair| pair[0].1.1 <= pair[1].1.1);
    if indexes_follow && last_is_full && early_grow {
        Ok(())
    } else {
        Err(format!("unexpected panes: {panes:?}").into())
    }
}

/// One second after [`Instant::END_OF_GLOBAL_WINDOW`].
const PAST_GLOBAL_WINDOW: Instant =
    Instant::from_millis(Instant::END_OF_GLOBAL_WINDOW.as_millis() + 1_000);

/// Numbers `count` down to 1 under the key `k`, each timestamped at its value in
/// milliseconds. After the value `late_after`, the watermark passes the global window, so
/// the smaller values are droppable.
fn descending_stream(count: i64, late_after: Option<i64>) -> TestStream<(String, i64)> {
    (1..=count)
        .rev()
        .fold(TestStream::new(), |stream, value| {
            let stream = stream.add_timestamped_elements([(
                ("k".to_string(), value),
                Instant::from_millis(value),
            )]);
            match late_after {
                Some(late_after) if value == late_after => {
                    stream.advance_watermark_to(PAST_GLOBAL_WINDOW)
                }
                Some(_) | None => stream,
            }
        })
        .advance_watermark_to_infinity()
}

/// The number of values of [`descending_stream`] in the time-sorted input tests.
const SORTED_VALUES: i64 = 200;

/// The value of [`descending_stream`] after which the rest of the values are late.
const LATE_AFTER: i64 = 20;

/// Checks that the number of `deltas` is at least the on-time values and lower than all
/// values, so the runner dropped at least one late value.
fn check_late_count(deltas: &[i64]) -> Result {
    let count = deltas.len() as i64;
    if (SORTED_VALUES - LATE_AFTER..SORTED_VALUES).contains(&count) {
        Ok(())
    } else {
        Err(format!(
            "{count} values, expected {}..{SORTED_VALUES}",
            SORTED_VALUES - LATE_AFTER
        )
        .into())
    }
}

/// Validates `REQUIRES_TIME_SORTED_INPUT` with input from a TestStream in descending time
/// order: every value comes one after the value before it. Java:
/// `ParDoTest.testRequiresTimeSortedInputWithTestStream`.
pub fn build_time_sorted_input_from_test_stream(p: &TestPipeline) {
    let deltas = p
        .apply("TestStream", descending_stream(SORTED_VALUES, None))
        .apply("TimeSorted", ParDo::new(TimeSortedDeltaDoFn));
    passert::that("AssertDeltas", &deltas).contains_in_any_order(vec![1; SORTED_VALUES as usize]);
}

/// Validates that time-sorted input drops values that arrive after the watermark passed
/// the global window. Java: `ParDoTest.testRequiresTimeSortedInputWithLateData`.
pub fn build_time_sorted_input_drops_late_data(p: &TestPipeline) {
    let deltas = p
        .apply(
            "TestStream",
            descending_stream(SORTED_VALUES, Some(LATE_AFTER)),
        )
        .apply("TimeSorted", ParDo::new(TimeSortedDeltaDoFn));
    passert::that("AssertCount", &deltas).satisfies(check_late_count);
}

/// Validates that a second time-sorted stage keeps every value that the first one emits,
/// when late data arrives. Java: `ParDoTest.testTwoRequiresTimeSortedInputWithLateData`.
pub fn build_two_time_sorted_stages_drop_late_data(p: &TestPipeline) {
    let deltas = p
        .apply(
            "TestStream",
            descending_stream(SORTED_VALUES, Some(LATE_AFTER)),
        )
        .apply("FirstTimeSorted", ParDo::new(TimeSortedDeltaDoFn))
        .map("Key", |delta: i64| ("k".to_string(), delta))
        .apply("SecondTimeSorted", ParDo::new(TimeSortedDeltaDoFn));
    passert::that("AssertCount", &deltas).satisfies(check_late_count);
}
