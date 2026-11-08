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

//! Event-time timers, processing-time timers, dynamic timer tags, time-sorted input, and
//! output timestamps.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, TestStream, passert};

use crate::ValidatesRunnerOptions;
use crate::dofns::*;

/// Validates that event-time timers set out of order fire in timestamp order per key.
pub fn build_timer_strict_ordering(p: &TestPipeline) {
    let out = p
        .apply(
            "Events",
            Create::new(vec![("k1".to_string(), 1i64), ("k2".to_string(), 2i64)]),
        )
        .apply("TimerStrictOrdering", ParDo::new(TimerStrictOrderingDoFn));

    passert::that("AssertTimerOrder", &out).contains_in_any_order([
        (
            "k1".to_string(),
            "t1000@1000->t2000@2000->t3000@3000".to_string(),
        ),
        (
            "k2".to_string(),
            "t1000@1000->t2000@2000->t3000@3000".to_string(),
        ),
    ]);
}

/// Validates that an `on_timer` callback can rearm its own timer for a later timestamp.
pub fn build_timer_looping(p: &TestPipeline) {
    let out = p
        .apply(
            "Events",
            Create::new(vec![("k1".to_string(), 3i64), ("k2".to_string(), 3i64)]),
        )
        .apply("TimerLooping", ParDo::new(TimerLoopingDoFn));

    passert::that("AssertTimerLooping", &out).contains_in_any_order(
        [
            "k1:step=3:ts=1000",
            "k1:step=2:ts=2000",
            "k1:step=1:ts=3000",
            "k2:step=3:ts=1000",
            "k2:step=2:ts=2000",
            "k2:step=1:ts=3000",
        ]
        .map(String::from),
    );
}

/// Validates that timer outputs carry the hold timestamp: the firing time by default, or
/// the timestamp set with `.hold(..)`.
pub fn build_timer_output_timestamps(p: &TestPipeline) {
    let out = p
        .apply("Events", Create::new(vec![("k1".to_string(), 1i64)]))
        .apply(
            "TimerOutputTimestamps",
            ParDo::new(TimerOutputTimestampDoFn),
        )
        .par_do_fn(
            "ReadOutputTimestamp",
            |(key, label): (String, String),
             ctx: &mut ProcessContext<'_, (String, String, i64)>| {
                ctx.emit((key, label, ctx.timestamp().as_millis()))
            },
        );

    passert::that("AssertTimerOutputTimestamps", &out).contains_in_any_order([
        ("k1".to_string(), "held".to_string(), 500i64),
        ("k1".to_string(), "default".to_string(), 2_000i64),
    ]);
}

/// Validates dynamic timer tags (`Timer(String)`) and clearing one tag without affecting
/// the other tags in the same family.
pub fn build_timer_dynamic_tags(p: &TestPipeline) {
    let out = p
        .apply(
            "Events",
            Create::new(vec![("k1".to_string(), 1i64), ("k2".to_string(), 2i64)]),
        )
        .apply("DynamicTimers", ParDo::new(DynamicTimerDoFn));

    passert::that("AssertDynamicTimers", &out)
        .contains_in_any_order(["k1:keep_a+keep_c", "k2:keep_a+keep_c"].map(String::from));
}

/// Validates that a processing-time timer fires and flushes the buffered state of each key.
pub fn build_processing_time_timer(p: &TestPipeline) {
    let out = p
        .apply(
            "Events",
            Create::new(vec![("k1".to_string(), 10i64), ("k2".to_string(), 20i64)]),
        )
        .apply("ProcessingTimeTimer", ParDo::new(ProcessingTimeTimerDoFn));

    passert::that("AssertProcessingTimeTimer", &out)
        .contains_in_any_order([("k1".to_string(), 10), ("k2".to_string(), 20)]);
}

/// Validates reading a side input view inside `on_timer`.
pub fn build_timer_with_side_input(p: &TestPipeline) {
    let multiplier = p
        .apply("Multiplier", Create::new(vec![10i64]))
        .as_singleton();
    let events = p.apply(
        "Events",
        Create::new(vec![("k1".to_string(), 3i64), ("k2".to_string(), 7i64)]),
    );
    let out = events.apply(
        "TimerWithSideInput",
        ParDo::new(TimerWithSideInputDoFn { multiplier }),
    );

    passert::that("AssertTimerSideInput", &out)
        .contains_in_any_order([("k1".to_string(), 30), ("k2".to_string(), 70)]);
}

/// Validates `REQUIRES_TIME_SORTED_INPUT`: the runner delivers each key's elements in
/// ascending timestamp order before calling `process_element`.
pub fn build_requires_time_sorted_input(p: &TestPipeline) {
    let out = p
        .apply(
            "Events",
            Create::new(vec![
                ("k1".to_string(), (30i64, 3_000i64)),
                ("k1".to_string(), (10i64, 1_000i64)),
                ("k1".to_string(), (40i64, 4_000i64)),
                ("k1".to_string(), (20i64, 2_000i64)),
                ("k2".to_string(), (200i64, 2_000i64)),
                ("k2".to_string(), (100i64, 1_000i64)),
            ]),
        )
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .apply("TimeSortedInput", ParDo::new(TimeSortedInputDoFn));

    passert::that("AssertTimeSorted", &out).contains_in_any_order([
        ("k1".to_string(), vec![10, 20, 30, 40]),
        ("k2".to_string(), vec![100, 200]),
    ]);
}

fn window(start_ms: i64, end_ms: i64) -> IntervalWindow {
    IntervalWindow::new(Instant::from_millis(start_ms), Instant::from_millis(end_ms))
}

fn keyed_zeros(keys: &[&str]) -> Create<(String, i64)> {
    Create::new(
        keys.iter()
            .map(|key| (key.to_string(), 0i64))
            .collect::<Vec<_>>(),
    )
}

/// Validates a stateful DoFn with a timer right after a `GroupByKey`: each key and fixed
/// window emits its sum from a timer at the end of the window.
///
/// Java: `ParDoTest.TimerTests.testGbkFollowedByUserTimers`.
pub fn build_gbk_then_timers(p: &TestPipeline) {
    let sums = p
        .apply(
            "Events",
            Create::new(vec![
                ("a".to_string(), (1i64, 1_000i64)),
                ("a".to_string(), (2, 2_000)),
                ("b".to_string(), (5, 3_000)),
                ("a".to_string(), (10, 12_000)),
            ]),
        )
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .window_into("FixedWindows", FixedWindows::of(Duration::from_secs(10)))
        .group_by_key("Group")
        .par_do_fn(
            "Sum",
            |(key, values): (String, BeamIterable<i64>),
             ctx: &mut ProcessContext<'_, (String, i64)>| {
                let sum = values.into_iter().sum::<Result<i64>>()?;
                ctx.emit((key, sum))
            },
        )
        .apply("EmitAtWindowEnd", ParDo::new(EmitAtWindowEndDoFn));

    passert::that_windowed("AssertWindowSums", &sums).contains_in_any_order([
        (("a".to_string(), 3), window(0, 10_000)),
        (("b".to_string(), 5), window(0, 10_000)),
        (("a".to_string(), 10), window(10_000, 20_000)),
    ]);
}

/// Validates that setting an event-time timer again replaces it, both inside one call and
/// across elements: each key fires once, at the last time it set.
///
/// Java: `ParDoTest.TimerTests.testEventTimeTimerCanBeReset`,
/// `ParDoTest.TimerTests.duplicateTimerSetting`.
pub fn build_event_time_timer_reset(p: &TestPipeline) {
    let out = p
        .apply("Events", keyed_zeros(&["k1", "k1", "k2", "k2"]))
        .apply("ResetTimer", ParDo::new(EventTimeTimerResetDoFn));

    passert::that("AssertOneFiring", &out)
        .contains_in_any_order(["k1:count=2@2000", "k2:count=2@2000"].map(String::from));
}

/// Validates two event-time timers that set each other from `on_timer`.
///
/// Java: `ParDoTest.TimerTests.testTwoTimersSettingEachOtherBounded`,
/// `testTwoTimersSettingEachOtherUnbounded`,
/// `testTwoTimersSettingEachOtherWithCreateAsInputBounded`,
/// `testTwoTimersSettingEachOtherWithCreateAsInputUnbounded`.
pub fn build_timers_set_each_other(p: &TestPipeline) {
    let out = p
        .apply("Events", keyed_zeros(&["k1", "k2"]))
        .apply("PingPong", ParDo::new(TimerPingPongDoFn { firings: 4 }));

    let expected = ["k1", "k2"].into_iter().flat_map(|key| {
        ["ping@1000", "pong@2000", "ping@3000", "pong@4000"].map(|step| format!("{key}:{step}"))
    });
    passert::that("AssertPingPong", &out).contains_in_any_order(expected);
}

/// Validates that the hold of an event-time timer keeps a downstream window open: the
/// output at the 500 ms hold reaches the window `[0, 1 s)`, though the timer fires at 10 s.
///
/// A related check to Java `ParDoTest.TimerTests.testOutputTimestamp`, which checks that a
/// downstream timer waits for the hold.
pub fn build_timer_output_hold(p: &TestPipeline) {
    let counts = p
        .apply("Events", keyed_zeros(&["k"]))
        .apply("HoldTimers", ParDo::new(EventTimeOutputHoldDoFn))
        .window_into("OneSecondWindows", FixedWindows::of(Duration::from_secs(1)))
        .count_per_element("CountPerWindow");

    passert::that_windowed("AssertHeldWindow", &counts).contains_in_any_order([
        (("k:held".to_string(), 1), window(0, 1_000)),
        (("k:default".to_string(), 1), window(2_000, 3_000)),
    ]);
}

/// Validates that the hold of a processing-time timer keeps a downstream window open: the
/// output at the 500 ms hold reaches the window `[0, 1 s)`.
///
/// Java: `ParDoTest.TimerTests.testOutputTimestampWithProcessingTime`.
pub fn build_processing_time_timer_output_hold(p: &TestPipeline) {
    let counts = p
        .apply("Events", keyed_zeros(&["k1", "k2"]))
        .apply("HoldTimer", ParDo::new(ProcessingTimeOutputHoldDoFn))
        .window_into("OneSecondWindows", FixedWindows::of(Duration::from_secs(1)))
        .count_per_element("CountPerWindow");

    passert::that_windowed("AssertHeldWindow", &counts).contains_in_any_order([
        (("k1".to_string(), 1), window(0, 1_000)),
        (("k2".to_string(), 1), window(0, 1_000)),
    ]);
}

/// Validates a processing-time timer family with dynamic tags.
///
/// Java: `ParDoTest.TimerFamilyTests.testTimerFamilyProcessingTime`.
pub fn build_processing_time_timer_family(p: &TestPipeline) {
    let out = p
        .apply("Events", keyed_zeros(&["k1", "k2"]))
        .apply("TimerFamily", ParDo::new(ProcessingTimerFamilyDoFn));

    passert::that("AssertFiredTags", &out)
        .contains_in_any_order(["k1:a", "k1:b", "k2:a", "k2:b"].map(String::from));
}

/// Validates `on_window_expiration` in the global window: the callback emits the buffered
/// sum of each key when the input ends.
///
/// Java: `ParDoTest.OnWindowExpirationTests.testOnWindowExpirationSimpleBoundedGlobal`,
/// `testOnWindowExpirationSimpleUnboundedGlobal`.
pub fn build_on_window_expiration_global(p: &TestPipeline) {
    let sums = p
        .apply(
            "Events",
            Create::new(vec![
                ("a".to_string(), 1i64),
                ("a".to_string(), 2),
                ("b".to_string(), 5),
            ]),
        )
        .apply("SumOnExpiration", ParDo::new(SumOnWindowExpirationDoFn));

    passert::that("AssertSums", &sums)
        .contains_in_any_order([("a".to_string(), 3), ("b".to_string(), 5)]);
}

/// Validates that a value read from the pipeline options at construction reaches
/// `process_element` and `on_timer`. A Dataflow worker reads it from the job options.
///
/// Java: `ParDoTest.BasicTests.testPipelineOptionsParameter`,
/// `ParDoTest.TimerTests.testPipelineOptionsParameterOnTimer`.
pub fn build_pipeline_options_in_dofn(p: &TestPipeline) {
    let option = p
        .options()
        .view_as::<ValidatesRunnerOptions>()
        .ok()
        .and_then(|options| options.vr_test)
        .unwrap_or_else(|| "unset".to_string());
    let expected = [format!("process:{option}"), format!("timer:{option}")];
    let out = p
        .apply("Events", keyed_zeros(&["k"]))
        .apply("ReadOption", ParDo::new(PipelineOptionsDoFn { option }));

    passert::that("AssertOption", &out).contains_in_any_order(expected);
}

/// Validates `.at(..)` on the main and on a tagged output, with times before the input
/// timestamp.
///
/// Java: `ParDoTest.TimestampTests.testParDoTaggedOutputWithTimestamp`,
/// `ParDoTest.TimestampTests.testParDoShiftTimestampUnlimited`.
pub fn build_tagged_output_timestamps(p: &TestPipeline) {
    let ShiftedOutputsPCollections { main, tagged } = p
        .apply(
            "Events",
            Create::new(vec![
                ("k".to_string(), (1i64, 100_000i64)),
                ("k".to_string(), (2, 100_000)),
            ]),
        )
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .map("DropKey", |(_, v): (String, i64)| v)
        .par_do("Shift", TaggedOutputTimestampDoFn);

    let main_ts = main.par_do_fn(
        "ReadMainTimestamp",
        |v: i64, ctx: &mut ProcessContext<'_, (i64, i64)>| {
            ctx.emit((v, ctx.timestamp().as_millis()))
        },
    );
    let tagged_ts = tagged.par_do_fn(
        "ReadTaggedTimestamp",
        |v: String, ctx: &mut ProcessContext<'_, (String, i64)>| {
            let ts = ctx.timestamp().as_millis();
            ctx.emit((v, ts))
        },
    );

    passert::that("AssertMain", &main_ts).contains_in_any_order([(1, 1_000), (2, 2_000)]);
    passert::that("AssertTagged", &tagged_ts).contains_in_any_order([
        ("tagged:1".to_string(), 1_500),
        ("tagged:2".to_string(), 2_500),
    ]);
}

fn at(key: &str, timestamp_ms: i64) -> ((String, i64), Instant) {
    ((key.to_string(), 0), Instant::from_millis(timestamp_ms))
}

/// Validates that the event-time timers of a key fire in timestamp order when the
/// watermark of a TestStream advances in steps. Prism only.
///
/// Java: `ParDoTest.TimerTests.testEventTimeTimerOrdering`.
pub fn build_timer_firing_order_test_stream(p: &TestPipeline) {
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([at("k", 3_000), at("k", 1_000), at("k", 2_000)])
                .advance_watermark_to(Instant::from_millis(5_000))
                .add_timestamped_elements([at("k", 7_000), at("k", 6_000)])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FiringOrder",
            ParDo::new(TimerFiringOrderDoFn { expected: 5 }),
        );

    passert::that("AssertFiringOrder", &out)
        .contains_in_any_order([("k".to_string(), vec![1_000, 2_000, 3_000, 6_000, 7_000])]);
}

/// Validates that a late element within the allowed lateness can set a timer behind the
/// watermark, and that the timer fires. Prism only.
///
/// Java: `ParDoTest.TimerTests.testEventTimeTimerSetWithinAllowedLateness`.
pub fn build_timer_within_allowed_lateness(p: &TestPipeline) {
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .advance_watermark_to(Instant::from_millis(15_000))
                .add_timestamped_elements([(("k".to_string(), 5i64), Instant::from_millis(5_000))])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "FixedWindows",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
                .with_allowed_lateness(Duration::from_secs(60)),
        )
        .apply("EmitAtWindowEnd", ParDo::new(EmitAtWindowEndDoFn));

    passert::that_windowed("AssertLateTimer", &out)
        .contains_in_any_order([(("k".to_string(), 5), window(0, 10_000))]);
}

/// Validates that setting a processing-time timer again replaces it: the timer fires once,
/// after the TestStream advances the processing time. Prism only.
///
/// Java: `ParDoTest.TimerTests.testProcessingTimeTimerCanBeReset`.
pub fn build_processing_time_timer_reset(p: &TestPipeline) {
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_elements([("k".to_string(), 1i64), ("k".to_string(), 2)])
                .advance_processing_time(Duration::from_secs(20))
                .advance_watermark_to_infinity(),
        )
        .apply("ResetTimer", ParDo::new(ProcessingTimeTimerResetDoFn));

    passert::that("AssertOneFiring", &out).contains_in_any_order(["k:count=2".to_string()]);
}

/// Validates that an event-time timer set and cleared in `process_element` never fires.
/// Prism only.
///
/// Java: `ParDoTest.TimerTests.testSetAndClearEventTimeTimer`.
pub fn build_set_and_clear_event_time_timer(p: &TestPipeline) {
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([at("k", 0)])
                .advance_watermark_to(Instant::from_millis(5_000))
                .advance_watermark_to_infinity(),
        )
        .apply("SetAndClear", ParDo::new(SetAndClearEventTimeTimerDoFn));

    passert::that("AssertNotFired", &out).contains_in_any_order(["processed:k".to_string()]);
}

/// Validates that an event-time timer callback can clear a later event-time timer. Prism only.
///
/// Java: `ParDoTest.TimerTests.testClearEventTimeTimer`.
pub fn build_clear_event_time_timer_in_timer(p: &TestPipeline) {
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([at("k", 0)])
                .advance_watermark_to(Instant::from_millis(1_500))
                .advance_watermark_to(Instant::from_millis(5_000))
                .advance_watermark_to_infinity(),
        )
        .apply("ClearInTimer", ParDo::new(ClearEventTimeTimerInTimerDoFn));

    passert::that("AssertOnlyFirst", &out).contains_in_any_order(["k:first".to_string()]);
}

/// Validates that a processing-time timer set and cleared in `process_element` never fires.
/// Prism only.
///
/// Java: `ParDoTest.TimerTests.testSetAndClearProcessingTimeTimer`.
pub fn build_set_and_clear_processing_time_timer(p: &TestPipeline) {
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_elements([("k".to_string(), 1i64)])
                .advance_processing_time(Duration::from_secs(20))
                .advance_watermark_to_infinity(),
        )
        .apply(
            "SetAndClear",
            ParDo::new(SetAndClearProcessingTimeTimerDoFn),
        );

    passert::that("AssertNotFired", &out).contains_in_any_order(["processed:k".to_string()]);
}

/// Validates that a processing-time timer callback can clear a later processing-time timer.
/// Prism only.
///
/// Java: `ParDoTest.TimerTests.testClearProcessingTimeTimer`.
pub fn build_clear_processing_time_timer_in_timer(p: &TestPipeline) {
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_elements([("k".to_string(), 1i64)])
                .advance_processing_time(Duration::from_secs(6))
                .advance_processing_time(Duration::from_secs(20))
                .advance_watermark_to_infinity(),
        )
        .apply(
            "ClearInTimer",
            ParDo::new(ClearProcessingTimeTimerInTimerDoFn),
        );

    passert::that("AssertOnlyFirst", &out).contains_in_any_order(["k:first".to_string()]);
}

/// Validates that an upstream timer hold delays a downstream event-time timer until the hold
/// releases. Prism only.
///
/// Java: `ParDoTest.TimerTests.testOutputTimestamp`.
pub fn build_timer_hold_delays_downstream_timer(p: &TestPipeline) {
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([(("k".to_string(), 1i64), Instant::EPOCH)])
                .advance_watermark_to(Instant::from_millis(15_000))
                .add_timestamped_elements([(("k".to_string(), 2i64), Instant::from_millis(8_000))])
                .advance_watermark_to(Instant::from_millis(25_000))
                .advance_watermark_to_infinity(),
        )
        .apply("UpstreamHold", ParDo::new(UpstreamHoldDoFn))
        .apply("DownstreamFlush", ParDo::new(DownstreamCollectTimerDoFn));

    passert::that("AssertHeldValues", &out)
        .contains_in_any_order([("k".to_string(), vec![1, 2, 3])]);
}

/// Validates `OrderedListState::clear_range` driven by a timer on `TestStream` input. Prism
/// only.
///
/// Java: `ParDoTest.StateTests.testOrderedListStateRangeDelete`.
pub fn build_test_stream_ordered_list_clear_range(p: &TestPipeline) {
    let entry = |ts: i64, label: &str| {
        (
            ("hello".to_string(), (ts, label.to_string())),
            Instant::EPOCH,
        )
    };
    let out = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([
                    entry(97, "a"),
                    entry(42, "d"),
                    entry(52, "b"),
                    entry(12, "c"),
                ])
                .advance_watermark_to(Instant::from_millis(50))
                .advance_watermark_to_infinity(),
        )
        .apply("RangeDelete", ParDo::new(OrderedListRangeDeleteDoFn));

    passert::that("AssertRemaining", &out)
        .contains_in_any_order([(52i64, "b".to_string()), (97, "a".to_string())]);
}
