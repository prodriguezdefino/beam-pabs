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

//! Windowing and window-aware grouping.

use std::fmt::Debug;
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::Sum;
use beam::windowing::OutputTime;

use super::core_transforms::sorted_groups;
use crate::dofns::*;

/// Validates windowed grouping with `WindowInto` and `group_by_key`: each window's values
/// are grouped separately, and each result lands in the window it belongs to.
pub fn build_windowed_group_by_key(p: &TestPipeline) {
    let sums = p
        .apply(
            "Create",
            Create::new(vec![
                ("k1".to_string(), (1i64, 1_000i64)),
                ("k1".to_string(), (2i64, 2_000i64)),
                ("k1".to_string(), (10i64, 11_000i64)),
                ("k1".to_string(), (20i64, 12_000i64)),
            ]),
        )
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .apply(
            "WindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .group_by_key("Group")
        .par_do_fn(
            "SumGrouped",
            |(k, values): (String, BeamIterable<i64>),
             ctx: &mut ProcessContext<'_, (String, i64)>| {
                let sum = values.into_iter().sum::<Result<i64>>()?;
                ctx.emit((k, sum))
            },
        );

    passert::that_windowed("AssertSums", &sums).contains_in_any_order([
        (
            ("k1".to_string(), 3),
            IntervalWindow::new(Instant::EPOCH, Instant::from_millis(10_000)),
        ),
        (
            ("k1".to_string(), 30),
            IntervalWindow::new(Instant::from_millis(10_000), Instant::from_millis(20_000)),
        ),
    ]);
}

/// Validates that a window-observing `DoFn` after `SlidingWindows` runs once per
/// assigned window.
pub fn build_sliding_windows_pardo(p: &TestPipeline) {
    let observed = p
        .apply(
            "Create",
            Create::new(vec![("k".to_string(), (1i64, 7_000i64))]),
        )
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .apply(
            "SlidingWindows",
            WindowInto::new(
                SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5)),
            ),
        )
        .par_do("ObserveWindow", ValidatesObserveWindowDoFn);

    passert::that_windowed("AssertObserved", &observed).contains_in_any_order([
        (
            ("k".to_string(), (0, 1)),
            IntervalWindow::new(Instant::EPOCH, Instant::from_millis(10_000)),
        ),
        (
            ("k".to_string(), (5_000, 1)),
            IntervalWindow::new(Instant::from_millis(5_000), Instant::from_millis(15_000)),
        ),
    ]);
}

fn magic_square_events(p: &TestPipeline) -> PCollection<(String, i64)> {
    let values = [4i64, 9, 2, 3, 5, 7, 8, 1, 6];
    let rows: Vec<_> = values
        .into_iter()
        .enumerate()
        .map(|(i, v)| ("magic".to_string(), (v, ((i as i64 + 1) * 1_000) - 10)))
        .collect();
    p.apply("MagicSquare", Create::new(rows))
        .par_do("AssignTimestamps", AssignTimestampDoFn)
}

fn assert_window_sums(
    source: &PCollection<(String, i64)>,
    sum_fn: impl Fn(&str, PCollection<(String, i64)>) -> PCollection<i64>,
) {
    let w = Duration::from_secs(3);
    let check = |label: &str, windowed: PCollection<(String, i64)>, expected: &[i64]| {
        let sums = sum_fn(label, windowed)
            .apply(format!("{label}/Global"), WindowInto::new(GlobalWindows));
        passert::that(format!("{label}/Assert"), &sums)
            .contains_in_any_order(expected.iter().copied());
    };

    check(
        "Fixed",
        source.apply("Fixed/Window", WindowInto::new(FixedWindows::of(w))),
        &[15, 15, 15],
    );
    check(
        "SlidingFixed",
        source.apply(
            "SlidingFixed/Window",
            WindowInto::new(SlidingWindows::of(w).every(w)),
        ),
        &[15, 15, 15],
    );
    check(
        "Sliding",
        source.apply(
            "Sliding/Window",
            WindowInto::new(SlidingWindows::of(w * 3).every(w)),
        ),
        &[15, 30, 45, 30, 15],
    );
    check(
        "Session",
        source.apply(
            "Session/Window",
            WindowInto::new(Sessions::with_gap_duration(w)),
        ),
        &[45],
    );
}

/// Validates `FixedWindows`, `SlidingWindows`, and `Sessions` with `GroupByKey`.
pub fn build_window_sums_gbk(p: &TestPipeline) {
    let source = magic_square_events(p);
    assert_window_sums(&source, |label, windowed| {
        windowed.group_by_key(format!("{label}/GBK")).par_do_fn(
            format!("{label}/Sum"),
            |(_, vs): (String, BeamIterable<i64>), ctx: &mut ProcessContext<'_, i64>| {
                ctx.emit(vs.into_iter().sum::<Result<i64>>()?)
            },
        )
    });
}

/// Validates `FixedWindows`, `SlidingWindows`, and `Sessions` with lifted `CombinePerKey`.
pub fn build_window_sums_lifted(p: &TestPipeline) {
    let source = magic_square_events(p);
    assert_window_sums(&source, |label, windowed| {
        windowed
            .combine_per_key(format!("{label}/Combine"), Sum::new())
            .map(format!("{label}/Val"), |(_, sum): (String, i64)| sum)
    });
}

/// Validates that re-windowing after `SlidingWindows` preserves per-window multiplicity.
pub fn build_rewindow_preserves_multiplicity(p: &TestPipeline) {
    let rows: Vec<_> = (0..10i64)
        .map(|k| ("key".to_string(), (k, k * 1_000)))
        .collect();
    let grouped = p
        .apply("Create", Create::new(rows))
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .apply(
            "Sliding",
            WindowInto::new(
                SlidingWindows::of(Duration::from_secs(6)).every(Duration::from_secs(2)),
            ),
        )
        .apply(
            "Rewindow1",
            WindowInto::new(FixedWindows::of(Duration::from_secs(5))),
        )
        .apply(
            "Rewindow2",
            WindowInto::new(FixedWindows::of(Duration::from_secs(5))),
        )
        .group_by_key("Group");
    let sorted = sorted_groups(&grouped, "Sort").apply("ToGlobal", WindowInto::new(GlobalWindows));

    let w0: Vec<i64> = (0..5).flat_map(|x| [x, x, x]).collect();
    let w1: Vec<i64> = (5..10).flat_map(|x| [x, x, x]).collect();

    passert::that("AssertRewindowed", &sorted)
        .contains_in_any_order([("key".to_string(), w0), ("key".to_string(), w1)]);
}

/// Validates event-time assignment, timestamp shifting before windowing, and window reification.
pub fn build_timestamps_and_window_reify(p: &TestPipeline) {
    let reified = p
        .apply("Create", Create::new(vec![1_000i64, 7_000, 14_000]))
        .par_do_fn("AssignInitial", |ts: i64, ctx| {
            ctx.output(ts).at(Instant::from_millis(ts)).emit()
        })
        .par_do_fn("ShiftForward", |val: i64, ctx| {
            let shifted = ctx.timestamp() + Duration::from_secs(4);
            ctx.output(val).at(shifted).emit()
        })
        .apply(
            "WindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .par_do_fn(
            "Reify",
            |Windowed { value: val, window }: Windowed<i64>, ctx| {
                let win = window.interval().ok_or("expected IntervalWindow")?;
                ctx.emit((
                    val,
                    ctx.timestamp().as_millis(),
                    win.start.as_millis(),
                    win.end.as_millis(),
                ))
            },
        );

    let w0 = IntervalWindow::new(Instant::EPOCH, Instant::from_millis(10_000));
    let w1 = IntervalWindow::new(Instant::from_millis(10_000), Instant::from_millis(20_000));
    passert::that_windowed("AssertReified", &reified).contains_in_any_order([
        ((1_000i64, 5_000i64, 0i64, 10_000i64), w0),
        ((7_000, 11_000, 10_000, 20_000), w1),
        ((14_000, 18_000, 10_000, 20_000), w1),
    ]);
}

/// The interval window `[start, end)`, in milliseconds.
fn interval(start: i64, end: i64) -> IntervalWindow {
    IntervalWindow::new(Instant::from_millis(start), Instant::from_millis(end))
}

/// `(key, (value, timestamp in milliseconds))` rows as timestamped `(key, value)` elements.
fn timestamped_rows(
    p: &TestPipeline,
    name: &str,
    rows: &[(&str, i64, i64)],
) -> PCollection<(String, i64)> {
    let rows: Vec<_> = rows
        .iter()
        .map(|&(k, v, ts)| (k.to_string(), (v, ts)))
        .collect();
    p.apply(format!("{name}/Create"), Create::new(rows))
        .par_do(format!("{name}/Timestamps"), AssignTimestampDoFn)
}

/// Sums the values of each group.
fn sum_groups(
    grouped: &PCollection<(String, BeamIterable<i64>)>,
    name: &str,
) -> PCollection<(String, i64)> {
    grouped.par_do_fn(name, |(k, values): (String, BeamIterable<i64>), ctx| {
        let sum = values.into_iter().sum::<Result<i64>>()?;
        ctx.emit((k, sum))
    })
}

/// Validates a re-window after a `GroupByKey` whose output time is the latest timestamp: each
/// group lands in the 1 ms window of its latest input.
///
/// Covers `GroupByKeyTest$WindowTests.testRewindowWithTimestampCombiner`.
pub fn build_rewindow_with_output_time(p: &TestPipeline) {
    let sums = timestamped_rows(
        p,
        "Input",
        &[("foo", 1, 1), ("foo", 4, 4), ("bar", 3, 3), ("foo", 9, 9)],
    )
    .apply(
        "GlobalLatest",
        WindowInto::new(GlobalWindows).with_output_time(OutputTime::LatestInPane),
    )
    .group_by_key("Group");
    let rewindowed = sum_groups(&sums, "Sum").apply(
        "FixedWindows",
        WindowInto::new(FixedWindows::of(Duration::from_millis(1))),
    );
    passert::that_windowed("AssertRewindowed", &rewindowed).contains_in_any_order([
        (("foo".to_string(), 14), interval(9, 10)),
        (("bar".to_string(), 3), interval(3, 4)),
    ]);
}

/// Validates `CoGroupByKey` in fixed windows: each window groups only its own values, and a
/// window with values on one side gets an empty group on the other.
///
/// Covers `CoGroupByKeyTest.testCoGroupByKeyWithWindowing`.
pub fn build_cogroup_by_key_windowed(p: &TestPipeline) {
    let window = || WindowInto::new(FixedWindows::of(Duration::from_millis(4)));
    let clicks = timestamped_rows(p, "Clicks", &[("u1", 10, 0), ("u1", 14, 4), ("u2", 26, 6)])
        .apply("WindowClicks", window());
    let purchases = timestamped_rows(
        p,
        "Purchases",
        &[
            ("u1", 101, 1),
            ("u1", 106, 6),
            ("u2", 207, 7),
            ("u2", 209, 9),
        ],
    )
    .apply("WindowPurchases", window());

    let joined = clicks.co_group_by_key("Join", &purchases).par_do_fn(
        "SortGroups",
        |(k, (clicks, purchases)): (String, (BeamIterable<i64>, BeamIterable<i64>)), ctx| {
            let mut clicks = clicks.into_vec()?;
            clicks.sort_unstable();
            let mut purchases = purchases.into_vec()?;
            purchases.sort_unstable();
            ctx.emit((k, (clicks, purchases)))
        },
    );
    let group = |k: &str, clicks: &[i64], purchases: &[i64]| {
        (k.to_string(), (clicks.to_vec(), purchases.to_vec()))
    };
    passert::that_windowed("AssertJoined", &joined).contains_in_any_order([
        (group("u1", &[10], &[101]), interval(0, 4)),
        (group("u1", &[14], &[106]), interval(4, 8)),
        (group("u2", &[26], &[207]), interval(4, 8)),
        (group("u2", &[], &[209]), interval(8, 12)),
    ]);
}

/// Reshuffles `windowed` and checks that the output keeps its windowing strategy and holds
/// the `(element, window)` pairs `expected`.
fn assert_reshuffle_keeps_windows<T>(
    windowed: &PCollection<T>,
    label: &str,
    expected: Vec<(T, IntervalWindow)>,
) where
    T: DefaultCoder + Clone + PartialEq + Debug,
{
    let reshuffled = windowed.reshuffle(format!("{label}/Reshuffle"));
    assert_eq!(
        reshuffled.windowing_strategy_id(),
        windowed.windowing_strategy_id(),
        "{label}: Reshuffle must keep the windowing strategy"
    );
    passert::that_windowed(format!("{label}/Assert"), &reshuffled).contains_in_any_order(expected);
}

/// Values of [`build_reshuffle_keeps_windows`], with the extremes of `i64`.
const RESHUFFLE_ROWS: &[(&str, i64, i64)] = &[
    ("k1", 3, 1_000),
    ("k5", i64::MAX, 2_000),
    ("k5", i64::MIN, 12_000),
    ("k2", 66, 13_000),
    ("k1", 4, 25_000),
    ("k2", -33, 5_000),
    ("k3", 0, 9_999),
];

/// Validates that `Reshuffle` keeps the windows of its elements: fixed, sliding (each element
/// in two windows) and unmerged sessions.
///
/// Covers `ReshuffleTest.testReshuffleAfterFixedWindows` and
/// `ReshuffleTest.testReshuffleAfterSlidingWindows`.
pub fn build_reshuffle_keeps_windows(p: &TestPipeline) {
    let input = timestamped_rows(p, "Input", RESHUFFLE_ROWS);
    let pairs = |windows: fn(i64) -> Vec<IntervalWindow>| -> Vec<((String, i64), IntervalWindow)> {
        RESHUFFLE_ROWS
            .iter()
            .flat_map(|&(k, v, ts)| {
                windows(ts)
                    .into_iter()
                    .map(move |w| ((k.to_string(), v), w))
            })
            .collect()
    };

    assert_reshuffle_keeps_windows(
        &input.apply(
            "Fixed/Window",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        ),
        "Fixed",
        pairs(|ts| {
            let start = ts - ts.rem_euclid(10_000);
            vec![interval(start, start + 10_000)]
        }),
    );
    assert_reshuffle_keeps_windows(
        &input.apply(
            "Sliding/Window",
            WindowInto::new(
                SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5)),
            ),
        ),
        "Sliding",
        pairs(|ts| {
            let last = ts - ts.rem_euclid(5_000);
            vec![
                interval(last - 5_000, last + 5_000),
                interval(last, last + 10_000),
            ]
        }),
    );
    assert_reshuffle_keeps_windows(
        &input.apply(
            "Sessions/Window",
            WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(5))),
        ),
        "Sessions",
        pairs(|ts| vec![interval(ts, ts + 5_000)]),
    );
}

/// Validates that `Reshuffle` after a `GroupByKey` keeps the groups and their windows: fixed,
/// sliding and merged sessions.
///
/// Covers `ReshuffleTest.testReshuffleAfterFixedWindowsAndGroupByKey`,
/// `ReshuffleTest.testReshuffleAfterSlidingWindowsAndGroupByKey` and
/// `ReshuffleTest.testReshuffleAfterSessionsAndGroupByKey`.
pub fn build_reshuffle_after_group_by_key_keeps_windows(p: &TestPipeline) {
    let input = timestamped_rows(
        p,
        "Input",
        &[("k1", 3, 1_000), ("k1", 5, 3_000), ("k2", 4, 12_000)],
    );
    let group = |k: &str, values: &[i64]| (k.to_string(), values.to_vec());
    let check = |label: &str,
                 windowed: PCollection<(String, i64)>,
                 expected: Vec<((String, Vec<i64>), IntervalWindow)>| {
        let grouped = windowed.group_by_key(format!("{label}/Group"));
        let reshuffled = grouped.reshuffle(format!("{label}/Reshuffle"));
        assert_eq!(
            reshuffled.windowing_strategy_id(),
            grouped.windowing_strategy_id(),
            "{label}: Reshuffle must keep the windowing strategy"
        );
        let sorted = sorted_groups(&reshuffled, &format!("{label}/Sort"));
        passert::that_windowed(format!("{label}/Assert"), &sorted).contains_in_any_order(expected);
    };

    check(
        "Fixed",
        input.apply(
            "Fixed/Window",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        ),
        vec![
            (group("k1", &[3, 5]), interval(0, 10_000)),
            (group("k2", &[4]), interval(10_000, 20_000)),
        ],
    );
    check(
        "Sliding",
        input.apply(
            "Sliding/Window",
            WindowInto::new(
                SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5)),
            ),
        ),
        vec![
            (group("k1", &[3, 5]), interval(-5_000, 5_000)),
            (group("k1", &[3, 5]), interval(0, 10_000)),
            (group("k2", &[4]), interval(5_000, 15_000)),
            (group("k2", &[4]), interval(10_000, 20_000)),
        ],
    );
    check(
        "Sessions",
        input.apply(
            "Sessions/Window",
            WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(5))),
        ),
        vec![
            (group("k1", &[3, 5]), interval(1_000, 8_000)),
            (group("k2", &[4]), interval(12_000, 17_000)),
        ],
    );
}

/// Validates that `GroupByKey` merges session windows: values closer than the gap share one
/// session, and a value after a longer pause starts a new one.
///
/// Covers `GroupByKeyTest$WindowTests.testGroupByKeyMergingWindows`.
pub fn build_session_windows_merge_in_group_by_key(p: &TestPipeline) {
    let grouped = timestamped_rows(
        p,
        "Input",
        &[("s", 10, 1_000), ("s", 20, 4_000), ("s", 30, 20_000)],
    )
    .apply(
        "Sessions",
        WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(5))),
    )
    .group_by_key("Group");
    passert::that_windowed("AssertSessions", &sorted_groups(&grouped, "Sort"))
        .contains_in_any_order([
            (("s".to_string(), vec![10, 20]), interval(1_000, 9_000)),
            (("s".to_string(), vec![30]), interval(20_000, 25_000)),
        ]);
}

/// Validates the output timestamp of a `GroupByKey` for each `OutputTime`: the end of the
/// window, the earliest input and the latest input.
///
/// Covers `GroupByKeyTest$BasicTests.testTimestampCombinerEarliest`,
/// `GroupByKeyTest$BasicTests.testTimestampCombinerLatest` and
/// `WindowTest.testTimestampCombinerEndOfWindow`.
pub fn build_group_by_key_output_time(p: &TestPipeline) {
    let input = timestamped_rows(p, "Input", &[("k", 1, 1_000), ("k", 2, 5_000)]);
    for (label, output_time, expected_ts) in [
        ("EndOfWindow", OutputTime::EndOfWindow, 9_999),
        ("Earliest", OutputTime::EarliestInPane, 1_000),
        ("Latest", OutputTime::LatestInPane, 5_000),
    ] {
        let stamped = input
            .apply(
                format!("{label}/Window"),
                WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
                    .with_output_time(output_time),
            )
            .group_by_key(format!("{label}/Group"))
            .par_do_fn(
                format!("{label}/ReadTimestamp"),
                |(k, values): (String, BeamIterable<i64>), ctx| {
                    let mut values = values.into_vec()?;
                    values.sort_unstable();
                    ctx.emit((k, (ctx.timestamp().as_millis(), values)))
                },
            );
        passert::that_windowed(format!("{label}/Assert"), &stamped).contains_in_any_order([(
            ("k".to_string(), (expected_ts, vec![1, 2])),
            interval(0, 10_000),
        )]);
    }
}

/// Buffers each element with its window and timestamp, and emits the buffer in
/// `finish_bundle`.
#[derive(Default)]
struct EmitWindowedInFinishBundleDoFn {
    buffer: Vec<((String, i64), BoundedWindow, Instant)>,
}

/// A copy starts with an empty buffer: the buffer belongs to one bundle.
impl Clone for EmitWindowedInFinishBundleDoFn {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl DoFn for EmitWindowedInFinishBundleDoFn {
    type In = Windowed<(String, i64)>;
    type Out = (String, i64);

    fn process_element(
        &mut self,
        Windowed { value, window }: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        self.buffer.push((value, window, ctx.timestamp()));
        Ok(())
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> Result {
        self.buffer
            .drain(..)
            .try_for_each(|(value, window, timestamp)| ctx.emit(value, window, timestamp))
    }
}

/// Validates output from `finish_bundle` in interval windows: each element goes back to its
/// own window, so a `GroupByKey` after it groups per window.
///
/// Covers `ParDoTest$LifecycleTests.testWindowingInStartAndFinishBundle`.
pub fn build_finish_bundle_windowed_output(p: &TestPipeline) {
    let grouped = timestamped_rows(
        p,
        "Input",
        &[
            ("k", 1, 1_000),
            ("k", 2, 5_000),
            ("k", 3, 11_000),
            ("k", 4, 15_000),
        ],
    )
    .apply(
        "Window",
        WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
    )
    .par_do(
        "EmitInFinishBundle",
        EmitWindowedInFinishBundleDoFn::default(),
    )
    .group_by_key("Group");
    passert::that_windowed("AssertSums", &sum_groups(&grouped, "Sum")).contains_in_any_order([
        (("k".to_string(), 3), interval(0, 10_000)),
        (("k".to_string(), 7), interval(10_000, 20_000)),
    ]);
}

/// Validates lifted `CombinePerKey` and `count_per_element` in fixed, sliding and session
/// windows: each window gets the result of its own values.
///
/// Covers `CombineTest$WindowingTests.testFixedWindowsCombine`,
/// `CombineTest$WindowingTests.testSlidingWindowsCombine` and
/// `CombineTest$WindowingTests.testSessionsCombine`.
pub fn build_windowed_combine_per_key(p: &TestPipeline) {
    let input = timestamped_rows(
        p,
        "Input",
        &[
            ("a", 1, 1_000),
            ("a", 2, 4_000),
            ("b", 3, 4_000),
            ("a", 4, 12_000),
            ("b", 5, 16_000),
        ],
    );
    let kv = |k: &str, v: i64, start: i64, end: i64| ((k.to_string(), v), interval(start, end));

    let fixed = input
        .apply(
            "Fixed/Window",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .combine_per_key("Fixed/Sum", Sum::new());
    passert::that_windowed("Fixed/Assert", &fixed).contains_in_any_order([
        kv("a", 3, 0, 10_000),
        kv("a", 4, 10_000, 20_000),
        kv("b", 3, 0, 10_000),
        kv("b", 5, 10_000, 20_000),
    ]);

    let sliding = input
        .apply(
            "Sliding/Window",
            WindowInto::new(
                SlidingWindows::of(Duration::from_secs(20)).every(Duration::from_secs(10)),
            ),
        )
        .combine_per_key("Sliding/Sum", Sum::new());
    passert::that_windowed("Sliding/Assert", &sliding).contains_in_any_order([
        kv("a", 3, -10_000, 10_000),
        kv("a", 7, 0, 20_000),
        kv("a", 4, 10_000, 30_000),
        kv("b", 3, -10_000, 10_000),
        kv("b", 8, 0, 20_000),
        kv("b", 5, 10_000, 30_000),
    ]);

    let sessions = input
        .apply(
            "Sessions/Window",
            WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(5))),
        )
        .combine_per_key("Sessions/Sum", Sum::new());
    passert::that_windowed("Sessions/Assert", &sessions).contains_in_any_order([
        kv("a", 3, 1_000, 9_000),
        kv("a", 4, 12_000, 17_000),
        kv("b", 3, 4_000, 9_000),
        kv("b", 5, 16_000, 21_000),
    ]);

    let counts = input
        .map("Count/Keys", |(k, _): (String, i64)| k)
        .apply(
            "Count/Window",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .count_per_element("Count/PerElement");
    passert::that_windowed("Count/Assert", &counts).contains_in_any_order([
        kv("a", 2, 0, 10_000),
        kv("a", 1, 10_000, 20_000),
        kv("b", 1, 0, 10_000),
        kv("b", 1, 10_000, 20_000),
    ]);
}

/// Validates global combines without defaults in fixed windows: one result per window.
///
/// Covers `CombineTest$WindowingTests.testFixedWindowsCombine` for `CombineGlobally`.
pub fn build_windowed_combine_globally(p: &TestPipeline) {
    let windowed = timestamped_rows(
        p,
        "Input",
        &[
            ("k", 1, 1_000),
            ("k", 2, 5_000),
            ("k", 3, 11_000),
            ("k", 4, 15_000),
        ],
    )
    .map("DropKey", |(_, v): (String, i64)| v)
    .apply(
        "Window",
        WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
    );

    let sums = windowed.combine_globally_without_defaults("SumAll", Sum::new());
    passert::that_windowed("AssertSums", &sums)
        .contains_in_any_order([(3, interval(0, 10_000)), (7, interval(10_000, 20_000))]);

    let counts = windowed.apply("CountAll", CountGlobally::new().without_defaults());
    passert::that_windowed("AssertCounts", &counts)
        .contains_in_any_order([(2, interval(0, 10_000)), (2, interval(10_000, 20_000))]);
}

/// Validates that `Flatten` keeps the windowing of its inputs, so a `GroupByKey` after it
/// groups per window.
///
/// Covers `WindowingTest.testWindowPreservation`.
pub fn build_flatten_preserves_windowing(p: &TestPipeline) {
    let window = || WindowInto::new(FixedWindows::of(Duration::from_secs(10)));
    let left = timestamped_rows(p, "Left", &[("k", 1, 1_000), ("k", 3, 11_000)])
        .apply("WindowLeft", window());
    let right = timestamped_rows(p, "Right", &[("k", 2, 5_000), ("k", 4, 15_000)])
        .apply("WindowRight", window());
    let grouped = Flatten::pcollections("FlattenBoth", &[&left, &right]).group_by_key("Group");
    passert::that_windowed("AssertSums", &sum_groups(&grouped, "Sum")).contains_in_any_order([
        (("k".to_string(), 3), interval(0, 10_000)),
        (("k".to_string(), 7), interval(10_000, 20_000)),
    ]);
}
