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

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::Sum;

use super::core_transforms::sorted_groups;
use crate::dofns::*;

/// Validates windowed grouping with `WindowInto` and `group_by_key`: each window's values
/// are grouped separately, and each result lands in the window it belongs to.
pub fn build_windowed_group_by_key(p: &TestPipeline) {
    let sums = p
        .apply(Create::new(
            "Create",
            vec![
                ("k1".to_string(), (1i64, 1_000i64)),
                ("k1".to_string(), (2i64, 2_000i64)),
                ("k1".to_string(), (10i64, 11_000i64)),
                ("k1".to_string(), (20i64, 12_000i64)),
            ],
        ))
        .par_do("AssignTimestamps", ValidatesAssignTimestampDoFn)
        .apply(WindowInto::new(
            "WindowInto",
            FixedWindows::of(Duration::from_secs(10)),
        ))
        .group_by_key("Group")
        .map("SumGrouped", |(k, values): (String, BeamIterable<i64>)| {
            let sum: i64 = values.into_iter().sum();
            (k, sum)
        });

    passert::that_windowed("AssertSums", &sums).contains_in_any_order([
        (("k1".to_string(), 3), IntervalWindow::new(0, 10_000)),
        (("k1".to_string(), 30), IntervalWindow::new(10_000, 20_000)),
    ]);
}

/// Validates that a `DoFn` after `SlidingWindows` runs once per assigned window.
pub fn build_sliding_windows_pardo(p: &TestPipeline) {
    let observed = p
        .apply(Create::new(
            "Create",
            vec![("k".to_string(), (1i64, 7_000i64))],
        ))
        .par_do("AssignTimestamps", ValidatesAssignTimestampDoFn)
        .apply(WindowInto::new(
            "SlidingWindows",
            SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5)),
        ))
        .par_do_fn("ObserveWindow", |(k, v): (String, i64), ctx| {
            let win = ctx.interval_window().ok_or("expected IntervalWindow")?;
            ctx.emit((k, (win.start_millis, v)))
        });

    passert::that_windowed("AssertObserved", &observed).contains_in_any_order([
        (("k".to_string(), (0, 1)), IntervalWindow::new(0, 10_000)),
        (
            ("k".to_string(), (5_000, 1)),
            IntervalWindow::new(5_000, 15_000),
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
    p.apply(Create::new("MagicSquare", rows))
        .par_do("AssignTimestamps", ValidatesAssignTimestampDoFn)
}

fn assert_window_sums(
    source: &PCollection<(String, i64)>,
    sum_fn: impl Fn(&str, PCollection<(String, i64)>) -> PCollection<i64>,
) {
    let w = Duration::from_secs(3);
    let check = |label: &str, windowed: PCollection<(String, i64)>, expected: &[i64]| {
        let sums = sum_fn(label, windowed)
            .apply(WindowInto::new(format!("{label}/Global"), GlobalWindows));
        passert::that(format!("{label}/Assert"), &sums)
            .contains_in_any_order(expected.iter().copied());
    };

    check(
        "Fixed",
        source.apply(WindowInto::new("Fixed/Window", FixedWindows::of(w))),
        &[15, 15, 15],
    );
    check(
        "SlidingFixed",
        source.apply(WindowInto::new(
            "SlidingFixed/Window",
            SlidingWindows::of(w).every(w),
        )),
        &[15, 15, 15],
    );
    check(
        "Sliding",
        source.apply(WindowInto::new(
            "Sliding/Window",
            SlidingWindows::of(w * 3).every(w),
        )),
        &[15, 30, 45, 30, 15],
    );
    check(
        "Session",
        source.apply(WindowInto::new(
            "Session/Window",
            Sessions::with_gap_duration(w),
        )),
        &[45],
    );
}

/// Validates `FixedWindows`, `SlidingWindows`, and `Sessions` with `GroupByKey`.
pub fn build_window_sums_gbk(p: &TestPipeline) {
    let source = magic_square_events(p);
    assert_window_sums(&source, |label, windowed| {
        windowed.group_by_key(format!("{label}/GBK")).map(
            format!("{label}/Sum"),
            |(_, vs): (String, BeamIterable<i64>)| vs.into_iter().sum::<i64>(),
        )
    });
}

/// Validates `FixedWindows`, `SlidingWindows`, and `Sessions` with lifted `CombinePerKey`.
pub fn build_window_sums_lifted(p: &TestPipeline) {
    let source = magic_square_events(p);
    assert_window_sums(&source, |label, windowed| {
        windowed
            .combine_per_key(format!("{label}/Combine"), Sum)
            .map(format!("{label}/Val"), |(_, sum): (String, i64)| sum)
    });
}

/// Validates that re-windowing after `SlidingWindows` preserves per-window multiplicity.
pub fn build_rewindow_preserves_multiplicity(p: &TestPipeline) {
    let rows: Vec<_> = (0..10i64)
        .map(|k| ("key".to_string(), (k, k * 1_000)))
        .collect();
    let grouped = p
        .apply(Create::new("Create", rows))
        .par_do("AssignTimestamps", ValidatesAssignTimestampDoFn)
        .apply(WindowInto::new(
            "Sliding",
            SlidingWindows::of(Duration::from_secs(6)).every(Duration::from_secs(2)),
        ))
        .apply(WindowInto::new(
            "Rewindow1",
            FixedWindows::of(Duration::from_secs(5)),
        ))
        .apply(WindowInto::new(
            "Rewindow2",
            FixedWindows::of(Duration::from_secs(5)),
        ))
        .group_by_key("Group");
    let sorted = sorted_groups(&grouped, "Sort").apply(WindowInto::new("ToGlobal", GlobalWindows));

    let w0: Vec<i64> = (0..5).flat_map(|x| [x, x, x]).collect();
    let w1: Vec<i64> = (5..10).flat_map(|x| [x, x, x]).collect();

    passert::that("AssertRewindowed", &sorted)
        .contains_in_any_order([("key".to_string(), w0), ("key".to_string(), w1)]);
}
