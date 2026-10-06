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

use beam::prelude::*;
use beam::testing::{TestPipeline, TestStream, passert};

use super::core_transforms::sorted_groups;

/// The one window every element here falls into.
const WINDOW: IntervalWindow = IntervalWindow {
    start_millis: 0,
    end_millis: 10_000,
};

fn keyed(value: i64, timestamp: i64) -> ((String, i64), i64) {
    (("k".to_string(), value), timestamp)
}

fn fixed_windows() -> WindowInto<FixedWindows> {
    WindowInto::new("WindowInto", FixedWindows::of(Duration::from_secs(10)))
}

/// Validates a repeated count trigger in discarding mode: every two elements fire a pane
/// holding exactly those two, before the window closes.
pub fn build_trigger_repeated_count_discarding(p: &TestPipeline) {
    let panes = p
        .apply(
            TestStream::new("TestStream")
                .add_timestamped_elements([keyed(1, 1_000), keyed(2, 2_000)])
                .advance_watermark_to(5_000)
                .add_timestamped_elements([keyed(3, 3_000), keyed(4, 4_000)])
                .advance_watermark_to_infinity(),
        )
        .apply(
            fixed_windows()
                .triggering(Trigger::repeatedly(Trigger::after_count(2)))
                .discarding_fired_panes(),
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
            TestStream::new("TestStream")
                .add_timestamped_elements([keyed(10, 1_000), keyed(20, 2_000)])
                .advance_watermark_to(5_000)
                .add_timestamped_elements([keyed(30, 3_000), keyed(40, 4_000)])
                .advance_watermark_to(6_000)
                // Too few to fire early; only the on-time pane picks it up.
                .add_timestamped_elements([keyed(50, 4_500)])
                .advance_watermark_to_infinity(),
        )
        .apply(
            fixed_windows()
                .triggering(
                    Trigger::after_end_of_window().with_early_firings(Trigger::after_count(2)),
                )
                .accumulating_fired_panes(),
        )
        .group_by_key("Group")
        .map("Sum", |(k, v): (String, BeamIterable<i64>)| {
            (k, v.into_iter().sum::<i64>())
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
            TestStream::new("TestStream")
                .add_timestamped_elements([keyed(1, 1_000)])
                .advance_watermark_to(20_000)
                // Behind the watermark, but within the allowed lateness.
                .add_timestamped_elements([keyed(2, 2_000)])
                .advance_watermark_to(21_000)
                .add_timestamped_elements([keyed(3, 3_000)])
                .advance_watermark_to_infinity(),
        )
        .apply(
            fixed_windows()
                .triggering(
                    Trigger::after_end_of_window().with_late_firings(Trigger::after_count(3)),
                )
                .discarding_fired_panes()
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
