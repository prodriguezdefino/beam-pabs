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

//! In-graph assertions (`passert`) and `TestStream`.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, TestStream, passert};
use beam::transforms::Sum;

use crate::Expectation;

/// Validates that passing assertions let the job succeed, and that each one ran.
pub fn build_passert_success(p: &TestPipeline) {
    let doubled = p
        .apply(Create::new("Create", vec![1i64, 2, 3]))
        .map("Double", |x: i64| x * 2);

    passert::that("AssertDoubled", &doubled)
        .contains_in_any_order([6, 2, 4])
        .contains([4])
        .has_count(3)
        .not_empty()
        .all("even", |x| x % 2 == 0);
    passert::that("PAssert", &doubled.filter("None", |x: &i64| *x > 100)).empty();
    passert::that_singleton("PAssert", &doubled.combine_globally("Total", Sum)).is_equal_to(12);

    assert_eq!(p.assertion_count(), 7);
    // After the run, `TestPipeline` verifies that all seven ran and passed.
}

/// A failing assertion fails the job, naming the assertion and describing the mismatch.
pub const PASSERT_FAILURE: Expectation =
    Expectation::FailsWith(&["WrongContents", "missing: [4]", "unexpected: [3]"]);

/// Builds a pipeline whose assertion fails; see [`PASSERT_FAILURE`].
pub fn build_passert_failure(p: &TestPipeline) {
    let values = p.apply(Create::new("Create", vec![1i64, 2, 3]));
    passert::that("WrongContents", &values).contains_in_any_order([1, 2, 4]);
}

/// A failure message elides all but the first elements of a large collection.
pub const PASSERT_FAILURE_ELIDES_LARGE_COLLECTIONS: Expectation =
    Expectation::FailsWith(&["(and 5 more)"]);

/// Builds a pipeline asserting that 30 elements are empty; see
/// [`PASSERT_FAILURE_ELIDES_LARGE_COLLECTIONS`].
pub fn build_passert_failure_elides_large_collections(p: &TestPipeline) {
    let values = p.apply(Create::new("Create", 0..30i64));
    passert::that("ExpectsNothing", &values).empty();
}

/// An assertion over an empty collection still runs, and so can fail the job.
pub const PASSERT_ON_EMPTY_COLLECTION: Expectation = Expectation::FailsWith(&["ExpectsElements"]);

/// Builds a pipeline asserting a count of 3 on an empty collection; see
/// [`PASSERT_ON_EMPTY_COLLECTION`].
pub fn build_passert_on_empty_collection(p: &TestPipeline) {
    let nothing = p
        .apply(Create::new("Create", vec![1i64, 2, 3]))
        .filter("DropAll", |_: &i64| false);
    passert::that("ExpectsElements", &nothing).has_count(3);
}

/// Validates TestStream-driven event-time windowing: on-time panes and dropped late data.
pub fn build_test_stream_windowing(p: &TestPipeline) {
    let first = IntervalWindow::new(0, 10_000);
    let second = IntervalWindow::new(10_000, 20_000);

    let counts = p
        .apply(
            TestStream::new("TestStream")
                .add_timestamped_elements([
                    ("a".to_string(), 1_000),
                    ("b".to_string(), 2_000),
                    ("a".to_string(), 3_000),
                ])
                .advance_watermark_to(9_000)
                .add_timestamped_elements([("a".to_string(), 11_000)])
                .advance_watermark_to(25_000)
                // Behind the watermark, with no allowed lateness: dropped.
                .add_timestamped_elements([("a".to_string(), 4_000)])
                .advance_watermark_to_infinity(),
        )
        .apply(WindowInto::new(
            "WindowInto",
            FixedWindows::of(Duration::from_secs(10)),
        ))
        .count_per_element("Count");

    passert::that("FirstWindow", &counts)
        .in_on_time_pane(first)
        .contains_in_any_order([("a".to_string(), 2), ("b".to_string(), 1)]);
    passert::that("SecondWindow", &counts)
        .in_window(second)
        .contains_in_any_order([("a".to_string(), 1)]);
    passert::that("AllWindows", &counts).has_count(3);
}
