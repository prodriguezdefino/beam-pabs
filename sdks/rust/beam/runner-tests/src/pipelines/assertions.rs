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
use beam::transforms::{Impulse, Sum};

use crate::Expectation;
use crate::dofns::*;

/// Validates that passing assertions let the job succeed, and that each one ran.
pub fn build_passert_success(p: &TestPipeline) {
    let doubled = p
        .apply("Create", Create::new(vec![1i64, 2, 3]))
        .map("Double", |x: i64| x * 2);

    passert::that("AssertDoubled", &doubled)
        .contains_in_any_order([6, 2, 4])
        .contains([4])
        .has_count(3)
        .not_empty()
        .all("even", |x| x % 2 == 0);
    passert::that(
        "AssertNoneAbove100",
        &doubled.filter("None", |x: &i64| *x > 100),
    )
    .empty();
    passert::that_singleton(
        "AssertTotal",
        &doubled.combine_globally("Total", Sum::new()),
    )
    .is_equal_to(12);

    assert_eq!(p.assertion_count(), 7);
    // After the run, `TestPipeline` verifies that all seven ran and passed.
}

/// A failing assertion fails the job, naming the assertion and describing the mismatch.
pub const PASSERT_FAILURE: Expectation =
    Expectation::FailsWith(&["WrongContents", "missing: [4]", "unexpected: [3]"]);

/// Builds a pipeline whose assertion fails; see [`PASSERT_FAILURE`].
pub fn build_passert_failure(p: &TestPipeline) {
    let values = p.apply("Create", Create::new(vec![1i64, 2, 3]));
    passert::that("WrongContents", &values).contains_in_any_order([1, 2, 4]);
}

/// A failure message elides all but the first elements of a large collection.
pub const PASSERT_FAILURE_ELIDES_LARGE_COLLECTIONS: Expectation =
    Expectation::FailsWith(&["(and 5 more)"]);

/// Builds a pipeline asserting that 30 elements are empty; see
/// [`PASSERT_FAILURE_ELIDES_LARGE_COLLECTIONS`].
pub fn build_passert_failure_elides_large_collections(p: &TestPipeline) {
    let values = p.apply("Create", Create::new(0..30i64));
    passert::that("ExpectsNothing", &values).empty();
}

/// An assertion over an empty collection still runs, and so can fail the job.
pub const PASSERT_ON_EMPTY_COLLECTION: Expectation = Expectation::FailsWith(&["ExpectsElements"]);

/// Builds a pipeline asserting a count of 3 on an empty collection; see
/// [`PASSERT_ON_EMPTY_COLLECTION`].
pub fn build_passert_on_empty_collection(p: &TestPipeline) {
    let nothing = p
        .apply("Create", Create::new(vec![1i64, 2, 3]))
        .filter("DropAll", |_: &i64| false);
    passert::that("ExpectsElements", &nothing).has_count(3);
}

/// Validates TestStream-driven event-time windowing: on-time panes and dropped late data.
pub fn build_test_stream_windowing(p: &TestPipeline) {
    let first = IntervalWindow::new(Instant::EPOCH, Instant::from_millis(10_000));
    let second = IntervalWindow::new(Instant::from_millis(10_000), Instant::from_millis(20_000));

    let counts = p
        .apply(
            "TestStream",
            TestStream::new()
                .add_timestamped_elements([
                    ("a".to_string(), Instant::from_millis(1_000)),
                    ("b".to_string(), Instant::from_millis(2_000)),
                    ("a".to_string(), Instant::from_millis(3_000)),
                ])
                .advance_watermark_to(Instant::from_millis(9_000))
                .add_timestamped_elements([("a".to_string(), Instant::from_millis(11_000))])
                .advance_watermark_to(Instant::from_millis(25_000))
                // Behind the watermark, with no allowed lateness: dropped.
                .add_timestamped_elements([("a".to_string(), Instant::from_millis(4_000))])
                .advance_watermark_to_infinity(),
        )
        .apply(
            "WindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .count_per_element("Count");

    passert::that("FirstWindow", &counts)
        .in_on_time_pane(first)
        .contains_in_any_order([("a".to_string(), 2), ("b".to_string(), 1)]);
    passert::that("SecondWindow", &counts)
        .in_window(second)
        .contains_in_any_order([("a".to_string(), 1)]);
    passert::that("AllWindows", &counts).has_count(3);
}

/// A `DoFn` that returns an `Err` fails the job with the error message.
pub const USER_DOFN_FAILURE: Expectation =
    Expectation::FailsWith(&["custom dofn failure on element 13"]);

/// Builds a pipeline whose `DoFn` fails on element 13; see [`USER_DOFN_FAILURE`].
pub fn build_user_dofn_failure(p: &TestPipeline) {
    let out = p
        .apply("Create", Create::new(vec![1i64, 13, 20]))
        .par_do_fn("FailOn13", |x: i64, ctx| {
            if x == 13 {
                return Err("custom dofn failure on element 13".into());
            }
            ctx.emit(x)
        });
    passert::that("Unreachable", &out).has_count(3);
}

/// A singleton assertion with the wrong value fails the job and names both values.
pub const PASSERT_SINGLETON_NOT_EQUAL: Expectation =
    Expectation::FailsWith(&["SingletonNotEqual", "expected 44, but got 42"]);

/// Builds a pipeline that asserts 44 on a singleton of 42; see
/// [`PASSERT_SINGLETON_NOT_EQUAL`].
///
/// Java: `PAssertTest.testPAssertEqualsSingletonFalse`,
/// `PAssertTest.testPAssertEqualsSingletonFalseDefaultReasonString`.
pub fn build_passert_singleton_not_equal(p: &TestPipeline) {
    let value = p.apply("Create", Create::new(vec![42i64]));
    passert::that_singleton("SingletonNotEqual", &value).is_equal_to(44);
}

/// A singleton assertion on an empty collection fails the job.
pub const PASSERT_SINGLETON_OF_EMPTY: Expectation = Expectation::FailsWith(&[
    "SingletonOfEmpty",
    "expected exactly one element, but got 0",
]);

/// Builds a pipeline with a singleton assertion on an empty collection; see
/// [`PASSERT_SINGLETON_OF_EMPTY`].
///
/// Java: `PAssertTest.testPAssertEqualsSingletonFailsForEmptyPCollection`.
pub fn build_passert_singleton_of_empty(p: &TestPipeline) {
    let empty = p.apply("Create", Create::new(Vec::<i64>::new()));
    passert::that_singleton("SingletonOfEmpty", &empty).is_equal_to(44);
}

/// A singleton assertion on two elements fails the job.
pub const PASSERT_SINGLETON_OF_TWO: Expectation =
    Expectation::FailsWith(&["SingletonOfTwo", "expected exactly one element, but got 2"]);

/// Builds a pipeline with a singleton assertion on two elements; see
/// [`PASSERT_SINGLETON_OF_TWO`].
///
/// Java: `PAssertTest.testPAssertEqualsSingletonFailsForNonSingletonPCollection`.
pub fn build_passert_singleton_of_two(p: &TestPipeline) {
    let two = p.apply("Create", Create::new(vec![44i64, 44]));
    passert::that_singleton("SingletonOfTwo", &two).is_equal_to(44);
}

/// `(value, timestamp in milliseconds)` pairs, each emitted at its timestamp into fixed
/// windows of `window_ms`.
fn timestamped_in_fixed_windows(
    p: &TestPipeline,
    values: Vec<(i64, i64)>,
    window_ms: u64,
) -> PCollection<i64> {
    p.apply("Create", Create::new(values))
        .par_do_fn("AtTimestamp", |(value, ms): (i64, i64), ctx| {
            ctx.output(value).at(Instant::from_millis(ms)).emit()
        })
        .apply(
            "WindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_millis(window_ms))),
        )
}

/// Validates `that_singleton(..).in_on_time_pane(..).is_equal_to(..)` per window, after a
/// `GroupByKey` that closes each window once. One window starts before the epoch.
///
/// Java: `PAssertTest.testWindowedIsEqualTo`.
pub fn build_windowed_singleton_is_equal_to(p: &TestPipeline) {
    let values = timestamped_in_fixed_windows(p, vec![(43, 250), (22, -250)], 500)
        .map("WithKey", |v: i64| (0i64, v))
        .group_by_key("Group")
        .flat_map("Ungroup", |(_, values): (i64, BeamIterable<i64>)| {
            values
                .into_iter()
                .collect::<Result<Vec<i64>>>()
                .expect("group values decode")
        });
    passert::that_singleton("Window0To500", &values)
        .in_on_time_pane(IntervalWindow::new(
            Instant::EPOCH,
            Instant::from_millis(500),
        ))
        .is_equal_to(43);
    passert::that_singleton("WindowMinus500To0", &values)
        .in_on_time_pane(IntervalWindow::new(
            Instant::from_millis(-500),
            Instant::EPOCH,
        ))
        .is_equal_to(22);
}

/// Validates `that(..).in_window(..).satisfies(..)`: each check sees only its window.
///
/// Java: `PAssertTest.testWindowedSerializablePredicate`.
pub fn build_windowed_satisfies(p: &TestPipeline) {
    let values = timestamped_in_fixed_windows(p, vec![(1, 250), (2, 500)], 300);
    passert::that("Window0To300", &values)
        .in_window(IntervalWindow::new(
            Instant::EPOCH,
            Instant::from_millis(300),
        ))
        .satisfies(|values: &[i64]| match values {
            [1] => Ok(()),
            other => Err(format!("expected [1], got {other:?}").into()),
        });
    passert::that("Window300To600", &values)
        .in_window(IntervalWindow::new(
            Instant::from_millis(300),
            Instant::from_millis(600),
        ))
        .satisfies(|values: &[i64]| match values {
            [2] => Ok(()),
            other => Err(format!("expected [2], got {other:?}").into()),
        });
}

/// Validates a multi-output DoFn whose outputs are empty: on empty input both outputs are
/// empty, and on input of one parity the other output is empty.
///
/// Java: `ParDoTest$MultipleInputsAndOutputTests.testParDoEmptyWithTaggedOutput`,
/// `testParDoWithEmptyTaggedOutput`, `testParDoWithOnlyTaggedOutput`.
pub fn build_multi_output_pardo_empty_outputs(p: &TestPipeline) {
    let ParityPCollections { evens, odds } = p
        .apply("CreateNone", Create::new(Vec::<i64>::new()))
        .par_do("SplitNone", SplitTagsDoFn);
    passert::that("NoneEvens", &evens).empty();
    passert::that("NoneOdds", &odds).empty();

    let ParityPCollections { evens, odds } = p
        .apply("CreateEvens", Create::new(vec![2i64, 4]))
        .par_do("SplitEvens", SplitTagsDoFn);
    passert::that("EvensEvens", &evens)
        .contains_in_any_order(["even:2", "even:4"].map(String::from));
    passert::that("EvensOdds", &odds).empty();

    let ParityPCollections { evens, odds } = p
        .apply("CreateOdds", Create::new(vec![1i64, 3]))
        .par_do("SplitOdds", SplitTagsDoFn);
    passert::that("OddsEvens", &evens).empty();
    passert::that("OddsOdds", &odds).contains_in_any_order(["odd:1", "odd:3"].map(String::from));
}

/// Validates that `Impulse` emits one empty byte array.
///
/// Java: `ImpulseTest.testImpulse`.
pub fn build_impulse(p: &TestPipeline) {
    let impulse = p.apply("Impulse", Impulse::new());
    passert::that_singleton("AssertImpulse", &impulse).is_equal_to(Vec::new());
}

/// A composite whose expansion returns its input.
struct Identity;

impl PTransform<PCollection<i64>> for Identity {
    type Output = PCollection<i64>;

    fn expand(self, input: &PCollection<i64>) -> PCollection<i64> {
        input.clone()
    }
}

/// Validates a composite whose expansion returns its input, with a consumer after it.
///
/// Java: `PipelineTest.testIdentityTransform`.
pub fn build_identity_composite(p: &TestPipeline) {
    let same = p
        .apply("Create", Create::new(vec![1i64, 2, 3]))
        .apply("Identity", Identity);
    passert::that("AssertSame", &same).contains_in_any_order([1, 2, 3]);
    passert::that("AssertDoubled", &same.map("Double", |x: i64| x * 2))
        .contains_in_any_order([2, 4, 6]);
}
