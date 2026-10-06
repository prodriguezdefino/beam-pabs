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

//! Element-wise, partitioning, windowing and multi-output methods.

use std::time::Duration;

use beam::transforms::{ExceptionElement, ProcessContext};
use fluent::prelude::*;
use testing::{TestPipeline, passert};

use crate::common::kv;

#[tokio::test]
async fn map_filter_and_flat_map_transform_every_element() {
    let p = TestPipeline::new();
    let numbers = p.apply(Create::new("Numbers", vec![1i64, 2, 3, 4, 5]));

    let doubled = numbers.map("Double", |x| x * 2);
    let filtered = doubled.filter("KeepOverFive", |x| *x > 5);
    let expanded = filtered.flat_map("Expand", |x| vec![x, x + 1]);
    let dropped_all = numbers.flat_map("DropAll", |_| Vec::<i64>::new());

    passert::that("AssertDoubled", &doubled).contains_in_any_order([2, 4, 6, 8, 10]);
    passert::that("AssertFiltered", &filtered).contains_in_any_order([6, 8, 10]);
    passert::that("AssertExpanded", &expanded).contains_in_any_order([6, 7, 8, 9, 10, 11]);
    passert::that("AssertDroppedAll", &dropped_all).empty();

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn try_map_routes_errors_to_failures() {
    let p = TestPipeline::new();
    let lines = p.apply(Create::new(
        "Lines",
        vec!["1".to_string(), "x".to_string(), "22".to_string()],
    ));
    let clean = p.apply(Create::new(
        "CleanLines",
        vec!["1".to_string(), "22".to_string(), "333".to_string()],
    ));

    let parsed = lines.try_map("Parse", |s: &String| s.parse::<i64>());
    // When all inputs succeed, every element reaches `output` and `failures` is empty.
    let all_ok = clean.try_map("ParseClean", |s: &String| s.parse::<i64>());

    passert::that("PAssert", &parsed.output).contains_in_any_order([1, 22]);
    passert::that("PAssert", &parsed.failures).contains_in_any_order([Failure::new(
        "x".to_string(),
        "invalid digit found in string",
    )]);
    passert::that("PAssert", &all_ok.output).contains_in_any_order([1, 22, 333]);
    passert::that("PAssert", &all_ok.failures).empty();

    p.run()
        .await
        .expect("an Err from try_map must not fail the pipeline");
}

#[tokio::test]
async fn try_map_exceptions_via() {
    let p = TestPipeline::new();
    let lines = p.apply(Create::new(
        "Lines",
        vec!["7".to_string(), "abc".to_string()],
    ));
    let other = p.apply(Create::new("Other", vec!["5".to_string(), "?".to_string()]));

    // The handler inspects the element and the error.
    let custom = lines.apply(
        TryMap::new("Parse", |s: &String| s.parse::<i64>()).exceptions_via(
            |e: ExceptionElement<String, std::num::ParseIntError>| {
                (e.element.clone(), e.element.len() as i64)
            },
        ),
    );
    let formatted = other.apply(
        TryMap::new("ParseFormatted", |s: &String| s.parse::<i64>())
            .exceptions_via(|e| format!("{}: {}", e.element, e.exception)),
    );

    passert::that("PAssert", &custom.output).contains_in_any_order([7]);
    passert::that("PAssert", &custom.failures).contains_in_any_order([("abc".to_string(), 3i64)]);
    passert::that("PAssert", &formatted.output).contains_in_any_order([5]);
    passert::that("PAssert", &formatted.failures)
        .contains_in_any_order(["?: invalid digit found in string".to_string()]);

    p.run().await.expect("pipeline + assertions");
}

/// Sink for `failures_to` that asserts on what it receives.
struct AssertFailures(Vec<Failure<String>>);

impl PTransform<PCollection<Failure<String>>> for AssertFailures {
    type Output = PCollection<Failure<String>>;

    fn expand(&self, input: &PCollection<Failure<String>>) -> Self::Output {
        passert::that("AssertFailures", input).contains_in_any_order(self.0.clone());
        input.clone()
    }
}

#[tokio::test]
async fn failures_to_applies_the_sink_and_continues_on_output() {
    let p = TestPipeline::new();
    let lines = p.apply(Create::new(
        "Lines",
        vec!["2".to_string(), "oops".to_string()],
    ));

    let doubled = lines
        .try_map("Parse", |s: &String| s.parse::<i64>())
        .failures_to(AssertFailures(vec![Failure::new(
            "oops".to_string(),
            "invalid digit found in string",
        )]))
        .map("Double", |n| n * 2);

    passert::that("AssertDoubled", &doubled).contains_in_any_order([4]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn inspect_and_reshuffle_pass_elements_through_unchanged() {
    let p = TestPipeline::new();
    let numbers = p.apply(Create::new("Numbers", vec![3i64, 1, 2, 2]));

    let inspected = numbers.inspect("Inspect", |_| {});
    let reshuffled = inspected.reshuffle("Reshuffle");

    passert::that("AssertInspected", &inspected).contains_in_any_order([1, 2, 2, 3]);
    passert::that("AssertReshuffled", &reshuffled).contains_in_any_order([1, 2, 2, 3]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn key_by_pairs_each_element_with_its_computed_key() {
    let p = TestPipeline::new();
    let words = p.apply(Create::new(
        "Words",
        vec!["a".to_string(), "bb".to_string(), "cc".to_string()],
    ));

    let keyed = words.key_by("ByLength", |w: &String| w.len() as i64);

    passert::that("AssertKeyed", &keyed).contains_in_any_order([
        (1, "a".to_string()),
        (2, "bb".to_string()),
        (2, "cc".to_string()),
    ]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn partition_routes_each_element_and_flatten_merges_them_back() {
    let p = TestPipeline::new();
    let items = p.apply(Create::new("Items", vec![1i64, 2, 3, 4, 5, 6]));

    let partitions = items.partition("PartByEvenOdd", 2, |val| (*val as usize) % 2);
    let evens = &partitions.collections()[0];
    let odds = &partitions.collections()[1];
    let merged = partitions.flatten("Merge");

    passert::that("PAssert", evens).contains_in_any_order([2, 4, 6]);
    passert::that("PAssert", odds).contains_in_any_order([1, 3, 5]);
    passert::that("AssertMerged", &merged).contains_in_any_order([1, 2, 3, 4, 5, 6]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn flatten_with_one_a_slice_or_an_array_of_others_keeps_every_input() {
    let p = TestPipeline::new();
    let first = p.apply(Create::new("First", vec![1i64]));
    let second = p.apply(Create::new("Second", vec![2i64, 2]));
    let third = p.apply(Create::new("Third", vec![3i64]));

    let with_one = first.flatten("WithOne", &second);
    let with_array = first.flatten("WithArray", &[&second, &third]);
    let slice: &[&PCollection<i64>] = &[&second, &third];
    let with_slice = first.flatten("WithSlice", slice);

    passert::that("AssertWithOne", &with_one).contains_in_any_order([1, 2, 2]);
    passert::that("AssertWithArray", &with_array).contains_in_any_order([1, 2, 2, 3]);
    passert::that("AssertWithSlice", &with_slice).contains_in_any_order([1, 2, 2, 3]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn window_into_assigns_fixed_windows_before_grouping() {
    let p = TestPipeline::new();
    let events = p.apply(Create::new(
        "Events",
        vec![
            ("k".to_string(), (1i64, 1_000i64)),
            ("k".to_string(), (2, 2_000)),
            ("k".to_string(), (10, 11_000)),
            ("k".to_string(), (20, 12_000)),
        ],
    ));

    let windowed_sums = events
        .par_do_fn(
            "AssignTimestamps",
            |(k, (v, ts)): (String, (i64, i64)), ctx| ctx.output((k, v)).at(ts).emit(),
        )
        .window_into("Fixed10s", FixedWindows::of(Duration::from_secs(10)))
        .combine_per_key("SumPerWindow", Sum);

    // Fixed windows split the sums. Without the windows, the output is one `("k", 33)`.
    passert::that("AssertWindowedSums", &windowed_sums)
        .contains_in_any_order([kv("k", 3), kv("k", 30)]);

    p.run().await.expect("pipeline + assertions");
}

/// Routes even numbers to the first output and odd numbers to the second.
#[derive(Clone)]
struct EvenOddDoFn {
    tags: [&'static str; 2],
}

impl DoFn for EvenOddDoFn {
    type In = i64;
    type Out = i64;

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let tag = self.tags[(element % 2) as usize];
        out.output(element * 10).to(tag).emit()
    }
}

#[tokio::test]
async fn multi_output_par_dos_route_elements_to_the_chosen_output() {
    let p = TestPipeline::new();
    let numbers = p.apply(Create::new("Numbers", vec![1i64, 2, 3, 4, 5]));

    let numbered = numbers.par_do_multi("Numbered", 2, EvenOddDoFn { tags: ["0", "1"] });
    let tagged = numbers.par_do_multi_tags(
        "Tagged",
        ["evens", "odds"],
        EvenOddDoFn {
            tags: ["evens", "odds"],
        },
    );

    assert_eq!(numbered.len(), 2);
    assert_eq!(tagged.len(), 2);
    passert::that("PAssert", &numbered[0]).contains_in_any_order([20, 40]);
    passert::that("PAssert", &numbered[1]).contains_in_any_order([10, 30, 50]);
    passert::that("PAssert", &tagged[0]).contains_in_any_order([20, 40]);
    passert::that("PAssert", &tagged[1]).contains_in_any_order([10, 30, 50]);

    p.run().await.expect("pipeline + assertions");
}

#[derive(Clone)]
struct BatchSumDoFn;

impl beam::transforms::BatchedDoFn for BatchSumDoFn {
    type InBatch = Vec<i64>;
    type OutBatch = i64;

    fn process_batch(
        &mut self,
        batch: Self::InBatch,
        ctx: &mut ProcessContext<'_, Self::OutBatch>,
    ) -> Result {
        let sum: i64 = batch.into_iter().sum();
        ctx.emit(sum)
    }
}

#[derive(Clone)]
struct BatchMultiplyDoFn;

impl beam::transforms::BatchedDoFn for BatchMultiplyDoFn {
    type InBatch = Vec<i64>;
    type OutBatch = Vec<i64>;

    fn process_batch(
        &mut self,
        batch: Self::InBatch,
        ctx: &mut ProcessContext<'_, Self::OutBatch>,
    ) -> Result {
        let multiplied: Vec<i64> = batch.into_iter().map(|x| x * 10).collect();
        ctx.emit(multiplied)
    }
}

#[tokio::test]
async fn par_do_batch_variants() {
    let p = TestPipeline::new();
    let numbers = p.apply(Create::new("Numbers", vec![1i64, 2, 3, 4, 5, 6]));
    let five = p.apply(Create::new("Five", vec![1i64, 2, 3, 4, 5]));

    // `par_do_batch` emits one output per batch. `_elementwise` unbatches the outputs.
    let batch_sums = numbers.par_do_batch("BatchSum", 3, BatchSumDoFn);
    let multiplied = five.par_do_batch_elementwise("BatchMul", 2, BatchMultiplyDoFn);

    passert::that("AssertBatchSums", &batch_sums).contains_in_any_order([6, 15]);
    passert::that("AssertMultiplied", &multiplied).contains_in_any_order([10, 20, 30, 40, 50]);

    p.run().await.expect("pipeline + assertions");
}
