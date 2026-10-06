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

//! The built-in combine functions on Prism, per key and globally.
//!
//! These functions are composites over `CombinePerKey` and `CombineGlobally`, so they need
//! no runner feature of their own and are not in the ValidatesRunner suite. These tests run
//! them through the runner's lifted combine: partial accumulators per bundle, the shuffle,
//! and the merge.

use std::collections::HashSet;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::{AnySample, ApproximateQuantiles, ApproximateUnique};
use prism::PrismRunner;

/// Values 0..100 for key "a" and 0..10 for key "b".
fn keyed_values(p: &TestPipeline) -> PCollection<(String, i64)> {
    let values: Vec<(String, i64)> = (0..100)
        .map(|v| ("a".to_string(), v))
        .chain((0..10).map(|v| ("b".to_string(), v)))
        .collect();
    p.apply("Values", Create::new(values))
}

async fn run(p: TestPipeline) {
    p.run_with(&PrismRunner::new())
        .await
        .expect("the pipeline and its assertions succeed");
}

#[tokio::test]
async fn mean_per_key_and_globally() {
    let p = TestPipeline::new();
    let values = keyed_values(&p);
    passert::that(
        "AssertMeanPerKey",
        &values.combine_per_key("MeanPerKey", Mean::new()),
    )
    .contains_in_any_order([("a".to_string(), 49.5), ("b".to_string(), 4.5)]);
    let flat = values.map("DropKeys", |(_, v): (String, i64)| v);
    passert::that_singleton(
        "AssertMeanGlobally",
        &flat.combine_globally("MeanGlobally", Mean::new()),
    )
    .is_equal_to((4950.0 + 45.0) / 110.0);
    run(p).await;
}

#[tokio::test]
async fn top_per_key_and_globally() {
    let p = TestPipeline::new();
    let values = keyed_values(&p);
    passert::that(
        "AssertLargest",
        &values.combine_per_key("Largest", Top::largest(3)),
    )
    .contains_in_any_order([
        ("a".to_string(), vec![99, 98, 97]),
        ("b".to_string(), vec![9, 8, 7]),
    ]);
    let words = p.apply(
        "Words",
        Create::new(
            ["a", "abcd", "ab", "abcdef", "abc"]
                .map(String::from)
                .to_vec(),
        ),
    );
    let longest = Top::of(2, |a: &String, b: &String| a.len().cmp(&b.len()));
    passert::that_singleton("AssertLongest", &words.combine_globally("Longest", longest))
        .is_equal_to(vec!["abcdef".to_string(), "abcd".to_string()]);
    run(p).await;
}

/// The sample is random, so the checks are its size and that it holds distinct inputs.
#[tokio::test]
async fn samples_per_key() {
    let p = TestPipeline::new();
    let values = keyed_values(&p);
    let distinct_inputs = |(key, sample): &(String, Vec<i64>), size: usize| {
        let limit = if key == "a" { 100 } else { 10 };
        let distinct: HashSet<&i64> = sample.iter().collect();
        sample.len() == size
            && distinct.len() == size
            && sample.iter().all(|v| (0..limit).contains(v))
    };
    passert::that(
        "AssertSample",
        &values.combine_per_key("Sample", Sample::new(5)),
    )
    .has_count(2)
    .all("5 distinct inputs of the key", move |entry| {
        distinct_inputs(entry, 5)
    });
    passert::that(
        "AssertAnySample",
        &values.combine_per_key("AnySample", AnySample::new(3)),
    )
    .has_count(2)
    .all("3 distinct inputs of the key", move |entry| {
        distinct_inputs(entry, 3)
    });
    run(p).await;
}

#[tokio::test]
async fn approximate_unique_per_key() {
    let p = TestPipeline::new();
    // Each value appears twice, and both keys have fewer distinct values than the sample.
    let twice: Vec<(String, i64)> = (0..2)
        .flat_map(|_| {
            (0..100)
                .map(|v| ("a".to_string(), v))
                .chain((0..10).map(|v| ("b".to_string(), v)))
        })
        .collect();
    let values = p.apply("Values", Create::new(twice));
    let unique = ApproximateUnique::with_sample_size(1000).expect("1000 is a valid size");
    passert::that("AssertUnique", &values.combine_per_key("Unique", unique))
        .contains_in_any_order([("a".to_string(), 100), ("b".to_string(), 10)]);
    run(p).await;
}

#[tokio::test]
async fn approximate_quantiles_globally() {
    let p = TestPipeline::new();
    let values = p.apply("Values", Create::new((0..101i64).collect::<Vec<_>>()));
    let quartiles = ApproximateQuantiles::new(5).expect("5 quantiles are valid");
    passert::that_singleton(
        "AssertQuartiles",
        &values.combine_globally("Quartiles", quartiles),
    )
    .is_equal_to(vec![0, 25, 50, 75, 100]);
    run(p).await;
}

/// A nested `Compose` with `map_output`, per key: every part sees every value of the key.
#[tokio::test]
async fn nested_compose_per_key() {
    let p = TestPipeline::new();
    let values = keyed_values(&p);
    let stats = Compose::new((
        Compose::new((Sum::new().on(|_: &i64| 1i64), Sum::new().on(|v: &i64| *v))),
        Compose::new((
            Min::new().on(|v: &i64| *v),
            Max::new().on(|v: &i64| *v),
            Top::largest(2).on(|v: &i64| *v),
        )),
    ))
    .map_output(|((count, sum), (min, max, top))| (count, sum, min, max, top));
    passert::that("AssertStats", &values.combine_per_key("Stats", stats)).contains_in_any_order([
        ("a".to_string(), (100, 4950, 0, 99, vec![99, 98])),
        ("b".to_string(), (10, 45, 0, 9, vec![9, 8])),
    ]);
    run(p).await;
}

#[tokio::test]
async fn registered_sdk_combine_fns_per_key() {
    let p = TestPipeline::new();
    let numbers = p.apply(
        "Numbers",
        Create::new(vec![
            ("a".to_string(), 3i64),
            ("a".to_string(), 1),
            ("b".to_string(), 7),
            ("a".to_string(), 2),
        ]),
    );
    let totals = numbers.apply("Total", CombinePerKey::new(Sum::new()));
    let top = numbers.apply("Top", CombinePerKey::new(Top::largest(2)));
    let quantiles = numbers.apply(
        "Quantiles",
        CombinePerKey::new(ApproximateQuantiles::new(2).expect("quantiles")),
    );
    passert::that("AssertTotals", &totals)
        .contains_in_any_order([("a".to_string(), 6), ("b".to_string(), 7)]);
    passert::that("AssertTop", &top)
        .contains_in_any_order([("a".to_string(), vec![3, 2]), ("b".to_string(), vec![7])]);
    passert::that("AssertQuantiles", &quantiles)
        .contains_in_any_order([("a".to_string(), vec![1, 3]), ("b".to_string(), vec![7, 7])]);

    run(p).await;
}
