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

//! The shortcuts for the SDK combine functions, globally and per key.

use fluent::prelude::*;
use testing::{TestPipeline, passert};

use crate::common::{kv, s};

#[tokio::test]
async fn global_shortcuts_give_one_value() -> Result {
    let p = TestPipeline::new();
    let numbers = p.apply("Numbers", Create::new(vec![4i64, 1, 3, 2]));

    passert::that("AssertSum", &numbers.sum_globally("Sum")).contains_in_any_order([10]);
    passert::that("AssertMax", &numbers.max_globally("Max")).contains_in_any_order([4]);
    passert::that("AssertMin", &numbers.min_globally("Min")).contains_in_any_order([1]);
    passert::that("AssertMean", &numbers.mean_globally("Mean")).contains_in_any_order([2.5]);
    passert::that("AssertTop", &numbers.top_globally("Top", 2)).contains_in_any_order([vec![4, 3]]);
    passert::that("AssertSmallest", &numbers.smallest_globally("Smallest", 2))
        .contains_in_any_order([vec![1, 2]]);
    passert::that("AssertSample", &numbers.sample_globally("Sample", 4))
        .all("a sample of all 4 elements", |sample: &Vec<i64>| {
            sample.len() == 4
        });
    passert::that(
        "AssertUnique",
        &numbers.approximate_unique_globally("Unique", 16)?,
    )
    .contains_in_any_order([4]);
    passert::that(
        "AssertQuantiles",
        &numbers.approximate_quantiles_globally("Quantiles", 2)?,
    )
    .contains_in_any_order([vec![1, 4]]);

    p.run().await.expect("pipeline + assertions");
    Ok(())
}

#[tokio::test]
async fn per_key_shortcuts_give_one_value_per_key() -> Result {
    let p = TestPipeline::new();
    let scores = p.apply(
        "Scores",
        Create::new(vec![kv("a", 3), kv("b", 7), kv("a", 1), kv("a", 2)]),
    );

    passert::that("AssertSum", &scores.sum_per_key("Sum"))
        .contains_in_any_order([kv("a", 6), kv("b", 7)]);
    passert::that("AssertMax", &scores.max_per_key("Max"))
        .contains_in_any_order([kv("a", 3), kv("b", 7)]);
    passert::that("AssertMin", &scores.min_per_key("Min"))
        .contains_in_any_order([kv("a", 1), kv("b", 7)]);
    passert::that("AssertMean", &scores.mean_per_key("Mean"))
        .contains_in_any_order([(s("a"), 2.0), (s("b"), 7.0)]);
    passert::that("AssertTop", &scores.top_per_key("Top", 2))
        .contains_in_any_order([(s("a"), vec![3, 2]), (s("b"), vec![7])]);
    passert::that("AssertSmallest", &scores.smallest_per_key("Smallest", 1))
        .contains_in_any_order([(s("a"), vec![1]), (s("b"), vec![7])]);
    passert::that("AssertSample", &scores.sample_per_key("Sample", 5)).all(
        "a sample of every value of its key",
        |(key, sample): &(String, Vec<i64>)| sample.len() == if key == "a" { 3 } else { 1 },
    );
    passert::that(
        "AssertUnique",
        &scores.approximate_unique_per_key("Unique", 16)?,
    )
    .contains_in_any_order([kv("a", 3), kv("b", 1)]);
    passert::that(
        "AssertQuantiles",
        &scores.approximate_quantiles_per_key("Quantiles", 2)?,
    )
    .contains_in_any_order([(s("a"), vec![1, 3]), (s("b"), vec![7, 7])]);

    p.run().await.expect("pipeline + assertions");
    Ok(())
}

#[test]
fn invalid_sizes_are_errors() {
    let p = Pipeline::new();
    let numbers = p.apply("Numbers", Create::new(vec![1i64]));
    assert!(
        numbers
            .approximate_quantiles_globally("Quantiles", 1)
            .is_err()
    );
    assert!(numbers.approximate_unique_globally("Unique", 0).is_err());
}
