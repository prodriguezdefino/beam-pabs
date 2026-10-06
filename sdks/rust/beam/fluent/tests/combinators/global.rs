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

//! Global combines and counts, including default values for empty inputs.

use fluent::prelude::*;
use testing::{TestPipeline, TestStream, passert};

#[tokio::test]
async fn global_sum_and_count_produce_one_value() {
    let p = TestPipeline::new();
    let numbers = p.apply("Numbers", Create::new(vec![10i64, 20, 30]));

    let sum = numbers.combine_globally("GlobalSum", Sum::new());
    let sum_no_defaults =
        numbers.combine_globally_without_defaults("GlobalSumNoDefaults", Sum::new());
    let count = numbers.count_globally("GlobalCount");

    passert::that("AssertSum", &sum).contains_in_any_order([60]);
    passert::that("AssertSumNoDefaults", &sum_no_defaults).contains_in_any_order([60]);
    passert::that("AssertCount", &count).contains_in_any_order([3]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn global_combines_on_empty_input_inject_the_default_only_when_asked() {
    let p = TestPipeline::new();
    let empty = p.apply("Empty", Create::new(Vec::<i64>::new()));

    // `combine_globally`/`count_globally` insert the value of an empty accumulator.
    let sum = empty.combine_globally("GlobalSum", Sum::new());
    let count = empty.count_globally("GlobalCount");
    // `combine_globally_without_defaults` emits nothing for an empty input.
    let sum_no_defaults =
        empty.combine_globally_without_defaults("GlobalSumNoDefaults", Sum::new());

    passert::that("AssertSum", &sum).contains_in_any_order([0]);
    passert::that("AssertCount", &count).contains_in_any_order([0]);
    passert::that("AssertSumNoDefaults", &sum_no_defaults).empty();

    p.run().await.expect("pipeline + assertions");
}

/// Applies one global combine to its input.
type Expansion = fn(&PCollection<i64>);

#[test]
fn defaulting_global_combines_of_an_unbounded_input_are_construction_errors() {
    let expansions: [(&str, Expansion); 3] = [
        ("combine_globally", |input| {
            input.combine_globally("Sum", Sum::new());
        }),
        ("fold_globally", |input| {
            input.fold_globally("Fold", 0_i64, |a, b| a + b, |a, b| a + b);
        }),
        ("count_globally", |input| {
            input.count_globally("Count");
        }),
    ];
    expansions.into_iter().for_each(|(name, expand)| {
        let p = Pipeline::new();
        let events = p.apply("Events", TestStream::new().add_elements([1_i64]));
        expand(&events);
        let err = p.validate().expect_err(name);
        assert!(
            err.to_string().contains("needs a bounded input"),
            "{name}: {err}"
        );
    });
}

#[test]
fn global_combine_without_defaults_of_an_unbounded_input_is_unbounded() {
    let p = Pipeline::new();
    let sum = p
        .apply("Events", TestStream::new().add_elements([1_i64]))
        .combine_globally_without_defaults("Sum", Sum::new());
    assert_eq!(sum.is_bounded(), IsBounded::Unbounded);
}
