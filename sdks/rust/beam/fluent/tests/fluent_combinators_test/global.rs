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
use testing::{TestPipeline, passert};

#[tokio::test]
async fn global_sum_and_count_produce_one_value() {
    let p = TestPipeline::new();
    let numbers = p.apply(Create::new("Numbers", vec![10i64, 20, 30]));

    let sum = numbers.combine_globally("GlobalSum", Sum);
    let sum_no_defaults = numbers.combine_globally_without_defaults("GlobalSumNoDefaults", Sum);
    let count = numbers.count_globally("GlobalCount");

    passert::that("AssertSum", &sum).contains_in_any_order([60]);
    passert::that("AssertSumNoDefaults", &sum_no_defaults).contains_in_any_order([60]);
    passert::that("AssertCount", &count).contains_in_any_order([3]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn global_combines_on_empty_input_inject_the_default_only_when_asked() {
    let p = TestPipeline::new();
    let empty = p.apply(Create::new("Empty", Vec::<i64>::new()));

    // `combine_globally`/`count_globally` insert the value of an empty accumulator.
    let sum = empty.combine_globally("GlobalSum", Sum);
    let count = empty.count_globally("GlobalCount");
    // `combine_globally_without_defaults` emits nothing for an empty input.
    let sum_no_defaults = empty.combine_globally_without_defaults("GlobalSumNoDefaults", Sum);

    passert::that("AssertSum", &sum).contains_in_any_order([0]);
    passert::that("AssertCount", &count).contains_in_any_order([0]);
    passert::that("AssertSumNoDefaults", &sum_no_defaults).empty();

    p.run().await.expect("pipeline + assertions");
}
