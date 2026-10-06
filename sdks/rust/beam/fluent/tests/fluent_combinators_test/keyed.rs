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

//! Per-key grouping, combining and counting.

use fluent::prelude::*;
use testing::{TestPipeline, passert};

use crate::common::{kv, pairs, sorted_groups};

#[tokio::test]
async fn group_by_key_collects_every_value_under_its_key() {
    let p = TestPipeline::new();
    let pairs = pairs(&p);

    let grouped = sorted_groups(&pairs.group_by_key("GroupByKey"), "SortGroupByKey");

    let expected = [("a".to_string(), vec![10, 20]), ("b".to_string(), vec![30])];
    passert::that("AssertGrouped", &grouped).contains_in_any_order(expected);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn combine_per_key_and_count_per_element_aggregate_each_key_separately() {
    let p = TestPipeline::new();
    let pairs = pairs(&p);

    let summed = pairs.combine_per_key("Sum", Sum);
    let keys = pairs.map("Keys", |(k, _)| k);
    let counted = keys.count_per_element("CountPerKey");

    passert::that("AssertSummed", &summed).contains_in_any_order([kv("a", 30), kv("b", 30)]);
    passert::that("AssertCounted", &counted).contains_in_any_order([kv("a", 2), kv("b", 1)]);

    p.run().await.expect("pipeline + assertions");
}
