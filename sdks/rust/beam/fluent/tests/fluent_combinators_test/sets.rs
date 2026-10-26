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

//! `distinct`, `distinct_by` and the set operations.

use fluent::prelude::*;
use testing::{TestPipeline, passert};

use crate::common::s;

#[tokio::test]
async fn distinct_and_set_operations_chain() {
    let p = TestPipeline::new();
    let tags = p.apply(
        "Tags",
        Create::new(vec![s("rust"), s("go"), s("rust"), s("java")]),
    );
    let popular = p.apply(
        "Popular",
        Create::new(vec![s("java"), s("python"), s("rust")]),
    );

    passert::that("AssertDistinct", &tags.distinct("Distinct")).contains_in_any_order([
        s("rust"),
        s("go"),
        s("java"),
    ]);
    passert::that(
        "AssertOneOfEachLength",
        &tags.distinct_by("OneOfEachLength", |tag: &String| tag.len() as i64),
    )
    .has_count(2);
    passert::that("AssertUnion", &tags.union_distinct("Union", &popular)).contains_in_any_order([
        s("rust"),
        s("go"),
        s("java"),
        s("python"),
    ]);
    passert::that("AssertCommon", &tags.intersect_distinct("Common", &popular))
        .contains_in_any_order([s("rust"), s("java")]);
    passert::that("AssertNiche", &tags.except_distinct("Niche", &popular))
        .contains_in_any_order([s("go")]);
    passert::that("AssertAllTags", &tags.union_all("AllTags", &popular)).has_count(7);
    passert::that(
        "AssertCommonAll",
        &tags.intersect_all("CommonAll", &popular),
    )
    .contains_in_any_order([s("rust"), s("java")]);
    passert::that("AssertExtra", &tags.except_all("Extra", &popular))
        .contains_in_any_order([s("go"), s("rust")]);

    p.run().await.expect("pipeline + assertions");
}
