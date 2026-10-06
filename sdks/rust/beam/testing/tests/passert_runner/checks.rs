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

//! Every check can fail.

use beam::prelude::*;
use testing::{TestPipeline, passert};

use crate::{FIRST, assert_fails, kv, nothing, one_two_three, pipeline, windowed_counts};

type CheckCase = (&'static str, &'static str, fn(&TestPipeline));

#[tokio::test]
async fn every_check_fails_on_mismatching_input() {
    let cases: &[CheckCase] = &[
        ("Contains", "expected to contain [2, 4]", |p| {
            passert::that("Contains", &one_two_three(p)).contains([2, 4]);
        }),
        ("ContainsTwice", "missing: [1]", |p| {
            passert::that("ContainsTwice", &one_two_three(p)).contains([1, 1]);
        }),
        ("Exact", "unexpected: [3]", |p| {
            passert::that("Exact", &one_two_three(p)).contains_in_any_order([1, 2]);
        }),
        ("ExactOfNothing", "missing: [1]", |p| {
            passert::that("ExactOfNothing", &nothing(p)).contains_in_any_order([1]);
        }),
        ("Empty", "expected no elements, but got [", |p| {
            passert::that("Empty", &one_two_three(p)).empty();
        }),
        (
            "NotEmpty",
            "expected at least one element, but got none",
            |p| {
                passert::that("NotEmpty", &nothing(p)).not_empty();
            },
        ),
        ("Count", "expected 2 element(s), but got 3", |p| {
            passert::that("Count", &one_two_three(p)).has_count(2);
        }),
        (
            "AllEven",
            "expected every element to be even, but 2 of 3 were not",
            |p| {
                passert::that("AllEven", &one_two_three(p)).all("even", |x| x % 2 == 0);
            },
        ),
        ("Custom", "rejected 3 elements", |p| {
            passert::that("Custom", &one_two_three(p))
                .satisfies(|xs| Err(format!("rejected {} elements", xs.len()).into()));
        }),
        ("CustomOnNothing", "saw 0 elements", |p| {
            passert::that("CustomOnNothing", &nothing(p))
                .satisfies(|xs| Err(format!("saw {} elements", xs.len()).into()));
        }),
        ("Equal", "expected 6, but got 5", |p| {
            passert::that_singleton("Equal", &p.apply(Create::new("Create", vec![5i64])))
                .is_equal_to(6);
        }),
        ("SingleCustom", "rejected 5", |p| {
            passert::that_singleton("SingleCustom", &p.apply(Create::new("Create", vec![5i64])))
                .satisfies(|x| Err(format!("rejected {x}").into()));
        }),
        (
            "TwoFives",
            "expected exactly one element, but got 2: [5, 5]",
            |p| {
                passert::that_singleton("TwoFives", &p.apply(Create::new("Create", vec![5i64, 5])))
                    .is_equal_to(5);
            },
        ),
        (
            "NoOne",
            "expected exactly one element, but got 0: []",
            |p| {
                passert::that_singleton("NoOne", &nothing(p)).is_equal_to(1);
            },
        ),
        (
            "Groups",
            "missing: [Group { key: \"a\", values: [1, 3] }]",
            |p| {
                let grouped = p
                    .apply(Create::new("Create", vec![kv("a", 1), kv("a", 2)]))
                    .apply(GroupByKey::new("Group"));
                passert::that_grouped("Groups", &grouped)
                    .contains_in_any_order([("a".to_string(), vec![1, 3])]);
            },
        ),
        (
            "Windows",
            "missing: [((\"a\", 1), (0, 10000))]; unexpected: [((\"a\", 1), (10000, 20000))]",
            |p| {
                passert::that_windowed("Windows", &windowed_counts(p)).contains_in_any_order([
                    (kv("a", 2), FIRST),
                    (kv("b", 1), FIRST),
                    (kv("a", 1), FIRST),
                ]);
            },
        ),
    ];

    for &(name, expected, setup) in cases {
        let p = pipeline();
        setup(&p);
        assert_fails(p, name, expected).await;
    }
}
