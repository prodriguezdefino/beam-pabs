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

//! `Distinct` and the set operations on Prism, including duplicates, elements on one side
//! only and windows.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::{Distinct, Sets};
use beam::windowing::{FixedWindows, GlobalWindows};
use prism::PrismRunner;

fn s(value: &str) -> String {
    value.to_string()
}

#[tokio::test]
async fn distinct_keeps_one_of_each_element() {
    let p = TestPipeline::new();
    let words = p.apply(
        "Words",
        Create::new(vec![s("a"), s("b"), s("a"), s("c"), s("b"), s("a")]),
    );
    let distinct = words.apply("Distinct", Distinct::new());
    let by_length = words.apply(
        "ByLength",
        DistinctBy::new(|word: &String| word.len() as i64),
    );

    passert::that("AssertDistinct", &distinct).contains_in_any_order([s("a"), s("b"), s("c")]);
    passert::that("AssertByLength", &by_length).has_count(1);

    p.run_with(&PrismRunner::new())
        .await
        .expect("pipeline and assertions pass");
}

#[tokio::test]
async fn set_operations_follow_sql() {
    let p = TestPipeline::new();
    let left = p.apply("Left", Create::new(vec![1i64, 2, 2, 2, 3, 5]));
    let right = p.apply("Right", Create::new(vec![2i64, 2, 3, 3, 4]));

    let outputs = [
        (
            "UnionDistinct",
            left.apply("UnionDistinct", Sets::union_distinct(&right)),
            vec![1, 2, 3, 4, 5],
        ),
        (
            "UnionAll",
            left.apply("UnionAll", Sets::union_all(&right)),
            vec![1, 2, 2, 2, 3, 5, 2, 2, 3, 3, 4],
        ),
        (
            "IntersectDistinct",
            left.apply("IntersectDistinct", Sets::intersect_distinct(&right)),
            vec![2, 3],
        ),
        (
            "IntersectAll",
            left.apply("IntersectAll", Sets::intersect_all(&right)),
            vec![2, 2, 3],
        ),
        (
            "ExceptDistinct",
            left.apply("ExceptDistinct", Sets::except_distinct(&right)),
            vec![1, 5],
        ),
        (
            "ExceptAll",
            left.apply("ExceptAll", Sets::except_all(&right)),
            vec![1, 2, 5],
        ),
    ];
    for (name, output, expected) in outputs {
        passert::that(format!("Assert{name}"), &output).contains_in_any_order(expected);
    }

    p.run_with(&PrismRunner::new())
        .await
        .expect("pipeline and assertions pass");
}

/// Emits each value at its timestamp in seconds.
#[derive(Clone)]
struct StampSeconds;

impl DoFn for StampSeconds {
    type In = (i64, i64);
    type Out = i64;

    fn process_element(
        &mut self,
        (value, seconds): (i64, i64),
        ctx: &mut ProcessContext<'_, i64>,
    ) -> Result {
        ctx.output(value)
            .at(Instant::from_millis(seconds * 1_000))
            .emit()
    }
}

#[tokio::test]
async fn set_operations_apply_in_each_window() {
    let p = TestPipeline::new();
    let windowed = |name: &str, elements: Vec<(i64, i64)>| {
        p.apply(format!("{name}Elements"), Create::new(elements))
            .apply(format!("{name}Stamp"), ParDo::new(StampSeconds))
            .apply(
                format!("{name}Window"),
                WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
            )
    };
    // 7 is in both collections, but in different windows.
    let left = windowed("Left", vec![(7, 1), (8, 12)]);
    let right = windowed("Right", vec![(7, 11), (8, 13)]);

    let both = left
        .apply("Both", Sets::intersect_distinct(&right))
        .apply("Global", WindowInto::new(GlobalWindows));
    passert::that("AssertBoth", &both).contains_in_any_order([8]);

    p.run_with(&PrismRunner::new())
        .await
        .expect("pipeline and assertions pass");
}
