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

//! Composites with hierarchical names run on Prism: one user composite applied under two
//! names, with SDK composites and assertions inside the scopes.

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};

/// A user composite. Two instances differ in the name at `apply`.
struct SumOfSquares;

impl PTransform<PCollection<i64>> for SumOfSquares {
    type Output = PCollection<i64>;

    fn expand(&self, input: &PCollection<i64>) -> PCollection<i64> {
        input
            .apply("Square", Map::new(|x: i64| x * x))
            .apply("Sum", CombineGlobally::new(Sum::new()))
    }
}

#[tokio::test]
async fn the_same_composite_under_two_names_runs_twice() {
    let p = TestPipeline::new();
    let small = p.apply("Small", Create::new(vec![1_i64, 2, 3]));
    let large = p.apply("Large", Create::new(vec![10_i64, 20]));

    let small_sum = small.apply("SmallSquares", SumOfSquares);
    let large_sum = large.apply("LargeSquares", SumOfSquares);

    passert::that("SmallSum", &small_sum).contains_in_any_order([14]);
    passert::that("LargeSum", &large_sum).contains_in_any_order([500]);

    p.run().await.expect("pipeline and assertions pass");
}

#[tokio::test]
async fn nested_scopes_run() {
    let p = TestPipeline::new();
    let words = p.apply(
        "Words",
        Create::new(vec!["a".to_string(), "b".to_string(), "a".to_string()]),
    );

    let counts = words.apply("Count", CountPerElement::new());
    let doubled = counts.apply("Double", Map::new(|(w, n): (String, i64)| (w, 2 * n)));

    passert::that("Counts", &doubled)
        .contains_in_any_order([("a".to_string(), 4), ("b".to_string(), 2)]);

    p.run().await.expect("pipeline and assertions pass");
}
