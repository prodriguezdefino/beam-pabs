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

//! `fold_per_key`, `fold_values`, `fold_globally` and `FoldCombineFn` itself.

use fluent::prelude::*;
use testing::{TestPipeline, passert};

use crate::common::{kv, pairs};

#[tokio::test]
async fn fold_per_key_folds_values_and_merges_partial_accumulators() {
    let p = TestPipeline::new();
    let pairs = pairs(&p);

    // The fold and merge closures are different. If the closures are mixed up, or a
    // value or accumulator is dropped, the `(count, sum)` result changes.
    let count_and_sum = pairs.fold_per_key(
        "CountAndSum",
        (0i64, 0i64),
        |(count, sum), v| (count + 1, sum + v),
        |(c1, s1), (c2, s2)| (c1 + c2, s1 + s2),
    );
    let collected = pairs
        .fold_per_key(
            "Collect",
            Vec::<i64>::new(),
            |mut acc, v| {
                acc.push(v);
                acc
            },
            |mut a, b| {
                a.extend(b);
                a
            },
        )
        .map("SortCollected", |(k, mut values): (String, Vec<i64>)| {
            values.sort();
            (k, values)
        });

    passert::that("AssertCountAndSum", &count_and_sum)
        .contains_in_any_order([("a".to_string(), (2, 30)), ("b".to_string(), (1, 30))]);
    passert::that("AssertCollected", &collected)
        .contains_in_any_order([("a".to_string(), vec![10, 20]), ("b".to_string(), vec![30])]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn fold_values_starts_every_key_from_the_zero_once() {
    let p = TestPipeline::new();
    let pairs = pairs(&p);

    // The zero applies once per key. A zero shared across keys, or applied once per
    // value, gives other totals.
    let folded = pairs.fold_values("FoldValues", 100i64, |acc, val| acc + val);
    // Sequential folding allows non-associative fold functions.
    let squares = pairs.fold_values("SumOfSquares", 0i64, |acc, val| acc + val * val);

    passert::that("AssertFolded", &folded).contains_in_any_order([kv("a", 130), kv("b", 130)]);
    passert::that("AssertSquares", &squares).contains_in_any_order([kv("a", 500), kv("b", 900)]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn fold_globally_with_and_without_input() {
    let p = TestPipeline::new();
    let numbers = p.apply(Create::new("Numbers", vec![1i64, 2, 3, 4, 5]));
    let empty = p.apply(Create::new("Empty", Vec::<i64>::new()));

    let count_and_sum = numbers.fold_globally(
        "CountAndSum",
        (0i64, 0i64),
        |(count, sum), v| (count + 1, sum + v),
        |(c1, s1), (c2, s2)| (c1 + c2, s1 + s2),
    );
    // A distinctive zero shows that the default for an empty input is the zero of the fold.
    let folded_empty = empty.fold_globally(
        "FoldEmpty",
        (0i64, 7i64),
        |(count, sum), v| (count + 1, sum + v),
        |(c1, s1), (c2, s2)| (c1 + c2, s1 + s2),
    );

    passert::that("AssertCountAndSum", &count_and_sum).contains_in_any_order([(5, 15)]);
    passert::that("AssertFoldedEmpty", &folded_empty).contains_in_any_order([(0, 7)]);

    p.run().await.expect("pipeline + assertions");
}

// Direct unit tests for FoldCombineFn: pure logic that needs no runner.

fn concat_fn() -> FoldCombineFn<
    String,
    i64,
    impl Fn(String, i64) -> String + Clone,
    impl Fn(String, String) -> String + Clone,
> {
    FoldCombineFn::new(
        "z".to_string(),
        |acc: String, v: i64| format!("{acc}{v}"),
        |a: String, b: String| format!("{a}|{b}"),
    )
}

#[test]
fn fold_combine_fn_lifecycle() {
    let f = concat_fn();

    // Each accumulator starts from a clone of the zero. Earlier accumulators do not
    // consume or change the zero.
    assert_eq!(f.create_accumulator(), "z");
    assert_eq!(f.add_input(f.create_accumulator(), 1), "z1");
    assert_eq!(f.create_accumulator(), "z");

    // The fold closure adds the inputs. `extract_output` is the identity.
    let acc = [1, 2, 3]
        .into_iter()
        .fold(f.create_accumulator(), |acc, v| f.add_input(acc, v));
    assert_eq!(acc, "z123");
    assert_eq!(f.extract_output(acc), "z123");

    // Accumulators merge left to right. Merging no accumulators gives the zero.
    let merged = f.merge_accumulators(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    assert_eq!(merged, "a|b|c");
    assert_eq!(f.merge_accumulators(vec!["only".to_string()]), "only");
    assert_eq!(f.merge_accumulators(Vec::new()), "z");
}
