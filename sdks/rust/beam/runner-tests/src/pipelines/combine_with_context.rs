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

//! Combines that read side inputs (`CombineFnWithContext`).

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::{CombineContext, CombineFnWithContext};

use crate::dofns::AssignTimestampDoFn;

/// Concatenates the digits of the values, after the seed that a singleton side input gives.
/// The output is `seed:digits`, with the digits sorted.
///
/// Java: `CombineTest.TestCombineFnWithContext`.
pub struct SeedConcatFn;

type Seeded<'c> = CombineContext<'c, SingletonView<i64>>;

impl CombineFnWithContext<SingletonView<i64>> for SeedConcatFn {
    type Input = i64;
    type Accum = (String, String);
    type Output = String;

    fn create_accumulator(&self, ctx: &Seeded<'_>) -> Result<(String, String)> {
        Ok((ctx.side_inputs().to_string(), String::new()))
    }

    fn add_input(
        &self,
        (seed, digits): (String, String),
        value: i64,
        ctx: &Seeded<'_>,
    ) -> Result<(String, String)> {
        if seed != ctx.side_inputs().to_string() {
            return Err(format!("the side input changed from {seed}").into());
        }
        Ok((seed, digits + &value.to_string()))
    }

    fn merge_accumulators(
        &self,
        accumulators: Vec<(String, String)>,
        ctx: &Seeded<'_>,
    ) -> Result<(String, String)> {
        let digits = accumulators.into_iter().map(|(_, digits)| digits).collect();
        Ok((ctx.side_inputs().to_string(), digits))
    }

    fn extract_output(
        &self,
        (seed, digits): (String, String),
        _ctx: &Seeded<'_>,
    ) -> Result<String> {
        let mut sorted = digits.chars().collect::<Vec<_>>();
        sorted.sort_unstable();
        Ok(format!("{seed}:{}", sorted.into_iter().collect::<String>()))
    }
}

/// The per-key and global combines of `input` with the global sum of its values as the side
/// input. The global combine has no default.
fn combine_with_seed(
    input: &PCollection<(String, i64)>,
    sum: SingletonView<i64>,
) -> (PCollection<(String, String)>, PCollection<String>) {
    let per_key = input.apply(
        "CombinePerKeyWithContext",
        CombinePerKey::new(SeedConcatFn).with_side_inputs(&sum),
    );
    let globally = input.map("Values", |(_, v): (String, i64)| v).apply(
        "CombineGloballyWithContext",
        CombineGlobally::new(SeedConcatFn)
            .without_defaults()
            .with_side_inputs(sum),
    );
    (per_key, globally)
}

/// Validates a per-key and a global combine that read the global sum as a side input.
///
/// Java: `CombineTest.CombineWithContextTests.testSimpleCombineWithContext`.
pub fn build_simple_combine_with_context(p: &TestPipeline) {
    let input = p.apply(
        "Create",
        Create::new(vec![
            ("a".to_string(), 1i64),
            ("a".to_string(), 1),
            ("a".to_string(), 4),
            ("b".to_string(), 1),
            ("b".to_string(), 13),
        ]),
    );
    let sum = input
        .map("SumValues", |(_, v): (String, i64)| v)
        .sum_globally("Sum")
        .as_singleton();
    let (per_key, globally) = combine_with_seed(&input, sum);
    passert::that("AssertPerKey", &per_key).contains_in_any_order([
        ("a".to_string(), "20:114".to_string()),
        ("b".to_string(), "20:113".to_string()),
    ]);
    passert::that("AssertGlobally", &globally).contains_in_any_order(["20:111134".to_string()]);
}

/// Validates the combines of [`build_simple_combine_with_context`] on an empty input, and the
/// default of a global combine with a side input.
///
/// Java: `CombineTest.CombineWithContextTests.testSimpleCombineWithContextEmpty`.
pub fn build_simple_combine_with_context_empty(p: &TestPipeline) {
    let input = p.apply("Create", Create::new(Vec::<(String, i64)>::new()));
    let sum = input
        .map("SumValues", |(_, v): (String, i64)| v)
        .sum_globally("Sum")
        .as_singleton();
    let (per_key, globally) = combine_with_seed(&input, sum.clone());
    let with_default = input.map("DefaultValues", |(_, v): (String, i64)| v).apply(
        "CombineGloballyWithDefault",
        CombineGlobally::new(SeedConcatFn).with_side_inputs(sum),
    );
    passert::that("AssertPerKey", &per_key).empty();
    passert::that("AssertGlobally", &globally).empty();
    passert::that("AssertDefault", &with_default).contains_in_any_order(["0:".to_string()]);
}

/// Validates the combines in fixed windows of 2 ms, with the sum of each window as the side
/// input of that window.
///
/// Java: `CombineTest.WindowingTests.testFixedWindowsCombineWithContext`.
pub fn build_fixed_windows_combine_with_context(p: &TestPipeline) {
    let input = p
        .apply(
            "Create",
            Create::new(
                [
                    ("a", 1, 0),
                    ("a", 1, 1),
                    ("a", 4, 6),
                    ("b", 1, 7),
                    ("b", 13, 8),
                ]
                .map(|(k, v, ts): (&str, i64, i64)| (k.to_string(), (v, ts)))
                .to_vec(),
            ),
        )
        .par_do("Timestamp", AssignTimestampDoFn)
        .apply(
            "WindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_millis(2))),
        );
    let sum = input
        .map("SumValues", |(_, v): (String, i64)| v)
        .apply(
            "Sum",
            CombineGlobally::new(Sum::<i64>::new()).without_defaults(),
        )
        .as_singleton();
    let (per_key, globally) = combine_with_seed(&input, sum);
    passert::that("AssertPerKey", &per_key).contains_in_any_order(
        [("a", "2:11"), ("a", "5:4"), ("b", "5:1"), ("b", "13:13")]
            .map(|(k, v)| (k.to_string(), v.to_string())),
    );
    passert::that("AssertGlobally", &globally)
        .contains_in_any_order(["2:11", "5:14", "13:13"].map(String::from));
}
