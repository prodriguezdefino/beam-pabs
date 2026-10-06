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

//! Tests for `SplitLines`: its restrictions, and on Prism the same lines for all split
//! sizes, with self-checkpoints.

use std::num::NonZeroUsize;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::sdf::{OffsetRange, SplittableDoFn, SplittableParDo};
use splittable_dofn::SplitLines;

const POEM: &str = "Tyger Tyger, burning bright,\nIn the forests of the night;\n\n\
                    What immortal hand or eye,\nCould frame thy fearful symmetry?\n";
const NOTE: &str = "café\nno newline at the end";

fn lines_per_call(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("a positive number of lines")
}

fn expected_lines() -> Vec<String> {
    POEM.lines().chain(NOTE.lines()).map(String::from).collect()
}

#[test]
fn the_initial_restriction_is_split_into_chunks_of_the_text() {
    let split = SplitLines::new(10, lines_per_call(1));
    let text = "0123456789abcdefghijXYZ".to_string();

    let whole = split
        .initial_restriction(&text)
        .expect("a text length fits in i64");
    assert_eq!(whole, OffsetRange::new(0, 23));
    assert_eq!(
        split.split_restriction(&text, &whole),
        [
            OffsetRange::new(0, 10),
            OffsetRange::new(10, 20),
            OffsetRange::new(20, 23)
        ]
    );
}

/// Runs `split` over the test texts on Prism, checks that the output is their lines, and
/// returns the number of `process_element` calls.
async fn run_and_count_calls(split: SplitLines) -> i64 {
    let p = TestPipeline::new();
    let lines = p
        .apply(
            "Texts",
            Create::new(vec![POEM.to_string(), NOTE.to_string()]),
        )
        .apply("SplitLines", SplittableParDo::new(split));
    passert::that("AssertLines", &lines).contains_in_any_order(expected_lines());

    let result = p.run().await.expect("pipeline and assertions pass");
    result
        .metrics()
        .and_then(|metrics| metrics.counter("splittable_dofn", "process_calls"))
        .expect("the runner reports the process_calls counter")
}

fn count(n: usize) -> i64 {
    i64::try_from(n).expect("a small count")
}

#[tokio::test]
async fn one_chunk_without_checkpoints_emits_every_line() {
    run_and_count_calls(SplitLines::new(0, lines_per_call(1_000))).await;
}

/// Each chunk of 7 bytes is a restriction of its own, with at least one call.
#[tokio::test]
async fn small_chunks_emit_the_same_lines() {
    let calls = run_and_count_calls(SplitLines::new(7, lines_per_call(1_000))).await;
    let chunks = count(POEM.len().div_ceil(7) + NOTE.len().div_ceil(7));
    assert!(calls >= chunks, "{calls} calls for {chunks} chunks");
}

/// One line per call makes each call checkpoint, so the calls are more than the lines.
#[tokio::test]
async fn checkpoints_after_each_line_emit_the_same_lines() {
    let calls = run_and_count_calls(SplitLines::new(0, lines_per_call(1))).await;
    let lines = count(expected_lines().len());
    assert!(calls > lines, "{calls} calls for {lines} lines");
}

#[tokio::test]
async fn small_chunks_with_checkpoints_emit_the_same_lines() {
    run_and_count_calls(SplitLines::new(5, lines_per_call(1))).await;
}
