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

//! Executes assertions on Prism, linked through the `prism` feature of this crate.
//!
//! Each check and each window/pane selector must pass on the correct input. On the
//! incorrect input, it must FAIL the run, with a message that names the assertion. A
//! check that cannot fail makes every test that uses it a silent pass. These tests need
//! Prism: set `BEAM_PRISM_PATH`.

use std::time::Duration;

use beam::options::PipelineOptions;
use beam::prelude::*;
use testing::{TestPipeline, TestPipelineError, TestStream};

pub(crate) fn pipeline() -> TestPipeline {
    TestPipeline::with_options(PipelineOptions::default())
}

/// Runs `p`, which must fail because of the assertion `name`, with a message
/// containing `expected`.
pub(crate) async fn assert_fails(p: TestPipeline, name: &str, expected: &str) {
    let err = p
        .run()
        .await
        .expect_err("the failing assertion must fail the run");
    assert!(
        matches!(err, TestPipelineError::Runner(_)),
        "a failed check fails the job itself: {err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains(&format!("PAssert '{name}")),
        "the error should name the assertion '{name}': {message}"
    );
    assert!(
        message.contains(expected),
        "the error should contain {expected:?}: {message}"
    );
}

pub(crate) fn one_two_three(p: &TestPipeline) -> PCollection<i64> {
    p.apply(Create::new("Create", vec![1i64, 2, 3]))
}

pub(crate) fn nothing(p: &TestPipeline) -> PCollection<i64> {
    one_two_three(p).apply(Filter::new("DropAll", |_: &i64| false))
}

pub(crate) const FIRST: IntervalWindow = IntervalWindow {
    start_millis: 0,
    end_millis: 10_000,
};
pub(crate) const SECOND: IntervalWindow = IntervalWindow {
    start_millis: 10_000,
    end_millis: 20_000,
};

/// Per-element counts in 10 s fixed windows, fired once each by the default trigger:
/// `FIRST` holds `("a", 2)` and `("b", 1)`, `SECOND` holds `("a", 1)`.
pub(crate) fn windowed_counts(p: &TestPipeline) -> PCollection<(String, i64)> {
    p.apply(
        TestStream::new("TestStream")
            .add_timestamped_elements([
                ("a".to_string(), 1_000),
                ("b".to_string(), 2_000),
                ("a".to_string(), 3_000),
                ("a".to_string(), 11_000),
            ])
            .advance_watermark_to_infinity(),
    )
    .apply(WindowInto::new(
        "WindowInto",
        FixedWindows::of(Duration::from_secs(10)),
    ))
    .apply(CountPerElement::new("Count"))
}

pub(crate) fn kv(key: &str, value: i64) -> (String, i64) {
    (key.to_string(), value)
}

mod checks;
mod passing;
mod selectors;
