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

//! The ValidatesRunner registry the suites and the Dataflow worker are generated from,
//! and the expectations its tests are checked against.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use beam::options::PipelineOptions;
use beam::runners::{PipelineResult, RunnerError};
use beam::testing::{TestPipeline, TestPipelineError};
use tests::{
    Expectation, VALIDATES_RUNNER_TESTS, ValidatesRunnerOptions, find_validates_runner_test,
};

#[test]
fn every_test_has_a_unique_id_that_finds_it() {
    let ids: BTreeSet<&str> = VALIDATES_RUNNER_TESTS.iter().map(|t| t.id).collect();
    assert_eq!(
        ids.len(),
        VALIDATES_RUNNER_TESTS.len(),
        "duplicate test ids"
    );
    for test in VALIDATES_RUNNER_TESTS {
        assert_eq!(
            find_validates_runner_test(test.id).map(|t| t.id),
            Some(test.id)
        );
    }
    assert!(find_validates_runner_test("no_such_test").is_none());
}

#[test]
fn the_test_id_is_a_pipeline_option() {
    let options = PipelineOptions::parse_from(["app", "--vr_test=map_and_filter"]);
    assert_eq!(
        options
            .view_as::<ValidatesRunnerOptions>()
            .unwrap()
            .vr_test
            .as_deref(),
        Some("map_and_filter")
    );
    assert_eq!(
        PipelineOptions::default()
            .view_as::<ValidatesRunnerOptions>()
            .unwrap()
            .vr_test,
        None
    );
}

#[test]
fn every_test_builds_a_pipeline_with_assertions() {
    for test in VALIDATES_RUNNER_TESTS {
        let p = TestPipeline::with_options(PipelineOptions::default()).without_run_enforcement();
        (test.build)(&p);
        assert!(p.assertion_count() > 0, "{} asserts nothing", test.id);
    }
}

fn succeeded() -> Result<PipelineResult, TestPipelineError> {
    Ok(PipelineResult::new("job", "DONE"))
}

fn job_failed(message: &'static str) -> Result<PipelineResult, TestPipelineError> {
    Err(TestPipelineError::Runner(RunnerError::execution(message)))
}

fn verification_failed(message: &str) -> Result<PipelineResult, TestPipelineError> {
    Err(TestPipelineError::Verification(message.to_string()))
}

/// Runs `f`, which must panic with a message containing `fragment`.
fn assert_panics_with(fragment: &str, f: impl FnOnce() + std::panic::UnwindSafe) {
    let payload = std::panic::catch_unwind(f).expect_err("expected a panic");
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains(fragment),
        "panic {message:?} does not mention {fragment:?}"
    );
}

static CUSTOM_CHECK_CALLS: AtomicUsize = AtomicUsize::new(0);

#[test]
fn expectation_check_accepts_and_rejects_outcomes() {
    Expectation::Succeeds.check("t", succeeded());
    assert_panics_with("t: the pipeline should succeed", || {
        Expectation::Succeeds.check("t", job_failed("boom"));
    });
    assert_panics_with("t: the pipeline should succeed", || {
        Expectation::Succeeds.check("t", verification_failed("assertion never ran"));
    });

    Expectation::FailsWith(&["WrongContents", "missing: [4]"])
        .check("t", job_failed("WrongContents failed; missing: [4]"));
    assert_panics_with(r#"t: the error should mention ["missing: [4]"]"#, || {
        Expectation::FailsWith(&["WrongContents", "missing: [4]"])
            .check("t", job_failed("WrongContents failed"));
    });
    assert_panics_with("t: the job should fail, not only the verification", || {
        Expectation::FailsWith(&["WrongContents"]).check("t", verification_failed("WrongContents"));
    });
    assert_panics_with("t: the pipeline should fail, but ended in DONE", || {
        Expectation::FailsWith(&["WrongContents"]).check("t", succeeded());
    });

    let expect = Expectation::Custom(|result| {
        assert_eq!(result.expect("the run succeeded").state, "DONE");
        CUSTOM_CHECK_CALLS.fetch_add(1, Ordering::SeqCst);
    });
    expect.check("t", succeeded());
    assert_eq!(CUSTOM_CHECK_CALLS.load(Ordering::SeqCst), 1);
}
