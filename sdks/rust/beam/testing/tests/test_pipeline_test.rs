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

//! Configuration, assertion counting, and run enforcement of `TestPipeline`.
//!
//! To test enforcement, runs select a runner name that is not registered. To test
//! verification, runs use [`CannedRunner`], a fake runner that returns fixed metrics.
//! `tests/passert_runner` covers real runs.

use std::future::Future;
use std::pin::Pin;

use beam::metrics::{MetricResults, MetricsContainer};
use beam::options::PipelineOptions;
use beam::prelude::*;
use beam::runners::{PipelineResult, PipelineRunner, RunnerError};
use testing::{TestPipeline, TestPipelineError, parse_test_pipeline_options, passert};

fn unknown_runner() -> PipelineOptions {
    PipelineOptions::with_runner("no-such-runner")
}

/// Fake runner that returns fixed metrics or errors.
struct CannedRunner {
    /// Pairs of `(counter_name, value)` in the `PAssert` namespace, or `None` for no metrics.
    counters: Option<Vec<(String, i64)>>,
    /// If set, the run fails with this message.
    error: Option<String>,
}

impl CannedRunner {
    fn with_counters(counters: &[(String, i64)]) -> Self {
        Self {
            counters: Some(counters.to_vec()),
            error: None,
        }
    }

    fn outcome(&self) -> Result<PipelineResult, RunnerError> {
        if let Some(message) = &self.error {
            return Err(RunnerError::execution(message.clone()));
        }
        let result = PipelineResult::new("canned", "DONE");
        Ok(match &self.counters {
            None => result,
            Some(counters) => {
                let container = MetricsContainer::new();
                for (name, value) in counters {
                    container.inc_counter("t", passert::PASSERT_NAMESPACE, name, *value);
                }
                result.with_metrics(MetricResults::from_container(&container))
            }
        })
    }
}

// `PipelineRunner` is an `async_trait`. This is its expansion, written out so that the
// test needs no extra dependency.
impl PipelineRunner for CannedRunner {
    fn run<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _pipeline: &'life1 Pipeline,
    ) -> Pin<Box<dyn Future<Output = Result<PipelineResult, RunnerError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let outcome = self.outcome();
        Box::pin(async move { outcome })
    }
}

fn success(name: &str, times: i64) -> Vec<(String, i64)> {
    vec![
        (passert::SUCCESS_COUNTER.to_string(), times),
        (passert::success_counter_name(name), times),
    ]
}

/// Pipeline holding the assertions `First` and `Second`.
fn two_assertions() -> TestPipeline {
    let p = TestPipeline::with_options(unknown_runner());
    let values = p.apply(Create::new("Create", vec![1i64]));
    passert::that("First", &values).has_count(1);
    passert::that("Second", &values).not_empty();
    p
}

fn verification_error(err: TestPipelineError) -> String {
    match err {
        TestPipelineError::Verification(message) => message,
        other => panic!("expected a verification error, got {other:?}"),
    }
}

#[test]
fn parse_test_pipeline_options_reads_flags_and_defaults() {
    let options = parse_test_pipeline_options("--runner=dataflow  --job_name=it");
    assert_eq!(options.runner, "dataflow");
    assert_eq!(options.job_name.as_deref(), Some("it"));

    assert_eq!(
        parse_test_pipeline_options("").runner,
        PipelineOptions::default().runner
    );
}

#[tokio::test]
async fn verification_passes_when_every_assertion_passed() {
    let p = two_assertions();
    let runner = CannedRunner::with_counters(&[success("First", 1), success("Second", 1)].concat());
    let result = p.run_with(&runner).await.unwrap();
    assert_eq!(result.job_id, "canned");
}

#[tokio::test]
async fn run_with_verifies_assertions_against_runner_counters() {
    // Two successes for two assertions, but both come from `First`.
    let p = two_assertions();
    let runner = CannedRunner::with_counters(&success("First", 2));
    let err = verification_error(p.run_with(&runner).await.unwrap_err());
    assert!(
        err.starts_with(r#"1 of 2 PAssert assertion(s) never ran: ["Second"]"#),
        "{err}"
    );

    // No assertion ran.
    let p = two_assertions();
    let runner = CannedRunner::with_counters(&[]);
    let err = verification_error(p.run_with(&runner).await.unwrap_err());
    assert!(
        err.starts_with(r#"2 of 2 PAssert assertion(s) never ran: ["First", "Second"]"#),
        "{err}"
    );

    // An assertion failed.
    let p = two_assertions();
    let runner = CannedRunner::with_counters(
        &[
            success("First", 1),
            success("Second", 1),
            vec![(passert::failure_counter_name("Second"), 1)],
        ]
        .concat(),
    );
    let err = verification_error(p.run_with(&runner).await.unwrap_err());
    assert_eq!(err, r#"PAssert assertion(s) failed: ["Second"]"#);

    // The result has no metrics.
    let p = two_assertions();
    let runner = CannedRunner {
        counters: None,
        error: None,
    };
    let err = p.run_with(&runner).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "pipeline succeeded but its assertions could not be verified: the runner reported \
         no metrics for this pipeline"
    );
}

#[tokio::test]
async fn a_pipeline_without_assertions_needs_no_metrics() {
    let p = TestPipeline::with_options(unknown_runner());
    p.apply(Create::new("Create", vec![1i64]));
    let runner = CannedRunner {
        counters: None,
        error: None,
    };
    p.run_with(&runner).await.unwrap();
}

#[tokio::test]
async fn a_runner_failure_is_reported_as_such() {
    let p = two_assertions();
    let runner = CannedRunner {
        counters: None,
        error: Some("boom".to_string()),
    };
    let err = p.run_with(&runner).await.unwrap_err();
    assert!(
        matches!(&err, TestPipelineError::Runner(e) if e.to_string().contains("boom")),
        "{err:?}"
    );
}

#[test]
fn run_enforcement_rules() {
    // Empty pipeline need not run.
    drop(TestPipeline::with_options(unknown_runner()));

    // Enforcement can be disabled.
    let disabled = TestPipeline::with_options(unknown_runner()).without_run_enforcement();
    disabled.apply(Create::new("Create", vec![1i64]));
    drop(disabled);

    // Dropping an unrun pipeline panics.
    let payload = std::panic::catch_unwind(|| {
        let p = TestPipeline::with_options(unknown_runner());
        p.apply(Create::new("Create", vec![1i64]));
        drop(p);
    })
    .expect_err("dropping an unrun pipeline must panic");
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(message.contains("dropped without being run"), "{message:?}");
}

#[tokio::test]
async fn a_failed_run_still_counts_as_run() {
    let p = TestPipeline::with_options(unknown_runner());
    p.apply(Create::new("Create", vec![1i64]));

    let err = p.run().await.unwrap_err();
    assert!(
        matches!(
            err,
            TestPipelineError::Runner(RunnerError::UnknownRunner { .. })
        ),
        "{err:?}"
    );
}

#[tokio::test]
#[should_panic(expected = "added to the TestPipeline after it was run")]
async fn adding_transforms_after_running_panics() {
    let p = TestPipeline::with_options(unknown_runner());
    p.apply(Create::new("Create", vec![1i64]));
    let _ = p.run().await;

    p.apply(Create::new("Late", vec![2i64]));
}
