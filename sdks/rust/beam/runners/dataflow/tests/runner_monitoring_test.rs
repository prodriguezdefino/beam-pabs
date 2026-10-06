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

//! Integration tests for how the Dataflow runner follows a submitted job.
//!
//! Covers job status polling and the streaming `TestDataflowRunner`.

use std::sync::Arc;
use std::time::Duration;

mod common;
use common::MockDataflowClient;

use beam::options::PipelineOptions;
use beam::pipeline::Pipeline;
use beam::runners::PipelineRunner;
use dataflow::TestDataflowRunner;
use dataflow::client::{
    JOB_STATE_CANCELLED, JOB_STATE_DONE, JOB_STATE_FAILED, JOB_STATE_PENDING, JOB_STATE_RUNNING,
    JobMetricsResponse, JobState, MetricStructuredName, MetricUpdateItem,
};
use dataflow::constants::MAX_CONSECUTIVE_JOB_STATUS_ERRORS;
use dataflow::runner::DataflowRunner;
use fluent::prelude::*;
use testutils::InMemoryFileSystem;

#[tokio::test]
async fn test_dataflow_runner_terminal_failure_handling() {
    let options = PipelineOptions::parse_from([
        "app",
        "--runner=dataflow",
        "--project=beam-fail-proj",
        "--region=us-central1",
        "--temp_location=gs://fail-bucket/temp",
        "--job_name=failing-job",
        "--sdk_container_image=gcr.io/test/rust:v1",
    ]);

    let p = Pipeline::new();
    let fs = Arc::new(InMemoryFileSystem::default());

    // FAILED from creation: a state set from a spawned task races the runner's poll loop.
    let client = Arc::new(MockDataflowClient::with_initial_job_state(JOB_STATE_FAILED));
    let runner = DataflowRunner::with_options_and_clients(options, fs, client);

    let err = runner.run(&p).await.unwrap_err();
    assert!(
        err.to_string().contains("JOB_STATE_FAILED"),
        "Error must report terminal failure: {err}"
    );
}

fn wait_options(job_name: &str) -> PipelineOptions {
    PipelineOptions::parse_from([
        "app".to_string(),
        "--runner=dataflow".to_string(),
        "--project=poll-proj".to_string(),
        "--region=us-central1".to_string(),
        "--temp_location=gs://poll-bucket/temp".to_string(),
        format!("--job_name={job_name}"),
        "--sdk_container_image=gcr.io/test/rust:v1".to_string(),
    ])
}

/// Runs an empty pipeline against `client` with 1 ms polls. Fails instead of hanging.
async fn run_polling(
    client: Arc<MockDataflowClient>,
    job_name: &str,
) -> Result<beam::runners::PipelineResult, beam::runners::RunnerError> {
    let fs = Arc::new(InMemoryFileSystem::default());
    let runner = DataflowRunner::with_options_and_clients(wait_options(job_name), fs, client)
        .with_poll_interval(Duration::from_millis(1));
    tokio::time::timeout(Duration::from_secs(30), runner.run(&Pipeline::new()))
        .await
        .expect("wait_for_job did not return")
}

#[tokio::test]
async fn test_dataflow_runner_waits_through_running_until_done() {
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_PENDING,
    ));
    client.script_get_job([
        Ok(JOB_STATE_PENDING),
        Ok(JOB_STATE_RUNNING),
        Ok(JOB_STATE_RUNNING),
        Ok(JOB_STATE_DONE),
    ]);
    let result = run_polling(client.clone(), "poll-done").await.unwrap();
    assert_eq!(result.job_id, "job-1");
    assert_eq!(result.state, JOB_STATE_DONE);
    assert_eq!(client.get_job_calls(), 4, "must stop polling at DONE");
}

#[tokio::test]
async fn test_dataflow_runner_reports_failure_reached_after_running() {
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.script_get_job([Ok(JOB_STATE_RUNNING), Ok(JOB_STATE_CANCELLED)]);
    let err = run_polling(client.clone(), "poll-cancel")
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Dataflow job 'job-1' failed with terminal state 'JOB_STATE_CANCELLED'"),
        "{err}"
    );
    assert_eq!(client.get_job_calls(), 2);
}

#[tokio::test]
async fn test_dataflow_runner_tolerates_transient_status_errors() {
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    // Fewer than the cap in a row, twice, separated by a success that resets the count.
    let below_cap = (MAX_CONSECUTIVE_JOB_STATUS_ERRORS - 1) as usize;
    client.script_get_job(std::iter::repeat_n(Err(503), below_cap));
    client.script_get_job([Ok(JOB_STATE_RUNNING)]);
    client.script_get_job(std::iter::repeat_n(Err(503), below_cap));
    client.script_get_job([Ok(JOB_STATE_DONE)]);

    let result = run_polling(client.clone(), "poll-transient").await.unwrap();
    assert_eq!(result.state, JOB_STATE_DONE);
    assert_eq!(client.get_job_calls(), 2 * below_cap + 2);
}

#[tokio::test]
async fn test_dataflow_runner_gives_up_after_repeated_status_errors() {
    // get_job errors are retried with a cap, not forever.
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.script_get_job(std::iter::repeat_n(Err(503), 1000));

    let err = run_polling(client.clone(), "poll-dead").await.unwrap_err();
    assert!(
        err.to_string()
            .contains("API error (503): scripted failure for job-1"),
        "{err}"
    );
    assert_eq!(
        client.get_job_calls(),
        MAX_CONSECUTIVE_JOB_STATUS_ERRORS as usize
    );
}

#[tokio::test]
async fn test_dataflow_runner_without_wait_returns_submission_state() {
    let options = PipelineOptions::parse_from([
        "app",
        "--runner=dataflow",
        "--project=nowait-proj",
        "--region=us-central1",
        "--temp_location=gs://nowait-bucket/temp",
        "--job_name=nowait",
        "--sdk_container_image=gcr.io/test/rust:v1",
        "--async_job",
    ]);
    let fs = Arc::new(InMemoryFileSystem::default());
    let client = Arc::new(MockDataflowClient::with_initial_job_state(""));
    let runner = DataflowRunner::with_options_and_clients(options, fs, client.clone());
    let result = runner.run(&Pipeline::new()).await.unwrap();
    assert_eq!(result.job_id, "job-1");
    // An empty state from the create call is reported as PENDING.
    assert_eq!(result.state, JOB_STATE_PENDING);
    assert_eq!(client.get_job_calls(), 0, "must not poll when not waiting");
}

#[tokio::test]
async fn test_test_dataflow_runner_registration_and_delegation() {
    let opts = PipelineOptions::parse_from([
        "test",
        "--runner=TestDataflowRunner",
        "--project=test-proj",
        "--region=us-central1",
        "--temp_location=gs://bucket/temp",
        "--sdk_container_image=apache/beam_rust_sdk:latest",
    ]);

    let runner = TestDataflowRunner::with_options(opts);
    assert_eq!(runner.runner().options().runner, "TestDataflowRunner");

    let registered = beam::runners::registered_runners();
    assert!(
        registered.contains(&"testdataflowrunner"),
        "TestDataflowRunner must be registered in the link-time runner registry: {registered:?}"
    );
}

/// A streaming pipeline holding two assertions, and their names.
fn streaming_assertions_pipeline() -> (Pipeline, Vec<String>) {
    let p = Pipeline::new();
    let numbers = p.apply(Create::new("Create", vec![1i64, 2]));
    testing::passert::that("CountTwo", &numbers).has_count(2);
    testing::passert::that("NotEmpty", &numbers).not_empty();
    let names = testing::passert::assertion_names(&p);
    assert_eq!(names.len(), 2, "{names:?}");
    (p, names)
}

/// A Dataflow metrics response reporting `counters` in the `PAssert` namespace.
fn passert_metrics(counters: &[(String, i64)]) -> JobMetricsResponse {
    JobMetricsResponse {
        metrics: counters
            .iter()
            .map(|(name, value)| MetricUpdateItem {
                name: Some(MetricStructuredName {
                    origin: Some("user".to_string()),
                    name: name.clone(),
                    context: [
                        ("step".to_string(), "s1".to_string()),
                        (
                            "namespace".to_string(),
                            testing::passert::PASSERT_NAMESPACE.to_string(),
                        ),
                    ]
                    .into(),
                }),
                scalar: Some((*value).into()),
                distribution: None,
                gauge: None,
                update_time: None,
            })
            .collect(),
        metric_time: None,
    }
}

fn passed(assertion: &str) -> (String, i64) {
    (testing::passert::success_counter_name(assertion), 1)
}

/// Runs `p` in streaming mode on a `TestDataflowRunner` over `client`, whose job runs
/// until cancelled, and fails the test instead of hanging if the runner never returns.
async fn run_streaming_test(
    client: Arc<MockDataflowClient>,
    p: &Pipeline,
    test_timeout: Duration,
) -> Result<beam::runners::PipelineResult, beam::runners::RunnerError> {
    let mut options = wait_options("streaming-test");
    options.streaming = true;
    let fs = Arc::new(InMemoryFileSystem::default());
    let runner = TestDataflowRunner::from_runner(
        DataflowRunner::with_options_and_clients(options, fs, client)
            .with_poll_interval(Duration::from_millis(1)),
    )
    .with_test_timeout(test_timeout);
    tokio::time::timeout(Duration::from_secs(30), runner.run(p))
        .await
        .expect("the streaming test runner did not return")
}

#[tokio::test]
async fn test_streaming_test_runner_cancels_only_once_every_assertion_passed() {
    let (p, names) = streaming_assertions_pipeline();
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    // One assertion that passed twice must not count as two assertions that passed.
    let aggregate_two = (testing::passert::SUCCESS_COUNTER.to_string(), 2);
    let first = (testing::passert::success_counter_name(&names[0]), 2);
    client.script_metrics([
        passert_metrics(&[]),
        passert_metrics(&[aggregate_two.clone(), first.clone()]),
        passert_metrics(&[aggregate_two, first, passed(&names[1])]),
    ]);

    let result = run_streaming_test(client.clone(), &p, Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(result.state, JOB_STATE_CANCELLED);
    assert_eq!(
        client.get_job_metrics_calls(),
        3,
        "must decide on the third poll"
    );
    assert_eq!(
        client.update_requests(),
        [("job-1".to_string(), JobState::Cancelled)],
        "must cancel exactly once, after every assertion passed"
    );
    let metrics = result.metrics().expect("the deciding metrics are reported");
    testing::passert::verify_assertions(&result, &names).unwrap();
    assert!(!metrics.is_empty());
}

#[tokio::test]
async fn test_streaming_test_runner_cancels_and_fails_on_a_failed_assertion() {
    let (p, names) = streaming_assertions_pipeline();
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.set_metrics(passert_metrics(&[
        passed(&names[0]),
        (testing::passert::failure_counter_name(&names[1]), 1),
        (testing::passert::FAILURE_COUNTER.to_string(), 1),
    ]));

    let err = run_streaming_test(client.clone(), &p, Duration::from_secs(60))
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("PAssert assertion(s) failed") && err.contains(&names[1]),
        "{err}"
    );
    assert_eq!(
        client.update_requests(),
        [("job-1".to_string(), JobState::Cancelled)]
    );
}

#[tokio::test]
async fn test_streaming_test_runner_cancels_and_fails_at_the_timeout() {
    let (p, names) = streaming_assertions_pipeline();
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.set_metrics(passert_metrics(&[passed(&names[0])]));

    let err = run_streaming_test(client.clone(), &p, Duration::from_millis(50))
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("still pending") && err.contains(&names[1]) && !err.contains(&names[0]),
        "must name only the assertion still pending: {err}"
    );
    assert_eq!(
        client.update_requests(),
        [("job-1".to_string(), JobState::Cancelled)]
    );
}

#[tokio::test]
async fn test_streaming_test_runner_reports_a_job_that_failed_on_its_own() {
    let (p, _) = streaming_assertions_pipeline();
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.script_get_job([Ok(JOB_STATE_RUNNING), Ok(JOB_STATE_FAILED)]);

    let err = run_streaming_test(client.clone(), &p, Duration::from_secs(60))
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("JOB_STATE_FAILED"), "{err}");
    assert!(
        client.update_requests().is_empty(),
        "a stopped job is not cancelled"
    );
}

#[tokio::test]
async fn test_streaming_test_runner_passes_a_job_that_finished_on_its_own() {
    // A job that stops successfully is not cancelled; `TestPipeline` then checks the
    // final counts itself.
    let (p, _) = streaming_assertions_pipeline();
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.script_get_job([Ok(JOB_STATE_RUNNING), Ok(JOB_STATE_DONE)]);

    let result = run_streaming_test(client.clone(), &p, Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(result.state, JOB_STATE_DONE);
    assert!(client.update_requests().is_empty());
}

#[tokio::test]
async fn test_streaming_test_runner_fails_a_finished_job_whose_assertion_failed() {
    let (p, names) = streaming_assertions_pipeline();
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_PENDING,
    ));
    client.script_get_job([Ok(JOB_STATE_PENDING), Ok(JOB_STATE_DONE)]);
    client.set_metrics(passert_metrics(&[(
        testing::passert::failure_counter_name(&names[0]),
        1,
    )]));

    let err = run_streaming_test(client.clone(), &p, Duration::from_secs(60))
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("PAssert assertion(s) failed") && err.contains(&names[0]),
        "{err}"
    );
    assert!(
        client.update_requests().is_empty(),
        "a stopped job is not cancelled"
    );
}

#[tokio::test]
async fn test_streaming_test_runner_times_out_a_job_that_never_starts() {
    // Metrics are only read once the job runs, so every assertion is still pending.
    let (p, names) = streaming_assertions_pipeline();
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_PENDING,
    ));

    let err = run_streaming_test(client.clone(), &p, Duration::from_millis(50))
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("still pending") && names.iter().all(|n| err.contains(n.as_str())),
        "{err}"
    );
    assert_eq!(client.get_job_metrics_calls(), 0);
    assert_eq!(
        client.update_requests(),
        [("job-1".to_string(), JobState::Cancelled)]
    );
}

#[tokio::test]
async fn test_streaming_test_runner_waits_for_a_pipeline_without_assertions() {
    // With nothing to watch, the job runs until it stops on its own, or the timeout.
    let p = Pipeline::new();
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.script_get_job([
        Ok(JOB_STATE_RUNNING),
        Ok(JOB_STATE_RUNNING),
        Ok(JOB_STATE_DONE),
    ]);
    let result = run_streaming_test(client.clone(), &p, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(result.state, JOB_STATE_DONE);
    assert_eq!(client.get_job_calls(), 3);

    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    let err = run_streaming_test(client.clone(), &p, Duration::from_millis(50))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("still pending: []"), "{err}");
    assert_eq!(
        client.update_requests(),
        [("job-1".to_string(), JobState::Cancelled)]
    );
}

#[tokio::test]
async fn test_streaming_test_runner_reports_a_job_cancelled_before_its_assertions_passed() {
    let (p, names) = streaming_assertions_pipeline();
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.script_get_job([Ok(JOB_STATE_RUNNING), Ok(JOB_STATE_CANCELLED)]);
    client.set_metrics(passert_metrics(&[passed(&names[0])]));

    let err = run_streaming_test(client.clone(), &p, Duration::from_secs(60))
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("failed with terminal state 'JOB_STATE_CANCELLED'"),
        "{err}"
    );
    assert!(client.update_requests().is_empty());
}
