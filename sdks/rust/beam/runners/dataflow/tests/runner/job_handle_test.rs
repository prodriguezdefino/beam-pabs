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

//! Tests for the job handle that `DataflowRunner::submit` returns.

use std::sync::Arc;
use std::time::Duration;

use beam::metrics::MetricFilter;
use beam::options::PipelineOptions;
use beam::pipeline::Pipeline;
use beam::runners::{JobHandle, JobState as BeamJobState, PipelineRunner};
use beam_runner_dataflow::{
    DataflowRunner, JOB_STATE_FAILED, JOB_STATE_RUNNING, JobMetricsResponse, JobState,
    MetricStructuredName, MetricUpdateItem,
};
use beam_test_utils::InMemoryFileSystem;

use crate::common::MockDataflowClient;

fn options(extra: &[&str]) -> PipelineOptions {
    PipelineOptions::parse_from(
        [
            "app",
            "--runner=dataflow",
            "--project=handle-proj",
            "--region=us-central1",
            "--temp_location=gs://handle-bucket/temp",
            "--job_name=handle-job",
            "--sdk_container_image=gcr.io/test/rust:v1",
        ]
        .iter()
        .chain(extra),
    )
}

/// Submits an empty pipeline against `client` with 1 ms polls.
async fn submit(client: Arc<MockDataflowClient>, extra: &[&str]) -> Box<dyn JobHandle> {
    let runner = DataflowRunner::with_options_and_clients(
        options(extra),
        Arc::new(InMemoryFileSystem::default()),
        client,
    )
    .with_poll_interval(Duration::from_millis(1));
    runner.submit(&Pipeline::new()).await.unwrap()
}

/// Waits on `handle` and fails the test instead of hanging.
async fn wait(
    handle: &dyn JobHandle,
    timeout: Option<Duration>,
) -> Result<beam::runners::PipelineResult, beam::runners::RunnerError> {
    tokio::time::timeout(Duration::from_secs(30), handle.wait_until_finish(timeout))
        .await
        .expect("wait_until_finish did not return")
}

#[tokio::test]
async fn test_submit_returns_without_polling_even_with_async_job_unset() {
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    let handle = submit(Arc::clone(&client), &[]).await;
    assert_eq!(handle.job_id(), "job-1");
    assert_eq!(client.get_job_calls(), 0);
    assert_eq!(handle.state().await.unwrap(), BeamJobState::Running);
    assert_eq!(client.get_job_calls(), 1);
}

#[tokio::test]
async fn test_cancel_requests_cancelled_and_wait_reports_it() {
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    let handle = submit(Arc::clone(&client), &[]).await;
    handle.cancel().await.unwrap();
    assert_eq!(
        client.update_requests(),
        vec![("job-1".to_string(), JobState::Cancelled)]
    );
    let result = wait(&*handle, None).await.unwrap();
    assert_eq!(result.state, BeamJobState::Cancelled);
}

#[tokio::test]
async fn test_drain_requests_drained_and_wait_reports_it() {
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    let handle = submit(Arc::clone(&client), &[]).await;
    handle.drain().await.unwrap();
    assert_eq!(
        client.update_requests(),
        vec![("job-1".to_string(), JobState::Drained)]
    );
    let result = wait(&*handle, None).await.unwrap();
    assert_eq!(result.state, BeamJobState::Drained);
}

#[tokio::test]
async fn test_wait_timeout_returns_the_current_state() {
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    let handle = submit(Arc::clone(&client), &[]).await;
    let result = wait(&*handle, Some(Duration::from_millis(50)))
        .await
        .unwrap();
    assert_eq!(result.job_id, "job-1");
    assert_eq!(result.state, BeamJobState::Running);
    assert!(
        client.update_requests().is_empty(),
        "a timeout must not cancel"
    );
}

#[tokio::test]
async fn test_wait_on_a_failed_job_returns_an_error() {
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.script_get_job([Ok(JOB_STATE_RUNNING), Ok(JOB_STATE_FAILED)]);
    let handle = submit(client, &[]).await;
    let err = wait(&*handle, None).await.unwrap_err();
    assert!(err.to_string().contains("JOB_STATE_FAILED"), "{err}");
}

fn counter(name: &str, value: i64, tentative: bool) -> MetricUpdateItem {
    let context = [
        ("step", "s1"),
        ("namespace", "handle"),
        ("tentative", if tentative { "true" } else { "false" }),
    ];
    MetricUpdateItem {
        name: Some(MetricStructuredName {
            origin: Some("user".to_string()),
            name: name.to_string(),
            context: context
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }),
        scalar: Some(value.into()),
        distribution: None,
        gauge: None,
        set: None,
        bounded_trie: None,
        update_time: None,
    }
}

#[tokio::test]
async fn test_metrics_map_attempted_and_committed_from_get_job_metrics() {
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.set_metrics(JobMetricsResponse {
        metrics: vec![counter("elements", 7, true), counter("elements", 5, false)],
        metric_time: None,
    });
    let handle = submit(Arc::clone(&client), &[]).await;

    let metrics = handle.metrics().await.unwrap();
    let filter = MetricFilter::default()
        .with_namespace("handle")
        .with_name("elements");
    let [elements] = metrics.query_counters(&filter).try_into().unwrap();
    assert_eq!(elements.attempted, Some(7));
    assert_eq!(elements.committed, Some(5));
    assert_eq!(client.get_job_metrics_calls(), 1);
}

#[tokio::test]
async fn test_submit_with_a_template_location_returns_a_finished_handle() {
    let client = Arc::new(MockDataflowClient::new());
    let handle = submit(
        Arc::clone(&client),
        &["--template_location=gs://handle-bucket/templates/t"],
    )
    .await;
    assert_eq!(handle.job_id(), "template");
    assert_eq!(handle.state().await.unwrap(), BeamJobState::Done);
    assert!(
        client.submitted_jobs().is_empty(),
        "a template starts no job"
    );
}
