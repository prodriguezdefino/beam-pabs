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

//! Test doubles and mock infrastructure for Dataflow runner integration tests.

#![allow(
    dead_code,
    reason = "shared by several test binaries, each using a different subset"
)]
#![expect(clippy::unwrap_used, reason = "test helper")]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use dataflow::client::{
    ClientError, DataflowApiClient, JOB_STATE_DONE, JobMessageItem, JobMetricsResponse,
    JobResponse, JobState, ListJobMessagesResponse,
};
use dataflow::translate::DataflowJob;

/// In-memory mock Dataflow client for testing and offline development.
#[derive(Debug, Clone, Default)]
pub struct MockDataflowClient {
    jobs: Arc<Mutex<Vec<JobResponse>>>,
    messages: Arc<Mutex<Vec<JobMessageItem>>>,
    metrics: Arc<Mutex<Option<JobMetricsResponse>>>,
    initial_job_state: Option<String>,
    /// Scripted `get_job` outcomes, consumed one per call: `Ok(state)` reports that state
    /// (and records it on the job), `Err(status)` fails with an API error. When the script
    /// is exhausted the stored job is returned as is.
    get_job_script: Arc<Mutex<VecDeque<Result<String, u16>>>>,
    get_job_calls: Arc<Mutex<usize>>,
    submitted: Arc<Mutex<Vec<DataflowJob>>>,
    /// Every state requested through `update_job`, as `(job_id, state)`, in call order.
    update_requests: Arc<Mutex<Vec<(String, JobState)>>>,
    /// Scripted `get_job_metrics` responses, consumed one per call. When the script is
    /// exhausted the metrics set with `set_metrics` are returned.
    metrics_script: Arc<Mutex<VecDeque<JobMetricsResponse>>>,
    get_job_metrics_calls: Arc<Mutex<usize>>,
}

impl MockDataflowClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a mock whose jobs start in `state` rather than [`JOB_STATE_DONE`].
    ///
    /// Terminal-state tests need the state in place before `create_job` returns. Mutating
    /// it afterwards from a spawned task races the runner's poll loop, and losing that race
    /// means the first poll observes the default success state instead.
    pub fn with_initial_job_state(state: impl Into<String>) -> Self {
        Self {
            initial_job_state: Some(state.into()),
            ..Self::default()
        }
    }

    pub fn set_job_state(&self, job_id: &str, state: &str) {
        if let Ok(mut guard) = self.jobs.lock()
            && let Some(j) = guard.iter_mut().find(|j| j.id == job_id)
        {
            j.current_state = state.to_string();
        }
    }

    /// Scripts the outcomes of the next `get_job` calls; see `get_job_script`.
    pub fn script_get_job<'a>(&self, steps: impl IntoIterator<Item = Result<&'a str, u16>>) {
        self.get_job_script
            .lock()
            .unwrap()
            .extend(steps.into_iter().map(|s| s.map(str::to_string)));
    }

    /// Number of `get_job` calls made so far.
    pub fn get_job_calls(&self) -> usize {
        *self.get_job_calls.lock().unwrap()
    }

    /// Every job passed to `create_job`, in submission order.
    pub fn submitted_jobs(&self) -> Vec<DataflowJob> {
        self.submitted.lock().unwrap().clone()
    }

    pub fn add_message(&self, msg: impl Into<String>) {
        if let Ok(mut guard) = self.messages.lock() {
            let id = format!("msg-{}", guard.len() + 1);
            guard.push(JobMessageItem {
                id: Some(id),
                time: Some("2026-09-14T20:00:00Z".to_string()),
                message_text: Some(msg.into()),
                message_importance: Some("JOB_MESSAGE_BASIC".to_string()),
            });
        }
    }

    pub fn set_metrics(&self, metrics: JobMetricsResponse) {
        *self.metrics.lock().unwrap() = Some(metrics);
    }

    /// Scripts the responses of the next `get_job_metrics` calls; see `metrics_script`.
    pub fn script_metrics(&self, responses: impl IntoIterator<Item = JobMetricsResponse>) {
        self.metrics_script.lock().unwrap().extend(responses);
    }

    /// Number of `get_job_metrics` calls made so far.
    pub fn get_job_metrics_calls(&self) -> usize {
        *self.get_job_metrics_calls.lock().unwrap()
    }

    /// Every state requested through `update_job`, as `(job_id, state)`, in call order.
    pub fn update_requests(&self) -> Vec<(String, JobState)> {
        self.update_requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl DataflowApiClient for MockDataflowClient {
    async fn create_job(
        &self,
        project: &str,
        region: &str,
        job: &DataflowJob,
    ) -> Result<JobResponse, ClientError> {
        self.submitted.lock().unwrap().push(job.clone());
        let mut guard = self.jobs.lock().expect("mock client state lock poisoned");
        let job_id = format!("job-{}", guard.len() + 1);
        let resp = JobResponse {
            id: job_id,
            project_id: Some(project.to_string()),
            name: job.name.clone(),
            job_type: Some(job.job_type.clone()),
            current_state: self
                .initial_job_state
                .clone()
                .unwrap_or_else(|| JOB_STATE_DONE.to_string()),
            current_state_time: Some("2026-09-14T20:00:00Z".to_string()),
            create_time: Some("2026-09-14T20:00:00Z".to_string()),
            location: Some(region.to_string()),
        };
        guard.push(resp.clone());
        Ok(resp)
    }

    async fn get_job(
        &self,
        _project: &str,
        _region: &str,
        job_id: &str,
    ) -> Result<JobResponse, ClientError> {
        *self.get_job_calls.lock().unwrap() += 1;
        let scripted = self.get_job_script.lock().unwrap().pop_front();
        match scripted {
            Some(Err(status)) => {
                return Err(ClientError::Api {
                    status,
                    message: format!("scripted failure for {job_id}"),
                });
            }
            Some(Ok(state)) => self.set_job_state(job_id, &state),
            None => {}
        }
        self.jobs
            .lock()
            .expect("mock client state lock poisoned")
            .iter()
            .find(|j| j.id == job_id)
            .cloned()
            .ok_or_else(|| ClientError::Api {
                status: 404,
                message: format!("Job {job_id} not found"),
            })
    }

    async fn list_messages(
        &self,
        _project: &str,
        _region: &str,
        _job_id: &str,
        _start_time: Option<&str>,
    ) -> Result<ListJobMessagesResponse, ClientError> {
        let job_messages = self
            .messages
            .lock()
            .expect("mock client state lock poisoned")
            .clone();

        Ok(ListJobMessagesResponse {
            job_messages,
            next_page_token: None,
        })
    }

    async fn get_job_metrics(
        &self,
        _project: &str,
        _region: &str,
        _job_id: &str,
    ) -> Result<JobMetricsResponse, ClientError> {
        *self.get_job_metrics_calls.lock().unwrap() += 1;
        let scripted = self.metrics_script.lock().unwrap().pop_front();
        Ok(scripted.unwrap_or_else(|| self.metrics.lock().unwrap().clone().unwrap_or_default()))
    }

    async fn update_job(
        &self,
        _project: &str,
        _region: &str,
        job_id: &str,
        requested_state: JobState,
    ) -> Result<JobResponse, ClientError> {
        self.update_requests
            .lock()
            .unwrap()
            .push((job_id.to_string(), requested_state));
        // The service moves a job through the requested state; the mock completes the move
        // at once, so a drain lands in its terminal state.
        let reached = match requested_state {
            JobState::Draining => JobState::Drained,
            other => other,
        };
        self.set_job_state(job_id, reached.as_str());
        let mut guard = self.jobs.lock().expect("mock client state lock poisoned");
        guard
            .iter_mut()
            .find(|j| j.id == job_id)
            .cloned()
            .ok_or_else(|| ClientError::Api {
                status: 404,
                message: format!("Job {job_id} not found"),
            })
    }
}
