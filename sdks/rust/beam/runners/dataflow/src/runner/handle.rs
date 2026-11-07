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

//! Live handle to a submitted Dataflow job.

use std::sync::Arc;
use std::time::Duration;

use beam::metrics::MetricResults;
use beam::runners::{JobHandle, PipelineResult, RunnerError};

use super::monitor::{fetch_metrics, poll_until_terminal};
use super::{DataflowRunnerError, SubmittedJob};
use crate::client::{DataflowApiClient, JobState};

/// A job that Dataflow accepted. Each call asks the Dataflow API, so the handle holds no
/// state of the job.
pub(super) struct DataflowJobHandle {
    client: Arc<dyn DataflowApiClient>,
    job: SubmittedJob,
    poll_interval: Duration,
}

impl DataflowJobHandle {
    pub(super) fn new(
        client: Arc<dyn DataflowApiClient>,
        job: SubmittedJob,
        poll_interval: Duration,
    ) -> Self {
        Self {
            client,
            job,
            poll_interval,
        }
    }

    async fn current_state(&self) -> Result<JobState, DataflowRunnerError> {
        let job = &self.job;
        let status = self
            .client
            .get_job(job.project(), job.region(), job.id())
            .await?;
        Ok(JobState::parse(&status.current_state))
    }

    async fn request_state(&self, requested: JobState) -> Result<(), RunnerError> {
        let job = &self.job;
        self.client
            .update_job(job.project(), job.region(), job.id(), requested)
            .await
            .map_err(|e| RunnerError::execution(DataflowRunnerError::from(e)))
    }

    /// Polls until the job ends. A failed job gives [`DataflowRunnerError::JobFailed`].
    async fn finish(&self) -> Result<PipelineResult, DataflowRunnerError> {
        let state = poll_until_terminal(&*self.client, &self.job, self.poll_interval).await?;
        if state == JobState::Failed {
            return Err(DataflowRunnerError::JobFailed {
                job_id: self.job.id().to_string(),
                state: state.as_str().to_string(),
            });
        }
        let result = PipelineResult::new(self.job.id(), state.into());
        Ok(fetch_metrics(&*self.client, &self.job)
            .await
            .into_iter()
            .fold(result, PipelineResult::with_metrics))
    }
}

#[async_trait::async_trait]
impl JobHandle for DataflowJobHandle {
    fn job_id(&self) -> &str {
        self.job.id()
    }

    async fn state(&self) -> Result<beam::runners::JobState, RunnerError> {
        self.current_state()
            .await
            .map(Into::into)
            .map_err(RunnerError::execution)
    }

    /// Reads the tentative and committed metrics that Dataflow reports for the job.
    async fn metrics(&self) -> Result<MetricResults, RunnerError> {
        let job = &self.job;
        self.client
            .get_job_metrics(job.project(), job.region(), job.id())
            .await
            .map(|response| response.to_metric_results())
            .map_err(|e| RunnerError::execution(DataflowRunnerError::from(e)))
    }

    async fn cancel(&self) -> Result<(), RunnerError> {
        self.request_state(JobState::Cancelled).await
    }

    /// Dataflow drains only streaming jobs. For a batch job, the API returns an error.
    async fn drain(&self) -> Result<(), RunnerError> {
        self.request_state(JobState::Drained).await
    }

    async fn wait_until_finish(
        &self,
        timeout: Option<Duration>,
    ) -> Result<PipelineResult, RunnerError> {
        let finished = match timeout {
            None => Some(self.finish().await),
            Some(timeout) => tokio::time::timeout(timeout, self.finish()).await.ok(),
        };
        match finished {
            Some(result) => result.map_err(RunnerError::execution),
            None => Ok(PipelineResult::new(self.job.id(), self.state().await?)),
        }
    }
}
