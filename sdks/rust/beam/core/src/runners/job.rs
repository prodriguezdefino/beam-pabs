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

//! A live handle to a job that a runner accepted.

use std::time::Duration;

use super::{JobState, PipelineResult, RunnerError};
use crate::metrics::MetricResults;

/// Controls and observes a submitted job while it runs.
///
/// [`PipelineRunner::submit`](super::PipelineRunner::submit) returns a handle when the runner
/// accepts the job. A runner that cannot detach returns a handle to the finished job.
/// Resources that the runner started for the job, such as a local Prism process, live until
/// the handle drops.
#[async_trait::async_trait]
pub trait JobHandle: Send + Sync {
    /// Identifier that the runner gave to the job.
    fn job_id(&self) -> &str;

    /// Asks the runner for the current state of the job.
    async fn state(&self) -> Result<JobState, RunnerError>;

    /// Reads the current metrics of the job. The values change while the job runs.
    async fn metrics(&self) -> Result<MetricResults, RunnerError>;

    /// Asks the runner to stop the job. The job can stay in `Cancelling` for some time.
    async fn cancel(&self) -> Result<(), RunnerError>;

    /// Asks the runner to stop reading input and to finish the data that the job holds.
    ///
    /// Returns [`RunnerError::Unsupported`] when the runner cannot drain a job.
    async fn drain(&self) -> Result<(), RunnerError>;

    /// Waits until the job ends, or until `timeout` elapses.
    ///
    /// A failed job gives an error with the cause. A cancelled, drained or updated job gives
    /// its state. At the timeout, the result holds the current state, which is not terminal,
    /// so the caller can cancel the job.
    async fn wait_until_finish(
        &self,
        timeout: Option<Duration>,
    ) -> Result<PipelineResult, RunnerError>;
}

/// The handle of a job that ended before its runner returned.
struct FinishedJob(PipelineResult);

#[async_trait::async_trait]
impl JobHandle for FinishedJob {
    fn job_id(&self) -> &str {
        &self.0.job_id
    }

    async fn state(&self) -> Result<JobState, RunnerError> {
        Ok(self.0.state)
    }

    async fn metrics(&self) -> Result<MetricResults, RunnerError> {
        // The trait gives owned metrics, and the handle keeps its copy for later calls.
        Ok(self.0.metrics.clone().unwrap_or_default())
    }

    /// The job has ended, so there is nothing to stop.
    async fn cancel(&self) -> Result<(), RunnerError> {
        Ok(())
    }

    /// The job has ended, so there is nothing to drain.
    async fn drain(&self) -> Result<(), RunnerError> {
        Ok(())
    }

    async fn wait_until_finish(
        &self,
        _timeout: Option<Duration>,
    ) -> Result<PipelineResult, RunnerError> {
        Ok(self.0.clone())
    }
}

/// Wraps the result of a job that has ended, for a runner that cannot detach from a job.
impl From<PipelineResult> for Box<dyn JobHandle> {
    fn from(result: PipelineResult) -> Self {
        Box::new(FinishedJob(result))
    }
}
