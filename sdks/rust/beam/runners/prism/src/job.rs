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

//! Live handle to a job that runs on Prism.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use beam::metrics::MetricResults;
use beam::runners::{JobHandle, JobState, PipelineResult, RunnerError};
use beam_model::job_management::job_service_client::JobServiceClient;
use beam_model::job_management::{
    CancelJobRequest, DrainJobRequest, GetJobMetricsRequest, GetJobStateRequest,
    JobMessagesRequest, job_state,
};
use tonic::transport::Channel;
use tracing::warn;

use crate::runner::{PrismRunnerError, follow_job};
use crate::server::PrismServer;
use crate::worker_pool::WorkerPool;

/// A job that Prism accepted, with the local services that it needs.
///
/// The LOOPBACK worker pool stops when the job ends or the handle drops. A Prism process that
/// the runner started stops when the last handle that uses it drops.
pub(crate) struct PrismJob {
    job_id: String,
    client: JobServiceClient<Channel>,
    /// The lock is held only to take the pool out, never across an `.await`.
    worker_pool: Mutex<Option<WorkerPool>>,
    _server: Option<Arc<PrismServer>>,
}

impl PrismJob {
    pub(crate) fn new(
        job_id: String,
        client: JobServiceClient<Channel>,
        worker_pool: Option<WorkerPool>,
        server: Option<Arc<PrismServer>>,
    ) -> Self {
        Self {
            job_id,
            client,
            worker_pool: Mutex::new(worker_pool),
            _server: server,
        }
    }

    /// A client for one call. Tonic calls take `&mut self`, and a clone shares the channel.
    fn client(&self) -> JobServiceClient<Channel> {
        self.client.clone()
    }

    /// Follows the job until it ends, then stops the workers and reads the final metrics.
    ///
    /// A cancelled job gives its state. A failed job gives [`PrismRunnerError::JobFailed`].
    pub(crate) async fn finish(&self) -> Result<PipelineResult, PrismRunnerError> {
        let outcome = follow_job(&self.job_id, || {
            let mut client = self.client();
            let request = JobMessagesRequest {
                job_id: self.job_id.clone(),
            };
            async move {
                client
                    .get_message_stream(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })
        .await;
        self.stop_workers().await;

        let result = match outcome {
            Err(PrismRunnerError::JobCancelled(job_id)) => {
                PipelineResult::new(job_id, JobState::Cancelled)
            }
            outcome => outcome?,
        };
        Ok(match self.read_metrics().await {
            Ok(metrics) => metrics
                .into_iter()
                .fold(result, PipelineResult::with_metrics),
            Err(e) => {
                warn!(
                    "Failed to read the metrics of Prism job '{}': {e}",
                    self.job_id
                );
                result
            }
        })
    }

    async fn stop_workers(&self) {
        let pool = self
            .worker_pool
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(mut pool) = pool {
            pool.stop().await;
        }
    }

    /// Reads the attempted and committed metrics. `None` when Prism reports no metrics.
    async fn read_metrics(&self) -> Result<Option<MetricResults>, PrismRunnerError> {
        let response = self
            .client()
            .get_job_metrics(GetJobMetricsRequest {
                job_id: self.job_id.clone(),
            })
            .await?
            .into_inner();
        Ok(response.metrics.map(|metrics| {
            MetricResults::from_monitoring_infos(&metrics.attempted, &metrics.committed)
        }))
    }
}

fn grpc_error(status: tonic::Status) -> RunnerError {
    RunnerError::execution(PrismRunnerError::Grpc(status))
}

#[async_trait::async_trait]
impl JobHandle for PrismJob {
    fn job_id(&self) -> &str {
        &self.job_id
    }

    async fn state(&self) -> Result<JobState, RunnerError> {
        let event = self
            .client()
            .get_state(GetJobStateRequest {
                job_id: self.job_id.clone(),
            })
            .await
            .map_err(grpc_error)?
            .into_inner();
        Ok(job_state::Enum::try_from(event.state).map_or(JobState::Unknown, JobState::from))
    }

    async fn metrics(&self) -> Result<MetricResults, RunnerError> {
        self.read_metrics()
            .await
            .map(Option::unwrap_or_default)
            .map_err(RunnerError::execution)
    }

    async fn cancel(&self) -> Result<(), RunnerError> {
        self.client()
            .cancel(CancelJobRequest {
                job_id: self.job_id.clone(),
            })
            .await
            .map(drop)
            .map_err(grpc_error)
    }

    /// Prism answers the drain request with `Unimplemented` when it cannot drain.
    async fn drain(&self) -> Result<(), RunnerError> {
        match self
            .client()
            .drain(DrainJobRequest {
                job_id: self.job_id.clone(),
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                Err(RunnerError::Unsupported {
                    runner: "prism",
                    operation: "drain",
                })
            }
            Err(status) => Err(grpc_error(status)),
        }
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
            None => Ok(PipelineResult::new(
                self.job_id.as_str(),
                self.state().await?,
            )),
        }
    }
}
