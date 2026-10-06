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

//! Polling of a submitted Dataflow job until it finishes.

use std::time::Duration;

use beam::metrics::MetricResults;
use beam::runners::PipelineResult;
use tracing::{info, warn};

use super::{DataflowRunnerError, SubmittedJob};
use crate::client::{
    DataflowApiClient, JobMessageItem, JobResponse, MessageCursor, is_successful_state,
    is_terminal_state,
};
use crate::constants::MAX_CONSECUTIVE_JOB_STATUS_ERRORS;

fn log_new_messages(messages: Vec<JobMessageItem>, cursor: MessageCursor) -> MessageCursor {
    let (unseen, next) = cursor.advance(messages);
    unseen
        .iter()
        .filter_map(|msg| msg.message_text.as_deref())
        .for_each(|text| info!("[Dataflow] {text}"));
    next
}

/// Polls a submitted job's status one interval apart and logs the new job messages. Shared
/// by [`wait_for_job`] and the streaming test runner.
pub struct JobPoller<'a> {
    client: &'a dyn DataflowApiClient,
    job: &'a SubmittedJob,
    interval: Duration,
    cursor: MessageCursor,
}

impl<'a> JobPoller<'a> {
    pub fn new(
        client: &'a dyn DataflowApiClient,
        job: &'a SubmittedJob,
        interval: Duration,
    ) -> Self {
        Self {
            client,
            job,
            interval,
            cursor: MessageCursor::default(),
        }
    }

    /// Waits one interval, then returns the job status. Retries a failed request and returns
    /// the error after [`MAX_CONSECUTIVE_JOB_STATUS_ERRORS`] consecutive failures.
    pub async fn next_status(&mut self) -> Result<JobResponse, DataflowRunnerError> {
        let job = self.job;
        let mut consecutive_errors = 0;
        loop {
            tokio::time::sleep(self.interval).await;
            match self
                .client
                .get_job(job.project(), job.region(), job.id())
                .await
            {
                Ok(status) => {
                    self.log_new_messages().await;
                    return Ok(status);
                }
                Err(e) => {
                    consecutive_errors += 1;
                    warn!("Failed to retrieve job status ({consecutive_errors}): {e}");
                    if consecutive_errors >= MAX_CONSECUTIVE_JOB_STATUS_ERRORS {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    async fn log_new_messages(&mut self) {
        let job = self.job;
        if let Ok(msgs) = self
            .client
            .list_messages(
                job.project(),
                job.region(),
                job.id(),
                self.cursor.start_time(),
            )
            .await
        {
            self.cursor = log_new_messages(msgs.job_messages, std::mem::take(&mut self.cursor));
        }
    }
}

/// Fetches metrics for `job`, logging failures without propagating.
pub async fn fetch_metrics(
    client: &dyn DataflowApiClient,
    job: &SubmittedJob,
) -> Option<MetricResults> {
    client
        .get_job_metrics(job.project(), job.region(), job.id())
        .await
        .inspect_err(|e| {
            warn!(
                "Failed to retrieve job metrics for Dataflow job '{}': {e}",
                job.id()
            );
        })
        .ok()
        .map(|resp| resp.to_metric_results())
}

/// Returns [`DataflowRunnerError::JobFailed`] if the terminal `state` is not successful.
/// Fetches metrics only for a successful job, because a failed job reports none.
async fn terminal_result(
    client: &dyn DataflowApiClient,
    job: &SubmittedJob,
    state: &str,
) -> Result<PipelineResult, DataflowRunnerError> {
    if !is_successful_state(state) {
        return Err(DataflowRunnerError::JobFailed {
            job_id: job.id().to_string(),
            state: state.to_string(),
        });
    }
    info!(
        "Dataflow job '{}' completed successfully with state '{state}'.",
        job.id()
    );
    let result = PipelineResult::new(job.id(), state);
    Ok(fetch_metrics(client, job)
        .await
        .into_iter()
        .fold(result, PipelineResult::with_metrics))
}

/// Polls `job` until it reaches a terminal state, logging messages.
pub(super) async fn wait_for_job(
    client: &dyn DataflowApiClient,
    job: &SubmittedJob,
    poll_interval: Duration,
) -> Result<PipelineResult, DataflowRunnerError> {
    let mut poller = JobPoller::new(client, job, poll_interval);
    loop {
        let status = poller.next_status().await?;
        if is_terminal_state(&status.current_state) {
            return terminal_result(client, job, &status.current_state).await;
        }
    }
}
