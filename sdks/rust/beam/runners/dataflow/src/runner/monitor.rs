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
    DataflowApiClient, JobMessageItem, JobResponse, JobState, MessageCursor, is_successful_state,
};
use crate::constants::{JOB_MESSAGE_ERROR, MAX_CONSECUTIVE_JOB_STATUS_ERRORS};

fn record_new_messages(
    messages: Vec<JobMessageItem>,
    cursor: MessageCursor,
    errors: &mut Vec<String>,
) -> MessageCursor {
    let (unseen, next) = cursor.advance(messages);
    for msg in &unseen {
        if let Some(text) = msg.message_text.as_deref() {
            info!("[Dataflow] {text}");
            if msg.message_importance.as_deref() == Some(JOB_MESSAGE_ERROR) && !text.is_empty() {
                errors.push(text.to_string());
            }
        }
    }
    next
}

/// Polls a submitted job's status one interval apart and logs the new job messages. Shared
/// by [`wait_for_job`] and the streaming test runner.
pub(crate) struct JobPoller<'a> {
    client: &'a dyn DataflowApiClient,
    job: &'a SubmittedJob,
    interval: Duration,
    cursor: MessageCursor,
    errors: Vec<String>,
}

impl<'a> JobPoller<'a> {
    pub(crate) fn new(
        client: &'a dyn DataflowApiClient,
        job: &'a SubmittedJob,
        interval: Duration,
    ) -> Self {
        Self {
            client,
            job,
            interval,
            cursor: MessageCursor::default(),
            errors: Vec::new(),
        }
    }

    /// Error messages (`JOB_MESSAGE_ERROR`) recorded while polling the job.
    pub(crate) fn errors(&self) -> &[String] {
        &self.errors
    }

    /// Waits one interval, then returns the job status. Retries a failed request and returns
    /// the error after [`MAX_CONSECUTIVE_JOB_STATUS_ERRORS`] consecutive failures.
    pub(crate) async fn next_status(&mut self) -> Result<JobResponse, DataflowRunnerError> {
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

    /// Logs the first page of the messages after the cursor. The next poll starts after the
    /// last message of the page, so a running job loses no message.
    async fn log_new_messages(&mut self) {
        let start_time = self.cursor.start_time().map(str::to_string);
        self.log_page(start_time.as_deref(), None).await;
    }

    /// Logs every page of the messages after the cursor. Call it once after the job reaches
    /// a terminal state, because no later poll reads the remaining pages.
    pub(crate) async fn drain_messages(&mut self) {
        let start_time = self.cursor.start_time().map(str::to_string);
        let mut page_token = None;
        for _ in 0..MAX_DRAINED_MESSAGE_PAGES {
            page_token = self
                .log_page(start_time.as_deref(), page_token.as_deref())
                .await;
            if page_token.is_none() {
                return;
            }
        }
        warn!(
            "Stopped reading the messages of Dataflow job '{}' after {MAX_DRAINED_MESSAGE_PAGES} pages",
            self.job.id()
        );
    }

    /// Logs one page of messages and returns the token of the next page, if there is one.
    async fn log_page(
        &mut self,
        start_time: Option<&str>,
        page_token: Option<&str>,
    ) -> Option<String> {
        let job = self.job;
        match self
            .client
            .list_messages(
                job.project(),
                job.region(),
                job.id(),
                start_time,
                page_token,
            )
            .await
        {
            Ok(page) => {
                self.cursor = record_new_messages(
                    page.job_messages,
                    std::mem::take(&mut self.cursor),
                    &mut self.errors,
                );
                page.next_page_token.filter(|token| !token.is_empty())
            }
            Err(e) => {
                warn!(
                    "Failed to retrieve job messages for Dataflow job '{}': {e}",
                    job.id()
                );
                None
            }
        }
    }
}

/// The most pages of messages that [`JobPoller::drain_messages`] reads.
const MAX_DRAINED_MESSAGE_PAGES: usize = 100;

/// Fetches metrics for `job`, logging failures without propagating.
pub(crate) async fn fetch_metrics(
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
    errors: &[String],
) -> Result<PipelineResult, DataflowRunnerError> {
    if !is_successful_state(state) {
        return Err(DataflowRunnerError::job_failed(job.id(), state, errors));
    }
    info!(
        "Dataflow job '{}' completed successfully with state '{state}'.",
        job.id()
    );
    let result = PipelineResult::new(job.id(), state.parse().unwrap_or_default());
    Ok(fetch_metrics(client, job)
        .await
        .into_iter()
        .fold(result, PipelineResult::with_metrics))
}

/// Polls `job` until it reaches a terminal state, logging messages, and returns that state
/// together with any [`JOB_MESSAGE_ERROR`] messages observed.
pub(super) async fn poll_until_terminal(
    client: &dyn DataflowApiClient,
    job: &SubmittedJob,
    poll_interval: Duration,
) -> Result<(JobState, Vec<String>), DataflowRunnerError> {
    let mut poller = JobPoller::new(client, job, poll_interval);
    loop {
        let state = JobState::parse(&poller.next_status().await?.current_state);
        if state.is_terminal() {
            poller.drain_messages().await;
            return Ok((state, poller.errors));
        }
    }
}

/// Polls `job` until it reaches a terminal state, logging messages.
pub(super) async fn wait_for_job(
    client: &dyn DataflowApiClient,
    job: &SubmittedJob,
    poll_interval: Duration,
) -> Result<PipelineResult, DataflowRunnerError> {
    let (state, errors) = poll_until_terminal(client, job, poll_interval).await?;
    terminal_result(client, job, state.as_str(), &errors).await
}
