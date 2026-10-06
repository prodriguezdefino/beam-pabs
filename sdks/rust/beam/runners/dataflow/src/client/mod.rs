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

//! REST API client for Google Cloud Dataflow v1b3.

mod http;
mod job;
mod metrics;

use std::fmt;

use thiserror::Error;

use crate::translate::DataflowJob;

pub use http::HttpDataflowClient;
pub use job::{
    JobMessageItem, JobResponse, JobState, ListJobMessagesResponse, MessageCursor,
    is_successful_state, is_terminal_state,
};
pub use metrics::{JobMetricsResponse, MetricStructuredName, MetricUpdateItem};

/// Errors produced by Dataflow API client operations.
#[derive(Error, Debug)]
pub enum ClientError {
    /// The request failed to send or the response could not be read.
    #[error("HTTP transport error: {0}")]
    Http(#[from] ureq::Error),

    /// The Dataflow API returned an error status code.
    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },

    #[error("JSON serialization or deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// The blocking HTTP task panicked or was cancelled.
    #[error("HTTP request task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}

impl ClientError {
    /// Whether the same request can succeed later: a transport failure, or a status that the
    /// API returns when it is busy (429, 500, 502, 503, 504).
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Http(error) => matches!(
                error,
                ureq::Error::Timeout(_)
                    | ureq::Error::Io(_)
                    | ureq::Error::ConnectionFailed
                    | ureq::Error::HostNotFound
                    | ureq::Error::BodyStalled
            ),
            Self::Api { status, .. } => matches!(status, 429 | 500 | 502 | 503 | 504),
            Self::Json(_) | Self::Task(_) => false,
        }
    }
}

/// Client interface for Dataflow v1b3 operations.
#[async_trait::async_trait]
pub trait DataflowApiClient: Send + Sync + fmt::Debug {
    /// Submits a new job to Dataflow.
    async fn create_job(
        &self,
        project: &str,
        region: &str,
        job: &DataflowJob,
    ) -> Result<JobResponse, ClientError>;

    /// Fetches the latest status of a Dataflow job.
    async fn get_job(
        &self,
        project: &str,
        region: &str,
        job_id: &str,
    ) -> Result<JobResponse, ClientError>;

    /// Lists execution log messages for a Dataflow job.
    async fn list_messages(
        &self,
        project: &str,
        region: &str,
        job_id: &str,
        start_time: Option<&str>,
    ) -> Result<ListJobMessagesResponse, ClientError>;

    /// Fetches the latest execution metrics for a Dataflow job.
    async fn get_job_metrics(
        &self,
        project: &str,
        region: &str,
        job_id: &str,
    ) -> Result<JobMetricsResponse, ClientError>;

    /// Updates the state of an existing Dataflow job (e.g. to cancel or drain).
    async fn update_job(
        &self,
        project: &str,
        region: &str,
        job_id: &str,
        requested_state: JobState,
    ) -> Result<JobResponse, ClientError>;
}
