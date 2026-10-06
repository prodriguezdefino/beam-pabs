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

use beam::metrics::{MetricKey, MetricPhase, MetricReading, MetricResults, MetricValue};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use google_cloud_auth::credentials::{Builder, CacheableResource, Credentials};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::translate::DataflowJob;

pub use crate::constants::{
    DEFAULT_DATAFLOW_ENDPOINT, JOB_STATE_CANCELLED, JOB_STATE_CANCELLING, JOB_STATE_DONE,
    JOB_STATE_DRAINED, JOB_STATE_DRAINING, JOB_STATE_FAILED, JOB_STATE_PENDING, JOB_STATE_QUEUED,
    JOB_STATE_RUNNING, JOB_STATE_STOPPED, JOB_STATE_UNKNOWN, JOB_STATE_UPDATED,
};

/// State of a Dataflow job in the v1b3 API (`JOB_STATE_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobState {
    Unknown,
    Stopped,
    Running,
    Done,
    Failed,
    Cancelled,
    Updated,
    Draining,
    Drained,
    Pending,
    Cancelling,
    Queued,
}

impl JobState {
    const ALL: [Self; 12] = [
        Self::Unknown,
        Self::Stopped,
        Self::Running,
        Self::Done,
        Self::Failed,
        Self::Cancelled,
        Self::Updated,
        Self::Draining,
        Self::Drained,
        Self::Pending,
        Self::Cancelling,
        Self::Queued,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => JOB_STATE_UNKNOWN,
            Self::Stopped => JOB_STATE_STOPPED,
            Self::Running => JOB_STATE_RUNNING,
            Self::Done => JOB_STATE_DONE,
            Self::Failed => JOB_STATE_FAILED,
            Self::Cancelled => JOB_STATE_CANCELLED,
            Self::Updated => JOB_STATE_UPDATED,
            Self::Draining => JOB_STATE_DRAINING,
            Self::Drained => JOB_STATE_DRAINED,
            Self::Pending => JOB_STATE_PENDING,
            Self::Cancelling => JOB_STATE_CANCELLING,
            Self::Queued => JOB_STATE_QUEUED,
        }
    }

    /// Returns `Unknown` for a name that this client does not know.
    pub fn parse(name: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|s| s.as_str() == name)
            .unwrap_or(Self::Unknown)
    }

    /// Returns true if a job in this state has stopped for good.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Done | Self::Failed | Self::Cancelled | Self::Updated | Self::Drained
        )
    }

    pub fn is_successful(self) -> bool {
        matches!(self, Self::Done | Self::Drained)
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn is_terminal_state(state: &str) -> bool {
    JobState::parse(state).is_terminal()
}

pub fn is_successful_state(state: &str) -> bool {
    JobState::parse(state).is_successful()
}

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

/// Response returned by `jobs.create` and `jobs.get`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobResponse {
    pub id: String,
    pub project_id: Option<String>,
    pub name: String,
    #[serde(rename = "type")]
    pub job_type: Option<String>,
    #[serde(default)]
    pub current_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_state_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// Response returned by `jobs.messages.list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListJobMessagesResponse {
    #[serde(default)]
    pub job_messages: Vec<JobMessageItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// Individual job status message from Dataflow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobMessageItem {
    pub id: Option<String>,
    pub time: Option<String>,
    pub message_text: Option<String>,
    pub message_importance: Option<String>,
}

/// Response returned by `jobs.metrics.get`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct JobMetricsResponse {
    #[serde(default)]
    pub metrics: Vec<MetricUpdateItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_time: Option<String>,
}

/// Individual metric update from Dataflow v1b3 `JobMetrics`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricUpdateItem {
    pub name: Option<MetricStructuredName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scalar: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distribution: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gauge: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_time: Option<String>,
}

/// Structured identity of a metric in Dataflow v1b3.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricStructuredName {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    pub name: String,
    #[serde(default)]
    pub context: std::collections::HashMap<String, String>,
}

/// Suffixes of the metrics that Dataflow derives from distributions for its UI. They are
/// skipped as duplicates. Dataflow marks them only by name, so a user metric with one of
/// these suffixes is also skipped.
const SYNTHETIC_SUFFIXES: [&str; 4] = ["_MIN", "_MAX", "_MEAN", "_COUNT"];

/// Parses an int64 that Dataflow encodes as a JSON number or a string. A fractional number
/// is rejected, not truncated: no metric read here is fractional.
fn parse_scalar_i64(val: &serde_json::Value) -> Option<i64> {
    val.as_i64()
        .or_else(|| val.as_u64().and_then(|u| i64::try_from(u).ok()))
        .or_else(|| {
            val.as_f64()
                .filter(|f| f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64)
                .map(|f| f as i64)
        })
        .or_else(|| val.as_str()?.parse().ok())
}

/// Distribution as Dataflow reports it. The wire field stays a `serde_json::Value`, so one
/// unexpected int64 encoding does not fail the whole metrics response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DistributionUpdate {
    count: i64,
    sum: i64,
    min: i64,
    max: i64,
}

impl DistributionUpdate {
    fn from_value(value: &serde_json::Value) -> Option<Self> {
        let obj = value.as_object()?;
        let field = |name: &str| obj.get(name).and_then(parse_scalar_i64).unwrap_or(0);
        Some(Self {
            count: field("count"),
            sum: field("sum"),
            min: field("min"),
            max: field("max"),
        })
    }
}

impl From<DistributionUpdate> for beam::metrics::DistributionValue {
    fn from(d: DistributionUpdate) -> Self {
        Self {
            count: d.count,
            sum: d.sum,
            min: d.min,
            max: d.max,
        }
    }
}

/// Returns the reporting step and the namespace of a Dataflow metric.
fn metric_key(name: &MetricStructuredName) -> MetricKey {
    let context = |key: &str| name.context.get(key);
    let step = context("step")
        .or_else(|| context("output_user_name"))
        .or_else(|| context("original_name"))
        .cloned()
        .unwrap_or_default();
    let namespace = context("namespace")
        .cloned()
        .unwrap_or_else(|| match name.origin.as_deref() {
            Some("user") => String::new(),
            origin => origin.unwrap_or("dataflow/v1b3").to_string(),
        });
    MetricKey::new(step, namespace, name.name.clone())
}

/// Dataflow marks a value that retries can still change as `tentative`: Beam attempted.
fn metric_phase(name: &MetricStructuredName) -> MetricPhase {
    if name.context.get("tentative").is_some_and(|s| s == "true") {
        MetricPhase::Attempted
    } else {
        MetricPhase::Committed
    }
}

fn metric_value(item: &MetricUpdateItem) -> Option<MetricValue> {
    match (&item.scalar, &item.distribution) {
        (Some(scalar), _) => parse_scalar_i64(scalar).map(MetricValue::Counter),
        (None, Some(dist)) => {
            DistributionUpdate::from_value(dist).map(|d| MetricValue::Distribution(d.into()))
        }
        (None, None) => None,
    }
}

/// Converts a Dataflow metric update into a Beam metric reading, if it carries one.
fn parse_metric(item: &MetricUpdateItem) -> Option<MetricReading> {
    let name = item.name.as_ref()?;
    if SYNTHETIC_SUFFIXES.iter().any(|s| name.name.ends_with(s)) {
        return None;
    }
    Some(MetricReading {
        key: metric_key(name),
        phase: metric_phase(name),
        value: metric_value(item)?,
    })
}

impl JobMetricsResponse {
    /// Converts Dataflow v1b3 job metrics into queryable Beam `MetricResults`.
    pub fn to_metric_results(&self) -> MetricResults {
        self.metrics.iter().filter_map(parse_metric).collect()
    }
}

/// Position in a job message stream, used to fetch each message exactly once.
///
/// The `startTime` filter of `messages.list` is inclusive, so each poll returns the messages
/// at the last seen timestamp again. The cursor filters out the IDs already returned at that
/// timestamp. A message without an ID is always treated as new.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageCursor {
    time: Option<String>,
    ids_at_time: BTreeSet<String>,
}

impl MessageCursor {
    pub fn start_time(&self) -> Option<&str> {
        self.time.as_deref()
    }

    /// Splits a listed page into the messages not returned before and the cursor for the
    /// next poll. Expects messages in ascending time order.
    pub fn advance(self, messages: Vec<JobMessageItem>) -> (Vec<JobMessageItem>, Self) {
        let unseen: Vec<JobMessageItem> = messages
            .into_iter()
            .filter(|msg| !self.has_seen(msg))
            .collect();
        let next = unseen.iter().fold(self, Self::record);
        (unseen, next)
    }

    fn has_seen(&self, msg: &JobMessageItem) -> bool {
        msg.time.is_some()
            && msg.time == self.time
            && msg
                .id
                .as_ref()
                .is_some_and(|id| self.ids_at_time.contains(id))
    }

    fn record(self, msg: &JobMessageItem) -> Self {
        match &msg.time {
            None => self,
            Some(time) if self.time.as_ref() == Some(time) => Self {
                ids_at_time: self.ids_at_time.into_iter().chain(msg.id.clone()).collect(),
                ..self
            },
            Some(time) => Self {
                time: Some(time.clone()),
                ids_at_time: msg.id.iter().cloned().collect(),
            },
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

/// HTTP client for Google Cloud Dataflow REST API v1b3.
#[derive(Clone)]
pub struct HttpDataflowClient {
    endpoint: String,
    credentials: Option<Arc<Credentials>>,
    custom_token: Option<String>,
    /// Shared HTTP agent with connection pooling. Cloning it is cheap.
    agent: ureq::Agent,
}

impl fmt::Debug for HttpDataflowClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpDataflowClient")
            .field("endpoint", &self.endpoint)
            .field("has_credentials", &self.credentials.is_some())
            .field("has_custom_token", &self.custom_token.is_some())
            .finish_non_exhaustive()
    }
}

/// The agent returns non-success statuses as responses, not as errors, so
/// [`ClientError::Api`] can show the body that the Dataflow API sends with them.
fn new_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into()
}

impl HttpDataflowClient {
    /// Creates a client using default configuration and environment credentials.
    pub fn new() -> Self {
        Self {
            endpoint: DEFAULT_DATAFLOW_ENDPOINT.to_string(),
            credentials: Builder::default().build().ok().map(Arc::new),
            custom_token: None,
            agent: new_agent(),
        }
    }

    /// Creates a client with an API endpoint and optional OAuth bearer token.
    pub fn with_endpoint_and_token(
        endpoint: impl Into<String>,
        token: Option<impl Into<String>>,
    ) -> Self {
        Self {
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
            credentials: None,
            custom_token: token.map(Into::into),
            agent: new_agent(),
        }
    }

    async fn resolve_auth_header(&self) -> Option<String> {
        if let Some(ref token) = self.custom_token {
            return Some(format!("Bearer {token}"));
        }

        let creds = self.credentials.as_ref()?;
        match creds.headers(Default::default()).await.ok()? {
            CacheableResource::New { data, .. } => data
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(ToString::to_string),
            CacheableResource::NotModified => None,
        }
    }
}

impl Default for HttpDataflowClient {
    fn default() -> Self {
        Self::new()
    }
}

/// An HTTP request to the Dataflow API. `Post` and `Put` carry a JSON body.
enum Method {
    Get,
    Post(String),
    Put(String),
}

/// Adds the `Authorization` header when a token is available.
fn with_auth<B>(request: ureq::RequestBuilder<B>, auth: Option<&str>) -> ureq::RequestBuilder<B> {
    match auth {
        Some(auth) => request.header("Authorization", auth),
        None => request,
    }
}

/// Sends a request and decodes a successful JSON response.
///
/// Blocks the calling thread; [`dispatch_http`] runs it on the blocking pool.
fn execute_request<T: serde::de::DeserializeOwned>(
    agent: &ureq::Agent,
    url: &str,
    method: Method,
    auth: Option<&str>,
) -> Result<T, ClientError> {
    let mut response = match method {
        Method::Get => with_auth(agent.get(url), auth).call()?,
        Method::Post(body) => with_auth(agent.post(url), auth)
            .header("Content-Type", "application/json")
            .send(body)?,
        Method::Put(body) => with_auth(agent.put(url), auth)
            .header("Content-Type", "application/json")
            .send(body)?,
    };

    let status = response.status();
    let body = response.body_mut().read_to_string()?;
    if !status.is_success() {
        return Err(ClientError::Api {
            status: status.as_u16(),
            message: body,
        });
    }
    Ok(serde_json::from_str(&body)?)
}

async fn dispatch_http<T: serde::de::DeserializeOwned + Send + 'static>(
    agent: ureq::Agent,
    url: String,
    method: Method,
    auth: Option<String>,
) -> Result<T, ClientError> {
    tokio::task::spawn_blocking(move || execute_request(&agent, &url, method, auth.as_deref()))
        .await?
}

impl HttpDataflowClient {
    /// Base URL of the `jobs` collection for a project and region.
    fn jobs_url(&self, project: &str, region: &str) -> String {
        let endpoint = &self.endpoint;
        format!("{endpoint}/v1b3/projects/{project}/locations/{region}/jobs")
    }

    async fn send<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        url: String,
        method: Method,
    ) -> Result<T, ClientError> {
        let auth = self.resolve_auth_header().await;
        dispatch_http(self.agent.clone(), url, method, auth).await
    }
}

#[async_trait::async_trait]
impl DataflowApiClient for HttpDataflowClient {
    async fn create_job(
        &self,
        project: &str,
        region: &str,
        job: &DataflowJob,
    ) -> Result<JobResponse, ClientError> {
        let payload = serde_json::to_string(job)?;
        self.send(self.jobs_url(project, region), Method::Post(payload))
            .await
    }

    async fn get_job(
        &self,
        project: &str,
        region: &str,
        job_id: &str,
    ) -> Result<JobResponse, ClientError> {
        let jobs = self.jobs_url(project, region);
        self.send(format!("{jobs}/{job_id}"), Method::Get).await
    }

    async fn list_messages(
        &self,
        project: &str,
        region: &str,
        job_id: &str,
        start_time: Option<&str>,
    ) -> Result<ListJobMessagesResponse, ClientError> {
        let jobs = self.jobs_url(project, region);
        let query = start_time
            .map(|t| format!("?startTime={t}"))
            .unwrap_or_default();
        self.send(format!("{jobs}/{job_id}/messages{query}"), Method::Get)
            .await
    }

    async fn get_job_metrics(
        &self,
        project: &str,
        region: &str,
        job_id: &str,
    ) -> Result<JobMetricsResponse, ClientError> {
        let jobs = self.jobs_url(project, region);
        self.send(format!("{jobs}/{job_id}/metrics"), Method::Get)
            .await
    }

    async fn update_job(
        &self,
        project: &str,
        region: &str,
        job_id: &str,
        requested_state: JobState,
    ) -> Result<JobResponse, ClientError> {
        let jobs = self.jobs_url(project, region);
        let payload = serde_json::json!({
            "requestedState": requested_state.as_str(),
        })
        .to_string();
        self.send(format!("{jobs}/{job_id}"), Method::Put(payload))
            .await
    }
}
