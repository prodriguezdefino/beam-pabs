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

//! The HTTP implementation of [`DataflowApiClient`], with retries of transient errors.

use std::fmt;
use std::sync::Arc;

use super::{
    ClientError, DataflowApiClient, JobMetricsResponse, JobResponse, JobState,
    ListJobMessagesResponse,
};
use crate::constants::{DEFAULT_DATAFLOW_ENDPOINT, FIRST_RETRY_DELAY, MAX_REQUEST_ATTEMPTS};
use crate::translate::DataflowJob;

/// How an [`HttpDataflowClient`] authenticates its requests.
#[derive(Clone)]
enum ClientAuth {
    /// The Application Default Credentials of the process ([`gcp::auth`]).
    ApplicationDefault,
    /// An OAuth 2.0 access token.
    Token(String),
    /// No credentials, for example for a test server.
    None,
}

/// HTTP client for Google Cloud Dataflow REST API v1b3.
#[derive(Clone)]
pub struct HttpDataflowClient {
    endpoint: String,
    auth: ClientAuth,
    /// Shared HTTP agent with connection pooling. Cloning it is cheap.
    agent: ureq::Agent,
}

impl fmt::Debug for HttpDataflowClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpDataflowClient")
            .field("endpoint", &self.endpoint)
            .field(
                "has_credentials",
                &matches!(self.auth, ClientAuth::ApplicationDefault),
            )
            .field(
                "has_custom_token",
                &matches!(self.auth, ClientAuth::Token(_)),
            )
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
            auth: ClientAuth::ApplicationDefault,
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
            auth: token.map_or(ClientAuth::None, |token| ClientAuth::Token(token.into())),
            agent: new_agent(),
        }
    }

    /// The `Authorization` header of the requests. Without credentials, the requests have
    /// none, so a call to the Dataflow API fails with the reason from the API.
    async fn resolve_auth_header(&self) -> Option<String> {
        match &self.auth {
            ClientAuth::Token(token) => Some(format!("Bearer {token}")),
            ClientAuth::ApplicationDefault => gcp::auth::authorization()
                .await
                .inspect_err(|e| tracing::warn!("Dataflow requests have no credentials: {e}"))
                .ok(),
            ClientAuth::None => None,
        }
    }
}

impl Default for HttpDataflowClient {
    fn default() -> Self {
        Self::new()
    }
}

/// An HTTP request to the Dataflow API. `Post` and `Put` carry a JSON body, shared between
/// the attempts of the request.
#[derive(Clone)]
enum Method {
    Get,
    Post(Arc<str>),
    Put(Arc<str>),
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
            .send(body.as_bytes())?,
        Method::Put(body) => with_auth(agent.put(url), auth)
            .header("Content-Type", "application/json")
            .send(body.as_bytes())?,
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
    url: Arc<str>,
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

    /// Sends a request, and sends it again after a transient error, up to
    /// [`MAX_REQUEST_ATTEMPTS`] times. Each attempt reads the credentials again.
    async fn send<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        url: String,
        method: Method,
    ) -> Result<T, ClientError> {
        let url: Arc<str> = url.into();
        let mut delay = FIRST_RETRY_DELAY;
        let mut attempt = 1;
        loop {
            let auth = self.resolve_auth_header().await;
            match dispatch_http(self.agent.clone(), Arc::clone(&url), method.clone(), auth).await {
                Err(error) if error.is_transient() && attempt < MAX_REQUEST_ATTEMPTS => {
                    tracing::warn!(
                        "Dataflow request failed (attempt {attempt} of {MAX_REQUEST_ATTEMPTS}), \
                         retrying in {delay:?}: {error}"
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    attempt += 1;
                }
                result => return result,
            }
        }
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
        self.send(self.jobs_url(project, region), Method::Post(payload.into()))
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
        self.send(format!("{jobs}/{job_id}"), Method::Put(payload.into()))
            .await
    }
}
