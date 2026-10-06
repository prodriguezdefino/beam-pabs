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

//! Remote model inference.
//!
//! Provides [`RemoteModelHandler`], asynchronous micro-batch dispatching,
//! connection pooling, client-side semaphore rate limiting, exponential backoff retries,
//! and adapters for hosted LLM endpoints such as Google Cloud Vertex AI and Gemini.
//!
//! A batch fans out into one request per element. Each request takes its own concurrency
//! permit and retries independently, so a throttled request never resends the others.

mod auth;
mod gemini;
mod llm;

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::options::SecretError;
use beam::transforms::VecBatchConverter;
use futures::future::try_join_all;

use crate::handler::{BatchBounds, InferenceArgs, ModelHandler};

pub use auth::{RemoteAuth, ResolvedAuth, fetch_gcp_access_token};
pub use gemini::{GeminiAdapter, GeminiEndpoint, GenerationConfig, ThinkingConfig};
pub use llm::{LLMResponse, LLMResponseCoder, PromptRequest, PromptRequestCoder};

/// Errors encountered during remote model inference.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum RemoteInferenceError {
    /// Network connection or transport failure.
    #[error("Network I/O error: {0}")]
    Network(String),

    /// HTTP error response from the remote endpoint.
    #[error("HTTP error status {status}: {message}")]
    Http {
        /// HTTP status code.
        status: u16,
        /// Server response body or error message.
        message: String,
    },

    /// The configured credentials could not be resolved.
    #[error("Credentials error: {0}")]
    Credentials(#[from] SecretError),

    /// JSON serialization or deserialization failure.
    #[error("Serialization / Deserialization error: {0}")]
    Serialization(String),

    /// Client or server rate limit reached (HTTP 429).
    #[error("Rate limit exceeded (HTTP 429). Retry after {retry_after_ms:?} ms")]
    RateLimitExceeded {
        /// Optional retry after delay in milliseconds.
        retry_after_ms: Option<u64>,
    },

    /// Request exceeded the configured timeout duration.
    #[error("Request timed out: {0}")]
    Timeout(String),

    /// Custom adapter error.
    #[error("Adapter processing error: {0}")]
    Adapter(String),

    /// Other unclassified errors.
    #[error("Remote inference error: {0}")]
    Other(String),
}

impl RemoteInferenceError {
    /// Whether resending the same request may succeed: rate limits, timeouts, transport
    /// failures and server errors.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::RateLimitExceeded { .. } | Self::Timeout(_) | Self::Network(_) => true,
            Self::Http { status, .. } => *status == 429 || *status >= 500,
            _ => false,
        }
    }

    /// The delay the server asked for before the next attempt, if any.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::RateLimitExceeded {
                retry_after_ms: Some(ms),
            } => Some(Duration::from_millis(*ms)),
            _ => None,
        }
    }
}

/// Configuration for remote inference timeouts, retries, and rate limits.
///
/// The [`RemoteEndpointAdapter`] owns the endpoint. The default retry settings are 5
/// retries with exponential backoff that starts at 5 seconds.
#[derive(Clone, Debug)]
pub struct RemoteConfig {
    /// How requests authenticate.
    pub auth: RemoteAuth,
    /// HTTP request timeout.
    pub timeout: Duration,
    /// Maximum retry attempts per request for transient errors (429, 5xx, timeouts).
    pub max_retries: usize,
    /// Backoff before the first retry of a request; it doubles on each further retry.
    pub initial_retry_backoff: Duration,
    /// Maximum concurrent in-flight HTTP requests per worker process. A request holds a
    /// permit while it is in flight, not while it backs off.
    pub max_concurrent_requests: usize,
    /// Micro-batch sizing boundaries.
    pub batch_bounds: BatchBounds,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            auth: RemoteAuth::default(),
            timeout: Duration::from_secs(60),
            max_retries: 5,
            initial_retry_backoff: Duration::from_secs(5),
            max_concurrent_requests: 64,
            batch_bounds: BatchBounds {
                min_batch_size: 1,
                max_batch_size: 16,
                max_batch_duration: Some(Duration::from_millis(50)),
            },
        }
    }
}

impl RemoteConfig {
    /// Sets how requests authenticate.
    pub fn with_auth(mut self, auth: RemoteAuth) -> Self {
        self.auth = auth;
        self
    }

    /// Sets the request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the maximum retry attempts for transient failures.
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the initial retry backoff duration.
    pub fn with_initial_retry_backoff(mut self, backoff: Duration) -> Self {
        self.initial_retry_backoff = backoff;
        self
    }

    /// Sets the maximum concurrent in-flight requests per worker.
    pub fn with_max_concurrent_requests(mut self, max_concurrent: usize) -> Self {
        self.max_concurrent_requests = max_concurrent;
        self
    }

    /// Sets the micro-batch bounds.
    pub fn with_batch_bounds(mut self, bounds: BatchBounds) -> Self {
        self.batch_bounds = bounds;
        self
    }
}

/// The initialized remote client instance shared across bundle threads.
pub struct RemoteClient {
    /// Reusable HTTP/2 client connection pool.
    pub client: reqwest::Client,
    /// Client-side rate limiter / concurrency semaphore.
    pub semaphore: Arc<tokio::sync::Semaphore>,
    /// Active configuration.
    pub config: RemoteConfig,
    /// The resolved credentials of `config.auth`.
    pub auth: ResolvedAuth,
}

static REMOTE_TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn remote_runtime() -> &'static tokio::runtime::Runtime {
    REMOTE_TOKIO_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("beam-remote-inference")
            .build()
            .expect("Failed to initialize Remote Inference Tokio runtime")
    })
}

/// Synchronously blocks on an asynchronous future, handling nested Tokio runtimes safely.
pub fn block_on_remote<F, T>(fut: F) -> Result<T, RemoteInferenceError>
where
    F: Future<Output = Result<T, RemoteInferenceError>> + Send + 'static,
    T: Send + 'static,
{
    let rt = remote_runtime();
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| rt.block_on(fut))
            }
            _ => std::thread::scope(|s| {
                s.spawn(|| rt.block_on(fut)).join().map_err(|_| {
                    RemoteInferenceError::Network("Remote inference thread panicked".into())
                })?
            }),
        }
    } else {
        rt.block_on(fut)
    }
}

/// A future returned by a [`RemoteEndpointAdapter`], borrowing the adapter, client and
/// request.
pub type RemoteCall<'a, Out> =
    Pin<Box<dyn Future<Output = Result<Out, RemoteInferenceError>> + Send + 'a>>;

/// How one element is formatted, sent to a remote endpoint and parsed back.
///
/// [`RemoteModelHandler`] calls it once per element of a batch, concurrently, and retries
/// each call on its own.
pub trait RemoteEndpointAdapter<In, Out>: Send + Sync + 'static {
    /// Sends one request for `request` and parses its response.
    fn execute<'a>(&'a self, client: &'a RemoteClient, request: &'a In) -> RemoteCall<'a, Out>;
}

/// Sends `request`, holding a concurrency permit per attempt and backing off exponentially
/// (or for as long as the server asked) between retryable failures.
async fn send_with_retry<In, Out, A>(
    adapter: &A,
    remote: &RemoteClient,
    request: &In,
) -> Result<Out, RemoteInferenceError>
where
    A: RemoteEndpointAdapter<In, Out>,
{
    let max_retries = remote.config.max_retries;
    let mut backoff = remote.config.initial_retry_backoff;
    let mut attempt = 0;
    loop {
        let result = {
            let _permit = remote
                .semaphore
                .acquire()
                .await
                .map_err(|e| RemoteInferenceError::Other(e.to_string()))?;
            adapter.execute(remote, request).await
        };
        match result {
            Err(err) if err.is_retryable() && attempt < max_retries => {
                let delay = err
                    .retry_after()
                    .map_or(backoff, |asked| asked.max(backoff));
                attempt += 1;
                tracing::warn!(
                    "Remote inference request failed ({err}), retry {attempt}/{max_retries} in \
                     {delay:?}"
                );
                tokio::time::sleep(delay).await;
                backoff *= 2;
            }
            done => return done,
        }
    }
}

/// A [`ModelHandler`] implementation for remote / hosted HTTP endpoints.
pub struct RemoteModelHandler<In, Out, A> {
    config: RemoteConfig,
    adapter: Arc<A>,
    model_id: Option<String>,
    _phantom: PhantomData<fn(In) -> Out>,
}

impl<In, Out, A> Clone for RemoteModelHandler<In, Out, A> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            adapter: Arc::clone(&self.adapter),
            model_id: self.model_id.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<In, Out, A> RemoteModelHandler<In, Out, A>
where
    In: DefaultCoder + Clone,
    Out: DefaultCoder + Clone,
    A: RemoteEndpointAdapter<In, Out>,
{
    /// Creates a new `RemoteModelHandler` with the specified configuration and endpoint adapter.
    pub fn new(config: RemoteConfig, adapter: A) -> Self {
        Self {
            config,
            adapter: Arc::new(adapter),
            model_id: None,
            _phantom: PhantomData,
        }
    }

    /// Sets an explicit model identifier for metrics and tracing.
    pub fn with_model_id(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = Some(model_id.into());
        self
    }
}

impl<In, Out, A> ModelHandler<In, Out> for RemoteModelHandler<In, Out, A>
where
    In: DefaultCoder + Clone,
    Out: DefaultCoder + Clone,
    A: RemoteEndpointAdapter<In, Out>,
{
    type Model = Arc<RemoteClient>;
    type Batch = Vec<In>;
    type Converter = VecBatchConverter<In>;

    fn load_model(&self) -> beam::Result<Self::Model> {
        let client = reqwest::Client::builder()
            .timeout(self.config.timeout)
            .tcp_keepalive(Duration::from_secs(60))
            .pool_max_idle_per_host(self.config.max_concurrent_requests)
            .build()
            .map_err(|e| RemoteInferenceError::Network(e.to_string()))?;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.config.max_concurrent_requests,
        ));

        let auth = self.config.auth.clone();
        let auth = block_on_remote(async move { Ok(auth.resolve().await?) })?;

        Ok(Arc::new(RemoteClient {
            client,
            semaphore,
            config: self.config.clone(),
            auth,
        }))
    }

    fn run_inference(
        &self,
        batch: &Self::Batch,
        model: &Self::Model,
        _inference_args: Option<&InferenceArgs>,
    ) -> beam::Result<Vec<Out>> {
        let adapter = Arc::clone(&self.adapter);
        let remote = Arc::clone(model);
        let requests = batch.clone();

        block_on_remote(async move {
            try_join_all(
                requests
                    .iter()
                    .map(|request| send_with_retry(adapter.as_ref(), &remote, request)),
            )
            .await
        })
        .map_err(beam::Error::from)
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }

    fn get_batch_bounds(&self) -> BatchBounds {
        self.config.batch_bounds
    }

    fn model_id(&self) -> Option<String> {
        self.model_id.clone()
    }
}
