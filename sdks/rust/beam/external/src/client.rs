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

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::OnceCell;

use beam::internals::block_on_io;
use beam::retry::Backoff;
use beam_model::expansion::expansion_service_client::ExpansionServiceClient;
use beam_model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use tonic::transport::{Channel, Endpoint};

use super::artifact::is_automated_expansion_service;
use super::error::ExpansionError;
use super::retrieval::{
    fetch_environment_dependencies, fetched_artifacts_dir, redeem_deferred_dependencies,
};
use super::service::JavaExpansionServer;

/// Calls of an expansion RPC: 2 retries on a new connection, waits of 25 to 100 ms.
const RPC_RETRY: Backoff = Backoff {
    attempts: 3,
    initial: Duration::from_millis(50),
    max: Duration::from_millis(100),
};

/// Target expansion service kind.
#[derive(Clone, Debug)]
enum Target {
    Endpoint(String),
    Auto {
        raw: String,
        server: Arc<OnceCell<JavaExpansionServer>>,
    },
}

/// Client for transform expansion requests to a remote Beam expansion service.
#[derive(Clone, Debug)]
pub struct ExpansionClient {
    target: Target,
    endpoint: String,
    /// Cached `(endpoint, channel)` shared across clones so repeated calls reuse one connection.
    connection: Arc<Mutex<Option<(String, Channel)>>>,
    cache_dir: Option<PathBuf>,
}

impl ExpansionClient {
    /// Creates a client for an expansion service endpoint or an automated Java service.
    ///
    /// The target may be an endpoint such as `"localhost:8097"`, an automated Java
    /// target with prefix `"autojava::"`, a service URN, or `"auto"`.
    pub fn new(target: impl Into<String>) -> Self {
        let raw_target = target.into();
        let target = if is_automated_expansion_service(&raw_target) {
            Target::Auto {
                raw: raw_target,
                server: Arc::new(OnceCell::new()),
            }
        } else {
            Target::Endpoint(normalize_endpoint(raw_target))
        };

        let endpoint = match &target {
            Target::Endpoint(ep) => ep.clone(),
            Target::Auto { .. } => String::new(),
        };

        Self {
            target,
            endpoint,
            connection: Arc::new(Mutex::new(None)),
            cache_dir: None,
        }
    }

    /// Overrides the local directory where endpoint expansions cache fetched artifacts.
    #[must_use]
    pub fn with_cache_dir(mut self, cache_dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(cache_dir.into());
        self
    }

    /// Connects to the given expansion target and returns the ready client.
    pub async fn connect(target: impl Into<String>) -> Result<Self, ExpansionError> {
        let mut client = Self::new(target);
        client.ensure_connected().await?;
        Ok(client)
    }

    /// Returns the resolved expansion service endpoint string.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// JAR of the auto-started expansion service. A runner ships it to workers so they run the
    /// transforms the driver expanded. `None` for an endpoint target, before the service
    /// starts, or while another task holds the service lock.
    pub fn local_jar_path(&self) -> Option<PathBuf> {
        match &self.target {
            Target::Auto { server, .. } => {
                let s = server.get()?;
                Some(s.jar_path().to_path_buf())
            }
            Target::Endpoint(_) => None,
        }
    }

    async fn ensure_connected(&mut self) -> Result<Channel, ExpansionError> {
        let cached = self
            .connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some((endpoint, channel)) = cached {
            self.endpoint = endpoint;
            return Ok(channel);
        }

        let endpoint = match &self.target {
            Target::Endpoint(ep) => ep.clone(),
            Target::Auto { raw, server } => {
                let s = server
                    .get_or_try_init(|| async {
                        JavaExpansionServer::start(raw).await.map_err(|e| {
                            ExpansionError::Connection {
                                endpoint: raw.clone(),
                                source: Box::new(e),
                            }
                        })
                    })
                    .await?;
                normalize_endpoint(s.endpoint().to_string())
            }
        };
        self.endpoint.clone_from(&endpoint);

        let ep = Endpoint::from_shared(endpoint.clone())
            .map_err(|e| ExpansionError::Connection {
                endpoint: endpoint.clone(),
                source: Box::new(e),
            })?
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(60));

        let channel = ep.connect().await.map_err(|e| ExpansionError::Connection {
            endpoint: endpoint.clone(),
            source: Box::new(e),
        })?;

        let mut slot = self
            .connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *slot = Some((endpoint, channel.clone()));
        Ok(channel)
    }

    async fn call_with_retry<T, F, Fut>(&mut self, mut op: F) -> Result<T, ExpansionError>
    where
        F: FnMut(ExpansionServiceClient<Channel>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        let mut attempts = 0;
        loop {
            let channel = self.ensure_connected().await?;
            let grpc_client = ExpansionServiceClient::new(channel)
                .max_decoding_message_size(beam::internals::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(beam::internals::MAX_MESSAGE_BYTES);

            match op(grpc_client).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(status)
                    if RPC_RETRY.allows_retry(attempts) && is_transient_rpc_error(&status) =>
                {
                    {
                        let mut slot = self
                            .connection
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner);
                        *slot = None;
                    }
                    tokio::time::sleep(RPC_RETRY.delay(attempts)).await;
                    attempts += 1;
                }
                Err(status) => {
                    let msg = status.message();
                    let err_msg = if msg.is_empty() {
                        format!("status code {}", status.code())
                    } else {
                        format!("{}: {}", status.code(), msg)
                    };
                    return Err(ExpansionError::Rpc(err_msg));
                }
            }
        }
    }

    /// Issues an `expand` RPC asynchronously.
    ///
    /// For an endpoint target, also fetches the dependencies of each new environment to local
    /// files, so a runner can stage them. The paths of the service are on its own disk.
    pub async fn expand(
        &mut self,
        request: ExpansionRequest,
    ) -> Result<ExpansionResponse, ExpansionError> {
        let req_ref = &request;
        let mut response = self
            .call_with_retry(|mut client| {
                let req = req_ref.clone();
                async move { client.expand(tonic::Request::new(req)).await }
            })
            .await?;

        if !response.error.is_empty() {
            return Err(ExpansionError::ExpansionFailed(response.error));
        }

        match self.target {
            Target::Endpoint(_) => {
                let known: HashSet<String> = request
                    .components
                    .map(|components| components.environments.into_keys().collect())
                    .unwrap_or_default();
                let channel = self.ensure_connected().await?;
                if let Some(components) = response.components.as_mut() {
                    let default_cache_dir;
                    let cache_dir = match &self.cache_dir {
                        Some(dir) => dir.as_path(),
                        None => {
                            default_cache_dir = fetched_artifacts_dir();
                            default_cache_dir.as_path()
                        }
                    };
                    fetch_environment_dependencies(channel, components, &known, cache_dir).await?;
                }
                Ok(response)
            }
            // The driver started the service from a JAR on this machine.
            Target::Auto { ref server, .. } => {
                if let (Some(server), Some(components)) =
                    (server.get(), response.components.as_mut())
                {
                    redeem_deferred_dependencies(components, server.jar_path());
                }
                Ok(response)
            }
        }
    }

    /// Issues an `expand` RPC synchronously on the shared async I/O runtime.
    pub fn expand_blocking(
        &self,
        request: ExpansionRequest,
    ) -> Result<ExpansionResponse, ExpansionError> {
        let mut client = self.clone();
        block_on_io(async move { client.expand(request).await }).map_err(|e| {
            ExpansionError::Connection {
                endpoint: self.endpoint.clone(),
                source: Box::new(e),
            }
        })?
    }

    /// Discovers all schema transform providers available on the expansion service.
    pub async fn discover_schema_transforms(
        &mut self,
    ) -> Result<DiscoverSchemaTransformResponse, ExpansionError> {
        let response = self
            .call_with_retry(|mut client| async move {
                client
                    .discover_schema_transform(tonic::Request::new(
                        DiscoverSchemaTransformRequest {},
                    ))
                    .await
            })
            .await?;

        if !response.error.is_empty() {
            return Err(ExpansionError::ExpansionFailed(response.error));
        }

        Ok(response)
    }
}

fn normalize_endpoint(endpoint: String) -> String {
    if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
        format!("http://{endpoint}")
    } else {
        endpoint
    }
}

/// Check whether a gRPC status code is a transient transport error.
fn is_transient_rpc_error(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::Unknown
    )
}
