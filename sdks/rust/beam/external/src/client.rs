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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

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
    channel: Option<Channel>,
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
            channel: None,
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
        if let Some(channel) = &self.channel {
            return Ok(channel.clone());
        }
        if let Target::Auto {
            ref raw,
            ref server,
        } = self.target
        {
            let s = server
                .get_or_try_init(|| async {
                    JavaExpansionServer::start(raw)
                        .await
                        .map_err(|e| ExpansionError::Connection {
                            endpoint: raw.clone(),
                            source: Box::new(e),
                        })
                })
                .await?;
            self.endpoint = normalize_endpoint(s.endpoint().to_string());
        }

        let ep = Endpoint::from_shared(self.endpoint.clone())
            .map_err(|e| ExpansionError::Connection {
                endpoint: self.endpoint.clone(),
                source: Box::new(e),
            })?
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(60));

        let channel = ep.connect().await.map_err(|e| ExpansionError::Connection {
            endpoint: self.endpoint.clone(),
            source: Box::new(e),
        })?;

        Ok(self.channel.insert(channel).clone())
    }

    async fn call_with_retry<T, F, Fut>(&mut self, mut op: F) -> Result<T, ExpansionError>
    where
        F: FnMut(ExpansionServiceClient<Channel>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        let mut attempts = 0;
        loop {
            let channel = self.ensure_connected().await?;
            let grpc_client = ExpansionServiceClient::new(channel);

            match op(grpc_client).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(status)
                    if RPC_RETRY.allows_retry(attempts) && is_transient_rpc_error(&status) =>
                {
                    self.channel = None;
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

    /// Issues an `expand` RPC synchronously by driving the future on a Tokio runtime.
    pub fn expand_blocking(
        &self,
        request: ExpansionRequest,
    ) -> Result<ExpansionResponse, ExpansionError> {
        let mut client = self.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| handle.block_on(client.expand(request)))
                }
                _ => {
                    let (tx, rx) = std::sync::mpsc::channel();
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build();
                        match rt {
                            Ok(rt) => {
                                let res = rt.block_on(client.expand(request));
                                let _ = tx.send(res);
                            }
                            Err(e) => {
                                let _ = tx.send(Err(ExpansionError::Connection {
                                    endpoint: client.endpoint().to_string(),
                                    source: Box::new(e),
                                }));
                            }
                        }
                    });
                    rx.recv()
                        .map_err(|e| ExpansionError::Rpc(format!("Worker thread failed: {e}")))?
                }
            }
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| ExpansionError::Connection {
                    endpoint: self.endpoint.clone(),
                    source: Box::new(e),
                })?;
            rt.block_on(client.expand(request))
        }
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
