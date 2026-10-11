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

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

use model::expansion::expansion_service_client::ExpansionServiceClient;
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use tonic::transport::{Channel, Endpoint};

use super::artifact::is_automated_expansion_service;
use super::error::ExpansionError;
use super::service::JavaExpansionServer;

const MAX_RPC_RETRIES: usize = 2;
const RETRY_BACKOFF: Duration = Duration::from_millis(50);

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
        }
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
    pub fn local_jar_path(&self) -> Option<std::path::PathBuf> {
        match &self.target {
            Target::Auto { server, .. } => {
                let s = server.get()?;
                Some(s.jar_path().to_path_buf())
            }
            Target::Endpoint(_) => None,
        }
    }

    async fn ensure_connected(&mut self) -> Result<&mut Channel, ExpansionError> {
        if self.channel.is_none() {
            if let Target::Auto {
                ref raw,
                ref server,
            } = self.target
            {
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

            self.channel = Some(channel);
        }

        Ok(self.channel.as_mut().expect("Channel just connected"))
    }

    async fn call_with_retry<T, F, Fut>(&mut self, mut op: F) -> Result<T, ExpansionError>
    where
        F: FnMut(ExpansionServiceClient<Channel>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        let mut attempts = 0;
        loop {
            self.ensure_connected().await?;
            let channel = self.channel.clone().expect("Connected channel");
            let grpc_client = ExpansionServiceClient::new(channel);

            match op(grpc_client).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(status) if attempts < MAX_RPC_RETRIES && is_transient_rpc_error(&status) => {
                    attempts += 1;
                    self.channel = None;
                    tokio::time::sleep(RETRY_BACKOFF).await;
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
    pub async fn expand(
        &mut self,
        request: ExpansionRequest,
    ) -> Result<ExpansionResponse, ExpansionError> {
        let req_ref = &request;
        let response = self
            .call_with_retry(|mut client| {
                let req = req_ref.clone();
                async move { client.expand(tonic::Request::new(req)).await }
            })
            .await?;

        if !response.error.is_empty() {
            return Err(ExpansionError::ExpansionFailed(response.error));
        }

        Ok(response)
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
