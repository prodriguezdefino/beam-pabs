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
use tokio::sync::Mutex;

use model::expansion::expansion_service_client::ExpansionServiceClient;
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use tonic::transport::{Channel, Endpoint};

use super::artifact::is_automated_expansion_service;
use super::error::ExpansionError;
use super::service::JavaExpansionServer;

/// Client for transform expansion requests to a remote Beam expansion service.
#[derive(Clone, Debug)]
pub struct ExpansionClient {
    raw_target: String,
    endpoint: String,
    channel: Option<Channel>,
    server: Option<Arc<Mutex<Option<JavaExpansionServer>>>>,
}

impl ExpansionClient {
    /// The target can be:
    /// - An explicit endpoint: e.g. `"localhost:8097"` or `"http://127.0.0.1:8097"`
    /// - An automated Java target: e.g. `"autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService"`
    /// - A shorthand alias: e.g. `"auto"`, `"gcp"`, `"bigquery"`, or `"kafka"`
    pub fn new(target: impl Into<String>) -> Self {
        let raw_target = target.into();
        let is_auto = is_automated_expansion_service(&raw_target);
        let endpoint = if is_auto {
            String::new()
        } else {
            normalize_endpoint(raw_target.clone())
        };

        Self {
            raw_target,
            endpoint,
            channel: None,
            server: if is_auto {
                Some(Arc::new(Mutex::new(None)))
            } else {
                None
            },
        }
    }

    pub async fn connect(target: impl Into<String>) -> Result<Self, ExpansionError> {
        let mut client = Self::new(target);
        client.ensure_connected().await?;
        Ok(client)
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// JAR of the auto-started expansion service. A runner ships it to workers so they run the
    /// transforms the driver expanded. `None` for an endpoint target, before the service
    /// starts, or while another task holds the service lock.
    pub fn local_jar_path(&self) -> Option<std::path::PathBuf> {
        let guard = self.server.as_ref()?.try_lock().ok()?;
        guard.as_ref().map(|server| server.jar_path().to_path_buf())
    }

    async fn ensure_connected(&mut self) -> Result<&mut Channel, ExpansionError> {
        if self.channel.is_none() {
            if let Some(ref server_lock) = self.server {
                let mut guard = server_lock.lock().await;

                if guard.is_none() {
                    let s = JavaExpansionServer::start(&self.raw_target)
                        .await
                        .map_err(|e| {
                            ExpansionError::Connection(self.raw_target.clone(), e.to_string())
                        })?;
                    self.endpoint = normalize_endpoint(s.endpoint().to_string());
                    *guard = Some(s);
                } else if self.endpoint.is_empty()
                    && let Some(ref s) = *guard
                {
                    self.endpoint = normalize_endpoint(s.endpoint().to_string());
                }
            }

            let ep = Endpoint::from_shared(self.endpoint.clone())
                .map_err(|e| ExpansionError::Connection(self.endpoint.clone(), e.to_string()))?
                .connect_timeout(Duration::from_secs(15))
                .timeout(Duration::from_secs(60));

            let channel = ep
                .connect()
                .await
                .map_err(|e| ExpansionError::Connection(self.endpoint.clone(), e.to_string()))?;

            self.channel = Some(channel);
        }

        Ok(self.channel.as_mut().expect("Channel just connected"))
    }

    /// Issues an `expand` RPC asynchronously.
    pub async fn expand(
        &mut self,
        request: ExpansionRequest,
    ) -> Result<ExpansionResponse, ExpansionError> {
        let mut attempts = 0;
        let response = loop {
            self.ensure_connected().await?;
            let channel = self.channel.clone().expect("Connected channel");
            let mut grpc_client = ExpansionServiceClient::new(channel);

            match grpc_client
                .expand(tonic::Request::new(request.clone()))
                .await
            {
                Ok(resp) => break resp.into_inner(),
                Err(_) if attempts < 2 => {
                    attempts += 1;
                    self.channel = None;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => return Err(ExpansionError::Rpc(status.message().to_string())),
            }
        };

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
                                let _ = tx.send(Err(ExpansionError::Connection(
                                    client.endpoint().to_string(),
                                    e.to_string(),
                                )));
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
                .map_err(|e| ExpansionError::Connection(self.endpoint.clone(), e.to_string()))?;
            rt.block_on(client.expand(request))
        }
    }

    /// Discovers all schema transform providers available on the expansion service.
    pub async fn discover_schema_transforms(
        &mut self,
    ) -> Result<DiscoverSchemaTransformResponse, ExpansionError> {
        let mut attempts = 0;
        let response = loop {
            self.ensure_connected().await?;
            let channel = self.channel.clone().expect("Connected channel");
            let mut grpc_client = ExpansionServiceClient::new(channel);

            match grpc_client
                .discover_schema_transform(tonic::Request::new(DiscoverSchemaTransformRequest {}))
                .await
            {
                Ok(resp) => break resp.into_inner(),
                Err(_) if attempts < 2 => {
                    attempts += 1;
                    self.channel = None;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => return Err(ExpansionError::Rpc(status.message().to_string())),
            }
        };

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
