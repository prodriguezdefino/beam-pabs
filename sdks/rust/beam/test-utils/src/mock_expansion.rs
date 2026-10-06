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

//! Mock expansion service for cross-language transforms in tests.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use beam::coders::URN_ROW;
use beam::schema::{Row, Schema};
use model::expansion::expansion_service_server::{ExpansionService, ExpansionServiceServer};
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use model::pipeline as proto;
use prost::Message;
use tonic::{Request, Response, Status};

const URN_MANAGED: &str = "beam:transform:managed:v1";

/// Extracts the underlying transform URN from a SchemaTransform payload.
fn target_urn(payload: &[u8]) -> Option<String> {
    let payload = proto::SchemaTransformPayload::decode(payload).ok()?;
    if payload.identifier != URN_MANAGED {
        return Some(payload.identifier);
    }
    let schema_proto = payload.configuration_schema?;
    let schema: Schema = schema_proto.try_into().ok()?;
    let row = Row::from_row_bytes(&Arc::new(schema), &payload.configuration_row).ok()?;
    row.get_string("transform_identifier")
        .ok()
        .flatten()
        .map(str::to_string)
}

/// Mock expansion service that simulates SchemaTransforms in tests.
#[derive(Clone, Default)]
pub struct MockExpansionService {
    sources: HashMap<String, bool>,
}

impl MockExpansionService {
    /// Creates a new empty mock expansion service.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a source transform URN and its boundedness.
    pub fn with_source(mut self, urn: impl Into<String>, is_bounded: bool) -> Self {
        self.sources.insert(urn.into(), is_bounded);
        self
    }

    /// Starts this mock expansion service on an ephemeral TCP port.
    pub fn start(self) -> MockExpansionServer {
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let thread_handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind ephemeral port");
                let addr = listener.local_addr().expect("local addr");
                addr_tx.send(addr).expect("send addr");

                let _ = tonic::transport::Server::builder()
                    .add_service(ExpansionServiceServer::new(self))
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        async {
                            let _ = shutdown_rx.await;
                        },
                    )
                    .await;
            });
        });

        let addr = addr_rx.recv().expect("recv server addr");
        MockExpansionServer {
            addr,
            endpoint: format!("http://{addr}"),
            shutdown_tx: Some(shutdown_tx),
            thread_handle: Some(thread_handle),
        }
    }
}

#[tonic::async_trait]
impl ExpansionService for MockExpansionService {
    async fn expand(
        &self,
        request: Request<ExpansionRequest>,
    ) -> Result<Response<ExpansionResponse>, Status> {
        let req = request.into_inner();
        let transform = req
            .transform
            .ok_or_else(|| Status::invalid_argument("transform required"))?;
        let mut components = req.components.unwrap_or_default();

        let spec = transform
            .spec
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("spec required"))?;

        let underlying = target_urn(&spec.payload).unwrap_or_default();
        let mut outputs = HashMap::new();

        if let Some(&is_bounded) = self.sources.get(&underlying) {
            let out_pcoll_id = format!("{}/read_output", req.namespace);
            let coder_id = format!("{}/row_coder", req.namespace);

            components.coders.insert(
                coder_id.clone(),
                proto::Coder {
                    spec: Some(proto::FunctionSpec {
                        urn: URN_ROW.to_string(),
                        payload: Vec::new(),
                    }),
                    component_coder_ids: Vec::new(),
                },
            );

            components.pcollections.insert(
                out_pcoll_id.clone(),
                proto::PCollection {
                    unique_name: out_pcoll_id.clone(),
                    coder_id,
                    is_bounded: if is_bounded {
                        proto::is_bounded::Enum::Bounded as i32
                    } else {
                        proto::is_bounded::Enum::Unbounded as i32
                    },
                    windowing_strategy_id: "ws_default".to_string(),
                    display_data: Vec::new(),
                },
            );
            outputs.insert("output".to_string(), out_pcoll_id);
        }

        let expanded = proto::PTransform {
            unique_name: transform.unique_name,
            spec: transform.spec,
            subtransforms: vec![format!("{}/exec", req.namespace)],
            inputs: transform.inputs,
            outputs,
            display_data: Vec::new(),
            environment_id: "env_mock".to_string(),
            annotations: HashMap::new(),
        };

        Ok(Response::new(ExpansionResponse {
            components: Some(components),
            transform: Some(expanded),
            requirements: Vec::new(),
            error: String::new(),
        }))
    }

    async fn discover_schema_transform(
        &self,
        _request: Request<DiscoverSchemaTransformRequest>,
    ) -> Result<Response<DiscoverSchemaTransformResponse>, Status> {
        Ok(Response::new(DiscoverSchemaTransformResponse {
            schema_transform_configs: HashMap::new(),
            error: String::new(),
        }))
    }
}

/// Handle to a running mock expansion server.
pub struct MockExpansionServer {
    addr: SocketAddr,
    endpoint: String,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    thread_handle: Option<std::thread::JoinHandle<()>>,
}

impl MockExpansionServer {
    /// Returns the socket address the server is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the HTTP endpoint string.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Shuts down the server.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.thread_handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for MockExpansionServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}
