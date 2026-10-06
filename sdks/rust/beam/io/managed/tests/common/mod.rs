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

//! Shared helpers for the Managed tests: config decoding and a recording in-process
//! mock of the Managed expansion service.

// Each test binary uses a different subset of these helpers.
#![allow(
    dead_code,
    reason = "each test binary uses a different subset of these helpers"
)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beam::coders::URN_ROW;
use beam::prelude::*;
use managed_io::{URN_MANAGED, urns};
use model::expansion::expansion_service_server::{ExpansionService, ExpansionServiceServer};
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use model::pipeline as proto;
use prost::Message;
use serde_json::Value;
use tonic::{Request, Response, Status};

pub fn config_json(row: &Row) -> Value {
    let config = row
        .get_string("config")
        .expect("valid test payload")
        .expect("inline config present");
    serde_json::from_str(config).expect("valid test payload")
}

// Full in-process mock expansion service test.
/// (underlying transform URN, inline config) for each expansion request.
pub type SeenExpansions = Arc<Mutex<Vec<(String, Option<String>)>>>;

#[derive(Default, Clone)]
struct MockManagedExpansionService {
    seen: SeenExpansions,
}

#[tonic::async_trait]
impl ExpansionService for MockManagedExpansionService {
    async fn expand(
        &self,
        request: Request<ExpansionRequest>,
    ) -> Result<Response<ExpansionResponse>, Status> {
        let req = request.into_inner();
        let transform = req.transform.expect("transform required");
        let mut components = req.components.unwrap_or_default();

        let spec = transform.spec.as_ref().expect("spec required");
        let payload = proto::SchemaTransformPayload::decode(spec.payload.as_slice())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if payload.identifier != URN_MANAGED {
            return Err(Status::invalid_argument(format!(
                "unexpected identifier {}",
                payload.identifier
            )));
        }
        let schema: Schema = payload
            .configuration_schema
            .expect("configuration schema")
            .try_into()
            .map_err(|e| Status::invalid_argument(format!("{e:?}")))?;
        let config = Row::from_row_bytes(&Arc::new(schema), &payload.configuration_row)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let underlying = config
            .get_string("transform_identifier")
            .ok()
            .flatten()
            .unwrap_or_default()
            .to_string();
        let inline = config
            .get_string("config")
            .ok()
            .flatten()
            .map(str::to_string);
        self.seen
            .lock()
            .expect("mutex")
            .push((underlying.clone(), inline.clone()));

        let is_read = underlying.ends_with("_read:v1");
        if !is_read && !transform.inputs.contains_key("input") {
            return Err(Status::invalid_argument("write requires 'input' tag"));
        }
        // The tags Java produces: `output` for reads, `snapshots` for an Iceberg write,
        // plus the output named in `error_handling`.
        let error_tag = inline
            .as_deref()
            .and_then(|c| serde_json::from_str::<Value>(c).ok())
            .and_then(|c| c["error_handling"]["output"].as_str().map(str::to_string));
        let tags: Vec<String> = is_read
            .then(|| "output".to_string())
            .into_iter()
            .chain((underlying == urns::ICEBERG_WRITE).then(|| "snapshots".to_string()))
            .chain(error_tag)
            .collect();

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
        let outputs: HashMap<String, String> = tags
            .into_iter()
            .map(|tag| {
                let id = format!("{}/{tag}", req.namespace);
                (tag, id)
            })
            .collect();
        components.pcollections.extend(outputs.values().map(|id| {
            (
                id.clone(),
                proto::PCollection {
                    unique_name: id.clone(),
                    coder_id: coder_id.clone(),
                    is_bounded: proto::is_bounded::Enum::Bounded as i32,
                    windowing_strategy_id: "ws_default".to_string(),
                    display_data: Vec::new(),
                },
            )
        }));

        let expanded = proto::PTransform {
            unique_name: transform.unique_name,
            spec: transform.spec,
            subtransforms: vec![format!("{}/sub_exec", req.namespace)],
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

/// Starts the mock Managed expansion service, returning its endpoint, the requests it
/// saw, and a shutdown handle.
pub fn start_mock_service() -> (String, SeenExpansions, tokio::sync::oneshot::Sender<()>) {
    let service = MockManagedExpansionService::default();
    let seen = Arc::clone(&service.seen);
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock service");
            addr_tx
                .send(listener.local_addr().expect("local addr"))
                .expect("report address");
            tonic::transport::Server::builder()
                .add_service(ExpansionServiceServer::new(service))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .expect("mock service");
        });
    });

    let endpoint = format!("http://{}", addr_rx.recv().expect("mock service address"));
    (endpoint, seen, shutdown_tx)
}
