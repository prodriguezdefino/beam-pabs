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

//! Shared test helpers for the Kafka xlang wrappers: a strict, recording in-process
//! expansion service and schema/row renderers for fixture comparisons.

// Each test binary uses a different subset of these helpers.
#![allow(
    dead_code,
    reason = "each test binary uses a different subset of these helpers"
)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beam::coders::URN_ROW;
use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue, TypeInfo};
use managed_io::URN_MANAGED;
use model::expansion::expansion_service_server::{ExpansionService, ExpansionServiceServer};
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use model::pipeline as proto;
use prost::Message;
use tonic::{Request, Response, Status};

/// Environment id the mock attaches to every expanded transform.
pub const MOCK_ENV_ID: &str = "env_mock";
/// URN of the environment the mock returns in its components.
pub const MOCK_ENV_URN: &str = "beam:env:mock:v1";

/// Renders a field type as e.g. `ARRAY<STRING>?` (`?` = nullable) to compare with fixtures.
pub fn type_str(ft: &FieldType) -> String {
    let base = match &ft.type_info {
        TypeInfo::Atomic(a) => a.to_string(),
        TypeInfo::Array(e) => format!("ARRAY<{}>", type_str(e)),
        TypeInfo::Iterable(e) => format!("ITERABLE<{}>", type_str(e)),
        TypeInfo::Map(k, v) => format!("MAP<{}, {}>", type_str(k), type_str(v)),
        TypeInfo::Row(s) => format!(
            "ROW<{}>",
            s.fields
                .iter()
                .map(|f| format!("{}: {}", f.name, type_str(&f.field_type)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        TypeInfo::Logical { urn, .. } => format!("LOGICAL<{urn}>"),
    };
    if ft.nullable {
        format!("{base}?")
    } else {
        base
    }
}

/// `(name, type)` for every top-level field, in schema order.
pub fn describe(schema: &Schema) -> Vec<(String, String)> {
    schema
        .fields
        .iter()
        .map(|f| (f.name.clone(), type_str(&f.field_type)))
        .collect()
}

/// Converts a literal fixture into the shape [`describe`] returns.
pub fn fixture(fields: &[(&str, &str)]) -> Vec<(String, String)> {
    fields
        .iter()
        .map(|(n, t)| (n.to_string(), t.to_string()))
        .collect()
}

/// `(name, value)` for every top-level field, in schema order.
pub fn values(row: &Row) -> Vec<(String, Option<FieldValue>)> {
    row.schema()
        .fields
        .iter()
        .map(|f| (f.name.clone(), row.get_value(&f.name).cloned().flatten()))
        .collect()
}

pub fn s(v: &str) -> Option<FieldValue> {
    Some(FieldValue::String(v.to_string()))
}

pub fn str_array(items: &[&str]) -> Option<FieldValue> {
    Some(FieldValue::Array(items.iter().map(|i| s(i)).collect()))
}

/// Row `{output: <tag>}` as used by every `ErrorHandling` config field.
pub fn error_handling(tag: &str) -> Option<FieldValue> {
    let schema = Arc::new(Schema::new(vec![Field::new("output", FieldType::string())]));
    Some(FieldValue::Row(
        Row::new(schema, vec![s(tag)]).expect("error_handling row"),
    ))
}

/// Decodes a `SchemaTransformPayload` into `(identifier, configuration row)`.
pub fn decode_payload(payload: &[u8]) -> (String, Row) {
    let payload = proto::SchemaTransformPayload::decode(payload).expect("valid payload");
    let schema: Schema = payload
        .configuration_schema
        .expect("configuration schema")
        .try_into()
        .expect("valid schema");
    let row = Row::from_row_bytes(&Arc::new(schema), &payload.configuration_row)
        .expect("valid configuration row");
    (payload.identifier, row)
}

/// Whether a transform should produce an `output` or consume an `input`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Source,
    Sink,
}

/// One expansion request as received by the mock.
#[derive(Clone, Debug)]
pub struct SeenExpansion {
    pub namespace: String,
    pub unique_name: String,
    /// `FunctionSpec.urn` of the unexpanded transform.
    pub spec_urn: String,
    /// `SchemaTransformPayload.identifier`.
    pub identifier: String,
    /// The decoded configuration row of the payload.
    pub config: Row,
    /// `identifier`, or the Managed `transform_identifier` for a Managed payload.
    pub target: String,
    /// Inline Managed config string (only for Managed payloads).
    pub managed_config: Option<String>,
    pub inputs: HashMap<String, String>,
}

pub type SeenExpansions = Arc<Mutex<Vec<SeenExpansion>>>;

/// A strict mock: rejects unknown target URNs, sinks without exactly one `input` tag,
/// and inputs whose PCollection/coder were not shipped in the request components.
#[derive(Clone)]
struct StrictMockExpansionService {
    routes: HashMap<String, Role>,
    seen: SeenExpansions,
}

#[tonic::async_trait]
impl ExpansionService for StrictMockExpansionService {
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
        let (identifier, config) = decode_payload(&spec.payload);
        let (target, managed_config) = if identifier == URN_MANAGED {
            (
                config
                    .get_string("transform_identifier")
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .to_string(),
                config
                    .get_string("config")
                    .ok()
                    .flatten()
                    .map(str::to_string),
            )
        } else {
            (identifier.clone(), None)
        };
        let role = *self
            .routes
            .get(&target)
            .ok_or_else(|| Status::invalid_argument(format!("unexpected URN {target}")))?;

        self.seen.lock().expect("mutex").push(SeenExpansion {
            namespace: req.namespace.clone(),
            unique_name: transform.unique_name.clone(),
            spec_urn: spec.urn.clone(),
            identifier,
            config,
            target,
            managed_config,
            inputs: transform.inputs.clone(),
        });

        let mut outputs = HashMap::new();
        match role {
            Role::Source => {
                if !transform.inputs.is_empty() {
                    return Err(Status::invalid_argument("source takes no inputs"));
                }
                let coder_id = format!("{}/row_coder", req.namespace);
                let out_id = format!("{}/output", req.namespace);
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
                    out_id.clone(),
                    proto::PCollection {
                        unique_name: out_id.clone(),
                        coder_id,
                        is_bounded: proto::is_bounded::Enum::Unbounded as i32,
                        windowing_strategy_id: "ws_default".to_string(),
                        display_data: Vec::new(),
                    },
                );
                outputs.insert("output".to_string(), out_id);
            }
            Role::Sink => {
                if transform.inputs.len() != 1 {
                    return Err(Status::invalid_argument(format!(
                        "sink requires exactly one input, got {:?}",
                        transform.inputs
                    )));
                }
                let input = transform
                    .inputs
                    .get("input")
                    .ok_or_else(|| Status::invalid_argument("write requires 'input' tag"))?;
                let pcoll = components.pcollections.get(input).ok_or_else(|| {
                    Status::invalid_argument(format!("input PCollection {input} not shipped"))
                })?;
                if !components.coders.contains_key(&pcoll.coder_id) {
                    return Err(Status::invalid_argument(format!(
                        "input coder {} not shipped",
                        pcoll.coder_id
                    )));
                }
            }
        }
        components.environments.insert(
            MOCK_ENV_ID.to_string(),
            proto::Environment {
                urn: MOCK_ENV_URN.to_string(),
                ..Default::default()
            },
        );

        let expanded = proto::PTransform {
            unique_name: transform.unique_name,
            spec: transform.spec,
            subtransforms: vec![format!("{}/sub_exec", req.namespace)],
            inputs: transform.inputs,
            outputs,
            display_data: Vec::new(),
            environment_id: MOCK_ENV_ID.to_string(),
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

/// A running mock service; shuts down when dropped.
pub struct MockService {
    pub endpoint: String,
    pub seen: SeenExpansions,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockService {
    pub fn seen(&self) -> Vec<SeenExpansion> {
        self.seen.lock().expect("mutex").clone()
    }
}

impl Drop for MockService {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Starts a strict mock that accepts only the given `(target URN, role)` routes.
pub fn start_mock(routes: &[(&str, Role)]) -> MockService {
    let service = StrictMockExpansionService {
        routes: routes.iter().map(|(u, r)| (u.to_string(), *r)).collect(),
        seen: SeenExpansions::default(),
    };
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
    MockService {
        endpoint,
        seen,
        shutdown: Some(shutdown_tx),
    }
}

/// Returns the transform spliced under `name` after checking its environment, inputs,
/// outputs and subtransform.
pub fn assert_spliced(
    p: &Pipeline,
    seen: &SeenExpansion,
    inputs: &[(&str, &str)],
    outputs: &[(&str, &str)],
) -> proto::PTransform {
    let lock = p.lock();
    let t = lock
        .components
        .transforms
        .get(&seen.unique_name)
        .unwrap_or_else(|| panic!("{} not spliced", seen.unique_name))
        .clone();
    assert_eq!(t.environment_id, MOCK_ENV_ID);
    assert_eq!(
        lock.components.environments[MOCK_ENV_ID].urn, MOCK_ENV_URN,
        "mock environment must be merged into the pipeline"
    );
    let to_map = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    assert_eq!(t.inputs, to_map(inputs), "inputs of {}", seen.unique_name);
    assert_eq!(
        t.outputs,
        to_map(outputs),
        "outputs of {}",
        seen.unique_name
    );
    assert_eq!(t.subtransforms, [format!("{}/sub_exec", seen.namespace)]);
    t
}
