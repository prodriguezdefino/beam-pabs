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

//! The mock expansion service of the cross-language tests, and renderers that compare
//! schemas and rows with literal fixtures.
//!
//! One [`MockExpansionService`] covers every test. Its options select the behavior that a test
//! checks: which targets it expands ([`Route`]), whether it rejects unexpected requests
//! ([`strict`](MockExpansionService::strict)), and how it names the outputs ([`OutputIds`]).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use beam::coders::URN_ROW;
use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::schema::{Field, FieldType, FieldValue, Row, Schema, TypeInfo};
use beam::values::IsBounded;
use model::expansion::expansion_service_server::{ExpansionService, ExpansionServiceServer};
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use model::pipeline as proto;
use prost::Message;
use tonic::{Request, Response, Status};

/// Environment ID attached to every expanded transform.
pub const MOCK_ENV_ID: &str = "env_mock";
/// URN of the environment returned in response components.
pub const MOCK_ENV_URN: &str = "beam:env:mock:v1";

const URN_MANAGED: &str = "beam:transform:managed:v1";

// ---------------------------------------------------------------------------
// Schema and row renderers
// ---------------------------------------------------------------------------

/// Renders a field type such as `ARRAY<STRING>?` (`?` = nullable), to compare schemas with
/// literal fixtures without depending on IDs.
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

/// `(name, type)` of each top-level field, in schema order.
pub fn describe(schema: &Schema) -> Vec<(String, String)> {
    schema
        .fields
        .iter()
        .map(|f| (f.name.clone(), type_str(&f.field_type)))
        .collect()
}

/// Converts a literal fixture into the shape returned by [`describe`].
pub fn fixture(fields: &[(&str, &str)]) -> Vec<(String, String)> {
    fields
        .iter()
        .map(|(n, t)| (n.to_string(), t.to_string()))
        .collect()
}

/// `(name, value)` of each top-level field, in schema order.
pub fn values(row: &Row) -> Vec<(String, Option<FieldValue>)> {
    row.schema()
        .fields
        .iter()
        .map(|f| (f.name.clone(), row.get_value(&f.name).cloned().flatten()))
        .collect()
}

/// A non-null STRING field value.
pub fn string_value(v: &str) -> Option<FieldValue> {
    Some(FieldValue::String(v.to_string()))
}

/// A non-null ARRAY<STRING> field value.
pub fn string_array(items: &[&str]) -> Option<FieldValue> {
    Some(FieldValue::Array(
        items.iter().map(|i| string_value(i)).collect(),
    ))
}

/// `{output: <tag>}` row of an `ErrorHandling` config field.
pub fn error_handling(tag: &str) -> Option<FieldValue> {
    let schema = Arc::new(Schema::new(vec![Field::new("output", FieldType::string())]));
    Some(FieldValue::Row(
        Row::new(schema, vec![string_value(tag)]).expect("error_handling row"),
    ))
}

/// Decodes a `SchemaTransformPayload` into its identifier and configuration row.
pub fn decode_payload(payload: &[u8]) -> (String, Row) {
    try_decode_payload(payload).expect("valid SchemaTransformPayload")
}

fn try_decode_payload(payload: &[u8]) -> Option<(String, Row)> {
    let payload = proto::SchemaTransformPayload::decode(payload).ok()?;
    let schema: Schema = payload.configuration_schema?.try_into().ok()?;
    let row = Row::from_row_bytes(&Arc::new(schema), &payload.configuration_row).ok()?;
    Some((payload.identifier, row))
}

// ---------------------------------------------------------------------------
// Mock expansion service
// ---------------------------------------------------------------------------

/// What the mock does with a transform: the output tags it returns, and whether the
/// transform consumes the `input` tag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    outputs: Vec<String>,
    consumes_input: bool,
    bounded: IsBounded,
}

impl Route {
    /// A bounded source with one `output`.
    pub fn source() -> Self {
        Self {
            outputs: vec!["output".to_string()],
            consumes_input: false,
            bounded: IsBounded::Bounded,
        }
    }

    /// A sink that consumes `input` and returns no outputs.
    pub fn sink() -> Self {
        Self {
            outputs: Vec::new(),
            consumes_input: true,
            bounded: IsBounded::Bounded,
        }
    }

    /// Makes the outputs unbounded.
    pub fn unbounded(mut self) -> Self {
        self.bounded = IsBounded::Unbounded;
        self
    }

    /// Replaces the output tags.
    pub fn with_outputs<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.outputs = tags.into_iter().map(Into::into).collect();
        self
    }
}

/// How the mock names the output PCollections and their coder.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OutputIds {
    /// `{namespace}/{tag}`, with the coder `{namespace}/row_coder`.
    #[default]
    Slash,
    /// `{namespace}_{tag}`, with the coder `{namespace}_coder`.
    Underscore,
}

impl OutputIds {
    fn output(self, namespace: &str, tag: &str) -> String {
        match self {
            Self::Slash => format!("{namespace}/{tag}"),
            Self::Underscore => format!("{namespace}_{tag}"),
        }
    }

    fn coder(self, namespace: &str) -> String {
        match self {
            Self::Slash => format!("{namespace}/row_coder"),
            Self::Underscore => format!("{namespace}_coder"),
        }
    }
}

/// Expansion request recorded by the mock service.
#[derive(Clone, Debug)]
pub struct SeenExpansion {
    pub namespace: String,
    pub unique_name: String,
    /// `FunctionSpec.urn` of the unexpanded transform.
    pub spec_urn: String,
    /// `SchemaTransformPayload.identifier`.
    pub identifier: String,
    pub config: Row,
    /// `identifier`, or the Managed `transform_identifier` for a Managed payload.
    pub target: String,
    /// Inline Managed config string (only for Managed payloads).
    pub managed_config: Option<String>,
    pub inputs: HashMap<String, String>,
}

pub type SeenExpansions = Arc<Mutex<Vec<SeenExpansion>>>;

/// A mock expansion service. Each transform gets the [`Route`] of its target: the
/// `SchemaTransformPayload` identifier, or the `transform_identifier` of a Managed payload.
///
/// By default the mock is lenient: a target without a route gets no outputs, and requests are
/// not checked. Each expansion with a decodable payload is recorded.
#[derive(Clone, Default)]
pub struct MockExpansionService {
    routes: HashMap<String, Route>,
    any_target: Option<Route>,
    strict: bool,
    output_ids: OutputIds,
    error_output_from_managed_config: bool,
    seen: SeenExpansions,
}

impl MockExpansionService {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a source with one `output`.
    pub fn with_source(self, urn: impl Into<String>, bounded: IsBounded) -> Self {
        let route = match bounded {
            IsBounded::Bounded => Route::source(),
            IsBounded::Unbounded => Route::source().unbounded(),
        };
        self.with_route(urn, route)
    }

    pub fn with_route(mut self, urn: impl Into<String>, route: Route) -> Self {
        self.routes.insert(urn.into(), route);
        self
    }

    /// Uses `route` for every target that has no route of its own, also for a transform whose
    /// payload is not a `SchemaTransformPayload`.
    pub fn for_any_target(mut self, route: Route) -> Self {
        self.any_target = Some(route);
        self
    }

    /// Rejects a target without a route, a source with inputs, a sink without exactly one
    /// `input` tag, and an input whose PCollection or coder the request does not ship.
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    pub fn with_output_ids(mut self, output_ids: OutputIds) -> Self {
        self.output_ids = output_ids;
        self
    }

    /// Also returns the output that the `error_handling.output` field of the inline Managed
    /// config names, the way the Java Managed transforms do.
    pub fn with_error_output_from_managed_config(mut self) -> Self {
        self.error_output_from_managed_config = true;
        self
    }

    fn route(&self, target: &str) -> Result<Route, Status> {
        match (
            self.routes.get(target).or(self.any_target.as_ref()),
            self.strict,
        ) {
            (Some(route), _) => Ok(route.clone()),
            (None, true) => Err(Status::invalid_argument(format!("unexpected URN {target}"))),
            (None, false) => Ok(Route {
                outputs: Vec::new(),
                consumes_input: false,
                bounded: IsBounded::Bounded,
            }),
        }
    }

    /// Decodes the request. `None` for a payload that is not a `SchemaTransformPayload`, which
    /// strict mode rejects.
    fn decode(
        &self,
        namespace: &str,
        transform: &proto::PTransform,
        spec: &proto::FunctionSpec,
    ) -> Result<Option<SeenExpansion>, Status> {
        let Some((identifier, config)) = try_decode_payload(&spec.payload) else {
            return if self.strict {
                Err(Status::invalid_argument("invalid SchemaTransformPayload"))
            } else {
                Ok(None)
            };
        };
        let (target, managed_config) = if identifier == URN_MANAGED {
            let field = |name| config.get_string(name).ok().flatten().map(str::to_string);
            (
                field("transform_identifier").unwrap_or_default(),
                field("config"),
            )
        } else {
            (identifier.clone(), None)
        };
        Ok(Some(SeenExpansion {
            namespace: namespace.to_string(),
            unique_name: transform.unique_name.clone(),
            spec_urn: spec.urn.clone(),
            identifier,
            config,
            target,
            managed_config,
            inputs: transform.inputs.clone(),
        }))
    }

    /// In strict mode, checks the inputs of a request against its route.
    fn check_inputs(
        &self,
        route: &Route,
        inputs: &HashMap<String, String>,
        components: &proto::Components,
    ) -> Result<(), Status> {
        if !self.strict {
            return Ok(());
        }
        if !route.consumes_input {
            return if inputs.is_empty() {
                Ok(())
            } else {
                Err(Status::invalid_argument("source takes no inputs"))
            };
        }
        if inputs.len() != 1 {
            return Err(Status::invalid_argument(format!(
                "sink requires exactly one input, got {inputs:?}"
            )));
        }
        let input = inputs
            .get("input")
            .ok_or_else(|| Status::invalid_argument("write requires 'input' tag"))?;
        let pcoll = components.pcollections.get(input).ok_or_else(|| {
            Status::invalid_argument(format!("input PCollection {input} not shipped"))
        })?;
        if components.coders.contains_key(&pcoll.coder_id) {
            Ok(())
        } else {
            Err(Status::invalid_argument(format!(
                "input coder {} not shipped",
                pcoll.coder_id
            )))
        }
    }

    /// Starts this mock expansion service on an ephemeral TCP port.
    pub fn start(self) -> MockExpansionServer {
        let seen = Arc::clone(&self.seen);
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
            seen,
            shutdown_tx: Some(shutdown_tx),
            thread_handle: Some(thread_handle),
        }
    }
}

/// The output tag that the `error_handling.output` field of an inline Managed config names.
fn managed_error_output(config: &str) -> Option<String> {
    let config: serde_json::Value = serde_json::from_str(config).ok()?;
    config["error_handling"]["output"]
        .as_str()
        .map(str::to_string)
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
            .clone()
            .ok_or_else(|| Status::invalid_argument("spec required"))?;

        // Only an accepted request is recorded, so a test can count the expansions that worked.
        let request = self.decode(&req.namespace, &transform, &spec)?;
        let route = self.route(request.as_ref().map_or("", |r| r.target.as_str()))?;
        self.check_inputs(&route, &transform.inputs, &components)?;
        let error_output = request
            .as_ref()
            .and_then(|r| r.managed_config.as_deref())
            .filter(|_| self.error_output_from_managed_config)
            .and_then(managed_error_output);
        if let Some(request) = request {
            self.seen.lock().expect("mutex").push(request);
        }
        let namespace = &req.namespace;
        let coder_id = self.output_ids.coder(namespace);
        let outputs: HashMap<String, String> = route
            .outputs
            .iter()
            .cloned()
            .chain(error_output)
            .map(|tag| {
                let id = self.output_ids.output(namespace, &tag);
                (tag, id)
            })
            .collect();
        if !outputs.is_empty() {
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
        }
        let is_bounded = proto::is_bounded::Enum::from(route.bounded);
        components.pcollections.extend(outputs.values().map(|id| {
            (
                id.clone(),
                proto::PCollection {
                    unique_name: id.clone(),
                    coder_id: coder_id.clone(),
                    is_bounded: is_bounded as i32,
                    windowing_strategy_id: "ws_default".to_string(),
                    display_data: Vec::new(),
                },
            )
        }));
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
            subtransforms: vec![format!("{namespace}/sub_exec")],
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

/// Handle to a running mock expansion server. Dropping it stops the server.
pub struct MockExpansionServer {
    addr: SocketAddr,
    endpoint: String,
    seen: SeenExpansions,
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

    /// The expansion requests that the server saw, in order.
    pub fn seen(&self) -> Vec<SeenExpansion> {
        self.seen.lock().expect("mutex").clone()
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

/// Starts a strict mock that expands only the given `(target URN, route)` pairs.
pub fn start_mock(routes: &[(&str, Route)]) -> MockExpansionServer {
    routes
        .iter()
        .fold(
            MockExpansionService::new().strict(),
            |service, (urn, route)| service.with_route(*urn, route.clone()),
        )
        .start()
}

/// Asserts that the spliced transform has the mock environment, the given inputs and
/// outputs, and the mock subtransform. Returns the transform.
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
