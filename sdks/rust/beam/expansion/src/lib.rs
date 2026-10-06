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

//! Expansion service gRPC server and transform discovery for Apache Beam Rust SDK.
//!
//! Exposes native Rust transforms over gRPC to cross-language pipelines in Python,
//! Java, and Go using the standard Beam Portability Expansion Service protocol.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use clap::Parser;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::info;

pub type PCollectionId = String;

use beam::internals::TransformFn;
use beam::options::{HarnessOptions, OptionsSnapshot, PipelineOptions, parse_args};
use beam::pipeline::{
    Pipeline, URN_COMBINE_PER_KEY, URN_PAR_DO, URN_RUST_DOFN, URN_SDF_PAIR_WITH_RESTRICTION,
    URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS, URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS,
    URN_TEST_STREAM,
};
use beam::schema::{FieldType, Row, Schema, TypeInfo};
use beam::values::PCollection;
use harness::replay::{ExpandedSpec, ReplayEntry, ReplayRegistration, URN_RUST_DOFN_EXPANDED};
use model::expansion::expansion_service_server::{
    ExpansionService as ExpansionServiceGrpc, ExpansionServiceServer as GrpcServer,
};
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse, SchemaTransformConfig,
};
use model::pipeline as proto;

pub use inventory;

/// Provider interface for cross-language transforms configured with Beam schemas.
pub trait SchemaTransformProvider: Send + Sync + 'static {
    /// Unique URN of this SchemaTransform, for example
    /// `beam:schematransform:org.apache.beam:...:v1`.
    fn identifier(&self) -> &'static str;

    /// Human-readable documentation for this transform.
    fn description(&self) -> &'static str;

    /// Configuration schema describing parameters accepted by this transform.
    fn config_schema(&self) -> Schema;

    /// Expected tags for input PCollections (e.g. `["input"]`, or empty for sources).
    fn input_tags(&self) -> Vec<String> {
        vec!["input".to_string()]
    }

    /// Expected tags for output PCollections (e.g. `["output"]`).
    fn output_tags(&self) -> Vec<String> {
        vec!["output".to_string()]
    }

    /// Expands the transform into the given pipeline using the validated configuration row.
    fn build_transform(
        &self,
        config: Row,
        inputs: HashMap<String, PCollectionId>,
        pipeline: &mut Pipeline,
    ) -> Result<HashMap<String, PCollectionId>, String>;
}

/// Registration entry for link-time transform discovery through `inventory`.
pub struct SchemaTransformRegistration {
    pub provider: fn() -> Box<dyn SchemaTransformProvider>,
}

inventory::collect!(SchemaTransformRegistration);

#[macro_export]
macro_rules! register_schema_transform {
    ($provider_type:ty) => {
        $crate::inventory::submit! {
            $crate::SchemaTransformRegistration {
                provider: || Box::new(<$provider_type>::default()),
            }
        }
    };
}

/// Returns the input under `tag` as a `PCollection<T>`.
///
/// The PCollection keeps the coder of the caller, so `T` must decode with that coder.
pub fn input<T: 'static>(
    pipeline: &Pipeline,
    inputs: &HashMap<String, PCollectionId>,
    tag: &str,
) -> Result<PCollection<T>, String> {
    let id = inputs
        .get(tag)
        .ok_or_else(|| format!("no input tagged '{tag}'"))?;
    let coder_id = pipeline
        .lock()
        .components
        .pcollections
        .get(id)
        .map(|pcoll| pcoll.coder_id.clone())
        .ok_or_else(|| format!("input '{tag}' names unknown PCollection '{id}'"))?;
    Ok(PCollection::new(id.clone(), coder_id, pipeline.clone()))
}

/// Apache Beam Expansion Service implementation for Rust.
#[derive(Clone, Default)]
pub struct ExpansionServiceServer {
    environment_urn: String,
    environment_payload: Vec<u8>,
}

impl ExpansionServiceServer {
    pub fn new() -> Self {
        Self {
            environment_urn: beam::pipeline::URN_ENV_DEFAULT.to_string(),
            environment_payload: Vec::new(),
        }
    }

    pub fn with_docker_environment(mut self, image: impl Into<String>) -> Self {
        use prost::Message;
        self.environment_urn = beam::pipeline::URN_ENV_DOCKER.to_string();
        let payload = proto::DockerPayload {
            container_image: image.into(),
        };
        self.environment_payload = payload.encode_to_vec();
        self
    }

    pub fn with_external_environment(mut self, endpoint: impl Into<String>) -> Self {
        use prost::Message;
        self.environment_urn = beam::pipeline::URN_ENV_EXTERNAL.to_string();
        let payload = proto::ExternalPayload {
            endpoint: Some(proto::ApiServiceDescriptor {
                url: endpoint.into(),
                authentication: None,
            }),
            params: HashMap::new(),
        };
        self.environment_payload = payload.encode_to_vec();
        self
    }

    pub fn into_grpc_service(self) -> GrpcServer<Self> {
        GrpcServer::new(self)
    }
}

/// The flags of worker mode. [`parse_args`] drops flags that the runner adds and the
/// binary does not know.
#[derive(Parser, Debug)]
#[command(name = "beam-expansion-service", ignore_errors = true)]
struct WorkerArgs {
    #[command(flatten)]
    harness: HarnessOptions,
}

#[derive(Parser, Debug)]
#[command(
    name = "beam-expansion-service",
    about = "Apache Beam Rust SDK Expansion Service"
)]
struct ServiceArgs {
    #[arg(short, long, default_value = "8097")]
    port: u16,

    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    #[arg(long)]
    docker_image: Option<String>,
}

/// Runs the expansion service, or the SDK worker when the boot program passes `--worker`.
///
/// A binary calls it from `main` and serves every provider that it links.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // The worker has no pipeline of its own: each handler comes from the replay entry of an
    // expanded spec. A driver of another SDK sends no Rust options, so `--options_file` can
    // be absent.
    if std::env::args().any(|arg| arg == "--worker" || arg.starts_with("--worker=")) {
        let WorkerArgs { harness } = parse_args::<WorkerArgs>();
        if harness.is_worker() {
            let snapshot = match &harness.options_file {
                Some(path) => OptionsSnapshot::load(path)?,
                None => OptionsSnapshot::default(),
            };
            let options = PipelineOptions::from_snapshot(snapshot, harness)?;
            beam::runners::run(&Pipeline::new(), &options).await?;
            return Ok(());
        }
    }

    tracing_subscriber::fmt::init();
    let args = ServiceArgs::parse();

    let addr = format!("{}:{}", args.host, args.port).parse()?;
    info!("Starting Apache Beam Rust Expansion Service on {}", addr);

    let mut service = ExpansionServiceServer::new();
    if let Some(img) = args.docker_image {
        service = service.with_docker_environment(img);
    }

    Server::builder()
        .add_service(service.into_grpc_service())
        .serve(addr)
        .await?;

    Ok(())
}

/// Generic `spec.urn` of a `SchemaTransformPayload` request. The payload names the provider.
const URN_SCHEMA_TRANSFORM_PAYLOAD: &str = "beam:expansion:payload:schematransform:v1";

fn find_provider(identifier: &str) -> Option<Box<dyn SchemaTransformProvider>> {
    inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .map(|reg| (reg.provider)())
        .find(|provider| provider.identifier() == identifier)
}

/// Finds the provider and its config row for a request.
///
/// Callers send a `SchemaTransformPayload` under the generic SchemaTransform URN, or an
/// `ExternalConfigurationPayload` under the provider identifier. Errors are response messages.
fn resolve_request(
    spec: &proto::FunctionSpec,
) -> Result<(Box<dyn SchemaTransformProvider>, Row), String> {
    use prost::Message;
    let (identifier, caller_schema, row_bytes) = if spec.urn == URN_SCHEMA_TRANSFORM_PAYLOAD {
        let payload = proto::SchemaTransformPayload::decode(spec.payload.as_slice())
            .map_err(|e| format!("Failed to decode SchemaTransformPayload: {e}"))?;
        (
            payload.identifier,
            payload.configuration_schema,
            payload.configuration_row,
        )
    } else {
        let payload = proto::ExternalConfigurationPayload::decode(spec.payload.as_slice())
            .map_err(|e| {
                format!(
                    "Failed to decode ExternalConfigurationPayload for '{}': {e}",
                    spec.urn
                )
            })?;
        (spec.urn.clone(), payload.schema, payload.payload)
    };
    let provider = find_provider(&identifier)
        .ok_or_else(|| format!("No SchemaTransformProvider found for URN '{identifier}'"))?;
    let config = decode_config(provider.config_schema(), caller_schema, &row_bytes)
        .map_err(|e| format!("Failed to decode config row for '{identifier}': {e}"))?;
    Ok((provider, config))
}

/// Decodes `row_bytes` with the caller schema, then maps its fields by name to `target`.
///
/// The caller can omit fields and use any field order. A field that is not in `target`, or
/// that has a different type, is an error. A request without a schema and without row bytes
/// gives a row of nulls.
fn decode_config(
    target: Schema,
    caller_schema: Option<proto::Schema>,
    row_bytes: &[u8],
) -> Result<Row, String> {
    let target = Arc::new(target);
    let Some(caller_schema) = caller_schema else {
        if !row_bytes.is_empty() {
            return Err("configuration row has no schema".to_string());
        }
        return Row::new(target.clone(), vec![None; target.num_fields()])
            .map_err(|e| e.to_string());
    };
    let caller_schema = Arc::new(Schema::try_from(caller_schema).map_err(|e| e.to_string())?);
    for field in &caller_schema.fields {
        let expected = target
            .field(&field.name)
            .ok_or_else(|| format!("unknown field '{}'", field.name))?;
        if !same_shape(&expected.field_type, &field.field_type) {
            return Err(format!(
                "field '{}' has type {}, expected {}",
                field.name, field.field_type, expected.field_type
            ));
        }
    }
    let caller_row = Row::from_row_bytes(&caller_schema, row_bytes).map_err(|e| e.to_string())?;
    let values = target
        .fields
        .iter()
        .map(|field| caller_row.get_value(&field.name).cloned().flatten())
        .collect();
    Row::new(target, values).map_err(|e| e.to_string())
}

/// Compares two field types without nullability, schema ids and logical type payloads.
///
/// Nested rows must have the same fields in the same order, so their values keep the layout.
fn same_shape(a: &FieldType, b: &FieldType) -> bool {
    match (&a.type_info, &b.type_info) {
        (TypeInfo::Atomic(x), TypeInfo::Atomic(y)) => x == y,
        (TypeInfo::Array(x), TypeInfo::Array(y))
        | (TypeInfo::Iterable(x), TypeInfo::Iterable(y)) => same_shape(x, y),
        (TypeInfo::Map(xk, xv), TypeInfo::Map(yk, yv)) => same_shape(xk, yk) && same_shape(xv, yv),
        (TypeInfo::Row(x), TypeInfo::Row(y)) => {
            x.fields.len() == y.fields.len()
                && x.fields.iter().zip(&y.fields).all(|(xf, yf)| {
                    xf.name == yf.name && same_shape(&xf.field_type, &yf.field_type)
                })
        }
        (TypeInfo::Logical { urn: x, .. }, TypeInfo::Logical { urn: y, .. }) => x == y,
        _ => false,
    }
}

/// Copies the input PCollections of a request, with their coders and windowing strategies.
///
/// The provider and the worker replay start from these components only, so both register the
/// same handler keys. Ids that `caller` does not hold are skipped.
fn input_components(
    caller: Option<&proto::Components>,
    inputs: &HashMap<String, PCollectionId>,
) -> proto::Components {
    let mut seed = proto::Components::default();
    let Some(caller) = caller else {
        return seed;
    };
    let mut coder_ids = Vec::new();
    for id in inputs.values() {
        let Some(pcoll) = caller.pcollections.get(id) else {
            continue;
        };
        coder_ids.push(pcoll.coder_id.clone());
        if let Some(ws) = caller
            .windowing_strategies
            .get(&pcoll.windowing_strategy_id)
        {
            coder_ids.push(ws.window_coder_id.clone());
            seed.windowing_strategies
                .insert(pcoll.windowing_strategy_id.clone(), ws.clone());
        }
        seed.pcollections.insert(id.clone(), pcoll.clone());
    }
    while let Some(id) = coder_ids.pop() {
        if let Some(coder) = caller.coders.get(&id)
            && !seed.coders.contains_key(&id)
        {
            coder_ids.extend(coder.component_coder_ids.iter().cloned());
            seed.coders.insert(id, coder.clone());
        }
    }
    seed
}

/// Builds `provider` in a new pipeline that holds only the input components of `entry`.
///
/// The server and the worker replay both call it, so they register the same handler keys.
fn build_expansion(
    provider: &dyn SchemaTransformProvider,
    config: Row,
    entry: &ReplayEntry,
) -> Result<(Pipeline, HashMap<String, PCollectionId>), String> {
    let mut pipeline = Pipeline::new();
    if let Some(seed) = entry.components.clone() {
        let mut inner = pipeline.lock();
        inner.components.pcollections.extend(seed.pcollections);
        inner.components.coders.extend(seed.coders);
        inner
            .components
            .windowing_strategies
            .extend(seed.windowing_strategies);
    }
    let outputs = provider.build_transform(config, entry.inputs.clone(), &mut pipeline)?;
    Ok((pipeline, outputs))
}

/// Returns a new id for an expansion.
///
/// The caller sets the namespace, so it can be empty and two callers can send the same one.
/// The id holds the process id, the time of the first id in this process and a counter, so
/// it is unique across requests and across expansion service processes.
fn new_expansion_id() -> String {
    static PROCESS: LazyLock<String> = LazyLock::new(|| {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        format!("{}-{start}", std::process::id())
    });
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("{}-{}", *PROCESS, NEXT.fetch_add(1, Ordering::Relaxed))
}

/// Builds the handlers of an expansion again in a worker.
fn replay(entry: &ReplayEntry) -> Result<HashMap<String, TransformFn>, String> {
    let provider = find_provider(&entry.provider).ok_or_else(|| {
        format!(
            "No SchemaTransformProvider found for URN '{}'",
            entry.provider
        )
    })?;
    let config = decode_config(
        provider.config_schema(),
        entry.config_schema.clone(),
        &entry.config_row,
    )?;
    let (pipeline, _) = build_expansion(provider.as_ref(), config, entry)?;
    Ok(pipeline.transform_handlers())
}

inventory::submit! {
    ReplayRegistration { replay }
}

/// Rewrites the payload of `spec` for the caller pipeline.
///
/// Each bare handler key in a Rust `do_fn` becomes an [`ExpandedSpec`]. Each coder id in the
/// payload gets the id of the coder in the returned components, so runners find the
/// restriction, state and timer coders.
fn rewrite_payload(
    spec: &mut proto::FunctionSpec,
    replay: &ReplayEntry,
    coder_id: &dyn Fn(&str) -> String,
) {
    use prost::Message;
    let expand = |key_spec: &mut proto::FunctionSpec, urn: &str| {
        let expanded = ExpandedSpec {
            handler_key: String::from_utf8_lossy(&key_spec.payload).into_owned(),
            replay: Some(replay.clone()),
        };
        key_spec.urn = urn.to_string();
        key_spec.payload = expanded.encode_to_vec();
    };
    let rename = |id: &mut String| {
        if !id.is_empty() {
            *id = coder_id(id);
        }
    };
    match spec.urn.as_str() {
        URN_PAR_DO
        | URN_SDF_PAIR_WITH_RESTRICTION
        | URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS
        | URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS => {
            let Ok(mut payload) = proto::ParDoPayload::decode(spec.payload.as_slice()) else {
                return;
            };
            if let Some(do_fn) = payload.do_fn.as_mut().filter(|f| f.urn == URN_RUST_DOFN) {
                expand(do_fn, URN_RUST_DOFN_EXPANDED);
            }
            rename(&mut payload.restriction_coder_id);
            for state in payload.state_specs.values_mut() {
                use proto::state_spec::Spec;
                match state.spec.as_mut() {
                    Some(Spec::ReadModifyWriteSpec(s)) => rename(&mut s.coder_id),
                    Some(Spec::BagSpec(s)) => rename(&mut s.element_coder_id),
                    Some(Spec::CombiningSpec(s)) => rename(&mut s.accumulator_coder_id),
                    Some(Spec::MapSpec(s)) => {
                        rename(&mut s.key_coder_id);
                        rename(&mut s.value_coder_id);
                    }
                    Some(Spec::SetSpec(s)) => rename(&mut s.element_coder_id),
                    Some(Spec::OrderedListSpec(s)) => rename(&mut s.element_coder_id),
                    Some(Spec::MultimapSpec(s)) => {
                        rename(&mut s.key_coder_id);
                        rename(&mut s.value_coder_id);
                    }
                    None => {}
                }
            }
            for timer in payload.timer_family_specs.values_mut() {
                rename(&mut timer.timer_family_coder_id);
            }
            spec.payload = payload.encode_to_vec();
        }
        URN_TEST_STREAM => {
            let Ok(mut payload) = proto::TestStreamPayload::decode(spec.payload.as_slice()) else {
                return;
            };
            rename(&mut payload.coder_id);
            spec.payload = payload.encode_to_vec();
        }
        _ => {}
    }
}

#[async_trait]
impl ExpansionServiceGrpc for ExpansionServiceServer {
    async fn discover_schema_transform(
        &self,
        _request: Request<DiscoverSchemaTransformRequest>,
    ) -> Result<Response<DiscoverSchemaTransformResponse>, Status> {
        let configs = inventory::iter::<SchemaTransformRegistration>
            .into_iter()
            .map(|reg| {
                let provider = (reg.provider)();
                let id = provider.identifier().to_string();
                let config = SchemaTransformConfig {
                    config_schema: Some(provider.config_schema().into()),
                    input_pcollection_names: provider.input_tags(),
                    output_pcollection_names: provider.output_tags(),
                    description: provider.description().to_string(),
                };
                (id, config)
            })
            .collect();

        Ok(Response::new(DiscoverSchemaTransformResponse {
            schema_transform_configs: configs,
            error: String::new(),
        }))
    }

    async fn expand(
        &self,
        request: Request<ExpansionRequest>,
    ) -> Result<Response<ExpansionResponse>, Status> {
        let req = request.into_inner();
        let Some(transform_proto) = req.transform else {
            return Ok(Response::new(ExpansionResponse {
                components: None,
                transform: None,
                requirements: Vec::new(),
                error: "ExpansionRequest missing transform".to_string(),
            }));
        };

        let Some(spec) = &transform_proto.spec else {
            return Ok(Response::new(ExpansionResponse {
                components: None,
                transform: None,
                requirements: Vec::new(),
                error: "PTransform missing spec".to_string(),
            }));
        };

        let (provider, config_row) = match resolve_request(spec) {
            Ok(resolved) => resolved,
            Err(error) => {
                return Ok(Response::new(ExpansionResponse {
                    components: None,
                    transform: None,
                    requirements: Vec::new(),
                    error,
                }));
            }
        };
        let identifier = provider.identifier();
        let seed = input_components(req.components.as_ref(), &transform_proto.inputs);
        let replay = match config_row.to_row_bytes() {
            Ok(config_row_bytes) => ReplayEntry {
                provider: identifier.to_string(),
                config_schema: Some(provider.config_schema().into()),
                config_row: config_row_bytes,
                namespace: req.namespace.clone(),
                inputs: transform_proto.inputs.clone(),
                components: Some(seed.clone()),
                expansion_id: new_expansion_id(),
            },
            Err(e) => {
                return Ok(Response::new(ExpansionResponse {
                    components: None,
                    transform: None,
                    requirements: Vec::new(),
                    error: format!("Failed to encode config row for '{identifier}': {e}"),
                }));
            }
        };

        let (pipeline, output_ids) = match build_expansion(provider.as_ref(), config_row, &replay) {
            Ok(built) => built,
            Err(e) => {
                return Ok(Response::new(ExpansionResponse {
                    components: None,
                    transform: None,
                    requirements: Vec::new(),
                    error: format!("Failed to expand '{identifier}': {e}"),
                }));
            }
        };

        // The seed components belong to the caller. They keep their ids.
        let caller_pcoll_ids: HashSet<&str> = seed
            .pcollections
            .keys()
            .chain(transform_proto.inputs.values())
            .map(String::as_str)
            .collect();
        let caller_coder_ids: HashSet<&str> = seed.coders.keys().map(String::as_str).collect();
        let caller_ws_ids: HashSet<&str> = seed
            .windowing_strategies
            .keys()
            .map(String::as_str)
            .collect();

        // Sub-pipeline roots become the subtransforms of the expanded composite. The caller
        // adds the requirements to its pipeline, and a caller can reject a transform that
        // needs a feature, for example splittable DoFns, that the requirements do not list.
        let (mut components, root_ids, requirements) = {
            let inner = pipeline.lock();
            (
                inner.components.clone(),
                inner.compute_root_transform_ids(),
                inner.requirements(),
            )
        };

        let namespace = req.namespace;
        let prefix_id = |id: &str, is_caller: bool| -> String {
            if is_caller || id.is_empty() || namespace.is_empty() || id.starts_with(&namespace) {
                id.to_string()
            } else {
                format!("{namespace}{id}")
            }
        };

        let prefix_pcoll = |id: &str| prefix_id(id, caller_pcoll_ids.contains(id));
        let prefix_transform = |id: &str| prefix_id(id, false);
        let prefix_coder = |id: &str| prefix_id(id, caller_coder_ids.contains(id));
        let prefix_ws = |id: &str| prefix_id(id, caller_ws_ids.contains(id));
        let prefix_env = |id: &str| prefix_id(id, false);

        let env_id = prefix_env("rust_environment");
        let default_hints = components
            .environments
            .get("env_default")
            .map(|env| env.resource_hints.clone())
            .unwrap_or_default();
        let env_proto = proto::Environment {
            urn: self.environment_urn.clone(),
            payload: self.environment_payload.clone(),
            display_data: Vec::new(),
            capabilities: beam::pipeline::constants::standard_capabilities(),
            resource_hints: default_hints,
            dependencies: Vec::new(),
        };
        components.environments.remove("env_default");
        components.environments.insert(env_id.clone(), env_proto);

        components.environments = components
            .environments
            .into_iter()
            .map(|(k, v)| (prefix_env(&k), v))
            .collect();

        components.coders = components
            .coders
            .into_iter()
            .map(|(k, mut coder)| {
                coder.component_coder_ids = coder
                    .component_coder_ids
                    .into_iter()
                    .map(|id| prefix_coder(&id))
                    .collect();
                (prefix_coder(&k), coder)
            })
            .collect();

        components.windowing_strategies = components
            .windowing_strategies
            .into_iter()
            .map(|(k, mut ws)| {
                if caller_ws_ids.contains(k.as_str()) {
                    return (k, ws);
                }
                if !ws.window_coder_id.is_empty() {
                    ws.window_coder_id = prefix_coder(&ws.window_coder_id);
                }
                if !ws.environment_id.is_empty() {
                    ws.environment_id = prefix_env(&ws.environment_id);
                }
                (prefix_ws(&k), ws)
            })
            .collect();

        components.pcollections = components
            .pcollections
            .into_iter()
            .map(|(k, mut pcoll)| {
                if !pcoll.coder_id.is_empty() {
                    pcoll.coder_id = prefix_coder(&pcoll.coder_id);
                }
                if !pcoll.windowing_strategy_id.is_empty() {
                    pcoll.windowing_strategy_id = prefix_ws(&pcoll.windowing_strategy_id);
                }
                (prefix_pcoll(&k), pcoll)
            })
            .collect();

        components.transforms = components
            .transforms
            .into_iter()
            .map(|(k, mut transform)| {
                // The runner runs primitives and composites, so they keep no environment.
                if !transform.environment_id.is_empty() {
                    transform.environment_id = env_id.clone();
                }
                // A Rust combine composite holds its lifted stages as subtransforms. Runners
                // that lift a combine themselves expect `GroupByKey` and `CombineValues`
                // subtransforms (Dataflow rejects the job), so the composite carries no spec
                // and runners run the subtransforms.
                if !transform.subtransforms.is_empty()
                    && transform
                        .spec
                        .as_ref()
                        .is_some_and(|spec| spec.urn == URN_COMBINE_PER_KEY)
                {
                    transform.spec = None;
                }
                // Unique names must stay unique in the caller pipeline, which can expand
                // the same provider many times.
                if !transform_proto.unique_name.is_empty() {
                    transform.unique_name =
                        format!("{}/{}", transform_proto.unique_name, transform.unique_name);
                }
                if let Some(spec) = transform.spec.as_mut() {
                    rewrite_payload(spec, &replay, &prefix_coder);
                }
                transform.subtransforms = transform
                    .subtransforms
                    .into_iter()
                    .map(|id| prefix_transform(&id))
                    .collect();
                transform.inputs = transform
                    .inputs
                    .into_iter()
                    .map(|(tag, id)| (tag, prefix_pcoll(&id)))
                    .collect();
                transform.outputs = transform
                    .outputs
                    .into_iter()
                    .map(|(tag, id)| (tag, prefix_pcoll(&id)))
                    .collect();
                (prefix_transform(&k), transform)
            })
            .collect();

        let mut expanded_transform = transform_proto.clone();
        expanded_transform.subtransforms = root_ids.iter().map(|id| prefix_transform(id)).collect();
        expanded_transform.outputs.extend(
            output_ids
                .into_iter()
                .map(|(tag, pcoll_id)| (tag, prefix_pcoll(&pcoll_id))),
        );

        Ok(Response::new(ExpansionResponse {
            components: Some(components),
            transform: Some(expanded_transform),
            requirements,
            error: String::new(),
        }))
    }
}

// Built-in test SchemaTransform for GenerateSequence.
#[derive(Default)]
pub struct GenerateSequenceSchemaTransformProvider;

impl SchemaTransformProvider for GenerateSequenceSchemaTransformProvider {
    fn identifier(&self) -> &'static str {
        "beam:schematransform:org.apache.beam:generate_sequence:v1"
    }

    fn description(&self) -> &'static str {
        "Generates a sequence of integers from start to stop."
    }

    fn config_schema(&self) -> Schema {
        Schema::builder()
            .field("start", FieldType::int64())
            .field("stop", FieldType::int64())
            .build()
    }

    fn input_tags(&self) -> Vec<String> {
        Vec::new() // A source has no inputs.
    }

    fn output_tags(&self) -> Vec<String> {
        vec!["output".to_string()]
    }

    fn build_transform(
        &self,
        config: Row,
        _inputs: HashMap<String, PCollectionId>,
        pipeline: &mut Pipeline,
    ) -> Result<HashMap<String, PCollectionId>, String> {
        let start = config.get_i64("start").ok().flatten().unwrap_or(0);
        let stop = config.get_i64("stop").ok().flatten().unwrap_or(10);

        let pcoll = pipeline.apply(
            beam::transforms::GenerateSequence::new("GenerateSequence", start).with_end(stop),
        );

        Ok(HashMap::from([(
            "output".to_string(),
            pcoll.id().to_string(),
        )]))
    }
}

inventory::submit! {
    SchemaTransformRegistration {
        provider: || Box::new(GenerateSequenceSchemaTransformProvider),
    }
}
