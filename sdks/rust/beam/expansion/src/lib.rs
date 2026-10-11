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

use async_trait::async_trait;
use clap::Parser;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::info;

pub type PCollectionId = String;

use beam::options::{HarnessOptions, OptionsSnapshot, PipelineOptions, parse_args};
use beam::pipeline::{Pipeline, URN_COMBINE_PER_KEY};
use beam::schema::{Row, Schema};
use beam::values::PCollection;
use harness::replay::ReplayEntry;
use model::expansion::expansion_service_server::{
    ExpansionService as ExpansionServiceGrpc, ExpansionServiceServer as GrpcServer,
};
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse, SchemaTransformConfig,
};
use model::pipeline as proto;

pub use inventory;

#[doc(hidden)]
pub mod config;
mod discovery;
mod generate_sequence;
mod naming;
mod replay;
mod request;

pub use generate_sequence::GenerateSequenceSchemaTransformProvider;

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

        let (provider, config_row) = match request::resolve_request(spec) {
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
        let inputs = request::provider_inputs(&provider.input_tags(), &transform_proto.inputs);
        let seed = request::input_components(req.components.as_ref(), &inputs);
        let replay = match config_row.to_row_bytes() {
            Ok(config_row_bytes) => ReplayEntry {
                provider: identifier.to_string(),
                config_schema: Some(provider.config_schema().into()),
                config_row: config_row_bytes,
                namespace: req.namespace.clone(),
                inputs,
                components: Some(seed.clone()),
                expansion_id: naming::new_expansion_id(),
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

        let (pipeline, output_ids) =
            match replay::build_expansion(provider.as_ref(), config_row, &replay) {
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
                    replay::rewrite_payload(spec, &replay, &prefix_coder);
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
        // Replace any placeholder outputs from the request with the outputs that the provider
        // built.
        expanded_transform.outputs = output_ids
            .into_iter()
            .map(|(tag, pcoll_id)| (tag, prefix_pcoll(&pcoll_id)))
            .collect();

        Ok(Response::new(ExpansionResponse {
            components: Some(components),
            transform: Some(expanded_transform),
            requirements,
            error: String::new(),
        }))
    }
}
