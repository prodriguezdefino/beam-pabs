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

//! Prism runner implementation submitting and monitoring jobs on Prism.

use prost::Message;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info};

use beam::metrics::MetricResults;
use beam::options::{PipelineOptions, PortableOptions, WorkerOptions, resolve_container_image};
use beam::pipeline::{
    DockerEnvironment, Pipeline, URN_ARTIFACT_ROLE_WORKER_BINARY, URN_ARTIFACT_TYPE_FILE,
    URN_ENV_DOCKER, URN_ENV_EXTERNAL,
};
use beam::runners::{PipelineResult, PipelineRunner, RunnerError, RunnerRegistration};
use harness::grpc;
use model::job_management::job_service_client::JobServiceClient;
use model::job_management::{
    GetJobMetricsRequest, JobMessagesRequest, PrepareJobRequest, RunJobRequest,
    job_message::MessageImportance, job_messages_response::Response as JobMessagesResponse,
    job_state,
};
use model::pipeline::{
    ApiServiceDescriptor, ArtifactFilePayload, ArtifactInformation, Components, DockerPayload,
    Environment, ExternalPayload, PTransform,
};

use crate::server::PrismServer;
use crate::staging::{declares_dependencies, localize_deferred_dependencies, stage_artifacts};
use crate::worker_pool::WorkerPool;

#[derive(Error, Debug)]
pub enum PrismRunnerError {
    #[error("Failed to connect or spawn Prism runner: {0}")]
    Server(#[from] crate::server::PrismServerError),
    #[error("Worker pool error: {0}")]
    WorkerPool(#[from] crate::worker_pool::WorkerPoolError),
    #[error("Invalid pipeline options: {0}")]
    Options(#[from] beam::options::OptionsError),
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Failed to connect to the Prism job service: {0}")]
    Channel(#[from] grpc::ChannelError),
    #[error("Artifact staging error: {0}")]
    Staging(#[from] crate::staging::StagingError),
    #[error("Job '{0}' failed during execution")]
    JobFailed(String),
    #[error("Job '{0}' was cancelled")]
    JobCancelled(String),
    #[error("Job state stream ended prematurely without reaching terminal state")]
    PrematureStreamEnd,
}

/// Options for configuring the Prism runner.
#[derive(Clone, Debug, Default)]
pub struct PrismRunnerOptions {
    /// Endpoint of the Prism JobService (e.g. "http://localhost:8073"). If omitted, the
    /// runner finds and spawns a local Prism process.
    pub endpoint: Option<String>,

    pub job_name: Option<String>,

    /// Portable execution environment options.
    pub portable: PortableOptions,

    /// Full pipeline options preserved for JobService submission.
    pub pipeline_options: Option<PipelineOptions>,
}

/// The Apache Beam Prism portable runner.
pub struct PrismRunner {
    options: PrismRunnerOptions,
}

impl PrismRunner {
    pub fn new() -> Self {
        Self {
            options: PrismRunnerOptions::default(),
        }
    }

    pub fn with_options(options: PrismRunnerOptions) -> Self {
        Self { options }
    }

    pub fn options(&self) -> &PrismRunnerOptions {
        &self.options
    }
}

impl Default for PrismRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&PipelineOptions> for PrismRunnerOptions {
    fn from(options: &PipelineOptions) -> Self {
        Self {
            endpoint: options.endpoint.clone(),
            job_name: options.job_name.clone(),
            portable: options.view_as::<PortableOptions>().unwrap_or_default(),
            pipeline_options: Some(options.clone()),
        }
    }
}

impl From<&PipelineOptions> for PrismRunner {
    fn from(options: &PipelineOptions) -> Self {
        Self::with_options(PrismRunnerOptions::from(options))
    }
}

fn create_prism_runner(options: &PipelineOptions) -> Box<dyn PipelineRunner> {
    Box::new(PrismRunner::from(options))
}

// Selectable as `--runner=prism` or `--runner=prismrunner` whenever this crate is linked.
inventory::submit! { RunnerRegistration { name: "prism", factory: create_prism_runner } }
inventory::submit! { RunnerRegistration { name: "prismrunner", factory: create_prism_runner } }

/// True for transforms the runner executes itself rather than in an SDK environment.
fn is_runner_transform(transform: &PTransform) -> bool {
    transform.spec.as_ref().is_some_and(|spec| {
        matches!(
            spec.urn.as_str(),
            "beam:transform:impulse:v1"
                | "beam:transform:group_by_key:v1"
                | "beam:transform:flatten:v1"
                | "beam:transform:teststream:v1"
        )
    })
}

/// Constructs an `Environment` definition for execution inside a Docker container.
fn docker_environment(
    container_image: impl Into<String>,
    dependencies: Vec<ArtifactInformation>,
) -> Environment {
    Environment {
        urn: URN_ENV_DOCKER.to_string(),
        payload: DockerPayload {
            container_image: container_image.into(),
        }
        .encode_to_vec(),
        display_data: Vec::new(),
        capabilities: {
            let mut caps = beam::pipeline::standard_capabilities();
            caps.insert(0, URN_ENV_DOCKER.to_string());
            caps
        },
        resource_hints: HashMap::new(),
        dependencies,
    }
}

/// Declares `path` as the pipeline binary for the container's boot program. This process
/// resolves the path: Prism gets the bytes through reverse staging and serves the worker.
fn worker_binary_dependency(path: &str) -> ArtifactInformation {
    ArtifactInformation {
        type_urn: URN_ARTIFACT_TYPE_FILE.to_string(),
        type_payload: ArtifactFilePayload {
            path: path.to_string(),
            sha256: String::new(),
        }
        .encode_to_vec(),
        role_urn: URN_ARTIFACT_ROLE_WORKER_BINARY.to_string(),
        role_payload: Vec::new(),
    }
}

/// Constructs an `Environment` definition for external loopback execution.
fn external_environment(endpoint: impl Into<String>) -> Environment {
    Environment {
        urn: URN_ENV_EXTERNAL.to_string(),
        payload: ExternalPayload {
            endpoint: Some(ApiServiceDescriptor {
                url: endpoint.into(),
                authentication: None,
            }),
            params: HashMap::new(),
        }
        .encode_to_vec(),
        display_data: Vec::new(),
        capabilities: {
            let mut caps = beam::pipeline::standard_capabilities();
            caps.insert(0, URN_ENV_EXTERNAL.to_string());
            caps
        },
        resource_hints: HashMap::new(),
        dependencies: Vec::new(),
    }
}

/// Swaps the SDK's default environment for the one this runner executes. Only transforms
/// bound to `default_environment_id` move: a cross-language pipeline also has foreign
/// environments, and the Rust worker has no handler for their transforms.
#[doc(hidden)]
pub fn bind_pipeline_environment(
    components: &mut Components,
    default_environment_id: &str,
    env_id: &str,
    mut environment: Environment,
) {
    if let Some(removed) = components.environments.remove(default_environment_id)
        && environment.resource_hints.is_empty()
    {
        environment.resource_hints = removed.resource_hints;
    }
    components
        .environments
        .insert(env_id.to_string(), environment.clone());

    // Bind any hint-derived Rust environments that still have URN_ENV_DEFAULT
    for (id, env) in components.environments.iter_mut() {
        if id != env_id && env.urn == beam::pipeline::URN_ENV_DEFAULT {
            env.urn = environment.urn.clone();
            env.payload = environment.payload.clone();
            env.capabilities = environment.capabilities.clone();
            env.dependencies = environment.dependencies.clone();
        }
    }

    let is_this_sdks = |id: &str| id.is_empty() || id == default_environment_id;

    components.transforms.values_mut().for_each(|t| {
        if is_runner_transform(t) {
            t.environment_id = String::new();
        } else if is_this_sdks(&t.environment_id) {
            t.environment_id = env_id.to_string();
        }
    });

    components
        .windowing_strategies
        .values_mut()
        .filter(|ws| is_this_sdks(&ws.environment_id))
        .for_each(|ws| ws.environment_id = env_id.to_string());
}

/// What makes two environments interchangeable. Repeated and map fields are sorted, because
/// their order has no meaning. Display data is descriptive, so it is excluded.
#[derive(PartialEq, Eq, Hash)]
struct EnvironmentIdentity {
    urn: String,
    payload: Vec<u8>,
    capabilities: Vec<String>,
    resource_hints: Vec<(String, Vec<u8>)>,
    dependencies: Vec<(String, Vec<u8>, String, Vec<u8>)>,
}

impl EnvironmentIdentity {
    fn of(environment: &Environment) -> Self {
        let mut capabilities = environment.capabilities.clone();
        capabilities.sort();

        let mut resource_hints: Vec<_> = environment
            .resource_hints
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        resource_hints.sort();

        let mut dependencies: Vec<_> = environment
            .dependencies
            .iter()
            .map(|dependency| {
                (
                    dependency.type_urn.clone(),
                    dependency.type_payload.clone(),
                    dependency.role_urn.clone(),
                    dependency.role_payload.clone(),
                )
            })
            .collect();
        dependencies.sort();

        Self {
            urn: environment.urn.clone(),
            payload: environment.payload.clone(),
            capabilities,
            resource_hints,
            dependencies,
        }
    }
}

/// Applies caller-configured container image overrides to Docker environments.
#[doc(hidden)]
pub fn apply_container_image_overrides(components: &mut Components, overrides: &[String]) {
    if overrides.is_empty() {
        return;
    }
    for env in components.environments.values_mut() {
        if env.urn == URN_ENV_DOCKER
            && !env.payload.is_empty()
            && let Ok(mut payload) = DockerPayload::decode(env.payload.as_slice())
        {
            let resolved = resolve_container_image(&payload.container_image, overrides);
            if resolved != payload.container_image {
                info!(
                    "Overriding environment container image '{}' -> '{}'",
                    payload.container_image, resolved
                );
                payload.container_image = resolved;
                env.payload = payload.encode_to_vec();
            }
        }
    }
}

/// Collapses environments that ask for the same thing into a single environment.
///
/// Two expansions against one service leave two copies of its environment. Each copy gets a
/// worker, and the workers reuse metric short IDs for different metrics, so Prism aborts the
/// job. Call this after dependencies are rewritten: copies that name artifacts by
/// expansion-service token differ only by that token.
#[doc(hidden)]
pub fn merge_identical_environments(components: &mut Components) {
    // Sorted, so the surviving id (the worker name in the logs) is stable between runs.
    let mut environment_ids: Vec<String> = components.environments.keys().cloned().collect();
    environment_ids.sort();

    let (_, replacements) = environment_ids.into_iter().fold(
        (
            HashMap::<EnvironmentIdentity, String>::new(),
            HashMap::<String, String>::new(),
        ),
        |(mut canonical, mut replacements), id| {
            let identity = EnvironmentIdentity::of(&components.environments[&id]);
            match canonical.entry(identity) {
                Entry::Occupied(kept) => {
                    info!("Merging environment '{id}' into identical '{}'", kept.get());
                    components.environments.remove(&id);
                    replacements.insert(id, kept.get().clone());
                }
                Entry::Vacant(slot) => {
                    slot.insert(id);
                }
            }
            (canonical, replacements)
        },
    );

    if replacements.is_empty() {
        return;
    }

    let resolve = |id: &mut String| {
        if let Some(kept) = replacements.get(id.as_str()) {
            *id = kept.clone();
        }
    };
    components
        .transforms
        .values_mut()
        .for_each(|transform| resolve(&mut transform.environment_id));
    components
        .windowing_strategies
        .values_mut()
        .for_each(|strategy| resolve(&mut strategy.environment_id));
}

/// Builds the Docker environment to bind, or `None` for loopback. Follows
/// [`DockerEnvironment::for_portable_runner`]: `--environment_type` wins, then an image flag.
#[doc(hidden)]
pub fn docker_environment_for(
    options: &PrismRunnerOptions,
) -> Result<Option<Environment>, PrismRunnerError> {
    let worker = options
        .pipeline_options
        .as_ref()
        .map(PipelineOptions::view_as::<WorkerOptions>)
        .transpose()?
        .unwrap_or_default();

    Ok(
        DockerEnvironment::for_portable_runner(&options.portable, &worker)?.map(|docker| {
            info!(
                "Prism runner configuring DOCKER environment with image: {}",
                docker.image
            );
            // Without a staged binary the image must have one pre-baked, so there is
            // nothing for the container to fetch.
            let dependencies = docker
                .worker_binary
                .as_deref()
                .map(worker_binary_dependency)
                .into_iter()
                .collect();
            docker_environment(docker.image, dependencies)
        }),
    )
}

/// Maps a job state to its terminal outcome, or `None` while the job is still in progress.
#[doc(hidden)]
pub fn terminal_outcome(
    state: i32,
    job_id: &str,
    last_error_message: &str,
) -> Option<Result<PipelineResult, PrismRunnerError>> {
    match job_state::Enum::try_from(state).ok()? {
        job_state::Enum::Done => {
            info!("Job '{}' completed successfully (DONE)", job_id);
            Some(Ok(PipelineResult::new(job_id, "DONE")))
        }
        job_state::Enum::Failed => {
            let err_detail = if last_error_message.is_empty() {
                job_id.to_string()
            } else {
                format!("{job_id}: {last_error_message}")
            };
            Some(Err(PrismRunnerError::JobFailed(err_detail)))
        }
        job_state::Enum::Cancelled => Some(Err(PrismRunnerError::JobCancelled(job_id.to_string()))),
        _ => None,
    }
}

#[async_trait::async_trait]
impl PipelineRunner for PrismRunner {
    async fn run(&self, pipeline: &Pipeline) -> Result<PipelineResult, RunnerError> {
        self.execute(pipeline).await.map_err(RunnerError::execution)
    }
}

impl PrismRunner {
    /// Runs `pipeline`, reporting failures with this runner's own error type.
    async fn execute(&self, pipeline: &Pipeline) -> Result<PipelineResult, PrismRunnerError> {
        // Resolved first: it reads only the options, so a misconfigured launch fails
        // before Prism is spawned, or downloaded on a first run.
        let docker_environment = docker_environment_for(&self.options)?;

        let _server_guard;
        let job_endpoint = match &self.options.endpoint {
            Some(ep) => grpc::with_scheme(ep),
            None => {
                let server = PrismServer::start_or_connect(None).await?;
                let ep = server.endpoint().to_string();
                _server_guard = Some(server);
                ep
            }
        };

        let mut worker_pool_opt: Option<WorkerPool> = None;
        let mut proto_pipeline = pipeline.to_proto();
        let default_environment_id = pipeline.default_environment_id();

        if let Some(environment) = docker_environment {
            if let Some(ref mut components) = proto_pipeline.components {
                bind_pipeline_environment(
                    components,
                    &default_environment_id,
                    "env_prism_docker",
                    environment,
                );
            }
        } else {
            let worker_pool =
                WorkerPool::start(None, Arc::new(pipeline.transform_handlers())).await?;
            let pool_endpoint = worker_pool.endpoint().to_string();
            worker_pool_opt = Some(worker_pool);

            if let Some(ref mut components) = proto_pipeline.components {
                bind_pipeline_environment(
                    components,
                    &default_environment_id,
                    "env_prism_external",
                    external_environment(pool_endpoint),
                );
            }
        }

        // Restate foreign dependencies as files this process can serve. The merge must
        // follow, because only restated copies compare equal.
        let image_overrides = self
            .options
            .pipeline_options
            .as_ref()
            .and_then(|opts| opts.view_as::<WorkerOptions>().ok())
            .map(|worker| worker.sdk_harness_container_image_overrides)
            .unwrap_or_default();

        let expansion_artifacts = pipeline.lock().xlang_artifacts.clone();
        if let Some(ref mut components) = proto_pipeline.components {
            localize_deferred_dependencies(components, &expansion_artifacts)?;
            apply_container_image_overrides(components, &image_overrides);
            merge_identical_environments(components);
        }

        // Prism asks for artifacts in a stream this process opens. Without it, workers of
        // any SDK that declare dependencies never boot.
        let serves_artifacts = proto_pipeline
            .components
            .as_ref()
            .is_some_and(declares_dependencies);

        let mut job_client = JobServiceClient::new(grpc::channel(&job_endpoint).await?)
            .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(grpc::MAX_MESSAGE_BYTES);

        let job_name = self
            .options
            .job_name
            .clone()
            .unwrap_or_else(|| "rust_beam_job".to_string());

        let pipeline_options = self
            .options
            .pipeline_options
            .as_ref()
            .map(|opts| opts.to_proto_struct())
            .transpose()?;

        let prep_req = PrepareJobRequest {
            pipeline: Some(proto_pipeline),
            pipeline_options,
            job_name,
        };

        let prep_resp = job_client.prepare(prep_req).await?.into_inner();
        let prep_id = prep_resp.preparation_id;

        if serves_artifacts {
            let staging_endpoint = prep_resp
                .artifact_staging_endpoint
                .as_ref()
                .map(|descriptor| descriptor.url.clone())
                .unwrap_or_else(|| job_endpoint.clone());
            info!("Serving pipeline artifacts to Prism at {staging_endpoint}");
            stage_artifacts(&staging_endpoint, &prep_resp.staging_session_token).await?;
        }

        let run_req = RunJobRequest {
            preparation_id: prep_id,
            retrieval_token: prep_resp.staging_session_token,
        };

        let run_resp = job_client.run(run_req).await?.into_inner();
        let job_id = run_resp.job_id;
        info!("Submitted job '{}' to Prism on {}", job_id, job_endpoint);

        let msg_req = JobMessagesRequest {
            job_id: job_id.clone(),
        };

        let mut msg_stream = job_client.get_message_stream(msg_req).await?.into_inner();
        let mut last_error_message = String::new();

        while let Some(item) = msg_stream.message().await? {
            let Some(response) = item.response else {
                continue;
            };

            match response {
                JobMessagesResponse::MessageResponse(msg) => {
                    info!("[Prism] {}", msg.message_text);
                    if msg.importance == MessageImportance::JobMessageError as i32 {
                        last_error_message = msg.message_text;
                    }
                }
                JobMessagesResponse::StateResponse(state_ev) => {
                    debug!("Job '{}' state transition: {:?}", job_id, state_ev.state);
                    // Prism sends the failure cause *after* the FAILED state, then closes
                    // the stream.
                    if state_ev.state == job_state::Enum::Failed as i32 {
                        while let Ok(Some(item)) = msg_stream.message().await {
                            if let Some(JobMessagesResponse::MessageResponse(msg)) = item.response
                                && msg.importance == MessageImportance::JobMessageError as i32
                            {
                                last_error_message = msg.message_text;
                            }
                        }
                    }
                    let outcome = terminal_outcome(state_ev.state, &job_id, &last_error_message);
                    if let Some(outcome) = outcome {
                        if let Some(mut pool) = worker_pool_opt {
                            pool.stop().await;
                        }
                        let mut res = outcome?;
                        if res.state == "DONE" {
                            let metrics_req = GetJobMetricsRequest {
                                job_id: job_id.clone(),
                            };
                            if let Ok(metrics_resp) = job_client.get_job_metrics(metrics_req).await
                                && let Some(metric_results) = metrics_resp.into_inner().metrics
                            {
                                let metrics = MetricResults::from_monitoring_infos(
                                    &metric_results.attempted,
                                    &metric_results.committed,
                                );
                                res.metrics = Some(metrics);
                            }
                        }
                        return Ok(res);
                    }
                }
            }
        }

        if let Some(mut pool) = worker_pool_opt {
            pool.stop().await;
        }
        Err(PrismRunnerError::PrematureStreamEnd)
    }
}
