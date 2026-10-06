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

//! Translates a portable Beam pipeline into the Google Cloud Dataflow v1b3 REST job model.

use std::collections::{HashMap, hash_map::Entry};

use beam::options::{OptionsError, SDK_OPTIONS_OPTION};
use beam::pipeline::{URN_COMBINE_PER_KEY, URN_ENV_DOCKER};
use model::pipeline as proto;
use prost::Message;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::options::{DataflowJobOptions, DataflowOptions};

pub use crate::constants::*;

/// Errors that can occur during pipeline translation.
#[derive(Error, Debug)]
pub enum TranslateError {
    #[error("Failed to decode Docker environment payload: {0}")]
    DecodePayload(#[from] prost::DecodeError),

    #[error("Dataflow options validation error: {0}")]
    Options(String),

    #[error(transparent)]
    PipelineOptions(#[from] OptionsError),

    #[error("Serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// The top-level Dataflow v1b3 Job specification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataflowJob {
    pub project_id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub job_type: String,
    /// Steps array must be empty for Runner v2 Fn API execution.
    pub steps: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub labels: HashMap<String, String>,
    pub environment: DataflowEnvironment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataflowEnvironment {
    pub user_agent: UserAgent,
    pub version: Version,
    pub temp_storage_prefix: String,
    pub experiments: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_email: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub service_options: Vec<String>,
    pub sdk_pipeline_options: SdkPipelineOptions,
    pub worker_pools: Vec<WorkerPool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserAgent {
    pub name: String,
    pub version: String,
}

/// The free-form `environment.version` object. Its keys are `snake_case` (`job_type`,
/// `major`); Dataflow rejects a classic template that lacks `job_type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Version {
    pub job_type: String,
    pub major: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SdkPipelineOptions {
    pub options: SdkOptionsPayload,
    #[serde(
        rename = "display_data",
        skip_serializing_if = "Vec::is_empty",
        default
    )]
    pub display_data: Vec<DisplayDataItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SdkOptionsPayload {
    pub pipeline_url: String,
    pub pipeline_proto_hash: String,
    pub region: String,
    pub temp_location: String,
    pub experiments: Vec<String>,
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty", default)]
    pub additional_options: HashMap<String, serde_json::Value>,
}

pub use beam::transforms::display_data::DisplayDataItem;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPool {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_harness_container_image: Option<String>,
    pub sdk_harness_container_images: Vec<SdkHarnessContainerImage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine_type: Option<String>,
    pub num_workers: usize,
    pub ip_configuration: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subnetwork: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_size_gb: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub autoscaling_settings: Option<AutoscalingSettings>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub packages: Vec<PackageItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoscalingSettings {
    pub max_num_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackageItem {
    pub name: String,
    pub location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SdkHarnessContainerImage {
    pub container_image: String,
    pub use_single_core_per_container: bool,
    pub environment_id: String,
    pub capabilities: Vec<String>,
}

/// Artifacts staged to GCS for Dataflow execution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StagedArtifacts<'a> {
    pub model_url: &'a str,
    pub model_hash: &'a str,
    pub worker_url: Option<&'a str>,
    pub worker_hash: Option<&'a str>,
    pub packages: &'a [PackageItem],
}

/// Adapts a portable Beam pipeline proto for Dataflow submission.
///
/// [`CombinePerKey`](beam::transforms::CombinePerKey) expands into `PartialCombine`
/// (pre-shuffle `PartialCombineFn`), `GroupAccumulators` (`GroupByKey`) and
/// `MergeAccumulators`. The Dataflow Runner v2 graph compiler lifts a composite with the
/// [`URN_COMBINE_PER_KEY`] spec itself and expects exactly the subtransforms `GroupByKey` and
/// `CombineValues`. For other subtransforms it fails with
/// `Step <name>/PartialCombine not found in unordered_steps`.
///
/// This function sets `spec` to `None` on these composites and keeps their `subtransforms`.
/// Dataflow then runs each subtransform as a plain composite, and the pre-shuffle combine
/// stays.
#[must_use]
pub fn adapt_pipeline_for_dataflow(mut pipeline: proto::Pipeline) -> proto::Pipeline {
    if let Some(components) = pipeline.components.as_mut() {
        components
            .transforms
            .values_mut()
            .filter(|t| {
                t.spec
                    .as_ref()
                    .is_some_and(|spec| spec.urn == URN_COMBINE_PER_KEY)
                    && !t.subtransforms.is_empty()
            })
            .for_each(|t| {
                t.spec = None;
            });
    }
    pipeline
}

/// Rewrites environment Docker container images based on user overrides and development fallbacks.
pub fn apply_environment_overrides(pipeline: &mut proto::Pipeline, overrides: &[String]) {
    let Some(components) = pipeline.components.as_mut() else {
        return;
    };

    for (env_id, env) in &mut components.environments {
        if env.urn == URN_ENV_DOCKER
            && !env.payload.is_empty()
            && let Ok(mut payload) = proto::DockerPayload::decode(env.payload.as_slice())
        {
            let resolved = resolve_sdk_container_image(&payload.container_image, overrides);
            if resolved != payload.container_image {
                tracing::info!(
                    "Overriding environment '{}' container image: '{}' -> '{}'",
                    env_id,
                    payload.container_image,
                    resolved
                );
                payload.container_image = resolved;
                env.payload = payload.encode_to_vec();
            }
        }
    }
}

/// Resolves the effective container image for an environment given caller-supplied overrides.
pub fn resolve_sdk_container_image(image: &str, overrides: &[String]) -> String {
    beam::options::resolve_container_image(image, overrides)
}

/// Translates a Beam pipeline protobuf and configuration into a Dataflow v1b3 REST job.
pub fn translate_job(
    pipeline: &proto::Pipeline,
    opts: &DataflowJobOptions,
    job_name: &str,
    artifacts: &StagedArtifacts,
) -> Result<DataflowJob, TranslateError> {
    let project = opts
        .gcp
        .project
        .as_deref()
        .ok_or_else(|| TranslateError::Options("Missing GCP project".to_string()))?;
    let region = opts
        .gcp
        .region
        .as_deref()
        .ok_or_else(|| TranslateError::Options("Missing GCP region".to_string()))?;
    let temp_location = opts
        .gcp
        .temp_location
        .as_deref()
        .ok_or_else(|| TranslateError::Options("Missing GCP temp_location".to_string()))?;

    // Container images declared by the pipeline's environments.
    //
    // Capabilities are forwarded exactly as declared. Filling an empty list with this SDK's
    // own capabilities would misreport what a cross-language environment from another SDK
    // can do.
    let decoded: Result<Vec<_>, TranslateError> = pipeline
        .components
        .iter()
        .flat_map(|components| &components.environments)
        .filter(|(_, env)| env.urn == URN_ENV_DOCKER && !env.payload.is_empty())
        .map(|(env_id, env)| {
            let payload = proto::DockerPayload::decode(env.payload.as_slice())?;
            Ok((env_id, env, payload))
        })
        .collect();

    let mut sdk_harness_container_images: Vec<SdkHarnessContainerImage> = decoded?
        .into_iter()
        .filter(|(_, _, payload)| !payload.container_image.is_empty())
        .map(|(env_id, env, payload)| {
            let container_image = resolve_sdk_container_image(
                &payload.container_image,
                &opts.worker.sdk_harness_container_image_overrides,
            );
            SdkHarnessContainerImage {
                container_image,
                use_single_core_per_container: false,
                environment_id: env_id.clone(),
                capabilities: env.capabilities.clone(),
            }
        })
        .collect();

    // Environments are stored in a `HashMap`, so without this the submitted job would
    // differ between runs of the identical pipeline -- including which image is treated
    // as primary below.
    sdk_harness_container_images.sort_by(|a, b| a.environment_id.cmp(&b.environment_id));

    let effective_image = opts
        .worker
        .sdk_container_image
        .clone()
        .or_else(|| {
            sdk_harness_container_images
                .first()
                .map(|image| image.container_image.clone())
        })
        .unwrap_or_else(beam::pipeline::default_sdk_container_image);

    let sdk_harness_container_images = if sdk_harness_container_images.is_empty() {
        vec![SdkHarnessContainerImage {
            container_image: effective_image.clone(),
            use_single_core_per_container: false,
            environment_id: DEFAULT_RUST_ENVIRONMENT_ID.to_string(),
            capabilities: beam::pipeline::standard_capabilities(),
        }]
    } else {
        sdk_harness_container_images
    };

    let experiments = DataflowOptions::effective_experiments(&opts.debug.experiments);

    let ip_configuration = if opts.dataflow.no_use_public_ips {
        WORKER_IP_PRIVATE
    } else {
        WORKER_IP_UNSPECIFIED
    }
    .to_string();

    let packages: Vec<PackageItem> = artifacts
        .worker_url
        .map(|w_url| PackageItem {
            name: STAGED_WORKER_NAME.to_string(),
            location: w_url.to_string(),
            sha256: artifacts.worker_hash.map(ToString::to_string),
        })
        .into_iter()
        .chain(artifacts.packages.iter().cloned())
        .collect();

    let autoscaling_settings = opts.worker.max_num_workers.map(|max| AutoscalingSettings {
        max_num_workers: max,
    });

    let worker_pool = WorkerPool {
        kind: WORKER_POOL_KIND_HARNESS.to_string(),
        worker_harness_container_image: Some(effective_image.clone()),
        sdk_harness_container_images,
        machine_type: opts.dataflow.worker_machine_type.clone(),
        num_workers: opts.worker.num_workers.unwrap_or(1),
        ip_configuration,
        network: opts.dataflow.network.clone(),
        subnetwork: opts.dataflow.subnetwork.clone(),
        zone: opts.gcp.zone.clone(),
        disk_size_gb: opts.dataflow.disk_size_gb,
        disk_type: opts.dataflow.disk_type.clone(),
        autoscaling_settings,
        packages,
    };

    let core_items = [
        DisplayDataItem::text(
            "runner",
            OPTION_NAMESPACE_RUNNER,
            DATAFLOW_RUNNER_DISPLAY_NAME,
        ),
        DisplayDataItem::text("job_name", OPTION_NAMESPACE_CORE, job_name),
        DisplayDataItem::text("project", OPTION_NAMESPACE_GCP, project),
        DisplayDataItem::text("region", OPTION_NAMESPACE_GCP, region),
        DisplayDataItem::text("temp_location", OPTION_NAMESPACE_GCP, temp_location),
        DisplayDataItem::text(
            "sdk_container_image",
            OPTION_NAMESPACE_DATAFLOW,
            &effective_image,
        ),
    ];

    // Items describing how the job is launched come first, so they win over the
    // option they were derived from (for example the resolved `sdk_container_image`).
    let display_data: Vec<DisplayDataItem> = core_items
        .into_iter()
        .chain(opts.snapshot.display_data())
        .fold(Vec::new(), |mut acc, item| {
            if !acc.iter().any(|d| d.key == item.key) {
                acc.push(item);
            }
            acc
        });

    // Dataflow reads pipeline options by their camelCase names. The Rust harness reads the
    // full typed snapshot, which is encoded under `SDK_OPTIONS_OPTION`.
    let additional_options = opts
        .snapshot
        .flat_options()?
        .into_iter()
        .map(|(key, value)| (to_camel_case(&key), value))
        .filter(|(key, _)| !is_payload_struct_field(key))
        .chain(std::iter::once((
            SDK_OPTIONS_OPTION.to_string(),
            serde_json::Value::String(opts.snapshot.encode()),
        )))
        .try_fold(HashMap::new(), |mut acc, (key, value)| {
            match acc.entry(key) {
                Entry::Vacant(slot) => {
                    slot.insert(value);
                    Ok(acc)
                }
                Entry::Occupied(existing) => Err(TranslateError::Options(format!(
                    "two pipeline options are both named '{}' by Dataflow",
                    existing.key()
                ))),
            }
        })?;

    let has_unbounded = pipeline.components.as_ref().is_some_and(|components| {
        components
            .pcollections
            .values()
            .any(|p| p.is_bounded == proto::is_bounded::Enum::Unbounded as i32)
    });

    let is_streaming = opts.streaming || has_unbounded;

    let (job_type, job_type_version) = if is_streaming {
        (JOB_TYPE_STREAMING, FNAPI_STREAMING)
    } else {
        (JOB_TYPE_BATCH, FNAPI_BATCH)
    };

    let environment = DataflowEnvironment {
        user_agent: UserAgent {
            name: DATAFLOW_USER_AGENT_NAME.to_string(),
            version: BEAM_SDK_VERSION.to_string(),
        },
        version: Version {
            job_type: job_type_version.to_string(),
            major: DEFAULT_DATAFLOW_MAJOR_VERSION.to_string(),
        },
        temp_storage_prefix: temp_location.to_string(),
        experiments: experiments.clone(),
        service_account_email: opts.gcp.service_account_email.clone(),
        service_options: opts.dataflow.dataflow_service_options.clone(),
        sdk_pipeline_options: SdkPipelineOptions {
            options: SdkOptionsPayload {
                pipeline_url: artifacts.model_url.to_string(),
                pipeline_proto_hash: artifacts.model_hash.to_string(),
                region: region.to_string(),
                temp_location: temp_location.to_string(),
                experiments,
                additional_options,
            },
            display_data,
        },
        worker_pools: vec![worker_pool],
    };

    Ok(DataflowJob {
        project_id: project.to_string(),
        name: job_name.to_string(),
        job_type: job_type.to_string(),
        steps: Vec::new(),
        labels: opts.dataflow.parsed_labels(),
        environment,
    })
}

/// Whether `key` names a field [`SdkOptionsPayload`] already sets.
fn is_payload_struct_field(key: &str) -> bool {
    matches!(
        key,
        "pipelineUrl" | "pipelineProtoHash" | "region" | "tempLocation" | "experiments"
    )
}

fn to_camel_case(s: &str) -> String {
    let mut parts = s.split(['_', '-']);
    let first = parts.next().unwrap_or("");
    parts.fold(first.to_string(), |mut acc, part| {
        if let Some(first_char) = part.chars().next() {
            acc.extend(first_char.to_uppercase());
            acc.push_str(&part[first_char.len_utf8()..]);
        }
        acc
    })
}
