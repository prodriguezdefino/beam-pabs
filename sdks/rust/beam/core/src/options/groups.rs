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

//! Standard pipeline option groups and the trait that defines a group.

use std::path::PathBuf;

use clap::Args;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::options::pipeline_options::PipelineOptions;

/// Error from the parse, downcast or validation of an option group.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum OptionsError {
    /// The option group did not parse from the command-line arguments.
    #[error("failed to parse option group '{group}': {message}")]
    ParseError {
        group: &'static str,
        message: String,
    },

    /// The option group failed validation, for example a required field is missing.
    #[error("validation error in option group '{group}': {message}")]
    Validation {
        group: &'static str,
        message: String,
    },

    /// Two option groups set the same option to different values.
    #[error("option '{key}' is set to both {first} and {second} by different option groups")]
    Conflict {
        key: String,
        first: String,
        second: String,
    },

    /// The SDK could not write or read the typed options snapshot.
    #[error("{message}")]
    Snapshot { message: String },
}

/// A typed group of pipeline options.
///
/// Runners, I/O connectors and pipelines declare the options that they read as a group and
/// read it with [`PipelineOptions::view_as`]. Pipeline arguments are also a group; see
/// [`parse`](crate::options::parse).
///
/// clap parses a group on the driver and serde sends it to the workers, so a group needs
/// both. A worker never sees the command line: it deserializes the exact values that the
/// driver resolved, including defaults. Display data comes from the serialized form.
///
/// Put credentials in [`Secret`](crate::options::Secret) fields, which hold a reference to
/// the credential, not its value.
pub trait PipelineOptionGroup:
    Args + Serialize + DeserializeOwned + Clone + Send + Sync + 'static
{
    /// Display data namespace of the options in this group, for example `beam:option:worker:v1`.
    const NAMESPACE: &'static str = crate::pipeline::constants::OPTION_NAMESPACE_USER;

    /// Human-readable name of the option group for error messages.
    fn group_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Checks the values on the driver before job submission. Workers do not call it: they
    /// receive validated values, and a check such as "this local file exists" is true only on
    /// the submitting machine.
    fn validate(&self) -> Result<(), OptionsError> {
        Ok(())
    }
}

/// Registers an option group so that each job snapshot includes it. Register groups that
/// only execution reads (for example in `DoFn` setup); otherwise workers see their defaults.
///
/// ```ignore
/// inventory::submit! { OptionGroupRegistration::of::<GcpOptions>() }
/// ```
pub struct OptionGroupRegistration {
    pub(super) resolve: fn(&PipelineOptions) -> Result<(), OptionsError>,
}

impl OptionGroupRegistration {
    pub const fn of<G: PipelineOptionGroup>() -> Self {
        Self {
            resolve: resolve_group::<G>,
        }
    }
}

fn resolve_group<G: PipelineOptionGroup>(options: &PipelineOptions) -> Result<(), OptionsError> {
    options.view_as::<G>().map(drop)
}

inventory::collect!(OptionGroupRegistration);

inventory::submit! { OptionGroupRegistration::of::<PortableOptions>() }
inventory::submit! { OptionGroupRegistration::of::<WorkerOptions>() }
inventory::submit! { OptionGroupRegistration::of::<DebugOptions>() }
inventory::submit! { OptionGroupRegistration::of::<ResourceHintsOptions>() }

/// Fn API worker flags that the container boot program of a runner passes.
///
/// The boot program runs the pipeline binary with `--worker=true`, the service endpoints, and
/// `--options_file`, which holds the typed job options. These flags describe the worker
/// process, not the job, so they are omitted from the options snapshot.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
#[command(next_help_heading = "Worker harness (set by the runner's boot program)")]
pub struct HarnessOptions {
    /// Run as an Fn API worker harness. Do not submit the pipeline.
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        default_value_t = false
    )]
    pub worker: bool,

    /// Unique worker ID that the runner assigns.
    #[arg(long, alias = "worker_id")]
    pub id: Option<String>,

    /// Endpoint of the runner's BeamFnLogging service.
    #[arg(long)]
    pub logging_endpoint: Option<String>,

    /// Endpoint of the runner's BeamFnControl service.
    #[arg(long)]
    pub control_endpoint: Option<String>,

    /// Endpoint of the runner's BeamFnStatus service.
    #[arg(long)]
    pub status_endpoint: Option<String>,

    /// Endpoint of the runner's ProvisionService.
    #[arg(long)]
    pub provision_endpoint: Option<String>,

    /// Endpoint of the runner's ArtifactRetrievalService.
    #[arg(long)]
    pub artifact_endpoint: Option<String>,

    /// Directory that the runner provides for semi-persistent worker state.
    #[arg(long, default_value = "/tmp")]
    pub semi_persist_dir: String,

    /// JSON file with the options snapshot of the job. The container boot program writes it.
    #[arg(long)]
    pub options_file: Option<PathBuf>,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            worker: false,
            id: None,
            logging_endpoint: None,
            control_endpoint: None,
            status_endpoint: None,
            provision_endpoint: None,
            artifact_endpoint: None,
            // Same as the clap default, so a value built in code matches a parsed one.
            semi_persist_dir: "/tmp".to_string(),
            options_file: None,
        }
    }
}

impl HarnessOptions {
    /// Returns `true` if this process serves bundles and does not submit a pipeline. Only
    /// `--worker` decides (the `boot` program always passes it), never endpoint flags alone.
    pub fn is_worker(&self) -> bool {
        self.worker
    }
}

/// Execution environment options for portable and container runners, such as Prism and Flink.
#[derive(Args, Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PortableOptions {
    /// Environment type that runs worker bundles: `LOOPBACK`, `DOCKER`, `PROCESS` or `EXTERNAL`.
    #[arg(long, alias = "environmentType")]
    pub environment_type: Option<String>,

    /// Environment configuration payload, for example a container image URL or process JSON.
    #[arg(long, alias = "environmentConfig")]
    pub environment_config: Option<String>,
}

impl PortableOptions {
    pub const ENVIRONMENT_TYPE: &'static str = "environment_type";
    pub const ENVIRONMENT_CONFIG: &'static str = "environment_config";

    /// Returns `true` if the environment type is `DOCKER`. The match ignores case.
    pub fn is_docker(&self) -> bool {
        self.environment_type
            .as_deref()
            .is_some_and(|t| t.eq_ignore_ascii_case("DOCKER"))
    }

    /// Returns `true` if the environment type is `LOOPBACK`. The match ignores case.
    pub fn is_loopback(&self) -> bool {
        self.environment_type
            .as_deref()
            .is_some_and(|t| t.eq_ignore_ascii_case("LOOPBACK"))
    }

    /// Returns the environment configuration as container image, or `default` if it is empty.
    pub fn container_image<'a>(&'a self, default: &'a str) -> &'a str {
        self.environment_config
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(default)
    }
}

impl PipelineOptionGroup for PortableOptions {
    const NAMESPACE: &'static str = crate::pipeline::constants::OPTION_NAMESPACE_PORTABLE;

    fn group_name() -> &'static str {
        "PortableOptions"
    }
}

/// Worker and execution environment options for distributed and container runners.
#[derive(Args, Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WorkerOptions {
    /// Container image for the SDK worker harness, for example `apache/beam_rust_sdk:latest`.
    #[arg(
        long,
        alias = "sdkContainerImage",
        alias = "worker_harness_container_image"
    )]
    pub sdk_container_image: Option<String>,

    /// Path to a compiled Linux worker binary. The runner stages it to remote storage.
    #[arg(long, alias = "workerBinary")]
    pub worker_binary: Option<String>,

    /// Initial number of workers.
    #[arg(long, alias = "numWorkers")]
    pub num_workers: Option<usize>,

    /// Maximum number of workers for autoscaling.
    #[arg(long, alias = "maxNumWorkers")]
    pub max_num_workers: Option<usize>,

    /// SDK harness image overrides as `pattern,image` or `pattern=image`, for example
    /// `.*java.*,apache/beam_java21_sdk:latest`.
    #[arg(
        long,
        alias = "sdkHarnessContainerImageOverrides",
        alias = "sdk_harness_container_image_override"
    )]
    pub sdk_harness_container_image_overrides: Vec<String>,

    /// Maximum minutes that one element can stay in a transform without output or completion.
    /// Then the worker exits so the runner can retry elsewhere. Unset means no limit.
    #[arg(long, alias = "elementProcessingTimeoutMinutes")]
    pub element_processing_timeout_minutes: Option<u64>,
}

impl WorkerOptions {
    pub const SDK_CONTAINER_IMAGE: &'static str = "sdk_container_image";
    pub const WORKER_BINARY: &'static str = "worker_binary";
    pub const NUM_WORKERS: &'static str = "num_workers";
    pub const MAX_NUM_WORKERS: &'static str = "max_num_workers";
    pub const SDK_HARNESS_CONTAINER_IMAGE_OVERRIDES: &'static str =
        "sdk_harness_container_image_overrides";
    pub const ELEMENT_PROCESSING_TIMEOUT_MINUTES: &'static str =
        "element_processing_timeout_minutes";
}

impl PipelineOptionGroup for WorkerOptions {
    const NAMESPACE: &'static str = crate::pipeline::constants::OPTION_NAMESPACE_WORKER;

    fn group_name() -> &'static str {
        "WorkerOptions"
    }

    fn validate(&self) -> Result<(), OptionsError> {
        self.num_workers
            .zip(self.max_num_workers)
            .filter(|&(num, max)| num > max)
            .map(|(num, max)| OptionsError::Validation {
                group: Self::group_name(),
                message: format!(
                    "num_workers ({num}) cannot be greater than max_num_workers ({max})"
                ),
            })
            .or_else(|| {
                self.worker_binary
                    .as_deref()
                    .filter(|binary| !std::path::Path::new(binary).exists())
                    .map(|binary| OptionsError::Validation {
                        group: Self::group_name(),
                        message: format!("Configured worker_binary does not exist: '{binary}'"),
                    })
            })
            .map_or(Ok(()), Err)
    }
}

/// Applies the first matching `pattern,target` or `pattern=target` override rule to
/// `image`. Returns `image` unchanged if no rule matches.
pub fn resolve_container_image(image: &str, overrides: &[String]) -> String {
    overrides
        .iter()
        .find_map(|rule| {
            let (pattern, target) = rule.split_once(',').or_else(|| rule.split_once('='))?;
            pattern_matches(pattern.trim(), image).then(|| target.trim().to_string())
        })
        .unwrap_or_else(|| image.to_string())
}

/// Not a regex: strips leading and trailing `.*`, then does a substring match. An empty
/// pattern matches all images.
fn pattern_matches(pattern: &str, image: &str) -> bool {
    let clean = pattern.trim_start_matches(".*").trim_end_matches(".*");
    if clean.is_empty() {
        true
    } else {
        image.contains(clean)
    }
}

/// Debug, experiment and execution control options.
#[derive(Args, Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DebugOptions {
    /// Experiment flags. Separate values with commas or give the flag more than once.
    #[arg(long, value_delimiter = ',')]
    pub experiments: Vec<String>,

    /// Submit the job and return immediately. Do not wait for completion.
    #[arg(
        long,
        alias = "async",
        alias = "execute_async",
        default_value_t = false
    )]
    pub async_job: bool,

    /// Wait for job completion before `run()` returns.
    #[arg(long, alias = "waitUntilFinish", default_value_t = true)]
    pub wait_until_finish: bool,
}

impl DebugOptions {
    pub const EXPERIMENTS: &'static str = "experiments";
    pub const ASYNC_JOB: &'static str = "async_job";
    pub const WAIT_UNTIL_FINISH: &'static str = "wait_until_finish";

    /// Returns `true` if the runner must wait for job completion. `async_job` takes
    /// precedence over `wait_until_finish`.
    pub fn should_wait_until_finish(&self) -> bool {
        !self.async_job && self.wait_until_finish
    }
}

impl PipelineOptionGroup for DebugOptions {
    const NAMESPACE: &'static str = crate::pipeline::constants::OPTION_NAMESPACE_DEBUG;

    fn group_name() -> &'static str {
        "DebugOptions"
    }
}

/// Pipeline resource hint options, set with `--resource_hints` or `--resource_hint`.
#[derive(Args, Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResourceHintsOptions {
    /// Resource hints for the whole pipeline, for example
    /// `--resource_hints=accelerator=type:nvidia-tesla-t4;count:1,min_ram=16GB`.
    #[arg(
        long,
        alias = "resource_hint",
        alias = "resource-hint",
        alias = "resourceHint",
        alias = "resourceHints",
        value_delimiter = ','
    )]
    pub resource_hints: Vec<String>,
}

impl PipelineOptionGroup for ResourceHintsOptions {
    const NAMESPACE: &'static str = crate::pipeline::constants::OPTION_NAMESPACE_RESOURCE;

    fn group_name() -> &'static str {
        "ResourceHintsOptions"
    }

    fn validate(&self) -> Result<(), OptionsError> {
        for hint in &self.resource_hints {
            crate::pipeline::resources::ResourceHints::parse_hint(hint).map_err(|e| {
                OptionsError::Validation {
                    group: Self::group_name(),
                    message: e.to_string(),
                }
            })?;
        }
        Ok(())
    }
}
