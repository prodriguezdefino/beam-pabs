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

//! Validation and resolution of the options of a Dataflow job.

use beam::options::{
    DebugOptions, OptionsError, PipelineOptionGroup, PipelineOptions, WorkerOptions,
};
use beam::pipeline::{DockerEnvironment, default_sdk_container_image};
use gcp::GcpOptions;

use super::DataflowRunnerError;
use super::job_name::{generate_job_name, sanitize_job_name};
use crate::options::DataflowJobOptions;

/// Dataflow workers pull the image from a registry, so the image must be set: the default
/// SDK image is not published yet.
fn required_sdk_container_image(worker: &WorkerOptions) -> Result<&str, OptionsError> {
    worker
        .sdk_container_image
        .as_deref()
        .filter(|image| !image.is_empty())
        .ok_or_else(|| OptionsError::Validation {
            group: WorkerOptions::group_name(),
            message: format!(
                "Missing required option: sdk_container_image. Dataflow workers pull their \
                 image from a registry, and the default {} is not published yet. Build it with \
                 `./gradlew :sdks:rust:container:docker`, push it to a registry the workers \
                 can read (such as Artifact Registry) and pass --sdk_container_image=<image>.",
                default_sdk_container_image()
            ),
        })
}

/// Rejects experiments that disable Dataflow Runner v2, which portable Rust pipelines need.
fn reject_legacy_runner_experiments(experiments: &[String]) -> Result<(), OptionsError> {
    experiments
        .iter()
        .map(|experiment| {
            experiment
                .split_once('=')
                .map_or(experiment.as_str(), |(name, _)| name)
        })
        .find(|name| name.starts_with("disable_runner_v2") || *name == "disable_prime_runner_v2")
        .map_or(Ok(()), |name| {
            Err(OptionsError::Validation {
                group: DebugOptions::group_name(),
                message: format!(
                    "experiment '{name}' disables Dataflow Runner v2, which the Rust SDK \
                     requires; remove it"
                ),
            })
        })
}

/// Validated and resolved configuration for submitting a Dataflow job.
#[derive(Debug, Clone)]
pub(super) struct ResolvedJobConfig {
    pub(super) project: String,
    pub(super) region: String,
    pub(super) staging_location: String,
    pub(super) job_name: String,
    /// The worker container and, unless its image has one pre-baked, the binary to stage.
    pub(super) container: DockerEnvironment,
    pub(super) options: DataflowJobOptions,
}

impl ResolvedJobConfig {
    pub(super) fn from_options(options: &PipelineOptions) -> Result<Self, DataflowRunnerError> {
        let job_options = DataflowJobOptions::try_from(options)?;
        let gcp_opts = &job_options.gcp;
        gcp_opts.require_dataflow_fields()?;
        let worker_opts = &job_options.worker;
        reject_legacy_runner_experiments(&job_options.debug.experiments)?;

        let container = DockerEnvironment::resolve(
            Some(required_sdk_container_image(worker_opts)?),
            worker_opts.worker_binary.as_deref(),
        )?;

        let project = gcp_opts
            .project
            .clone()
            .ok_or_else(|| OptionsError::Validation {
                group: GcpOptions::group_name(),
                message: "Missing required option: project".to_string(),
            })?;

        let region = gcp_opts
            .region
            .clone()
            .ok_or_else(|| OptionsError::Validation {
                group: GcpOptions::group_name(),
                message: "Missing required option: region".to_string(),
            })?;

        let staging_location = gcp_opts
            .staging_location
            .clone()
            .or_else(|| gcp_opts.temp_location.clone())
            .ok_or_else(|| OptionsError::Validation {
                group: GcpOptions::group_name(),
                message: "Missing required option: staging_location or temp_location".to_string(),
            })?;

        let job_name = options
            .job_name
            .as_deref()
            .map(sanitize_job_name)
            .unwrap_or_else(generate_job_name);

        Ok(Self {
            project,
            region,
            staging_location,
            job_name,
            container,
            options: job_options,
        })
    }
}
