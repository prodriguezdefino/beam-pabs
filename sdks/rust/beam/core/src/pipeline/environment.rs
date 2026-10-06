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

//! Selection of the container in which a runner launches Rust SDK workers. Runners resolve it
//! at run time, not at build time. Dataflow always uses containers; a portable runner such as
//! Prism can also run workers in the submitting process.

use tracing::{info, warn};

use super::constants::{PREBAKED_WORKER_BINARY_PATH, default_sdk_container_image};
use crate::options::{OptionsError, PortableOptions, WorkerOptions};

/// The container in which Rust SDK workers run, and the source of its pipeline binary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DockerEnvironment {
    pub image: String,
    /// Linux build of the pipeline to stage for the image `boot` program. `None` when the
    /// image has the binary pre-baked at [`PREBAKED_WORKER_BINARY_PATH`].
    pub worker_binary: Option<String>,
}

impl DockerEnvironment {
    /// Applies the rules shared by every runner that launches worker containers.
    ///
    /// - With a worker binary, stage it and run it in `image`, or in the SDK default image.
    /// - With only an image, the image must have the binary pre-baked.
    /// - With a custom image and a binary at [`PREBAKED_WORKER_BINARY_PATH`], stage nothing.
    ///   Flex Templates use this. The launcher adds
    ///   `--worker_binary=/opt/apache/beam/worker_binary`.
    /// - With neither, return [`OptionsError::Validation`] before submission.
    pub fn resolve(image: Option<&str>, worker_binary: Option<&str>) -> Result<Self, OptionsError> {
        let image = image.filter(|image| !image.is_empty());
        match (image, worker_binary.filter(|binary| !binary.is_empty())) {
            (Some(image), Some(binary))
                if binary == PREBAKED_WORKER_BINARY_PATH
                    && image != default_sdk_container_image() =>
            {
                info!(
                    "Worker binary '{binary}' matches the pre-baked location in container image \
                     '{image}'; skipping binary staging"
                );
                Ok(Self {
                    image: image.to_string(),
                    worker_binary: None,
                })
            }
            (image, Some(binary)) => Ok(Self {
                image: image.map_or_else(default_sdk_container_image, str::to_string),
                worker_binary: Some(binary.to_string()),
            }),
            (Some(image), None) => {
                warn!(
                    "No --worker_binary given: assuming image '{image}' has the pipeline binary \
                     pre-baked at {PREBAKED_WORKER_BINARY_PATH}"
                );
                Ok(Self {
                    image: image.to_string(),
                    worker_binary: None,
                })
            }
            (None, None) => Err(OptionsError::Validation {
                group: "WorkerOptions",
                message: format!(
                    "workers run in a container, but nothing tells it which pipeline binary to \
                     run. Pass --worker_binary with a Linux build of this pipeline to stage, or \
                     --sdk_container_image with an image that has the binary pre-baked at \
                     {PREBAKED_WORKER_BINARY_PATH}."
                ),
            }),
        }
    }

    /// Resolves the container for a portable runner, or `None` when workers do not run in one.
    ///
    /// An explicit `--environment_type` decides. Without one, an image flag
    /// (`--environment_config` or `--sdk_container_image`) selects DOCKER, else LOOPBACK. Both
    /// image flags must name the same image. Other environment types ignore image flags with a
    /// warning.
    pub fn for_portable_runner(
        portable: &PortableOptions,
        worker: &WorkerOptions,
    ) -> Result<Option<Self>, OptionsError> {
        let config = portable
            .environment_config
            .as_deref()
            .filter(|config| !config.is_empty());
        let sdk_image = worker
            .sdk_container_image
            .as_deref()
            .filter(|image| !image.is_empty());

        let docker = match portable.environment_type.as_deref() {
            None => config.or(sdk_image).is_some(),
            Some(kind) if kind.eq_ignore_ascii_case("DOCKER") => true,
            Some(kind) => {
                // Other types read --environment_config as their own setting: an endpoint or
                // a command. It names an image only for LOOPBACK, which has no setting.
                let loopback = kind.eq_ignore_ascii_case("LOOPBACK");
                if let Some(image) = sdk_image.or(config.filter(|_| loopback)) {
                    warn!(
                        "Ignoring container image '{image}': --environment_type={kind} does not \
                         run workers in a container"
                    );
                }
                false
            }
        };

        docker
            .then(|| match (config, sdk_image) {
                (Some(config), Some(sdk_image)) if config != sdk_image => {
                    Err(OptionsError::Validation {
                        group: "PortableOptions",
                        message: format!(
                            "--environment_config ('{config}') and --sdk_container_image \
                             ('{sdk_image}') name different images; pass only one of them"
                        ),
                    })
                }
                (config, sdk_image) => {
                    Self::resolve(config.or(sdk_image), worker.worker_binary.as_deref())
                }
            })
            .transpose()
    }
}
