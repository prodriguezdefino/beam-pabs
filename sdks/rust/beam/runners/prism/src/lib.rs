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

//! Apache Beam Prism Portable Runner for Rust.
//!
//! Provides:
//! - [`PrismRunner`]: Executes Beam pipelines locally using the Beam Prism portable runner.
//! - [`PrismServer`]: Manages the lifecycle of the Prism runner background process.
//! - [`WorkerPool`]: Serves `BeamFnExternalWorkerPool` to execute SDK worker harnesses in loopback mode.
//!
//! Loopback is the default. With `--environment_type=DOCKER` the pipeline binary named by
//! `--worker_binary` is served to the container over the artifact staging service, matching
//! how a production runner delivers it.

pub(crate) mod runner;
pub(crate) mod server;
pub(crate) mod staging;
pub(crate) mod worker_pool;

pub use runner::PrismRunnerError;
pub use runner::{PrismRunner, PrismRunnerOptions};
#[doc(hidden)]
pub use runner::{
    apply_container_image_overrides, bind_pipeline_environment, docker_environment_for, follow_job,
    merge_identical_environments, terminal_outcome,
};
pub use server::PrismServer;
#[doc(hidden)]
pub use server::{newest_prism_in, version_key};
pub use staging::StagingError;
pub use staging::stage_artifacts;
#[doc(hidden)]
pub use staging::{declares_dependencies, localize_deferred_dependencies};
pub use worker_pool::WorkerPool;
