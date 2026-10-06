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

//! Apache Beam Google Cloud Dataflow runner for the Rust SDK.
//!
//! Submits portable Beam pipelines to Google Cloud Dataflow using the Dataflow
//! Runner v2 (Fn API) architecture.
//!
//! # Worker containers
//!
//! Every worker container runs `/opt/apache/beam/boot`, which execs the pipeline
//! binary. `--sdk_container_image` is required, and the binary reaches the
//! container in one of two ways.
//!
//! ## Staged binary on the SDK base image
//! The runner uploads a Linux build of the pipeline to the staging location and
//! the workers download it at startup:
//!
//! ```bash
//! cargo run --bin wordcount -- \
//!   --runner=DataflowRunner \
//!   --project=my-gcp-project \
//!   --region=us-central1 \
//!   --temp_location=gs://my-bucket/temp \
//!   --sdk_container_image=us-docker.pkg.dev/my-gcp-project/beam/beam_rust_sdk:<beam-version> \
//!   --worker_binary=target/x86_64-unknown-linux-gnu/release/wordcount
//! ```
//!
//! ## Pre-baked image
//! The image already holds the binary at `/opt/apache/beam/worker_binary`, so
//! nothing is staged:
//!
//! ```bash
//! cargo run --bin wordcount -- \
//!   --runner=DataflowRunner \
//!   --project=my-gcp-project \
//!   --region=us-central1 \
//!   --temp_location=gs://my-bucket/temp \
//!   --sdk_container_image=us-docker.pkg.dev/my-gcp-project/beam/wordcount:latest
//! ```
//!
//! Adding `--template_location=gs://my-bucket/templates/wordcount` to either
//! command stages the pipeline and writes a classic template instead of
//! starting a job.

pub mod client;
pub mod constants;
pub mod options;
pub mod staging;
pub mod translate;

pub use client::{
    DataflowApiClient, HttpDataflowClient, JobMetricsResponse, JobState, MetricStructuredName,
    MetricUpdateItem, is_successful_state, is_terminal_state,
};
pub use constants::*;
pub use options::{DataflowJobOptions, DataflowOptions};
pub use translate::{
    DataflowJob, StagedArtifacts, TranslateError, adapt_pipeline_for_dataflow,
    apply_environment_overrides, resolve_sdk_container_image, translate_job,
};
