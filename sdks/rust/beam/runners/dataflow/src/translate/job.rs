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

//! The job resource of the Dataflow v1b3 REST API, as `jobs.create` takes it.

use std::collections::HashMap;

use beam::transforms::display_data::DisplayDataItem;
use serde::{Deserialize, Serialize};

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
    /// A unique id of the submission. Dataflow returns the existing job when a retried
    /// `create` sends the same id, so a retry never starts a second job.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub client_request_id: Option<String>,
}

/// Environment configuration for a Dataflow job execution.
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

/// User agent information identifying the SDK creating the job.
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

/// Pipeline options passed to the worker SDK harness.
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

/// Payload containing options and URLs for the SDK pipeline.
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

/// Worker pool configuration for running Dataflow workers.
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

/// Autoscaling settings controlling worker pool scaling behavior.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoscalingSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_num_workers: Option<usize>,
}

/// Staged package or file artifact available to worker processes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackageItem {
    pub name: String,
    pub location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// SDK harness container image specification for portable execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SdkHarnessContainerImage {
    pub container_image: String,
    pub use_single_core_per_container: bool,
    pub environment_id: String,
    pub capabilities: Vec<String>,
}
