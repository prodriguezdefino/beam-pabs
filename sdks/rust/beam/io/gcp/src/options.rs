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

//! Google Cloud Platform pipeline options.

use beam::options::{OptionGroupRegistration, OptionsError, PipelineOptionGroup};
use clap::Args;
use serde::{Deserialize, Serialize};

/// Google Cloud Platform options for GCP I/O connectors and Dataflow.
#[derive(Args, Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GcpOptions {
    /// Google Cloud project ID.
    #[arg(long)]
    pub project: Option<String>,

    /// Google Cloud region, such as `us-central1`.
    #[arg(long)]
    pub region: Option<String>,

    /// Google Cloud compute zone.
    #[arg(long)]
    pub zone: Option<String>,

    /// GCS path for temporary files, such as `gs://my-bucket/temp`.
    #[arg(long, alias = "tempLocation")]
    pub temp_location: Option<String>,

    /// GCS path for staging job artifacts, such as `gs://my-bucket/staging`.
    #[arg(long, alias = "stagingLocation")]
    pub staging_location: Option<String>,

    /// Service account email for worker instances.
    #[arg(long, alias = "serviceAccountEmail", alias = "service_account")]
    pub service_account_email: Option<String>,
}

// GCP I/O reads these on workers, so every job records them.
inventory::submit! { OptionGroupRegistration::of::<GcpOptions>() }

impl PipelineOptionGroup for GcpOptions {
    const NAMESPACE: &'static str = beam::pipeline::OPTION_NAMESPACE_GCP;

    fn group_name() -> &'static str {
        "GcpOptions"
    }

    fn validate(&self) -> Result<(), OptionsError> {
        [
            ("temp_location", &self.temp_location),
            ("staging_location", &self.staging_location),
        ]
        .into_iter()
        .try_for_each(|(name, value)| match value {
            Some(uri) if !uri.starts_with("gs://") => Err(OptionsError::Validation {
                group: Self::group_name(),
                message: format!("{name} must be a valid gs:// URI, found: '{uri}'"),
            }),
            _ => Ok(()),
        })
    }
}

impl GcpOptions {
    /// Checks the Dataflow fields: project, region, and a staging or temp location.
    pub fn require_dataflow_fields(&self) -> Result<(), OptionsError> {
        if self.project.is_none() {
            return Err(OptionsError::Validation {
                group: <Self as PipelineOptionGroup>::group_name(),
                message: "Google Cloud project (--project) is required for Dataflow".to_string(),
            });
        }
        if self.region.is_none() {
            return Err(OptionsError::Validation {
                group: <Self as PipelineOptionGroup>::group_name(),
                message: "Google Cloud region (--region) is required for Dataflow".to_string(),
            });
        }
        if self.staging_location.is_none() && self.temp_location.is_none() {
            return Err(OptionsError::Validation {
                group: <Self as PipelineOptionGroup>::group_name(),
                message: "A GCS staging or temp location (--staging_location or --temp_location) is required for Dataflow".to_string(),
            });
        }
        Ok(())
    }
}
