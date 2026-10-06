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

//! Google Cloud Dataflow runner options.

use std::collections::HashMap;

use beam::options::{
    DebugOptions, OptionGroupRegistration, OptionsError, OptionsSnapshot, PipelineOptionGroup,
    PipelineOptions, WorkerOptions,
};
use clap::Args;
use gcp::GcpOptions;
use serde::{Deserialize, Serialize};

/// Options specifically for executing pipelines on Google Cloud Dataflow.
#[derive(Args, Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DataflowOptions {
    /// GCE machine type for worker instances (e.g. `e2-standard-2`).
    #[arg(
        long,
        alias = "workerMachineType",
        alias = "machine_type",
        alias = "machineType"
    )]
    pub worker_machine_type: Option<String>,

    /// Size of the root disk for worker VMs in gigabytes.
    #[arg(long, alias = "diskSizeGb")]
    pub disk_size_gb: Option<usize>,

    /// Type of the root disk for worker VMs (e.g. `pd-standard` or `pd-ssd`).
    #[arg(long, alias = "diskType")]
    pub disk_type: Option<String>,

    /// Google Cloud network for worker VMs.
    #[arg(long)]
    pub network: Option<String>,

    /// Google Cloud subnetwork for worker VMs.
    #[arg(long)]
    pub subnetwork: Option<String>,

    /// Disable public external IP addresses for worker VMs.
    #[arg(long, alias = "noUsePublicIps", default_value_t = false)]
    pub no_use_public_ips: bool,

    /// Additional Dataflow service options (comma-separated or multiple occurrences).
    #[arg(long, alias = "dataflowServiceOptions", value_delimiter = ',')]
    #[serde(default)]
    pub dataflow_service_options: Vec<String>,

    /// Comma-separated or JSON key=value job labels.
    #[arg(long)]
    pub labels: Option<String>,

    /// Custom Dataflow API endpoint root URL (defaults to `https://dataflow.googleapis.com`).
    #[arg(long, alias = "dataflowEndpoint")]
    pub dataflow_endpoint: Option<String>,

    /// If set, dumps the translated Dataflow job JSON payload to this file path instead of submitting.
    #[arg(long, alias = "dataflowJobFile")]
    pub dataflow_job_file: Option<String>,

    /// If set, stages the pipeline and writes the Dataflow job description to this location
    /// (`gs://` or local) as a classic template to launch later, instead of submitting a job.
    #[arg(long, alias = "templateLocation")]
    pub template_location: Option<String>,
}

// Recorded for every job so the submitted options describe the whole Dataflow setup.
inventory::submit! { OptionGroupRegistration::of::<DataflowOptions>() }

impl PipelineOptionGroup for DataflowOptions {
    const NAMESPACE: &'static str = crate::constants::OPTION_NAMESPACE_DATAFLOW;

    fn group_name() -> &'static str {
        "DataflowOptions"
    }

    fn validate(&self) -> Result<(), OptionsError> {
        match (&self.dataflow_job_file, &self.template_location) {
            (Some(_), Some(_)) => Err(OptionsError::Validation {
                group: Self::group_name(),
                message: "dataflow_job_file and template_location cannot be combined: the \
                          first writes the job description without staging anything, the \
                          second stages the pipeline and writes a template to launch later"
                    .to_string(),
            }),
            _ => Ok(()),
        }
    }
}

impl DataflowOptions {
    /// Returns the effective list of experiments, guaranteeing `use_runner_v2` is present.
    pub fn effective_experiments(experiments: &[String]) -> Vec<String> {
        if experiments
            .iter()
            .any(|e| e == crate::constants::EXPERIMENT_USE_RUNNER_V2)
        {
            experiments.to_vec()
        } else {
            std::iter::once(crate::constants::EXPERIMENT_USE_RUNNER_V2.to_string())
                .chain(experiments.iter().cloned())
                .collect()
        }
    }

    /// Parses labels into a key-value map.
    pub fn parsed_labels(&self) -> HashMap<String, String> {
        let Some(raw) = &self.labels else {
            return HashMap::new();
        };
        serde_json::from_str::<HashMap<String, String>>(raw).unwrap_or_else(|_| {
            raw.split(',')
                .filter_map(|pair| pair.trim().split_once('='))
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .collect()
        })
    }
}

/// Everything a Dataflow job is configured from, resolved from its [`PipelineOptions`].
#[derive(Debug, Clone)]
pub struct DataflowJobOptions {
    pub gcp: GcpOptions,
    pub worker: WorkerOptions,
    pub debug: DebugOptions,
    pub dataflow: DataflowOptions,
    /// Whether the job was asked to run in streaming mode.
    pub streaming: bool,
    /// The typed options of the job, taken after the groups above were resolved. Its
    /// display data and flat options describe the job; workers receive it whole.
    pub snapshot: OptionsSnapshot,
}

impl TryFrom<&PipelineOptions> for DataflowJobOptions {
    type Error = OptionsError;

    fn try_from(options: &PipelineOptions) -> Result<Self, Self::Error> {
        Ok(Self {
            gcp: options.view_as()?,
            worker: options.view_as()?,
            debug: options.view_as()?,
            dataflow: options.view_as()?,
            streaming: options.streaming,
            snapshot: options.snapshot()?,
        })
    }
}
