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

//! Dataflow runner implementation for Apache Beam Rust SDK.

mod config;
mod handle;
mod job_name;
mod monitor;
mod submit;

use std::sync::Arc;
use std::time::Duration;

use beam::options::{OptionsError, PipelineOptions};
use beam::pipeline::Pipeline;
use beam::runners::{
    JobHandle, JobState, PipelineResult, PipelineRunner, RunnerError, RunnerRegistration,
};
use file::{FileSystem, get_filesystem};
use thiserror::Error;
use tracing::info;

use crate::client::{DataflowApiClient, HttpDataflowClient};
use crate::staging::write_bytes;
use crate::translate::{DataflowJob, translate_job};
use config::ResolvedJobConfig;
use handle::DataflowJobHandle;
use monitor::wait_for_job;
use submit::{handle_dry_run, new_client_request_id, stage_artifacts, submit_job};

pub use job_name::{generate_job_name, job_name_prefix};
pub(super) use monitor::{JobPoller, fetch_metrics};

/// Errors produced by Dataflow runner execution.
#[derive(Error, Debug)]
pub enum DataflowRunnerError {
    #[error("Options error: {0}")]
    Options(#[from] OptionsError),

    #[error("Staging error: {0}")]
    Staging(#[from] crate::staging::StagingError),

    #[error("Job translation error: {0}")]
    Translate(#[from] crate::translate::TranslateError),

    #[error("Dataflow API error: {0}")]
    Api(#[from] crate::client::ClientError),

    #[error("Dataflow job '{job_id}' failed with terminal state '{state}'")]
    JobFailed { job_id: String, state: String },

    #[error(
        "Dataflow already has an active job named '{name}' with id '{job_id}'. Set another \
         --job_name to submit a second job."
    )]
    JobAlreadyExists { name: String, job_id: String },

    #[error(
        "Dataflow test job '{job_id}' was cancelled because PAssert assertion(s) failed: \
         {failed:?} ({total} failure(s) counted in total)"
    )]
    AssertionsFailed {
        job_id: String,
        failed: Vec<String>,
        total: i64,
    },

    #[error(
        "Dataflow test job '{job_id}' was cancelled after {timeout:?} with PAssert \
         assertion(s) still pending: {missing:?}"
    )]
    TestTimedOut {
        job_id: String,
        timeout: Duration,
        missing: Vec<String>,
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

pub use crate::constants::DEFAULT_JOB_POLL_INTERVAL;

/// Apache Beam Google Cloud Dataflow runner.
#[derive(Clone)]
pub struct DataflowRunner {
    options: PipelineOptions,
    filesystem: Option<Arc<dyn FileSystem>>,
    dataflow_client: Option<Arc<dyn DataflowApiClient>>,
    poll_interval: Duration,
}

impl std::fmt::Debug for DataflowRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataflowRunner")
            .field("options", &self.options)
            .field("poll_interval", &self.poll_interval)
            .finish_non_exhaustive()
    }
}

impl DataflowRunner {
    /// Creates a `DataflowRunner` using default options and credentials.
    pub fn new() -> Self {
        Self::with_options(PipelineOptions::default())
    }

    /// Creates a `DataflowRunner` with specific pipeline options.
    pub fn with_options(options: PipelineOptions) -> Self {
        Self {
            options,
            filesystem: None,
            dataflow_client: None,
            poll_interval: DEFAULT_JOB_POLL_INTERVAL,
        }
    }

    /// Configures the status polling interval when waiting for job completion.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Creates a `DataflowRunner` with custom options and clients.
    pub fn with_options_and_clients(
        options: PipelineOptions,
        filesystem: Arc<dyn FileSystem>,
        dataflow_client: Arc<dyn DataflowApiClient>,
    ) -> Self {
        Self {
            options,
            filesystem: Some(filesystem),
            dataflow_client: Some(dataflow_client),
            poll_interval: Duration::from_millis(50),
        }
    }

    fn resolve_filesystem(&self, location: &str) -> std::io::Result<Arc<dyn FileSystem>> {
        self.filesystem
            .clone()
            .map(Ok)
            .unwrap_or_else(|| get_filesystem(location))
    }

    async fn resolve_dataflow_client(&self, endpoint: Option<&str>) -> Arc<dyn DataflowApiClient> {
        self.dataflow_client
            .clone()
            .unwrap_or_else(|| match endpoint {
                Some(ep) => Arc::new(HttpDataflowClient::with_endpoint_and_token(
                    ep,
                    None::<String>,
                )),
                None => Arc::new(HttpDataflowClient::new()),
            })
    }
}

impl Default for DataflowRunner {
    fn default() -> Self {
        Self::new()
    }
}

// Selectable as `--runner=dataflow` or `--runner=DataflowRunner`
inventory::submit! {
    RunnerRegistration {
        name: "dataflow",
        factory: |options: &PipelineOptions| Box::new(DataflowRunner::with_options(options.clone())),
    }
}

inventory::submit! {
    RunnerRegistration {
        name: "dataflowrunner",
        factory: |options: &PipelineOptions| Box::new(DataflowRunner::with_options(options.clone())),
    }
}

/// Coordinates of a submitted Dataflow job.
#[derive(Debug, Clone)]
pub(super) struct SubmittedJob {
    id: String,
    project: String,
    region: String,
    /// State reported by the create call; empty if the service did not report one.
    initial_state: String,
}

impl SubmittedJob {
    pub(super) fn id(&self) -> &str {
        &self.id
    }

    pub(super) fn project(&self) -> &str {
        &self.project
    }

    pub(super) fn region(&self) -> &str {
        &self.region
    }
}

/// What [`DataflowRunner::launch`] did with a pipeline.
enum Launch {
    /// Wrote a job file or a template. No job runs.
    Offline(Box<PipelineResult>),
    /// Dataflow accepted the job.
    Submitted(SubmittedJob, Arc<dyn DataflowApiClient>),
}

#[async_trait::async_trait]
impl PipelineRunner for DataflowRunner {
    async fn run(&self, pipeline: &Pipeline) -> Result<PipelineResult, RunnerError> {
        self.execute(pipeline).await.map_err(RunnerError::execution)
    }

    /// Returns when Dataflow accepts the job, whatever `--async_job` says.
    async fn submit(&self, pipeline: &Pipeline) -> Result<Box<dyn JobHandle>, RunnerError> {
        self.start(pipeline).await.map_err(RunnerError::execution)
    }
}

impl DataflowRunner {
    /// Returns the pipeline options configured for this runner.
    pub fn options(&self) -> &PipelineOptions {
        &self.options
    }

    /// Returns the interval between job status polling requests.
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Prepares artifacts and submits `pipeline` to Google Cloud Dataflow, returning
    /// the submitted job handle and API client without waiting for completion.
    pub(super) async fn prepare_and_submit(
        &self,
        pipeline: &Pipeline,
    ) -> Result<(SubmittedJob, Arc<dyn DataflowApiClient>), DataflowRunnerError> {
        let config = ResolvedJobConfig::from_options(&self.options)?;
        pipeline.set_docker_environment(&config.container.image);
        self.prepare_and_submit_with_config(pipeline, &config).await
    }

    /// The Docker environment of `pipeline` must already be set.
    async fn prepare_and_submit_with_config(
        &self,
        pipeline: &Pipeline,
        config: &ResolvedJobConfig,
    ) -> Result<(SubmittedJob, Arc<dyn DataflowApiClient>), DataflowRunnerError> {
        let job = DataflowJob {
            client_request_id: Some(new_client_request_id()),
            ..self.stage_and_translate(pipeline, config)?
        };
        let client = self
            .resolve_dataflow_client(config.options.dataflow.dataflow_endpoint.as_deref())
            .await;
        let created = submit_job(&*client, config, &job).await?;
        let submitted = SubmittedJob {
            id: created.id,
            project: config.project.clone(),
            region: config.region.clone(),
            initial_state: created.current_state,
        };
        Ok((submitted, client))
    }

    /// Stages the artifacts of `pipeline` and translates it into the job that describes
    /// them. The pipeline's Docker environment must already be set.
    fn stage_and_translate(
        &self,
        pipeline: &Pipeline,
        config: &ResolvedJobConfig,
    ) -> Result<DataflowJob, DataflowRunnerError> {
        let fs = self.resolve_filesystem(&config.staging_location)?;
        let staged = stage_artifacts(
            &*fs,
            &config.staging_location,
            &config.job_name,
            pipeline,
            config.container.worker_binary.as_deref(),
            &config.options.worker.sdk_harness_container_image_overrides,
        )?;
        let artifacts = staged.as_staged_artifacts();
        Ok(translate_job(
            &staged.proto,
            &config.options,
            &config.job_name,
            &artifacts,
        )?)
    }

    /// Runs `pipeline`, reporting failures with this runner's own error type.
    async fn execute(&self, pipeline: &Pipeline) -> Result<PipelineResult, DataflowRunnerError> {
        let config = ResolvedJobConfig::from_options(&self.options)?;
        pipeline.set_docker_environment(&config.container.image);

        match self.launch(pipeline, &config).await? {
            Launch::Offline(result) => Ok(*result),
            Launch::Submitted(job, _) if !config.options.debug.should_wait_until_finish() => {
                let state = if job.initial_state.is_empty() {
                    JobState::Pending
                } else {
                    job.initial_state.parse().unwrap_or_default()
                };
                Ok(PipelineResult::new(job.id, state))
            }
            Launch::Submitted(job, client) => {
                wait_for_job(&*client, &job, self.poll_interval).await
            }
        }
    }

    /// Submits `pipeline` and returns a handle to the job without waiting for it.
    async fn start(&self, pipeline: &Pipeline) -> Result<Box<dyn JobHandle>, DataflowRunnerError> {
        let config = ResolvedJobConfig::from_options(&self.options)?;
        pipeline.set_docker_environment(&config.container.image);

        Ok(match self.launch(pipeline, &config).await? {
            Launch::Offline(result) => (*result).into(),
            Launch::Submitted(job, client) => {
                Box::new(DataflowJobHandle::new(client, job, self.poll_interval))
            }
        })
    }

    /// Writes the job file or the template that `config` asks for, or else submits the job.
    /// The Docker environment of `pipeline` must already be set.
    async fn launch(
        &self,
        pipeline: &Pipeline,
        config: &ResolvedJobConfig,
    ) -> Result<Launch, DataflowRunnerError> {
        if let Some(ref job_file) = config.options.dataflow.dataflow_job_file {
            return handle_dry_run(job_file, &pipeline.to_proto(), config)
                .map(|result| Launch::Offline(Box::new(result)));
        }

        if let Some(ref location) = config.options.dataflow.template_location {
            let job = self.stage_and_translate(pipeline, config)?;
            return self
                .write_template(location, &job)
                .map(|result| Launch::Offline(Box::new(result)));
        }

        let (job, client) = self
            .prepare_and_submit_with_config(pipeline, config)
            .await?;
        Ok(Launch::Submitted(job, client))
    }

    /// Writes `job` to `location` as a classic template instead of submitting. Launch it with
    /// `gcloud dataflow jobs run --gcs-location=<location>` while the staged artifacts exist.
    fn write_template(
        &self,
        location: &str,
        job: &DataflowJob,
    ) -> Result<PipelineResult, DataflowRunnerError> {
        let fs = self.resolve_filesystem(location)?;
        write_bytes(&*fs, location, &serde_json::to_vec_pretty(job)?)?;
        info!(
            "Wrote Dataflow template for job '{}' to {location}",
            job.name
        );
        Ok(PipelineResult::new("template", JobState::Done))
    }
}
