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

//! Staging of the artifacts, the dry run and the submission of a job.

use std::path::Path;

use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::runners::{JobState, PipelineResult};
use beam_io_file::FileSystem;
use prost::Message;
use tracing::info;

use super::DataflowRunnerError;
use super::config::ResolvedJobConfig;
use crate::client::{DataflowApiClient, JobResponse};
use crate::staging::{
    stage_and_resolve_environment_artifacts, stage_pipeline_model, stage_worker_binary,
};
use crate::translate::{
    DataflowJob, PackageItem, StagedArtifacts, apply_environment_overrides, translate_job,
};

/// Locations and hashes of staged pipeline artifacts.
#[derive(Debug, Clone)]
pub(super) struct StagedArtifactLocations {
    /// The graph as staged, so the submitted job describes the uploaded pipeline.
    pub(super) proto: beam_model::pipeline::Pipeline,
    model_url: String,
    model_hash: String,
    worker_url: Option<String>,
    worker_hash: Option<String>,
    xlang_packages: Vec<PackageItem>,
}

impl StagedArtifactLocations {
    pub(super) fn as_staged_artifacts(&self) -> StagedArtifacts<'_> {
        StagedArtifacts {
            model_url: &self.model_url,
            model_hash: &self.model_hash,
            worker_url: self.worker_url.as_deref(),
            worker_hash: self.worker_hash.as_deref(),
            packages: &self.xlang_packages,
        }
    }
}

/// Uploads the pipeline binary, declares it on `pipeline`, then uploads the graph. In this
/// order the graph names the artifact that the worker must fetch and execute.
pub(super) fn stage_artifacts(
    fs: &dyn FileSystem,
    staging_location: &str,
    job_name: &str,
    pipeline: &Pipeline,
    worker_binary: Option<&str>,
    overrides: &[String],
) -> Result<StagedArtifactLocations, DataflowRunnerError> {
    let worker = worker_binary
        .map(|path| stage_worker_binary(fs, staging_location, Path::new(path)))
        .transpose()?;

    if let Some((url, hash)) = &worker {
        pipeline.set_worker_binary_artifact(url, hash);
    }

    let mut proto = pipeline.to_proto();
    // Drop the expansion clients outside the pipeline lock, so their services (a JVM) stop
    // before the job runs.
    let expansion_clients = std::mem::take(&mut pipeline.lock().expansion_clients);
    drop(expansion_clients);
    apply_environment_overrides(&mut proto, overrides);
    let xlang_artifacts = pipeline.lock().xlang_artifacts.clone();
    let xlang_packages = stage_and_resolve_environment_artifacts(
        fs,
        staging_location,
        job_name,
        &mut proto,
        &xlang_artifacts,
    )?;

    let (model_url, model_hash) =
        stage_pipeline_model(fs, staging_location, job_name, &proto.encode_to_vec())?;

    let (worker_url, worker_hash) = worker.unzip();

    Ok(StagedArtifactLocations {
        proto,
        model_url,
        model_hash,
        worker_url,
        worker_hash,
        xlang_packages,
    })
}

pub(super) fn handle_dry_run(
    job_file: &str,
    proto_pipeline: &beam_model::pipeline::Pipeline,
    config: &ResolvedJobConfig,
) -> Result<PipelineResult, DataflowRunnerError> {
    let mut adapted_pipeline = proto_pipeline.clone();
    apply_environment_overrides(
        &mut adapted_pipeline,
        &config.options.worker.sdk_harness_container_image_overrides,
    );
    let dry_model_url = format!("{}/{}/model", config.staging_location, config.job_name);
    let artifacts = StagedArtifacts {
        model_url: &dry_model_url,
        model_hash: "dry-run-hash",
        worker_url: None,
        worker_hash: None,
        packages: &[],
    };
    let job = translate_job(
        &adapted_pipeline,
        &config.options,
        &config.job_name,
        &artifacts,
    )?;
    let json = serde_json::to_string_pretty(&job)?;
    std::fs::write(job_file, json)?;
    info!("Wrote Dataflow job description to '{job_file}'");
    Ok(PipelineResult::new("dry-run", JobState::Done))
}

pub(super) async fn submit_job(
    client: &dyn DataflowApiClient,
    config: &ResolvedJobConfig,
    job: &DataflowJob,
) -> Result<JobResponse, DataflowRunnerError> {
    info!(
        "Submitting Dataflow job '{}' to project '{}' ({})...",
        config.job_name, config.project, config.region
    );
    let created = client
        .create_job(&config.project, &config.region, job)
        .await?;
    // Dataflow returns the job of another request when a job with the same name is active.
    let other_request = created
        .client_request_id
        .as_deref()
        .is_some_and(|id| !id.is_empty() && Some(id) != job.client_request_id.as_deref());
    if other_request {
        return Err(DataflowRunnerError::JobAlreadyExists {
            name: config.job_name.clone(),
            job_id: created.id,
        });
    }
    info!(
        "Submitted Dataflow job '{}' with ID: {}",
        config.job_name, created.id
    );
    info!(
        "Dataflow Console: https://console.cloud.google.com/dataflow/jobs/{}/{}?project={}",
        config.region, created.id, config.project
    );
    info!(
        "Cloud Logging: https://console.cloud.google.com/logs/viewer?project={}&resource=dataflow_step%2Fjob_id%2F{}",
        config.project, created.id
    );
    Ok(created)
}

/// A `clientRequestId` that no other submission uses: the time in milliseconds, the process
/// id and a counter of this process.
pub(super) fn new_client_request_id() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    let count = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{millis}_{}_{count}", std::process::id())
}
