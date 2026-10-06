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

//! Apache Beam Rust worker container boot program.
//!
//! The container `ENTRYPOINT` (`/opt/apache/beam/boot`). The SDK image is generic; the
//! pipeline binary is either baked into a custom image at `/opt/apache/beam/worker_binary`
//! (always preferred) or staged by the runner at submission (`--worker_binary`) and fetched
//! here at startup.
//!
//! Startup:
//! 1. Parse the runner's worker flags.
//! 2. Get the environment's artifacts and the job's options from the ProvisionService.
//! 3. Use the pre-baked binary, or fetch the staged one over `ArtifactRetrievalService`.
//! 4. Write the driver's options snapshot to a file and re-execute the binary with the
//!    worker flags plus `--options_file`, so it rebuilds the identical graph and serves it.

use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::Parser;
use tracing::{info, warn};

use beam::options::{HarnessOptions, SDK_OPTIONS_OPTION, parse_args};
use beam::pipeline::{PREBAKED_WORKER_BINARY_PATH, URN_ARTIFACT_TYPE_URL};
use harness::grpc;
use harness::provisioning::{
    WorkerBinary, artifact_endpoint, resolve_worker_binary, sdk_options_from, worker_flags,
    worker_options, write_artifact,
};
use model::fn_execution::{
    GetProvisionInfoRequest, ProvisionInfo, provision_service_client::ProvisionServiceClient,
};
use model::job_management::{
    GetArtifactRequest, ResolveArtifactsRequest,
    artifact_retrieval_service_client::ArtifactRetrievalServiceClient,
};
use model::pipeline::ArtifactInformation;

/// Directory where the fetched pipeline binary and the job's options snapshot are written.
///
/// Uses `/tmp` instead of `--semi_persist_dir` because `--semi_persist_dir` may be mounted
/// `noexec`, whereas `/tmp` is writable and executable in the container image.
const WORKER_BINARY_DIR: &str = "/tmp/beam-rust-worker";

/// File name of the options snapshot inside [`WORKER_BINARY_DIR`].
const OPTIONS_FILE_NAME: &str = "pipeline_options.json";

/// Flags the boot program consumes. Unknown flags are dropped before parsing
/// ([`parse_args`]) so that a runner adding a new flag cannot stop the container from
/// starting, or hide the flags that follow it.
#[derive(Parser, Debug)]
#[command(name = "boot", about = "Apache Beam Rust SDK worker boot", version)]
#[command(ignore_errors = true)]
struct BootArgs {
    #[command(flatten)]
    harness: HarnessOptions,
}

type BootResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> BootResult<()> {
    harness::init_logging();

    let args = parse_args::<BootArgs>().harness;
    info!(
        "Apache Beam Rust worker boot starting (id: {:?}, control: {:?}, provision: {:?})",
        args.id, args.control_endpoint, args.provision_endpoint
    );

    if let Err(e) = boot(args).await {
        // The runner sees only the exit status, so print the reason to stderr for the
        // worker-startup log collector.
        eprintln!("Apache Beam Rust worker boot failed: {e}");
        return Err(e);
    }

    Ok(())
}

async fn boot(args: HarnessOptions) -> BootResult<()> {
    let provision_endpoint = args.provision_endpoint.clone().ok_or(
        "No --provision_endpoint supplied. The boot program needs the provision service to \
         discover which pipeline binary to run.",
    )?;

    let info = fetch_provision_info(&provision_endpoint, args.id.as_deref()).await?;
    info!(
        "Provision info received: {} dependencies, retrieval token {}",
        info.dependencies.len(),
        if info.retrieval_token.is_empty() {
            "absent"
        } else {
            "present"
        }
    );

    let artifact_endpoint = artifact_endpoint(&args, &info);

    let prebaked = Path::new(PREBAKED_WORKER_BINARY_PATH);
    let binary =
        match resolve_worker_binary(prebaked.is_file().then_some(prebaked), &info.dependencies)? {
            WorkerBinary::Prebaked {
                path,
                ignored_staged,
            } => {
                if ignored_staged {
                    warn!(
                        "Ignoring the staged pipeline binary: the image has one pre-baked at {}, \
                     which takes precedence",
                        path.display()
                    );
                }
                info!("Using the pre-baked pipeline binary at {}", path.display());
                path.to_path_buf()
            }
            WorkerBinary::Staged(artifact) => {
                let endpoint = artifact_endpoint
                    .as_deref()
                    .ok_or("No artifact endpoint supplied by flags or provision info")?;
                let staged = materialize(endpoint, artifact).await?;
                info!("Pipeline binary staged at {}", staged.display());
                staged
            }
        };

    let encoded = sdk_options_from(&info).ok_or_else(|| {
        format!(
            "Provision info carries no '{SDK_OPTIONS_OPTION}'. The pipeline cannot rebuild \
             the graph its driver submitted without the driver's options."
        )
    })?;
    let options_file = write_options_file(encoded).await?;

    let final_args = worker_options(args, &info, options_file);

    Err(exec_pipeline(&binary, &final_args))
}

/// Writes the driver's encoded options snapshot for the pipeline binary.
async fn write_options_file(encoded: &str) -> BootResult<PathBuf> {
    tokio::fs::create_dir_all(WORKER_BINARY_DIR).await?;
    let path = Path::new(WORKER_BINARY_DIR).join(OPTIONS_FILE_NAME);
    tokio::fs::write(&path, encoded).await?;
    Ok(path)
}

/// Asks the runner which artifacts belong to this environment.
async fn fetch_provision_info(
    endpoint: &str,
    worker_id: Option<&str>,
) -> BootResult<ProvisionInfo> {
    let mut client = ProvisionServiceClient::new(grpc::channel(endpoint).await?)
        .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES);

    let mut request = tonic::Request::new(GetProvisionInfoRequest {});
    if let Some(id) = worker_id {
        grpc::attach_worker_id(&mut request, id);
    }

    client
        .get_provision_info(request)
        .await?
        .into_inner()
        .info
        .ok_or_else(|| "Provision service returned an empty ProvisionInfo".into())
}

/// Downloads `artifact` and returns the path to the executable copy on local disk.
async fn materialize(endpoint: &str, artifact: &ArtifactInformation) -> BootResult<PathBuf> {
    let mut client = ArtifactRetrievalServiceClient::new(grpc::channel(endpoint).await?)
        .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES);

    // The service may swap a URL for something it serves directly, else it echoes it back.
    let resolved = client
        .resolve_artifacts(ResolveArtifactsRequest {
            artifacts: vec![artifact.clone()],
            preferred_urns: vec![URN_ARTIFACT_TYPE_URL.to_string()],
        })
        .await?
        .into_inner()
        .replacements
        .into_iter()
        .next()
        .unwrap_or_else(|| artifact.clone());

    let mut stream = client
        .get_artifact(GetArtifactRequest {
            artifact: Some(resolved),
        })
        .await?
        .into_inner();

    tokio::fs::create_dir_all(WORKER_BINARY_DIR).await?;
    let path = Path::new(WORKER_BINARY_DIR).join("worker");

    let written = write_artifact(&mut stream, &path).await?;

    tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).await?;
    info!("Fetched pipeline binary ({written} bytes)");
    Ok(path)
}

/// Replaces this process with the pipeline binary; returns only on failure. The pipeline
/// stays PID 1, so the runner's health checks and signals reach it directly.
fn exec_pipeline(binary: &Path, args: &HarnessOptions) -> Box<dyn std::error::Error + Send + Sync> {
    let worker_flags = worker_flags(args);

    info!(
        "Executing pipeline binary {} with worker flags {:?}",
        binary.display(),
        worker_flags
    );

    Command::new(binary).args(worker_flags).exec().into()
}
