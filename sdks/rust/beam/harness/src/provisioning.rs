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

//! Helpers of the boot program (`src/boot.rs`): read `ProvisionInfo`, write the fetched
//! binary and build its worker flags. In the library only so `tests/` can use them; not
//! public API.

use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;
use tokio_stream::{Stream, StreamExt};

use beam::options::{HarnessOptions, SDK_OPTIONS_OPTION};
use beam::pipeline::{PREBAKED_WORKER_BINARY_PATH, URN_ARTIFACT_ROLE_WORKER_BINARY};
use model::fn_execution::ProvisionInfo;
use model::job_management::GetArtifactResponse;
use model::pipeline::ArtifactInformation;

/// Error type the boot program reports.
pub type BootError = Box<dyn std::error::Error + Send + Sync>;

/// The pipeline binary a worker container executes.
#[derive(Debug, PartialEq)]
pub enum WorkerBinary<'a> {
    /// The binary baked into the image at [`PREBAKED_WORKER_BINARY_PATH`].
    Prebaked {
        path: &'a Path,
        /// Whether the runner also staged a binary, which goes unused.
        ignored_staged: bool,
    },
    /// A binary the runner staged, still to be fetched over the artifact retrieval service.
    Staged(&'a ArtifactInformation),
}

/// Decides which pipeline binary the container executes.
///
/// A `prebaked` binary from the image wins over a staged one, since the image was built for
/// this pipeline. Without it, the staged `dependencies` must name the binary.
pub fn resolve_worker_binary<'a>(
    prebaked: Option<&'a Path>,
    dependencies: &'a [ArtifactInformation],
) -> Result<WorkerBinary<'a>, BootError> {
    match (prebaked, dependencies) {
        (Some(path), _) => Ok(WorkerBinary::Prebaked {
            path,
            ignored_staged: dependencies
                .iter()
                .any(|dep| dep.role_urn == URN_ARTIFACT_ROLE_WORKER_BINARY),
        }),
        (None, []) => Err(format!(
            "No pipeline binary to run: the image has none pre-baked at \
             {PREBAKED_WORKER_BINARY_PATH} and the runner staged none. Submit with \
             --worker_binary=<Linux build of the pipeline> to stage one, or bake it into \
             the image at that path."
        )
        .into()),
        (None, _) => select_worker_binary(dependencies).map(WorkerBinary::Staged),
    }
}

/// Picks the dependency that is this pipeline's executable.
///
/// Matches the worker-binary role first, or falls back to a single staged dependency.
pub fn select_worker_binary(
    dependencies: &[ArtifactInformation],
) -> Result<&ArtifactInformation, BootError> {
    dependencies
        .iter()
        .find(|dep| dep.role_urn == URN_ARTIFACT_ROLE_WORKER_BINARY)
        .or(match dependencies {
            [only] => Some(only),
            _ => None,
        })
        .ok_or_else(|| {
            format!(
                "No pipeline binary among {} staged dependencies. Expected one with role '{}', \
                 found roles: [{}]. Runners add it when the job is submitted with \
                 --worker_binary.",
                dependencies.len(),
                URN_ARTIFACT_ROLE_WORKER_BINARY,
                dependencies
                    .iter()
                    .map(|d| d.role_urn.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .into()
        })
}

/// The driver's encoded [`OptionsSnapshot`](beam::options::OptionsSnapshot) from the
/// forwarded options; `None` if the job was not submitted by a Rust driver of this version.
pub fn sdk_options_from(info: &ProvisionInfo) -> Option<&str> {
    let options = info.pipeline_options.as_ref()?;

    // Portable runners key the option by URN; Dataflow nests it one level down. Try both.
    let nested = || {
        options
            .fields
            .get("options")
            .and_then(|value| match &value.kind {
                Some(prost_types::value::Kind::StructValue(inner)) => {
                    inner.fields.get(SDK_OPTIONS_OPTION)
                }
                _ => None,
            })
    };

    let value = options
        .fields
        .get(&format!("beam:option:{SDK_OPTIONS_OPTION}:v1"))
        .or_else(nested)?;

    match value.kind.as_ref()? {
        prost_types::value::Kind::StringValue(encoded) => Some(encoded),
        _ => None,
    }
}

/// The artifact endpoint: the `--artifact_endpoint` flag, else the provision info's.
#[doc(hidden)]
pub fn artifact_endpoint(args: &HarnessOptions, info: &ProvisionInfo) -> Option<String> {
    args.artifact_endpoint
        .clone()
        .or_else(|| info.artifact_endpoint.as_ref().map(|e| e.url.clone()))
}

/// The options the pipeline binary is re-executed with: `args`, with unset endpoints taken
/// from the provision info and `options_file` set. `options_file` is `None` when the job
/// carries no Rust options snapshot, because a driver of another SDK submitted it.
#[doc(hidden)]
pub fn worker_options(
    args: HarnessOptions,
    info: &ProvisionInfo,
    options_file: Option<PathBuf>,
) -> HarnessOptions {
    let artifact_endpoint = artifact_endpoint(&args, info);
    let status_endpoint = args
        .status_endpoint
        .clone()
        .or_else(|| info.status_endpoint.as_ref().map(|e| e.url.clone()));

    HarnessOptions {
        status_endpoint,
        artifact_endpoint,
        options_file,
        ..args
    }
}

/// The flags that pass `args` to the pipeline binary, led by `--worker=true`.
#[doc(hidden)]
pub fn worker_flags(args: &HarnessOptions) -> Vec<String> {
    let optional_flags = [
        ("id", args.id.clone()),
        ("logging_endpoint", args.logging_endpoint.clone()),
        ("control_endpoint", args.control_endpoint.clone()),
        ("status_endpoint", args.status_endpoint.clone()),
        ("provision_endpoint", args.provision_endpoint.clone()),
        ("artifact_endpoint", args.artifact_endpoint.clone()),
        ("semi_persist_dir", Some(args.semi_persist_dir.clone())),
        (
            "options_file",
            args.options_file.as_ref().map(|p| p.display().to_string()),
        ),
    ];

    std::iter::once("--worker=true".to_string())
        .chain(
            optional_flags
                .into_iter()
                .filter_map(|(name, value)| value.map(|v| format!("--{name}={v}"))),
        )
        .collect()
}

/// Writes a `GetArtifact` stream to a new file at `path`, returning the bytes written.
///
/// An empty artifact is an error, since executing it would fail less clearly later.
#[doc(hidden)]
pub async fn write_artifact<S>(mut stream: S, path: &Path) -> Result<usize, BootError>
where
    S: Stream<Item = Result<GetArtifactResponse, tonic::Status>> + Unpin,
{
    let mut file = tokio::fs::File::create(path).await?;
    let mut written = 0usize;
    while let Some(chunk) = stream.next().await.transpose()? {
        written += chunk.data.len();
        file.write_all(&chunk.data).await?;
    }
    file.flush().await?;
    drop(file);

    if written == 0 {
        return Err(format!(
            "Artifact retrieval returned no bytes for the pipeline binary at {}",
            path.display()
        )
        .into());
    }
    Ok(written)
}
