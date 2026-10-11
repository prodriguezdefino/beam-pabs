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

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;
use tokio_stream::{Stream, StreamExt};

use crate::bundle_processor::STREAMING_PROCESSOR_IDLE_TIMEOUT;
use beam::options::{HarnessOptions, SDK_OPTIONS_OPTION, WorkerOptions};
use beam::pipeline::{PREBAKED_WORKER_BINARY_PATH, URN_RUST_ARTIFACT_ROLE_WORKER_BINARY};
use model::fn_execution::ProvisionInfo;
use model::job_management::GetArtifactResponse;
use model::pipeline::ArtifactInformation;
use prost_types::value::Kind;

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
                .any(|dep| dep.role_urn == URN_RUST_ARTIFACT_ROLE_WORKER_BINARY),
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
        .find(|dep| dep.role_urn == URN_RUST_ARTIFACT_ROLE_WORKER_BINARY)
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
                URN_RUST_ARTIFACT_ROLE_WORKER_BINARY,
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
    match portable_option(info, SDK_OPTIONS_OPTION, SDK_OPTIONS_OPTION)? {
        Kind::StringValue(encoded) => Some(encoded),
        _ => None,
    }
}

/// The value of the option that a driver forwarded as `name` (snake case) or `camel`.
/// Portable runners key it by URN; Dataflow nests it one level down, by the name that the
/// driver's SDK uses.
fn portable_option<'a>(info: &'a ProvisionInfo, name: &str, camel: &str) -> Option<&'a Kind> {
    let options = info.pipeline_options.as_ref()?;
    let nested = || match options.fields.get("options")?.kind.as_ref()? {
        Kind::StructValue(inner) => inner.fields.get(name).or_else(|| inner.fields.get(camel)),
        _ => None,
    };
    options
        .fields
        .get(&format!("beam:option:{name}:v1"))
        .or_else(nested)?
        .kind
        .as_ref()
}

/// The worker options that a driver of another SDK forwarded: the log levels, the element
/// processing timeout, the state cache size, the `data_buffer_time_limit_ms` and
/// `data_buffer_size_limit` experiments, and the streaming mode as the processor cache
/// timeout. Options that this SDK does not read are ignored.
pub fn portable_worker_options(info: &ProvisionInfo) -> WorkerOptions {
    let string = |name, camel| match portable_option(info, name, camel)? {
        Kind::StringValue(value) => Some(value.clone()),
        _ => None,
    };
    let number = |name, camel| match portable_option(info, name, camel)? {
        Kind::NumberValue(value) if *value >= 0.0 => Some(*value as u64),
        Kind::StringValue(value) => value.trim().parse().ok(),
        _ => None,
    };
    let experiment = |key: &str| match portable_option(info, "experiments", "experiments")? {
        Kind::ListValue(list) => list.values.iter().find_map(|value| match &value.kind {
            Some(Kind::StringValue(entry)) => entry
                .strip_prefix(key)
                .and_then(|rest| rest.strip_prefix('='))
                .and_then(|n| n.trim().parse().ok()),
            _ => None,
        }),
        _ => None,
    };
    WorkerOptions {
        default_sdk_harness_log_level: string(
            "default_sdk_harness_log_level",
            "defaultSdkHarnessLogLevel",
        )
        .map(|level| portable_level(&level)),
        sdk_harness_log_level_overrides: portable_option(
            info,
            "sdk_harness_log_level_overrides",
            "sdkHarnessLogLevelOverrides",
        )
        .map(level_overrides)
        .unwrap_or_default(),
        element_processing_timeout_minutes: number(
            "element_processing_timeout_minutes",
            "elementProcessingTimeoutMinutes",
        ),
        state_cache_size_mb: number("max_cache_memory_usage_mb", "maxCacheMemoryUsageMb")
            .and_then(|mb| usize::try_from(mb).ok()),
        data_buffer_time_limit_ms: experiment("data_buffer_time_limit_ms"),
        data_buffer_size_limit: experiment("data_buffer_size_limit")
            .and_then(|bytes| usize::try_from(bytes).ok()),
        bundle_processor_cache_timeout_secs: matches!(
            portable_option(info, "streaming", "streaming"),
            Some(Kind::BoolValue(true))
        )
        .then_some(STREAMING_PROCESSOR_IDLE_TIMEOUT.as_secs()),
        ..WorkerOptions::default()
    }
}

/// `target=level` entries from a map of target to level.
fn level_overrides(value: &Kind) -> Vec<String> {
    match value {
        Kind::StructValue(map) => map
            .fields
            .iter()
            .filter_map(|(target, level)| match &level.kind {
                Some(Kind::StringValue(level)) => {
                    Some(format!("{target}={}", portable_level(level)))
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// A level name of another SDK as a level that this SDK parses.
fn portable_level(level: &str) -> String {
    match level.trim().to_ascii_lowercase().as_str() {
        "warning" => "warn".to_string(),
        "critical" | "fatal" => "error".to_string(),
        other => other.to_string(),
    }
}

/// The artifact endpoint: the `--artifact_endpoint` flag, else the provision info's.
pub fn artifact_endpoint(args: &HarnessOptions, info: &ProvisionInfo) -> Option<String> {
    args.artifact_endpoint
        .clone()
        .or_else(|| info.artifact_endpoint.as_ref().map(|e| e.url.clone()))
}

/// The options the pipeline binary is re-executed with: `args`, with unset endpoints taken
/// from the provision info and `options_file` set. `options_file` is `None` when the job
/// carries no Rust options snapshot, because a driver of another SDK submitted it.
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

/// The capability URNs that the runner declares in `ProvisionInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunnerCapabilities {
    urns: HashSet<String>,
}

impl RunnerCapabilities {
    /// The capabilities that boot passes in [`RUNNER_CAPABILITIES_ENV`], or `None` when the
    /// variable is unset.
    pub fn from_env() -> Option<Self> {
        std::env::var(RUNNER_CAPABILITIES_ENV)
            .ok()
            .and_then(|list| list.parse().ok())
    }

    /// Whether the runner declares `urn`.
    pub fn contains(&self, urn: &str) -> bool {
        self.urns.contains(urn)
    }

    /// Returns the number of capability URNs.
    pub fn len(&self) -> usize {
        self.urns.len()
    }

    /// Returns true if the set contains no capability URNs.
    pub fn is_empty(&self) -> bool {
        self.urns.is_empty()
    }
}

impl From<&ProvisionInfo> for RunnerCapabilities {
    fn from(info: &ProvisionInfo) -> Self {
        Self {
            urns: info.runner_capabilities.iter().cloned().collect(),
        }
    }
}

/// The comma-separated form that boot writes to [`RUNNER_CAPABILITIES_ENV`].
impl std::fmt::Display for RunnerCapabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut urns: Vec<&str> = self.urns.iter().map(String::as_str).collect();
        urns.sort_unstable();
        f.write_str(&urns.join(","))
    }
}

/// Reads the comma-separated form. Empty entries are skipped.
impl std::str::FromStr for RunnerCapabilities {
    type Err = std::convert::Infallible;

    fn from_str(list: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            urns: list
                .split(',')
                .map(str::trim)
                .filter(|urn| !urn.is_empty())
                .map(str::to_string)
                .collect(),
        })
    }
}

/// The environment variable in which boot passes the runner capabilities to the worker.
pub const RUNNER_CAPABILITIES_ENV: &str = "BEAM_RUNNER_CAPABILITIES";
