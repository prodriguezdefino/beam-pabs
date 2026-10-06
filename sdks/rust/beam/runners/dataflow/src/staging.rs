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

//! Staging pipeline model artifacts and worker binaries to Google Cloud Storage.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use thiserror::Error;

use file::filesystem::FileSystem;
use model::pipeline as proto;
use prost::Message as _;
use sha2::{Digest as _, Sha256};

/// Errors that can occur during artifact staging.
#[derive(Error, Debug)]
pub enum StagingError {
    #[error("Invalid GCS URI: {0}")]
    InvalidUri(String),

    #[error("I/O error reading local artifact: {0}")]
    Io(#[from] std::io::Error),

    #[error("GCS upload failed for '{uri}': {source}")]
    Upload {
        uri: String,
        #[source]
        source: std::io::Error,
    },
}

/// Normalizes a base GCS staging location and appends a relative path.
pub fn join_gcs_path(base: &str, relative: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    let rel = relative.trim_start_matches('/');
    format!("{trimmed}/{rel}")
}

/// Computes the SHA-256 digest of `data` and returns it as a 64-character lowercase hex string.
pub fn compute_sha256(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Writes `bytes` to `target_uri`, mapping every failure to [`StagingError::Upload`].
pub(super) fn write_bytes(
    fs: &dyn FileSystem,
    target_uri: &str,
    bytes: &[u8],
) -> Result<(), StagingError> {
    let upload_error = |source| StagingError::Upload {
        uri: target_uri.to_string(),
        source,
    };
    let mut writer = fs.open_write(target_uri).map_err(upload_error)?;
    writer.write_all(bytes).map_err(upload_error)?;
    writer.flush().map_err(upload_error)
}

/// Stages the serialized Pipeline protobuf model to Google Cloud Storage.
///
/// Returns `(model_gcs_uri, sha256_hash)`.
pub fn stage_pipeline_model(
    fs: &dyn FileSystem,
    staging_location: &str,
    job_id: &str,
    model_bytes: &[u8],
) -> Result<(String, String), StagingError> {
    let target_uri = join_gcs_path(
        staging_location,
        &format!("{job_id}/{}", crate::constants::STAGED_MODEL_NAME),
    );
    write_bytes(fs, &target_uri, model_bytes)?;

    let hash = compute_sha256(model_bytes);
    tracing::info!(
        "Staged pipeline model ({len} bytes, sha256: {hash}) to {target_uri}",
        len = model_bytes.len()
    );
    Ok((target_uri, hash))
}

/// Stages a local compiled worker binary to Google Cloud Storage.
///
/// Returns `(worker_gcs_uri, sha256_hash)`.
pub fn stage_worker_binary(
    fs: &dyn FileSystem,
    staging_location: &str,
    job_id: &str,
    binary_path: &Path,
) -> Result<(String, String), StagingError> {
    let bytes = std::fs::read(binary_path)?;
    let target_uri = join_gcs_path(
        staging_location,
        &format!("{job_id}/{}", crate::constants::STAGED_WORKER_NAME),
    );
    write_bytes(fs, &target_uri, &bytes)?;

    let hash = compute_sha256(&bytes);
    tracing::info!(
        "Staged worker binary '{path}' ({len} bytes, sha256: {hash}) to {target_uri}",
        path = binary_path.display(),
        len = bytes.len()
    );
    Ok((target_uri, hash))
}

/// Beam artifact URNs, as used in `Environment.dependencies`, under the short names this
/// module reads best with.
mod artifact_urn {
    pub use beam::pipeline::{
        URN_ARTIFACT_ROLE_STAGING_TO as ROLE_STAGING_TO,
        URN_ARTIFACT_TYPE_DEFERRED as TYPE_DEFERRED, URN_ARTIFACT_TYPE_FILE as TYPE_FILE,
        URN_ARTIFACT_TYPE_URL as TYPE_URL,
    };
}

/// Stages a local file to `{staging_location}/{job_id}/xlang/{staged_name}`.
///
/// Returns `(gcs_uri, sha256_hash)`.
pub fn stage_file_artifact(
    fs: &dyn FileSystem,
    staging_location: &str,
    job_id: &str,
    file_path: &Path,
    staged_name: &str,
) -> Result<(String, String), StagingError> {
    let bytes = std::fs::read(file_path)?;
    let target_uri = join_gcs_path(staging_location, &format!("{job_id}/xlang/{staged_name}"));
    write_bytes(fs, &target_uri, &bytes)?;

    let hash = compute_sha256(&bytes);
    tracing::info!(
        "Staged artifact '{path}' ({len} bytes, sha256: {hash}) to {target_uri}",
        path = file_path.display(),
        len = bytes.len()
    );
    Ok((target_uri, hash))
}

/// Uploads local artifacts to GCS, at most once per path, and accumulates the
/// [`PackageItem`](crate::translate::PackageItem)s that Dataflow's `WorkerPool` needs.
struct ArtifactStager<'a> {
    fs: &'a dyn FileSystem,
    staging_location: &'a str,
    job_id: &'a str,
    staged: HashMap<PathBuf, proto::ArtifactInformation>,
    packages: Vec<crate::translate::PackageItem>,
}

impl<'a> ArtifactStager<'a> {
    fn new(fs: &'a dyn FileSystem, staging_location: &'a str, job_id: &'a str) -> Self {
        Self {
            fs,
            staging_location,
            job_id,
            staged: HashMap::new(),
            packages: Vec::new(),
        }
    }

    /// Stages `path` and returns the `url:v1` dependency that replaces the original.
    fn stage(
        &mut self,
        path: &Path,
        staged_name: String,
    ) -> Result<proto::ArtifactInformation, StagingError> {
        if let Some(existing) = self.staged.get(path) {
            return Ok(existing.clone());
        }

        let (url, sha256) = stage_file_artifact(
            self.fs,
            self.staging_location,
            self.job_id,
            path,
            &staged_name,
        )?;
        self.packages.push(crate::translate::PackageItem {
            name: staged_name.clone(),
            location: url.clone(),
            sha256: Some(sha256.clone()),
        });

        let dependency = proto::ArtifactInformation {
            type_urn: artifact_urn::TYPE_URL.to_string(),
            type_payload: proto::ArtifactUrlPayload { url, sha256 }.encode_to_vec(),
            role_urn: artifact_urn::ROLE_STAGING_TO.to_string(),
            role_payload: proto::ArtifactStagingToRolePayload { staged_name }.encode_to_vec(),
        };
        self.staged.insert(path.to_path_buf(), dependency.clone());
        Ok(dependency)
    }
}

fn default_staged_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into()
}

/// The name a dependency asks to be staged under, falling back to the file's own name.
fn staged_name_for(dependency: &proto::ArtifactInformation, path: &Path) -> String {
    if dependency.role_urn != artifact_urn::ROLE_STAGING_TO {
        return default_staged_name(path);
    }
    proto::ArtifactStagingToRolePayload::decode(dependency.role_payload.as_slice())
        .map(|payload| payload.staged_name)
        .unwrap_or_else(|_| default_staged_name(path))
}

/// Rewrites one dependency into a form a Dataflow worker can fetch.
///
/// `file:v1` dependencies name a path on the driver machine, and `deferred:v1` dependencies
/// name nothing at all — the expansion service expected to supply them later. Both are
/// unusable from a worker, so each is uploaded and replaced by a `url:v1` dependency.
/// Anything already resolvable (`url:v1`) or not recognised is passed through untouched.
fn resolve_dependency(
    stager: &mut ArtifactStager<'_>,
    dependency: &proto::ArtifactInformation,
    expansion_artifacts: &[PathBuf],
) -> Result<proto::ArtifactInformation, StagingError> {
    match dependency.type_urn.as_str() {
        artifact_urn::TYPE_FILE => {
            let Ok(payload) =
                proto::ArtifactFilePayload::decode(dependency.type_payload.as_slice())
            else {
                return Ok(dependency.clone());
            };
            let path = PathBuf::from(&payload.path);
            if !path.is_file() {
                return Ok(dependency.clone());
            }
            let staged_name = staged_name_for(dependency, &path);
            stager.stage(&path, staged_name)
        }
        artifact_urn::TYPE_DEFERRED => {
            let Some(path) = expansion_artifacts.first() else {
                tracing::warn!(
                    "Deferred artifact cannot be staged: the pipeline recorded no local \
                     expansion service artifact"
                );
                return Ok(dependency.clone());
            };
            stager.stage(path, default_staged_name(path))
        }
        _ => Ok(dependency.clone()),
    }
}

/// Stages every cross-language dependency reachable from `pipeline.components.environments`
/// and rewrites it to point at its GCS URI.
///
/// `expansion_artifacts` are the local files the driver's expansion services ran from, as
/// recorded on the pipeline during expansion. A Java environment that ends up with no
/// fetchable dependency is given them explicitly: some expansion services describe their
/// own JAR only as a deferred artifact, or omit it entirely.
///
/// Returns the [`PackageItem`](crate::translate::PackageItem)s to attach to
/// `WorkerPool.packages`.
pub fn stage_and_resolve_environment_artifacts(
    fs: &dyn FileSystem,
    staging_location: &str,
    job_id: &str,
    pipeline: &mut proto::Pipeline,
    expansion_artifacts: &[PathBuf],
) -> Result<Vec<crate::translate::PackageItem>, StagingError> {
    let Some(components) = pipeline.components.as_mut() else {
        return Ok(Vec::new());
    };

    let mut stager = ArtifactStager::new(fs, staging_location, job_id);

    for environment in components.environments.values_mut() {
        let mut resolved = environment
            .dependencies
            .iter()
            .map(|dependency| resolve_dependency(&mut stager, dependency, expansion_artifacts))
            .collect::<Result<Vec<_>, _>>()?;

        let has_fetchable = resolved
            .iter()
            .any(|dependency| dependency.type_urn == artifact_urn::TYPE_URL);
        if !has_fetchable && is_java_environment(environment) {
            let extra = expansion_artifacts
                .iter()
                .map(|path| stager.stage(path, default_staged_name(path)))
                .collect::<Result<Vec<_>, _>>()?;
            resolved.extend(extra);
        }

        environment.dependencies = resolved;
    }

    Ok(stager.packages)
}

/// Whether `environment` runs the Java SDK harness, per its Docker container image.
fn is_java_environment(environment: &proto::Environment) -> bool {
    environment.urn == beam::pipeline::URN_ENV_DOCKER
        && proto::DockerPayload::decode(environment.payload.as_slice())
            .is_ok_and(|payload| payload.container_image.contains("java"))
}
