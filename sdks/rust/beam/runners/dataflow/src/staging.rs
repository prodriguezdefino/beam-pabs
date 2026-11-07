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

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use thiserror::Error;

use beam_io_file::FileSystem;
use beam_model::pipeline as proto;
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

/// Formats a SHA-256 digest as a 64-character lowercase hex string.
fn format_sha256(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Computes the SHA-256 digest of `data` and returns it as a 64-character lowercase hex string.
pub fn compute_sha256(data: &[u8]) -> String {
    format_sha256(&Sha256::digest(data))
}

/// Adapts `Sha256` to [`Write`] so [`std::io::copy`] can stream files through the hasher.
struct HashWriter(Sha256);

impl Write for HashWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Streams `path` through SHA-256 and returns `(hex_digest, byte_len)`.
fn hash_file(path: &Path) -> Result<(String, u64), StagingError> {
    let mut file = File::open(path)?;
    let mut hasher = HashWriter(Sha256::new());
    let len = std::io::copy(&mut file, &mut hasher)?;
    Ok((format_sha256(&hasher.0.finalize()), len))
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

/// Streams `path` to `target_uri`, mapping upload failures to [`StagingError::Upload`].
fn write_file(fs: &dyn FileSystem, target_uri: &str, path: &Path) -> Result<(), StagingError> {
    let mut file = File::open(path)?;
    let upload_error = |source| StagingError::Upload {
        uri: target_uri.to_string(),
        source,
    };
    let mut writer = fs.open_write(target_uri).map_err(upload_error)?;
    std::io::copy(&mut file, &mut writer).map_err(upload_error)?;
    writer.flush().map_err(upload_error)
}

/// Serializes check-and-upload per target URI so parallel callers share one upload.
static IN_FLIGHT_UPLOADS: LazyLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn upload_guard(target_uri: &str) -> Arc<Mutex<()>> {
    let mut guards = IN_FLIGHT_UPLOADS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Arc::clone(
        guards
            .entry(target_uri.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

fn release_upload_guard(target_uri: &str, guard: &Arc<Mutex<()>>) {
    let mut guards = IN_FLIGHT_UPLOADS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if guards
        .get(target_uri)
        .is_some_and(|entry| Arc::ptr_eq(entry, guard) && Arc::strong_count(guard) == 2)
    {
        guards.remove(target_uri);
    }
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
/// The object name holds the SHA-256 of the binary, so jobs that use the same binary share
/// one object. The upload is skipped when an object of the same size is already there.
///
/// Returns `(worker_gcs_uri, sha256_hash)`.
pub fn stage_worker_binary(
    fs: &dyn FileSystem,
    staging_location: &str,
    binary_path: &Path,
) -> Result<(String, String), StagingError> {
    // The upload reads the file again, so a rebuild between the two reads stages other bytes
    // under this hash. Do not rebuild the binary while a job submits.
    let (hash, len) = hash_file(binary_path)?;
    let target_uri = worker_binary_uri(staging_location, &hash);

    let guard = upload_guard(&target_uri);
    let result = {
        let _upload_lock = guard.lock().unwrap_or_else(PoisonError::into_inner);
        // A missing object gives an error; then upload.
        let staged = fs.size(&target_uri).is_ok_and(|size| size == len);
        if staged {
            tracing::info!("Worker binary already staged ({len} bytes) at {target_uri}");
            Ok(())
        } else {
            write_file(fs, &target_uri, binary_path).inspect(|()| {
                tracing::info!(
                    "Staged worker binary '{path}' ({len} bytes, sha256: {hash}) to {target_uri}",
                    path = binary_path.display(),
                );
            })
        }
    };
    release_upload_guard(&target_uri, &guard);
    result?;
    Ok((target_uri, hash))
}

/// The staged location of a worker binary with SHA-256 `hash`.
fn worker_binary_uri(staging_location: &str, hash: &str) -> String {
    join_gcs_path(
        staging_location,
        &format!("{}-{hash}", crate::constants::STAGED_WORKER_NAME),
    )
}

/// Returns `(worker_gcs_uri, sha256_hash)` where [`stage_worker_binary`] stages
/// `binary_path`, without an upload.
pub(crate) fn locate_worker_binary(
    staging_location: &str,
    binary_path: &Path,
) -> Result<(String, String), StagingError> {
    let (hash, _) = hash_file(binary_path)?;
    Ok((worker_binary_uri(staging_location, &hash), hash))
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
    let (hash, len) = hash_file(file_path)?;
    let target_uri = join_gcs_path(staging_location, &format!("{job_id}/xlang/{staged_name}"));
    write_file(fs, &target_uri, file_path)?;

    tracing::info!(
        "Staged artifact '{path}' ({len} bytes, sha256: {hash}) to {target_uri}",
        path = file_path.display(),
    );
    Ok((target_uri, hash))
}

/// Uploads local artifacts to GCS, at most once per path, and accumulates the
/// [`PackageItem`](crate::translate::PackageItem)s that Dataflow's `WorkerPool` needs.
/// Each path gets its own staged name, so two files with the same name do not overwrite
/// each other.
struct ArtifactStager<'a> {
    fs: &'a dyn FileSystem,
    staging_location: &'a str,
    job_id: &'a str,
    staged: HashMap<PathBuf, proto::ArtifactInformation>,
    /// The staged names in use.
    names: HashSet<String>,
    packages: Vec<crate::translate::PackageItem>,
}

impl<'a> ArtifactStager<'a> {
    fn new(fs: &'a dyn FileSystem, staging_location: &'a str, job_id: &'a str) -> Self {
        Self {
            fs,
            staging_location,
            job_id,
            staged: HashMap::new(),
            names: HashSet::new(),
            packages: Vec::new(),
        }
    }

    /// `name` if no other path has it, else `name` with the first free number before its
    /// extension (`beam.jar`, `beam-1.jar`, `beam-2.jar`).
    fn unique_name(&self, name: String) -> String {
        if !self.names.contains(&name) {
            return name;
        }
        let (stem, extension) = match name.rfind('.') {
            Some(dot) if dot > 0 => name.split_at(dot),
            Some(_) | None => (name.as_str(), ""),
        };
        let mut number = 1;
        loop {
            let candidate = format!("{stem}-{number}{extension}");
            if !self.names.contains(&candidate) {
                return candidate;
            }
            number += 1;
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

        let staged_name = self.unique_name(staged_name);
        let (url, sha256) = stage_file_artifact(
            self.fs,
            self.staging_location,
            self.job_id,
            path,
            &staged_name,
        )?;
        self.names.insert(staged_name.clone());
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
        // The expansion client redeems the deferred dependencies of its own services. Stage
        // one only if there is exactly one expansion artifact.
        artifact_urn::TYPE_DEFERRED => match expansion_artifacts {
            [path] => stager.stage(path, default_staged_name(path)),
            [] | [_, _, ..] => {
                tracing::warn!(
                    "Deferred artifact cannot be staged: the pipeline recorded {} local \
                     expansion service artifacts, not one",
                    expansion_artifacts.len()
                );
                Ok(dependency.clone())
            }
        },
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
