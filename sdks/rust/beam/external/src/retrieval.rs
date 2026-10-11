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

//! Fetches the dependencies of the environments of an expansion to files on this machine,
//! through the `ArtifactRetrievalService` of the expansion service.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use beam::pipeline::{URN_ARTIFACT_TYPE_FILE, URN_ARTIFACT_TYPE_URL};
use model::job_management::artifact_retrieval_service_client::ArtifactRetrievalServiceClient;
use model::job_management::{GetArtifactRequest, ResolveArtifactsRequest};
use model::pipeline::{ArtifactFilePayload, ArtifactInformation, Components};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tonic::transport::Channel;

use super::artifact::beam_jar_cache_dir;
use super::error::ExpansionError;

/// An artifact that carries its bytes in its payload.
const URN_ARTIFACT_TYPE_EMBEDDED: &str = "beam:artifact:type:embedded:v1";

/// Directory of the fetched artifacts. A file name is the SHA-256 of its content, so the
/// files stay valid after the run and a second fetch of the same bytes reuses the name.
pub(crate) fn fetched_artifacts_dir() -> PathBuf {
    beam_jar_cache_dir().join("resolved")
}

/// Replaces the dependencies of each environment that `known` does not name with local
/// `file` artifacts. `url` and `embedded` artifacts stay as they are. The paths of the
/// service are on its disk, so the bytes of every other artifact come through `GetArtifact`.
pub(crate) async fn fetch_environment_dependencies(
    channel: Channel,
    components: &mut Components,
    known: &HashSet<String>,
    dir: &Path,
) -> Result<(), ExpansionError> {
    let mut client = ArtifactRetrievalServiceClient::new(channel);
    let environments = components
        .environments
        .iter_mut()
        .filter(|(id, environment)| !known.contains(*id) && !environment.dependencies.is_empty())
        .map(|(_, environment)| environment);
    for environment in environments {
        let request = ResolveArtifactsRequest {
            artifacts: std::mem::take(&mut environment.dependencies),
            preferred_urns: Vec::new(),
        };
        let resolved = client
            .resolve_artifacts(request)
            .await
            .map_err(|status| rpc_error("ResolveArtifacts", &status))?
            .into_inner()
            .replacements;
        let mut local = Vec::with_capacity(resolved.len());
        for artifact in resolved {
            local.push(fetch(&mut client, artifact, dir).await?);
        }
        environment.dependencies = local;
    }
    Ok(())
}

/// Returns `artifact` as a local `file` artifact with the same role.
async fn fetch(
    client: &mut ArtifactRetrievalServiceClient<Channel>,
    artifact: ArtifactInformation,
    dir: &Path,
) -> Result<ArtifactInformation, ExpansionError> {
    if matches!(
        artifact.type_urn.as_str(),
        URN_ARTIFACT_TYPE_URL | URN_ARTIFACT_TYPE_EMBEDDED
    ) {
        return Ok(artifact);
    }
    let ArtifactInformation {
        type_urn,
        type_payload,
        role_urn,
        role_payload,
    } = artifact;
    let expected = expected_sha256(&type_urn, &type_payload)?;
    let request = GetArtifactRequest {
        artifact: Some(ArtifactInformation {
            type_urn,
            type_payload,
            role_urn: String::new(),
            role_payload: Vec::new(),
        }),
    };
    let mut stream = client
        .get_artifact(request)
        .await
        .map_err(|status| rpc_error("GetArtifact", &status))?
        .into_inner();

    tokio::fs::create_dir_all(dir).await.map_err(io_error)?;
    // Unique for each fetch, so concurrent fetches never write the same file.
    static FETCH_SEQ: AtomicU64 = AtomicU64::new(0);
    let temp = dir.join(format!(
        "fetch.{}.{}.tmp",
        std::process::id(),
        FETCH_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let written = async {
        let mut file = tokio::fs::File::create(&temp).await.map_err(io_error)?;
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|status| rpc_error("GetArtifact", &status))?
        {
            hasher.update(&chunk.data);
            file.write_all(&chunk.data).await.map_err(io_error)?;
        }
        file.flush().await.map_err(io_error)?;
        let sha256 = hex(&hasher.finalize());
        match expected {
            Some(want) if want != sha256 => Err(ExpansionError::ArtifactRetrieval(format!(
                "GetArtifact: the bytes have SHA-256 {sha256}, the service sent {want}"
            ))),
            Some(_) | None => Ok(sha256),
        }
    }
    .await;
    let sha256 = match written {
        Ok(sha256) => sha256,
        Err(e) => {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(e);
        }
    };
    let path = dir.join(&sha256);
    tokio::fs::rename(&temp, &path).await.map_err(io_error)?;
    Ok(ArtifactInformation {
        type_urn: URN_ARTIFACT_TYPE_FILE.to_string(),
        type_payload: ArtifactFilePayload {
            path: path.to_string_lossy().into_owned(),
            sha256,
        }
        .encode_to_vec(),
        role_urn,
        role_payload,
    })
}

/// The SHA-256 that the service sends for a `file` artifact, if it sends one.
fn expected_sha256(type_urn: &str, type_payload: &[u8]) -> Result<Option<String>, ExpansionError> {
    if type_urn != URN_ARTIFACT_TYPE_FILE {
        return Ok(None);
    }
    let payload = ArtifactFilePayload::decode(type_payload).map_err(|e| {
        ExpansionError::ArtifactRetrieval(format!("cannot decode a file artifact payload: {e}"))
    })?;
    Ok(Some(payload.sha256).filter(|sha256| !sha256.is_empty()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn rpc_error(method: &str, status: &tonic::Status) -> ExpansionError {
    ExpansionError::ArtifactRetrieval(format!(
        "{method}: {:?}: {}",
        status.code(),
        status.message()
    ))
}

fn io_error(error: std::io::Error) -> ExpansionError {
    ExpansionError::ArtifactRetrieval(format!("cannot write the fetched artifact: {error}"))
}
