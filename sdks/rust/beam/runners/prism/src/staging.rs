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

//! Artifact staging for portable JobServices.
//!
//! Implements the client half of `ArtifactStagingService.ReverseArtifactRetrievalService`:
//! the runner calls back into the submitting process for the bytes of each environment
//! dependency.

use std::path::PathBuf;

use prost::Message;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

use beam::pipeline::{URN_ARTIFACT_TYPE_DEFERRED, URN_ARTIFACT_TYPE_FILE};
use harness::grpc;
use model::job_management::{
    ArtifactResponseWrapper, GetArtifactResponse, ResolveArtifactsResponse,
    artifact_request_wrapper::Request, artifact_response_wrapper::Response,
    artifact_staging_service_client::ArtifactStagingServiceClient,
};
use model::pipeline::{ArtifactFilePayload, Components};

/// Size of each `GetArtifactResponse` body: 1 MiB.
const CHUNK_BYTES: usize = 1 << 20;

/// Bound on queued outbound messages, which paces file reads against the gRPC stream.
const OUTBOUND_QUEUE: usize = 8;

#[derive(Error, Debug)]
pub enum StagingError {
    #[error(transparent)]
    Dial(#[from] grpc::ChannelError),
    #[error("Artifact staging stream failed: {0}")]
    Grpc(Box<tonic::Status>),
    #[error("Failed to read artifact '{path}': {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error(
        "Runner requested an artifact of unsupported type '{0}'; only '{URN_ARTIFACT_TYPE_FILE}' can be served from the submitting process"
    )]
    UnsupportedType(String),
    #[error(
        "An environment depends on a '{URN_ARTIFACT_TYPE_DEFERRED}' artifact, but the pipeline recorded no local expansion service artifact to redeem it with"
    )]
    UnredeemableDeferred,
    #[error("Runner sent a malformed {URN_ARTIFACT_TYPE_FILE} payload: {0}")]
    MalformedPayload(#[from] prost::DecodeError),
    #[error("Artifact staging stream closed before the runner finished requesting artifacts")]
    StreamClosed,
}

impl From<tonic::Status> for StagingError {
    fn from(status: tonic::Status) -> Self {
        Self::Grpc(Box::new(status))
    }
}

/// Whether any environment declares a dependency that this process must serve.
pub fn declares_dependencies(components: &Components) -> bool {
    components
        .environments
        .values()
        .any(|environment| !environment.dependencies.is_empty())
}

/// Rewrites every deferred dependency to the local file it stands for.
///
/// An expansion service can describe its JAR as a deferred token that only it can redeem.
/// Prism echoes the token back, and the foreign boot program cannot use it. The driver
/// launched the service from the JAR, so the token becomes that `file:v1` path. The role
/// stays the same.
pub fn localize_deferred_dependencies(
    components: &mut Components,
    expansion_artifacts: &[PathBuf],
) -> Result<(), StagingError> {
    components
        .environments
        .values_mut()
        .flat_map(|environment| environment.dependencies.iter_mut())
        .filter(|dependency| dependency.type_urn == URN_ARTIFACT_TYPE_DEFERRED)
        .try_for_each(|dependency| {
            let path = expansion_artifacts
                .first()
                .ok_or(StagingError::UnredeemableDeferred)?;
            debug!("Redeeming deferred artifact against {}", path.display());
            dependency.type_urn = URN_ARTIFACT_TYPE_FILE.to_string();
            dependency.type_payload = ArtifactFilePayload {
                path: path.to_string_lossy().into_owned(),
                sha256: String::new(),
            }
            .encode_to_vec();
            Ok(())
        })
}

/// Serves the runner's artifact requests for `staging_token` until the runner closes the
/// request stream.
pub async fn stage_artifacts(endpoint: &str, staging_token: &str) -> Result<(), StagingError> {
    let mut client = ArtifactStagingServiceClient::new(grpc::channel(endpoint).await?)
        .max_decoding_message_size(grpc::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(grpc::MAX_MESSAGE_BYTES);

    let (tx, rx) = mpsc::channel::<ArtifactResponseWrapper>(OUTBOUND_QUEUE);

    // The runner identifies the session from this first message: queue it before the call.
    tx.send(ArtifactResponseWrapper {
        staging_token: staging_token.to_string(),
        ..Default::default()
    })
    .await
    .map_err(|_| StagingError::StreamClosed)?;

    let mut requests = client
        .reverse_artifact_retrieval_service(ReceiverStream::new(rx))
        .await?
        .into_inner();

    while let Some(wrapper) = requests.message().await? {
        match wrapper.request {
            Some(Request::ResolveArtifact(request)) => {
                // Nothing to substitute: this process can serve the declared artifacts.
                send(
                    &tx,
                    ArtifactResponseWrapper {
                        response: Some(Response::ResolveArtifactResponse(
                            ResolveArtifactsResponse {
                                replacements: request.artifacts,
                            },
                        )),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Some(Request::GetArtifact(request)) => {
                let artifact = request.artifact.unwrap_or_default();
                if artifact.type_urn != URN_ARTIFACT_TYPE_FILE {
                    return Err(StagingError::UnsupportedType(artifact.type_urn));
                }
                let payload = ArtifactFilePayload::decode(artifact.type_payload.as_slice())?;
                send_file(&tx, &payload.path).await?;
            }
            None => debug!("Ignoring empty artifact request from the runner"),
        }
    }

    Ok(())
}

/// Streams `path` to the runner and ends the response with an empty last message.
async fn send_file(
    tx: &mpsc::Sender<ArtifactResponseWrapper>,
    path: &str,
) -> Result<(), StagingError> {
    let read_error = |source| StagingError::Read {
        path: path.to_string(),
        source,
    };

    let mut file = tokio::fs::File::open(path).await.map_err(read_error)?;
    let mut buffer = vec![0u8; CHUNK_BYTES];
    let mut sent = 0usize;

    loop {
        let read = file.read(&mut buffer).await.map_err(read_error)?;
        if read == 0 {
            break;
        }
        sent += read;
        send(
            tx,
            ArtifactResponseWrapper {
                response: Some(Response::GetArtifactResponse(GetArtifactResponse {
                    data: buffer[..read].to_vec(),
                })),
                ..Default::default()
            },
        )
        .await?;
    }

    send(
        tx,
        ArtifactResponseWrapper {
            is_last: true,
            response: Some(Response::GetArtifactResponse(GetArtifactResponse::default())),
            ..Default::default()
        },
    )
    .await?;

    info!("Staged artifact {path} ({sent} bytes) to the runner");
    Ok(())
}

async fn send(
    tx: &mpsc::Sender<ArtifactResponseWrapper>,
    message: ArtifactResponseWrapper,
) -> Result<(), StagingError> {
    tx.send(message)
        .await
        .map_err(|_| StagingError::StreamClosed)
}
