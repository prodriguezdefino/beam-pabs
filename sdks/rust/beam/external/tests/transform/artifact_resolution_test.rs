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

//! The client fetches the dependencies of the new environments of an expansion from a
//! service endpoint, through the `ArtifactRetrievalService` of that service.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use beam::pipeline::{URN_ARTIFACT_TYPE_DEFERRED, URN_ARTIFACT_TYPE_FILE, URN_ARTIFACT_TYPE_URL};
use beam_external::{ExpansionClient, ExpansionError};
use beam_model::expansion::expansion_service_server::{ExpansionService, ExpansionServiceServer};
use beam_model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use beam_model::job_management::artifact_retrieval_service_server::{
    ArtifactRetrievalService, ArtifactRetrievalServiceServer,
};
use beam_model::job_management::{
    GetArtifactRequest, GetArtifactResponse, ResolveArtifactsRequest, ResolveArtifactsResponse,
};
use beam_model::pipeline::{
    ArtifactFilePayload, ArtifactInformation, Components, Environment, PTransform,
};
use prost::Message;
use tonic::{Request, Response, Status};

const ROLE: &str = "beam:artifact:role:staging_to:v1";
const EMBEDDED: &str = "beam:artifact:type:embedded:v1";
/// The deferred payload for which `GetArtifact` fails.
const BROKEN: &str = "broken";
/// The chunks that `GetArtifact` sends for every other artifact.
const CHUNKS: [&[u8]; 3] = [b"first ", b"second ", b"third"];
/// SHA-256 of the concatenated `CHUNKS`.
const CHUNKS_SHA256: &str = "c26d5c3681c97e53b32d3f11f774e88a150afd174eeb4d0129e9ca9dfeed5932";

struct TempCacheDir(PathBuf);

impl TempCacheDir {
    fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "beam_artifact_resolution_test_{}_{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp cache dir");
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempCacheDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn artifact(type_urn: &str, payload: &str) -> ArtifactInformation {
    ArtifactInformation {
        type_urn: type_urn.to_string(),
        type_payload: payload.as_bytes().to_vec(),
        role_urn: ROLE.to_string(),
        role_payload: b"name.jar".to_vec(),
    }
}

/// A `file` artifact on the disk of the service, with the hash that the service sends.
fn file_artifact(path: &str, sha256: &str) -> ArtifactInformation {
    ArtifactInformation {
        type_payload: ArtifactFilePayload {
            path: path.to_string(),
            sha256: sha256.to_string(),
        }
        .encode_to_vec(),
        ..artifact(URN_ARTIFACT_TYPE_FILE, "")
    }
}

/// Expands every transform into one environment `env` with `dependencies`.
struct Expansion {
    dependencies: Vec<ArtifactInformation>,
}

#[tonic::async_trait]
impl ExpansionService for Expansion {
    async fn expand(
        &self,
        request: Request<ExpansionRequest>,
    ) -> Result<Response<ExpansionResponse>, Status> {
        let environment = Environment {
            urn: "beam:env:docker:v1".to_string(),
            dependencies: self.dependencies.clone(),
            ..Default::default()
        };
        Ok(Response::new(ExpansionResponse {
            components: Some(Components {
                environments: HashMap::from([("env".to_string(), environment)]),
                ..Default::default()
            }),
            transform: request.into_inner().transform,
            requirements: Vec::new(),
            error: String::new(),
        }))
    }

    async fn discover_schema_transform(
        &self,
        _request: Request<DiscoverSchemaTransformRequest>,
    ) -> Result<Response<DiscoverSchemaTransformResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

/// Resolves each artifact to itself, and sends `CHUNKS` as the bytes of each artifact that
/// is not `BROKEN`.
struct Retrieval;

type ArtifactStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<GetArtifactResponse, Status>> + Send>>;

#[tonic::async_trait]
impl ArtifactRetrievalService for Retrieval {
    async fn resolve_artifacts(
        &self,
        request: Request<ResolveArtifactsRequest>,
    ) -> Result<Response<ResolveArtifactsResponse>, Status> {
        Ok(Response::new(ResolveArtifactsResponse {
            replacements: request.into_inner().artifacts,
        }))
    }

    type GetArtifactStream = ArtifactStream;

    async fn get_artifact(
        &self,
        request: Request<GetArtifactRequest>,
    ) -> Result<Response<Self::GetArtifactStream>, Status> {
        let artifact = request.into_inner().artifact.unwrap_or_default();
        if artifact.type_payload == BROKEN.as_bytes() {
            return Err(Status::not_found("no such artifact"));
        }
        let chunks = CHUNKS.map(|data| {
            Ok(GetArtifactResponse {
                data: data.to_vec(),
            })
        });
        Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
    }
}

/// Starts the service on a free port and returns its endpoint.
async fn start(dependencies: Vec<ArtifactInformation>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a free port");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ExpansionServiceServer::new(Expansion { dependencies }))
            .add_service(ArtifactRetrievalServiceServer::new(Retrieval))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    endpoint
}

async fn expand(
    endpoint: &str,
    cache_dir: &Path,
) -> Result<Vec<ArtifactInformation>, ExpansionError> {
    let request = ExpansionRequest {
        transform: Some(PTransform {
            unique_name: "External".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut client = ExpansionClient::new(endpoint).with_cache_dir(cache_dir);
    let response = client.expand(request).await?;
    Ok(response.components.expect("components").environments["env"]
        .dependencies
        .clone())
}

#[tokio::test]
async fn fetched_artifacts_become_local_files_with_their_sha256() {
    let cache = TempCacheDir::new();
    let endpoint = start(vec![
        artifact(URN_ARTIFACT_TYPE_DEFERRED, "jar"),
        file_artifact("/service/disk/other.jar", CHUNKS_SHA256),
        file_artifact("/service/disk/no_hash.jar", ""),
    ])
    .await;
    let dependencies = expand(&endpoint, cache.path()).await.expect("expand");
    assert_eq!(dependencies.len(), 3);
    dependencies.iter().for_each(|dependency| {
        assert_eq!(dependency.type_urn, URN_ARTIFACT_TYPE_FILE);
        assert_eq!(dependency.role_urn, ROLE);
        assert_eq!(dependency.role_payload, b"name.jar");
        let payload =
            ArtifactFilePayload::decode(dependency.type_payload.as_slice()).expect("file payload");
        assert_eq!(payload.sha256, CHUNKS_SHA256);
        assert!(
            Path::new(&payload.path).starts_with(cache.path()),
            "expected cached file in {:?}, got {}",
            cache.path(),
            payload.path
        );
        assert_eq!(
            std::fs::read(&payload.path).expect("fetched file"),
            CHUNKS.concat()
        );
    });
}

#[tokio::test]
async fn url_and_embedded_artifacts_stay_unchanged() {
    let cache = TempCacheDir::new();
    let kept = vec![
        artifact(URN_ARTIFACT_TYPE_URL, "https://example.com/a.jar"),
        artifact(EMBEDDED, "bytes"),
    ];
    let endpoint = start(kept.clone()).await;
    assert_eq!(expand(&endpoint, cache.path()).await.expect("expand"), kept);
}

#[test]
fn deferred_dependencies_are_redeemed_against_the_jar_of_their_service() {
    let environment = |dependencies| Environment {
        dependencies,
        ..Default::default()
    };
    let mut components = Components {
        environments: HashMap::from([
            (
                "java".to_string(),
                environment(vec![
                    artifact(URN_ARTIFACT_TYPE_DEFERRED, "token"),
                    artifact(URN_ARTIFACT_TYPE_URL, "https://example.com/a.jar"),
                ]),
            ),
            (
                "other".to_string(),
                environment(vec![artifact(URN_ARTIFACT_TYPE_DEFERRED, "token")]),
            ),
        ]),
        ..Default::default()
    };

    beam_external::redeem_deferred_dependencies(&mut components, Path::new("/jars/io.jar"));

    let redeemed: Vec<&ArtifactInformation> = components
        .environments
        .values()
        .flat_map(|environment| &environment.dependencies)
        .filter(|dependency| dependency.type_urn != URN_ARTIFACT_TYPE_URL)
        .collect();
    assert_eq!(redeemed.len(), 2);
    for dependency in redeemed {
        assert_eq!(dependency.type_urn, URN_ARTIFACT_TYPE_FILE);
        assert_eq!(dependency.role_payload, b"name.jar");
        let payload =
            ArtifactFilePayload::decode(dependency.type_payload.as_slice()).expect("file payload");
        assert_eq!(payload.path, "/jars/io.jar");
    }
}

#[tokio::test]
async fn a_failed_get_artifact_is_an_expansion_error() {
    let cache = TempCacheDir::new();
    let endpoint = start(vec![artifact(URN_ARTIFACT_TYPE_DEFERRED, BROKEN)]).await;
    assert_eq!(
        expand(&endpoint, cache.path()).await,
        Err(ExpansionError::ArtifactRetrieval(
            "GetArtifact: NotFound: no such artifact".to_string()
        ))
    );
}

#[tokio::test]
async fn bytes_that_do_not_match_the_service_sha256_are_an_expansion_error() {
    let cache = TempCacheDir::new();
    let wrong = "0".repeat(64);
    let endpoint = start(vec![file_artifact("/service/disk/bad.jar", &wrong)]).await;
    assert_eq!(
        expand(&endpoint, cache.path()).await,
        Err(ExpansionError::ArtifactRetrieval(format!(
            "GetArtifact: the bytes have SHA-256 {CHUNKS_SHA256}, the service sent {wrong}"
        )))
    );
}

#[tokio::test]
async fn a_file_artifact_already_in_the_cache_is_not_fetched_again() {
    let cache = TempCacheDir::new();
    let cached = cache.path().join(CHUNKS_SHA256);
    // Bytes that differ from the service bytes, so a fetch would replace them.
    std::fs::write(&cached, b"cached").expect("write the cached file");
    let endpoint = start(vec![file_artifact(
        "/service/disk/other.jar",
        CHUNKS_SHA256,
    )])
    .await;

    let dependencies = expand(&endpoint, cache.path()).await.expect("expand");

    let [dependency] = dependencies.as_slice() else {
        panic!("one dependency, got {dependencies:?}");
    };
    let payload =
        ArtifactFilePayload::decode(dependency.type_payload.as_slice()).expect("file payload");
    assert_eq!(Path::new(&payload.path), cached);
    assert_eq!(payload.sha256, CHUNKS_SHA256);
    assert_eq!(dependency.role_payload, b"name.jar");
    assert_eq!(std::fs::read(&cached).expect("cached file"), b"cached");
}
