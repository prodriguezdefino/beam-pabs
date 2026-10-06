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

//! Tests for Prism artifact staging: the reverse artifact retrieval client, driven by an
//! in-process fake `ArtifactStagingService` that plays the runner's side of the protocol.
#![expect(clippy::unwrap_used, reason = "integration test helpers")]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use beam::pipeline::{URN_ARTIFACT_TYPE_DEFERRED, URN_ARTIFACT_TYPE_FILE};
use model::job_management::artifact_request_wrapper::Request as Req;
use model::job_management::artifact_response_wrapper::Response as Resp;
use model::job_management::artifact_staging_service_server::{
    ArtifactStagingService, ArtifactStagingServiceServer,
};
use model::job_management::{
    ArtifactRequestWrapper, ArtifactResponseWrapper, GetArtifactRequest, ResolveArtifactsRequest,
};
use model::pipeline::{ArtifactFilePayload, ArtifactInformation, Components, Environment};
use prism::staging::{
    StagingError, declares_dependencies, localize_deferred_dependencies, stage_artifacts,
};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

/// Plays the runner: sends each scripted request, waits for the complete response to it
/// (one message for Resolve, chunks up to `is_last` for Get), then closes the stream.
struct FakeRunner {
    script: Mutex<Option<Vec<ArtifactRequestWrapper>>>,
    received: Arc<Mutex<Vec<ArtifactResponseWrapper>>>,
}

#[tonic::async_trait]
impl ArtifactStagingService for FakeRunner {
    type ReverseArtifactRetrievalServiceStream =
        ReceiverStream<Result<ArtifactRequestWrapper, tonic::Status>>;

    async fn reverse_artifact_retrieval_service(
        &self,
        request: tonic::Request<tonic::Streaming<ArtifactResponseWrapper>>,
    ) -> Result<tonic::Response<Self::ReverseArtifactRetrievalServiceStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let script = self.script.lock().unwrap().take().unwrap_or_default();
        let received = self.received.clone();
        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            // The first message only identifies the staging session.
            match inbound.message().await {
                Ok(Some(first)) => received.lock().unwrap().push(first),
                _ => return,
            }
            for request in script {
                let is_get = matches!(request.request, Some(Req::GetArtifact(_)));
                if tx.send(Ok(request)).await.is_err() {
                    return;
                }
                loop {
                    let Ok(Some(msg)) = inbound.message().await else {
                        return;
                    };
                    let last = msg.is_last;
                    received.lock().unwrap().push(msg);
                    if !is_get || last {
                        break;
                    }
                }
            }
            // Dropping `tx` closes the request stream: the runner has everything.
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

/// Starts the fake runner and returns its endpoint and the log of client messages.
async fn start_fake(
    script: Vec<ArtifactRequestWrapper>,
) -> (String, Arc<Mutex<Vec<ArtifactResponseWrapper>>>) {
    let received = Arc::new(Mutex::new(Vec::new()));
    let service = ArtifactStagingServiceServer::new(FakeRunner {
        script: Mutex::new(Some(script)),
        received: received.clone(),
    })
    .max_decoding_message_size(64 << 20);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (endpoint, received)
}

fn file_artifact(path: &str) -> ArtifactInformation {
    ArtifactInformation {
        type_urn: URN_ARTIFACT_TYPE_FILE.to_string(),
        type_payload: ArtifactFilePayload {
            path: path.to_string(),
            sha256: String::new(),
        }
        .encode_to_vec(),
        role_urn: "beam:artifact:role:staging_to:v1".to_string(),
        role_payload: vec![7],
    }
}

fn get(artifact: ArtifactInformation) -> ArtifactRequestWrapper {
    ArtifactRequestWrapper {
        request: Some(Req::GetArtifact(GetArtifactRequest {
            artifact: Some(artifact),
        })),
    }
}

fn temp_file(tag: &str, contents: &[u8]) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "beam_prism_staging_test_{tag}_{}",
        std::process::id()
    ));
    std::fs::write(&path, contents).unwrap();
    path
}

fn data_of(msg: &ArtifactResponseWrapper) -> &[u8] {
    match &msg.response {
        Some(Resp::GetArtifactResponse(r)) => &r.data,
        other => panic!("expected GetArtifactResponse, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// stage_artifacts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stage_sends_token_then_echoes_resolve_requests() {
    let artifacts = vec![file_artifact("/a"), file_artifact("/b")];
    let resolve = ArtifactRequestWrapper {
        request: Some(Req::ResolveArtifact(ResolveArtifactsRequest {
            artifacts: artifacts.clone(),
            preferred_urns: vec![URN_ARTIFACT_TYPE_FILE.to_string()],
        })),
    };
    let (endpoint, received) = start_fake(vec![resolve]).await;

    stage_artifacts(&endpoint, "token-123").await.unwrap();

    let received = received.lock().unwrap();
    assert_eq!(received.len(), 2, "{received:?}");
    assert_eq!(received[0].staging_token, "token-123");
    assert_eq!(received[0].response, None);
    match &received[1].response {
        Some(Resp::ResolveArtifactResponse(r)) => assert_eq!(r.replacements, artifacts),
        other => panic!("expected ResolveArtifactResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn stage_streams_file_in_1mib_chunks_with_empty_last_message() {
    const MIB: usize = 1 << 20;
    let contents: Vec<u8> = (0..(2 * MIB + MIB / 2)).map(|i| (i % 251) as u8).collect();
    let path = temp_file("chunks", &contents);
    let (endpoint, received) = start_fake(vec![get(file_artifact(path.to_str().unwrap()))]).await;

    stage_artifacts(&endpoint, "tok").await.unwrap();
    let _ = std::fs::remove_file(&path);

    let received = received.lock().unwrap();
    let chunks = &received[1..];
    let sizes: Vec<usize> = chunks.iter().map(|m| data_of(m).len()).collect();
    assert_eq!(sizes, vec![MIB, MIB, MIB / 2, 0]);
    let last_flags: Vec<bool> = chunks.iter().map(|m| m.is_last).collect();
    assert_eq!(last_flags, vec![false, false, false, true]);
    let reassembled: Vec<u8> = chunks.iter().flat_map(|m| data_of(m).to_vec()).collect();
    assert_eq!(reassembled, contents);
}

#[tokio::test]
async fn stage_serves_multiple_requests_in_order() {
    let a = temp_file("multi_a", b"alpha");
    let b = temp_file("multi_b", b"");
    let (endpoint, received) = start_fake(vec![
        get(file_artifact(a.to_str().unwrap())),
        get(file_artifact(b.to_str().unwrap())),
    ])
    .await;

    stage_artifacts(&endpoint, "tok").await.unwrap();
    let _ = std::fs::remove_file(&a);
    let _ = std::fs::remove_file(&b);

    let received = received.lock().unwrap();
    let summary: Vec<(Vec<u8>, bool)> = received[1..]
        .iter()
        .map(|m| (data_of(m).to_vec(), m.is_last))
        .collect();
    // An empty file still gets its terminating is_last message.
    assert_eq!(
        summary,
        vec![
            (b"alpha".to_vec(), false),
            (Vec::new(), true),
            (Vec::new(), true),
        ]
    );
}

#[tokio::test]
async fn stage_reports_bad_artifacts() {
    let missing = std::env::temp_dir().join("beam_prism_staging_test_definitely_missing");
    let missing_str = missing.to_str().unwrap().to_string();

    #[derive(Debug)]
    enum ExpectedError {
        UnsupportedType(&'static str),
        ReadMissing(String),
        MalformedPayload,
    }

    let cases = vec![
        (
            ArtifactInformation {
                type_urn: "beam:artifact:type:url:v1".to_string(),
                ..Default::default()
            },
            ExpectedError::UnsupportedType("beam:artifact:type:url:v1"),
        ),
        (
            file_artifact(&missing_str),
            ExpectedError::ReadMissing(missing_str.clone()),
        ),
        (
            ArtifactInformation {
                type_urn: URN_ARTIFACT_TYPE_FILE.to_string(),
                type_payload: vec![0xff, 0xff, 0xff],
                ..Default::default()
            },
            ExpectedError::MalformedPayload,
        ),
    ];

    for (artifact, expected) in cases {
        let (endpoint, _) = start_fake(vec![get(artifact)]).await;
        let err = stage_artifacts(&endpoint, "tok").await.unwrap_err();
        match (expected, err) {
            (ExpectedError::UnsupportedType(expected_urn), StagingError::UnsupportedType(urn)) => {
                assert_eq!(urn, expected_urn);
            }
            (ExpectedError::ReadMissing(expected_path), StagingError::Read { path, source }) => {
                assert_eq!(path, expected_path);
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            (ExpectedError::MalformedPayload, StagingError::MalformedPayload(_)) => {}
            (expected, other) => panic!("unexpected error {other:?} for case {expected:?}"),
        }
    }
}

#[tokio::test]
async fn stage_reports_dial_failure() {
    // Reserve and release a port so nothing listens on it.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let err = stage_artifacts(&format!("127.0.0.1:{port}"), "tok")
        .await
        .unwrap_err();
    assert!(matches!(err, StagingError::Dial(_)), "{err:?}");
    assert!(err.to_string().contains(&port.to_string()), "{err}");
}

// ---------------------------------------------------------------------------
// localize_deferred_dependencies / declares_dependencies
// ---------------------------------------------------------------------------

fn deferred(role: &str) -> ArtifactInformation {
    ArtifactInformation {
        type_urn: URN_ARTIFACT_TYPE_DEFERRED.to_string(),
        type_payload: b"opaque-token".to_vec(),
        role_urn: role.to_string(),
        role_payload: vec![1, 2],
    }
}

fn components_with(deps: Vec<(&str, Vec<ArtifactInformation>)>) -> Components {
    Components {
        environments: deps
            .into_iter()
            .map(|(id, dependencies)| {
                (
                    id.to_string(),
                    Environment {
                        dependencies,
                        ..Default::default()
                    },
                )
            })
            .collect(),
        ..Default::default()
    }
}

#[test]
fn localize_rewrites_every_deferred_dependency_to_first_artifact() {
    let untouched = file_artifact("/already/a/file");
    let mut c = components_with(vec![
        ("java1", vec![deferred("role:jar"), untouched.clone()]),
        ("java2", vec![deferred("role:other")]),
    ]);
    let artifacts = vec![
        PathBuf::from("/cache/expansion.jar"),
        PathBuf::from("/cache/second.jar"),
    ];

    localize_deferred_dependencies(&mut c, &artifacts).unwrap();

    let expected_payload = ArtifactFilePayload {
        path: "/cache/expansion.jar".to_string(),
        sha256: String::new(),
    }
    .encode_to_vec();
    let java1 = &c.environments["java1"].dependencies;
    assert_eq!(
        java1[0],
        ArtifactInformation {
            type_urn: URN_ARTIFACT_TYPE_FILE.to_string(),
            type_payload: expected_payload.clone(),
            role_urn: "role:jar".to_string(),
            role_payload: vec![1, 2],
        }
    );
    assert_eq!(java1[1], untouched);
    let java2 = &c.environments["java2"].dependencies[0];
    assert_eq!(java2.type_urn, URN_ARTIFACT_TYPE_FILE);
    assert_eq!(java2.type_payload, expected_payload);
    assert_eq!(java2.role_urn, "role:other");
}

#[test]
fn localize_without_artifacts_fails_only_if_something_is_deferred() {
    let mut c = components_with(vec![("env", vec![deferred("role")])]);
    let err = localize_deferred_dependencies(&mut c, &[]).unwrap_err();
    assert!(matches!(err, StagingError::UnredeemableDeferred), "{err:?}");

    let mut c = components_with(vec![("env", vec![file_artifact("/x")])]);
    let before = c.clone();
    localize_deferred_dependencies(&mut c, &[]).unwrap();
    assert_eq!(c, before);
}

#[test]
fn declares_dependencies_detects_any_environment_with_deps() {
    assert!(!declares_dependencies(&Components::default()));
    assert!(!declares_dependencies(&components_with(vec![
        ("a", vec![]),
        ("b", vec![])
    ])));
    assert!(declares_dependencies(&components_with(vec![
        ("a", vec![]),
        ("b", vec![file_artifact("/x")]),
    ])));
}
