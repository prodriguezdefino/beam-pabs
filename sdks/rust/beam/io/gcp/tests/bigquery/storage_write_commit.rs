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

//! The `BatchCommitWriteStreams` call of exactly-once writes against a local fake of the
//! BigQuery Write service: the routing header, the retries and the stream errors.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use beam_io_gcp::bigquery::write::commit::CommitClient;
use beam_io_gcp::runtime::{block_on_async, client_runtime};
use tonic::codegen::{BoxFuture, http};
use tonic::server::{Grpc, NamedService, UnaryService};
use tonic::{Code, Status};
use tonic_prost::ProstCodec;

const TABLE: &str = "projects/p/datasets/d/tables/t";
const STREAM: &str = "projects/p/datasets/d/tables/t/streams/s";
const PATH: &str = "/google.cloud.bigquery.storage.v1.BigQueryWrite/BatchCommitWriteStreams";

/// `StorageError.StorageErrorCode` values.
const STREAM_ALREADY_COMMITTED: i32 = 2;
const STREAM_NOT_FOUND: i32 = 3;

#[derive(Clone, PartialEq, prost::Message)]
struct BatchCommitWriteStreamsRequest {
    #[prost(string, tag = "1")]
    parent: String,
    #[prost(string, repeated, tag = "2")]
    write_streams: Vec<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Timestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StorageError {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    entity: String,
    #[prost(string, tag = "3")]
    error_message: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct BatchCommitWriteStreamsResponse {
    #[prost(message, optional, tag = "1")]
    commit_time: Option<Timestamp>,
    #[prost(message, repeated, tag = "2")]
    stream_errors: Vec<StorageError>,
}

/// A call that the fake service received: the routing header and the request.
#[derive(Clone, Debug, PartialEq)]
struct Call {
    routing: Option<String>,
    request: BatchCommitWriteStreamsRequest,
}

/// The answers of the fake service, one for each call in order, and the calls it received.
#[derive(Default)]
struct Answers {
    answers: Mutex<VecDeque<Result<BatchCommitWriteStreamsResponse, Status>>>,
    calls: Mutex<Vec<Call>>,
}

#[derive(Clone)]
struct FakeCommit(Arc<Answers>);

impl NamedService for FakeCommit {
    const NAME: &'static str = "google.cloud.bigquery.storage.v1.BigQueryWrite";
}

impl UnaryService<BatchCommitWriteStreamsRequest> for FakeCommit {
    type Response = BatchCommitWriteStreamsResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, Status>;

    fn call(&mut self, request: tonic::Request<BatchCommitWriteStreamsRequest>) -> Self::Future {
        let routing = request
            .metadata()
            .get("x-goog-request-params")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let Answers { answers, calls } = self.0.as_ref();
        calls.lock().expect("no test thread panics").push(Call {
            routing,
            request: request.into_inner(),
        });
        let answer = answers
            .lock()
            .expect("no test thread panics")
            .pop_front()
            .unwrap_or_else(|| Err(Status::internal("no answer left")))
            .map(tonic::Response::new);
        Box::pin(async move { answer })
    }
}

impl<B> tonic::codegen::Service<http::Request<B>> for FakeCommit
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Infallible>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let service = self.clone();
        let path = request.uri().path().to_string();
        Box::pin(async move {
            let response = match path.as_str() {
                PATH => {
                    Grpc::new(ProstCodec::default())
                        .unary(service, request)
                        .await
                }
                _ => Status::unimplemented(path).into_http(),
            };
            Ok(response)
        })
    }
}

/// Commits the stream of `TABLE` against a fake service with `answers`, and returns the result
/// and the calls that the service received.
fn commit(
    answers: Vec<Result<BatchCommitWriteStreamsResponse, Status>>,
) -> (std::io::Result<()>, Vec<Call>) {
    let answers = Arc::new(Answers {
        answers: Mutex::new(answers.into()),
        calls: Mutex::default(),
    });
    let runtime = client_runtime().expect("starts the client runtime");
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("binds a local port");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("the listener has an address")
    );
    let server = runtime.spawn(
        tonic::transport::Server::builder()
            .add_service(FakeCommit(Arc::clone(&answers)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let result = block_on_async(async move {
        CommitClient::connect(Some(&endpoint))
            .await?
            .commit_streams(TABLE, vec![STREAM.to_string()])
            .await
    });
    server.abort();
    let calls = answers.calls.lock().expect("no test thread panics").clone();
    (result, calls)
}

fn committed() -> BatchCommitWriteStreamsResponse {
    BatchCommitWriteStreamsResponse {
        commit_time: Some(Timestamp { seconds: 1 }),
        stream_errors: Vec::new(),
    }
}

fn stream_error(code: i32) -> BatchCommitWriteStreamsResponse {
    BatchCommitWriteStreamsResponse {
        commit_time: None,
        stream_errors: vec![StorageError {
            code,
            entity: STREAM.to_string(),
            error_message: "stream error".to_string(),
        }],
    }
}

#[test]
fn a_commit_routes_on_the_percent_encoded_table() {
    let (result, calls) = commit(vec![Ok(committed())]);
    result.expect("commits the stream");
    assert_eq!(
        calls,
        vec![Call {
            routing: Some("parent=projects%2Fp%2Fdatasets%2Fd%2Ftables%2Ft".to_string()),
            request: BatchCommitWriteStreamsRequest {
                parent: TABLE.to_string(),
                write_streams: vec![STREAM.to_string()],
            },
        }]
    );
}

#[test]
fn a_transient_error_is_retried_with_the_same_request() {
    let (result, calls) = commit(vec![
        Err(Status::new(Code::Unavailable, "try later")),
        Ok(committed()),
    ]);
    result.expect("commits the stream on the retry");
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
}

#[test]
fn a_permanent_error_fails_at_once() {
    let (result, calls) = commit(vec![Err(Status::new(Code::PermissionDenied, "denied"))]);
    let error = result.expect_err("a permanent error fails the commit");
    assert!(error.to_string().contains("denied"), "{error}");
    assert_eq!(calls.len(), 1);
}

#[test]
fn streams_that_are_already_committed_count_as_committed() {
    let (result, _) = commit(vec![Ok(stream_error(STREAM_ALREADY_COMMITTED))]);
    result.expect("an earlier call committed the stream");
}

#[test]
fn other_stream_errors_fail_the_commit() {
    let (result, _) = commit(vec![Ok(stream_error(STREAM_NOT_FOUND))]);
    let error = result.expect_err("a missing stream fails the commit");
    assert!(
        error
            .to_string()
            .contains(&format!("{STREAM}: stream error")),
        "{error}"
    );
}
