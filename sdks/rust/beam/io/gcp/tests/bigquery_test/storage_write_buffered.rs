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

//! The offsets that the exactly-once writer of an unbounded input keeps for each shard, and
//! the retire of a stream against a local fake of the BigQuery Write service.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow_io::arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use beam::coders::DefaultCoder;
use beam::prelude::{Create, Pipeline};
use beam::schema::{FieldType, Schema};
use bytes::Bytes;
use gcp::bigquery::storage::write::buffered::{StreamState, WriteBufferedFn};
use gcp::bigquery::storage::write::encode::{Encoder, schema_bytes};
use gcp::bigquery::storage::write::error_schema;
use gcp::bigquery::storage::write::projection::{FieldSelection, Selection};
use gcp::bigquery::storage::write::table::Destination;
use gcp::runtime::{block_on_async, client_runtime};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_bigquery::client::Write;
use tonic::codegen::{BoxFuture, http};
use tonic::server::{Grpc, NamedService, UnaryService};
use tonic::{Code, Status};
use tonic_prost::ProstCodec;

const TABLE: &str = "projects/p/datasets/d/tables/t";
const STREAM: &str = "projects/p/datasets/d/tables/t/streams/s";
const SERVICE: &str = "google.cloud.bigquery.storage.v1.BigQueryWrite";

fn state(next_offset: i64, flushed: i64) -> StreamState {
    StreamState {
        next_offset,
        flushed,
        ..StreamState::new(STREAM.to_string())
    }
}

#[test]
fn a_new_stream_has_no_row_to_flush() {
    let state = StreamState::new(STREAM.to_string());
    assert_eq!((state.next_offset, state.flushed), (0, -1));
    assert!(!state.flush_pending);
    assert_eq!(state.unflushed(), None);
}

#[test]
fn unflushed_is_the_last_written_offset_after_the_flushed_one() {
    let cases = [
        // Ten rows at offsets 0..=9, none flushed.
        ((10, -1), Some(9)),
        // A flush made rows up to 3 visible.
        ((10, 3), Some(9)),
        // One row after the flushed rows.
        ((10, 8), Some(9)),
        // Every written row is flushed.
        ((10, 9), None),
        // An earlier flush went past the written rows, as after a retry.
        ((10, 12), None),
    ];
    for ((next_offset, flushed), expected) in cases {
        assert_eq!(
            state(next_offset, flushed).unflushed(),
            expected,
            "next_offset {next_offset}, flushed {flushed}"
        );
    }
}

#[test]
fn the_state_coder_keeps_every_field() {
    let state = StreamState {
        flush_pending: true,
        ..state(1 << 40, (1 << 40) - 7)
    };
    let mut bytes = Vec::new();
    state.encode_element(&mut bytes).expect("encodes the state");
    let decoded = StreamState::decode_element(&mut bytes.as_slice()).expect("decodes the state");
    assert_eq!(decoded, state);
}

/// A call that the fake service received.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Get(String),
    Flush(String, i64),
    Finalize(String),
}

#[derive(Clone, PartialEq, prost::Message)]
struct GetWriteStreamRequest {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WriteStream {
    #[prost(string, tag = "1")]
    name: String,
    /// `WriteStream.Type`: 3 is `BUFFERED`.
    #[prost(int32, tag = "2")]
    r#type: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Int64Value {
    #[prost(int64, tag = "1")]
    value: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct FlushRowsRequest {
    #[prost(string, tag = "1")]
    write_stream: String,
    #[prost(message, optional, tag = "2")]
    offset: Option<Int64Value>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct FlushRowsResponse {
    #[prost(int64, tag = "1")]
    offset: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct FinalizeWriteStreamRequest {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct FinalizeWriteStreamResponse {
    #[prost(int64, tag = "1")]
    row_count: i64,
}

/// The answers of the fake service, and the calls that it received.
struct Answers {
    /// The offset that a flush confirms, or the error of the flush.
    flush: Result<i64, Status>,
    /// The row count after a finalize, or the error of the finalize.
    finalize: Result<i64, Status>,
    calls: Mutex<Vec<Call>>,
}

impl Answers {
    fn record(&self, call: Call) {
        self.calls.lock().expect("no test thread panics").push(call);
    }
}

/// One unary method of the fake service.
struct Method<Req, Resp>(Arc<Answers>, fn(&Answers, Req) -> Result<Resp, Status>);

impl<Req: Send + 'static, Resp: Send + 'static> UnaryService<Req> for Method<Req, Resp> {
    type Response = Resp;
    type Future = BoxFuture<tonic::Response<Resp>, Status>;

    fn call(&mut self, request: tonic::Request<Req>) -> Self::Future {
        let answer = (self.1)(&self.0, request.into_inner()).map(tonic::Response::new);
        Box::pin(async move { answer })
    }
}

/// A fake of the BigQuery Write service with the calls that a retire makes.
#[derive(Clone)]
struct FakeWrite(Arc<Answers>);

impl NamedService for FakeWrite {
    const NAME: &'static str = SERVICE;
}

impl<B> tonic::codegen::Service<http::Request<B>> for FakeWrite
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
        let answers = Arc::clone(&self.0);
        let method = request
            .uri()
            .path()
            .strip_prefix(&format!("/{SERVICE}/"))
            .unwrap_or_default()
            .to_string();
        Box::pin(async move {
            // Each method decodes its own request type, so each takes its own codec.
            let response = match method.as_str() {
                "GetWriteStream" => {
                    let method = Method(answers, get);
                    Grpc::new(ProstCodec::default())
                        .unary(method, request)
                        .await
                }
                "FlushRows" => {
                    let method = Method(answers, flush);
                    Grpc::new(ProstCodec::default())
                        .unary(method, request)
                        .await
                }
                "FinalizeWriteStream" => {
                    let method = Method(answers, finalize);
                    Grpc::new(ProstCodec::default())
                        .unary(method, request)
                        .await
                }
                _ => Status::unimplemented(method).into_http(),
            };
            Ok(response)
        })
    }
}

fn get(answers: &Answers, request: GetWriteStreamRequest) -> Result<WriteStream, Status> {
    answers.record(Call::Get(request.name.clone()));
    Ok(WriteStream {
        name: request.name,
        r#type: 3,
    })
}

fn flush(answers: &Answers, request: FlushRowsRequest) -> Result<FlushRowsResponse, Status> {
    let offset = request.offset.map_or(0, |offset| offset.value);
    answers.record(Call::Flush(request.write_stream, offset));
    answers
        .flush
        .clone()
        .map(|offset| FlushRowsResponse { offset })
}

fn finalize(
    answers: &Answers,
    request: FinalizeWriteStreamRequest,
) -> Result<FinalizeWriteStreamResponse, Status> {
    answers.record(Call::Finalize(request.name));
    answers
        .finalize
        .clone()
        .map(|row_count| FinalizeWriteStreamResponse { row_count })
}

/// Retires `state` against a fake service with the given answers, and returns the result and
/// the calls in their order.
fn retire(
    state: &StreamState,
    flush: Result<i64, Status>,
    finalize: Result<i64, Status>,
) -> (beam::Result, Vec<Call>) {
    let answers = Arc::new(Answers {
        flush,
        finalize,
        calls: Mutex::new(Vec::new()),
    });
    let listener = client_runtime()
        .expect("starts the client runtime")
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("binds a local port");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("the listener has an address")
    );
    let server = client_runtime().expect("starts the client runtime").spawn(
        tonic::transport::Server::builder()
            .add_service(FakeWrite(Arc::clone(&answers)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let client = block_on_async(async move {
        Write::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .map_err(std::io::Error::other)
    })
    .expect("builds a Write client");

    let input = Arc::new(Schema::builder().field("id", FieldType::int64()).build());
    let selection =
        Selection::new(Arc::clone(&input), &FieldSelection::All, false).expect("selects id");
    let errors = Arc::new(error_schema(&input));
    let columns = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int64, true)]);
    let destination = Destination {
        table: TABLE.to_string(),
        arrow_schema: Bytes::from(schema_bytes(&columns).expect("encodes the schema")),
    };
    let encoder =
        Encoder::new(&selection, Arc::clone(&errors), &destination).expect("maps id to a column");
    let pipeline = Pipeline::new();
    let view = pipeline
        .apply("Destination", Create::new([destination]))
        .as_singleton();
    let mut writer = WriteBufferedFn::new(
        view,
        Arc::new(selection),
        errors,
        Some(Arc::new(client)),
        Duration::from_secs(1),
        Duration::from_secs(10),
    );
    let result = writer.retire(&(encoder, TABLE.to_string()), state);
    server.abort();
    let calls = answers.calls.lock().expect("no test thread panics").clone();
    (result, calls)
}

fn stream() -> String {
    STREAM.to_string()
}

#[test]
fn retire_flushes_the_committed_rows_before_it_finalizes() {
    let (result, calls) = retire(&state(10, 3), Ok(9), Ok(10));
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        calls,
        [
            Call::Get(stream()),
            Call::Flush(stream(), 9),
            Call::Finalize(stream())
        ]
    );
}

#[test]
fn retire_of_a_flushed_stream_only_finalizes() {
    let (result, calls) = retire(&state(10, 9), Ok(9), Ok(10));
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(calls, [Call::Get(stream()), Call::Finalize(stream())]);
}

#[test]
fn retire_accepts_rows_of_failed_attempts_after_the_committed_rows() {
    let (result, _) = retire(&state(10, -1), Ok(9), Ok(25));
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn retire_fails_and_does_not_finalize_if_the_flush_stops_early() {
    let (result, calls) = retire(&state(10, 3), Ok(5), Ok(10));
    let error = result.expect_err("a short flush loses rows").to_string();
    assert!(error.contains("stopped at offset 5"), "{error}");
    assert_eq!(calls, [Call::Get(stream()), Call::Flush(stream(), 9)]);
}

#[test]
fn retire_fails_and_does_not_finalize_if_the_flush_finds_no_stream() {
    let (result, calls) = retire(&state(10, 3), Err(Status::not_found("no stream")), Ok(10));
    let error = result.expect_err("the rows are not visible").to_string();
    assert!(error.contains("Cannot flush"), "{error}");
    assert_eq!(calls, [Call::Get(stream()), Call::Flush(stream(), 9)]);
}

#[test]
fn retire_fails_if_the_stream_holds_fewer_rows_than_the_committed_bundles() {
    let (result, calls) = retire(&state(10, 9), Ok(9), Ok(7));
    let error = result.expect_err("committed rows are missing").to_string();
    assert!(
        error.contains("has 7 rows after finalize, but the committed bundles wrote 10"),
        "{error}"
    );
    assert_eq!(calls, [Call::Get(stream()), Call::Finalize(stream())]);
}

#[test]
fn retire_accepts_a_flush_that_an_earlier_flush_went_past() {
    let (result, calls) = retire(
        &state(10, 3),
        Err(Status::already_exists("offset already flushed")),
        Ok(10),
    );
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(calls.last(), Some(&Call::Finalize(stream())));
}

#[test]
fn retire_accepts_a_stream_that_is_already_finalized() {
    let finalized = || Status::new(Code::FailedPrecondition, "Stream is finalized");
    let (result, calls) = retire(&state(10, 3), Err(finalized()), Err(finalized()));
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(calls.len(), 3, "{calls:?}");
}

#[test]
fn retire_fails_on_another_finalize_error() {
    let (result, _) = retire(
        &state(10, 9),
        Ok(9),
        Err(Status::permission_denied("no access")),
    );
    let error = result.expect_err("the finalize failed").to_string();
    assert!(error.contains("Cannot finalize"), "{error}");
}
