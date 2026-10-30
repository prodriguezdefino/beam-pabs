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

//! End-to-end execution tests for [`SampleKeyRangesFn`], [`ReadRangesFn`], [`ReadRows`],
//! [`WriteMutationsFn`], and [`WriteMutations`] against an in-process mock Bigtable gRPC server.
#![expect(clippy::unwrap_used, reason = "mock gRPC test fixtures")]

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::internals::{BundleHandler, DoFnHandler, ElementSink, HandlerContext};
use beam::pipeline::Pipeline;
use beam::schema::{Row, RowElement};
use beam::transforms::sdf::{ProcessContinuation, RestrictionTracker, SplittableDoFn};
use beam::transforms::{Create, DoFn};
use bytes::Bytes;
use gcp::bigtable::RowFilterExt as _;
use gcp::bigtable::proto::mutate_rows_response::Entry as MutateRespEntry;
use gcp::bigtable::proto::read_rows_response::CellChunk;
use gcp::bigtable::proto::read_rows_response::cell_chunk::RowStatus;
use gcp::bigtable::proto::{
    BytesValue, MutateRowsRequest, MutateRowsResponse, ReadRowsRequest, ReadRowsResponse,
    RowFilter, SampleRowKeysRequest, SampleRowKeysResponse, Status as RpcStatus, StringValue,
};
use gcp::bigtable::read::{
    KeyRangeRestriction, KeyRangeTracker, ReadOptions, ReadRangesFn, ReadRows, SampleKeyRangesFn,
    read_error_schema,
};
use gcp::bigtable::target::{ReadTarget, WriteTarget};
use gcp::bigtable::write::{
    MutationInputKind, WriteMutations, WriteMutationsFn, WriteOptions, error_schema,
};
use gcp::bigtable::{BigtableColumn, BigtableMutation, flattened_row_schema, mutation_schema};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tonic::codegen::http::{Request as HttpRequest, Response as HttpResponse};
use tonic::codegen::{Body, BoxFuture, Service};
use tonic::server::{NamedService, ServerStreamingService};
use tonic::{Request, Response, Status};

#[derive(Default)]
struct VecSink(Vec<Vec<u8>>);

impl ElementSink for VecSink {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        self.0.push(element);
        Ok(())
    }
}

/// Keeps the main output and the `errors` output of a `Result` output apart.
#[derive(Default)]
struct TaggedSink {
    output: Vec<Vec<u8>>,
    errors: Vec<Vec<u8>>,
}

impl ElementSink for TaggedSink {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        self.output.push(element);
        Ok(())
    }

    fn push_tagged(&mut self, tag: &str, element: Vec<u8>) -> Result<(), String> {
        match tag {
            "errors" => self.errors.push(element),
            _ => self.output.push(element),
        }
        Ok(())
    }
}

#[derive(Default)]
struct MockBigtableState {
    routing_headers: Mutex<Vec<String>>,
    sample_requests: Mutex<Vec<SampleRowKeysRequest>>,
    read_requests: Mutex<Vec<ReadRowsRequest>>,
    mutate_requests: Mutex<Vec<MutateRowsRequest>>,
    samples: Vec<SampleRowKeysResponse>,
    read_responses: Vec<ReadRowsResponse>,
    /// One script per `ReadRows` call, in call order. After the scripts, calls use
    /// `read_responses`.
    read_scripts: Mutex<VecDeque<Vec<Result<ReadRowsResponse, Status>>>>,
    read_delay: Option<Duration>,
    mutate_calls: AtomicUsize,
}

#[derive(Clone)]
struct MockBigtableService {
    state: Arc<MockBigtableState>,
}

impl NamedService for MockBigtableService {
    const NAME: &'static str = "google.bigtable.v2.Bigtable";
}

type BoxStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>>;

struct ReceiverStream<T>(mpsc::Receiver<Result<T, Status>>);

impl<T> tokio_stream::Stream for ReceiverStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

struct SampleSvc(Arc<MockBigtableState>);
impl ServerStreamingService<SampleRowKeysRequest> for SampleSvc {
    type Response = SampleRowKeysResponse;
    type ResponseStream = BoxStream<SampleRowKeysResponse>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send>>;

    fn call(&mut self, request: Request<SampleRowKeysRequest>) -> Self::Future {
        let state = Arc::clone(&self.0);
        Box::pin(async move {
            if let Some(h) = request.metadata().get("x-goog-request-params") {
                state
                    .routing_headers
                    .lock()
                    .unwrap()
                    .push(h.to_str().unwrap().to_string());
            }
            state
                .sample_requests
                .lock()
                .unwrap()
                .push(request.into_inner());
            let (tx, rx) = mpsc::channel(16);
            for s in state.samples.clone() {
                tx.send(Ok(s)).await.unwrap();
            }
            Ok(Response::new(
                Box::pin(ReceiverStream(rx)) as Self::ResponseStream
            ))
        })
    }
}

struct ReadSvc(Arc<MockBigtableState>);
impl ServerStreamingService<ReadRowsRequest> for ReadSvc {
    type Response = ReadRowsResponse;
    type ResponseStream = BoxStream<ReadRowsResponse>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send>>;

    fn call(&mut self, request: Request<ReadRowsRequest>) -> Self::Future {
        let state = Arc::clone(&self.0);
        Box::pin(async move {
            if let Some(h) = request.metadata().get("x-goog-request-params") {
                state
                    .routing_headers
                    .lock()
                    .unwrap()
                    .push(h.to_str().unwrap().to_string());
            }
            state
                .read_requests
                .lock()
                .unwrap()
                .push(request.into_inner());
            let (tx, rx) = mpsc::channel(16);
            let script = state.read_scripts.lock().unwrap().pop_front();
            let responses =
                script.unwrap_or_else(|| state.read_responses.iter().cloned().map(Ok).collect());
            let delay = state.read_delay;
            tokio::spawn(async move {
                for resp in responses {
                    if let Some(d) = delay {
                        tokio::time::sleep(d).await;
                    }
                    if tx.send(resp).await.is_err() {
                        break;
                    }
                }
            });
            Ok(Response::new(
                Box::pin(ReceiverStream(rx)) as Self::ResponseStream
            ))
        })
    }
}

struct MutateSvc(Arc<MockBigtableState>);
impl ServerStreamingService<MutateRowsRequest> for MutateSvc {
    type Response = MutateRowsResponse;
    type ResponseStream = BoxStream<MutateRowsResponse>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send>>;

    fn call(&mut self, request: Request<MutateRowsRequest>) -> Self::Future {
        let state = Arc::clone(&self.0);
        Box::pin(async move {
            if let Some(h) = request.metadata().get("x-goog-request-params") {
                state
                    .routing_headers
                    .lock()
                    .unwrap()
                    .push(h.to_str().unwrap().to_string());
            }
            let req = request.into_inner();
            let call_num = state.mutate_calls.fetch_add(1, Ordering::SeqCst);
            state.mutate_requests.lock().unwrap().push(req.clone());

            let entries = req
                .entries
                .iter()
                .enumerate()
                .map(|(idx, entry)| {
                    let status = if entry.row_key.as_ref() == b"bad-key" {
                        RpcStatus {
                            code: 3, // INVALID_ARGUMENT (permanent)
                            message: "bad row key".to_string(),
                            ..Default::default()
                        }
                    } else if entry.row_key.as_ref() == b"flaky-key" && call_num == 0 {
                        RpcStatus {
                            code: 14, // UNAVAILABLE (transient on first call)
                            message: "tablet busy".to_string(),
                            ..Default::default()
                        }
                    } else {
                        RpcStatus {
                            code: 0,
                            message: String::new(),
                            ..Default::default()
                        }
                    };
                    MutateRespEntry {
                        index: idx as i64,
                        status: Some(status),
                    }
                })
                .collect();

            let (tx, rx) = mpsc::channel(4);
            tx.send(Ok(MutateRowsResponse {
                entries,
                rate_limit_info: None,
            }))
            .await
            .unwrap();
            Ok(Response::new(
                Box::pin(ReceiverStream(rx)) as Self::ResponseStream
            ))
        })
    }
}

impl<B> Service<HttpRequest<B>> for MockBigtableService
where
    B: Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = HttpResponse<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HttpRequest<B>) -> Self::Future {
        let state = Arc::clone(&self.state);
        match req.uri().path() {
            "/google.bigtable.v2.Bigtable/SampleRowKeys" => {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Box::pin(async move { Ok(grpc.server_streaming(SampleSvc(state), req).await) })
            }
            "/google.bigtable.v2.Bigtable/ReadRows" => {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Box::pin(async move { Ok(grpc.server_streaming(ReadSvc(state), req).await) })
            }
            "/google.bigtable.v2.Bigtable/MutateRows" => {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Box::pin(async move { Ok(grpc.server_streaming(MutateSvc(state), req).await) })
            }
            _ => Box::pin(async move {
                Ok(HttpResponse::builder()
                    .status(404)
                    .body(tonic::body::Body::empty())
                    .unwrap())
            }),
        }
    }
}

fn start_mock_bigtable(state: Arc<MockBigtableState>) -> String {
    let rt = gcp::runtime::client_runtime().unwrap();
    let listener = rt
        .block_on(TcpListener::bind("127.0.0.1:0"))
        .expect("bind local port");
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let svc = MockBigtableService { state };
    rt.spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await;
    });
    format!("http://{addr}")
}

#[test]
fn sample_and_read_materialized_view_with_databoost_and_value_bitmask_filter() {
    let state = Arc::new(MockBigtableState {
        samples: vec![
            SampleRowKeysResponse {
                row_key: Bytes::from_static(b"k5"),
                offset_bytes: 1000,
            },
            SampleRowKeysResponse {
                row_key: Bytes::new(),
                offset_bytes: 2000,
            },
        ],
        read_responses: vec![ReadRowsResponse {
            chunks: vec![CellChunk {
                row_key: Bytes::from_static(b"k1"),
                family_name: Some(StringValue {
                    value: "agg".to_string(),
                }),
                qualifier: Some(BytesValue {
                    value: Bytes::from_static(b"total"),
                }),
                timestamp_micros: 500,
                labels: Vec::new(),
                value: Bytes::from_static(b"99"),
                value_size: 0,
                row_status: Some(RowStatus::CommitRow(true)),
            }],
            last_scanned_row_key: Bytes::new(),
            ..Default::default()
        }],
        ..MockBigtableState::default()
    });
    let endpoint = start_mock_bigtable(Arc::clone(&state));

    let target = ReadTarget::materialized_view("proj", "inst", "daily_mv");
    let bitmask = RowFilter::column_value_bitmask("agg", "total", vec![0x30, 0x30]);
    let options = ReadOptions::new(target.clone())
        .with_app_profile_id("databoost-isolation")
        .with_row_filter(&bitmask)
        .with_endpoint(&endpoint);

    let mut sample_fn = SampleKeyRangesFn {
        target,
        app_profile_id: Some("databoost-isolation".to_string()),
        start_key: Vec::new(),
        end_key: Vec::new(),
        max_initial_bundles: 16,
        sub_split_tablets: true,
        attempt_timeout: Duration::from_secs(5),
        endpoint: Some(endpoint),
        client: None,
    };
    let mut range_sink = VecSink::default();
    {
        let mut hctx = HandlerContext::new(&mut range_sink);
        let mut sample_ctx = hctx.as_process_context::<KeyRangeRestriction>();
        sample_fn
            .process_element(Vec::new(), &mut sample_ctx)
            .unwrap();
    }
    let emitted_ranges: Vec<KeyRangeRestriction> = range_sink
        .0
        .iter()
        .map(|b| KeyRangeRestriction::decode(b).unwrap())
        .collect();
    assert_eq!(emitted_ranges.len(), 2);

    let mut read_fn: ReadRangesFn<RowElement> = ReadRangesFn::new(options, false, None);
    let restriction = KeyRangeRestriction::new(Vec::new(), b"k5".to_vec(), 1000);
    let tracker = read_fn.create_tracker(&restriction);
    let mut row_sink = VecSink::default();
    {
        let mut hctx = HandlerContext::new(&mut row_sink);
        let mut read_ctx = hctx.as_process_context::<RowElement>();
        let cont = read_fn
            .process_element(restriction, &tracker, &mut read_ctx)
            .unwrap();
        assert_eq!(cont, ProcessContinuation::Stop);
    }
    tracker.check_done().unwrap();

    assert_eq!(row_sink.0.len(), 1);
    let decoded_row = Row::from_row_bytes(&flattened_row_schema(), &row_sink.0[0]).unwrap();
    let col = BigtableColumn::try_from(&decoded_row).unwrap();
    assert_eq!(col.key, b"k1");
    assert_eq!(col.family_name, "agg");
    assert_eq!(col.latest_value(), Some(&b"99"[..]));

    // Verify Data Boost + MaterializedView routing headers and proto fields.
    let headers = state.routing_headers.lock().unwrap();
    assert!(
        headers
            .iter()
            .all(|h| h
                == "name=projects%2Fproj%2Finstances%2Finst&app_profile_id=databoost-isolation")
    );
    let read_reqs = state.read_requests.lock().unwrap();
    assert_eq!(
        read_reqs[0].materialized_view_name,
        "projects/proj/instances/inst/materializedViews/daily_mv"
    );
    assert_eq!(read_reqs[0].app_profile_id, "databoost-isolation");
    assert_eq!(read_reqs[0].filter, Some(bitmask));
}

#[test]
fn sparse_scan_advances_progress_on_scanned_heartbeats_and_self_checkpoints() {
    let state = Arc::new(MockBigtableState {
        read_responses: vec![
            ReadRowsResponse {
                chunks: Vec::new(),
                last_scanned_row_key: Bytes::from_static(b"key-0500"),
                ..Default::default()
            },
            ReadRowsResponse {
                chunks: Vec::new(),
                last_scanned_row_key: Bytes::from_static(b"key-0900"),
                ..Default::default()
            },
        ],
        read_delay: Some(Duration::from_millis(15)),
        ..MockBigtableState::default()
    });
    let endpoint = start_mock_bigtable(Arc::clone(&state));

    let options = ReadOptions::new(ReadTarget::table("p", "i", "t"))
        .with_max_silent_scan_duration(Duration::from_millis(5))
        .with_endpoint(&endpoint);

    let mut read_fn: ReadRangesFn<RowElement> = ReadRangesFn::new(options, true, None);
    let restriction = KeyRangeRestriction::new(b"key-0000".to_vec(), b"key-9999".to_vec(), 10_000);
    let tracker = read_fn.create_tracker(&restriction);

    let mut sink = VecSink::default();
    let cont = {
        let mut hctx = HandlerContext::new(&mut sink);
        let mut ctx = hctx.as_process_context::<RowElement>();
        read_fn
            .process_element(restriction, &tracker, &mut ctx)
            .unwrap()
    };

    // Because `max_silent_scan_duration` is 5ms and each heartbeat takes 15ms, the reader
    // claims `"key-0500"`, advances progress, and returns `ProcessContinuation::Resume`.
    assert_eq!(cont, ProcessContinuation::Resume);
    assert_eq!(
        tracker.last_claimed_key().as_deref(),
        Some(&b"key-0500"[..])
    );
    assert!(tracker.current_progress().work_completed > 0.0);

    // Runner checkpoints at 0.0 after `ProcessContinuation::Resume`.
    let (primary, residual) = tracker.try_split(0.0).unwrap();
    assert_eq!(primary.end_key, b"key-0500\0");
    assert_eq!(residual.start_key, b"key-0500\0");
    tracker.check_done().unwrap();
}

fn committed_cell(key: &'static [u8]) -> ReadRowsResponse {
    ReadRowsResponse {
        chunks: vec![CellChunk {
            row_key: Bytes::from_static(key),
            family_name: Some(StringValue {
                value: "cf".to_string(),
            }),
            qualifier: Some(BytesValue {
                value: Bytes::from_static(b"q"),
            }),
            timestamp_micros: 1,
            labels: Vec::new(),
            value: Bytes::from_static(b"v"),
            value_size: 0,
            row_status: Some(RowStatus::CommitRow(true)),
        }],
        last_scanned_row_key: Bytes::new(),
        ..Default::default()
    }
}

/// Runs one `process_element` call of the failures mode and returns its continuation.
fn process_with_failures(
    read_fn: &mut ReadRangesFn<Result<RowElement, Row>>,
    restriction: KeyRangeRestriction,
    tracker: &KeyRangeTracker,
    sink: &mut TaggedSink,
) -> ProcessContinuation {
    let mut hctx = HandlerContext::new(sink);
    let mut ctx = hctx.as_process_context::<Result<RowElement, Row>>();
    read_fn
        .process_element(restriction, tracker, &mut ctx)
        .unwrap()
}

#[test]
fn failures_output_resumes_after_a_permanent_stream_error_and_names_unread_ranges() {
    let state = Arc::new(MockBigtableState {
        read_scripts: Mutex::new(VecDeque::from([
            vec![
                Ok(committed_cell(b"k1")),
                Err(Status::invalid_argument("corrupt tablet")),
            ],
            vec![Err(Status::invalid_argument("corrupt tablet"))],
        ])),
        ..MockBigtableState::default()
    });
    let endpoint = start_mock_bigtable(Arc::clone(&state));
    let options = ReadOptions::new(ReadTarget::table("p", "i", "t")).with_endpoint(&endpoint);
    let mut read_fn: ReadRangesFn<Result<RowElement, Row>> = ReadRangesFn::new(options, true, None);

    // The error after `k1` emits a failure and resumes after `k1`, so the rest is read later.
    let restriction = KeyRangeRestriction::new(b"k0".to_vec(), b"k9".to_vec(), 1000);
    let tracker = read_fn.create_tracker(&restriction);
    let mut sink = TaggedSink::default();
    let cont = process_with_failures(&mut read_fn, restriction, &tracker, &mut sink);
    assert_eq!(cont, ProcessContinuation::Resume);
    assert_eq!(sink.output.len(), 1);
    assert_eq!(sink.errors.len(), 1);
    let err_row = Row::from_row_bytes(read_error_schema(), &sink.errors[0]).unwrap();
    assert_eq!(err_row.get_bytes("row_key").unwrap(), Some(&b"k1"[..]));
    let (_, residual) = tracker.try_split(0.0).unwrap();
    assert_eq!(residual.start_key, b"k1\0");
    assert_eq!(residual.end_key, b"k9");
    tracker.check_done().unwrap();

    // The same error before the first claimed key stops, and the failure names the unread range.
    let tracker = read_fn.create_tracker(&residual);
    let mut sink = TaggedSink::default();
    let cont = process_with_failures(&mut read_fn, residual, &tracker, &mut sink);
    assert_eq!(cont, ProcessContinuation::Stop);
    assert!(sink.output.is_empty());
    assert_eq!(sink.errors.len(), 1);
    let err_row = Row::from_row_bytes(read_error_schema(), &sink.errors[0]).unwrap();
    let msg = err_row.get_string("error_message").unwrap().unwrap();
    assert!(msg.contains("corrupt tablet"), "msg = {msg}");
    assert!(msg.contains(r"[k1\x00, k9) is not read"), "msg = {msg}");
    tracker.check_done().unwrap();
}

#[test]
fn write_mutations_retries_transient_per_row_errors_and_routes_permanent_rejections() {
    let state = Arc::new(MockBigtableState::default());
    let endpoint = start_mock_bigtable(Arc::clone(&state));

    let options = WriteOptions::new(WriteTarget::table("p", "i", "t"))
        .with_app_profile_id("write-profile")
        .with_max_elements_per_batch(2)
        .with_endpoint(&endpoint);

    let in_schema = mutation_schema();
    let err_schema = Arc::new(error_schema(&in_schema));
    let write_fn = WriteMutationsFn::new(
        options,
        MutationInputKind::Flat,
        Arc::clone(&in_schema),
        Arc::clone(&err_schema),
        None,
    );
    let mut handler = DoFnHandler::new(write_fn);

    let rows = [
        Row::from(BigtableMutation::set_cell("ok-key", "cf", "q", "v1")),
        Row::from(BigtableMutation::set_cell("flaky-key", "cf", "q", "v2")),
        Row::from(BigtableMutation::set_cell("bad-key", "cf", "q", "v3")),
    ];

    let mut sink = VecSink::default();
    handler.start_bundle().unwrap();
    for r in rows {
        let mut hctx = HandlerContext::new(&mut sink);
        handler
            .process(&r.to_row_bytes().unwrap(), &mut hctx)
            .unwrap();
    }
    {
        let mut hctx = HandlerContext::new(&mut sink);
        handler.finish_bundle(&mut hctx).unwrap();
    }

    // `flaky-key` was retried and succeeded on the second call; only `bad-key` was emitted as an error Row.
    assert_eq!(sink.0.len(), 1);
    let err_row = Row::from_row_bytes(&err_schema, &sink.0[0]).unwrap();
    let msg = err_row.get_string("error_message").unwrap().unwrap();
    assert!(msg.contains("bad row key"), "msg = {msg}");
    assert!(state.mutate_calls.load(Ordering::SeqCst) >= 2);
}

#[test]
fn pipeline_construction_for_native_read_and_write_transforms() {
    let pipeline = Pipeline::new();
    let read_opts = ReadOptions::new(ReadTarget::table("p", "i", "t"));
    let read_outputs = pipeline
        .try_apply(
            "NativeRead",
            ReadRows::new(read_opts).with_encoded_rows().with_failures(),
        )
        .unwrap();
    assert_eq!(
        read_outputs.output.row_schema().unwrap(),
        flattened_row_schema()
    );

    let mutations = pipeline
        .apply(
            "SeedMutations",
            Create::new(vec![Row::from(BigtableMutation::set_cell(
                "k", "cf", "q", "v",
            ))]),
        )
        .with_row_schema(&mutation_schema());
    let write_opts = WriteOptions::new(WriteTarget::table("p", "i", "t"));
    let write_errors = mutations
        .try_apply("NativeWrite", WriteMutations::new(write_opts))
        .unwrap();
    assert_eq!(
        write_errors.row_schema().unwrap(),
        Arc::new(error_schema(&mutation_schema()))
    );
}
