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
#![expect(clippy::unwrap_used, reason = "integration test helpers")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::coders::{Coder, Context, IterableCoder, KvCoder, StringUtf8Coder, VarIntCoder};
use beam::internals::ElementSink;
use beam::options::PipelineOptions;
use beam::pipeline::Pipeline;
use beam::runners::{registered_runners, runner_for};
use fluent::prelude::*;
use model::fn_execution::beam_fn_control_server::{BeamFnControl, BeamFnControlServer};
use model::fn_execution::beam_fn_data_server::{BeamFnData, BeamFnDataServer};
use model::fn_execution::beam_fn_external_worker_pool_client::BeamFnExternalWorkerPoolClient;
use model::fn_execution::{
    Elements, GetProcessBundleDescriptorRequest, InstructionRequest, InstructionResponse,
    ProcessBundleDescriptor, StartWorkerRequest, StopWorkerRequest,
};
use model::pipeline::ApiServiceDescriptor;
use prism::worker_pool::WorkerPool;
use prism::{PrismRunner, PrismRunnerOptions};
use testing::passert;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

/// What the fake runner observed on its Fn API control service.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ControlEvent {
    Connected(String),
    Closed(String),
}

/// Minimal runner-side Fn API: accepts control and data streams, never sends instructions,
/// and reports when each worker's control stream opens and closes.
struct FakeFnApi {
    events: tokio::sync::mpsc::UnboundedSender<ControlEvent>,
}

fn worker_id_of<T>(request: &tonic::Request<T>) -> String {
    request
        .metadata()
        .get("worker_id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

#[tonic::async_trait]
impl BeamFnControl for FakeFnApi {
    type ControlStream = ReceiverStream<Result<InstructionRequest, tonic::Status>>;

    async fn control(
        &self,
        request: tonic::Request<tonic::Streaming<InstructionResponse>>,
    ) -> Result<tonic::Response<Self::ControlStream>, tonic::Status> {
        let id = worker_id_of(&request);
        let mut inbound = request.into_inner();
        let events = self.events.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let _ = events.send(ControlEvent::Connected(id.clone()));
        tokio::spawn(async move {
            // Keep the outbound stream open until the worker goes away.
            while let Ok(Some(_)) = inbound.message().await {}
            drop(tx);
            let _ = events.send(ControlEvent::Closed(id));
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn get_process_bundle_descriptor(
        &self,
        _request: tonic::Request<GetProcessBundleDescriptorRequest>,
    ) -> Result<tonic::Response<ProcessBundleDescriptor>, tonic::Status> {
        Err(tonic::Status::unimplemented("fake"))
    }
}

#[tonic::async_trait]
impl BeamFnData for FakeFnApi {
    type DataStream = ReceiverStream<Result<Elements, tonic::Status>>;

    async fn data(
        &self,
        request: tonic::Request<tonic::Streaming<Elements>>,
    ) -> Result<tonic::Response<Self::DataStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            while let Ok(Some(_)) = inbound.message().await {}
            drop(tx);
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

async fn start_fake_fn_api() -> (String, tokio::sync::mpsc::UnboundedReceiver<ControlEvent>) {
    let (events_tx, events_rx) = tokio::sync::mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(BeamFnControlServer::new(FakeFnApi {
                events: events_tx.clone(),
            }))
            .add_service(BeamFnDataServer::new(FakeFnApi { events: events_tx }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (endpoint, events_rx)
}

async fn next_event(rx: &mut tokio::sync::mpsc::UnboundedReceiver<ControlEvent>) -> ControlEvent {
    tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .expect("timed out waiting for a control event")
        .expect("fake Fn API stopped")
}

fn start_request(worker_id: &str, control: &str) -> StartWorkerRequest {
    StartWorkerRequest {
        worker_id: worker_id.to_string(),
        control_endpoint: Some(ApiServiceDescriptor {
            url: control.to_string(),
            authentication: None,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_worker_pool_lifecycle() {
    let (control, mut events) = start_fake_fn_api().await;
    let mut pool = WorkerPool::start(None, Arc::new(std::collections::HashMap::new()))
        .await
        .expect("WorkerPool must start on ephemeral port");
    assert_eq!(pool.endpoint(), format!("127.0.0.1:{}", pool.port()));

    let mut client = BeamFnExternalWorkerPoolClient::connect(format!("http://{}", pool.endpoint()))
        .await
        .expect("pool must serve gRPC");

    // StartWorker boots a harness that registers on the runner's control stream.
    let resp = client
        .start_worker(start_request("w1", &control))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.error, "");
    assert_eq!(
        next_event(&mut events).await,
        ControlEvent::Connected("w1".into())
    );

    // StopWorker tears that harness down, closing its control stream.
    let resp = client
        .stop_worker(StopWorkerRequest {
            worker_id: "w1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.error, "");
    assert_eq!(
        next_event(&mut events).await,
        ControlEvent::Closed("w1".into())
    );

    // Stopping an unknown worker is not an error.
    let resp = client
        .stop_worker(StopWorkerRequest {
            worker_id: "nope".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.error, "");

    // stop() aborts every remaining worker and shuts the pool's server down.
    client
        .start_worker(start_request("w2", &control))
        .await
        .unwrap();
    assert_eq!(
        next_event(&mut events).await,
        ControlEvent::Connected("w2".into())
    );
    drop(client);
    pool.stop().await;
    assert_eq!(
        next_event(&mut events).await,
        ControlEvent::Closed("w2".into())
    );

    let mut refused = false;
    for _ in 0..50 {
        if BeamFnExternalWorkerPoolClient::connect(format!("http://{}", pool.endpoint()))
            .await
            .is_err()
        {
            refused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(refused, "pool still accepting connections after stop()");
}

#[tokio::test]
async fn test_prism_runner_e2e_execution() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Pipeline: Impulse -> Map.
    let p = Pipeline::new();
    let out = p.impulse().map("SampleTransform", |bytes: Vec<u8>| {
        let mut out = bytes;
        out.extend_from_slice(b"_processed");
        out
    });
    // Impulse emits one empty element, so exactly one "_processed" must come out.
    // Do not also register a raw `register_transform_handler` for "SampleTransform". It
    // overrides the closure, appends to the encoded element, and makes the output decode as
    // empty bytes. A check for the DONE state alone does not detect this.
    passert::that("AssertOut", &out).contains_in_any_order(vec![b"_processed".to_vec()]);

    let runner = PrismRunner::with_options(PrismRunnerOptions {
        endpoint: None,
        job_name: Some("prism_test_job".to_string()),
        ..Default::default()
    });

    let result = p
        .run_with_runner(&runner)
        .await
        .expect("Pipeline execution on Prism must succeed");

    assert_eq!(result.state, "DONE");
    assert!(result.job_id.starts_with("job-"), "{}", result.job_id);
}

#[tokio::test]
async fn test_prism_wordcount_pipeline_e2e() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // WordCount DAG with GroupByKey.
    let p = Pipeline::new();
    let _ = p
        .impulse()
        .flat_map("EmitWords", |_: Vec<u8>| {
            vec![
                ("beam".to_string(), 1i64),
                ("rust".to_string(), 1i64),
                ("beam".to_string(), 1i64),
            ]
        })
        .group_by_key("GroupWords")
        .map(
            "FormatCounts",
            |(word, counts): (String, BeamIterable<i64>)| {
                let total: i64 = counts.into_iter().sum();
                format!("{word}: {total}")
            },
        );

    // Register transform handlers for the two stages.
    let kv_coder = KvCoder::new(StringUtf8Coder, VarIntCoder);
    let mut word_beam_1 = Vec::new();
    kv_coder
        .encode(
            &("beam".to_string(), 1i64),
            &mut word_beam_1,
            Context::WholeStream,
        )
        .unwrap();
    let mut word_rust_1 = Vec::new();
    kv_coder
        .encode(
            &("rust".to_string(), 1i64),
            &mut word_rust_1,
            Context::WholeStream,
        )
        .unwrap();
    let mut word_beam_2 = Vec::new();
    kv_coder
        .encode(
            &("beam".to_string(), 1i64),
            &mut word_beam_2,
            Context::WholeStream,
        )
        .unwrap();

    p.register_transform_handler(
        "EmitWords",
        Arc::new(move |_: &[u8], sink: &mut dyn ElementSink| {
            sink.push(word_beam_1.clone())?;
            sink.push(word_rust_1.clone())?;
            sink.push(word_beam_2.clone())
        }),
    );

    let captured_results = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured_results.clone();

    p.register_transform_handler(
        "FormatCounts",
        Arc::new(move |bytes: &[u8], sink: &mut dyn ElementSink| {
            let gbk_coder =
                KvCoder::new(StringUtf8Coder, IterableCoder::<i64, _>::new(VarIntCoder));
            let mut reader = bytes;
            let (key, counts): (String, Vec<i64>) = gbk_coder
                .decode(&mut reader, Context::WholeStream)
                .map_err(|e| format!("Failed to decode GBK output: {e}"))?;
            let total: i64 = counts.into_iter().sum();
            let line = format!("{key}: {total}");

            let mut cap = captured_clone.lock().unwrap();
            cap.push(line.clone());

            let mut out = Vec::new();
            StringUtf8Coder
                .encode(&line, &mut out, Context::WholeStream)
                .map_err(|e| e.to_string())?;
            sink.push(out)
        }),
    );

    let runner = PrismRunner::with_options(PrismRunnerOptions {
        endpoint: None,
        job_name: Some("prism_wordcount_job".to_string()),
        ..Default::default()
    });

    let result = p
        .run_with_runner(&runner)
        .await
        .expect("Multi-stage WordCount execution on Prism must succeed");

    assert_eq!(result.state, "DONE");
    assert!(!result.job_id.is_empty());

    let mut final_counts = captured_results.lock().unwrap().clone();
    final_counts.sort();
    assert_eq!(
        final_counts,
        vec!["beam: 2".to_string(), "rust: 1".to_string()]
    );
}

#[test]
fn test_prism_runner_options_from_pipeline_options() {
    let opts = PipelineOptions::parse_from([
        "app",
        "--runner=prism",
        "--endpoint=http://127.0.0.1:8073",
        "--job_name=test-modular-job",
        "--environment_type=DOCKER",
        "--environment_config=apache/beam_rust_sdk:custom_tag",
    ]);

    let prism_opts = PrismRunnerOptions::from(&opts);
    assert_eq!(
        prism_opts.endpoint.as_deref(),
        Some("http://127.0.0.1:8073")
    );
    assert_eq!(prism_opts.job_name.as_deref(), Some("test-modular-job"));
    assert!(prism_opts.portable.is_docker());
    assert_eq!(
        prism_opts.portable.container_image("fallback:latest"),
        "apache/beam_rust_sdk:custom_tag"
    );

    let runner = PrismRunner::from(&opts);
    assert_eq!(
        runner.options().endpoint.as_deref(),
        Some("http://127.0.0.1:8073")
    );
    assert_eq!(
        runner.options().job_name.as_deref(),
        Some("test-modular-job")
    );
    assert!(runner.options().portable.is_docker());
}

#[test]
fn test_prism_runner_inventory_resolution() {
    let runners = registered_runners();
    assert!(
        runners.contains(&"prism"),
        "registered_runners must contain 'prism', found: {runners:?}"
    );
    assert!(
        runners.contains(&"prismrunner"),
        "registered_runners must contain 'prismrunner', found: {runners:?}"
    );

    let opts_prism = PipelineOptions::parse_from(["app", "--runner=prism"]);
    assert!(
        runner_for(&opts_prism).is_ok(),
        "runner_for with 'prism' must resolve successfully"
    );

    let opts_prismrunner = PipelineOptions::parse_from(["app", "--runner=prismrunner"]);
    assert!(
        runner_for(&opts_prismrunner).is_ok(),
        "runner_for with 'prismrunner' must resolve successfully"
    );

    // Names are matched case-insensitively, and an unregistered one is rejected.
    let opts_mixed = PipelineOptions::parse_from(["app", "--runner=PrismRunner"]);
    assert!(runner_for(&opts_mixed).is_ok());
    let opts_unknown = PipelineOptions::parse_from(["app", "--runner=no_such_runner"]);
    match runner_for(&opts_unknown) {
        Err(beam::runners::RunnerError::UnknownRunner { name, available }) => {
            assert_eq!(name, "no_such_runner");
            assert!(available.contains(&"prism"), "{available:?}");
        }
        Err(other) => panic!("expected UnknownRunner, got {other:?}"),
        Ok(_) => panic!("unknown runner name resolved"),
    }
}

#[tokio::test]
async fn test_prism_runner_generate_sequence_splittable_dofn() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    let p = Pipeline::new();
    let doubled = p
        .apply(
            GenerateSequence::new("GenerateSequence", 1)
                .with_end(21)
                .with_split_size(5),
        )
        .map("DoubleNumber", |x: i64| x * 2);
    // Every restriction is processed exactly once across splits: no drops, no duplicates.
    passert::that("AssertDoubled", &doubled)
        .contains_in_any_order((1..21).map(|x| x * 2).collect::<Vec<i64>>());

    let runner = PrismRunner::with_options(PrismRunnerOptions {
        endpoint: None,
        job_name: Some("prism_sdf_seq_job".to_string()),
        ..Default::default()
    });

    let result = p
        .run_with_runner(&runner)
        .await
        .expect("GenerateSequence on Prism must succeed");

    assert_eq!(result.state, "DONE");
    assert!(!result.job_id.is_empty());
}
