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

//! Multi-output expansion: declared output tags, [`ExternalOutputs`], and verification of
//! the declaration against the expansion service's response.

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures unwrap; a failure is a test failure"
)]

use std::collections::HashMap;

use beam::coders::URN_ROW;
use beam::pipeline::Pipeline;
use external::*;
use model::expansion::expansion_service_server::{ExpansionService, ExpansionServiceServer};
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use model::pipeline as proto;
use tonic::{Request, Response, Status};

/// An expansion service whose every transform produces the fixed set of `tags`, the way
/// a Kafka read with error handling produces `output` plus its error tag.
struct MultiOutputService {
    tags: Vec<&'static str>,
}

#[tonic::async_trait]
impl ExpansionService for MultiOutputService {
    async fn expand(
        &self,
        request: Request<ExpansionRequest>,
    ) -> Result<Response<ExpansionResponse>, Status> {
        let req = request.into_inner();
        let transform = req.transform.expect("transform");
        let coder_id = format!("{}_coder", req.namespace);

        let mut components = req.components.unwrap_or_default();
        components.coders.insert(
            coder_id.clone(),
            proto::Coder {
                spec: Some(proto::FunctionSpec {
                    urn: URN_ROW.to_string(),
                    payload: Vec::new(),
                }),
                component_coder_ids: Vec::new(),
            },
        );

        let outputs: HashMap<String, String> = self
            .tags
            .iter()
            .map(|tag| (tag.to_string(), format!("{}_{tag}", req.namespace)))
            .collect();
        components.pcollections.extend(outputs.values().map(|id| {
            (
                id.clone(),
                proto::PCollection {
                    unique_name: id.clone(),
                    coder_id: coder_id.clone(),
                    is_bounded: proto::is_bounded::Enum::Bounded as i32,
                    windowing_strategy_id: "ws_global_default".to_string(),
                    display_data: Vec::new(),
                },
            )
        }));

        let expanded = proto::PTransform {
            unique_name: transform.unique_name,
            spec: transform.spec,
            subtransforms: Vec::new(),
            inputs: transform.inputs,
            outputs,
            display_data: Vec::new(),
            environment_id: "env_mock".to_string(),
            annotations: HashMap::new(),
        };

        Ok(Response::new(ExpansionResponse {
            components: Some(components),
            transform: Some(expanded),
            requirements: Vec::new(),
            error: String::new(),
        }))
    }

    async fn discover_schema_transform(
        &self,
        _request: Request<DiscoverSchemaTransformRequest>,
    ) -> Result<Response<DiscoverSchemaTransformResponse>, Status> {
        Ok(Response::new(DiscoverSchemaTransformResponse::default()))
    }
}

/// Starts a [`MultiOutputService`] on an ephemeral port, returning its endpoint and a
/// shutdown handle.
fn start_service(tags: Vec<&'static str>) -> (String, tokio::sync::oneshot::Sender<()>) {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            tonic::transport::Server::builder()
                .add_service(ExpansionServiceServer::new(MultiOutputService { tags }))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .unwrap();
        });
    });

    (format!("http://{}", addr_rx.recv().unwrap()), shutdown_tx)
}

fn transform(name: &str, endpoint: &str) -> ExternalTransform {
    ExternalTransform::new(name, URN_EXPANSION_SCHEMA_TRANSFORM, endpoint, Vec::new())
}

/// A placeholder pipeline with one row-typed input, for exercising sinks offline.
fn placeholder_input(pipeline: &Pipeline) -> beam::values::PCollection<beam::schema::Row> {
    pipeline.begin().apply(ExternalSource::new(
        transform("Input", "http://127.0.0.1:1").with_namespace("ns_input"),
    ))
}

#[test]
fn placeholder_source_emits_a_pcollection_per_declared_tag() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);

    let outputs = pipeline.begin().apply(
        ExternalSource::new(
            transform("Read", "http://127.0.0.1:1")
                .with_namespace("ns_read")
                .with_output_tags(["output", "errors"]),
        )
        .all_outputs(),
    );

    assert_eq!(outputs.tags().collect::<Vec<_>>(), ["errors", "output"]);
    // The main output keeps the ID without a suffix, so existing worker graphs do not change.
    assert_eq!(outputs.expect("output").unwrap().id(), "ns_read_out");
    assert_eq!(outputs.expect("errors").unwrap().id(), "ns_read_out_errors");

    let lock = pipeline.lock();
    let errors = lock
        .components
        .pcollections
        .get("ns_read_out_errors")
        .expect("placeholder error output recorded");
    assert_eq!(errors.coder_id, UNEXPANDED_PLACEHOLDER_ID);
    assert_eq!(errors.unique_name, "ns_read.out_errors");
}

#[test]
fn placeholder_sink_exposes_declared_outputs() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let input = placeholder_input(&pipeline);

    let outputs = input.apply(
        ExternalSink::new(
            transform("Write", "http://127.0.0.1:1")
                .with_namespace("ns_write")
                .with_output_tags(["snapshots"]),
        )
        .all_outputs(),
    );

    assert_eq!(
        outputs.expect("snapshots").unwrap().id(),
        "ns_write_out_snapshots"
    );
    let lock = pipeline.lock();
    let write = lock.components.transforms.get("ns_write").unwrap();
    assert_eq!(
        write.inputs.get("input").map(String::as_str),
        Some("ns_input_out")
    );
    assert!(write.outputs.contains_key("snapshots"));
}

#[test]
fn placeholder_without_declared_tags_is_unchanged() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);

    let outputs = pipeline.begin().apply(
        ExternalSource::new(transform("Read", "http://127.0.0.1:1").with_namespace("ns_plain"))
            .all_outputs(),
    );

    assert_eq!(outputs.tags().collect::<Vec<_>>(), ["output"]);
    assert_eq!(outputs.expect("output").unwrap().id(), "ns_plain_out");
}

#[test]
fn remote_source_returns_every_output_with_its_coder() {
    let (endpoint, shutdown) = start_service(vec!["output", "errors"]);
    let pipeline = Pipeline::new();

    let outputs = ExternalSource::new(
        transform("KafkaRead", &endpoint)
            .with_namespace("ns_kafka")
            .with_output_tags(["errors"]),
    )
    .try_expand_all(&pipeline.begin())
    .unwrap();

    assert_eq!(outputs.len(), 2);
    let errors = outputs.expect("errors").unwrap();
    assert_eq!(errors.id(), "ns_kafka_errors");
    assert_eq!(errors.coder_id(), "ns_kafka_coder");

    let _ = shutdown.send(());
}

#[test]
fn remote_source_main_output_still_resolves_with_extra_outputs() {
    let (endpoint, shutdown) = start_service(vec!["errors", "output"]);
    let pipeline = Pipeline::new();

    let main = ExternalSource::new(transform("KafkaRead", &endpoint).with_namespace("ns_main"))
        .try_expand(&pipeline.begin())
        .unwrap();

    assert_eq!(main.id(), "ns_main_output");
    let _ = shutdown.send(());
}

#[test]
fn remote_sink_returns_its_outputs() {
    let (endpoint, shutdown) = start_service(vec!["snapshots"]);
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let input = placeholder_input(&pipeline);
    pipeline.lock().expansion_mode = ExpansionMode::Remote;

    let outputs = ExternalSink::new(
        transform("IcebergWrite", &endpoint)
            .with_namespace("ns_iceberg")
            .with_output_tags(["snapshots"]),
    )
    .try_expand_all(&input)
    .unwrap();

    assert_eq!(outputs.tags().collect::<Vec<_>>(), ["snapshots"]);
    assert_eq!(
        outputs.expect("snapshots").unwrap().id(),
        "ns_iceberg_snapshots"
    );
    let _ = shutdown.send(());
}

#[test]
fn remote_expansion_rejects_a_missing_declared_tag() {
    let (endpoint, shutdown) = start_service(vec!["output"]);
    let pipeline = Pipeline::new();

    let err = ExternalSource::new(
        transform("KafkaRead", &endpoint)
            .with_namespace("ns_typo")
            .with_output_tags(["erorrs"]),
    )
    .try_expand_all(&pipeline.begin())
    .expect_err("a declared tag the service does not produce must fail expansion");

    let message = err.to_string();
    assert!(
        message.contains("erorrs"),
        "names the missing tag: {message}"
    );
    assert!(
        message.contains("output"),
        "names the produced tags: {message}"
    );
    // Rejected before splicing, so the pipeline carries no half-applied transform.
    assert!(
        !pipeline
            .lock()
            .components
            .transforms
            .contains_key("KafkaRead")
    );

    let _ = shutdown.send(());
}

#[test]
fn expect_on_an_unknown_tag_lists_the_available_ones() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let outputs = pipeline.begin().apply(
        ExternalSource::new(transform("Read", "http://127.0.0.1:1").with_output_tags(["errors"]))
            .all_outputs(),
    );

    assert!(outputs.get("snapshots").is_none());
    let message = outputs.expect("snapshots").unwrap_err().to_string();
    assert!(message.contains("snapshots"), "{message}");
    assert!(message.contains("errors"), "{message}");
    assert!(message.contains("output"), "{message}");
}

#[test]
fn with_output_tags_accumulates_and_dedups() {
    let t = transform("T", "http://127.0.0.1:1")
        .with_output_tags(["a", "b"])
        .with_output_tags(vec!["b".to_string(), "c".to_string()]);

    assert_eq!(t.output_tags.iter().collect::<Vec<_>>(), ["a", "b", "c"]);
}
