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

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures unwrap; a failure is a test failure"
)]

use std::collections::HashMap;
use std::sync::Arc;

use beam::coders::{RowCoder, URN_ROW};
use beam::pipeline::{Pipeline, PipelineInner};
use beam::schema::{FieldType, Row, Schema};
use external::payload::*;
use external::splicing::*;
use external::*;
use model::expansion::expansion_service_server::{ExpansionService, ExpansionServiceServer};
use model::expansion::{
    DiscoverSchemaTransformRequest, DiscoverSchemaTransformResponse, ExpansionRequest,
    ExpansionResponse,
};
use model::pipeline as proto;
use prost::Message;
use tonic::{Request, Response, Status};

#[test]
fn test_schema_transform_payload_encoding() {
    let schema = Arc::new(
        Schema::builder()
            .field("table", FieldType::string())
            .field("write_method", FieldType::string())
            .nullable_field("batch_size", FieldType::int32())
            .build(),
    );

    let config = Row::builder(schema.clone())
        .with_value("my-project:dataset.table")
        .with_value("STORAGE_WRITE_API")
        .with_value(500i32)
        .build()
        .unwrap();
    let test_urn = "beam:schematransform:org.apache.beam:test_transform:v1";
    let bytes = encode_schema_transform_payload(test_urn, &config).unwrap();
    let decoded_proto = proto::SchemaTransformPayload::decode(bytes.as_slice()).unwrap();

    assert_eq!(decoded_proto.identifier, test_urn);

    let decoded_schema =
        Schema::try_from(decoded_proto.configuration_schema.expect("schema")).unwrap();
    assert_eq!(&decoded_schema, schema.as_ref());

    let decoded_row =
        RowCoder::decode_row(&schema, &mut decoded_proto.configuration_row.as_slice()).unwrap();
    assert_eq!(decoded_row, config);
}

#[test]
fn test_external_configuration_payload_encoding() {
    let schema = Arc::new(
        Schema::builder()
            .field("query", FieldType::string())
            .build(),
    );
    let config = Row::builder(schema.clone())
        .with_value("SELECT 1")
        .build()
        .unwrap();

    let bytes = encode_external_configuration_payload(&config).unwrap();
    let decoded_proto = proto::ExternalConfigurationPayload::decode(bytes.as_slice()).unwrap();

    let decoded_schema = Schema::try_from(decoded_proto.schema.expect("schema")).unwrap();
    assert_eq!(&decoded_schema, schema.as_ref());

    let decoded_row = RowCoder::decode_row(&schema, &mut decoded_proto.payload.as_slice()).unwrap();
    assert_eq!(decoded_row, config);
}

#[test]
fn test_java_class_lookup_payload_with_constructor_and_builders() {
    let ctor_schema = Arc::new(
        Schema::builder()
            .field("bootstrap_servers", FieldType::string())
            .build(),
    );
    let ctor_args = Row::builder(ctor_schema.clone())
        .with_value("localhost:9092")
        .build()
        .unwrap();

    let builder_schema = Arc::new(
        Schema::builder()
            .field("topic", FieldType::string())
            .build(),
    );
    let builder_args = Row::builder(builder_schema.clone())
        .with_value("events")
        .build()
        .unwrap();

    let bytes = encode_java_class_lookup_payload(
        "org.apache.beam.sdk.io.kafka.KafkaIO",
        Some("read".to_string()),
        Some(&ctor_args),
        vec![JavaBuilderMethodCall::new(
            "withTopic",
            builder_args.clone(),
        )],
    )
    .unwrap();

    let decoded = proto::JavaClassLookupPayload::decode(bytes.as_slice()).unwrap();
    assert_eq!(decoded.class_name, "org.apache.beam.sdk.io.kafka.KafkaIO");
    assert_eq!(decoded.constructor_method, "read");

    let decoded_ctor_schema =
        Schema::try_from(decoded.constructor_schema.expect("constructor schema")).unwrap();
    assert_eq!(&decoded_ctor_schema, ctor_schema.as_ref());
    assert_eq!(
        RowCoder::decode_row(&ctor_schema, &mut decoded.constructor_payload.as_slice()).unwrap(),
        ctor_args
    );

    assert_eq!(decoded.builder_methods.len(), 1);
    let method = &decoded.builder_methods[0];
    assert_eq!(method.name, "withTopic");
    let decoded_builder_schema =
        Schema::try_from(method.schema.clone().expect("builder schema")).unwrap();
    assert_eq!(&decoded_builder_schema, builder_schema.as_ref());
    assert_eq!(
        RowCoder::decode_row(&builder_schema, &mut method.payload.as_slice()).unwrap(),
        builder_args
    );
}

#[test]
fn test_java_class_lookup_payload_without_constructor_args() {
    // A no-arg constructor leaves the schema unset and the payload empty. It does not encode
    // an empty row, because the Java expansion service rejects an empty row.
    let bytes =
        encode_java_class_lookup_payload("com.example.Transform", None, None, Vec::new()).unwrap();

    let decoded = proto::JavaClassLookupPayload::decode(bytes.as_slice()).unwrap();
    assert_eq!(decoded.class_name, "com.example.Transform");
    assert!(decoded.constructor_method.is_empty());
    assert!(decoded.constructor_schema.is_none());
    assert!(decoded.constructor_payload.is_empty());
    assert!(decoded.builder_methods.is_empty());
}

#[test]
fn test_extract_input_components() {
    let mut inner = PipelineInner::new();
    let schema = Arc::new(Schema::builder().field("id", FieldType::int64()).build());
    let schema_bytes = schema.to_proto_bytes();

    let row_coder_id = "row_coder_test".to_string();
    inner.components.coders.insert(
        row_coder_id.clone(),
        proto::Coder {
            spec: Some(proto::FunctionSpec {
                urn: URN_ROW.to_string(),
                payload: schema_bytes,
            }),
            component_coder_ids: Vec::new(),
        },
    );

    let pcoll_id = "pcoll_input_1".to_string();
    inner.components.pcollections.insert(
        pcoll_id.clone(),
        proto::PCollection {
            unique_name: pcoll_id.clone(),
            coder_id: row_coder_id.clone(),
            is_bounded: proto::is_bounded::Enum::Bounded as i32,
            windowing_strategy_id: inner.default_windowing_strategy_id.clone(),
            display_data: Vec::new(),
        },
    );

    let extracted = extract_input_components(&inner, &[&pcoll_id]);
    assert!(extracted.pcollections.contains_key(&pcoll_id));
    assert!(extracted.coders.contains_key(&row_coder_id));
    assert!(
        extracted
            .windowing_strategies
            .contains_key(&inner.default_windowing_strategy_id)
    );
}

#[test]
fn test_splice_expansion_response() {
    let mut inner = PipelineInner::new();

    let mut response_components = proto::Components::default();
    let ext_coder_id = "ext_coder_1".to_string();
    response_components.coders.insert(
        ext_coder_id.clone(),
        proto::Coder {
            spec: Some(proto::FunctionSpec {
                urn: "beam:coder:string_utf8:v1".to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );

    let ext_pcoll_id = "ext_pcoll_out".to_string();
    response_components.pcollections.insert(
        ext_pcoll_id.clone(),
        proto::PCollection {
            unique_name: ext_pcoll_id.clone(),
            coder_id: ext_coder_id.clone(),
            is_bounded: proto::is_bounded::Enum::Bounded as i32,
            windowing_strategy_id: inner.default_windowing_strategy_id.clone(),
            display_data: Vec::new(),
        },
    );

    let expanded_transform = proto::PTransform {
        unique_name: "External_BigQueryRead".to_string(),
        spec: Some(proto::FunctionSpec {
            urn: URN_EXPANSION_SCHEMA_TRANSFORM.to_string(),
            payload: Vec::new(),
        }),
        subtransforms: vec!["External_BigQueryRead/Step1".to_string()],
        inputs: HashMap::new(),
        outputs: HashMap::from([("output".to_string(), ext_pcoll_id.clone())]),
        display_data: Vec::new(),
        environment_id: String::new(),
        annotations: HashMap::new(),
    };

    let response = ExpansionResponse {
        components: Some(response_components),
        transform: Some(expanded_transform),
        requirements: vec!["beam:requirement:external_test".to_string()],
        error: String::new(),
    };

    let root_id = splice_expansion_response(&mut inner, response).unwrap();
    assert_eq!(root_id, "External_BigQueryRead");
    assert!(inner.components.transforms.contains_key(&root_id));
    assert!(inner.components.pcollections.contains_key(&ext_pcoll_id));
    assert!(inner.components.coders.contains_key(&ext_coder_id));
    assert_eq!(inner.transform_order.last().unwrap(), &root_id);
}

// Mock expansion service for end-to-end integration testing.
#[derive(Default)]
struct MockExpansionService;

#[tonic::async_trait]
impl ExpansionService for MockExpansionService {
    async fn expand(
        &self,
        request: Request<ExpansionRequest>,
    ) -> Result<Response<ExpansionResponse>, Status> {
        let req = request.into_inner();
        let transform = req.transform.expect("transform");

        let mut components = req.components.unwrap_or_default();
        let out_pcoll_id = format!("{}_out", req.namespace);
        let out_coder_id = format!("{}_coder", req.namespace);

        components.coders.insert(
            out_coder_id.clone(),
            proto::Coder {
                spec: Some(proto::FunctionSpec {
                    urn: URN_ROW.to_string(),
                    payload: Vec::new(),
                }),
                component_coder_ids: Vec::new(),
            },
        );

        components.pcollections.insert(
            out_pcoll_id.clone(),
            proto::PCollection {
                unique_name: out_pcoll_id.clone(),
                coder_id: out_coder_id,
                is_bounded: proto::is_bounded::Enum::Bounded as i32,
                windowing_strategy_id: "ws_global_default".to_string(),
                display_data: Vec::new(),
            },
        );

        let expanded = proto::PTransform {
            unique_name: transform.unique_name,
            spec: transform.spec,
            subtransforms: vec![format!("{}/sub_op", req.namespace)],
            inputs: transform.inputs,
            outputs: HashMap::from([("output".to_string(), out_pcoll_id)]),
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
        Ok(Response::new(DiscoverSchemaTransformResponse {
            schema_transform_configs: HashMap::new(),
            error: String::new(),
        }))
    }
}

/// Starts the mock expansion service on an ephemeral port.
///
/// Returns its endpoint and a shutdown handle. Drop the handle or send on it to stop the
/// server.
fn start_mock_expansion_service() -> (String, tokio::sync::oneshot::Sender<()>) {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            addr_tx.send(addr).unwrap();

            tonic::transport::Server::builder()
                .add_service(ExpansionServiceServer::new(MockExpansionService))
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

    let addr = addr_rx.recv().unwrap();
    (format!("http://{addr}"), shutdown_tx)
}

#[test]
fn test_mock_expansion_service_e2e() {
    let (endpoint, shutdown_tx) = start_mock_expansion_service();
    let client = ExpansionClient::new(&endpoint);

    let request = ExpansionRequest {
        components: Some(proto::Components::default()),
        transform: Some(proto::PTransform {
            unique_name: "TestRead".to_string(),
            spec: Some(proto::FunctionSpec {
                urn: URN_EXPANSION_SCHEMA_TRANSFORM.to_string(),
                payload: Vec::new(),
            }),
            subtransforms: Vec::new(),
            inputs: HashMap::new(),
            outputs: HashMap::new(),
            display_data: Vec::new(),
            environment_id: String::new(),
            annotations: HashMap::new(),
        }),
        namespace: "test_ns".to_string(),
        output_coder_requests: HashMap::new(),
        requirements: Vec::new(),
        pipeline_options: None,
    };

    let response = client.expand_blocking(request).unwrap();
    assert!(response.error.is_empty());

    let expanded = response.transform.unwrap();
    assert_eq!(expanded.unique_name, "TestRead");
    assert!(expanded.outputs.contains_key("output"));

    let pipeline = Pipeline::new();
    let pbegin = pipeline.begin();

    let ext_transform = ExternalTransform::new(
        "MockSource",
        URN_EXPANSION_SCHEMA_TRANSFORM,
        &endpoint,
        Vec::new(),
    );

    let pcoll = pbegin.apply(ExternalSource::new(ext_transform));
    assert!(pcoll.id().contains("out"));

    let proto = pipeline.to_proto();
    let transforms = proto.components.as_ref().unwrap().transforms.clone();
    assert!(transforms.contains_key("MockSource"));

    let _ = shutdown_tx.send(());
}

/// Transforms that use the same service share one cached client and, if auto-started, one JVM.
#[test]
fn test_expansion_clients_are_shared_per_target() {
    let (endpoint, shutdown_tx) = start_mock_expansion_service();
    let (other_endpoint, other_shutdown_tx) = start_mock_expansion_service();

    let pipeline = Pipeline::new();
    let apply_source = |name: &str, endpoint: &str| {
        pipeline
            .begin()
            .apply(ExternalSource::new(ExternalTransform::new(
                name,
                URN_EXPANSION_SCHEMA_TRANSFORM,
                endpoint,
                Vec::new(),
            )))
    };

    apply_source("MockSourceA", &endpoint);
    apply_source("MockSourceB", &endpoint);

    assert_eq!(
        pipeline.lock().expansion_clients.len(),
        1,
        "two transforms naming the same service must share a single client"
    );

    apply_source("MockSourceC", &other_endpoint);

    let lock = pipeline.lock();
    assert_eq!(
        lock.expansion_clients.len(),
        2,
        "a distinct service must get its own client"
    );
    assert!(lock.expansion_clients.contains_key(&endpoint));
    assert!(lock.expansion_clients.contains_key(&other_endpoint));
    drop(lock);

    let _ = shutdown_tx.send(());
    let _ = other_shutdown_tx.send(());
}

/// Expansion mode comes from the pipeline options, not the process, so both modes must be
/// reachable without a running worker.
#[test]
fn test_expansion_mode_follows_pipeline_options() {
    use beam::options::PipelineOptions;

    let driver = PipelineOptions::default();
    assert_eq!(ExpansionMode::from_options(&driver), ExpansionMode::Remote);
    assert_eq!(
        Pipeline::create(&driver).expansion_mode(),
        ExpansionMode::Remote
    );

    let mut worker = PipelineOptions::default();
    worker.harness.worker = true;
    assert!(worker.harness.is_worker());
    assert_eq!(
        ExpansionMode::from_options(&worker),
        ExpansionMode::Placeholder
    );
    assert_eq!(
        Pipeline::create(&worker).expansion_mode(),
        ExpansionMode::Placeholder
    );
}

/// `Placeholder` mode must not use the network: the runner has the expanded graph and a worker
/// has no service. It still produces an output PCollection, so downstream native transforms
/// register their handlers under the names that the runner uses.
#[test]
fn test_placeholder_expansion_does_not_contact_a_service() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);

    // An unroutable endpoint: if the code reaches it, the test fails and does not hang.
    let external = ExternalTransform::new(
        "PlaceholderRead",
        URN_EXPANSION_SCHEMA_TRANSFORM,
        "http://127.0.0.1:1",
        Vec::new(),
    )
    .with_namespace("ns_placeholder");

    let pcoll = pipeline.begin().apply(ExternalSource::new(external));
    assert_eq!(pcoll.id(), "ns_placeholder_out");

    let lock = pipeline.lock();
    let transform = lock
        .components
        .transforms
        .get("ns_placeholder")
        .expect("placeholder transform recorded");
    assert_eq!(transform.unique_name, "PlaceholderRead");
    assert_eq!(
        transform.outputs.get("output").map(String::as_str),
        Some("ns_placeholder_out")
    );

    // Coder and windowing strategy are unknown until the service replies, so both are pending.
    let output = lock
        .components
        .pcollections
        .get("ns_placeholder_out")
        .expect("placeholder output recorded");
    assert_eq!(output.coder_id, UNEXPANDED_PLACEHOLDER_ID);
    assert_eq!(output.windowing_strategy_id, UNEXPANDED_PLACEHOLDER_ID);

    // Nothing was expanded, so no artifacts are staged.
    assert!(lock.xlang_artifacts.is_empty());
}

/// An unreachable service returns an error from `try_expand`, the only way to recover, since
/// `PTransform::expand` panics. A panic here fails the test.
#[test]
fn test_try_expand_reports_an_unreachable_service_as_an_error() {
    let pipeline = Pipeline::new();

    // Port 1 is reserved and unroutable, so the connection fails and does not hang.
    let source = ExternalSource::new(ExternalTransform::new(
        "UnreachableRead",
        URN_EXPANSION_SCHEMA_TRANSFORM,
        "http://127.0.0.1:1",
        Vec::new(),
    ));

    let err = source
        .try_expand(&pipeline.begin())
        .expect_err("expansion against a dead endpoint cannot succeed");

    assert!(
        matches!(err, ExpansionError::Connection(..)),
        "an unreachable endpoint should surface as a connection error, got: {err:?}"
    );

    // Name the endpoint so a pipeline with many services shows which expansion failed.
    assert!(
        err.to_string().contains("127.0.0.1:1"),
        "error must name the endpoint it failed to reach: {err}"
    );
}

/// The sink, which expands an input PCollection and not a root, has the same contract.
#[test]
fn test_sink_try_expand_reports_an_unreachable_service_as_an_error() {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);

    // Produces an input PCollection without network calls.
    let input = pipeline.begin().apply(ExternalSource::new(
        ExternalTransform::new(
            "PlaceholderRead",
            URN_EXPANSION_SCHEMA_TRANSFORM,
            "http://127.0.0.1:1",
            Vec::new(),
        )
        .with_namespace("ns_sink_input"),
    ));

    // Expands the sink against an unreachable service.
    let sink = ExternalSink::new(ExternalTransform::new(
        "UnreachableWrite",
        URN_EXPANSION_SCHEMA_TRANSFORM,
        "http://127.0.0.1:1",
        Vec::new(),
    ));

    let err = {
        pipeline.lock().expansion_mode = ExpansionMode::Remote;
        sink.try_expand(&input)
            .expect_err("expansion against a dead endpoint cannot succeed")
    };

    assert!(
        matches!(err, ExpansionError::Connection(..)),
        "an unreachable endpoint should surface as a connection error, got: {err:?}"
    );
}

#[tokio::test]
async fn test_expansion_client_methods_and_errors() {
    use external::ExpansionClient;
    use model::expansion::ExpansionRequest;

    let client = ExpansionClient::new("http://localhost:12345");
    assert_eq!(client.endpoint(), "http://localhost:12345");
    assert_eq!(client.local_jar_path(), None);

    let conn_res = ExpansionClient::connect("http://127.0.0.1:1").await;
    assert!(conn_res.is_err());

    let req = ExpansionRequest::default();
    let res = client.expand_blocking(req);
    assert!(res.is_err());
}
