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

//! Tests for the expansion service. The tests call the gRPC trait methods directly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use beam::pipeline::{Pipeline, URN_PAR_DO};
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use expansion::{ExpansionServiceServer, PCollectionId, SchemaTransformProvider};
use model::expansion::expansion_service_server::ExpansionService;
use model::expansion::{DiscoverSchemaTransformRequest, ExpansionRequest, ExpansionResponse};
use model::pipeline as proto;
use prost::Message;
use tonic::Request;

const GENERATE_SEQUENCE_URN: &str = "beam:schematransform:org.apache.beam:generate_sequence:v1";
const FAILING_URN: &str = "beam:schematransform:test:always_fails:v1";
const INPUT_CHECKING_URN: &str = "beam:schematransform:test:requires_input_pcollection:v1";
const ECHO_URN: &str = "beam:schematransform:test:echo_config:v1";
const SCHEMA_TRANSFORM_URN: &str = "beam:expansion:payload:schematransform:v1";

/// Provider whose build step always fails. It tests the build-failure error path.
#[derive(Default)]
struct FailingProvider;

impl SchemaTransformProvider for FailingProvider {
    fn identifier(&self) -> &'static str {
        FAILING_URN
    }
    fn description(&self) -> &'static str {
        "always fails"
    }
    fn config_schema(&self) -> Schema {
        Schema::builder().build()
    }
    fn build_transform(
        &self,
        _config: Row,
        _inputs: HashMap<String, PCollectionId>,
        _pipeline: &mut Pipeline,
    ) -> Result<HashMap<String, PCollectionId>, String> {
        Err("deliberate build failure".to_string())
    }
}
expansion::register_schema_transform!(FailingProvider);

/// Provider that requires its "input" PCollection in the sub-pipeline.
///
/// A non-source transform needs this PCollection to apply anything to it.
#[derive(Default)]
struct InputCheckingProvider;

impl SchemaTransformProvider for InputCheckingProvider {
    fn identifier(&self) -> &'static str {
        INPUT_CHECKING_URN
    }
    fn description(&self) -> &'static str {
        "requires its input PCollection"
    }
    fn config_schema(&self) -> Schema {
        Schema::builder().build()
    }
    fn build_transform(
        &self,
        _config: Row,
        inputs: HashMap<String, PCollectionId>,
        pipeline: &mut Pipeline,
    ) -> Result<HashMap<String, PCollectionId>, String> {
        let input = inputs.get("input").ok_or("no 'input' tag")?;
        if !pipeline.lock().components.pcollections.contains_key(input) {
            return Err(format!(
                "input PCollection '{input}' unknown to sub-pipeline"
            ));
        }
        Ok(HashMap::from([("output".to_string(), input.clone())]))
    }
}
expansion::register_schema_transform!(InputCheckingProvider);

/// Provider that writes its config into the name of the transform that it applies.
///
/// Tests read the name back to see the config row that the provider received.
#[derive(Default)]
struct EchoConfigProvider;

impl SchemaTransformProvider for EchoConfigProvider {
    fn identifier(&self) -> &'static str {
        ECHO_URN
    }
    fn description(&self) -> &'static str {
        "echoes its config"
    }
    fn config_schema(&self) -> Schema {
        Schema::builder()
            .nullable_field("start", FieldType::int64())
            .nullable_field("stop", FieldType::int64())
            .nullable_field("nested", FieldType::row(echo_nested_schema(["a", "b"])))
            .build()
    }
    fn input_tags(&self) -> Vec<String> {
        Vec::new()
    }
    fn build_transform(
        &self,
        config: Row,
        _inputs: HashMap<String, PCollectionId>,
        pipeline: &mut Pipeline,
    ) -> Result<HashMap<String, PCollectionId>, String> {
        let start = config.get_i64("start").map_err(|e| e.to_string())?;
        let stop = config.get_i64("stop").map_err(|e| e.to_string())?;
        let name = format!("echo start={start:?} stop={stop:?}");
        let pcoll = pipeline.apply(beam::transforms::GenerateSequence::new(name, 0).with_end(1));
        Ok(HashMap::from([(
            "output".to_string(),
            pcoll.id().to_string(),
        )]))
    }
}
expansion::register_schema_transform!(EchoConfigProvider);

/// Nested config schema with INT64 fields in the given order.
fn echo_nested_schema(names: [&str; 2]) -> Schema {
    Schema::new(
        names
            .iter()
            .map(|n| Field::new(*n, FieldType::int64()))
            .collect(),
    )
}

// Payloads that Python 2.76 sends for `ECHO_URN`. Each comment gives the Python call.
// `SchemaAwareExternalTransform` builds the schema from its keyword arguments in their order.

/// `SchemaAwareExternalTransform(ECHO_URN, service, stop=7, start=3)`
const PY_SCHEMA_TRANSFORM_REORDERED: &str = "0a286265616d3a736368656d617472616e73666f726d3a746573743a6563686f5f636f6e6669673a7631123f0a0a0a0473746f701a0210040a0b0a0573746172741a021004122435336532656365362d343636362d346666612d393634302d6564376564303435616365341a0402000703";
/// `SchemaAwareExternalTransform(ECHO_URN, service, stop=7)`
const PY_SCHEMA_TRANSFORM_OMITTED: &str = "0a286265616d3a736368656d617472616e73666f726d3a746573743a6563686f5f636f6e6669673a763112320a0a0a0473746f701a021004122462343239623066612d376462652d346363612d623431362d3530646564363963383132361a03010007";
/// `SchemaAwareExternalTransform(ECHO_URN, service, start=3, step=2)`
const PY_SCHEMA_TRANSFORM_UNKNOWN_FIELD: &str = "0a286265616d3a736368656d617472616e73666f726d3a746573743a6563686f5f636f6e6669673a7631123f0a0b0a0573746172741a0210040a0a0a04737465701a021004122436356331653131642d333264342d343331612d393032342d3865316433316339633832341a0402000302";
/// `SchemaAwareExternalTransform(ECHO_URN, service, start=3, stop="7")`
const PY_SCHEMA_TRANSFORM_WRONG_TYPE: &str = "0a286265616d3a736368656d617472616e73666f726d3a746573743a6563686f5f636f6e6669673a7631123f0a0b0a0573746172741a0210040a0a0a0473746f701a021007122466383066353136352d383538312d343332662d616661652d6666646665646138363362321a050200030137";
/// `ExternalTransform(ECHO_URN, ImplicitSchemaPayloadBuilder({"start": 3, "stop": 7}), service)`
const PY_EXTERNAL_CONFIGURATION: &str = "0a3f0a0b0a0573746172741a0210040a0a0a0473746f701a021004122461306333393665302d363632332d343162332d613164302d646164383930393835303935120402000307";

fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex digit pair"))
        .collect()
}

fn transform_with_spec(urn: &str, payload: Vec<u8>) -> proto::PTransform {
    proto::PTransform {
        unique_name: "MyExpansion".to_string(),
        spec: Some(proto::FunctionSpec {
            urn: urn.to_string(),
            payload,
        }),
        ..Default::default()
    }
}

fn request(transform: Option<proto::PTransform>) -> ExpansionRequest {
    ExpansionRequest {
        transform,
        ..Default::default()
    }
}

async fn expand(service: &ExpansionServiceServer, req: ExpansionRequest) -> ExpansionResponse {
    service
        .expand(Request::new(req))
        .await
        .expect("expand never returns a gRPC status; errors go in the response")
        .into_inner()
}

fn generate_sequence_row(start: i64, stop: i64) -> Row {
    let schema = Arc::new(
        Schema::builder()
            .field("start", FieldType::int64())
            .field("stop", FieldType::int64())
            .build(),
    );
    Row::new(
        schema,
        vec![
            Some(FieldValue::Int64(start)),
            Some(FieldValue::Int64(stop)),
        ],
    )
    .expect("Row matches schema")
}

/// `ExternalConfigurationPayload` bytes, for a spec whose URN is the provider identifier.
fn external_configuration_payload(row: &Row) -> Vec<u8> {
    proto::ExternalConfigurationPayload {
        schema: Some((**row.schema()).clone().into()),
        payload: row.to_row_bytes().expect("Row encodes to bytes"),
    }
    .encode_to_vec()
}

fn generate_sequence_payload(start: i64, stop: i64) -> Vec<u8> {
    external_configuration_payload(&generate_sequence_row(start, stop))
}

/// Unique names of all transforms in a successful expansion.
fn transform_names(resp: &ExpansionResponse) -> Vec<String> {
    assert_eq!(resp.error, "");
    resp.components
        .as_ref()
        .expect("components")
        .transforms
        .values()
        .map(|t| t.unique_name.clone())
        .collect()
}

/// Asserts that `resp` has exactly the error `expected` and no other content.
fn assert_error_response(resp: &ExpansionResponse, expected: &str) {
    assert_eq!(resp.error, expected);
    assert_eq!(resp.transform, None);
    assert_eq!(resp.components, None);
    assert!(resp.requirements.is_empty(), "{:?}", resp.requirements);
}

// ---------------------------------------------------------------------------
// Error paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expand_missing_inputs_table() {
    let cases = [
        (request(None), "ExpansionRequest missing transform"),
        (
            request(Some(proto::PTransform {
                unique_name: "NoSpec".to_string(),
                ..Default::default()
            })),
            "PTransform missing spec",
        ),
        (
            request(Some(transform_with_spec(
                "beam:schematransform:does_not_exist:v1",
                Vec::new(),
            ))),
            "No SchemaTransformProvider found for URN 'beam:schematransform:does_not_exist:v1'",
        ),
        (
            request(Some(transform_with_spec(
                SCHEMA_TRANSFORM_URN,
                proto::SchemaTransformPayload {
                    identifier: "beam:schematransform:does_not_exist:v1".to_string(),
                    ..Default::default()
                }
                .encode_to_vec(),
            ))),
            "No SchemaTransformProvider found for URN 'beam:schematransform:does_not_exist:v1'",
        ),
    ];

    let service = ExpansionServiceServer::new();
    for (req, expected_err) in cases {
        let resp = expand(&service, req).await;
        assert_error_response(&resp, expected_err);
    }
}

#[tokio::test]
async fn expand_rejects_config_that_does_not_match_provider_table() {
    let cases = [
        (
            SCHEMA_TRANSFORM_URN,
            hex_bytes(PY_SCHEMA_TRANSFORM_UNKNOWN_FIELD),
            format!("Failed to decode config row for '{ECHO_URN}': unknown field 'step'"),
        ),
        (
            SCHEMA_TRANSFORM_URN,
            hex_bytes(PY_SCHEMA_TRANSFORM_WRONG_TYPE),
            format!(
                "Failed to decode config row for '{ECHO_URN}': field 'stop' has type STRING, expected INT64?"
            ),
        ),
        (
            GENERATE_SEQUENCE_URN,
            proto::ExternalConfigurationPayload {
                schema: None,
                payload: vec![1],
            }
            .encode_to_vec(),
            format!(
                "Failed to decode config row for '{GENERATE_SEQUENCE_URN}': configuration row has no schema"
            ),
        ),
    ];
    let service = ExpansionServiceServer::new();
    for (urn, payload, expected_err) in cases {
        let resp = expand(&service, request(Some(transform_with_spec(urn, payload)))).await;
        assert_error_response(&resp, &expected_err);
    }
}

#[tokio::test]
async fn expand_nested_config_row_needs_same_field_order() {
    let nested_config = |nested: Schema| {
        let nested_row = Row::new(
            Arc::new(nested.clone()),
            vec![Some(FieldValue::Int64(1)), Some(FieldValue::Int64(2))],
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            FieldType::row(nested),
        )]));
        let row = Row::new(schema, vec![Some(FieldValue::Row(nested_row))]).unwrap();
        transform_with_spec(ECHO_URN, external_configuration_payload(&row))
    };
    let service = ExpansionServiceServer::new();

    // A schema id and other nullability do not change the layout of the nested values.
    let mut same_order = echo_nested_schema(["a", "b"]);
    same_order.id = Some("caller-schema-id".to_string());
    same_order.fields[0].field_type.nullable = true;
    let resp = expand(&service, request(Some(nested_config(same_order)))).await;
    assert_eq!(resp.error, "");

    let resp = expand(
        &service,
        request(Some(nested_config(echo_nested_schema(["b", "a"])))),
    )
    .await;
    let prefix =
        format!("Failed to decode config row for '{ECHO_URN}': field 'nested' has type ROW");
    assert!(resp.error.starts_with(&prefix), "error was: {}", resp.error);
}

#[tokio::test]
async fn expand_undecodable_payload_returns_error_table() {
    // A truncated varint is neither a valid protobuf message nor a valid row encoding.
    let bad_row = proto::ExternalConfigurationPayload {
        schema: Some((**generate_sequence_row(0, 1).schema()).clone().into()),
        payload: vec![0xff],
    }
    .encode_to_vec();
    let cases = [
        (
            GENERATE_SEQUENCE_URN,
            vec![0xff],
            format!(
                "Failed to decode ExternalConfigurationPayload for '{GENERATE_SEQUENCE_URN}': "
            ),
        ),
        (
            SCHEMA_TRANSFORM_URN,
            vec![0xff],
            "Failed to decode SchemaTransformPayload: ".to_string(),
        ),
        (
            GENERATE_SEQUENCE_URN,
            bad_row,
            format!("Failed to decode config row for '{GENERATE_SEQUENCE_URN}': "),
        ),
    ];
    let service = ExpansionServiceServer::new();
    for (urn, payload, prefix) in cases {
        let resp = expand(&service, request(Some(transform_with_spec(urn, payload)))).await;
        assert!(resp.error.starts_with(&prefix), "error was: {}", resp.error);
        assert!(resp.error.len() > prefix.len(), "decoder cause missing");
        assert_eq!(resp.transform, None);
        assert_eq!(resp.components, None);
    }
}

#[tokio::test]
async fn expand_build_failure_returns_error() {
    let transform = transform_with_spec(FAILING_URN, Vec::new());
    let resp = expand(&ExpansionServiceServer::new(), request(Some(transform))).await;
    assert_error_response(
        &resp,
        &format!("Failed to expand '{FAILING_URN}': deliberate build failure"),
    );
}

// ---------------------------------------------------------------------------
// Successful expansion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expand_decodes_python_payloads_table() {
    let cases = [
        (
            SCHEMA_TRANSFORM_URN,
            hex_bytes(PY_SCHEMA_TRANSFORM_REORDERED),
            "echo start=Some(3) stop=Some(7)",
        ),
        (
            SCHEMA_TRANSFORM_URN,
            hex_bytes(PY_SCHEMA_TRANSFORM_OMITTED),
            "echo start=None stop=Some(7)",
        ),
        (
            ECHO_URN,
            hex_bytes(PY_EXTERNAL_CONFIGURATION),
            "echo start=Some(3) stop=Some(7)",
        ),
        (ECHO_URN, Vec::new(), "echo start=None stop=None"),
    ];
    let service = ExpansionServiceServer::new();
    for (urn, payload, expected) in cases {
        let resp = expand(&service, request(Some(transform_with_spec(urn, payload)))).await;
        let names = transform_names(&resp);
        let echoed: Vec<&str> = names
            .iter()
            .filter_map(|n| n.strip_prefix("MyExpansion/"))
            .filter(|n| n.starts_with("echo "))
            .collect();
        assert!(
            !echoed.is_empty() && echoed.iter().all(|n| n.starts_with(expected)),
            "{urn}: expected {expected}, got {names:?}"
        );
    }
}

#[tokio::test]
async fn expand_generate_sequence_returns_consistent_composite() {
    let transform = transform_with_spec(GENERATE_SEQUENCE_URN, generate_sequence_payload(3, 7));
    let resp = expand(
        &ExpansionServiceServer::new(),
        request(Some(transform.clone())),
    )
    .await;

    assert_eq!(resp.error, "");
    let components = resp.components.expect("components");
    let expanded = resp.transform.expect("transform");

    // The caller's transform is returned with its identity preserved.
    assert_eq!(expanded.unique_name, "MyExpansion");
    assert_eq!(expanded.spec, transform.spec);
    assert!(expanded.inputs.is_empty());

    // Exactly the provider's declared output, pointing at a PCollection that is returned.
    assert_eq!(
        expanded.outputs.keys().collect::<Vec<_>>(),
        vec!["output"],
        "outputs: {:?}",
        expanded.outputs
    );
    let out_id = &expanded.outputs["output"];
    let out_pcoll = components
        .pcollections
        .get(out_id)
        .unwrap_or_else(|| panic!("output PCollection {out_id} missing from components"));
    assert!(
        components.coders.contains_key(&out_pcoll.coder_id),
        "coder {} of output missing",
        out_pcoll.coder_id
    );
    assert!(
        components
            .windowing_strategies
            .contains_key(&out_pcoll.windowing_strategy_id),
        "windowing strategy of output missing"
    );

    // The expanded composite must name its sub-graph; otherwise a runner sees an unknown
    // schematransform URN as a leaf and the sub-transforms as unrelated roots.
    assert!(
        !expanded.subtransforms.is_empty(),
        "expanded transform has no subtransforms"
    );
    for sub in &expanded.subtransforms {
        assert!(
            components.transforms.contains_key(sub),
            "subtransform {sub} not in components"
        );
    }
    // Every returned transform is reachable from the composite.
    let mut reachable: HashSet<&str> = HashSet::new();
    let mut stack: Vec<&str> = expanded.subtransforms.iter().map(String::as_str).collect();
    while let Some(id) = stack.pop() {
        if reachable.insert(id) {
            stack.extend(
                components.transforms[id]
                    .subtransforms
                    .iter()
                    .map(String::as_str),
            );
        }
    }
    let all: HashSet<&str> = components.transforms.keys().map(String::as_str).collect();
    assert_eq!(reachable, all);
    // Names stay unique when a caller pipeline expands the same provider many times.
    for t in components.transforms.values() {
        assert!(
            t.unique_name.starts_with("MyExpansion/"),
            "{} is not under the caller transform",
            t.unique_name
        );
    }
    // Some subtransform produces the declared output.
    assert!(
        components
            .transforms
            .values()
            .any(|t| t.outputs.values().any(|o| o == out_id)),
        "no transform produces {out_id}"
    );
    // GenerateSequence is a splittable DoFn, so the caller must declare that requirement.
    assert_eq!(
        resp.requirements,
        vec!["beam:requirement:pardo:splittable_dofn:v1".to_string()]
    );
}

#[tokio::test]
async fn expand_assigns_service_environment_to_sdk_transforms() {
    let transform = transform_with_spec(GENERATE_SEQUENCE_URN, Vec::new());
    let resp = expand(&ExpansionServiceServer::new(), request(Some(transform))).await;
    assert_eq!(resp.error, "");
    let components = resp.components.unwrap();

    let env = components
        .environments
        .get("rust_environment")
        .expect("rust_environment");
    assert_eq!(env.urn, beam::pipeline::URN_ENV_DEFAULT);
    assert!(env.payload.is_empty());
    assert_eq!(
        env.capabilities,
        beam::pipeline::constants::standard_capabilities()
    );
    // SDK transforms use the service environment. Runner primitives such as Impulse and
    // composites have none, so the runner runs them.
    assert!(!components.transforms.is_empty());
    for (id, t) in &components.transforms {
        let urn = t.spec.as_ref().map_or("", |s| s.urn.as_str());
        if urn == URN_PAR_DO {
            assert_eq!(t.environment_id, "rust_environment", "transform {id}");
        } else {
            assert!(
                t.environment_id.is_empty(),
                "transform {id} ({urn}) has an env"
            );
        }
    }
}

#[tokio::test]
async fn expand_with_docker_environment_table() {
    let images = ["apache/beam_rust_sdk:x", "custom/beam_sdk:v1"];
    for image in images {
        let service = ExpansionServiceServer::new().with_docker_environment(image);
        let transform = transform_with_spec(GENERATE_SEQUENCE_URN, Vec::new());
        let resp = expand(&service, request(Some(transform))).await;
        assert_eq!(resp.error, "");
        let components = resp.components.unwrap();
        for (id, t) in &components.transforms {
            assert!(
                t.environment_id.is_empty() || t.environment_id == "rust_environment",
                "transform {id}"
            );
        }
        let env = &components.environments["rust_environment"];
        assert_eq!(env.urn, beam::pipeline::URN_ENV_DOCKER);
        let payload = proto::DockerPayload::decode(env.payload.as_slice()).unwrap();
        assert_eq!(payload.container_image, image);
    }
}

#[tokio::test]
async fn expand_with_external_environment_encodes_endpoint() {
    let service = ExpansionServiceServer::new().with_external_environment("localhost:50000");
    let transform = transform_with_spec(GENERATE_SEQUENCE_URN, Vec::new());
    let resp = expand(&service, request(Some(transform))).await;
    assert_eq!(resp.error, "");
    let env = &resp.components.unwrap().environments["rust_environment"];
    assert_eq!(env.urn, beam::pipeline::URN_ENV_EXTERNAL);
    let payload = proto::ExternalPayload::decode(env.payload.as_slice()).unwrap();
    assert_eq!(payload.endpoint.unwrap().url, "localhost:50000");
    assert!(payload.params.is_empty());
}

#[tokio::test]
async fn expand_empty_payload_and_explicit_payload_differ() {
    // With an empty payload the provider falls back to its defaults (0..10); an explicit
    // row must reach the provider and change the expansion.
    let service = ExpansionServiceServer::new();
    let default_resp = expand(
        &service,
        request(Some(transform_with_spec(GENERATE_SEQUENCE_URN, Vec::new()))),
    )
    .await;
    let explicit_resp = expand(
        &service,
        request(Some(transform_with_spec(
            GENERATE_SEQUENCE_URN,
            generate_sequence_payload(100, 105),
        ))),
    )
    .await;
    let payloads = |r: &ExpansionResponse| -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = r
            .components
            .as_ref()
            .unwrap()
            .transforms
            .values()
            .filter_map(|t| t.spec.as_ref())
            .map(|s| s.payload.clone())
            .collect();
        v.sort();
        v
    };
    assert_ne!(payloads(&default_resp), payloads(&explicit_resp));
}

#[tokio::test]
async fn discover_lists_registered_providers() {
    let resp = ExpansionServiceServer::new()
        .discover_schema_transform(Request::new(DiscoverSchemaTransformRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.error, "");

    let gs = resp
        .schema_transform_configs
        .get(GENERATE_SEQUENCE_URN)
        .expect("GenerateSequence registered");
    assert_eq!(
        gs.description,
        "Generates a sequence of integers from start to stop."
    );
    assert!(gs.input_pcollection_names.is_empty());
    assert_eq!(gs.output_pcollection_names, vec!["output".to_string()]);
    let field_names: Vec<&str> = gs
        .config_schema
        .as_ref()
        .unwrap()
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(field_names, vec!["start", "stop"]);

    // Providers registered by this test binary are discovered too, with default tags.
    let failing = &resp.schema_transform_configs[FAILING_URN];
    assert_eq!(failing.description, "always fails");
    assert_eq!(failing.input_pcollection_names, vec!["input".to_string()]);
    assert_eq!(failing.output_pcollection_names, vec!["output".to_string()]);
    assert_eq!(resp.schema_transform_configs.len(), 4);
}

// ---------------------------------------------------------------------------
// Namespaces and request components
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expand_prefixes_new_ids_with_request_namespace() {
    let req = ExpansionRequest {
        transform: Some(transform_with_spec(GENERATE_SEQUENCE_URN, Vec::new())),
        namespace: "ns42".to_string(),
        ..Default::default()
    };
    let resp = expand(&ExpansionServiceServer::new(), req).await;
    assert_eq!(resp.error, "");
    let components = resp.components.unwrap();
    let ids = components
        .transforms
        .keys()
        .chain(components.pcollections.keys())
        .chain(components.coders.keys())
        .chain(components.windowing_strategies.keys())
        .chain(components.environments.keys());
    for id in ids {
        assert!(id.starts_with("ns42"), "id {id} is not namespaced");
    }
}

#[tokio::test]
async fn expand_makes_request_components_available_to_provider() {
    let input_pcoll = proto::PCollection {
        unique_name: "caller_input".to_string(),
        coder_id: "caller_coder".to_string(),
        windowing_strategy_id: "caller_ws".to_string(),
        ..Default::default()
    };
    let mut transform = transform_with_spec(INPUT_CHECKING_URN, Vec::new());
    transform
        .inputs
        .insert("input".to_string(), "caller_input".to_string());
    let req = ExpansionRequest {
        transform: Some(transform),
        components: Some(proto::Components {
            pcollections: HashMap::from([("caller_input".to_string(), input_pcoll)]),
            ..Default::default()
        }),
        namespace: "ns".to_string(),
        ..Default::default()
    };
    let resp = expand(&ExpansionServiceServer::new(), req).await;
    assert_eq!(resp.error, "");
    let expanded = resp.transform.unwrap();
    assert_eq!(expanded.inputs["input"], "caller_input");
}

#[tokio::test]
async fn expand_passes_input_tags_to_provider() {
    // The provider receives the caller input map and fails when the PCollection is unknown.
    let mut transform = transform_with_spec(INPUT_CHECKING_URN, Vec::new());
    transform
        .inputs
        .insert("input".to_string(), "caller_input".to_string());
    let resp = expand(&ExpansionServiceServer::new(), request(Some(transform))).await;
    assert_error_response(
        &resp,
        &format!(
            "Failed to expand '{INPUT_CHECKING_URN}': input PCollection 'caller_input' unknown to sub-pipeline"
        ),
    );
}
