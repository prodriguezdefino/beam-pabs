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

//! Managed requests: the YAML config, the Managed identifiers, the config URL and the
//! annotations.

use std::collections::HashMap;
use std::sync::Arc;

use beam::pipeline::{Pipeline, URN_EXPANSION_SCHEMA_TRANSFORM};
use beam::schema::{FieldType, FieldValue, Row, Schema};
use beam_expansion::{
    ExpansionServiceServer, PCollectionId, SchemaTransformProvider, TransformInputs,
};
use beam_io_managed::{URN_MANAGED, managed_config_schema};
use beam_model::expansion::expansion_service_server::ExpansionService;
use beam_model::expansion::{DiscoverSchemaTransformRequest, ExpansionResponse};
use beam_model::pipeline as proto;
use prost::Message;
use tonic::Request;

use super::expansion_service_test::{expand, hex_bytes, request, transform_with_spec};

const TYPED_URN: &str = "beam:schematransform:test:typed_config:v1";
const JAVA_URN: &str = "beam:schematransform:test:java_typed:v1";

/// Provider that names its transform after the hex bytes of its config row.
#[derive(Default)]
struct TypedConfigProvider;

impl SchemaTransformProvider for TypedConfigProvider {
    fn identifier(&self) -> &'static str {
        TYPED_URN
    }
    fn description(&self) -> &'static str {
        "names its transform after its config"
    }
    fn config_schema(&self) -> Schema {
        typed_schema()
    }
    fn input_tags(&self) -> Vec<String> {
        Vec::new()
    }
    fn managed_identifiers(&self) -> &'static [&'static str] {
        &[JAVA_URN]
    }
    fn managed_key_aliases(&self) -> &'static [(&'static str, &'static str)] {
        &[("n", "count")]
    }
    fn build_transform(
        &self,
        config: Row,
        _inputs: TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let bytes = config
            .to_row_bytes()
            .map_err(|e| beam::Error::msg(e.to_string()))?;
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let name = format!("config {hex}");
        let pcoll = pipeline.apply(name, beam::transforms::GenerateSequence::new(0).with_end(1));
        Ok(HashMap::from([(
            "output".to_string(),
            PCollectionId::from(pcoll.id()),
        )]))
    }
}
beam_expansion::register_schema_transform!(TypedConfigProvider);

fn typed_schema() -> Schema {
    let errors = Schema::builder()
        .field("output", FieldType::string())
        .build();
    Schema::builder()
        .field("count", FieldType::int32())
        .nullable_field("ratio", FieldType::double())
        .nullable_field("name", FieldType::string())
        .nullable_field("flag", FieldType::boolean())
        .nullable_field("key", FieldType::bytes())
        .nullable_field("tags", FieldType::array(FieldType::string()))
        .nullable_field(
            "labels",
            FieldType::map(FieldType::string(), FieldType::int64()),
        )
        .nullable_field("error_handling", FieldType::row(errors))
        .build()
}

fn managed_request(
    identifier: &str,
    config: Option<&str>,
    url: Option<&str>,
) -> beam_model::expansion::ExpansionRequest {
    let schema = managed_config_schema();
    let text = |value: Option<&str>| value.map(|v| FieldValue::String(v.to_string()));
    let row = Row::new(
        Arc::clone(&schema),
        vec![text(config), text(url), text(Some(identifier))],
    )
    .unwrap();
    let payload = proto::SchemaTransformPayload {
        identifier: URN_MANAGED.to_string(),
        configuration_schema: Some(Schema::clone(&schema).into()),
        configuration_row: row.to_row_bytes().unwrap(),
    };
    request(Some(transform_with_spec(
        URN_EXPANSION_SCHEMA_TRANSFORM,
        payload.encode_to_vec(),
    )))
}

async fn expand_managed(
    identifier: &str,
    config: Option<&str>,
    url: Option<&str>,
) -> ExpansionResponse {
    let req = managed_request(identifier, config, url);
    expand(&ExpansionServiceServer::new(), req).await
}

/// The config row that the provider received.
fn received_config(resp: &ExpansionResponse) -> Row {
    assert_eq!(resp.error, "");
    let name = resp
        .components
        .as_ref()
        .unwrap()
        .transforms
        .values()
        .find_map(|t| {
            let (_, rest) = t.unique_name.split_once("config ")?;
            rest.split('/').next().map(str::to_string)
        })
        .expect("the transform of the provider");
    Row::from_row_bytes(&Arc::new(typed_schema()), &hex_bytes(&name)).unwrap()
}

/// A config row with a value in every field.
fn full_row() -> Row {
    let errors = Schema::builder()
        .field("output", FieldType::string())
        .build();
    Row::new(
        Arc::new(typed_schema()),
        vec![
            Some(FieldValue::Int32(7)),
            Some(FieldValue::Double(2.0)),
            Some(FieldValue::String("12".to_string())),
            Some(FieldValue::Boolean(true)),
            Some(FieldValue::Bytes(vec![0, 1, 2])),
            Some(FieldValue::Array(vec![
                Some(FieldValue::String("a".to_string())),
                Some(FieldValue::String("b".to_string())),
            ])),
            Some(FieldValue::Map(vec![(
                FieldValue::String("x".to_string()),
                Some(FieldValue::Int64(1)),
            )])),
            Some(FieldValue::Row(
                Row::new(
                    Arc::new(errors),
                    vec![Some(FieldValue::String("errors".to_string()))],
                )
                .unwrap(),
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn a_managed_request_converts_the_yaml_of_python_to_the_config_row() {
    // `yaml.dump` of PyYAML quotes strings that look like other types and writes bytes as
    // `!!binary`.
    let config = "n: '7'\nratio: 2\nname: 12\nflag: 'true'\nkey: !!binary |\n  AAEC\n\
                  tags: [a, b]\nlabels: {x: 1}\nerror_handling: {output: errors}\n";
    let resp = expand_managed(JAVA_URN, Some(config), None).await;

    assert_eq!(received_config(&resp), full_row());

    let annotations = &resp.transform.as_ref().unwrap().annotations;
    let text = |key: &str| String::from_utf8(annotations[key].clone()).unwrap();
    assert_eq!(text("schematransform_urn"), URN_MANAGED);
    assert_eq!(text("managed_underlying_transform_urn"), JAVA_URN);
    let schema = proto::Schema::decode(annotations["config_row_schema"].as_slice()).unwrap();
    assert_eq!(Schema::try_from(schema).unwrap(), *managed_config_schema());
    let managed =
        Row::from_row_bytes(&managed_config_schema(), &annotations["config_row"]).unwrap();
    assert_eq!(managed.get_string("config").unwrap(), Some(config));
}

#[tokio::test]
async fn the_json_config_of_the_rust_managed_client_gives_the_same_row() {
    let config = beam_io_managed::row_to_config(&full_row()).unwrap();
    let config = serde_json::Value::Object(config).to_string();
    let resp = expand_managed(TYPED_URN, Some(&config), None).await;
    assert_eq!(received_config(&resp), full_row());
}

#[tokio::test]
async fn a_managed_request_also_takes_the_identifier_and_keys_of_the_provider() {
    let resp = expand_managed(TYPED_URN, Some("{\"count\": 3}"), None).await;
    assert_eq!(received_config(&resp).get_i32("count").unwrap(), Some(3));
}

#[tokio::test]
async fn a_direct_request_has_no_managed_annotations() {
    let row = Row::new(Arc::new(typed_schema()), {
        let mut values = vec![None; 8];
        values[0] = Some(FieldValue::Int32(1));
        values
    })
    .unwrap();
    let payload = proto::SchemaTransformPayload {
        identifier: TYPED_URN.to_string(),
        configuration_schema: Some(typed_schema().into()),
        configuration_row: row.to_row_bytes().unwrap(),
    };
    let req = request(Some(transform_with_spec(
        URN_EXPANSION_SCHEMA_TRANSFORM,
        payload.encode_to_vec(),
    )));
    let resp = expand(&ExpansionServiceServer::new(), req).await;
    assert_eq!(received_config(&resp), row);
    assert!(resp.transform.unwrap().annotations.is_empty());
}

#[tokio::test]
async fn a_managed_request_reads_the_config_url() {
    let path = std::env::temp_dir().join(format!("managed-{}.yaml", std::process::id()));
    std::fs::write(&path, "count: 5\n").unwrap();
    let path = path.to_str().unwrap();
    for url in [path.to_string(), format!("file://{path}")] {
        let resp = expand_managed(JAVA_URN, None, Some(&url)).await;
        assert_eq!(
            received_config(&resp).get_i32("count").unwrap(),
            Some(5),
            "{url}"
        );
    }
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn managed_request_errors_table() {
    let not_both = "Please specify a config or a config URL, but not both.";
    let cases = [
        (JAVA_URN, Some("count: 1"), Some("/tmp/c.yaml"), not_both),
        (JAVA_URN, None, None, not_both),
        (
            JAVA_URN,
            Some("count: 1\nbad: 2"),
            None,
            "unknown key 'bad', expected one of: count, ratio, name, flag, key, tags, labels, \
             error_handling",
        ),
        (
            JAVA_URN,
            Some("error_handling: {output: e, extra: 1}\ncount: 1"),
            None,
            "unknown key 'error_handling.extra', expected one of: output",
        ),
        (
            JAVA_URN,
            Some("ratio: 1"),
            None,
            "missing required key 'count'",
        ),
        (
            JAVA_URN,
            Some("count: 3000000000"),
            None,
            "key 'count' value 3000000000 is out of range for INT32",
        ),
        (
            JAVA_URN,
            Some("count: x"),
            None,
            "key 'count' is 'x', expected INT32",
        ),
        (
            JAVA_URN,
            Some("[1]"),
            None,
            "the config is a sequence, expected a mapping",
        ),
        (
            "beam:schematransform:test:unknown:v1",
            Some("count: 1"),
            None,
            "No SchemaTransformProvider found for the Managed identifier \
             'beam:schematransform:test:unknown:v1'",
        ),
        (
            JAVA_URN,
            Some("count: 1\nlabels: {x: null}"),
            None,
            "key 'labels' has a null map value",
        ),
        (
            JAVA_URN,
            Some("n: 1\ncount: 2"),
            None,
            "duplicate key 'count'",
        ),
        (
            JAVA_URN,
            Some("n: null\ncount: 2"),
            None,
            "duplicate key 'count'",
        ),
        (
            JAVA_URN,
            Some("count: 2\nn: null"),
            None,
            "duplicate key 'count'",
        ),
    ];
    for (identifier, config, url, expected) in cases {
        let resp = expand_managed(identifier, config, url).await;
        assert!(resp.error.contains(expected), "{config:?}: {}", resp.error);
        assert_eq!(resp.transform, None);
    }

    let duplicate_literal = expand_managed(JAVA_URN, Some("count: 1\ncount: 2"), None).await;
    assert!(
        !duplicate_literal.error.is_empty(),
        "duplicate literal keys must be rejected"
    );
    assert_eq!(duplicate_literal.transform, None);
}

#[tokio::test]
async fn config_url_enforces_the_1mib_size_limit() {
    const MAX_CONFIG_BYTES: usize = 1024 * 1024;
    let base = "count: 5\n";

    let ok_path =
        std::env::temp_dir().join(format!("managed-limit-ok-{}.yaml", std::process::id()));
    let mut ok_bytes = base.as_bytes().to_vec();
    ok_bytes.resize(MAX_CONFIG_BYTES, b' ');
    std::fs::write(&ok_path, &ok_bytes).unwrap();

    let resp = expand_managed(JAVA_URN, None, Some(ok_path.to_str().unwrap())).await;
    let _ = std::fs::remove_file(&ok_path);
    assert_eq!(received_config(&resp).get_i32("count").unwrap(), Some(5));

    let too_large_path =
        std::env::temp_dir().join(format!("managed-limit-large-{}.yaml", std::process::id()));
    let mut too_large_bytes = base.as_bytes().to_vec();
    too_large_bytes.resize(MAX_CONFIG_BYTES + 1, b' ');
    std::fs::write(&too_large_path, &too_large_bytes).unwrap();

    let resp = expand_managed(JAVA_URN, None, Some(too_large_path.to_str().unwrap())).await;
    let _ = std::fs::remove_file(&too_large_path);
    assert!(
        resp.error.contains("1048576"),
        "expected size limit error, got: {}",
        resp.error
    );
    assert_eq!(resp.transform, None);
}

#[tokio::test]
async fn a_managed_identifier_does_not_resolve_without_managed() {
    let payload = proto::SchemaTransformPayload {
        identifier: JAVA_URN.to_string(),
        ..Default::default()
    };
    let req = request(Some(transform_with_spec(
        URN_EXPANSION_SCHEMA_TRANSFORM,
        payload.encode_to_vec(),
    )));
    let resp = expand(&ExpansionServiceServer::new(), req).await;
    assert_eq!(
        resp.error,
        format!("No SchemaTransformProvider found for URN '{JAVA_URN}'")
    );
}

#[tokio::test]
async fn discovery_lists_the_managed_transform_and_no_managed_identifier() {
    let configs = ExpansionServiceServer::new()
        .discover_schema_transform(Request::new(DiscoverSchemaTransformRequest {}))
        .await
        .unwrap()
        .into_inner()
        .schema_transform_configs;
    let managed = &configs[URN_MANAGED];
    let names: Vec<&str> = managed
        .config_schema
        .as_ref()
        .unwrap()
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, ["config", "config_url", "transform_identifier"]);
    assert!(managed.input_pcollection_names.is_empty());
    assert_eq!(managed.output_pcollection_names, ["output"]);
    assert!(configs.contains_key(TYPED_URN));
    assert!(!configs.contains_key(JAVA_URN));
}
