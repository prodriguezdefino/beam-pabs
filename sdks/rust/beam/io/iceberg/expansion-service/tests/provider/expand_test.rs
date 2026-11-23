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

//! `DiscoverSchemaTransform` and `Expand` RPC tests for [`IcebergWriteProvider`].

use std::collections::HashMap;
use std::sync::Arc;

use beam::model::expansion::expansion_service_server::ExpansionService;
use beam::model::expansion::{DiscoverSchemaTransformRequest, ExpansionRequest, ExpansionResponse};
use beam::model::pipeline as proto;
use beam::pipeline::{Pipeline, URN_EXPANSION_SCHEMA_TRANSFORM};
use beam::schema::{BeamRow, Field, FieldType, FieldValue, Row, Schema, URN_MILLIS_INSTANT};
use beam::values::IsBounded;
use beam_expansion::ExpansionServiceServer;
use beam_io_iceberg_expansion::{ErrorHandling, IcebergWriteConfig, URN_ICEBERG_WRITE};
use prost::Message;
use tonic::Request;

use crate::common::{CATALOG_URI, base_config, catalog_props};

fn metric_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new(
            "timestamp_ms",
            FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64()),
        ),
        Field::new("device_id", FieldType::string()),
    ])
}

/// Builds caller components with one `"caller_input"` PCollection of `schema` and `bounded`.
fn caller_components(schema: &Schema, bounded: IsBounded) -> proto::Components {
    let p = Pipeline::new();
    let coder = p.register_row_coder(schema);
    let _ = p.add_pcollection::<Row>("caller_input", &coder, bounded);
    let mut components = p.to_proto().components.unwrap_or_default();
    let pcoll = components
        .pcollections
        .values()
        .next()
        .cloned()
        .expect("one pcollection");
    components.pcollections.clear();
    components.transforms.clear();
    components
        .pcollections
        .insert("caller_input".to_string(), pcoll);
    components
}

fn schema_transform_request(
    row: &Row,
    components: proto::Components,
    input_id: Option<&str>,
) -> ExpansionRequest {
    let payload = proto::SchemaTransformPayload {
        identifier: URN_ICEBERG_WRITE.to_string(),
        configuration_schema: Some((**row.schema()).clone().into()),
        configuration_row: row.to_row_bytes().expect("config row encodes"),
    }
    .encode_to_vec();
    let mut inputs = HashMap::new();
    if let Some(id) = input_id {
        inputs.insert("input".to_string(), id.to_string());
    }
    ExpansionRequest {
        components: Some(components),
        transform: Some(proto::PTransform {
            unique_name: "WriteIceberg".to_string(),
            spec: Some(proto::FunctionSpec {
                urn: URN_EXPANSION_SCHEMA_TRANSFORM.to_string(),
                payload,
            }),
            inputs,
            ..Default::default()
        }),
        namespace: "ns_".to_string(),
        ..Default::default()
    }
}

async fn run_expand(req: ExpansionRequest) -> ExpansionResponse {
    ExpansionServiceServer::new()
        .expand(Request::new(req))
        .await
        .expect("expand returns response")
        .into_inner()
}

#[tokio::test]
async fn discover_schema_transform_lists_iceberg_write() {
    let resp = ExpansionServiceServer::new()
        .discover_schema_transform(Request::new(DiscoverSchemaTransformRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.error, "");
    let entry = resp
        .schema_transform_configs
        .get(URN_ICEBERG_WRITE)
        .expect("iceberg_write registered");
    assert_eq!(entry.input_pcollection_names, vec!["input".to_string()]);
    assert_eq!(
        entry.output_pcollection_names,
        vec!["snapshots".to_string(), "errors".to_string()]
    );
    assert!(entry.description.contains("Iceberg"));
    assert_eq!(
        entry.config_schema.as_ref().unwrap().fields.len(),
        15,
        "all 15 configuration fields discovered"
    );
}

#[tokio::test]
async fn expand_bounded_without_error_handling_outputs_snapshots_and_attaches_fail_on_error() {
    let config = IcebergWriteConfig {
        partition_fields: Some(vec!["hour(timestamp_ms)".to_string()]),
        triggering_frequency_seconds: Some(30),
        ..base_config("db.metrics")
    };
    let row = config.to_row().unwrap();
    let resp = run_expand(schema_transform_request(
        &row,
        caller_components(&metric_schema(), IsBounded::Bounded),
        Some("caller_input"),
    ))
    .await;

    assert_eq!(resp.error, "", "unexpected error: {}", resp.error);
    let expanded = resp.transform.expect("expanded transform");
    let components = resp.components.expect("expanded components");
    assert_eq!(
        expanded
            .outputs
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["snapshots"]
    );
    let snapshots_id = &expanded.outputs["snapshots"];
    assert!(components.pcollections.contains_key(snapshots_id));
    let has_fail_on_error = components
        .transforms
        .values()
        .any(|t| t.unique_name.ends_with("/FailOnError"));
    assert!(has_fail_on_error, "FailOnError step must be attached");
    assert!(
        components
            .pcollections
            .values()
            .all(|p| IsBounded::from(p.is_bounded()) == IsBounded::Bounded)
    );
}

#[tokio::test]
async fn expand_unbounded_with_error_handling_and_python_int64_fields_returns_both_outputs() {
    // Python sends integer kwargs as INT64; decode_config narrows them to INT32.
    let py_schema = Arc::new(
        Schema::builder()
            .field("table", FieldType::string())
            .field(
                "catalog_properties",
                FieldType::map(FieldType::string(), FieldType::string()),
            )
            .field("triggering_frequency_seconds", FieldType::int64())
            .field("num_shards", FieldType::int64())
            .field(
                "error_handling",
                FieldType::row(Arc::unwrap_or_clone(ErrorHandling::beam_schema())),
            )
            .build(),
    );
    let props_map = vec![(
        FieldValue::String("uri".to_string()),
        Some(FieldValue::String(CATALOG_URI.to_string())),
    )];
    let err_row = ErrorHandling {
        output: "rejected".to_string(),
    }
    .to_row()
    .unwrap();
    let row = Row::new(
        py_schema,
        vec![
            Some(FieldValue::String("db.metrics".to_string())),
            Some(FieldValue::Map(props_map)),
            Some(FieldValue::Int64(15)),
            Some(FieldValue::Int64(4)),
            Some(FieldValue::Row(err_row)),
        ],
    )
    .unwrap();

    let resp = run_expand(schema_transform_request(
        &row,
        caller_components(&metric_schema(), IsBounded::Unbounded),
        Some("caller_input"),
    ))
    .await;

    assert_eq!(resp.error, "", "unexpected error: {}", resp.error);
    let expanded = resp.transform.expect("expanded transform");
    let components = resp.components.expect("expanded components");
    assert!(expanded.outputs.contains_key("snapshots"));
    assert!(expanded.outputs.contains_key("rejected"));
    assert_eq!(expanded.outputs.len(), 2);
    let has_assign_shard = components
        .transforms
        .values()
        .any(|t| t.unique_name.contains("AssignShard"));
    assert!(has_assign_shard, "num_shards=4 adds AssignShard");
    let has_fail_on_error = components
        .transforms
        .values()
        .any(|t| t.unique_name.ends_with("/FailOnError"));
    assert!(!has_fail_on_error);
}

#[tokio::test]
async fn expand_rejects_invalid_inputs_schemas_and_partitions() {
    let valid_row = base_config("db.metrics").to_row().unwrap();

    // Missing input tag.
    let missing = run_expand(schema_transform_request(
        &valid_row,
        caller_components(&metric_schema(), IsBounded::Bounded),
        None,
    ))
    .await;
    assert!(
        missing.error.contains("no input tagged 'input'"),
        "{}",
        missing.error
    );

    // Input PCollection without a row schema (plain bytes coder).
    let p = Pipeline::new();
    let bytes_coder = <Vec<u8> as beam::coders::DefaultCoder>::register_coder(&p);
    let _ = p.add_pcollection::<Vec<u8>>("caller_input", &bytes_coder, IsBounded::Bounded);
    let mut non_row_components = p.to_proto().components.unwrap_or_default();
    let bytes_pcoll = non_row_components
        .pcollections
        .values()
        .next()
        .cloned()
        .unwrap();
    non_row_components.pcollections.clear();
    non_row_components
        .pcollections
        .insert("caller_input".to_string(), bytes_pcoll);
    let no_schema = run_expand(schema_transform_request(
        &valid_row,
        non_row_components,
        Some("caller_input"),
    ))
    .await;
    assert!(
        no_schema.error.contains("no row schema"),
        "{}",
        no_schema.error
    );

    // Unsupported Beam field type (INT16).
    let bad_schema = Schema::new(vec![Field::new("small", FieldType::int16())]);
    let unsupported_type = run_expand(schema_transform_request(
        &valid_row,
        caller_components(&bad_schema, IsBounded::Bounded),
        Some("caller_input"),
    ))
    .await;
    assert!(
        unsupported_type.error.contains("small"),
        "{}",
        unsupported_type.error
    );

    // Partition field naming a missing column.
    let bad_partition = IcebergWriteConfig {
        partition_fields: Some(vec!["hour(no_such_col)".to_string()]),
        ..base_config("db.metrics")
    }
    .to_row()
    .unwrap();
    let missing_col = run_expand(schema_transform_request(
        &bad_partition,
        caller_components(&metric_schema(), IsBounded::Bounded),
        Some("caller_input"),
    ))
    .await;
    assert!(
        missing_col.error.contains("no_such_col"),
        "{}",
        missing_col.error
    );

    // error_handling.output colliding with 'snapshots'.
    let colliding_output = IcebergWriteConfig {
        catalog_properties: Some(catalog_props(&[("uri", CATALOG_URI)])),
        error_handling: Some(ErrorHandling {
            output: "snapshots".to_string(),
        }),
        ..base_config("db.metrics")
    }
    .to_row()
    .unwrap();
    let collided = run_expand(schema_transform_request(
        &colliding_output,
        caller_components(&metric_schema(), IsBounded::Bounded),
        Some("caller_input"),
    ))
    .await;
    assert!(collided.error.contains("snapshots"), "{}", collided.error);
}
