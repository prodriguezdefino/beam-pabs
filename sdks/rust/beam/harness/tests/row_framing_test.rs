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

//! Runner-inserted length prefixes around nested Rows, at the bundle's data ports.
//!
//! A runner that cannot read a nested coder (e.g. Prism with `beam:coder:row:v1`) wraps it
//! in `beam:coder:length_prefix:v1`. The SDK encoding has no such prefix, so the harness
//! strips it inbound and adds it outbound. These tests nest a string in place of a Row:
//! the reframing is the same and needs no schema.

use std::collections::HashMap;
use std::sync::Arc;

use beam::coders::{
    PaneInfo, RowCoder, URN_BYTES, URN_GLOBAL_WINDOW, URN_KV, URN_LENGTH_PREFIX,
    URN_PARAM_WINDOWED_VALUE, URN_ROW, URN_STRING_UTF8, URN_VARINT, URN_WINDOWED_VALUE,
    VarIntCoder, WindowedHeader,
};
use beam::internals::{DynamicSplitResult, ResidualApplication, TransformFn};
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use harness::bundle_processor::{PortFraming, RootEncoder};
use model::fn_execution::{BundleApplication, ProcessBundleDescriptor};
use model::pipeline as proto_pipeline;

mod common;
use common::{
    CODER_RAW, DescriptorBuilder, Observer, SINK_ID, STAGE_ID, identity_handlers, run_bundle,
};

/// `KV<varint, length_prefix<string>>`: the element coder as the runner rewrote it.
const CODER_KV_PREFIXED: &str = "coder_kv_prefixed";
/// `WindowedValue<KV<varint, length_prefix<string>>, GlobalWindow>`.
const CODER_WINDOWED_KV: &str = "coder_windowed_kv";
/// `WindowedValue<length_prefix<KV<varint, length_prefix<string>>>, GlobalWindow>`: the
/// whole element prefixed too, as runners do for an element coder they cannot read.
const CODER_WINDOWED_ROOT_PREFIXED_KV: &str = "coder_windowed_root_prefixed_kv";

fn coder(urn: &str, components: &[&str]) -> proto_pipeline::Coder {
    proto_pipeline::Coder {
        spec: Some(proto_pipeline::FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: components.iter().map(|c| c.to_string()).collect(),
    }
}

/// The coders of the runner-rewritten element, at both port shapes.
fn prefixed_kv_coders() -> HashMap<String, proto_pipeline::Coder> {
    HashMap::from([
        (CODER_RAW.to_string(), coder(URN_BYTES, &[])),
        ("coder_string".to_string(), coder(URN_STRING_UTF8, &[])),
        ("coder_varint".to_string(), coder(URN_VARINT, &[])),
        (
            "coder_global_window".to_string(),
            coder(URN_GLOBAL_WINDOW, &[]),
        ),
        (
            "coder_prefixed_string".to_string(),
            coder(URN_LENGTH_PREFIX, &["coder_string"]),
        ),
        (
            CODER_KV_PREFIXED.to_string(),
            coder(URN_KV, &["coder_varint", "coder_prefixed_string"]),
        ),
        (
            CODER_WINDOWED_KV.to_string(),
            coder(
                URN_WINDOWED_VALUE,
                &[CODER_KV_PREFIXED, "coder_global_window"],
            ),
        ),
        (
            "coder_root_prefixed_kv".to_string(),
            coder(URN_LENGTH_PREFIX, &[CODER_KV_PREFIXED]),
        ),
        (
            CODER_WINDOWED_ROOT_PREFIXED_KV.to_string(),
            coder(
                URN_WINDOWED_VALUE,
                &["coder_root_prefixed_kv", "coder_global_window"],
            ),
        ),
    ])
}

fn varint(value: usize) -> Vec<u8> {
    let mut out = Vec::new();
    VarIntCoder::encode_varint(value as i64, &mut out).expect("encoding into a Vec cannot fail");
    out
}

/// `value` behind a VarInt length.
fn length_prefixed(value: &[u8]) -> Vec<u8> {
    [varint(value.len()), value.to_vec()].concat()
}

/// A nested string, standing in for the nested Row.
fn nested_string(s: &str) -> Vec<u8> {
    length_prefixed(s.as_bytes())
}

/// The SDK-native encoding of `KV<key, value>`: no prefix around the value.
fn native_kv(key: usize, value: &str) -> Vec<u8> {
    [varint(key), nested_string(value)].concat()
}

/// The runner's encoding of the same `KV`: the value behind its own length prefix.
fn runner_kv(key: usize, value: &str) -> Vec<u8> {
    [varint(key), length_prefixed(&nested_string(value))].concat()
}

fn global_header() -> Vec<u8> {
    WindowedHeader::global(0, PaneInfo::NO_FIRING)
        .as_bytes()
        .to_vec()
}

#[tokio::test]
async fn inbound_nested_rows_have_their_runner_length_prefixes_stripped() {
    let observer = Observer::default();
    let handlers: HashMap<String, TransformFn> =
        HashMap::from([(STAGE_ID.to_string(), observer.handler())]);
    let descriptor = DescriptorBuilder::new("desc_inbound_nested_rows")
        .with_coders(CODER_WINDOWED_KV, prefixed_kv_coders())
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    let chunk = [
        global_header(),
        runner_kv(7, "row"),
        global_header(),
        runner_kv(300, "another row"),
    ]
    .concat();

    let run = run_bundle(handlers, descriptor, vec![chunk]).await;
    run.bundle_response();

    let delivered: Vec<Vec<u8>> = observer.seen().into_iter().map(|o| o.element).collect();
    assert_eq!(
        delivered,
        vec![native_kv(7, "row"), native_kv(300, "another row")]
    );
}

/// Under a root prefix, the outer length covers the reframed element.
#[tokio::test]
async fn outbound_nested_rows_get_the_runner_length_prefix() {
    let element = runner_kv(7, "row");
    let cases = [
        (
            "nested prefix only",
            CODER_WINDOWED_KV,
            [global_header(), element.clone()].concat(),
        ),
        (
            "nested prefix under a root prefix",
            CODER_WINDOWED_ROOT_PREFIXED_KV,
            [global_header(), length_prefixed(&element)].concat(),
        ),
    ];
    for (case, sink_coder, expected) in cases {
        let descriptor = DescriptorBuilder::new("desc_outbound_nested_rows")
            .with_coders(CODER_RAW, prefixed_kv_coders())
            .stage(STAGE_ID, "pcoll_input", "pcoll_output")
            .sink_with_coder(SINK_ID, "pcoll_output", sink_coder)
            .build();

        // A raw-bytes source hands the native encoding to the identity stage verbatim.
        let run = run_bundle(
            identity_handlers(&[STAGE_ID]),
            descriptor,
            vec![native_kv(7, "row")],
        )
        .await;
        run.bundle_response();

        assert_eq!(
            run.sink_bytes.get(SINK_ID).cloned().unwrap_or_default(),
            expected,
            "{case}"
        );
    }
}

#[tokio::test]
async fn sdf_residual_and_split_applications_get_the_runner_length_prefix() {
    let coders = prefixed_kv_coders();
    // A split application holds the windowed header and the payload in the SDK encoding.
    // The runner sends it back to the data port, so it must use the wire encoding.
    let native_app = [global_header(), native_kv(7, "row")].concat();
    let expected_runner_kv = runner_kv(7, "row");
    let cases = [
        (
            "nested prefix only",
            CODER_WINDOWED_KV,
            [global_header(), expected_runner_kv.clone()].concat(),
        ),
        (
            "nested prefix under a root prefix",
            CODER_WINDOWED_ROOT_PREFIXED_KV,
            [global_header(), length_prefixed(&expected_runner_kv)].concat(),
        ),
    ];
    let shared = Arc::new(coders.clone());
    cases.iter().for_each(|(case, source_coder, expected)| {
        let reframed = PortFraming::resolve(source_coder, Arc::clone(&shared))
            .and_then(|framing| framing.reframe(native_app.clone()))
            .expect("the application reframes");
        assert_eq!(&reframed, expected, "{case}");
    });

    // The data port gives the stage the SDK encoding of the reframed application again.
    let observer = Observer::default();
    let handlers: HashMap<String, TransformFn> =
        HashMap::from([(STAGE_ID.to_string(), observer.handler())]);
    let descriptor = DescriptorBuilder::new("desc_sdf_residual_roundtrip")
        .with_coders(CODER_WINDOWED_KV, coders)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    let run = run_bundle(handlers, descriptor, vec![cases[0].2.clone()]).await;
    run.bundle_response();

    let delivered: Vec<Vec<u8>> = observer.seen().into_iter().map(|o| o.element).collect();
    assert_eq!(delivered, vec![native_kv(7, "row")]);
}

fn split_application(element: Vec<u8>) -> BundleApplication {
    BundleApplication {
        transform_id: "sdf".to_string(),
        input_id: "in".to_string(),
        element,
        ..BundleApplication::default()
    }
}

fn split_residual(element: Vec<u8>) -> ResidualApplication {
    ResidualApplication {
        transform_id: "sdf".to_string(),
        input_id: "in".to_string(),
        element,
        output_watermark: None,
        delay: None,
    }
}

fn encoder() -> RootEncoder {
    let framing = PortFraming::resolve(
        CODER_WINDOWED_ROOT_PREFIXED_KV,
        Arc::new(prefixed_kv_coders()),
    )
    .expect("the port coder resolves");
    RootEncoder::new(&ProcessBundleDescriptor::default(), Arc::new(framing))
        .expect("root encoder builds")
}

/// A granted SDF split goes back to the runner, so its primary and every residual use the
/// wire encoding of the port.
#[test]
fn a_split_reframes_its_primary_and_every_residual() {
    let native_app = [global_header(), native_kv(7, "row")].concat();
    let expected = [global_header(), length_prefixed(&runner_kv(7, "row"))].concat();
    let split = DynamicSplitResult {
        primary: split_application(native_app.clone()),
        residuals: vec![
            split_residual(native_app.clone()),
            split_residual(native_app),
        ],
    };

    let roots = encoder().split(split).expect("the split reframes");

    assert_eq!(roots.primary_roots.len(), 1);
    assert_eq!(roots.primary_roots[0].element, expected);
    assert_eq!(roots.primary_roots[0].transform_id, "sdf");
    let residuals: Vec<Vec<u8>> = roots
        .residual_roots
        .iter()
        .filter_map(|r| r.application.as_ref().map(|a| a.element.clone()))
        .collect();
    assert_eq!(residuals, vec![expected.clone(), expected]);
}

/// The SDF already gave up the residual when a split does not reframe. The split fails, so the
/// bundle processor sends no split and fails the bundle, and the runner retries it.
#[test]
fn a_split_whose_application_does_not_reframe_fails() {
    // A windowed value header that ends after its first byte.
    let cut_off = global_header()[..1].to_vec();
    let split = DynamicSplitResult {
        primary: split_application([global_header(), native_kv(7, "row")].concat()),
        residuals: vec![split_residual(cut_off)],
    };

    let error = encoder()
        .split(split)
        .expect_err("a cut-off header does not reframe");

    assert!(
        error.to_string().contains("bundle application header"),
        "{error}"
    );
}

/// `WindowedValue<Row, GlobalWindow>`: rows with no length prefix, which the schema measures.
const CODER_WINDOWED_ROW: &str = "coder_windowed_row";

fn row_coders(schema_payload: Vec<u8>) -> HashMap<String, proto_pipeline::Coder> {
    let row = proto_pipeline::Coder {
        spec: Some(proto_pipeline::FunctionSpec {
            urn: URN_ROW.to_string(),
            payload: schema_payload,
        }),
        component_coder_ids: Vec::new(),
    };
    HashMap::from([
        (CODER_RAW.to_string(), coder(URN_BYTES, &[])),
        ("coder_row".to_string(), row),
        (
            "coder_global_window".to_string(),
            coder(URN_GLOBAL_WINDOW, &[]),
        ),
        (
            CODER_WINDOWED_ROW.to_string(),
            coder(URN_WINDOWED_VALUE, &["coder_row", "coder_global_window"]),
        ),
    ])
}

fn encoded_row(schema: &Arc<Schema>, id: i64, name: Option<&str>) -> Vec<u8> {
    let values = vec![
        Some(FieldValue::Int64(id)),
        name.map(|name| FieldValue::String(name.to_string())),
    ];
    let row = Row::new(Arc::clone(schema), values).expect("the values match the schema");
    let mut out = Vec::new();
    RowCoder::encode_row(&row, &mut out).expect("encoding into a Vec cannot fail");
    out
}

#[tokio::test]
async fn rows_with_no_length_prefix_are_measured_with_their_schema_across_chunks() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::nullable("name", FieldType::string()),
    ]));
    let rows = [
        encoded_row(&schema, 7, Some("first")),
        encoded_row(&schema, 300, None),
        encoded_row(&schema, -1, Some("third")),
    ];
    let stream = rows
        .iter()
        .flat_map(|row| [global_header(), row.clone()])
        .collect::<Vec<_>>()
        .concat();
    // The first chunk ends inside the second row, so that row waits for the next chunk.
    let split_at = global_header().len() * 2 + rows[0].len() + 2;
    let chunks = vec![stream[..split_at].to_vec(), stream[split_at..].to_vec()];

    let observer = Observer::default();
    let handlers: HashMap<String, TransformFn> =
        HashMap::from([(STAGE_ID.to_string(), observer.handler())]);
    let descriptor = DescriptorBuilder::new("desc_rows_by_schema")
        .with_coders(CODER_WINDOWED_ROW, row_coders(schema.to_proto_bytes()))
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    let run = run_bundle(handlers, descriptor, chunks).await;
    run.bundle_response();

    let delivered: Vec<Vec<u8>> = observer.seen().into_iter().map(|o| o.element).collect();
    assert_eq!(delivered, rows.to_vec());
}

#[test]
fn a_row_port_with_a_schema_that_does_not_decode_fails_to_resolve() {
    let coders = Arc::new(row_coders(vec![0xFF, 0xFF, 0xFF, 0xFF]));
    let error = PortFraming::resolve(CODER_WINDOWED_ROW, coders)
        .err()
        .expect("a bad schema fails when the processor is built");
    assert!(error.to_string().contains("coder_row"), "{error}");
}

#[test]
fn a_param_windowed_value_port_with_an_invalid_payload_fails_to_resolve() {
    let mut coders = row_coders(Vec::new());
    coders.insert(
        "coder_param".to_string(),
        proto_pipeline::Coder {
            spec: Some(proto_pipeline::FunctionSpec {
                urn: URN_PARAM_WINDOWED_VALUE.to_string(),
                payload: vec![0xFF],
            }),
            component_coder_ids: vec!["coder_row".to_string(), "coder_global_window".to_string()],
        },
    );
    let error = PortFraming::resolve("coder_param", Arc::new(coders))
        .err()
        .expect("an invalid payload fails when the port resolves");
    assert!(
        error.to_string().contains("param_windowed_value"),
        "{error}"
    );
}
