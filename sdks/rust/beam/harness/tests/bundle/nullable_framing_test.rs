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

//! `beam:coder:nullable:v1` elements at the data ports of a bundle.
//!
//! The SDK encodes `Option<T>` as a presence byte, then the value. A runner that knows the
//! component coder sends the same bytes. A runner that does not know it (for example a Row)
//! wraps the component in `beam:coder:length_prefix:v1`, so a present value also has a
//! VarInt length on the wire. Each test sends wire bytes in, checks the SDK bytes that the
//! transform sees, and checks that the sink writes the wire bytes again.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beam::coders::{
    PaneInfo, RowCoder, URN_GLOBAL_WINDOW, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_ROW,
    URN_STRING_UTF8, URN_VARINT, URN_WINDOWED_VALUE, VarIntCoder, WindowedHeader,
};
use beam::internals::{ElementSink, TransformFn};
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use model::pipeline as proto_pipeline;

use crate::common;
use common::{DescriptorBuilder, SINK_ID, STAGE_ID, run_bundle};

/// The source and sink coder: `WindowedValue<coder_element, GlobalWindow>`.
const CODER_WINDOWED: &str = "coder_windowed";
/// A coder that only the SDK knows. The runner sends it behind a length prefix.
const URN_SDK_ONLY: &str = "beam:coder:rust:test_only:v1";

fn coder(urn: &str, components: &[&str]) -> proto_pipeline::Coder {
    coder_with_payload(urn, components, Vec::new())
}

fn coder_with_payload(urn: &str, components: &[&str], payload: Vec<u8>) -> proto_pipeline::Coder {
    proto_pipeline::Coder {
        spec: Some(proto_pipeline::FunctionSpec {
            urn: urn.to_string(),
            payload,
        }),
        component_coder_ids: components.iter().map(|c| c.to_string()).collect(),
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("name", FieldType::string()),
    ]))
}

/// The leaf coders, plus `coder_element` and the windowed port coder around it.
fn coders(element: proto_pipeline::Coder) -> HashMap<String, proto_pipeline::Coder> {
    HashMap::from([
        ("coder_varint".to_string(), coder(URN_VARINT, &[])),
        ("coder_string".to_string(), coder(URN_STRING_UTF8, &[])),
        (
            "coder_row".to_string(),
            coder_with_payload(URN_ROW, &[], schema().to_proto_bytes()),
        ),
        ("coder_sdk_only".to_string(), coder(URN_SDK_ONLY, &[])),
        (
            "coder_prefixed_row".to_string(),
            coder(URN_LENGTH_PREFIX, &["coder_row"]),
        ),
        (
            "coder_prefixed_sdk_only".to_string(),
            coder(URN_LENGTH_PREFIX, &["coder_sdk_only"]),
        ),
        (
            "coder_nullable_varint".to_string(),
            coder(URN_NULLABLE, &["coder_varint"]),
        ),
        (
            "coder_nullable_prefixed_row".to_string(),
            coder(URN_NULLABLE, &["coder_prefixed_row"]),
        ),
        (
            "coder_global_window".to_string(),
            coder(URN_GLOBAL_WINDOW, &[]),
        ),
        ("coder_element".to_string(), element),
        (
            CODER_WINDOWED.to_string(),
            coder(
                URN_WINDOWED_VALUE,
                &["coder_element", "coder_global_window"],
            ),
        ),
    ])
}

fn varint(value: usize) -> Vec<u8> {
    let mut out = Vec::new();
    VarIntCoder::encode_varint(value as i64, &mut out).expect("encoding into a Vec cannot fail");
    out
}

fn row(id: i64, name: &str) -> Vec<u8> {
    let values = vec![
        Some(FieldValue::Int64(id)),
        Some(FieldValue::String(name.to_string())),
    ];
    let row = Row::new(schema(), values).expect("the values match the schema");
    let mut out = Vec::new();
    RowCoder::encode_row(&row, &mut out).expect("encoding into a Vec cannot fail");
    out
}

const ABSENT: [u8; 1] = [0x00];

fn present(value: &[u8]) -> Vec<u8> {
    [&[0x01][..], value].concat()
}

/// A present value behind a runner length prefix.
fn present_prefixed(value: &[u8]) -> Vec<u8> {
    [&[0x01][..], &varint(value.len()), value].concat()
}

fn nested_string(s: &str) -> Vec<u8> {
    [varint(s.len()), s.as_bytes().to_vec()].concat()
}

/// Each element of `wire` after a global window header, as on the data channel.
fn with_headers(wire: &[Vec<u8>]) -> Vec<u8> {
    let header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
    wire.iter()
        .flat_map(|element| [header.as_bytes(), element].concat())
        .collect()
}

/// Sends `wire` elements through a transform that forwards each element. Returns the
/// elements that the transform sees and the bytes that the sink writes.
async fn round_trip(
    descriptor_id: &str,
    element: proto_pipeline::Coder,
    wire: &[Vec<u8>],
) -> (Vec<Vec<u8>>, Vec<u8>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler: TransformFn = Arc::new({
        let seen = Arc::clone(&seen);
        move |element: &[u8], sink: &mut dyn ElementSink| {
            seen.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(element.to_vec());
            sink.push(element.to_vec())
        }
    });
    let descriptor = DescriptorBuilder::new(descriptor_id)
        .with_coders(CODER_WINDOWED, coders(element))
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink_with_coder(SINK_ID, "pcoll_output", CODER_WINDOWED)
        .build();
    let run = run_bundle(
        HashMap::from([(STAGE_ID.to_string(), handler)]),
        descriptor,
        vec![with_headers(wire)],
    )
    .await;
    run.bundle_response();
    let written = run.sink_bytes.get(SINK_ID).cloned().unwrap_or_default();
    let seen = seen.lock().unwrap_or_else(|e| e.into_inner()).clone();
    (seen, written)
}

#[tokio::test]
async fn nullable_varint_elements_cross_the_ports_unchanged() {
    let wire = vec![ABSENT.to_vec(), present(&varint(300))];
    let (seen, written) = round_trip(
        "desc_nullable_varint",
        coder(URN_NULLABLE, &["coder_varint"]),
        &wire,
    )
    .await;
    assert_eq!(seen, wire);
    assert_eq!(written, with_headers(&wire));
}

#[tokio::test]
async fn nullable_string_elements_cross_the_ports_unchanged() {
    let wire = vec![present(&nested_string("ab")), ABSENT.to_vec()];
    let (seen, written) = round_trip(
        "desc_nullable_string",
        coder(URN_NULLABLE, &["coder_string"]),
        &wire,
    )
    .await;
    assert_eq!(seen, wire);
    assert_eq!(written, with_headers(&wire));
}

/// The runner sends a present Row with a length. The transform sees the Row without it.
#[tokio::test]
async fn nullable_prefixed_rows_lose_the_length_inbound_and_get_it_outbound() {
    let first = row(7, "first");
    let wire = vec![present_prefixed(&first), ABSENT.to_vec()];
    let (seen, written) = round_trip(
        "desc_nullable_prefixed_row",
        coder(URN_NULLABLE, &["coder_prefixed_row"]),
        &wire,
    )
    .await;
    assert_eq!(seen, vec![present(&first), ABSENT.to_vec()]);
    assert_eq!(written, with_headers(&wire));
}

/// The last value of an element takes the rest of the bytes, so the sink frames a coder
/// that only the SDK knows.
#[tokio::test]
async fn nullable_prefixed_sdk_only_values_get_the_length_outbound() {
    let value = b"opaque".to_vec();
    let wire = vec![present_prefixed(&value), ABSENT.to_vec()];
    let (seen, written) = round_trip(
        "desc_nullable_prefixed_sdk_only",
        coder(URN_NULLABLE, &["coder_prefixed_sdk_only"]),
        &wire,
    )
    .await;
    assert_eq!(seen, vec![present(&value), ABSENT.to_vec()]);
    assert_eq!(written, with_headers(&wire));
}

#[tokio::test]
async fn kv_values_that_are_nullable_cross_the_ports_unchanged() {
    let key = nested_string("k");
    let wire = vec![
        [key.clone(), present(&varint(5))].concat(),
        [key, ABSENT.to_vec()].concat(),
    ];
    let (seen, written) = round_trip(
        "desc_kv_nullable_varint",
        coder(URN_KV, &["coder_string", "coder_nullable_varint"]),
        &wire,
    )
    .await;
    assert_eq!(seen, wire);
    assert_eq!(written, with_headers(&wire));
}

#[tokio::test]
async fn kv_values_that_are_nullable_prefixed_rows_are_reframed() {
    let first = row(1, "one");
    let wire = vec![
        [varint(3), present_prefixed(&first)].concat(),
        [varint(4), ABSENT.to_vec()].concat(),
    ];
    let (seen, written) = round_trip(
        "desc_kv_nullable_prefixed_row",
        coder(URN_KV, &["coder_varint", "coder_nullable_prefixed_row"]),
        &wire,
    )
    .await;
    assert_eq!(
        seen,
        vec![
            [varint(3), present(&first)].concat(),
            [varint(4), ABSENT.to_vec()].concat(),
        ]
    );
    assert_eq!(written, with_headers(&wire));
}
