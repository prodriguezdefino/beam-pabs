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

use beam::coders::{
    PaneInfo, URN_BYTES, URN_GLOBAL_WINDOW, URN_KV, URN_LENGTH_PREFIX, URN_STRING_UTF8, URN_VARINT,
    URN_WINDOWED_VALUE, VarIntCoder, WindowedHeader,
};
use harness::bundle_processor::TransformFn;
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
