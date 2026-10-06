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

//! Tests for the coder-driven formatter behind `beam:transform:to_string:v1`. Runners use
//! it to render sampled elements, for example in Dataflow data sampling.

use std::collections::HashMap;

use beam::coders::{
    ElementFormatter, PaneInfo, URN_BOOL, URN_BYTES, URN_DOUBLE, URN_GLOBAL_WINDOW,
    URN_INTERVAL_WINDOW, URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_STRING_UTF8,
    URN_VARINT, URN_WINDOWED_VALUE, VarIntCoder, WindowedHeader,
};
use beam::pipeline::{URN_TO_STRING, standard_capabilities};
use model::pipeline as proto;

fn coder(urn: &str, components: &[&str]) -> proto::Coder {
    proto::Coder {
        spec: Some(proto::FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: components.iter().map(|c| (*c).to_string()).collect(),
    }
}

/// Every coder the tests need, by id.
fn coders() -> HashMap<String, proto::Coder> {
    [
        ("str", coder(URN_STRING_UTF8, &[])),
        ("bytes", coder(URN_BYTES, &[])),
        ("varint", coder(URN_VARINT, &[])),
        ("double", coder(URN_DOUBLE, &[])),
        ("bool", coder(URN_BOOL, &[])),
        ("gw", coder(URN_GLOBAL_WINDOW, &[])),
        ("iw", coder(URN_INTERVAL_WINDOW, &[])),
        ("kv_str_varint", coder(URN_KV, &["str", "varint"])),
        ("iter_varint", coder(URN_ITERABLE, &["varint"])),
        ("nullable_str", coder(URN_NULLABLE, &["str"])),
        ("custom", coder("beam:coder:java:custom:v1", &[])),
        ("lp_custom", coder(URN_LENGTH_PREFIX, &["custom"])),
        ("wv_str_iw", coder(URN_WINDOWED_VALUE, &["str", "iw"])),
        ("cyclic", coder(URN_KV, &["cyclic", "cyclic"])),
    ]
    .into_iter()
    .map(|(id, c)| (id.to_string(), c))
    .collect()
}

fn nested_str(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    VarIntCoder::encode_varint(s.len() as i64, &mut out).expect("writing to a Vec succeeds");
    out.extend_from_slice(s.as_bytes());
    out
}

fn varint(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    VarIntCoder::encode_varint(v, &mut out).expect("writing to a Vec succeeds");
    out
}

fn format(coder_id: &str, bytes: &[u8]) -> String {
    ElementFormatter::new(coder_id, &coders()).format(bytes)
}

#[test]
fn formats_primitives() {
    assert_eq!(format("str", &nested_str("hello")), "hello");
    assert_eq!(format("varint", &varint(-42)), "-42");
    assert_eq!(format("double", &1.0f64.to_be_bytes()), "1.0");
    assert_eq!(format("bool", &[1]), "true");
    assert_eq!(format("bytes", &[2, b'a', 0xff]), "b\"a\\xff\"");
    assert_eq!(format("gw", &[]), "GlobalWindow");
}

#[test]
fn formats_composites_with_quoted_strings() {
    let kv = [nested_str("k"), varint(7)].concat();
    assert_eq!(format("kv_str_varint", &kv), "(\"k\", 7)");

    let iter = [3i32.to_be_bytes().to_vec(), varint(1), varint(2), varint(3)].concat();
    assert_eq!(format("iter_varint", &iter), "[1, 2, 3]");

    // Chunked encoding with a continuation token.
    let chunked = [
        (-1i32).to_be_bytes().to_vec(),
        varint(1),
        varint(9),
        varint(-1),
        nested_str("tok"),
    ]
    .concat();
    assert_eq!(format("iter_varint", &chunked), "[9, ...]");

    assert_eq!(format("nullable_str", &[0]), "null");
    assert_eq!(
        format("nullable_str", &[[1].as_slice(), &nested_str("x")].concat()),
        "x"
    );
}

#[test]
fn formats_windows_and_windowed_values() {
    let end: i64 = 2_000;
    let interval = [
        ((end as u64) ^ (1 << 63)).to_be_bytes().to_vec(),
        varint(1_000),
    ]
    .concat();
    assert_eq!(format("iw", &interval), "[1000, 2000)");

    let header = WindowedHeader::new(1_500, &[interval], PaneInfo::NO_FIRING);
    let wv = [header.as_bytes().to_vec(), nested_str("v")].concat();
    let rendered = format("wv_str_iw", &wv);
    assert!(
        rendered.starts_with("\"v\" @ 1500 in [[1000, 2000)]"),
        "{rendered}"
    );
}

/// Coders the SDK cannot interpret arrive length-prefixed; their bytes are shown escaped.
#[test]
fn unknown_coders_render_as_escaped_bytes() {
    assert_eq!(format("lp_custom", b"\x03a\x00b"), "b\"a\\x00b\"");
    assert_eq!(format("missing", b"raw"), "b\"raw\"");
}

/// Mismatched bytes and hostile coder graphs never fail or overflow the stack.
#[test]
fn malformed_input_is_rendered_not_failed() {
    assert_eq!(format("str", b"\x09ab"), "<undecodable b\"\\tab\">");
    let rendered = format("cyclic", b"\x01a\x01b");
    assert!(!rendered.is_empty());
}

#[test]
fn to_string_is_advertised() {
    assert!(standard_capabilities().iter().any(|c| c == URN_TO_STRING));
}
