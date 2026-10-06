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

//! Tests for `beam:coder:param_windowed_value:v1`.
//!
//! This coder splits a windowed value in two. The element goes on the wire. The timestamp,
//! windows and pane are constants that the coder parses once from the coder proto payload.
//! This split is the risk. A payload misparse is silent: every element that the coder
//! produces then has the same wrong timestamp. Also, a skip path that expects the usual
//! windowed-value header would consume the element as a header.
//!
//! The canonical wire examples are in `standard_coders.yaml`, and `standard_coders_test`
//! runs them. These tests check the behavior around them: payload validation, payload
//! retention, and the coder-walking helpers that the harness uses to find keys and schemas.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use beam::coders::{
    Coder, Context, GlobalWindow, GlobalWindowCoder, IntervalWindow, IntervalWindowCoder, PaneInfo,
    ParamWindowedValueCoder, StringUtf8Coder, Timing, URN_BYTES, URN_GLOBAL_WINDOW, URN_KV,
    URN_LENGTH_PREFIX, URN_PARAM_WINDOWED_VALUE, URN_ROW, URN_STRING_UTF8, URN_VARINT, VarIntCoder,
    WindowedValue, extract_kv_key_bytes, extract_row_schema, skip_coder_value,
};
use beam::schema::{Field, FieldType, Schema};
use model::pipeline::{Coder as ProtoCoder, FunctionSpec};

/// Byte appended after an encoded value so tests can prove a skip stopped there.
const SENTINEL: u8 = 0xAB;

fn coder(urn: &str, components: &[&str], payload: Vec<u8>) -> ProtoCoder {
    ProtoCoder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload,
        }),
        component_coder_ids: components.iter().map(|s| s.to_string()).collect(),
    }
}

/// Builds a `param_windowed_value` payload: a windowed value of `bytes` whose element is an
/// empty placeholder.
fn payload<W, WC: Coder<W>>(
    window_coder: &WC,
    timestamp_millis: i64,
    windows: &[W],
    pane: PaneInfo,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&((timestamp_millis as u64) ^ (1u64 << 63)).to_be_bytes());
    payload.extend_from_slice(&(windows.len() as i32).to_be_bytes());
    for w in windows {
        window_coder
            .encode(w, &mut payload, Context::Nested)
            .unwrap();
    }
    pane.encode(false, &mut payload).unwrap();
    payload.push(0); // Placeholder element: empty bytes value.
    payload
}

// ---------------------------------------------------------------------------
// Payload parsing
// ---------------------------------------------------------------------------

#[test]
fn payload_supplies_the_constants() {
    let windows = vec![IntervalWindow::from_end_and_span(20, 10)];
    let bytes = payload(&IntervalWindowCoder, 1_000, &windows, PaneInfo::NO_FIRING);

    let coder = ParamWindowedValueCoder::<i64, _, IntervalWindow>::from_payload(
        VarIntCoder,
        &IntervalWindowCoder,
        &bytes,
    )
    .expect("payload should parse");

    let constants = coder.constants();
    assert_eq!(constants.timestamp_millis, 1_000);
    assert_eq!(constants.windows, windows);
    assert_eq!(constants.pane, PaneInfo::NO_FIRING);
}

#[test]
fn payload_is_retained_verbatim() {
    // The coder proto must re-emit byte for byte. Keeping the payload makes this lossless.
    // Constants may not round trip: an `Unknown`-timed pane always re-encodes without its
    // indices.
    let bytes = payload(&GlobalWindowCoder, 42, &[GlobalWindow], PaneInfo::NO_FIRING);

    let coder = ParamWindowedValueCoder::<i64, _, GlobalWindow>::from_payload(
        VarIntCoder,
        &GlobalWindowCoder,
        &bytes,
    )
    .unwrap();

    assert_eq!(coder.payload(), Some(bytes.as_slice()));
}

/// The payload for a global-window, NO_FIRING constant at timestamp 0, spelled
/// out byte by byte rather than produced by this file's `payload()` helper.
const GLOBAL_NO_FIRING_AT_EPOCH: [u8; 14] = [
    0x80, 0, 0, 0, 0, 0, 0, 0, // Timestamp 0 with flipped sign bit.
    0, 0, 0, 1,    // One global window (0 bytes).
    0x0F, // Pane: first | last | timing UNKNOWN (3 << 2).
    0x00, // Placeholder element: empty nested bytes value.
];

#[test]
fn a_golden_payload_parses() {
    // This test also checks the test helper, so the other payload tests build what they claim.
    assert_eq!(
        payload(&GlobalWindowCoder, 0, &[GlobalWindow], PaneInfo::NO_FIRING),
        GLOBAL_NO_FIRING_AT_EPOCH
    );

    let coder = ParamWindowedValueCoder::<i64, _, GlobalWindow>::from_payload(
        VarIntCoder,
        &GlobalWindowCoder,
        &GLOBAL_NO_FIRING_AT_EPOCH,
    )
    .expect("golden payload");
    let constants = coder.constants();
    assert_eq!(constants.timestamp_millis, 0);
    assert_eq!(constants.windows, vec![GlobalWindow]);
    assert_eq!(constants.pane, PaneInfo::NO_FIRING);
}

#[test]
fn a_payload_without_the_placeholder_element_is_rejected() {
    // Other SDKs decode the payload as a windowed value of bytes, so a bare header is
    // malformed. Accepting it would hide a producer bug until the payload reaches them.
    let header_only = &GLOBAL_NO_FIRING_AT_EPOCH[..13];
    let err = ParamWindowedValueCoder::<i64, _, GlobalWindow>::from_payload(
        VarIntCoder,
        &GlobalWindowCoder,
        header_only,
    )
    .expect_err("the placeholder element is missing");
    assert!(
        matches!(&err, beam::coders::CoderError::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof),
        "{err:?}"
    );
}

#[test]
fn payload_with_trailing_bytes_is_accepted() {
    // Accept trailing bytes after the placeholder element. Other SDKs decode the payload with
    // a windowed value coder and do not check for trailing bytes. A rejection here would fail
    // pipelines that other SDKs accept.
    let mut bytes = payload(&GlobalWindowCoder, 42, &[GlobalWindow], PaneInfo::NO_FIRING);
    bytes.push(SENTINEL);

    let coder = ParamWindowedValueCoder::<i64, _, GlobalWindow>::from_payload(
        VarIntCoder,
        &GlobalWindowCoder,
        &bytes,
    )
    .expect("trailing bytes must be tolerated");

    assert_eq!(coder.constants().timestamp_millis, 42);
}

#[test]
fn payload_with_negative_window_count_is_rejected() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(1u64 << 63).to_be_bytes());
    payload.extend_from_slice(&(-1i32).to_be_bytes());

    let err = ParamWindowedValueCoder::<i64, _, GlobalWindow>::from_payload(
        VarIntCoder,
        &GlobalWindowCoder,
        &payload,
    )
    .expect_err("a negative window count must be rejected");

    assert!(
        err.to_string().contains("declares -1 windows"),
        "unexpected: {err}"
    );
}

#[test]
fn payload_with_multi_byte_pane_parses() {
    // This pane has two trailing VarInt indices. A parser that reads only the leading byte
    // stops two bytes short. The placeholder element is then not at the expected position,
    // and the parser rejects the payload as truncated.
    let pane = PaneInfo::new(false, true, Timing::OnTime, 30, 40);
    let windows = [IntervalWindow::from_end_and_span(20, 10)];
    let bytes = payload(&IntervalWindowCoder, 1_000, &windows, pane);
    assert_eq!(
        bytes.len(),
        payload(&IntervalWindowCoder, 1_000, &windows, PaneInfo::NO_FIRING).len() + 2,
        "the pane must carry two trailing VarInt indices"
    );

    let coder = ParamWindowedValueCoder::<i64, _, IntervalWindow>::from_payload(
        VarIntCoder,
        &IntervalWindowCoder,
        &bytes,
    )
    .expect("a payload with a multi-byte pane should parse");

    let constants = coder.constants();
    assert_eq!(constants.timestamp_millis, 1_000);
    assert_eq!(constants.pane, pane);
    assert_eq!(constants.windows, windows);
}

// ---------------------------------------------------------------------------
// Wire encoding
// ---------------------------------------------------------------------------

#[test]
fn only_the_element_is_encoded_and_decode_restores_the_constants() {
    // The element's own timestamp, windows and pane are dropped: decode returns the constants.
    let coder = ParamWindowedValueCoder::<i64, _, GlobalWindow>::new(
        VarIntCoder,
        WindowedValue::new((), 1_000, vec![GlobalWindow], PaneInfo::NO_FIRING),
    );
    let mut buf = Vec::new();
    coder
        .encode(
            &WindowedValue::new(
                7,
                999_999,
                vec![GlobalWindow],
                PaneInfo::ON_TIME_AND_ONLY_FIRING,
            ),
            &mut buf,
            Context::Nested,
        )
        .unwrap();
    assert_eq!(buf, vec![0x07], "no windowed value header may be written");
    assert_eq!(
        coder.decode(&mut &buf[..], Context::Nested).unwrap(),
        WindowedValue::new(7, 1_000, vec![GlobalWindow], PaneInfo::NO_FIRING)
    );

    let windows = vec![IntervalWindow::from_end_and_span(20, 10)];
    let pane = PaneInfo::new(false, true, Timing::OnTime, 30, 40);
    let coder = ParamWindowedValueCoder::<i64, _, IntervalWindow>::new(
        VarIntCoder,
        WindowedValue::new((), 1_000, windows.clone(), pane),
    );
    assert_eq!(
        coder.decode(&mut &[0x02][..], Context::Nested).unwrap(),
        WindowedValue::new(2, 1_000, windows, pane)
    );
}

// ---------------------------------------------------------------------------
// Harness coder walking
//
// The skip assertions check the exact cursor position with a trailing sentinel. An over-skip
// or under-skip does not raise an error: it mis-keys user state and timers silently.
// ---------------------------------------------------------------------------

/// A coder table wiring `param_windowed_value(kv(string, varint), global_window)`.
fn kv_param_windowed_coders() -> HashMap<String, ProtoCoder> {
    HashMap::from([
        (
            "pwv".to_string(),
            coder(URN_PARAM_WINDOWED_VALUE, &["kv", "gwindow"], Vec::new()),
        ),
        ("kv".to_string(), coder(URN_KV, &["key", "val"], Vec::new())),
        ("key".to_string(), coder(URN_STRING_UTF8, &[], Vec::new())),
        ("val".to_string(), coder(URN_VARINT, &[], Vec::new())),
        (
            "gwindow".to_string(),
            coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
    ])
}

#[test]
fn skip_consumes_the_element_and_no_header() {
    let coders = kv_param_windowed_coders();

    let mut data = Vec::new();
    StringUtf8Coder
        .encode(&"key".to_string(), &mut data, Context::Nested)
        .unwrap();
    VarIntCoder::encode_varint(42, &mut data).unwrap();
    let element_len = data.len();
    data.push(SENTINEL);

    let mut cursor = Cursor::new(data.as_slice());
    skip_coder_value(&mut cursor, "pwv", &coders, false).expect("skip should succeed");

    assert_eq!(
        cursor.position() as usize,
        element_len,
        "skip must stop exactly at the sentinel"
    );
}

#[test]
fn skip_handles_a_length_prefixed_element() {
    // A runner that cannot interpret the element coder wraps it in a length prefix. Nothing
    // separates the two, so the wrapper is directly under the param_windowed_value coder.
    let coders = HashMap::from([
        (
            "pwv".to_string(),
            coder(URN_PARAM_WINDOWED_VALUE, &["lp", "gwindow"], Vec::new()),
        ),
        (
            "lp".to_string(),
            coder(URN_LENGTH_PREFIX, &["bytes"], Vec::new()),
        ),
        ("bytes".to_string(), coder(URN_BYTES, &[], Vec::new())),
        (
            "gwindow".to_string(),
            coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
    ]);

    let data = [0x03, b'a', b'b', b'c', SENTINEL];
    let mut cursor = Cursor::new(&data[..]);
    skip_coder_value(&mut cursor, "pwv", &coders, false).expect("skip should succeed");

    assert_eq!(cursor.position(), 4, "skip must stop at the sentinel");
}

#[test]
fn extract_kv_key_bytes_sees_through_the_wrapper() {
    let coders = kv_param_windowed_coders();

    let mut expected_key = Vec::new();
    StringUtf8Coder
        .encode(&"key".to_string(), &mut expected_key, Context::Nested)
        .unwrap();

    let mut data = expected_key.clone();
    VarIntCoder::encode_varint(42, &mut data).unwrap();

    assert_eq!(
        extract_kv_key_bytes(&data, "pwv", &coders),
        Some(expected_key)
    );
}

#[test]
fn extract_row_schema_sees_through_the_wrapper() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("name", FieldType::string()),
    ]));
    let coders = HashMap::from([
        (
            "pwv".to_string(),
            coder(URN_PARAM_WINDOWED_VALUE, &["row", "gwindow"], Vec::new()),
        ),
        (
            "row".to_string(),
            coder(URN_ROW, &[], schema.to_proto_bytes()),
        ),
        (
            "gwindow".to_string(),
            coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
    ]);

    assert_eq!(
        extract_row_schema("pwv", &coders).as_deref(),
        Some(schema.as_ref())
    );
}
