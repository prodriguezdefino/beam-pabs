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

//! Wire-traversal tests for [`skip_coder_value`] and related functions.
//!
//! The harness uses these functions to find the key boundary in an encoded element
//! without full decoding. Each skip operation must place the cursor on the exact next byte.
//! Incorrect offsets corrupt state and timer keys without raising errors. These tests
//! check the final cursor position against a trailing sentinel byte, not only for `Ok`.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use beam::coders::{
    RowCoder, URN_BOOL, URN_BYTES, URN_DOUBLE, URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW,
    URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_PARAM_WINDOWED_VALUE, URN_ROW,
    URN_STRING_UTF8, URN_TIMER, URN_VARINT, URN_WINDOWED_VALUE, VarIntCoder, extract_kv_key_bytes,
    extract_row_schema, skip_coder_value, skip_pane_info,
};
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use model::pipeline::{Coder as ProtoCoder, FunctionSpec};

/// Sentinel byte appended to verify that skip operations stop at the exact boundary.
const SENTINEL: u8 = 0xAB;

/// Builds a coder protocol buffer with the given URN, components, and payload.
fn coder(urn: &str, components: &[&str], payload: Vec<u8>) -> ProtoCoder {
    ProtoCoder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload,
        }),
        component_coder_ids: components.iter().map(|s| s.to_string()).collect(),
    }
}

/// Returns a coder table containing primitive coders for tests.
fn base_coders() -> HashMap<String, ProtoCoder> {
    HashMap::from([
        ("varint".to_string(), coder(URN_VARINT, &[], Vec::new())),
        ("bytes".to_string(), coder(URN_BYTES, &[], Vec::new())),
        (
            "string".to_string(),
            coder(URN_STRING_UTF8, &[], Vec::new()),
        ),
        ("bool".to_string(), coder(URN_BOOL, &[], Vec::new())),
        ("double".to_string(), coder(URN_DOUBLE, &[], Vec::new())),
        (
            "gwindow".to_string(),
            coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
        (
            "iwindow".to_string(),
            coder(URN_INTERVAL_WINDOW, &[], Vec::new()),
        ),
    ])
}

/// Asserts that skipping `coder_id` over `body` consumes `body` and stops at the sentinel byte.
fn assert_skips_exactly(
    body: &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
    nested: bool,
) {
    let mut encoded = body.to_vec();
    encoded.push(SENTINEL);

    let mut cursor = Cursor::new(encoded.as_slice());
    skip_coder_value(&mut cursor, coder_id, coders, nested).expect("skip should succeed");

    assert_eq!(
        cursor.position() as usize,
        body.len(),
        "skip of '{coder_id}' stopped at the wrong offset"
    );
}

/// Encodes `bytes` with a VarInt length prefix, the nested form of bytes and strings.
fn length_prefixed(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![bytes.len() as u8];
    out.extend_from_slice(bytes);
    out
}

#[test]
fn primitives_skip_exactly_their_encoding() {
    let coders = base_coders();

    assert_skips_exactly(&[0x96, 0x01], "varint", &coders, true); // varint 150
    assert_skips_exactly(&[0x00; 8], "double", &coders, true);
    assert_skips_exactly(&[0x01], "bool", &coders, true);
    assert_skips_exactly(&length_prefixed(b"hello"), "string", &coders, true);
    assert_skips_exactly(&length_prefixed(b"\x00\x01"), "bytes", &coders, true);
    assert_skips_exactly(&[], "gwindow", &coders, true);
    // IntervalWindow format: an 8-byte end timestamp followed by a VarInt duration.
    assert_skips_exactly(&[0, 0, 0, 0, 0, 0, 0, 0, 0x0A], "iwindow", &coders, true);
}

#[test]
fn undelimitable_values_consume_the_rest_of_the_slice() {
    // Unprefixed top-level bytes run to the end of the element. For a value it cannot
    // interpret, the harness consumes the rest and does not return a false key boundary.
    let mut coders = base_coders();
    coders.insert(
        "custom".to_string(),
        coder("beam:coder:not_a_real_coder:v1", &[], Vec::new()),
    );
    coders.insert("row".to_string(), coder(URN_ROW, &[], Vec::new()));
    // (coder id, nested, encoded)
    let cases: [(&str, bool, &[u8]); 4] = [
        ("bytes", false, b"no prefix at all"),
        ("does_not_exist", true, &[1, 2, 3, 4]),
        ("custom", true, &[9, 9, 9]),
        ("row", true, &[1, 2, 3]), // Row coder without a schema.
    ];
    for (coder_id, nested, encoded) in cases {
        let mut cursor = Cursor::new(encoded);
        skip_coder_value(&mut cursor, coder_id, &coders, nested).expect(coder_id);
        assert_eq!(cursor.position() as usize, encoded.len(), "{coder_id}");
    }
}

#[test]
fn kv_skips_the_key_nested_and_the_value_in_context() {
    let mut coders = base_coders();
    coders.insert(
        "kv".to_string(),
        coder(URN_KV, &["string", "varint"], Vec::new()),
    );

    let mut body = length_prefixed(b"key");
    body.push(0x07);
    assert_skips_exactly(&body, "kv", &coders, true);
}

#[test]
fn a_kv_without_components_skips_nothing() {
    // A malformed KV coder must not consume bytes it cannot account for.
    let mut coders = base_coders();
    coders.insert("kv_bad".to_string(), coder(URN_KV, &[], Vec::new()));

    assert_skips_exactly(&[], "kv_bad", &coders, true);
}

#[test]
fn nullable_reads_only_the_tag_when_null() {
    let mut coders = base_coders();
    coders.insert(
        "maybe".to_string(),
        coder(URN_NULLABLE, &["varint"], Vec::new()),
    );

    // Tag 0 indicates an absent value.
    assert_skips_exactly(&[0x00], "maybe", &coders, true);
    // A non-zero tag precedes the inner value.
    assert_skips_exactly(&[0x01, 0x2A], "maybe", &coders, true);
    // Without a component coder, the skip consumes only the tag.
    coders.insert("bare".to_string(), coder(URN_NULLABLE, &[], Vec::new()));
    assert_skips_exactly(&[0x01], "bare", &coders, true);
}

#[test]
fn iterable_skips_every_element() {
    let mut coders = base_coders();
    coders.insert(
        "iter".to_string(),
        coder(URN_ITERABLE, &["varint"], Vec::new()),
    );

    let mut body = 3i32.to_be_bytes().to_vec();
    body.extend_from_slice(&[0x01, 0x02, 0x03]);
    assert_skips_exactly(&body, "iter", &coders, true);

    // An empty iterable contains only the count.
    assert_skips_exactly(&0i32.to_be_bytes(), "iter", &coders, true);
}

#[test]
fn windowed_value_skips_timestamp_windows_pane_and_value() {
    let mut coders = base_coders();
    coders.insert(
        "wv".to_string(),
        coder(URN_WINDOWED_VALUE, &["varint", "iwindow"], Vec::new()),
    );

    let mut body = vec![0u8; 8]; // timestamp
    body.extend_from_slice(&2i32.to_be_bytes()); // two windows
    body.extend_from_slice(&[0u8; 8]); // window 1 end
    body.push(0x0A); // window 1 duration
    body.extend_from_slice(&[0u8; 8]); // window 2 end
    body.push(0x0B); // window 2 duration
    body.push(0x00); // pane info: no indices, no metadata
    body.push(0x2A); // value
    assert_skips_exactly(&body, "wv", &coders, true);
}

#[test]
fn pane_info_length_depends_on_its_leading_byte() {
    // Bits 0x70 select the number of VarInt indices. Bit 0x80 adds length-prefixed metadata.
    // A skip that treats the pane as one fixed byte misaligns all the fields that follow.
    let cases: Vec<Vec<u8>> = vec![
        vec![0x00],                         // no indices
        vec![0x10, 0x05],                   // one index
        vec![0x20, 0x05, 0x06],             // two indices
        vec![0x80, 0x02, 0xAA, 0xBB],       // metadata only
        vec![0x90, 0x05, 0x02, 0xAA, 0xBB], // one index plus metadata
    ];

    for body in cases {
        let mut encoded = body.clone();
        encoded.push(SENTINEL);

        let mut cursor = Cursor::new(encoded.as_slice());
        skip_pane_info(&mut cursor).expect("pane info should skip");

        assert_eq!(
            cursor.position() as usize,
            body.len(),
            "pane byte {:#04x} consumed the wrong number of bytes",
            body[0]
        );
    }

    // Tag 3 is undefined. A guessed length would misalign the rest of the element.
    skip_pane_info(&mut Cursor::new(&[0x30, 0x05, 0x06][..]))
        .expect_err("an undefined pane encoding must be rejected");
}

#[test]
fn timer_skips_the_payload_only_when_it_is_not_a_clear() {
    let mut coders = base_coders();
    coders.insert(
        "timer".to_string(),
        coder(URN_TIMER, &["string", "gwindow"], Vec::new()),
    );

    // A set timer includes fire timestamp, hold timestamp, and pane after the clear flag.
    let mut set = length_prefixed(b"key");
    set.extend_from_slice(&length_prefixed(b"tag"));
    set.extend_from_slice(&1i32.to_be_bytes()); // one global window (zero bytes)
    set.push(0x00); // clear = false
    set.extend_from_slice(&[0u8; 8]); // fire timestamp
    set.extend_from_slice(&[0u8; 8]); // hold timestamp
    set.push(0x0F); // pane
    assert_skips_exactly(&set, "timer", &coders, true);

    // A cleared timer ends immediately after the clear flag.
    let mut cleared = length_prefixed(b"key");
    cleared.extend_from_slice(&length_prefixed(b"tag"));
    cleared.extend_from_slice(&1i32.to_be_bytes());
    cleared.push(0x01); // clear = true
    assert_skips_exactly(&cleared, "timer", &coders, true);
}

/// Returns a two-field schema for Row tests.
fn row_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("name", FieldType::string()),
    ]))
}

/// Encodes a sample row with a schema from [`row_schema`].
fn encoded_row(schema: &Arc<Schema>) -> Vec<u8> {
    let row = Row::new(
        Arc::clone(schema),
        vec![
            Some(FieldValue::Int64(42)),
            Some(FieldValue::String("beam".to_string())),
        ],
    )
    .expect("row must match its schema");

    let mut buf = Vec::new();
    RowCoder::encode_row(&row, &mut buf).expect("row must encode");
    buf
}

#[test]
fn row_skips_exactly_one_encoded_row() {
    let schema = row_schema();
    let mut coders = base_coders();
    coders.insert(
        "row".to_string(),
        coder(URN_ROW, &[], schema.to_proto_bytes()),
    );

    assert_skips_exactly(&encoded_row(&schema), "row", &coders, true);
}

#[test]
fn a_row_coder_with_a_corrupt_schema_is_an_error() {
    let mut coders = base_coders();
    coders.insert(
        "row".to_string(),
        coder(URN_ROW, &[], vec![0xFF, 0xFF, 0xFF, 0xFF]),
    );

    let encoded = vec![0u8; 4];
    let mut cursor = Cursor::new(encoded.as_slice());
    let err = skip_coder_value(&mut cursor, "row", &coders, true)
        .expect_err("an undecodable schema must not be skipped silently");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn extract_kv_key_bytes_returns_the_encoded_key() {
    let mut coders = base_coders();
    coders.insert(
        "kv".to_string(),
        coder(URN_KV, &["string", "varint"], Vec::new()),
    );

    let key = length_prefixed(b"user_1");
    let mut element = key.clone();
    element.push(0x63); // Value byte that must not be included.

    assert_eq!(
        extract_kv_key_bytes(&element, "kv", &coders),
        Some(key.clone())
    );

    // A WindowedValue wrapper resolves to the inner KV coder.
    coders.insert(
        "wv".to_string(),
        coder(URN_WINDOWED_VALUE, &["kv", "gwindow"], Vec::new()),
    );
    assert_eq!(extract_kv_key_bytes(&element, "wv", &coders), Some(key));
}

#[test]
fn extract_kv_key_bytes_returns_none_when_there_is_no_key() {
    let mut coders = base_coders();
    coders.insert(
        "kv".to_string(),
        coder(URN_KV, &["string", "varint"], Vec::new()),
    );
    coders.insert(
        "wv_novalue".to_string(),
        coder(URN_WINDOWED_VALUE, &["varint", "gwindow"], Vec::new()),
    );
    coders.insert("kv_bare".to_string(), coder(URN_KV, &[], Vec::new()));

    // Empty element.
    assert_eq!(extract_kv_key_bytes(&[], "kv", &coders), None);
    // Unregistered coder ID.
    assert_eq!(extract_kv_key_bytes(&[0x01], "missing", &coders), None);
    // Non-KV coder.
    assert_eq!(extract_kv_key_bytes(&[0x01], "varint", &coders), None);
    // WindowedValue without KV element coder.
    assert_eq!(extract_kv_key_bytes(&[0x01], "wv_novalue", &coders), None);
    // KV without component coders.
    assert_eq!(extract_kv_key_bytes(&[0x01], "kv_bare", &coders), None);
}

#[test]
fn extract_row_schema_returns_none_for_non_row_coders() {
    let mut coders = base_coders();
    coders.insert("row".to_string(), coder(URN_ROW, &[], Vec::new()));
    coders.insert(
        "lp_empty".to_string(),
        coder(URN_LENGTH_PREFIX, &[], Vec::new()),
    );

    // Unregistered coder ID.
    assert!(extract_row_schema("missing", &coders).is_none());
    // Primitive coder.
    assert!(extract_row_schema("varint", &coders).is_none());
    // Row coder without schema payload.
    assert!(extract_row_schema("row", &coders).is_none());
    // Wrapper coder without components.
    assert!(extract_row_schema("lp_empty", &coders).is_none());
}

#[test]
fn extract_row_schema_terminates_on_a_cyclic_coder_graph() {
    // A malicious or defective runner can send coders that refer to each other. The
    // traversal must return `None` and not loop forever.
    let mut coders = base_coders();
    coders.insert(
        "a".to_string(),
        coder(URN_LENGTH_PREFIX, &["b"], Vec::new()),
    );
    coders.insert("b".to_string(), coder(URN_NULLABLE, &["a"], Vec::new()));

    assert!(extract_row_schema("a", &coders).is_none());
}

// ---------------------------------------------------------------------------
// Hostile coder graphs and window counts
//
// The coder table comes from the runner. A cyclic graph must fail the element, not overflow
// the stack and abort. A negative window count must fail, not leave the cursor misplaced.
// ---------------------------------------------------------------------------

fn assert_invalid_data(err: &std::io::Error, message: &str) {
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
    assert_eq!(err.to_string(), message);
}

#[test]
fn a_nullable_cycle_is_rejected() {
    // Each present tag re-enters the other nullable; enough 0x01 tags for any depth.
    let mut coders = base_coders();
    coders.insert("a".to_string(), coder(URN_NULLABLE, &["b"], Vec::new()));
    coders.insert("b".to_string(), coder(URN_NULLABLE, &["a"], Vec::new()));

    let data = [0x01u8; 256];
    let err = skip_coder_value(&mut Cursor::new(&data[..]), "a", &coders, true)
        .expect_err("a nullable cycle has no finite encoding");
    assert_invalid_data(&err, "coder 'b' nests deeper than 64 levels");
}

#[test]
fn an_iterable_of_itself_is_rejected() {
    let mut coders = base_coders();
    coders.insert("it".to_string(), coder(URN_ITERABLE, &["it"], Vec::new()));

    // Each element is another nested iterable of length 1.
    let data: Vec<u8> = std::iter::repeat_n(1i32.to_be_bytes(), 100)
        .flatten()
        .collect();
    let err = skip_coder_value(&mut Cursor::new(data.as_slice()), "it", &coders, true)
        .expect_err("a self-containing iterable has no finite encoding");
    assert_invalid_data(&err, "coder 'it' nests deeper than 64 levels");
}

#[test]
fn a_negative_window_count_in_a_windowed_value_is_rejected() {
    let mut coders = base_coders();
    coders.insert(
        "wv".to_string(),
        coder(URN_WINDOWED_VALUE, &["varint", "gwindow"], Vec::new()),
    );

    let mut data = vec![0x80, 0, 0, 0, 0, 0, 0, 0]; // timestamp
    data.extend((-1i32).to_be_bytes());
    data.extend([0x0F, 0x05, SENTINEL]); // pane, element 5
    let err = skip_coder_value(&mut Cursor::new(data.as_slice()), "wv", &coders, true)
        .expect_err("a negative window count is malformed");
    assert_invalid_data(&err, "Negative window count: -1");
}

#[test]
fn a_negative_window_count_in_a_timer_is_rejected() {
    let mut coders = base_coders();
    coders.insert(
        "timer".to_string(),
        coder(URN_TIMER, &["varint", "gwindow"], Vec::new()),
    );

    let mut data = vec![0x07]; // key
    data.extend(length_prefixed(b"tag"));
    data.extend(i32::MIN.to_be_bytes());
    data.extend([0x01, SENTINEL]); // clear bit set: no timestamps follow
    let err = skip_coder_value(&mut Cursor::new(data.as_slice()), "timer", &coders, true)
        .expect_err("a negative window count is malformed");
    assert_invalid_data(&err, "Negative window count: -2147483648");
}

/// Builds coders `{name}0 .. {name}{levels-1}`, each wrapping the next with `wrap`; the
/// last wraps `varint`.
fn nested_chain(
    coders: &mut HashMap<String, ProtoCoder>,
    name: &str,
    levels: usize,
    wrap: impl Fn(&str) -> ProtoCoder,
) {
    for level in 0..levels {
        let inner = if level + 1 == levels {
            "varint".to_string()
        } else {
            format!("{name}{}", level + 1)
        };
        coders.insert(format!("{name}{level}"), wrap(&inner));
    }
}

#[test]
fn every_recursive_path_is_bounded_by_the_depth_guard() {
    // Each recursion site has one self-referential coder. The depth guard trips on the
    // component of the deepest level that the traversal visits first.
    let repeat = |pattern: &[u8]| -> Vec<u8> { pattern.repeat(100) };
    let timestamp_and_one_window = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    // (recursion site, self-referential coder id, its coder, data, deepest culprit)
    let cases = [
        (
            "chunked iterable element",
            "it",
            coder(URN_ITERABLE, &["it"], Vec::new()),
            repeat(&[0xFF, 0xFF, 0xFF, 0xFF, 0x01]),
            "it",
        ),
        (
            "kv key",
            "kv",
            coder(URN_KV, &["kv", "varint"], Vec::new()),
            repeat(&[0x01]),
            "kv",
        ),
        (
            "kv value",
            "kv",
            coder(URN_KV, &["varint", "kv"], Vec::new()),
            repeat(&[0x00]),
            "varint",
        ),
        (
            "windowed value window",
            "wv",
            coder(URN_WINDOWED_VALUE, &["varint", "wv"], Vec::new()),
            repeat(&timestamp_and_one_window),
            "wv",
        ),
        (
            "windowed value element",
            "wv",
            coder(URN_WINDOWED_VALUE, &["wv", "gwindow"], Vec::new()),
            repeat(&[timestamp_and_one_window.as_slice(), &[0x0F]].concat()),
            "gwindow",
        ),
        (
            "timer window",
            "t",
            coder(URN_TIMER, &["varint", "t"], Vec::new()),
            repeat(&[0x00, 0x00, 0, 0, 0, 1]),
            "varint",
        ),
    ];
    for (path, root, cyclic, data, culprit) in cases {
        let mut coders = base_coders();
        coders.insert(root.to_string(), cyclic);
        let err = skip_coder_value(&mut Cursor::new(data.as_slice()), root, &coders, true)
            .expect_err(path);
        assert_invalid_data(
            &err,
            &format!("coder '{culprit}' nests deeper than 64 levels"),
        );
    }

    // These coders recurse before they read a byte, so use finite chains, not cycles.
    let mut coders = base_coders();
    nested_chain(&mut coders, "p", 70, |inner| {
        coder(URN_PARAM_WINDOWED_VALUE, &[inner], Vec::new())
    });
    nested_chain(&mut coders, "tk", 70, |inner| {
        coder(URN_TIMER, &[inner, "gwindow"], Vec::new())
    });
    let mut timer_data = vec![0x07]; // innermost key
    timer_data.extend([0x00, 0, 0, 0, 0, 0x01].repeat(70)); // tag, no windows, clear
    for (root, data, culprit) in [("p0", vec![0x07], "p65"), ("tk0", timer_data, "tk65")] {
        let err = skip_coder_value(&mut Cursor::new(data.as_slice()), root, &coders, true)
            .expect_err(root);
        assert_invalid_data(
            &err,
            &format!("coder '{culprit}' nests deeper than 64 levels"),
        );
    }
}

#[test]
fn a_windowed_value_may_carry_no_windows() {
    let mut coders = base_coders();
    coders.insert(
        "wv".to_string(),
        coder(URN_WINDOWED_VALUE, &["varint", "gwindow"], Vec::new()),
    );

    let mut body = vec![0x80, 0, 0, 0, 0, 0, 0, 0]; // timestamp
    body.extend(0i32.to_be_bytes());
    body.extend([0x0F, 0x05]); // pane, element 5
    assert_skips_exactly(&body, "wv", &coders, true);
}

#[test]
fn a_continuation_token_may_be_empty_but_not_negative() {
    let mut coders = base_coders();
    coders.insert(
        "iter".to_string(),
        coder(URN_ITERABLE, &["varint"], Vec::new()),
    );
    let mut empty_token = (-1i32).to_be_bytes().to_vec();
    empty_token.extend([0x01, 0x2A]); // a chunk of one inline element
    VarIntCoder::encode_varint(-1, &mut empty_token).expect("vec writes are infallible");
    empty_token.push(0x00); // token length
    assert_skips_exactly(&empty_token, "iter", &coders, true);

    let mut negative = (-1i32).to_be_bytes().to_vec();
    VarIntCoder::encode_varint(-1, &mut negative).expect("vec writes are infallible");
    VarIntCoder::encode_varint(-1, &mut negative).expect("vec writes are infallible");
    let err = skip_coder_value(&mut Cursor::new(negative.as_slice()), "iter", &coders, true)
        .expect_err("a negative token length is malformed");
    assert_invalid_data(&err, "Invalid continuation token length: -1");
}
