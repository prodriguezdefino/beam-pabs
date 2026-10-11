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

//! Tests for rewriting runner-inserted length prefixes around nested Row coders.

use std::collections::HashMap;
use std::sync::Arc;

use beam::coders::{
    RowCoder, URN_GLOBAL_WINDOW, URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_ROW,
    URN_STRING_UTF8, URN_VARINT, URN_WINDOWED_VALUE, VarIntCoder, add_nested_row_length_prefixes,
    has_nested_row_length_prefix, strip_nested_row_length_prefixes,
};
use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use model::pipeline::{Coder as ProtoCoder, FunctionSpec};

fn coder(urn: &str, components: &[&str], payload: Vec<u8>) -> ProtoCoder {
    ProtoCoder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload,
        }),
        component_coder_ids: components.iter().map(|s| s.to_string()).collect(),
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::nullable("name", FieldType::string()),
    ]))
}

fn encoded_row(id: i64, name: Option<&str>) -> Vec<u8> {
    let row = Row::new(
        schema(),
        vec![
            Some(FieldValue::Int64(id)),
            name.map(|n| FieldValue::String(n.to_string())),
        ],
    )
    .expect("row must match its schema");
    let mut buf = Vec::new();
    RowCoder::encode_row(&row, &mut buf).expect("row must encode");
    buf
}

fn prefixed(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![u8::try_from(bytes.len()).expect("short test payload")];
    out.extend_from_slice(bytes);
    out
}

/// The coders that Prism sends for a grouped `KV<varint, iterable<row>>` and for the
/// SDK-native `KV<varint, row>`, before and after length-prefixing.
fn coders() -> HashMap<String, ProtoCoder> {
    HashMap::from([
        ("varint".to_string(), coder(URN_VARINT, &[], Vec::new())),
        (
            "string".to_string(),
            coder(URN_STRING_UTF8, &[], Vec::new()),
        ),
        (
            "row".to_string(),
            coder(URN_ROW, &[], schema().to_proto_bytes()),
        ),
        ("row_noschema".to_string(), coder(URN_ROW, &[], Vec::new())),
        (
            "lp_row".to_string(),
            coder(URN_LENGTH_PREFIX, &["row"], Vec::new()),
        ),
        (
            "lp_row_noschema".to_string(),
            coder(URN_LENGTH_PREFIX, &["row_noschema"], Vec::new()),
        ),
        (
            "kv_row".to_string(),
            coder(URN_KV, &["varint", "row"], Vec::new()),
        ),
        (
            "kv_lp_row".to_string(),
            coder(URN_KV, &["varint", "lp_row"], Vec::new()),
        ),
        (
            "kv_lp_row_noschema".to_string(),
            coder(URN_KV, &["varint", "lp_row_noschema"], Vec::new()),
        ),
        (
            "iter_lp_row".to_string(),
            coder(URN_ITERABLE, &["lp_row"], Vec::new()),
        ),
        (
            "kv_iter_lp_row".to_string(),
            coder(URN_KV, &["varint", "iter_lp_row"], Vec::new()),
        ),
        (
            "nullable_lp_row".to_string(),
            coder(URN_NULLABLE, &["lp_row"], Vec::new()),
        ),
        (
            "kv_string_nullable".to_string(),
            coder(URN_KV, &["string", "nullable_lp_row"], Vec::new()),
        ),
        (
            "kv_plain".to_string(),
            coder(URN_KV, &["varint", "string"], Vec::new()),
        ),
        (
            "nullable_string".to_string(),
            coder(URN_NULLABLE, &["string"], Vec::new()),
        ),
        (
            "lp_nullable_string".to_string(),
            coder(URN_LENGTH_PREFIX, &["nullable_string"], Vec::new()),
        ),
        (
            "kv_varint_lp_nullable".to_string(),
            coder(URN_KV, &["varint", "lp_nullable_string"], Vec::new()),
        ),
        (
            "kv_lp_row_key".to_string(),
            coder(URN_KV, &["lp_row", "varint"], Vec::new()),
        ),
        (
            "gwindow".to_string(),
            coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
        (
            "wv_lp_row".to_string(),
            coder(URN_WINDOWED_VALUE, &["lp_row", "gwindow"], Vec::new()),
        ),
    ])
}

#[test]
fn detects_only_nested_row_prefixes() {
    let coders = coders();
    assert!(has_nested_row_length_prefix("kv_lp_row", &coders));
    assert!(has_nested_row_length_prefix("kv_iter_lp_row", &coders));
    assert!(has_nested_row_length_prefix("kv_string_nullable", &coders));
    // Root length prefix is managed by the data channel and not rewritten here.
    assert!(!has_nested_row_length_prefix("lp_row", &coders));
    assert!(!has_nested_row_length_prefix("kv_row", &coders));
    assert!(!has_nested_row_length_prefix("kv_plain", &coders));
    assert!(!has_nested_row_length_prefix("missing", &coders));
}

#[test]
fn grouped_iterable_of_rows_round_trips() {
    let coders = coders();
    let rows = [
        encoded_row(1, None),
        encoded_row(2, Some("b")),
        encoded_row(3, Some("")),
    ];
    let mut native = vec![0x02];
    let mut wire = vec![0x02];
    native.extend_from_slice(&3i32.to_be_bytes());
    wire.extend_from_slice(&3i32.to_be_bytes());
    for row in &rows {
        native.extend_from_slice(row);
        wire.extend_from_slice(&prefixed(row));
    }

    assert_eq!(
        strip_nested_row_length_prefixes(&wire, "kv_iter_lp_row", &coders).expect("strip"),
        native
    );
    assert_eq!(
        add_nested_row_length_prefixes(&native, "kv_iter_lp_row", &coders).expect("add"),
        wire
    );
}

#[test]
fn chunked_iterable_of_rows_is_rewritten_chunk_by_chunk() {
    let coders = coders();
    let (a, b) = (encoded_row(10, Some("x")), encoded_row(11, None));
    let mut wire = vec![0x01];
    wire.extend_from_slice(&(-1i32).to_be_bytes());
    wire.push(0x02); // chunk of two
    wire.extend_from_slice(&prefixed(&a));
    wire.extend_from_slice(&prefixed(&b));
    wire.push(0x00); // terminator

    let mut native = vec![0x01];
    native.extend_from_slice(&(-1i32).to_be_bytes());
    native.push(0x02);
    native.extend_from_slice(&a);
    native.extend_from_slice(&b);
    native.push(0x00);

    assert_eq!(
        strip_nested_row_length_prefixes(&wire, "kv_iter_lp_row", &coders).expect("strip"),
        native
    );
}

#[test]
fn nullable_rows_keep_their_presence_byte() {
    let coders = coders();
    let row = encoded_row(4, Some("four"));
    let key = prefixed(b"k");

    let mut present_native = key.clone();
    present_native.push(0x01);
    present_native.extend_from_slice(&row);
    let mut present_wire = key.clone();
    present_wire.push(0x01);
    present_wire.extend_from_slice(&prefixed(&row));
    assert_eq!(
        add_nested_row_length_prefixes(&present_native, "kv_string_nullable", &coders)
            .expect("add"),
        present_wire
    );

    let mut absent = key;
    absent.push(0x00);
    assert_eq!(
        strip_nested_row_length_prefixes(&absent, "kv_string_nullable", &coders).expect("strip"),
        absent
    );
}

#[test]
fn adding_a_prefix_needs_the_row_schema() {
    let coders = coders();
    let mut native = vec![0x05];
    native.extend_from_slice(&encoded_row(1, None));
    let err = add_nested_row_length_prefixes(&native, "kv_lp_row_noschema", &coders)
        .expect_err("a schemaless row cannot be measured");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn a_truncated_prefix_is_reported() {
    let coders = coders();
    let wire = vec![0x05, 0x10, 0x01];
    let err = strip_nested_row_length_prefixes(&wire, "kv_lp_row", &coders)
        .expect_err("prefix claims more bytes than follow");
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn nested_runner_prefixed_nullable_round_trips() {
    let coders = coders();
    assert!(has_nested_row_length_prefix(
        "kv_varint_lp_nullable",
        &coders
    ));

    // Case 1: Present (Some("hello"))
    // Native: key (varint 0: 0x00) + present tag (0x01) + string ("hello": 0x05, b"hello")
    let mut native_some = vec![0x00, 0x01, 0x05];
    native_some.extend_from_slice(b"hello");
    // Wire: key (0x00) + length prefix (7 bytes: tag 1 + len 1 + "hello" 5) + present tag (0x01) + string
    let mut wire_some = vec![0x00, 0x07, 0x01, 0x05];
    wire_some.extend_from_slice(b"hello");

    assert_eq!(
        add_nested_row_length_prefixes(&native_some, "kv_varint_lp_nullable", &coders)
            .expect("add some"),
        wire_some
    );
    assert_eq!(
        strip_nested_row_length_prefixes(&wire_some, "kv_varint_lp_nullable", &coders)
            .expect("strip some"),
        native_some
    );

    // Case 2: Absent (None)
    // Native: key (0x00) + absent tag (0x00)
    let native_none = vec![0x00, 0x00];
    // Wire: key (0x00) + length prefix (1 byte) + absent tag (0x00)
    let wire_none = vec![0x00, 0x01, 0x00];

    assert_eq!(
        add_nested_row_length_prefixes(&native_none, "kv_varint_lp_nullable", &coders)
            .expect("add none"),
        wire_none
    );
    assert_eq!(
        strip_nested_row_length_prefixes(&wire_none, "kv_varint_lp_nullable", &coders)
            .expect("strip none"),
        native_none
    );
}

#[test]
fn a_prefix_below_a_composite_that_is_not_traversed_is_not_reported() {
    // The rewriter copies windowed values verbatim, so a prefix inside one isn't reported.
    assert!(!has_nested_row_length_prefix("wv_lp_row", &coders()));
}

#[test]
fn a_root_prefix_is_left_to_the_data_channel() {
    let coders = coders();
    let wire = prefixed(&encoded_row(9, Some("nine")));
    assert_eq!(
        strip_nested_row_length_prefixes(&wire, "lp_row", &coders).expect("strip"),
        wire
    );
}

#[test]
fn prefixes_directly_under_the_root_composite_are_stripped() {
    let coders = coders();
    let row = encoded_row(5, Some("five"));
    let one = 1i32.to_be_bytes();
    let chunked = (-1i32).to_be_bytes();
    // (root coder, wire prefix, wire suffix): the native form drops the row's own prefix.
    let cases: [(&str, Vec<u8>, Vec<u8>); 4] = [
        ("kv_lp_row_key", Vec::new(), vec![0x07]),
        ("iter_lp_row", one.to_vec(), Vec::new()),
        ("iter_lp_row", [&chunked[..], &[0x01]].concat(), vec![0x00]),
        ("nullable_lp_row", vec![0x01], Vec::new()),
    ];
    for (root, before, after) in cases {
        let wire = [before.as_slice(), &prefixed(&row), &after].concat();
        let native = [before.as_slice(), &row, &after].concat();
        assert_eq!(
            strip_nested_row_length_prefixes(&wire, root, &coders).expect(root),
            native,
            "{root}"
        );
    }
}

/// Coders `it0 .. it{levels-1}`, each an iterable of the next, the last of `lp_row`, so
/// the prefix sits `levels` levels below the root.
fn iterable_chain(levels: usize) -> HashMap<String, ProtoCoder> {
    let mut coders = coders();
    for level in 0..levels {
        let inner = if level + 1 == levels {
            "lp_row".to_string()
        } else {
            format!("it{}", level + 1)
        };
        coders.insert(
            format!("it{level}"),
            coder(URN_ITERABLE, &[inner.as_str()], Vec::new()),
        );
    }
    coders
}

#[test]
fn prefix_detection_and_rewriting_stop_at_the_depth_limit() {
    // Detection looks through 64 composites below the root and no further.
    assert!(has_nested_row_length_prefix("it0", &iterable_chain(65)));
    assert!(!has_nested_row_length_prefix("it0", &iterable_chain(66)));

    // Rewriting visits values down to depth 64; one level more is refused.
    let row = encoded_row(3, None);
    let element =
        |levels: usize, row: &[u8]| [1i32.to_be_bytes().repeat(levels), row.to_vec()].concat();
    let coders = iterable_chain(64);
    assert_eq!(
        strip_nested_row_length_prefixes(&element(64, &prefixed(&row)), "it0", &coders)
            .expect("64 levels are within the limit"),
        element(64, &row)
    );
    let err =
        strip_nested_row_length_prefixes(&element(65, &prefixed(&row)), "it0", &iterable_chain(65))
            .expect_err("65 levels exceed the limit");
    assert_eq!(
        err.to_string(),
        "coder 'lp_row' nests deeper than 64 levels"
    );
}

#[test]
fn a_prefix_longer_than_one_varint_byte_is_inserted_whole() {
    // A row of 128 bytes or more needs a two-byte VarInt length.
    let coders = coders();
    let row = encoded_row(1, Some(&"x".repeat(200)));
    let native = [&[0x05][..], &row].concat();
    let mut wire = vec![0x05];
    VarIntCoder::encode_varint(row.len() as i64, &mut wire).expect("vec writes are infallible");
    assert_eq!(wire.len(), 3, "the length must take two VarInt bytes");
    wire.extend_from_slice(&row);

    assert_eq!(
        add_nested_row_length_prefixes(&native, "kv_lp_row", &coders).expect("add"),
        wire
    );
    assert_eq!(
        strip_nested_row_length_prefixes(&wire, "kv_lp_row", &coders).expect("strip"),
        native
    );
}

#[test]
fn bytes_after_the_element_are_carried_over() {
    let coders = coders();
    let row = encoded_row(2, Some("two"));
    let trailer = [0xAB, 0xCD];
    let wire = [&[0x05][..], &prefixed(&row), &trailer].concat();
    assert_eq!(
        strip_nested_row_length_prefixes(&wire, "kv_lp_row", &coders).expect("strip"),
        [&[0x05][..], &row, &trailer].concat()
    );
}

#[test]
fn an_invalid_chunk_header_is_rejected() {
    let coders = coders();
    let mut wire = vec![0x01];
    wire.extend_from_slice(&(-1i32).to_be_bytes());
    VarIntCoder::encode_varint(-2, &mut wire).expect("vec writes are infallible");
    let err = strip_nested_row_length_prefixes(&wire, "kv_iter_lp_row", &coders)
        .expect_err("only -1 introduces a continuation token");
    assert_eq!(err.to_string(), "Invalid iterable chunk header: -2");
}
