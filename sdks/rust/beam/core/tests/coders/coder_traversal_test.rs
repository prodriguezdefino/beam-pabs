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
use std::io::ErrorKind;
use std::sync::Arc;

use beam_core::coders::{
    RowCoder, SkipPlan, URN_BOOL, URN_BYTES, URN_DOUBLE, URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW,
    URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_PARAM_WINDOWED_VALUE, URN_ROW,
    URN_STRING_UTF8, URN_TIMER, URN_VARINT, URN_WINDOWED_VALUE, VarIntCoder, extract_kv_key_bytes,
    extract_row_schema, skip_pane_info, skips_exactly,
};
use beam_core::schema::{
    Field, FieldType, FieldValue, Row, Schema, TypeInfo, URN_DECIMAL, URN_MILLIS_INSTANT,
};
use beam_model::pipeline::{Coder as ProtoCoder, FunctionSpec};

/// Skips with [`skip_coder_value`] and with a compiled [`SkipPlan`], and checks that both end
/// at the same position with the same result. Returns the [`skip_coder_value`] result.
fn skip_coder_value(
    cursor: &mut &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
    nested: bool,
) -> Result<(), std::io::Error> {
    let mut planned = *cursor;
    let plan_result = SkipPlan::new(coder_id, coders).skip(&mut planned, nested);
    let result = beam_core::coders::skip_coder_value(cursor, coder_id, coders, nested);
    match (&result, &plan_result) {
        (Ok(()), Ok(())) => assert_eq!(cursor.len(), planned.len(), "{coder_id}"),
        (Err(a), Err(b)) => {
            assert_eq!(
                (a.kind(), a.to_string()),
                (b.kind(), b.to_string()),
                "{coder_id}"
            );
        }
        _ => panic!("{coder_id}: skip_coder_value {result:?}, SkipPlan {plan_result:?}"),
    }
    result
}

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

    let mut cursor = &encoded[..];
    skip_coder_value(&mut cursor, coder_id, coders, nested).expect("skip should succeed");

    assert_eq!(
        encoded.len() - cursor.len(),
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
        let mut cursor = encoded;
        skip_coder_value(&mut cursor, coder_id, &coders, nested).expect(coder_id);
        assert_eq!(encoded.len() - cursor.len(), encoded.len(), "{coder_id}");
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
fn a_kv_without_components_is_rejected() {
    // A malformed KV coder cannot say where the value ends, so the skip reports it.
    let mut coders = base_coders();
    coders.insert("kv_bad".to_string(), coder(URN_KV, &["varint"], Vec::new()));

    let err = skip_coder_value(&mut &[0x01][..], "kv_bad", &coders, true)
        .expect_err("a KV coder needs a key and a value component");
    assert_eq!(err.kind(), ErrorKind::InvalidData);
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

        let mut cursor = &encoded[..];
        skip_pane_info(&mut cursor).expect("pane info should skip");

        assert_eq!(
            encoded.len() - cursor.len(),
            body.len(),
            "pane byte {:#04x} consumed the wrong number of bytes",
            body[0]
        );
    }

    // Tag 3 is undefined. A guessed length would misalign the rest of the element.
    skip_pane_info(&mut &[0x30, 0x05, 0x06][..])
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

#[test]
fn timer_skips_a_pane_with_indices_and_metadata() {
    let mut coders = base_coders();
    coders.insert(
        "timer".to_string(),
        coder(URN_TIMER, &["string", "gwindow"], Vec::new()),
    );
    let panes: [&[u8]; 3] = [
        &[0x1A, 0x02],                   // ON_TIME, one index
        &[0x2A, 0x02, 0x01],             // ON_TIME, two indices
        &[0x9A, 0x02, 0x02, 0xAA, 0xBB], // one index plus metadata
    ];
    for pane in panes {
        let mut set = length_prefixed(b"key");
        set.extend_from_slice(&length_prefixed(b"tag"));
        set.extend_from_slice(&1i32.to_be_bytes());
        set.push(0x00); // clear = false
        set.extend_from_slice(&[0u8; 16]); // fire and hold timestamps
        set.extend_from_slice(pane);
        assert_skips_exactly(&set, "timer", &coders, true);
    }
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

    let encoded = [0u8; 4];
    let mut cursor = &encoded[..];
    let err = skip_coder_value(&mut cursor, "row", &coders, true)
        .expect_err("an undecodable schema must not be skipped silently");
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

/// A schema with every kind of field that changes the length of a row.
fn every_kind_schema() -> Arc<Schema> {
    let point = Schema::new(vec![
        Field::new("x", FieldType::int32()),
        Field::nullable("label", FieldType::string()),
    ]);
    Arc::new(Schema::new(vec![
        Field::new("byte", FieldType::byte()),
        Field::new("short", FieldType::int16()),
        Field::new("float", FieldType::float()),
        Field::new("double", FieldType::double()),
        Field::new("flag", FieldType::boolean()),
        Field::new("bytes", FieldType::bytes()),
        Field::nullable("maybe", FieldType::int64()),
        Field::new(
            "tags",
            FieldType::array(FieldType::string().with_nullable(true)),
        ),
        Field::new(
            "counts",
            FieldType::map(FieldType::string(), FieldType::int64().with_nullable(true)),
        ),
        Field::new("point", FieldType::row(point)),
        Field::new(
            "millis",
            FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64()),
        ),
        Field::new(
            "decimal",
            FieldType::logical(URN_DECIMAL, Vec::new(), FieldType::bytes()),
        ),
    ]))
}

fn decimal_wire(scale: i64, unscaled: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    VarIntCoder::encode_varint_to_vec(scale, &mut out);
    VarIntCoder::encode_varint_to_vec(unscaled.len() as i64, &mut out);
    out.extend_from_slice(unscaled);
    out
}

fn every_kind_row(schema: &Arc<Schema>, maybe: Option<i64>) -> Vec<u8> {
    let point = match &schema.fields[9].field_type.type_info {
        TypeInfo::Row(point) => Arc::clone(point),
        _ => unreachable!("field 9 is a row"),
    };
    let values = vec![
        Some(FieldValue::Byte(-3)),
        Some(FieldValue::Int16(1_000)),
        Some(FieldValue::Float(0.5)),
        Some(FieldValue::Double(2.25)),
        Some(FieldValue::Boolean(true)),
        Some(FieldValue::Bytes(vec![0, 1, 2])),
        maybe.map(FieldValue::Int64),
        Some(FieldValue::Array(vec![
            Some(FieldValue::String("a".to_string())),
            None,
        ])),
        Some(FieldValue::Map(vec![
            (
                FieldValue::String("k".to_string()),
                Some(FieldValue::Int64(300)),
            ),
            (FieldValue::String("absent".to_string()), None),
        ])),
        Some(FieldValue::Row(
            Row::new(point, vec![Some(FieldValue::Int32(-7)), None]).expect("point"),
        )),
        Some(FieldValue::Int64(1_700_000_000_000)),
        Some(FieldValue::Bytes(decimal_wire(-2, &[0x04, 0xD2]))),
    ];
    let row = Row::new(Arc::clone(schema), values).expect("the values match the schema");
    let mut buf = Vec::new();
    RowCoder::encode_row(&row, &mut buf).expect("row must encode");
    buf
}

fn assert_row_skip_stops_at_sentinel(
    schema: &Arc<Schema>,
    plan: &SkipPlan,
    body: &[u8],
    case: &str,
) {
    let encoded = [body, &[SENTINEL]].concat();

    let mut row_cursor = &encoded[..];
    RowCoder::skip_row(schema, &mut row_cursor).expect(case);
    assert_eq!(row_cursor, &[SENTINEL], "{case}");

    for nested in [true, false] {
        let mut plan_cursor = &encoded[..];
        plan.skip(&mut plan_cursor, nested).expect(case);
        assert_eq!(plan_cursor, &[SENTINEL], "{case} (nested={nested})");
    }
}

fn assert_row_skip_err(
    schema: &Schema,
    plan: &SkipPlan,
    bytes: &[u8],
    expected: ErrorKind,
    case: &str,
) {
    let row_err = RowCoder::skip_row(schema, &mut &bytes[..]).expect_err(case);
    let plan_err = plan.skip(&mut &bytes[..], true).expect_err(case);
    assert_eq!(row_err.kind(), expected, "{case}: {row_err}");
    assert_eq!(plan_err.kind(), expected, "{case}: {plan_err}");
    assert_eq!(plan_err.to_string(), row_err.to_string(), "{case}");
}

#[test]
fn skip_row_stops_at_the_end_of_every_kind_of_row() {
    let schema = every_kind_schema();
    let positioned = Arc::new(Schema {
        encoding_positions_set: true,
        ..Schema::new(vec![
            Field::new("late", FieldType::string()).with_encoding_position(1),
            Field::new("early", FieldType::int64()).with_encoding_position(0),
        ])
    });
    let positioned_row = Row::new(
        Arc::clone(&positioned),
        vec![
            Some(FieldValue::String("text".to_string())),
            Some(FieldValue::Int64(5)),
        ],
    )
    .expect("positioned row");
    let mut positioned_bytes = Vec::new();
    RowCoder::encode_row(&positioned_row, &mut positioned_bytes).expect("row must encode");

    let cases = [
        ("every kind", &schema, every_kind_row(&schema, Some(9))),
        ("a null field", &schema, every_kind_row(&schema, None)),
        ("encoding positions", &positioned, positioned_bytes),
    ];
    for (case, case_schema, body) in &cases {
        let plan = SkipPlan::for_row(case_schema.as_ref());
        assert_row_skip_stops_at_sentinel(case_schema, &plan, body, case);
    }
}

#[test]
fn skip_plan_for_row_handles_reordered_fields_nulls_and_nested_containers() {
    let inner = Arc::new(Schema::new(vec![
        Field::nullable("note", FieldType::string()).with_encoding_position(10),
        Field::new("code", FieldType::int32()).with_encoding_position(2),
        Field::nullable(
            "amount",
            FieldType::logical(URN_DECIMAL, Vec::new(), FieldType::bytes()),
        )
        .with_encoding_position(5),
    ]));
    let outer = Arc::new(Schema::new(vec![
        Field::nullable("label", FieldType::string()).with_encoding_position(30),
        Field::nullable("nested", FieldType::row(Arc::clone(&inner))).with_encoding_position(10),
        Field::new(
            "items",
            FieldType::array(FieldType::row(Arc::clone(&inner)).with_nullable(true)),
        )
        .with_encoding_position(20),
        Field::new(
            "prices",
            FieldType::map(
                FieldType::string(),
                FieldType::logical(URN_DECIMAL, Vec::new(), FieldType::bytes()).with_nullable(true),
            ),
        )
        .with_encoding_position(0),
    ]));
    let plan = SkipPlan::for_row(Arc::clone(&outer));

    let inner_full = Row::new(
        Arc::clone(&inner),
        vec![
            Some(FieldValue::String("ok".to_string())),
            Some(FieldValue::Int32(-42)),
            Some(FieldValue::Bytes(decimal_wire(4, &[0x01, 0x00]))),
        ],
    )
    .expect("inner full");
    let inner_nulls = Row::new(
        Arc::clone(&inner),
        vec![None, Some(FieldValue::Int32(7)), None],
    )
    .expect("inner nulls");

    let populated = Row::new(
        Arc::clone(&outer),
        vec![
            Some(FieldValue::String("tail".to_string())),
            Some(FieldValue::Row(inner_full.clone())),
            Some(FieldValue::Array(vec![
                Some(FieldValue::Row(inner_nulls)),
                None,
                Some(FieldValue::Row(inner_full)),
            ])),
            Some(FieldValue::Map(vec![
                (
                    FieldValue::String("a".to_string()),
                    Some(FieldValue::Bytes(decimal_wire(2, &[0x2A]))),
                ),
                (FieldValue::String("b".to_string()), None),
            ])),
        ],
    )
    .expect("populated outer");
    let sparse = Row::new(
        Arc::clone(&outer),
        vec![
            None,
            None,
            Some(FieldValue::Array(Vec::new())),
            Some(FieldValue::Map(Vec::new())),
        ],
    )
    .expect("sparse outer");

    for (case, row) in [("populated", populated), ("sparse", sparse)] {
        let mut bytes = Vec::new();
        RowCoder::encode_row(&row, &mut bytes).expect("row encodes");
        assert_row_skip_stops_at_sentinel(&outer, &plan, &bytes, case);
    }

    // Short null bitmask on a 9-field schema: missing bitmask bytes mean non-null.
    let nine_fields = Arc::new(Schema::new(
        (0..9)
            .map(|i| Field::nullable(format!("f{i}"), FieldType::int32()))
            .collect(),
    ));
    let nine_plan = SkipPlan::for_row(&*nine_fields);
    let mut short_mask = vec![0x09, 0x01, 0x05]; // fields 0 and 2 are null; 1 and 3..=8 follow
    short_mask.extend(1..=7u8);
    assert_row_skip_stops_at_sentinel(&nine_fields, &nine_plan, &short_mask, "short bitmask");
}

#[test]
fn skip_row_reports_a_cut_off_row_as_unexpected_eof() {
    let schema = every_kind_schema();
    let plan = SkipPlan::for_row(&*schema);
    let body = every_kind_row(&schema, Some(9));
    // Every cut must say "wait for more bytes", or the harness drops a partial element.
    for cut in 0..body.len() {
        assert_row_skip_err(
            &schema,
            &plan,
            &body[..cut],
            ErrorKind::UnexpectedEof,
            "cut-off row",
        );
    }
}

#[test]
fn skip_row_rejects_corrupt_headers_containers_and_lengths() {
    let schema = row_schema();
    let plan = SkipPlan::for_row(&*schema);
    assert_row_skip_err(
        &schema,
        &plan,
        &[0x03, 0x00],
        ErrorKind::InvalidData,
        "wrong field count",
    );

    let mut neg_fields = Vec::new();
    VarIntCoder::encode_varint_to_vec(-1, &mut neg_fields);
    assert_row_skip_err(
        &schema,
        &plan,
        &neg_fields,
        ErrorKind::InvalidData,
        "negative field count",
    );

    let mut neg_mask = vec![0x02];
    VarIntCoder::encode_varint_to_vec(-1, &mut neg_mask);
    assert_row_skip_err(
        &schema,
        &plan,
        &neg_mask,
        ErrorKind::InvalidData,
        "negative bitmask length",
    );

    let mut overlong_hdr = vec![0x80u8; 10];
    overlong_hdr.push(0x01);
    assert_row_skip_err(
        &schema,
        &plan,
        &overlong_hdr,
        ErrorKind::InvalidData,
        "overlong header varint",
    );

    let mut neg_str = vec![0x02, 0x00, 0x01];
    VarIntCoder::encode_varint_to_vec(-2, &mut neg_str);
    assert_row_skip_err(
        &schema,
        &plan,
        &neg_str,
        ErrorKind::InvalidData,
        "negative string length",
    );

    let dec_schema = Schema::new(vec![Field::new(
        "d",
        FieldType::logical(URN_DECIMAL, Vec::new(), FieldType::bytes()),
    )]);
    let dec_plan = SkipPlan::for_row(&dec_schema);
    let mut neg_dec = vec![0x01, 0x00, 0x02];
    VarIntCoder::encode_varint_to_vec(-3, &mut neg_dec);
    assert_row_skip_err(
        &dec_schema,
        &dec_plan,
        &neg_dec,
        ErrorKind::InvalidData,
        "negative decimal length",
    );

    let mut huge_dec = vec![0x01, 0x00, 0x02];
    VarIntCoder::encode_varint_to_vec(1 << 60, &mut huge_dec);
    huge_dec.extend_from_slice(b"abc");
    assert_row_skip_err(
        &dec_schema,
        &dec_plan,
        &huge_dec,
        ErrorKind::UnexpectedEof,
        "huge decimal length",
    );

    let tags = Schema::new(vec![Field::new(
        "tags",
        FieldType::array(FieldType::string().with_nullable(true)),
    )]);
    let tags_plan = SkipPlan::for_row(&tags);
    // `-1` starts a chunked iterable, so `-2` is an invalid negative count while a bare `-1`
    // truncates before the first chunk header.
    let negative = [&[0x01, 0x00][..], &(-2i32).to_be_bytes()].concat();
    assert_row_skip_err(
        &tags,
        &tags_plan,
        &negative,
        ErrorKind::InvalidData,
        "negative iterable count",
    );

    let truncated_chunked = [&[0x01, 0x00][..], &(-1i32).to_be_bytes()].concat();
    assert_row_skip_err(
        &tags,
        &tags_plan,
        &truncated_chunked,
        ErrorKind::UnexpectedEof,
        "truncated chunked iterable",
    );

    let bad_presence = [&[0x01, 0x00][..], &1i32.to_be_bytes(), &[0x02]].concat();
    assert_row_skip_err(
        &tags,
        &tags_plan,
        &bad_presence,
        ErrorKind::InvalidData,
        "bad array presence byte",
    );

    let map_schema = Schema::new(vec![Field::new(
        "m",
        FieldType::map(FieldType::string(), FieldType::int64().with_nullable(true)),
    )]);
    let map_plan = SkipPlan::for_row(&map_schema);
    let neg_map = [&[0x01, 0x00][..], &(-1i32).to_be_bytes()].concat();
    assert_row_skip_err(
        &map_schema,
        &map_plan,
        &neg_map,
        ErrorKind::InvalidData,
        "negative map count",
    );

    let nested_schema = Schema::new(vec![Field::new("inner", FieldType::row(row_schema()))]);
    let nested_plan = SkipPlan::for_row(&nested_schema);
    assert_row_skip_err(
        &nested_schema,
        &nested_plan,
        &[0x01, 0x00, 0x05, 0x00],
        ErrorKind::InvalidData,
        "wrong nested row field count",
    );
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

#[test]
fn extract_row_schema_sees_through_kv_and_iterable() {
    let schema = Schema::builder()
        .field("id", FieldType::int64())
        .nullable_field("name", FieldType::string())
        .build();
    let mut coders = base_coders();
    coders.insert(
        "row".to_string(),
        coder(URN_ROW, &[], schema.to_proto_bytes()),
    );
    coders.insert(
        "lp_row".to_string(),
        coder(URN_LENGTH_PREFIX, &["row"], Vec::new()),
    );
    coders.insert(
        "iter_row".to_string(),
        coder(URN_ITERABLE, &["lp_row"], Vec::new()),
    );
    coders.insert(
        "kv_iter_row".to_string(),
        coder(URN_KV, &["varint", "iter_row"], Vec::new()),
    );

    assert_eq!(
        extract_row_schema("kv_iter_row", &coders).as_deref(),
        Some(&schema)
    );
}

#[test]
fn extract_row_schema_is_none_for_two_different_row_schemas() {
    let key = Schema::builder().field("id", FieldType::int64()).build();
    let value = Schema::builder().field("name", FieldType::string()).build();
    let mut coders = base_coders();
    coders.insert("key".to_string(), coder(URN_ROW, &[], key.to_proto_bytes()));
    coders.insert(
        "value".to_string(),
        coder(URN_ROW, &[], value.to_proto_bytes()),
    );
    coders.insert(
        "kv".to_string(),
        coder(URN_KV, &["key", "value"], Vec::new()),
    );
    coders.insert(
        "same".to_string(),
        coder(URN_KV, &["key", "key"], Vec::new()),
    );

    assert!(extract_row_schema("kv", &coders).is_none());
    assert_eq!(extract_row_schema("same", &coders).as_deref(), Some(&key));
}

// ---------------------------------------------------------------------------
// Hostile coder graphs and window counts
//
// The coder table comes from the runner. A cyclic graph must fail the element, not overflow
// the stack and abort. A negative window count must fail, not leave the cursor misplaced.
// ---------------------------------------------------------------------------

fn assert_invalid_data(err: &std::io::Error, message: &str) {
    assert_eq!(err.kind(), ErrorKind::InvalidData, "{err}");
    assert_eq!(err.to_string(), message);
}

#[test]
fn a_nullable_cycle_is_rejected() {
    // Each present tag re-enters the other nullable; enough 0x01 tags for any depth.
    let mut coders = base_coders();
    coders.insert("a".to_string(), coder(URN_NULLABLE, &["b"], Vec::new()));
    coders.insert("b".to_string(), coder(URN_NULLABLE, &["a"], Vec::new()));

    let data = [0x01u8; 256];
    let err = skip_coder_value(&mut &data[..], "a", &coders, true)
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
    let err = skip_coder_value(&mut &data.as_slice()[..], "it", &coders, true)
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
    let err = skip_coder_value(&mut &data.as_slice()[..], "wv", &coders, true)
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
    let err = skip_coder_value(&mut &data.as_slice()[..], "timer", &coders, true)
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
        let err = skip_coder_value(&mut &data.as_slice()[..], root, &coders, true).expect_err(path);
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
        let err = skip_coder_value(&mut &data.as_slice()[..], root, &coders, true).expect_err(root);
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
    let err = skip_coder_value(&mut &negative.as_slice()[..], "iter", &coders, true)
        .expect_err("a negative token length is malformed");
    assert_invalid_data(&err, "Invalid continuation token length: -1");
}

#[test]
fn skips_exactly_holds_only_for_coders_whose_end_the_skip_finds() {
    let custom = "beam:coder:test:custom:v1";
    let mut coders = base_coders();
    coders.extend([
        ("custom".to_string(), coder(custom, &[], Vec::new())),
        (
            "kv".to_string(),
            coder(URN_KV, &["string", "varint"], Vec::new()),
        ),
        (
            "kv_of_custom".to_string(),
            coder(URN_KV, &["string", "custom"], Vec::new()),
        ),
        (
            "iterable".to_string(),
            coder(URN_ITERABLE, &["kv"], Vec::new()),
        ),
        (
            "nullable_custom".to_string(),
            coder(URN_NULLABLE, &["custom"], Vec::new()),
        ),
        (
            "prefixed_custom".to_string(),
            coder(URN_LENGTH_PREFIX, &["custom"], Vec::new()),
        ),
        (
            "row".to_string(),
            coder(URN_ROW, &[], row_schema().to_proto_bytes()),
        ),
        ("row_no_schema".to_string(), coder(URN_ROW, &[], Vec::new())),
        // Refers to itself, so only the depth limit ends the check.
        (
            "cycle".to_string(),
            coder(URN_KV, &["string", "cycle"], Vec::new()),
        ),
    ]);

    for id in [
        "varint",
        "string",
        "bool",
        "double",
        "gwindow",
        "iwindow",
        "kv",
        "iterable",
        "prefixed_custom",
        "row",
    ] {
        assert!(skips_exactly(id, &coders), "{id}");
    }
    for id in [
        "custom",
        "kv_of_custom",
        "nullable_custom",
        "row_no_schema",
        "cycle",
        "missing",
    ] {
        assert!(!skips_exactly(id, &coders), "{id}");
    }
}
