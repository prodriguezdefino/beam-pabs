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

use std::sync::Arc;

use beam_core::coders::{
    BeamIterable, Coder, CoderError, Context, DefaultCoder, FieldMapCache, LengthPrefixed,
    RowCoder, VarIntCoder, extract_row_schema,
};
use beam_core::pipeline::Pipeline;
use beam_core::schema::{FieldType, FieldValue, Row, Schema, TypeInfo};

#[test]
fn test_row_coder_all_primitive_types_roundtrip() {
    let schema = Arc::new(
        Schema::builder()
            .field("b", FieldType::byte())
            .field("i16", FieldType::int16())
            .field("i32", FieldType::int32())
            .field("i64", FieldType::int64())
            .field("f", FieldType::float())
            .field("d", FieldType::double())
            .field("s", FieldType::string())
            .field("bool", FieldType::boolean())
            .field("bytes", FieldType::bytes())
            .build(),
    );

    let row = Row::builder(Arc::clone(&schema))
        .with_value(12i8)
        .with_value(1000i16)
        .with_value(100_000i32)
        .with_value(10_000_000_000i64)
        .with_value(1.25f32)
        .with_value(42.5f64)
        .with_value("Apache Beam Rust")
        .with_value(true)
        .with_value(vec![0xDE, 0xAD, 0xBE, 0xEF])
        .build()
        .unwrap();

    // Built by hand from the row coder spec, not from the encoder under test.
    let mut golden = vec![
        0x09, // Field count.
        0x00, // Empty null bitmask.
        0x0C, // BYTE 12.
        0x03, 0xE8, // INT16 1000: fixed-width big-endian, not VarInt.
        0xA0, 0x8D, 0x06, // INT32 100_000 as VarInt.
        0x80, 0xC8, 0xAF, 0xA0, 0x25, // INT64 10_000_000_000 as VarInt.
        0x3F, 0xA0, 0x00, 0x00, // FLOAT 1.25 big-endian.
        0x40, 0x45, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, // DOUBLE 42.5 big-endian.
        0x10, // STRING length 16.
    ];
    golden.extend_from_slice(b"Apache Beam Rust");
    golden.extend_from_slice(&[
        0x01, // BOOLEAN true.
        0x04, 0xDE, 0xAD, 0xBE, 0xEF, // BYTES.
    ]);

    let mut buf = Vec::new();
    RowCoder::encode_row(&row, &mut buf).unwrap();
    assert_eq!(buf, golden);

    let decoded = RowCoder::decode_row(&schema, &mut golden.as_slice()).unwrap();
    assert_eq!(row, decoded);

    let coder = RowCoder::new(schema);
    let mut trait_buf = Vec::new();
    coder
        .encode(&row, &mut trait_buf, Context::WholeStream)
        .unwrap();
    let trait_decoded = coder
        .decode(&mut trait_buf.as_slice(), Context::WholeStream)
        .unwrap();
    assert_eq!(row, trait_decoded);
}

#[test]
fn test_row_coder_with_nulls() {
    let schema = Arc::new(
        Schema::builder()
            .field("id", FieldType::int64())
            .nullable_field("opt_str", FieldType::string())
            .nullable_field("opt_num", FieldType::int32())
            .field("flag", FieldType::boolean())
            .build(),
    );

    let row = Row::builder(Arc::clone(&schema))
        .with_value(101i64)
        .with_null()
        .with_value(999i32)
        .with_value(false)
        .build()
        .unwrap();

    // Bit 1 of a one-byte bitmask marks opt_str null; its value is then omitted.
    let golden = [0x04, 0x01, 0x02, 0x65, 0xE7, 0x07, 0x00];

    let mut buf = Vec::new();
    RowCoder::encode_row(&row, &mut buf).unwrap();
    assert_eq!(buf, golden);

    let decoded = RowCoder::decode_row(&schema, &mut golden.as_slice()).unwrap();
    assert_eq!(decoded, row);
    assert_eq!(decoded.get_i64("id").unwrap(), Some(101));
    assert_eq!(decoded.get_string("opt_str").unwrap(), None);
    assert_eq!(decoded.get_i32("opt_num").unwrap(), Some(999));
    assert_eq!(decoded.get_bool("flag").unwrap(), Some(false));
}

#[test]
fn test_row_coder_nested_row_and_collections() {
    let address_schema = Schema::builder()
        .field("city", FieldType::string())
        .field("zip", FieldType::int32())
        .build();

    let person_schema = Arc::new(
        Schema::builder()
            .field("name", FieldType::string())
            .field("address", FieldType::row(address_schema.clone()))
            .field("tags", FieldType::array(FieldType::string()))
            .build(),
    );

    let address_row = Row::builder(Arc::new(address_schema))
        .with_value("Seattle")
        .with_value(98101i32)
        .build()
        .unwrap();

    let person_row = Row::builder(Arc::clone(&person_schema))
        .with_value("Alice")
        .with_value(address_row)
        .with_value(FieldValue::Array(vec![
            Some(FieldValue::String("developer".into())),
            Some(FieldValue::String("beam".into())),
        ]))
        .build()
        .unwrap();

    let mut buf = Vec::new();
    RowCoder::encode_row(&person_row, &mut buf).unwrap();

    let decoded = RowCoder::decode_row(&person_schema, &mut buf.as_slice()).unwrap();
    assert_eq!(person_row, decoded);

    let addr = decoded.get_row("address").unwrap().unwrap();
    assert_eq!(addr.get_string("city").unwrap(), Some("Seattle"));
    assert_eq!(addr.get_i32("zip").unwrap(), Some(98101));
}

#[test]
fn test_row_decode_with_schema_and_without() {
    use beam_core::coders::DefaultCoder;

    let schema = Arc::new(
        Schema::builder()
            .field("title", FieldType::string())
            .field("views", FieldType::int64())
            .build(),
    );

    let row = Row::builder(Arc::clone(&schema))
        .with_value("Rust")
        .with_value(42i64)
        .build()
        .unwrap();

    let bytes = row.encode().unwrap();

    // Without active schema, decoding Row directly fails with clear error
    let no_schema = |r: Result<Row, CoderError>| {
        matches!(r, Err(CoderError::Format(msg)) if msg == "Row cannot be decoded without a Schema; \
                  use decode_with_schema or RowCoder::new(schema)")
    };
    assert!(no_schema(Row::decode(&bytes)));
    assert!(no_schema(Row::decode_with_schema(&bytes, None)));
    assert!(matches!(
        RowCoder::default().decode(&mut bytes.as_slice(), Context::Nested),
        Err(CoderError::Format(msg)) if msg == "Cannot decode Row without an associated Schema in RowCoder"
    ));

    // With explicit schema, decoding succeeds
    let decoded = Row::decode_with_schema(&bytes, Some(&schema)).unwrap();
    assert_eq!(row, decoded);
    assert_eq!(decoded.get_string("title").unwrap(), Some("Rust"));
    assert_eq!(decoded.get_i64("views").unwrap(), Some(42));

    // The coder reports the schema it was built with, and none by default.
    assert_eq!(RowCoder::new(Arc::clone(&schema)).schema(), Some(&schema));
    assert_eq!(RowCoder::default().schema(), None);

    // A list of rows hands the schema down to every element.
    let rows = vec![row.clone(), row];
    let list_bytes = rows.encode().unwrap();
    assert_eq!(
        Vec::<Row>::decode_with_schema(&list_bytes, Some(&schema)).unwrap(),
        rows
    );
}

#[derive(Debug, PartialEq, beam_core::schema::BeamRow)]
#[beam(crate = "beam_core")]
struct Article {
    title: String,
    views: i64,
}

#[test]
fn a_struct_coder_writes_the_row_encoding_of_the_struct() {
    use beam_core::coders::RowStructCoder;
    use beam_core::schema::BeamRow;

    let article = Article {
        title: "Rust".to_string(),
        views: 42,
    };
    let mut expected = Vec::new();
    RowCoder::encode_row(&article.to_row().unwrap(), &mut expected).unwrap();

    let coder = RowStructCoder::<Article>::new();
    let mut buf = Vec::new();
    coder.encode(&article, &mut buf, Context::Nested).unwrap();
    assert_eq!(buf, expected);
    assert_eq!(
        coder.decode(&mut buf.as_slice(), Context::Nested).unwrap(),
        article
    );
}

fn two_field_schema() -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .field("id", FieldType::int64())
            .nullable_field("name", FieldType::string())
            .build(),
    )
}

#[test]
fn a_field_count_that_disagrees_with_the_schema_is_rejected() {
    // Three fields on the wire for a two-field schema.
    let err = RowCoder::decode_row(
        &two_field_schema(),
        &mut [0x03, 0x00, 0x01, 0x00, 0x00].as_slice(),
    )
    .expect_err("count mismatch");
    assert!(
        matches!(&err, CoderError::Format(msg) if msg == "Row field count mismatch: expected 2, got 3"),
        "{err:?}"
    );
}

#[test]
fn a_truncated_row_is_an_eof_error() {
    let schema = two_field_schema();
    // Every strict prefix of a valid encoding must fail, never decode a partial row.
    let full = [0x02, 0x00, 0x2A, 0x02, b'h', b'i'];
    assert!(RowCoder::decode_row(&schema, &mut full.as_slice()).is_ok());
    for cut in 0..full.len() {
        let err = RowCoder::decode_row(&schema, &mut &full[..cut])
            .expect_err("a truncated row must not decode");
        assert!(
            matches!(&err, CoderError::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof),
            "cut at {cut}: {err:?}"
        );
    }
}

#[test]
fn a_bitmask_shorter_than_the_field_count_leaves_the_rest_non_null() {
    // Nine fields need two bitmask bytes, but a writer can omit trailing zero bytes.
    // Absent bits are treated as 0 (non-null).
    let schema = Arc::new(Schema::new(
        (0..9)
            .map(|i| beam_core::schema::Field::nullable(format!("f{i}"), FieldType::int32()))
            .collect(),
    ));
    let mut wire = vec![0x09, 0x01, 0x01]; // one bitmask byte: field 0 is null
    wire.extend(1..=8u8); // fields 1..=8, including field 8 beyond the bitmask
    let row = RowCoder::decode_row(&schema, &mut wire.as_slice()).expect("short bitmask");

    let mut expected = vec![None];
    expected.extend((1..=8).map(|v| Some(FieldValue::Int32(v))));
    assert_eq!(row.values(), expected.as_slice());
}

#[test]
fn a_value_of_the_wrong_type_is_rejected_on_encode() {
    // `Row::new` checks only the value count, so the encoder is the last check of the types.
    let row = Row::new(
        two_field_schema(),
        vec![Some(FieldValue::String("x".into())), None],
    )
    .expect("count matches");
    let err = RowCoder::encode_row(&row, &mut Vec::new()).expect_err("type mismatch");
    assert!(
        matches!(&err, CoderError::Format(msg) if msg.starts_with("Atomic type mismatch: expected Int64")),
        "{err:?}"
    );
}

/// `BeamIterable` and `LengthPrefixed` pass the schema to the rows they hold, and register
/// the coder of the row value with its schema.
#[test]
fn row_wrappers_decode_and_register_with_the_row_schema() {
    let schema = Arc::new(Schema::builder().field("n", FieldType::int64()).build());
    let row = Row::new(Arc::clone(&schema), vec![Some(FieldValue::Int64(7))]).unwrap();

    let iterable = BeamIterable::from_vec(vec![row.clone()]);
    let bytes = iterable.encode().unwrap();
    assert!(BeamIterable::<Row>::decode(&bytes).is_err());
    let decoded = BeamIterable::<Row>::decode_with_schema(&bytes, Some(&schema)).unwrap();
    assert_eq!(decoded.into_vec().unwrap(), vec![row.clone()]);

    let prefixed = LengthPrefixed(row.clone());
    let bytes = prefixed.encode().unwrap();
    let decoded = LengthPrefixed::<Row>::decode_with_schema(&bytes, Some(&schema)).unwrap();
    assert_eq!(decoded.0, row);

    let p = Pipeline::new();
    let coders = |id: &str| {
        let proto = p.to_proto();
        extract_row_schema(id, &proto.components.unwrap_or_default().coders)
    };
    let iterable_id = BeamIterable::from_vec(vec![row]).register_value_coder(&p);
    assert_eq!(coders(&iterable_id).as_deref(), Some(schema.as_ref()));
    let prefixed_id = prefixed.register_value_coder(&p);
    assert_eq!(coders(&prefixed_id).as_deref(), Some(schema.as_ref()));
}

#[derive(Debug, PartialEq, beam_core::schema::BeamRow)]
#[beam(crate = "beam_core")]
struct ChunkedRow {
    items: Vec<i64>,
    nullable_items: Vec<Option<String>>,
    tail: i64,
}

#[derive(Debug, PartialEq, beam_core::schema::BeamRow)]
#[beam(crate = "beam_core")]
struct TailOnly {
    tail: i64,
}

fn encode_chunked_row(
    item_chunks: &[&[i64]],
    nullable_chunks: &[&[Option<&str>]],
    tail: i64,
) -> Vec<u8> {
    let mut wire = vec![0x03, 0x00]; // 3 fields, empty null bitmask.

    wire.extend_from_slice(&(-1i32).to_be_bytes());
    for chunk in item_chunks {
        VarIntCoder::encode_varint_to_vec(chunk.len() as i64, &mut wire);
        for &value in *chunk {
            VarIntCoder::encode_varint_to_vec(value, &mut wire);
        }
    }
    VarIntCoder::encode_varint_to_vec(0, &mut wire);

    wire.extend_from_slice(&(-1i32).to_be_bytes());
    for chunk in nullable_chunks {
        VarIntCoder::encode_varint_to_vec(chunk.len() as i64, &mut wire);
        for value in *chunk {
            match value {
                Some(text) => {
                    wire.push(1);
                    VarIntCoder::encode_varint_to_vec(text.len() as i64, &mut wire);
                    wire.extend_from_slice(text.as_bytes());
                }
                None => wire.push(0),
            }
        }
    }
    VarIntCoder::encode_varint_to_vec(0, &mut wire);

    VarIntCoder::encode_varint_to_vec(tail, &mut wire);
    wire
}

#[test]
fn chunked_iterables_decode_and_skip_in_rows_and_derived_structs() {
    let nullable_str = FieldType::string().with_nullable(true);
    let array_schema = Arc::new(
        Schema::builder()
            .field("items", FieldType::array(FieldType::int64()))
            .field("nullable_items", FieldType::array(nullable_str.clone()))
            .field("tail", FieldType::int64())
            .build(),
    );
    let iterable_schema = Arc::new(
        Schema::builder()
            .field(
                "items",
                FieldType::new(TypeInfo::Iterable(Box::new(FieldType::int64())), false),
            )
            .field(
                "nullable_items",
                FieldType::new(TypeInfo::Iterable(Box::new(nullable_str)), false),
            )
            .field("tail", FieldType::int64())
            .build(),
    );

    let wire = encode_chunked_row(
        &[&[10, 20], &[30]],
        &[&[Some("a"), None], &[Some("bc")]],
        99,
    );
    let expected_items = FieldValue::Array(vec![
        Some(FieldValue::Int64(10)),
        Some(FieldValue::Int64(20)),
        Some(FieldValue::Int64(30)),
    ]);
    let expected_nullable = FieldValue::Array(vec![
        Some(FieldValue::String("a".to_string())),
        None,
        Some(FieldValue::String("bc".to_string())),
    ]);

    for schema in [&array_schema, &iterable_schema] {
        let mut cursor = wire.as_slice();
        let row = RowCoder::decode_row(schema, &mut cursor).expect("chunked row decodes");
        assert!(cursor.is_empty());
        assert_eq!(
            row.get_value("items").unwrap().as_ref(),
            Some(&expected_items)
        );
        assert_eq!(
            row.get_value("nullable_items").unwrap().as_ref(),
            Some(&expected_nullable)
        );
        assert_eq!(row.get_i64("tail").unwrap(), Some(99));

        let mut with_sentinel = wire.clone();
        with_sentinel.push(0xEE);
        let mut skip_cursor = with_sentinel.as_slice();
        RowCoder::skip_row(schema, &mut skip_cursor).expect("chunked row skips");
        assert_eq!(skip_cursor, &[0xEE]);
    }

    let mut cursor = wire.as_slice();
    let decoded = ChunkedRow::decode_element(&mut cursor).expect("derived struct decodes");
    assert!(cursor.is_empty());
    assert_eq!(
        decoded,
        ChunkedRow {
            items: vec![10, 20, 30],
            nullable_items: vec![Some("a".to_string()), None, Some("bc".to_string())],
            tail: 99,
        }
    );

    let mut cache = FieldMapCache::new();
    let mapped: TailOnly = cache
        .decode(&wire, Some(&array_schema), None)
        .expect("mapped decode skips chunked arrays");
    assert_eq!(mapped, TailOnly { tail: 99 });

    // Empty chunked iterables (-1 followed immediately by 0) decode and skip cleanly.
    let empty_wire = encode_chunked_row(&[], &[], 7);
    let empty_row =
        RowCoder::decode_row(&array_schema, &mut empty_wire.as_slice()).expect("empty chunked row");
    assert_eq!(
        empty_row.get_value("items").unwrap().as_ref(),
        Some(&FieldValue::Array(Vec::new()))
    );
    assert_eq!(
        empty_row.get_value("nullable_items").unwrap().as_ref(),
        Some(&FieldValue::Array(Vec::new()))
    );
    assert_eq!(empty_row.get_i64("tail").unwrap(), Some(7));
    assert_eq!(
        ChunkedRow::decode(&empty_wire).expect("empty chunked struct"),
        ChunkedRow {
            items: Vec::new(),
            nullable_items: Vec::new(),
            tail: 7,
        }
    );
    let mut skip_empty = empty_wire.as_slice();
    RowCoder::skip_row(&array_schema, &mut skip_empty).expect("empty chunked skips");
    assert!(skip_empty.is_empty());
}
