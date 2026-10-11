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

use beam::coders::{Coder, CoderError, Context, RowCoder};
use beam::schema::{FieldType, FieldValue, Row, Schema};

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

    let row = Row::builder(schema.clone())
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

    let row = Row::builder(schema.clone())
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

    let person_row = Row::builder(person_schema.clone())
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
    use beam::coders::DefaultCoder;

    let schema = Arc::new(
        Schema::builder()
            .field("title", FieldType::string())
            .field("views", FieldType::int64())
            .build(),
    );

    let row = Row::builder(schema.clone())
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
    assert_eq!(RowCoder::new(schema.clone()).schema(), Some(&schema));
    assert_eq!(RowCoder::default().schema(), None);

    // A list of rows hands the schema down to every element.
    let rows = vec![row.clone(), row];
    let list_bytes = rows.encode().unwrap();
    assert_eq!(
        Vec::<Row>::decode_with_schema(&list_bytes, Some(&schema)).unwrap(),
        rows
    );
}

#[derive(Debug, PartialEq, beam::schema::BeamRow)]
#[beam(crate = "::beam")]
struct Article {
    title: String,
    views: i64,
}

#[test]
fn a_struct_coder_writes_the_row_encoding_of_the_struct() {
    use beam::coders::RowStructCoder;
    use beam::schema::BeamRow;

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
            .map(|i| beam::schema::Field::nullable(format!("f{i}"), FieldType::int32()))
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
