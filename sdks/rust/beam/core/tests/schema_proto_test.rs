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

//! Golden `Schema` protos: what `register_coder` sends to runners and other SDKs.
//!
//! Round trips through `to_proto_bytes` and `from_proto_bytes` are symmetric, so a wrong
//! mapping (for example `u16` to INT16) would pass them. Each expectation here is a
//! `model::pipeline::Schema` built by hand from the portable spec. The tests compare it
//! structurally and as encoded bytes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use beam::coders::RowCoder;
use beam::schema::{
    BeamEnum, BeamRow, FieldType, FieldValue, Schema, SchemaError, TypeInfo, URN_DATE, URN_DECIMAL,
    URN_MICROS_INSTANT,
};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use model::pipeline as proto;
use model::pipeline::AtomicType as A;
use model::pipeline::field_type::TypeInfo as ProtoTypeInfo;
use prost::Message;
use rust_decimal::Decimal;

// ---------------------------------------------------------------------------
// Hand-built proto fixtures
// ---------------------------------------------------------------------------

fn atomic(t: proto::AtomicType) -> proto::FieldType {
    proto::FieldType {
        nullable: false,
        type_info: Some(ProtoTypeInfo::AtomicType(t as i32)),
    }
}

fn nullable(mut ft: proto::FieldType) -> proto::FieldType {
    ft.nullable = true;
    ft
}

fn array(elem: proto::FieldType) -> proto::FieldType {
    proto::FieldType {
        nullable: false,
        type_info: Some(ProtoTypeInfo::ArrayType(Box::new(proto::ArrayType {
            element_type: Some(Box::new(elem)),
        }))),
    }
}

fn iterable(elem: proto::FieldType) -> proto::FieldType {
    proto::FieldType {
        nullable: false,
        type_info: Some(ProtoTypeInfo::IterableType(Box::new(proto::IterableType {
            element_type: Some(Box::new(elem)),
        }))),
    }
}

fn map(key: proto::FieldType, value: proto::FieldType) -> proto::FieldType {
    proto::FieldType {
        nullable: false,
        type_info: Some(ProtoTypeInfo::MapType(Box::new(proto::MapType {
            key_type: Some(Box::new(key)),
            value_type: Some(Box::new(value)),
        }))),
    }
}

fn row(schema: proto::Schema) -> proto::FieldType {
    proto::FieldType {
        nullable: false,
        type_info: Some(ProtoTypeInfo::RowType(proto::RowType {
            schema: Some(schema),
        })),
    }
}

fn logical(urn: &str, representation: proto::FieldType) -> proto::FieldType {
    proto::FieldType {
        nullable: false,
        type_info: Some(ProtoTypeInfo::LogicalType(Box::new(proto::LogicalType {
            urn: urn.to_string(),
            payload: Vec::new(),
            representation: Some(Box::new(representation)),
            argument_type: None,
            argument: None,
        }))),
    }
}

fn field(name: &str, ft: proto::FieldType) -> proto::Field {
    proto::Field {
        name: name.to_string(),
        r#type: Some(ft),
        ..Default::default()
    }
}

fn schema(id: &str, fields: Vec<proto::Field>) -> proto::Schema {
    proto::Schema {
        fields,
        id: id.to_string(),
        ..Default::default()
    }
}

/// Asserts `actual` serializes to exactly `expected`, reporting the structural diff.
fn assert_proto(actual: &Schema, expected: &proto::Schema) {
    let bytes = actual.to_proto_bytes();
    assert_eq!(
        &proto::Schema::decode(bytes.as_slice()).expect("valid proto"),
        expected
    );
    assert_eq!(bytes, expected.encode_to_vec(), "encoded bytes differ");
}

// ---------------------------------------------------------------------------
// Builder-constructed schemas
// ---------------------------------------------------------------------------

#[test]
fn builder_schema_maps_to_the_portable_proto() {
    let schema_value = Schema::builder()
        .field("flag", FieldType::boolean())
        .nullable_field("count", FieldType::int64())
        .field("tags", FieldType::array(FieldType::string()))
        .field(
            "mapping",
            FieldType::map(FieldType::string(), FieldType::int32().with_nullable(true)),
        )
        .field(
            "stream",
            FieldType::new(TypeInfo::Iterable(Box::new(FieldType::double())), false),
        )
        .id("roundtrip-test")
        .build();

    let expected = schema(
        "roundtrip-test",
        vec![
            field("flag", atomic(A::Boolean)),
            field("count", nullable(atomic(A::Int64))),
            field("tags", array(atomic(A::String))),
            field(
                "mapping",
                map(atomic(A::String), nullable(atomic(A::Int32))),
            ),
            field("stream", iterable(atomic(A::Double))),
        ],
    );
    assert_proto(&schema_value, &expected);

    // And back: a proto another SDK produced reads as the same schema.
    assert_eq!(
        Schema::from_proto_bytes(&expected.encode_to_vec()).expect("valid proto"),
        schema_value
    );
}

#[test]
fn descriptions_and_ids_are_carried() {
    let mut f = beam::schema::Field::new("a", FieldType::int64()).with_description("doc");
    f.id = Some(7);
    let s = Schema::new(vec![f]);

    let expected = proto::Schema {
        fields: vec![proto::Field {
            name: "a".to_string(),
            description: "doc".to_string(),
            r#type: Some(atomic(A::Int64)),
            id: 7,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_proto(&s, &expected);
    assert_eq!(
        Schema::from_proto_bytes(&expected.encode_to_vec()).expect("valid"),
        s
    );
}

// ---------------------------------------------------------------------------
// Derived schemas: every supported Rust type
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, BeamEnum)]
#[beam(crate = "::beam")]
enum Color {
    Red,
}

#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam", id = "test.Inner")]
struct Inner {
    x: i32,
}

#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam", id = "test.EveryType")]
struct EveryType {
    flag: bool,
    tiny: i8,
    small: i16,
    medium: i32,
    large: i64,
    single: f32,
    double: f64,
    unsigned_small: u16,
    unsigned_medium: u32,
    text: String,
    raw: Bytes,
    #[beam(bytes)]
    blob: Vec<u8>,
    // Spelled with a path prefix on purpose: the derive must accept it as Vec<u8>.
    #[beam(bytes)]
    #[expect(unused_qualifications, reason = "exercises the derive's path handling")]
    qualified_blob: std::vec::Vec<u8>,
    maybe: Option<i64>,
    tiny_array: Vec<i8>,
    sparse: Vec<Option<String>>,
    hashed: HashMap<String, i64>,
    ordered: BTreeMap<i32, Option<f64>>,
    inner: Inner,
    color: Color,
    maybe_color: Option<Color>,
    at: DateTime<Utc>,
    day: NaiveDate,
    amount: Decimal,
}

fn micros_instant_representation() -> proto::FieldType {
    row(schema(
        "",
        vec![
            field("seconds", atomic(A::Int64)),
            field("micros", atomic(A::Int64)),
        ],
    ))
}

#[test]
fn every_supported_rust_type_maps_to_its_portable_field_type() {
    let expected = schema(
        "test.EveryType",
        vec![
            field("flag", atomic(A::Boolean)),
            field("tiny", atomic(A::Byte)),
            field("small", atomic(A::Int16)),
            field("medium", atomic(A::Int32)),
            field("large", atomic(A::Int64)),
            field("single", atomic(A::Float)),
            field("double", atomic(A::Double)),
            // Unsigned types widen to the next signed type rather than wrapping.
            field("unsigned_small", atomic(A::Int32)),
            field("unsigned_medium", atomic(A::Int64)),
            field("text", atomic(A::String)),
            field("raw", atomic(A::Bytes)),
            field("blob", atomic(A::Bytes)),
            field("qualified_blob", atomic(A::Bytes)),
            field("maybe", nullable(atomic(A::Int64))),
            // `Vec<i8>` is the explicit spelling of ARRAY<BYTE>.
            field("tiny_array", array(atomic(A::Byte))),
            field("sparse", array(nullable(atomic(A::String)))),
            field("hashed", map(atomic(A::String), atomic(A::Int64))),
            field(
                "ordered",
                map(atomic(A::Int32), nullable(atomic(A::Double))),
            ),
            field(
                "inner",
                row(schema("test.Inner", vec![field("x", atomic(A::Int32))])),
            ),
            field("color", atomic(A::String)),
            field("maybe_color", nullable(atomic(A::String))),
            field(
                "at",
                logical(URN_MICROS_INSTANT, micros_instant_representation()),
            ),
            field("day", logical(URN_DATE, atomic(A::Int64))),
            field("amount", logical(URN_DECIMAL, atomic(A::Bytes))),
        ],
    );

    assert_proto(EveryType::beam_schema(), &expected);
}

/// The logical-type branch of the proto conversion, both directions, including a
/// nullable logical type and a payload.
#[test]
fn logical_types_convert_to_and_from_proto() {
    let s = Schema::builder()
        .nullable_field(
            "at",
            <DateTime<Utc> as beam::schema::BeamField>::beam_field_type(),
        )
        .field(
            "custom",
            FieldType::logical("example:custom:v1", vec![1, 2, 3], FieldType::string()),
        )
        .build();

    let mut custom = logical("example:custom:v1", atomic(A::String));
    if let Some(ProtoTypeInfo::LogicalType(l)) = custom.type_info.as_mut() {
        l.payload = vec![1, 2, 3];
    }
    let expected = schema(
        "",
        vec![
            field(
                "at",
                nullable(logical(URN_MICROS_INSTANT, micros_instant_representation())),
            ),
            field("custom", custom),
        ],
    );

    assert_proto(&s, &expected);
    assert_eq!(
        Schema::from_proto_bytes(&expected.encode_to_vec()).expect("valid"),
        s
    );
}

// ---------------------------------------------------------------------------
// Encoding positions with nulls
// ---------------------------------------------------------------------------

/// Declared a, b, c; on the wire b, c, a.
#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct Positioned {
    #[beam(encoding_position = 2)]
    a: i64,
    #[beam(encoding_position = 0)]
    b: Option<String>,
    #[beam(encoding_position = 1)]
    c: Option<i32>,
}

#[test]
fn encoding_positions_reach_the_proto() {
    let expected = proto::Schema {
        fields: vec![
            proto::Field {
                encoding_position: 2,
                ..field("a", atomic(A::Int64))
            },
            proto::Field {
                encoding_position: 0,
                ..field("b", nullable(atomic(A::String)))
            },
            proto::Field {
                encoding_position: 1,
                ..field("c", nullable(atomic(A::Int32)))
            },
        ],
        encoding_positions_set: true,
        ..Default::default()
    };
    assert_proto(Positioned::beam_schema(), &expected);
}

/// Both values and the null bitmask are indexed by encoding position.
#[test]
fn encoding_positions_order_values_and_the_null_bitmask() {
    let cases: [(Positioned, &[u8]); 3] = [
        (
            // b (position 0) is null: bit 0.
            Positioned {
                a: 5,
                b: None,
                c: Some(7),
            },
            &[0x03, 0x01, 0x01, 0x07, 0x05],
        ),
        (
            // c (position 1) is null: bit 1, although c is declared third.
            Positioned {
                a: 5,
                b: Some("x".to_string()),
                c: None,
            },
            &[0x03, 0x01, 0x02, 0x01, b'x', 0x05],
        ),
        (
            // No nulls: an empty bitmask, then b, c, a.
            Positioned {
                a: 5,
                b: Some("x".to_string()),
                c: Some(7),
            },
            &[0x03, 0x00, 0x01, b'x', 0x07, 0x05],
        ),
    ];

    for (value, golden) in cases {
        assert_eq!(
            value.to_row_bytes().expect("encode"),
            golden,
            "encoding {value:?}"
        );
        assert_eq!(
            Positioned::from_row_bytes(golden).expect("decode"),
            value,
            "decoding {golden:02x?}"
        );
    }
}

/// A schema read back from the runner's proto must decode the same bytes the same way,
/// although proto3 cannot distinguish position 0 from "no position".
#[test]
fn a_schema_read_from_its_proto_decodes_positioned_rows_identically() {
    let from_proto = Arc::new(
        Schema::from_proto_bytes(&Positioned::beam_schema().to_proto_bytes()).expect("valid"),
    );
    let decoded = RowCoder::decode_row(&from_proto, &mut &[0x03, 0x01, 0x02, 0x01, b'x', 0x05][..])
        .expect("decode");
    assert_eq!(
        decoded.values(),
        &[
            Some(FieldValue::Int64(5)),
            Some(FieldValue::String("x".to_string())),
            None,
        ]
    );
}

// ---------------------------------------------------------------------------
// Malformed protos
// ---------------------------------------------------------------------------

fn conversion_error(ft: proto::FieldType) -> SchemaError {
    let bytes = schema("", vec![field("f", ft)]).encode_to_vec();
    Schema::from_proto_bytes(&bytes).expect_err("malformed field type")
}

fn proto_conversion(msg: &str) -> SchemaError {
    SchemaError::ProtoConversion(msg.to_string())
}

#[test]
fn malformed_field_types_are_rejected_with_the_missing_part() {
    let empty = proto::FieldType::default();

    assert_eq!(
        conversion_error(empty.clone()),
        proto_conversion("Missing type_info in FieldType")
    );
    assert_eq!(
        conversion_error(atomic(A::Unspecified)),
        proto_conversion("Unspecified atomic type")
    );
    assert_eq!(
        conversion_error(proto::FieldType {
            nullable: false,
            type_info: Some(ProtoTypeInfo::AtomicType(99)),
        }),
        proto_conversion("Unknown atomic type: 99")
    );
    assert_eq!(
        conversion_error(proto::FieldType {
            nullable: false,
            type_info: Some(ProtoTypeInfo::ArrayType(Box::default())),
        }),
        proto_conversion("Missing array element type")
    );
    assert_eq!(
        conversion_error(proto::FieldType {
            nullable: false,
            type_info: Some(ProtoTypeInfo::IterableType(Box::default())),
        }),
        proto_conversion("Missing iterable element type")
    );
    assert_eq!(
        conversion_error(proto::FieldType {
            nullable: false,
            type_info: Some(ProtoTypeInfo::MapType(Box::new(proto::MapType {
                key_type: None,
                value_type: Some(Box::new(atomic(A::Int64))),
            }))),
        }),
        proto_conversion("Missing map key type")
    );
    assert_eq!(
        conversion_error(proto::FieldType {
            nullable: false,
            type_info: Some(ProtoTypeInfo::MapType(Box::new(proto::MapType {
                key_type: Some(Box::new(atomic(A::Int64))),
                value_type: None,
            }))),
        }),
        proto_conversion("Missing map value type")
    );
    assert_eq!(
        conversion_error(proto::FieldType {
            nullable: false,
            type_info: Some(ProtoTypeInfo::RowType(proto::RowType { schema: None })),
        }),
        proto_conversion("Missing nested row schema")
    );
    assert_eq!(
        conversion_error(proto::FieldType {
            nullable: false,
            type_info: Some(ProtoTypeInfo::LogicalType(Box::default())),
        }),
        proto_conversion("Missing logical representation type")
    );
    // Errors inside a nested element surface unchanged.
    assert_eq!(
        conversion_error(array(empty)),
        proto_conversion("Missing type_info in FieldType")
    );
}

#[test]
fn a_field_without_a_type_is_rejected() {
    let bytes = schema(
        "",
        vec![proto::Field {
            name: "untyped".to_string(),
            ..Default::default()
        }],
    )
    .encode_to_vec();
    assert_eq!(
        Schema::from_proto_bytes(&bytes),
        Err(proto_conversion("Missing type for field untyped"))
    );
}

#[test]
fn bytes_that_are_not_a_proto_are_a_decode_error() {
    // Field 1, wire type 2 (length-delimited), claiming 100 bytes that are not there.
    let err = Schema::from_proto_bytes(&[0x0A, 0x64]).expect_err("truncated");
    assert!(matches!(err, SchemaError::Decode(_)), "{err:?}");
}
