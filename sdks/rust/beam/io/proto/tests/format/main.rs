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

//! Protocol Buffers and Beam rows (one binary): the schema of a message, and the round trip of
//! the wire format.

use std::sync::Arc;

use beam::schema::{FieldType, FieldValue, Row, Schema, TypeInfo};
use beam_io_proto::ProtoSchema;
use prost::Message;
use prost::encoding::{self, WireType};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MessageOptions,
};

fn field(name: &str, number: i32, label: Label, field_type: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(field_type as i32),
        ..Default::default()
    }
}

fn typed(mut field: FieldDescriptorProto, type_name: &str) -> FieldDescriptorProto {
    field.type_name = Some(type_name.to_string());
    field
}

fn message(name: &str, fields: Vec<FieldDescriptorProto>) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.to_string()),
        field: fields,
        ..Default::default()
    }
}

fn file(syntax: &str, messages: Vec<DescriptorProto>) -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some("test.proto".to_string()),
        package: Some("test".to_string()),
        syntax: Some(syntax.to_string()),
        message_type: messages,
        enum_type: vec![EnumDescriptorProto {
            name: Some("Color".to_string()),
            value: ["RED", "GREEN", "BLUE"]
                .iter()
                .zip(0..)
                .map(|(name, number)| EnumValueDescriptorProto {
                    name: Some(name.to_string()),
                    number: Some(number),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn descriptor_set(files: Vec<FileDescriptorProto>) -> Vec<u8> {
    FileDescriptorSet { file: files }.encode_to_vec()
}

/// The descriptor set of:
///
/// ```text
/// syntax = "proto3";
/// package test;
/// enum Color { RED = 0; GREEN = 1; BLUE = 2; }
/// message Inner { string label = 1; int64 count = 2; }
/// message Outer {
///   int32 id = 1; string name = 2; double score = 3; bool ok = 4; bytes data = 5;
///   Color color = 6; Inner inner = 7; repeated int64 values = 8; repeated Inner items = 9;
///   optional string note = 10; sint32 delta = 11; uint32 small = 12; fixed64 big = 13;
///   float ratio = 14; repeated string tags = 15;
/// }
/// ```
fn outer_set() -> Vec<u8> {
    let inner = message(
        "Inner",
        vec![
            field("label", 1, Label::Optional, Type::String),
            field("count", 2, Label::Optional, Type::Int64),
        ],
    );
    let mut note = field("note", 10, Label::Optional, Type::String);
    note.proto3_optional = Some(true);
    note.oneof_index = Some(0);
    let mut outer = message(
        "Outer",
        vec![
            field("id", 1, Label::Optional, Type::Int32),
            field("name", 2, Label::Optional, Type::String),
            field("score", 3, Label::Optional, Type::Double),
            field("ok", 4, Label::Optional, Type::Bool),
            field("data", 5, Label::Optional, Type::Bytes),
            typed(
                field("color", 6, Label::Optional, Type::Enum),
                ".test.Color",
            ),
            typed(
                field("inner", 7, Label::Optional, Type::Message),
                ".test.Inner",
            ),
            field("values", 8, Label::Repeated, Type::Int64),
            typed(
                field("items", 9, Label::Repeated, Type::Message),
                ".test.Inner",
            ),
            note,
            field("delta", 11, Label::Optional, Type::Sint32),
            field("small", 12, Label::Optional, Type::Uint32),
            field("big", 13, Label::Optional, Type::Fixed64),
            field("ratio", 14, Label::Optional, Type::Float),
            field("tags", 15, Label::Repeated, Type::String),
        ],
    );
    outer.oneof_decl = vec![prost_types::OneofDescriptorProto {
        name: Some("_note".to_string()),
        ..Default::default()
    }];
    descriptor_set(vec![file("proto3", vec![inner, outer])])
}

fn row_schema(field_type: &FieldType) -> Arc<Schema> {
    match &field_type.type_info {
        TypeInfo::Row(schema) => Arc::clone(schema),
        other => panic!("a row type, got {other:?}"),
    }
}

fn inner_bytes(label: &str, count: i64) -> Vec<u8> {
    let mut bytes = Vec::new();
    encoding::string::encode(1, &label.to_string(), &mut bytes);
    encoding::int64::encode(2, &count, &mut bytes);
    bytes
}

/// A message with every field set, in field order, as a protobuf encoder writes it.
fn outer_bytes() -> Vec<u8> {
    let mut bytes = Vec::new();
    encoding::int32::encode(1, &-7, &mut bytes);
    encoding::string::encode(2, &"héllo".to_string(), &mut bytes);
    encoding::double::encode(3, &2.5, &mut bytes);
    encoding::bool::encode(4, &true, &mut bytes);
    encoding::bytes::encode(5, &vec![0_u8, 255], &mut bytes);
    encoding::int32::encode(6, &2, &mut bytes);
    encoding::bytes::encode(7, &inner_bytes("in", 3), &mut bytes);
    encoding::int64::encode_packed(8, &[1, -2, 300], &mut bytes);
    encoding::bytes::encode(9, &inner_bytes("a", 1), &mut bytes);
    encoding::bytes::encode(9, &inner_bytes("b", 2), &mut bytes);
    encoding::string::encode(10, &"n".to_string(), &mut bytes);
    encoding::sint32::encode(11, &-3, &mut bytes);
    encoding::uint32::encode(12, &u32::MAX, &mut bytes);
    encoding::fixed64::encode(13, &u64::MAX, &mut bytes);
    encoding::float::encode(14, &0.5, &mut bytes);
    encoding::string::encode(15, &"x".to_string(), &mut bytes);
    encoding::string::encode(15, &"y".to_string(), &mut bytes);
    bytes
}

fn make_row(schema: &Arc<Schema>, values: Vec<Option<FieldValue>>) -> Row {
    Row::new(Arc::clone(schema), values).expect("values of the schema")
}

fn decode(proto: &ProtoSchema, bytes: &[u8]) -> Row {
    proto.decode(bytes).expect("a message")
}

fn encode(proto: &ProtoSchema, row: &Row) -> Vec<u8> {
    proto
        .check_fields(row.schema())
        .expect("the rows fit the message");
    let mut buffer = Vec::new();
    proto.encode(row, &mut buffer).expect("the row encodes");
    buffer
}

#[test]
fn the_schema_of_a_message_follows_its_fields() {
    let proto = ProtoSchema::new(&outer_set(), "test.Outer").unwrap();
    let schema = proto.schema();
    let fields: Vec<(&str, String, bool)> = schema
        .fields
        .iter()
        .map(|f| {
            (
                f.name.as_str(),
                f.field_type.to_string(),
                f.field_type.nullable,
            )
        })
        .collect();
    let inner = row_schema(&schema.fields[6].field_type);
    let names: Vec<&str> = inner.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["label", "count"]);
    let expected = [
        ("id", FieldType::int32(), false),
        ("name", FieldType::string(), false),
        ("score", FieldType::double(), false),
        ("ok", FieldType::boolean(), false),
        ("data", FieldType::bytes(), false),
        ("color", FieldType::string(), false),
        ("inner", FieldType::row(Arc::clone(&inner)), true),
        ("values", FieldType::array(FieldType::int64()), false),
        ("items", FieldType::array(FieldType::row(inner)), false),
        ("note", FieldType::string(), true),
        ("delta", FieldType::int32(), false),
        ("small", FieldType::int64(), false),
        ("big", FieldType::int64(), false),
        ("ratio", FieldType::float(), false),
        ("tags", FieldType::array(FieldType::string()), false),
    ];
    let expected: Vec<(&str, String, bool)> = expected
        .into_iter()
        .map(|(name, field_type, nullable)| {
            (
                name,
                field_type.with_nullable(nullable).to_string(),
                nullable,
            )
        })
        .collect();
    assert_eq!(fields, expected);
    // A leading dot names the same message.
    assert_eq!(
        ProtoSchema::new(&outer_set(), ".test.Outer")
            .unwrap()
            .schema(),
        schema
    );
}

#[test]
fn a_read_gives_scalars_enums_messages_and_repeated_fields() {
    let proto = ProtoSchema::new(&outer_set(), "test.Outer").unwrap();
    let schema = proto.schema();
    let inner = row_schema(&schema.fields[6].field_type);
    let inner_row = |label: &str, count: i64| {
        FieldValue::Row(make_row(
            &inner,
            vec![
                Some(FieldValue::String(label.to_string())),
                Some(FieldValue::Int64(count)),
            ],
        ))
    };
    let row = decode(&proto, &outer_bytes());
    assert_eq!(
        row.values(),
        [
            Some(FieldValue::Int32(-7)),
            Some(FieldValue::String("héllo".to_string())),
            Some(FieldValue::Double(2.5)),
            Some(FieldValue::Boolean(true)),
            Some(FieldValue::Bytes(vec![0, 255])),
            Some(FieldValue::String("BLUE".to_string())),
            Some(inner_row("in", 3)),
            Some(FieldValue::Array(vec![
                Some(FieldValue::Int64(1)),
                Some(FieldValue::Int64(-2)),
                Some(FieldValue::Int64(300)),
            ])),
            Some(FieldValue::Array(vec![
                Some(inner_row("a", 1)),
                Some(inner_row("b", 2)),
            ])),
            Some(FieldValue::String("n".to_string())),
            Some(FieldValue::Int32(-3)),
            Some(FieldValue::Int64(i64::from(u32::MAX))),
            Some(FieldValue::Int64(-1)),
            Some(FieldValue::Float(0.5)),
            Some(FieldValue::Array(vec![
                Some(FieldValue::String("x".to_string())),
                Some(FieldValue::String("y".to_string())),
            ])),
        ]
    );
}

#[test]
fn a_write_gives_the_bytes_of_a_protobuf_encoder_and_reads_back() {
    let proto = ProtoSchema::new(&outer_set(), "test.Outer").unwrap();
    let bytes = outer_bytes();
    let row = decode(&proto, &bytes);
    assert_eq!(encode(&proto, &row), bytes);
}

#[test]
fn missing_fields_read_as_defaults_or_null_and_defaults_are_not_written() {
    let proto = ProtoSchema::new(&outer_set(), "test.Outer").unwrap();
    let row = decode(&proto, &[]);
    assert_eq!(
        row.values(),
        [
            Some(FieldValue::Int32(0)),
            Some(FieldValue::String(String::new())),
            Some(FieldValue::Double(0.0)),
            Some(FieldValue::Boolean(false)),
            Some(FieldValue::Bytes(Vec::new())),
            Some(FieldValue::String("RED".to_string())),
            None,
            Some(FieldValue::Array(Vec::new())),
            Some(FieldValue::Array(Vec::new())),
            None,
            Some(FieldValue::Int32(0)),
            Some(FieldValue::Int64(0)),
            Some(FieldValue::Int64(0)),
            Some(FieldValue::Float(0.0)),
            Some(FieldValue::Array(Vec::new())),
        ]
    );
    assert_eq!(encode(&proto, &row), Vec::<u8>::new());
}

#[test]
fn a_read_takes_unpacked_repeated_values_and_skips_unknown_fields() {
    let proto = ProtoSchema::new(&outer_set(), "test.Outer").unwrap();
    let mut bytes = Vec::new();
    encoding::int64::encode_repeated(8, &[4, 5], &mut bytes);
    encoding::string::encode(99, &"unknown".to_string(), &mut bytes);
    encoding::fixed32::encode(98, &1, &mut bytes);
    encoding::encode_key(97, WireType::SixtyFourBit, &mut bytes);
    bytes.extend_from_slice(&[0; 8]);
    encoding::int32::encode(1, &9, &mut bytes);
    let row = decode(&proto, &bytes);
    assert_eq!(row.get_value("id").unwrap(), &Some(FieldValue::Int32(9)));
    assert_eq!(
        row.get_value("values").unwrap(),
        &Some(FieldValue::Array(vec![
            Some(FieldValue::Int64(4)),
            Some(FieldValue::Int64(5)),
        ]))
    );
}

#[test]
fn records_that_do_not_fit_the_message_fail_with_the_reason() {
    let proto = ProtoSchema::new(&outer_set(), "test.Outer").unwrap();
    let mut unknown_enum = Vec::new();
    encoding::int32::encode(6, &9, &mut unknown_enum);
    let mut wrong_wire_type = Vec::new();
    encoding::string::encode(1, &"not an int".to_string(), &mut wrong_wire_type);
    let mut bad_utf8 = Vec::new();
    encoding::bytes::encode(2, &vec![0xff, 0xfe], &mut bad_utf8);
    let cases: [(&[u8], &str); 4] = [
        (&unknown_enum, "no value 9"),
        (&wrong_wire_type, "does not decode"),
        (&bad_utf8, "does not decode"),
        (&[0x08], "does not decode"),
    ];
    cases.iter().for_each(|(bytes, expected)| {
        let error = proto.decode(bytes).unwrap_err();
        assert!(error.contains(expected), "{error}");
    });
}

#[test]
fn a_write_rejects_rows_that_do_not_fit_the_message() {
    let proto = ProtoSchema::new(&outer_set(), "test.Outer").unwrap();
    let other = Arc::new(Schema::new(vec![beam::schema::Field::new(
        "id",
        FieldType::int32(),
    )]));
    let error = proto.check_fields(&other).unwrap_err();
    assert!(error.contains("test.Outer"), "{error}");

    let row = decode(&proto, &outer_bytes());
    let mut values = row.values().to_vec();
    values[5] = Some(FieldValue::String("PURPLE".to_string()));
    let bad_enum = make_row(row.schema(), values);
    let error = proto.encode(&bad_enum, &mut Vec::new()).unwrap_err();
    assert!(error.contains("PURPLE"), "{error}");
}

#[test]
fn proto2_optional_fields_are_nullable_and_required_fields_are_always_written() {
    let set = descriptor_set(vec![file(
        "proto2",
        vec![message(
            "Legacy",
            vec![
                field("key", 1, Label::Required, Type::Int32),
                field("label", 2, Label::Optional, Type::String),
            ],
        )],
    )]);
    let proto = ProtoSchema::new(&set, "test.Legacy").unwrap();
    let nullable: Vec<bool> = proto
        .schema()
        .fields
        .iter()
        .map(|f| f.field_type.nullable)
        .collect();
    assert_eq!(nullable, [false, true]);
    let row = make_row(proto.schema(), vec![Some(FieldValue::Int32(0)), None]);
    assert_eq!(encode(&proto, &row), [0x08, 0x00]);
    assert_eq!(decode(&proto, &[0x08, 0x00]), row);
}

#[test]
fn map_fields_are_beam_maps_sorted_by_key() {
    let map_entry = DescriptorProto {
        name: Some("CountsEntry".to_string()),
        field: vec![
            field("key", 1, Label::Optional, Type::String),
            field("value", 2, Label::Optional, Type::Int32),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut with_map = message(
        "WithMap",
        vec![typed(
            field("counts", 1, Label::Repeated, Type::Message),
            ".test.WithMap.CountsEntry",
        )],
    );
    with_map.nested_type = vec![map_entry];
    let set = descriptor_set(vec![file("proto3", vec![with_map])]);
    let proto = ProtoSchema::new(&set, "test.WithMap").unwrap();
    assert_eq!(
        proto.schema().fields[0].field_type,
        FieldType::map(FieldType::string(), FieldType::int32())
    );
    let mut bytes = Vec::new();
    for (key, value) in [("b", 2), ("a", 1)] {
        let mut entry = Vec::new();
        encoding::string::encode(1, &key.to_string(), &mut entry);
        encoding::int32::encode(2, &value, &mut entry);
        encoding::bytes::encode(1, &entry, &mut bytes);
    }
    let row = decode(&proto, &bytes);
    assert_eq!(
        row.values(),
        [Some(FieldValue::Map(vec![
            (
                FieldValue::String("a".to_string()),
                Some(FieldValue::Int32(1))
            ),
            (
                FieldValue::String("b".to_string()),
                Some(FieldValue::Int32(2))
            ),
        ]))]
    );
    assert_eq!(decode(&proto, &encode(&proto, &row)), row);
}

#[test]
fn unsupported_messages_fail_with_the_reason() {
    let node = message(
        "Node",
        vec![typed(
            field("next", 1, Label::Optional, Type::Message),
            ".test.Node",
        )],
    );
    let set = descriptor_set(vec![file("proto3", vec![node])]);
    let cases: [(&[u8], &str, &str); 3] = [
        (&set, "test.Node", "recursive"),
        (&set, "test.Missing", "no message"),
        (b"not a descriptor set", "test.Outer", "does not decode"),
    ];
    cases.iter().for_each(|(set, name, expected)| {
        let error = ProtoSchema::new(set, name).unwrap_err();
        assert!(error.contains(expected), "{name}: {error}");
    });
}

#[test]
fn a_message_reads_its_descriptor_set_from_a_file() {
    let path = std::env::temp_dir().join(format!("proto-io-{}.pb", std::process::id()));
    std::fs::write(&path, outer_set()).unwrap();
    let proto = ProtoSchema::from_file(&path, "test.Outer").unwrap();
    assert_eq!(proto.message_name(), "test.Outer");
    std::fs::remove_file(&path).unwrap();
    let error = ProtoSchema::from_file(&path, "test.Outer").unwrap_err();
    assert!(error.contains("Cannot read"), "{error}");
}
