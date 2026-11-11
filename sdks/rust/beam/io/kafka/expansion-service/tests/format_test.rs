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

//! The record formats of the configurations: the codec of a format name and its schema.

use beam_io_kafka_expansion::Format;
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};

/// The descriptor set of `message test.Event { string id = 1; int64 count = 2; }`.
fn event_set() -> Vec<u8> {
    let field = |name: &str, number: i32, field_type: Type| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(field_type as i32),
        ..Default::default()
    };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("event.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("Event".to_string()),
                field: vec![field("id", 1, Type::String), field("count", 2, Type::Int64)],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

#[test]
fn formats_parse_in_any_case() {
    assert!(matches!(Format::parse("raw", None), Ok(Format::Raw(_))));
    assert!(matches!(
        Format::parse("String", None),
        Ok(Format::String(_))
    ));
    let json = Format::parse("json", Some(r#"{"properties": {"a": {"type": "string"}}}"#));
    assert!(matches!(json, Ok(Format::Json(_))));
    let avro = Format::parse(
        "AVRO",
        Some(r#"{"type": "record", "name": "R", "fields": [{"name": "a", "type": "int"}]}"#),
    );
    assert!(matches!(avro, Ok(Format::Avro(_))));
    let cases = [
        ("PROTO", None, "file_descriptor_path"),
        ("XML", None, "Unknown format"),
        ("AVRO", Some("{}"), "Avro schema"),
    ];
    cases.iter().for_each(|(name, schema, expected)| {
        let error = Format::parse(name, *schema)
            .expect_err("an error")
            .to_string();
        assert!(error.contains(expected), "{name}: {error}");
    });
}

#[test]
fn the_proto_format_reads_the_descriptor_set_from_a_file() {
    let path = std::env::temp_dir().join(format!("kafka-proto-{}.pb", std::process::id()));
    std::fs::write(&path, event_set()).unwrap();
    let format = Format::proto(&path.display().to_string(), "test.Event").unwrap();
    assert!(matches!(format, Format::Proto(_)), "{format:?}");
    assert!(format.validate_read().is_ok());
    std::fs::remove_file(&path).unwrap();
    let error = Format::proto(&path.display().to_string(), "test.Event")
        .unwrap_err()
        .to_string();
    assert!(error.contains("Cannot read"), "{error}");
    let error = Format::parse("proto", None).unwrap_err().to_string();
    assert!(error.contains("file_descriptor_path"), "{error}");
}
