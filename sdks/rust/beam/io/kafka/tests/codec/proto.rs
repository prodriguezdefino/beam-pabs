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

//! The PROTO codec of Kafka records, on `proto_io`. `proto_io` tests the schema mapping.

use std::sync::Arc;

use beam::prelude::BeamRow;
use beam::schema::{Field, FieldType, FieldValue, RowElement, Schema};
use beam_io_kafka::codec::{Encoder, Proto, Typed};
use prost::Message;
use prost::encoding;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};

use super::common::{bind, decode, make_row, record};

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

fn event_bytes(id: &str, count: i64) -> Vec<u8> {
    let mut bytes = Vec::new();
    encoding::string::encode(1, &id.to_string(), &mut bytes);
    encoding::int64::encode(2, &count, &mut bytes);
    bytes
}

#[test]
fn proto_records_round_trip_through_rows() {
    let mut proto = Proto::new(&event_set(), "test.Event").unwrap();
    let schema = Arc::clone(proto.row_schema());
    let bytes = event_bytes("e-1", 42);
    let RowElement::Row(row) = decode(&proto, &record(Some(&bytes))).unwrap() else {
        panic!("a decoded row")
    };
    assert_eq!(
        row,
        make_row(
            &schema,
            vec![
                Some(FieldValue::String("e-1".to_string())),
                Some(FieldValue::Int64(42)),
            ],
        )
    );
    bind(&mut proto, &schema).unwrap();
    let element = RowElement::Row(row);
    let mut buffer = vec![1, 2, 3];
    let outgoing = proto.encode(&mut (), &element, &mut buffer).unwrap();
    assert_eq!(outgoing.value, Some(bytes.as_slice()));
    assert!(decode(&proto, &record(None)).is_err());
}

#[test]
fn a_proto_write_needs_rows_with_the_fields_of_the_message() {
    let mut proto = Proto::new(&event_set(), "test.Event").unwrap();
    let other = Arc::new(Schema::new(vec![Field::new("id", FieldType::string())]));
    let error = bind(&mut proto, &other).unwrap_err().to_string();
    assert!(error.contains("test.Event"), "{error}");
    let error = Proto::new(&event_set(), "test.Missing")
        .unwrap_err()
        .to_string();
    assert!(error.contains("no message"), "{error}");
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct Event {
    id: String,
    count: i64,
}

#[test]
fn a_typed_proto_codec_writes_the_message_that_it_reads() {
    let mut proto = Typed::<_, Event>::new(Proto::new(&event_set(), "test.Event").unwrap());
    proto.prepare().unwrap();
    let item = Event {
        id: "e-2".to_string(),
        count: 7,
    };
    let mut buffer = Vec::new();
    let sent = proto.encode(&mut (), &item, &mut buffer).unwrap();
    assert_eq!(sent.value, Some(event_bytes("e-2", 7).as_slice()));
    assert_eq!(
        decode(&proto, &record(Some(&event_bytes("e-2", 7)))).unwrap(),
        item
    );
}
