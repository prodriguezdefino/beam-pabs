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

use beam_core::coders::{CoderError, RowCoder};
use beam_core::internals::{RowOps, RowVisitor, RowWalk, WireValue};
use beam_core::schema::{
    Field, FieldType, FieldValue, Row, Schema, TypeInfo, URN_DECIMAL, URN_MILLIS_INSTANT,
};

/// An operation tree whose operation is the value name.
struct Node {
    name: String,
    children: Vec<Node>,
}

impl Node {
    fn of(name: &str, field_type: &FieldType) -> Self {
        let node = |children| Node {
            name: name.to_string(),
            children,
        };
        match &field_type.type_info {
            TypeInfo::Atomic(_) => node(Vec::new()),
            TypeInfo::Array(item) | TypeInfo::Iterable(item) => {
                node(vec![Node::of(&format!("{name}[]"), item)])
            }
            TypeInfo::Map(key, value) => node(vec![
                Node::of(&format!("{name}.key"), key),
                Node::of(&format!("{name}.value"), value),
            ]),
            TypeInfo::Row(schema) => node(Node::fields(schema)),
            TypeInfo::Logical { representation, .. } => Node::of(name, representation),
        }
    }

    fn fields(schema: &Schema) -> Vec<Node> {
        schema
            .fields
            .iter()
            .map(|field| Node::of(&field.name, &field.field_type))
            .collect()
    }
}

impl RowOps for Node {
    type Op = String;

    fn op(&self) -> String {
        self.name.clone()
    }

    fn child(&self, index: usize) -> Option<&Self> {
        self.children.get(index)
    }
}

/// A visitor that records each call as one line.
#[derive(Default)]
struct Recorder(Vec<String>);

impl RowVisitor for Recorder {
    type Op = String;
    type Mark = usize;
    type Error = CoderError;

    fn null(&mut self, op: &String) -> Result<(), CoderError> {
        self.0.push(format!("{op} null"));
        Ok(())
    }

    fn present(&mut self, op: &String) {
        self.0.push(format!("{op} present"));
    }

    fn value(&mut self, op: &String, value: WireValue<'_>) -> Result<(), CoderError> {
        self.0.push(format!("{op} = {value:?}"));
        Ok(())
    }

    fn begin_container(&mut self, op: &String, count: Option<usize>) -> Result<usize, CoderError> {
        self.0.push(format!("{op} begin {count:?}"));
        Ok(self.0.len())
    }

    fn end_container(&mut self, op: &String, mark: usize, count: usize) -> Result<(), CoderError> {
        let lines = self.0.len() - mark;
        self.0.push(format!("{op} end {count} after {lines} lines"));
        Ok(())
    }
}

fn root(schema: &Schema) -> Node {
    Node {
        name: "row".into(),
        children: Node::fields(schema),
    }
}

fn visit(schema: &Schema, bytes: &[u8]) -> Result<Vec<String>, CoderError> {
    let walk = RowWalk::compile(schema, &root(schema))?;
    let mut recorder = Recorder::default();
    let mut reader = bytes;
    walk.walk(&mut reader, &mut recorder)?;
    assert!(reader.is_empty(), "the walk reads the whole row");
    Ok(recorder.0)
}

#[test]
fn visits_values_in_encoding_order_with_nested_rows_maps_and_logical_types() {
    let inner = Arc::new(Schema::new(vec![Field::new("x", FieldType::int32())]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", FieldType::int64()).with_encoding_position(1),
        Field::nullable("b", FieldType::string()).with_encoding_position(0),
        Field::new("c", FieldType::row(Arc::clone(&inner))).with_encoding_position(2),
        Field::new(
            "d",
            FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64()),
        )
        .with_encoding_position(3),
        Field::new(
            "e",
            FieldType::logical(URN_DECIMAL, Vec::new(), FieldType::bytes()),
        )
        .with_encoding_position(4),
        Field::new(
            "f",
            FieldType::map(FieldType::string(), FieldType::int16().with_nullable(true)),
        )
        .with_encoding_position(5),
    ]));
    let row = Row::new(
        Arc::clone(&schema),
        vec![
            Some(FieldValue::Int64(7)),
            None,
            Some(FieldValue::Row(
                Row::new(inner, vec![Some(FieldValue::Int32(-3))]).unwrap(),
            )),
            Some(FieldValue::Int64(-1234)),
            // Scale 2, then the length-prefixed unscaled value 12345.
            Some(FieldValue::Bytes(vec![2, 2, 0x30, 0x39])),
            Some(FieldValue::Map(vec![
                (FieldValue::String("k".into()), Some(FieldValue::Int16(5))),
                (FieldValue::String("n".into()), None),
            ])),
        ],
    )
    .unwrap();
    let mut bytes = Vec::new();
    RowCoder::encode_row(&row, &mut bytes).unwrap();

    assert_eq!(
        visit(&schema, &bytes).unwrap(),
        [
            "b null",
            "a present",
            "a = Int64(7)",
            "c present",
            "x present",
            "x = Int32(-3)",
            "d present",
            "d = Int64(-1234)",
            "e present",
            "e = Decimal { scale: 2, unscaled: [48, 57] }",
            "f present",
            "f begin Some(2)",
            "f.key = String(\"k\")",
            "f.value present",
            "f.value = Int16(5)",
            "f.key = String(\"n\")",
            "f.value null",
            "f end 2 after 5 lines",
        ]
    );
}

#[test]
fn gives_the_count_of_a_chunked_iterable_at_its_end() {
    let schema = Schema::new(vec![Field::new(
        "tags",
        FieldType::new(TypeInfo::Iterable(Box::new(FieldType::string())), false),
    )]);
    // Count -1, a chunk of 2 elements, a chunk of 1 element, then the 0 chunk.
    let bytes = [
        1, 0, 0xff, 0xff, 0xff, 0xff, 2, 1, b'a', 2, b'b', b'b', 1, 1, b'c', 0,
    ];

    assert_eq!(
        visit(&schema, &bytes).unwrap(),
        [
            "tags present",
            "tags begin None",
            "tags[] present",
            "tags[] = String(\"a\")",
            "tags[] present",
            "tags[] = String(\"bb\")",
            "tags[] present",
            "tags[] = String(\"c\")",
            "tags end 3 after 6 lines",
        ]
    );
}

#[test]
fn gives_the_count_of_a_counted_array_at_its_start() {
    let schema = Schema::new(vec![Field::new(
        "ids",
        FieldType::array(FieldType::int64()),
    )]);
    let bytes = [1, 0, 0, 0, 0, 2, 4, 6];

    assert_eq!(
        visit(&schema, &bytes).unwrap(),
        [
            "ids present",
            "ids begin Some(2)",
            "ids[] present",
            "ids[] = Int64(4)",
            "ids[] present",
            "ids[] = Int64(6)",
            "ids end 2 after 4 lines",
        ]
    );
}

#[test]
fn fails_on_a_state_backed_iterable() {
    let schema = Schema::new(vec![Field::new(
        "tags",
        FieldType::new(TypeInfo::Iterable(Box::new(FieldType::string())), false),
    )]);
    // Count -1, then chunk header -1 and a 1-byte continuation token.
    let mut bytes = vec![1, 0, 0xff, 0xff, 0xff, 0xff];
    bytes.extend_from_slice(&[
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 1, 9,
    ]);
    let err = visit(&schema, &bytes).unwrap_err();

    assert!(matches!(err, CoderError::StateStream(_)), "{err}");
}

#[test]
fn fails_on_an_int32_varint_outside_the_i32_range() {
    let schema = Schema::new(vec![Field::new("n", FieldType::int32())]);
    // 2^32 - 1: the five-byte varint of -1 with no sign extension.
    let bytes = [1, 0, 0xff, 0xff, 0xff, 0xff, 0x0f];

    let err = visit(&schema, &bytes).unwrap_err();
    assert!(matches!(err, CoderError::Format(_)), "{err}");
    assert!(RowCoder::decode_row(&Arc::new(schema), &mut &bytes[..]).is_err());
}

#[test]
fn compile_fails_when_the_operations_have_no_child_for_a_field() {
    let schema = Schema::new(vec![Field::new("n", FieldType::int64())]);
    let empty = root(&Schema::new(Vec::new()));
    let err = RowWalk::compile(&schema, &empty)
        .err()
        .expect("no child for field 0");

    assert!(matches!(err, CoderError::Format(_)), "{err}");
}
