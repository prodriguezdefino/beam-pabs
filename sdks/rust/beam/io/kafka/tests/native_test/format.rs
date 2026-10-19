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

//! Read formats, write formats and the encoders of written rows.

use std::sync::Arc;

use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use kafka_io::native::format::{Encoder, ReadFormat, WriteFormat, read_error_schema};

#[test]
fn read_formats_parse_in_any_case() {
    assert_eq!(ReadFormat::parse("raw", None).unwrap(), ReadFormat::Raw);
    assert_eq!(
        ReadFormat::parse("String", None).unwrap(),
        ReadFormat::String
    );
    let json = ReadFormat::parse("json", Some(r#"{"properties": {"a": {"type": "string"}}}"#));
    assert!(matches!(json, Ok(ReadFormat::Json(_))));
}

#[test]
fn read_formats_that_cannot_be_built_say_why() {
    let cases = [
        ("JSON", None, "needs a JSON Schema"),
        ("AVRO", Some("{}"), "not support"),
        ("PROTO", None, "not support"),
        ("XML", None, "Unknown format"),
    ];
    cases.iter().for_each(|(name, schema, expected)| {
        let error = ReadFormat::parse(name, *schema).unwrap_err().to_string();
        assert!(error.contains(expected), "{name}: {error}");
    });
}

#[test]
fn raw_and_string_rows_hold_the_payload() {
    let raw = ReadFormat::Raw;
    let row = raw.decode(&raw.row_schema(), &[0, 255]).unwrap();
    assert_eq!(row.get_bytes("payload").unwrap(), Some(&[0, 255][..]));

    let text = ReadFormat::String;
    let schema = text.row_schema();
    let row = text.decode(&schema, "héllo".as_bytes()).unwrap();
    assert_eq!(row.get_string("payload").unwrap(), Some("héllo"));
    assert!(text.decode(&schema, &[0xff]).unwrap_err().contains("UTF-8"));
}

#[test]
fn json_rows_have_the_fields_of_the_json_schema() {
    let format = ReadFormat::json(r#"{"properties": {"a": {"type": "integer"}}}"#).unwrap();
    let schema = format.row_schema();
    assert_eq!(schema.fields[0].name, "a");
    let row = format.decode(&schema, br#"{"a": 3}"#).unwrap();
    assert_eq!(row.get_i64("a").unwrap(), Some(3));
}

#[test]
fn the_read_error_row_holds_the_value_and_the_reason() {
    let names: Vec<String> = read_error_schema()
        .fields
        .iter()
        .map(|f| format!("{}: {}", f.name, f.field_type))
        .collect();
    assert_eq!(names, ["failed_row: BYTES", "error_message: STRING"]);
}

#[test]
fn write_formats_parse_and_print_their_names() {
    assert_eq!("json".parse::<WriteFormat>().unwrap(), WriteFormat::Json);
    assert_eq!("RAW".parse::<WriteFormat>().unwrap(), WriteFormat::Raw);
    assert_eq!(WriteFormat::Json.to_string(), "JSON");
    let cases = [("STRING", "Unknown format"), ("AVRO", "not support")];
    cases.iter().for_each(|(name, expected)| {
        let error = name.parse::<WriteFormat>().unwrap_err().to_string();
        assert!(error.contains(expected), "{name}: {error}");
    });
}

#[test]
fn raw_writes_need_exactly_one_bytes_field() {
    let one = Schema::new(vec![Field::new("value", FieldType::bytes())]);
    assert_eq!(
        Encoder::new(WriteFormat::Raw, &one).unwrap(),
        Encoder::Raw(0)
    );
    let text = Schema::new(vec![Field::new("value", FieldType::string())]);
    let error = Encoder::new(WriteFormat::Raw, &text)
        .unwrap_err()
        .to_string();
    assert!(error.contains("'value' is STRING"), "{error}");
    let two = Schema::new(vec![
        Field::new("a", FieldType::bytes()),
        Field::new("b", FieldType::bytes()),
    ]);
    let error = Encoder::new(WriteFormat::Raw, &two)
        .unwrap_err()
        .to_string();
    assert!(error.contains("found 2 fields"), "{error}");
    assert_eq!(
        Encoder::new(WriteFormat::Json, &two).unwrap(),
        Encoder::Json
    );
}

#[test]
fn encoders_write_the_payload_or_a_json_object() {
    let schema = Arc::new(Schema::new(vec![Field::nullable(
        "value",
        FieldType::bytes(),
    )]));
    let row = Row::new(
        Arc::clone(&schema),
        vec![Some(FieldValue::Bytes(b"v".to_vec()))],
    )
    .unwrap();
    let mut buffer = Vec::new();
    assert_eq!(Encoder::Raw(0).encode(&row, &mut buffer).unwrap(), b"v");
    assert!(buffer.is_empty(), "RAW borrows the payload from the row");
    assert_eq!(
        Encoder::Json.encode(&row, &mut buffer).unwrap(),
        br#"{"value":"dg=="}"#
    );

    let null = Row::new(schema, vec![None]).unwrap();
    let error = Encoder::Raw(0).encode(&null, &mut buffer).unwrap_err();
    assert!(error.contains("null"), "{error}");
}
