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

//! JSON Schemas to Beam schemas, and rows to and from JSON objects.

use std::sync::Arc;

use beam::schema::{Field, FieldType, FieldValue, Row, Schema};
use json_io::{JsonReader, schema_from_json_schema, write_json};
use serde_json::Value;

const SCHEMA: &str = r#"{
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "id": {"type": "integer"},
        "score": {"type": "number"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "point": {
            "type": "object",
            "properties": {"x": {"type": "integer"}, "y": {"type": "integer"}},
            "required": ["x", "y"]
        },
        "note": {"type": ["string", "null"]}
    },
    "required": ["id", "name", "note"]
}"#;

fn reader() -> JsonReader {
    let schema = schema_from_json_schema(SCHEMA).expect("SCHEMA is a valid JSON Schema");
    JsonReader::new(Arc::new(schema)).expect("SCHEMA has only supported types")
}

fn reader_of(fields: Vec<Field>) -> JsonReader {
    JsonReader::new(Arc::new(Schema::new(fields))).expect("the schema has only supported types")
}

fn json(row: &Row) -> Value {
    let mut buffer = Vec::new();
    write_json(row, &mut buffer).expect("the row has a JSON form");
    serde_json::from_slice(&buffer).expect("write_json writes JSON")
}

#[test]
fn required_properties_come_first_in_order_then_the_others_by_name() {
    let schema = schema_from_json_schema(SCHEMA).unwrap();
    let fields: Vec<(&str, bool)> = schema
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.nullable))
        .collect();
    assert_eq!(
        fields,
        [
            ("id", false),
            ("name", false),
            ("note", true),
            ("point", true),
            ("score", true),
            ("tags", true),
        ]
    );
    let types: Vec<String> = schema.fields[..3]
        .iter()
        .chain(&schema.fields[4..])
        .map(|f| f.field_type.to_string())
        .collect();
    assert_eq!(
        types,
        ["INT64", "STRING", "STRING?", "DOUBLE?", "ARRAY<STRING>?"]
    );
}

#[test]
fn unsupported_schemas_name_the_property() {
    let cases = [
        (
            r#"{"properties": {"t": {"type": "array", "items": [{"type": "string"}]}}}"#,
            ".t",
        ),
        (r##"{"properties": {"r": {"$ref": "#/defs/x"}}}"##, "$ref"),
        (r#"{"properties": {}, "required": ["x"]}"#, "'x'"),
        (
            r#"{"properties": {"u": {"type": ["string", "integer"]}}}"#,
            ".u",
        ),
        (r#"{"type": "string"}"#, "properties"),
        ("{", "not JSON"),
    ];
    cases.iter().for_each(|(schema, expected)| {
        let error = schema_from_json_schema(schema).unwrap_err();
        assert!(error.contains(expected), "{schema}: {error}");
    });
}

#[test]
fn an_object_round_trips_through_a_row_and_unknown_members_are_ignored() {
    let reader = reader();
    let text = r#"{"id": 7, "name": "a", "note": null, "point": {"x": 1, "y": -2},
                   "score": 1.5, "tags": ["p", "q"], "extra": {"deep": [1, {"x": null}]}}"#;
    let row = reader.read(text.as_bytes()).unwrap();
    assert_eq!(row.get_i64("id").unwrap(), Some(7));
    assert_eq!(row.get_string("note").unwrap(), None);
    let expected: Value = serde_json::from_str(
        r#"{"id": 7, "name": "a", "note": null, "point": {"x": 1, "y": -2},
            "score": 1.5, "tags": ["p", "q"]}"#,
    )
    .unwrap();
    assert_eq!(json(&row), expected);
}

/// Each message is exact: the reader keeps its own text, so serde_json adds no position.
#[test]
fn bad_values_fail_with_the_path_of_the_field_and_the_reason() {
    let reader = reader();
    let cases: [(&[u8], &str); 9] = [
        (
            br#"{"name": "a", "note": null}"#,
            "field 'id': null in a field that is not nullable",
        ),
        (
            br#"{"id": null, "name": "a"}"#,
            "field 'id': null in a field that is not nullable",
        ),
        (
            br#"{"id": "7", "name": "a"}"#,
            "field 'id': expected INT64, found a string",
        ),
        (
            br#"{"id": 7, "name": "a", "point": {"x": 1}}"#,
            "field 'point': field 'y': null in a field that is not nullable",
        ),
        (
            br#"{"id": 7, "name": "a", "point": {"x": true, "y": 1}}"#,
            "field 'point': field 'x': expected INT64, found a boolean",
        ),
        (
            br#"{"id": 7, "name": "a", "point": [1, 2]}"#,
            "field 'point': expected a JSON object, found an array",
        ),
        (
            br#"{"id": 7, "name": "a", "tags": "p"}"#,
            "field 'tags': expected an array, found a string",
        ),
        (
            br#"{"id": 7, "name": "a", "tags": ["p", 1]}"#,
            "field 'tags': expected STRING, found a number",
        ),
        (
            br#"{"id": 1.5, "name": "a"}"#,
            "field 'id': expected INT64, found a number",
        ),
    ];
    cases.iter().for_each(|(text, expected)| {
        assert_eq!(&reader.read(text).unwrap_err(), expected);
    });
}

#[test]
fn a_root_that_is_not_an_object_names_what_it_is() {
    let reader = reader();
    let cases: [(&[u8], &str); 5] = [
        (b"null", "expected a JSON object, found null"),
        (b"5", "expected a JSON object, found a number"),
        (br#""x""#, "expected a JSON object, found a string"),
        (b"true", "expected a JSON object, found a boolean"),
        (b"[1]", "expected a JSON object, found an array"),
    ];
    cases.iter().for_each(|(text, expected)| {
        assert_eq!(&reader.read(text).unwrap_err(), expected);
    });
}

#[test]
fn text_that_is_not_json_fails_as_not_json() {
    let reader = reader();
    let cases: [&[u8]; 4] = [
        b"{",
        br#"{"id": 7, "name": "a"} trailing"#,
        br#"{"id": 7 "name": "a"}"#,
        b"",
    ];
    cases.iter().for_each(|text| {
        let error = reader.read(text).unwrap_err();
        assert!(error.starts_with("not JSON: "), "{error}");
    });
}

#[test]
fn the_last_member_with_a_name_counts_and_escaped_names_match() {
    let reader = reader();
    let row = reader
        .read(br#"{"id": 1, "na\u006de": "escaped", "name": "last", "id": 2}"#)
        .unwrap();
    assert_eq!(row.get_i64("id").unwrap(), Some(2));
    assert_eq!(row.get_string("name").unwrap(), Some("last"));

    let row = reader
        .read(br#"{"id": 1, "na\u006de": "escaped"}"#)
        .unwrap();
    assert_eq!(row.get_string("name").unwrap(), Some("escaped"));
}

#[test]
fn map_entries_are_in_key_order_and_a_repeated_key_keeps_its_last_value() {
    let reader = reader_of(vec![Field::new(
        "m",
        FieldType::map(FieldType::string(), FieldType::int64()),
    )]);
    let row = reader.read(br#"{"m": {"b": 1, "a": 2, "b": 3}}"#).unwrap();
    assert_eq!(
        row.values()[0],
        Some(FieldValue::Map(vec![
            (
                FieldValue::String("a".to_string()),
                Some(FieldValue::Int64(2))
            ),
            (
                FieldValue::String("b".to_string()),
                Some(FieldValue::Int64(3))
            ),
        ]))
    );
}

#[test]
fn map_keys_of_integer_types_must_hold_integers_in_range() {
    let reader = reader_of(vec![Field::new(
        "m",
        FieldType::map(FieldType::byte(), FieldType::string()),
    )]);
    assert_eq!(
        reader.read(br#"{"m": {"x": "a"}}"#).unwrap_err(),
        "field 'm': the map key 'x' is not an integer"
    );
    assert_eq!(
        reader.read(br#"{"m": {"300": "a"}}"#).unwrap_err(),
        "field 'm': 300 is out of range for BYTE"
    );
}

#[test]
fn a_schema_with_many_fields_finds_each_member() {
    let fields: Vec<Field> = (0..40)
        .map(|i| Field::nullable(format!("f{i}"), FieldType::int64()))
        .collect();
    let reader = reader_of(fields);
    let row = reader
        .read(br#"{"f39": 39, "f0": 0, "f17": 17, "unknown": 1}"#)
        .unwrap();
    assert_eq!(row.get_i64("f0").unwrap(), Some(0));
    assert_eq!(row.get_i64("f17").unwrap(), Some(17));
    assert_eq!(row.get_i64("f39").unwrap(), Some(39));
    assert_eq!(row.get_i64("f1").unwrap(), None);
}

#[test]
fn numbers_convert_to_the_numeric_type_of_the_field() {
    let reader = reader_of(vec![
        Field::nullable("i", FieldType::int64()),
        Field::nullable("d", FieldType::double()),
        Field::nullable("f", FieldType::float()),
    ]);
    let row = reader
        .read(br#"{"i": -9223372036854775808, "d": 18446744073709551615, "f": 3}"#)
        .unwrap();
    assert_eq!(row.get_i64("i").unwrap(), Some(i64::MIN));
    assert_eq!(
        row.values()[1],
        Some(FieldValue::Double(18_446_744_073_709_551_615_f64))
    );
    assert_eq!(row.values()[2], Some(FieldValue::Float(3.0)));
    assert_eq!(
        reader.read(br#"{"i": 18446744073709551615}"#).unwrap_err(),
        "field 'i': expected INT64, found a number"
    );
}

#[test]
fn bytes_are_base64_and_maps_are_objects_with_integer_keys() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", FieldType::bytes()),
        Field::new("m", FieldType::map(FieldType::int32(), FieldType::string())),
        Field::new("f", FieldType::float()),
        Field::nullable("n", FieldType::int16()),
    ]));
    let row = Row::new(
        Arc::clone(&schema),
        vec![
            Some(FieldValue::Bytes(vec![0, 1, 255])),
            Some(FieldValue::Map(vec![(
                FieldValue::Int32(3),
                Some(FieldValue::String("three".to_string())),
            )])),
            Some(FieldValue::Float(0.5)),
            None,
        ],
    )
    .unwrap();
    let object = json(&row);
    assert_eq!(object["b"], "AAH/");
    assert_eq!(object["m"]["3"], "three");
    assert_eq!(object["n"], Value::Null);
    let back = JsonReader::new(schema)
        .unwrap()
        .read(object.to_string().as_bytes())
        .unwrap();
    assert_eq!(back, row);

    let reader = reader_of(vec![Field::new("b", FieldType::bytes())]);
    let error = reader.read(br#"{"b": "not base64!"}"#).unwrap_err();
    assert!(error.starts_with("field 'b': not base64: "), "{error}");
}

#[test]
fn a_null_item_needs_a_nullable_item_type() {
    let strict = reader_of(vec![Field::new("a", FieldType::array(FieldType::int64()))]);
    assert_eq!(
        strict.read(br#"{"a": [1, null]}"#).unwrap_err(),
        "field 'a': null in a field that is not nullable"
    );
    let lenient = reader_of(vec![Field::new(
        "a",
        FieldType::array(FieldType::int64().with_nullable(true)),
    )]);
    let row = lenient.read(br#"{"a": [1, null]}"#).unwrap();
    assert_eq!(
        row.values()[0],
        Some(FieldValue::Array(vec![Some(FieldValue::Int64(1)), None]))
    );
}

#[test]
fn values_with_no_json_form_or_out_of_range_fail() {
    let schema = Arc::new(Schema::new(vec![Field::new("d", FieldType::double())]));
    let row = Row::new(schema, vec![Some(FieldValue::Double(f64::NAN))]).unwrap();
    let error = write_json(&row, &mut Vec::new()).unwrap_err();
    assert!(error.contains("no JSON number"), "{error}");

    let narrow = reader_of(vec![Field::new("n", FieldType::int16())]);
    assert_eq!(
        narrow.read(br#"{"n": 70000}"#).unwrap_err(),
        "field 'n': 70000 is out of range for INT16"
    );
}

#[test]
fn write_json_replaces_the_buffer_contents() {
    let schema = Arc::new(Schema::new(vec![Field::new("s", FieldType::string())]));
    let row = Row::new(schema, vec![Some(FieldValue::String("x".to_string()))]).unwrap();
    let mut buffer = b"old contents".to_vec();
    write_json(&row, &mut buffer).unwrap();
    assert_eq!(buffer, br#"{"s":"x"}"#);
}
