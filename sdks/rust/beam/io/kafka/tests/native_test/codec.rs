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

//! The codecs: how records become elements and elements become records. No test sends or
//! reads a record.

use std::sync::Arc;

use beam::Instant;
use beam::coders::DefaultCoder;
use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use kafka_io::native::codec::managed::{Format, Payload};
use kafka_io::native::codec::{
    Avro, BeamRow, CodecError, Decoder, Encoder, Framing, Json, KafkaRecord, Raw, RecordRef, Utf8,
    WithMetadata, read_error_schema, write_error_schema,
};

const NO_HEADERS: &[(&str, Option<&[u8]>)] = &[];

fn record(value: Option<&[u8]>) -> RecordRef<'_> {
    RecordRef {
        topic: "events",
        partition: 3,
        offset: 42,
        timestamp: Some(Instant::from_millis(1_000)),
        key: None,
        value,
        headers: NO_HEADERS,
    }
}

/// Gives `encoder` the schema of rows of `schema`, as the write does.
fn bind<E: Encoder<Input = Row>>(encoder: &mut E, schema: &Arc<Schema>) -> Result {
    let pipeline = Pipeline::new();
    let rows = pipeline
        .apply("Create", Create::new(Vec::<Row>::new()))
        .with_row_schema(schema);
    encoder.elements(&rows).map(|_| ())
}

fn make_row(schema: &Arc<Schema>, values: Vec<Option<FieldValue>>) -> Row {
    Row::new(Arc::clone(schema), values).expect("values of the schema")
}

fn encode<E: Encoder>(encoder: &E, element: &E::Element) -> Result<Vec<u8>, CodecError> {
    let mut buffer = Vec::new();
    let outgoing = encoder.encode(element, &mut buffer)?;
    Ok(outgoing.value.unwrap_or_default().to_vec())
}

#[test]
fn raw_values_are_the_bytes_and_a_tombstone_does_not_convert() {
    assert_eq!(Raw.decode(&record(Some(&[0, 255]))).unwrap(), vec![0, 255]);
    let error = Raw.decode(&record(None)).unwrap_err();
    assert!(error.message().contains("no value"), "{error}");

    let element = vec![1, 2, 3];
    let mut buffer = Vec::new();
    let outgoing = Raw.encode(&element, &mut buffer).unwrap();
    assert_eq!(
        outgoing.value.map(<[u8]>::as_ptr),
        Some(element.as_ptr()),
        "Raw sends the element as it is"
    );
    assert_eq!(Raw.failed_type(), FieldType::bytes());
}

#[test]
fn utf8_values_are_text() {
    assert_eq!(
        Utf8.decode(&record(Some("héllo".as_bytes()))).unwrap(),
        "héllo"
    );
    let error = Utf8.decode(&record(Some(&[0xff]))).unwrap_err();
    assert!(error.message().contains("UTF-8"), "{error}");
    assert_eq!(
        encode(&Utf8, &"héllo".to_string()).unwrap(),
        "héllo".as_bytes()
    );
}

#[test]
fn managed_formats_parse_in_any_case() {
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
        ("PROTO", None, "not support"),
        ("XML", None, "Unknown format"),
        ("AVRO", Some("{}"), "no 'type'"),
    ];
    cases.iter().for_each(|(name, schema, expected)| {
        let error = Format::parse(name, *schema)
            .expect_err("an error")
            .to_string();
        assert!(error.contains(expected), "{name}: {error}");
    });
}

#[test]
fn payload_rows_hold_the_value_of_the_native_codec() {
    let raw = Payload::raw();
    let element = raw.decode(&record(Some(&[0, 255]))).unwrap();
    let RowElement::Row(row) = element else {
        panic!("a row")
    };
    assert_eq!(row.get_bytes("payload").unwrap(), Some(&[0, 255][..]));

    let text = Payload::string();
    let RowElement::Row(row) = text.decode(&record(Some("hé".as_bytes()))).unwrap() else {
        panic!("a row")
    };
    assert_eq!(row.get_string("payload").unwrap(), Some("hé"));
    assert!(text.decode(&record(Some(&[0xff]))).is_err());
}

#[test]
fn a_payload_write_lends_the_field_to_the_native_codec() {
    let mut raw = Payload::raw();
    let schema = Arc::new(Schema::new(vec![Field::nullable(
        "value",
        FieldType::bytes(),
    )]));
    bind(&mut raw, &schema).unwrap();
    let element = RowElement::Row(make_row(
        &schema,
        vec![Some(FieldValue::Bytes(b"v".to_vec()))],
    ));
    let mut buffer = Vec::new();
    let outgoing = raw.encode(&element, &mut buffer).unwrap();
    let RowElement::Row(row_ref) = &element else {
        unreachable!()
    };
    let field = row_ref.get_bytes("value").unwrap().unwrap();
    assert_eq!(
        outgoing.value.map(<[u8]>::as_ptr),
        Some(field.as_ptr()),
        "no copy"
    );
    assert!(buffer.is_empty());

    let null = RowElement::Row(make_row(&schema, vec![None]));
    let error = encode(&raw, &null).unwrap_err();
    assert!(error.message().contains("null"), "{error}");

    // A row from another SDK reaches the write still encoded.
    let encoded = RowElement::Encoded(row_ref.to_row_bytes().unwrap());
    assert_eq!(encode(&raw, &encoded).unwrap(), b"v");
}

#[test]
fn payload_writes_need_exactly_one_field_of_the_type_of_the_codec() {
    let text = Arc::new(Schema::new(vec![Field::new("value", FieldType::string())]));
    let error = bind(&mut Payload::raw(), &text).unwrap_err().to_string();
    assert!(error.contains("'value' is STRING"), "{error}");
    assert!(bind(&mut Payload::string(), &text).is_ok());
    let two = Arc::new(Schema::new(vec![
        Field::new("a", FieldType::bytes()),
        Field::new("b", FieldType::bytes()),
    ]));
    let error = bind(&mut Payload::raw(), &two).unwrap_err().to_string();
    assert!(error.contains("found 2 fields"), "{error}");
    assert!(bind(&mut Json::new(), &two).is_ok());
}

#[test]
fn json_reads_rows_of_the_json_schema_and_writes_objects() {
    let json = Json::schema(r#"{"properties": {"a": {"type": "integer"}}}"#).unwrap();
    let schema = json.row_schema().unwrap();
    assert_eq!(schema.fields[0].name, "a");
    let RowElement::Row(row) = json.decode(&record(Some(br#"{"a": 3}"#))).unwrap() else {
        panic!("a row")
    };
    assert_eq!(row.get_i64("a").unwrap(), Some(3));

    let schema = Arc::new(Schema::new(vec![Field::nullable(
        "value",
        FieldType::bytes(),
    )]));
    let mut writer = Json::new();
    bind(&mut writer, &schema).unwrap();
    let element = RowElement::Row(make_row(
        &schema,
        vec![Some(FieldValue::Bytes(b"v".to_vec()))],
    ));
    assert_eq!(encode(&writer, &element).unwrap(), br#"{"value":"dg=="}"#);
}

#[test]
fn beam_rows_pass_through_still_encoded() {
    let schema = Arc::new(Schema::new(vec![Field::new("n", FieldType::int64())]));
    let bytes = make_row(&schema, vec![Some(FieldValue::Int64(7))])
        .to_row_bytes()
        .unwrap();
    let read = BeamRow::schema(Arc::clone(&schema));
    assert_eq!(
        read.decode(&record(Some(&bytes))).unwrap(),
        RowElement::Encoded(bytes.clone())
    );

    let mut write = BeamRow::new();
    bind(&mut write, &schema).unwrap();
    let encoded = RowElement::Encoded(bytes.clone());
    let mut buffer = Vec::new();
    let outgoing = write.encode(&encoded, &mut buffer).unwrap();
    let RowElement::Encoded(inner) = &encoded else {
        unreachable!()
    };
    assert_eq!(
        outgoing.value.map(<[u8]>::as_ptr),
        Some(inner.as_ptr()),
        "no copy"
    );
    let decoded = RowElement::Row(make_row(&schema, vec![Some(FieldValue::Int64(7))]));
    assert_eq!(encode(&write, &decoded).unwrap(), bytes);
}

const AVRO_SCHEMA: &str = r#"{
  "type": "record", "name": "Event", "namespace": "test",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": ["null", "string"]},
    {"name": "tags", "type": {"type": "array", "items": "string"}},
    {"name": "color", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}},
    {"name": "day", "type": {"type": "int", "logicalType": "date"}}
  ]
}"#;

/// The datum of `{id: 1, name: "ab", tags: ["x"], color: GREEN, day: 3}`: zigzag varints,
/// the union branch, length-prefixed strings, one array block and its end, the enum index.
const AVRO_DATUM: [u8; 11] = [
    0x02, 0x02, 0x04, b'a', b'b', 0x02, 0x02, b'x', 0x00, 0x02, 0x06,
];

#[test]
fn avro_reads_a_datum_into_a_row_of_the_avro_schema() {
    let avro = Avro::schema(AVRO_SCHEMA).unwrap();
    let schema = avro.row_schema().unwrap();
    let names: Vec<String> = schema
        .fields
        .iter()
        .map(|f| format!("{}: {}", f.name, f.field_type))
        .collect();
    assert_eq!(names[0], "id: INT64");
    assert!(schema.fields[1].field_type.nullable);
    let RowElement::Row(row) = avro.decode(&record(Some(&AVRO_DATUM))).unwrap() else {
        panic!("a row")
    };
    assert_eq!(row.get_i64("id").unwrap(), Some(1));
    assert_eq!(row.get_string("name").unwrap(), Some("ab"));
    assert_eq!(row.get_string("color").unwrap(), Some("GREEN"));
    assert_eq!(
        row.values()[2],
        Some(FieldValue::Array(vec![Some(FieldValue::String(
            "x".into()
        ))]))
    );
    assert_eq!(row.values()[4], Some(FieldValue::Int64(3)));

    let mut trailing = AVRO_DATUM.to_vec();
    trailing.push(0);
    let error = avro.decode(&record(Some(&trailing))).unwrap_err();
    assert!(error.message().contains("after the Avro datum"), "{error}");
    let error = avro.decode(&record(Some(&AVRO_DATUM[..4]))).unwrap_err();
    assert!(error.message().contains("ends too early"), "{error}");
}

#[test]
fn avro_writes_the_datum_that_it_reads() {
    let mut avro = Avro::schema(AVRO_SCHEMA).unwrap();
    let schema = avro.row_schema().unwrap();
    let RowElement::Row(row) = avro.decode(&record(Some(&AVRO_DATUM))).unwrap() else {
        panic!("a row")
    };
    bind(&mut avro, &schema).unwrap();
    assert_eq!(
        encode(&avro, &RowElement::Row(row.clone())).unwrap(),
        AVRO_DATUM
    );
    let mut null_name = row.values().to_vec();
    null_name[1] = None;
    let null_row = make_row(&schema, null_name);
    let datum = encode(&avro, &RowElement::Row(null_row)).unwrap();
    assert_eq!(&datum[..2], &[0x02, 0x00], "the null branch of the union");
}

#[test]
fn avro_writes_rows_of_any_schema_with_the_avro_schema_of_their_beam_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::nullable("name", FieldType::string()),
    ]));
    let mut avro = Avro::new();
    bind(&mut avro, &schema).unwrap();
    let values = vec![
        Some(FieldValue::Int64(-1)),
        Some(FieldValue::String("ab".into())),
    ];
    let element = RowElement::Row(make_row(&schema, values));
    assert_eq!(
        encode(&avro, &element).unwrap(),
        [0x01, 0x02, 0x04, b'a', b'b']
    );

    // A given Avro schema must have the fields of the rows.
    let mut other = Avro::schema(AVRO_SCHEMA).unwrap();
    let error = bind(&mut other, &schema).unwrap_err().to_string();
    assert!(error.contains("fields"), "{error}");
}

#[test]
fn avro_in_the_confluent_wire_format_has_a_header() {
    let mut framed = vec![0, 0, 0, 0, 7];
    framed.extend_from_slice(&AVRO_DATUM);
    let avro = Avro::schema(AVRO_SCHEMA)
        .unwrap()
        .with_framing(Framing::Confluent { schema_id: Some(7) });
    assert!(avro.decode(&record(Some(&framed))).is_ok());
    let other_id = Avro::schema(AVRO_SCHEMA)
        .unwrap()
        .with_framing(Framing::Confluent { schema_id: Some(8) });
    let error = other_id.decode(&record(Some(&framed))).unwrap_err();
    assert!(error.message().contains("schema ID 7"), "{error}");
    let error = avro.decode(&record(Some(&AVRO_DATUM))).unwrap_err();
    assert!(error.message().contains("Confluent"), "{error}");

    let mut writer = avro.clone();
    let RowElement::Row(row) = avro.decode(&record(Some(&framed))).unwrap() else {
        panic!("a row")
    };
    bind(&mut writer, &avro.row_schema().unwrap()).unwrap();
    assert_eq!(encode(&writer, &RowElement::Row(row)).unwrap(), framed);
}

#[test]
fn records_with_metadata_have_the_key_the_headers_and_the_timestamp() {
    let headers = [("trace", Some(&b"t1"[..])), ("empty", None)];
    let source = RecordRef {
        key: Some(b"k1"),
        headers: &headers,
        ..record(Some(b"v1"))
    };
    let codec = WithMetadata::new(Raw).with_key(Utf8);
    let decoded = codec.decode(&source).unwrap();
    assert_eq!(decoded.key.as_deref(), Some("k1"));
    assert_eq!(decoded.value.as_deref(), Some(&b"v1"[..]));
    assert_eq!((decoded.partition, decoded.offset), (3, 42));
    assert_eq!(decoded.timestamp, Some(Instant::from_millis(1_000)));
    assert_eq!(
        decoded.headers,
        vec![
            ("trace".to_string(), Some(b"t1".to_vec())),
            ("empty".to_string(), None)
        ]
    );
    let tombstone = codec.decode(&record(None)).unwrap();
    assert_eq!((tombstone.key, tombstone.value), (None, None));

    let mut buffer = Vec::new();
    let outgoing = codec.encode(&decoded, &mut buffer).unwrap();
    assert_eq!(outgoing.key.as_deref(), Some(&b"k1"[..]));
    assert_eq!(outgoing.value, Some(&b"v1"[..]));
    assert_eq!(
        outgoing.headers,
        vec![("trace", Some(&b"t1"[..])), ("empty", None)]
    );
    assert_eq!(outgoing.timestamp, Some(Instant::from_millis(1_000)));
}

#[test]
fn a_kafka_record_round_trips_through_its_coder() {
    let record = KafkaRecord::of_value(b"v".to_vec())
        .with_key("k".to_string())
        .with_header("h", None)
        .with_timestamp(Instant::from_millis(5));
    let bytes = record.encode().unwrap();
    assert_eq!(
        KafkaRecord::<String, Vec<u8>>::decode(&bytes).unwrap(),
        record
    );
}

#[test]
fn the_failure_rows_hold_the_input_and_the_reason() {
    let names = |schema: Schema| -> Vec<String> {
        schema
            .fields
            .iter()
            .map(|f| format!("{}: {}", f.name, f.field_type))
            .collect()
    };
    assert_eq!(
        names(read_error_schema()),
        ["failed_row: BYTES", "error_message: STRING"]
    );
    assert_eq!(
        names(write_error_schema(FieldType::string())),
        ["failed_row: STRING", "error_message: STRING"]
    );
}
