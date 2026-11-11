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

//! Record-level Avro datum mapping to and from Beam [`Row`]s.

use std::sync::Arc;

use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema, TypeInfo};
use beam_io_avro::datum::{
    DatumDecoder, DatumEncoder, Frame, PLAIN_ID, WireFormat, avro_schema, beam_schema,
};

#[test]
fn logical_types_map_to_beam_logical_types() {
    let schema = r#"{"type": "record", "name": "Times", "fields": [
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "millis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "micros", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "counts", "type": {"type": "map", "values": "long"}},
        {"name": "hash", "type": {"type": "fixed", "name": "Hash", "size": 2}}
    ]}"#;
    let row_schema = beam_schema(schema).unwrap();
    let types: Vec<FieldType> = row_schema
        .fields
        .iter()
        .map(|f| f.field_type.clone())
        .collect();
    assert_eq!(
        types,
        [
            beam_io_arrow::date_type(),
            beam_io_arrow::millis_instant_type(),
            beam_io_arrow::micros_instant_type(),
            FieldType::map(FieldType::string(), FieldType::int64()),
            FieldType::bytes(),
        ]
    );
}

#[test]
fn nested_enums_in_records_arrays_and_maps_round_trip() {
    let raw_schema: Arc<str> = r#"{
      "type": "record", "name": "Outer", "namespace": "test",
      "fields": [
        {"name": "inner", "type": {
          "type": "record", "name": "Inner",
          "fields": [
            {"name": "color", "type": {
              "type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]
            }}
          ]
        }},
        {"name": "palette", "type": {"type": "array", "items": "Color"}},
        {"name": "named", "type": {"type": "map", "values": "Color"}}
      ]
    }"#
    .into();
    let row_schema = beam_schema(&raw_schema).unwrap();
    let TypeInfo::Row(inner_schema) = &row_schema.fields[0].field_type.type_info else {
        panic!("nested row schema")
    };
    let inner = Row::new(
        Arc::clone(inner_schema),
        vec![Some(FieldValue::String("BLUE".into()))],
    )
    .unwrap();
    let row = Row::new(
        Arc::clone(&row_schema),
        vec![
            Some(FieldValue::Row(inner)),
            Some(FieldValue::Array(vec![
                Some(FieldValue::String("RED".into())),
                Some(FieldValue::String("BLUE".into())),
            ])),
            Some(FieldValue::Map(vec![(
                FieldValue::String("primary".into()),
                Some(FieldValue::String("GREEN".into())),
            )])),
        ],
    )
    .unwrap();

    let mut encoder = DatumEncoder::default();
    let mut bytes = Vec::new();
    encoder
        .encode_row(
            &raw_schema,
            WireFormat::Plain,
            &row_schema,
            &RowElement::Row(row.clone()),
            &mut bytes,
        )
        .unwrap();

    let frame = Frame {
        schema_id: PLAIN_ID,
        datum: &bytes,
    };
    let mut decoder = DatumDecoder::default();
    let mut rows = decoder.decode_frames(&raw_schema, &row_schema, &[frame], |_| {
        Ok(Arc::clone(&raw_schema))
    });
    assert_eq!(rows.pop().unwrap().unwrap(), row);
}

#[test]
fn beam_schema_derives_round_trippable_avro_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::nullable("name", FieldType::string()),
    ]));
    let derived: Arc<str> = avro_schema(&schema).unwrap().into();
    assert_eq!(beam_schema(&derived).unwrap().as_ref(), schema.as_ref());
}

const PERSON: &str = r#"{"type": "record", "name": "Person", "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"}
]}"#;

fn person(row_schema: &Arc<Schema>, id: i64, name: &str) -> Row {
    Row::new(
        Arc::clone(row_schema),
        vec![
            Some(FieldValue::Int64(id)),
            Some(FieldValue::String(name.into())),
        ],
    )
    .unwrap()
}

fn encode_plain(encoder: &mut DatumEncoder, row_schema: &Schema, row: &RowElement) -> Vec<u8> {
    let schema: Arc<str> = PERSON.into();
    let mut bytes = Vec::new();
    encoder
        .encode_row(&schema, WireFormat::Plain, row_schema, row, &mut bytes)
        .unwrap();
    bytes
}

#[test]
fn an_encoder_state_writes_a_good_row_after_a_row_that_fails_partway() {
    let row_schema = beam_schema(PERSON).unwrap();
    let good = RowElement::Row(person(&row_schema, 7, "ok"));
    let fresh = encode_plain(&mut DatumEncoder::default(), &row_schema, &good);
    // The bytes end inside `name`, so the builder holds the `id` of the bad row.
    let bytes = person(&row_schema, 1, "cut").to_row_bytes().unwrap();
    let cut = RowElement::Encoded(bytes[..bytes.len() - 1].to_vec());
    let schema: Arc<str> = PERSON.into();
    let mut reused = DatumEncoder::default();
    encode_plain(&mut reused, &row_schema, &good);
    let mut out = Vec::new();
    assert!(
        reused
            .encode_row(&schema, WireFormat::Plain, &row_schema, &cut, &mut out)
            .is_err()
    );
    assert_eq!(encode_plain(&mut reused, &row_schema, &good), fresh);
}

#[test]
fn one_encoder_state_writes_each_row_as_a_new_encoder_does() {
    let row_schema = beam_schema(PERSON).unwrap();
    let mut reused = DatumEncoder::default();
    for id in 0..100 {
        let row = person(&row_schema, id, &format!("name-{id}"));
        let encoded = RowElement::Encoded(row.to_row_bytes().unwrap());
        let row = RowElement::Row(row);
        let fresh = encode_plain(&mut DatumEncoder::default(), &row_schema, &row);
        assert_eq!(encode_plain(&mut reused, &row_schema, &row), fresh);
        assert_eq!(encode_plain(&mut reused, &row_schema, &encoded), fresh);
    }
}

#[test]
fn a_bad_frame_fails_alone_and_each_writer_schema_is_asked_once() {
    let schema: Arc<str> = PERSON.into();
    let row_schema = beam_schema(PERSON).unwrap();
    let rows: Vec<Row> = (0..4).map(|id| person(&row_schema, id, "x")).collect();
    let mut encoder = DatumEncoder::default();
    let datums: Vec<Vec<u8>> = rows
        .iter()
        .map(|row| encode_plain(&mut encoder, &row_schema, &RowElement::Row(row.clone())))
        .collect();
    let mut trailing = datums[2].clone();
    trailing.push(7);
    // IDs 1 and 2 have the same writer schema, so the decoder changes schema in the batch.
    let frame = |schema_id, datum| Frame { schema_id, datum };
    let frames = [
        frame(1, datums[0].as_slice()),
        frame(1, &datums[1][..1]),
        frame(2, datums[1].as_slice()),
        frame(1, trailing.as_slice()),
        frame(9, datums[2].as_slice()),
        frame(2, datums[3].as_slice()),
    ];
    let mut asked = Vec::new();
    let mut decoder = DatumDecoder::default();
    let mut writer = |id| {
        asked.push(id);
        match id {
            9 => Err("no schema 9".to_string()),
            _ => Ok(Arc::clone(&schema)),
        }
    };
    let decoded = decoder.decode_frames(&schema, &row_schema, &frames, &mut writer);
    let decoded: Vec<Option<Row>> = decoded.into_iter().map(Result::ok).collect();
    assert_eq!(
        decoded,
        [
            Some(rows[0].clone()),
            None,
            Some(rows[1].clone()),
            None,
            None,
            Some(rows[3].clone()),
        ]
    );
    let again = decoder.decode_frames(&schema, &row_schema, &frames[..1], &mut writer);
    assert_eq!(again.into_iter().next().unwrap().unwrap(), rows[0]);
    assert_eq!(asked, [1, 2, 9]);
}
