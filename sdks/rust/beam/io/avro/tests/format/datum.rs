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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::{
    AVRO_ENUM_SYMBOLS_METADATA_KEY, AvroSchema, Fingerprint, FingerprintAlgorithm,
    FingerprintStrategy, SCHEMA_METADATA_KEY, SchemaStore,
};
use arrow_avro::writer::WriterBuilder;
use arrow_avro::writer::format::{AvroBinaryFormat, AvroSoeFormat};
use arrow_schema::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
use beam::Instant;
use beam::schema::{
    BeamRow, Field, FieldType, FieldValue, Row, RowElement, Schema, TypeInfo, URN_MICROS_INSTANT,
};
use beam_io_arrow::RowBatchBuilder;
use beam_io_arrow::arrow_array::cast::AsArray;
use beam_io_arrow::arrow_array::types::Int32Type;
use beam_io_arrow::arrow_array::{
    Array, ArrayRef, DictionaryArray, Int32Array, ListArray, MapArray, RecordBatch, StringArray,
    StructArray,
};
use beam_io_avro::datum::{
    CONFLUENT_MAGIC, DatumDecoder, DatumEncoder, Frame, PLAIN_ID, WireFormat, avro_schema,
    beam_schema,
};
use chrono::{DateTime, NaiveDate, Utc};

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
        .encode_row_element(
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
        .encode_row_element(&schema, WireFormat::Plain, row_schema, row, &mut bytes)
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
            .encode_row_element(&schema, WireFormat::Plain, &row_schema, &cut, &mut out)
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

#[test]
fn a_row_that_does_not_convert_fails_alone() {
    let schema: Arc<str> = r#"{"type": "record", "name": "Note", "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": ["null", "string"]}
    ]}"#
    .into();
    // `name` is not nullable in the row schema, so a datum with a null `name` does not convert.
    let row_schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("name", FieldType::string()),
    ]));
    // Each datum is the `id`, the branch of the union, and the string of branch 1.
    let datums: [&[u8]; 3] = [&[2, 2, 2, b'a'], &[4, 0], &[6, 2, 2, b'c']];
    let frames = datums.map(|datum| Frame {
        schema_id: PLAIN_ID,
        datum,
    });
    let mut decoder = DatumDecoder::default();
    let decoded = decoder.decode_frames(&schema, &row_schema, &frames, |_| Ok(Arc::clone(&schema)));
    let error = decoded[1].as_ref().unwrap_err();
    assert!(error.contains("'name'"), "{error}");
    let decoded: Vec<Option<Row>> = decoded.into_iter().map(Result::ok).collect();
    assert_eq!(
        decoded,
        [
            Some(person(&row_schema, 1, "a")),
            None,
            Some(person(&row_schema, 3, "c")),
        ]
    );
}

/// Encodes one `Row` directly through `arrow-avro`'s `BatchEncoder` for byte-for-byte parity.
fn arrow_avro_encode(
    avro_schema: &str,
    format: WireFormat,
    row_schema: &Schema,
    row: &Row,
) -> Vec<u8> {
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    store
        .set(
            Fingerprint::Id(PLAIN_ID),
            AvroSchema::new(avro_schema.to_string()),
        )
        .unwrap();
    let target = ReaderBuilder::new()
        .with_reader_schema(AvroSchema::new(avro_schema.to_string()))
        .with_writer_schema_store(store)
        .build_decoder()
        .unwrap()
        .schema();
    let metadata = HashMap::from([(SCHEMA_METADATA_KEY.to_string(), avro_schema.to_string())]);
    let builder = WriterBuilder::new(ArrowSchema::new_with_metadata(
        target.fields().clone(),
        metadata,
    ));
    let mut encoder = match format {
        WireFormat::Plain => builder.build_encoder::<AvroBinaryFormat>(),
        WireFormat::Confluent(id) => builder
            .with_fingerprint_strategy(FingerprintStrategy::Id(id))
            .build_encoder::<AvroSoeFormat>(),
    }
    .unwrap();
    let mut rows = RowBatchBuilder::new(row_schema).unwrap();
    rows.append_row(row).unwrap();
    let batch = rows.finish().unwrap();
    let target_schema = encoder.schema();
    let columns = ref_cast_columns(batch.columns(), target_schema.fields());
    let conformed = RecordBatch::try_new(target_schema, columns).unwrap();
    encoder.encode(&conformed).unwrap();
    encoder.flush().bytes().to_vec()
}

fn ref_cast_columns(columns: &[ArrayRef], fields: &Fields) -> Vec<ArrayRef> {
    columns
        .iter()
        .zip(fields)
        .map(|(column, field)| ref_cast_column(column, field))
        .collect()
}

fn ref_cast_column(column: &ArrayRef, field: &ArrowField) -> ArrayRef {
    if column.data_type() == field.data_type() {
        return Arc::clone(column);
    }
    if let Some(raw_symbols) = field.metadata().get(AVRO_ENUM_SYMBOLS_METADATA_KEY) {
        let symbols: Vec<String> = serde_json::from_str(raw_symbols).unwrap();
        let strings = arrow_cast::cast(column, &DataType::Utf8).unwrap();
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        let keys: Int32Array = strings
            .iter()
            .map(|val| {
                val.map(|s| {
                    let pos = symbols.iter().position(|sym| sym == s).unwrap();
                    i32::try_from(pos).unwrap()
                })
            })
            .collect();
        let values: ArrayRef = Arc::new(StringArray::from(symbols));
        return Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            if let Some(array) = column.as_struct_opt() {
                let children = ref_cast_columns(array.columns(), fields);
                let struct_array =
                    StructArray::try_new(fields.clone(), children, array.nulls().cloned()).unwrap();
                Arc::new(struct_array)
            } else {
                arrow_cast::cast(column, field.data_type()).unwrap()
            }
        }
        DataType::List(item) => {
            if let Some(array) = column.as_list_opt::<i32>() {
                Arc::new(
                    ListArray::try_new(
                        Arc::clone(item),
                        array.offsets().clone(),
                        ref_cast_column(array.values(), item),
                        array.nulls().cloned(),
                    )
                    .unwrap(),
                )
            } else {
                arrow_cast::cast(column, field.data_type()).unwrap()
            }
        }
        DataType::Map(entries, sorted) => {
            if let Some(array) = column.as_map_opt() {
                let DataType::Struct(fields) = entries.data_type() else {
                    panic!("map entries struct");
                };
                let children = ref_cast_columns(array.entries().columns(), fields);
                let pairs = StructArray::try_new(
                    fields.clone(),
                    children,
                    array.entries().nulls().cloned(),
                )
                .unwrap();
                Arc::new(
                    MapArray::try_new(
                        Arc::clone(entries),
                        array.offsets().clone(),
                        pairs,
                        array.nulls().cloned(),
                        *sorted,
                    )
                    .unwrap(),
                )
            } else {
                arrow_cast::cast(column, field.data_type()).unwrap()
            }
        }
        other => arrow_cast::cast(column, other).unwrap(),
    }
}

fn decode_single(
    avro_schema: &Arc<str>,
    row_schema: &Arc<Schema>,
    format: WireFormat,
    bytes: &[u8],
) -> Row {
    let frame = match format {
        WireFormat::Plain => Frame {
            schema_id: PLAIN_ID,
            datum: bytes,
        },
        WireFormat::Confluent(id) => {
            let [a, b, c, d] = id.to_be_bytes();
            assert_eq!(&bytes[..5], &[CONFLUENT_MAGIC, a, b, c, d]);
            Frame {
                schema_id: id,
                datum: &bytes[5..],
            }
        }
    };
    let mut decoder = DatumDecoder::default();
    decoder
        .decode_frames(avro_schema, row_schema, &[frame], |_| {
            Ok(Arc::clone(avro_schema))
        })
        .pop()
        .unwrap()
        .unwrap()
}

fn assert_matches_arrow_avro(
    encoder: &mut DatumEncoder,
    buffer: &mut Vec<u8>,
    avro_schema: &Arc<str>,
    format: WireFormat,
    row_schema: &Schema,
    row: &Row,
) -> Vec<u8> {
    let expected = arrow_avro_encode(avro_schema, format, row_schema, row);

    encoder
        .encode_row(avro_schema, format, row_schema, row, buffer)
        .unwrap();
    assert_eq!(buffer.as_slice(), expected.as_slice(), "encode_row");

    let as_row = RowElement::Row(row.clone());
    encoder
        .encode_row_element(avro_schema, format, row_schema, &as_row, buffer)
        .unwrap();
    assert_eq!(
        buffer.as_slice(),
        expected.as_slice(),
        "encode_row_element(Row)"
    );

    let as_encoded = RowElement::Encoded(row.to_row_bytes().unwrap());
    encoder
        .encode_row_element(avro_schema, format, row_schema, &as_encoded, buffer)
        .unwrap();
    assert_eq!(
        buffer.as_slice(),
        expected.as_slice(),
        "encode_row_element(Encoded)"
    );

    expected
}

fn micros_instant_value(seconds: i64, micros: i64) -> FieldValue {
    let schema = Arc::new(Schema::new(vec![
        Field::new("seconds", FieldType::int64()),
        Field::new("micros", FieldType::int64()),
    ]));
    FieldValue::Row(
        Row::new(
            schema,
            vec![
                Some(FieldValue::Int64(seconds)),
                Some(FieldValue::Int64(micros)),
            ],
        )
        .unwrap(),
    )
}

#[test]
fn direct_encoder_matches_arrow_avro_for_all_primitives_and_logical_types() {
    let avro: Arc<str> = r#"{
      "type": "record", "name": "AllPrimitives",
      "fields": [
        {"name": "b", "type": "boolean"},
        {"name": "i8", "type": "int"},
        {"name": "i16", "type": "int"},
        {"name": "i32", "type": "int"},
        {"name": "i64", "type": "long"},
        {"name": "f32", "type": "float"},
        {"name": "f64", "type": "double"},
        {"name": "s", "type": "string"},
        {"name": "bin", "type": "bytes"},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "ms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "us", "type": {"type": "long", "logicalType": "timestamp-micros"}}
      ]
    }"#
    .into();
    let row_schema = Arc::new(Schema::new(vec![
        Field::new("b", FieldType::boolean()),
        Field::new("i8", FieldType::byte()),
        Field::new("i16", FieldType::int16()),
        Field::new("i32", FieldType::int32()),
        Field::new("i64", FieldType::int64()),
        Field::new("f32", FieldType::float()),
        Field::new("f64", FieldType::double()),
        Field::new("s", FieldType::string()),
        Field::new("bin", FieldType::bytes()),
        Field::new("day", beam_io_arrow::date_type()),
        Field::new("ms", beam_io_arrow::millis_instant_type()),
        Field::new("us", beam_io_arrow::micros_instant_type()),
    ]));

    let samples = [
        Row::new(
            Arc::clone(&row_schema),
            vec![
                Some(FieldValue::Boolean(false)),
                Some(FieldValue::Byte(i8::MIN)),
                Some(FieldValue::Int16(i16::MIN)),
                Some(FieldValue::Int32(i32::MIN)),
                Some(FieldValue::Int64(i64::MIN)),
                Some(FieldValue::Float(-123.25)),
                Some(FieldValue::Double(-0.25)),
                Some(FieldValue::String(String::new())),
                Some(FieldValue::Bytes(Vec::new())),
                Some(FieldValue::Int64(-1)),
                Some(FieldValue::Int64(-1)),
                Some(micros_instant_value(-1, 999_999)),
            ],
        )
        .unwrap(),
        Row::new(
            Arc::clone(&row_schema),
            vec![
                Some(FieldValue::Boolean(true)),
                Some(FieldValue::Byte(0)),
                Some(FieldValue::Int16(0)),
                Some(FieldValue::Int32(-1)),
                Some(FieldValue::Int64(64)),
                Some(FieldValue::Float(0.0)),
                Some(FieldValue::Double(12.5)),
                Some(FieldValue::String("héllo 🚀".into())),
                Some(FieldValue::Bytes(vec![0x00, 0x7f, 0xff])),
                Some(FieldValue::Int64(0)),
                Some(FieldValue::Int64(0)),
                Some(micros_instant_value(0, 0)),
            ],
        )
        .unwrap(),
        Row::new(
            Arc::clone(&row_schema),
            vec![
                Some(FieldValue::Boolean(true)),
                Some(FieldValue::Byte(i8::MAX)),
                Some(FieldValue::Int16(i16::MAX)),
                Some(FieldValue::Int32(i32::MAX)),
                Some(FieldValue::Int64(i64::MAX)),
                Some(FieldValue::Float(1.5)),
                Some(FieldValue::Double(f64::MAX)),
                Some(FieldValue::String("ascii-text".into())),
                Some(FieldValue::Bytes(b"payload".to_vec())),
                Some(FieldValue::Int64(19_723)),
                Some(FieldValue::Int64(1_700_000_000_123)),
                Some(micros_instant_value(1_700_000_000, 123_456)),
            ],
        )
        .unwrap(),
    ];

    for format in [WireFormat::Plain, WireFormat::Confluent(42)] {
        let mut encoder = DatumEncoder::default();
        let mut buffer = Vec::new();
        for row in &samples {
            let bytes = assert_matches_arrow_avro(
                &mut encoder,
                &mut buffer,
                &avro,
                format,
                &row_schema,
                row,
            );
            assert_eq!(decode_single(&avro, &row_schema, format, &bytes), *row);
        }
    }

    // Widening casts: BYTE/INT16/INT32 -> Avro long, FLOAT -> Avro double.
    let widened_avro: Arc<str> = r#"{
      "type": "record", "name": "Widened",
      "fields": [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "long"},
        {"name": "c", "type": "long"},
        {"name": "d", "type": "double"}
      ]
    }"#
    .into();
    let widened_schema = Arc::new(Schema::new(vec![
        Field::new("a", FieldType::byte()),
        Field::new("b", FieldType::int16()),
        Field::new("c", FieldType::int32()),
        Field::new("d", FieldType::float()),
    ]));
    let widened_row = Row::new(
        Arc::clone(&widened_schema),
        vec![
            Some(FieldValue::Byte(-7)),
            Some(FieldValue::Int16(-300)),
            Some(FieldValue::Int32(-100_000)),
            Some(FieldValue::Float(3.5)),
        ],
    )
    .unwrap();
    let mut encoder = DatumEncoder::default();
    let mut buffer = Vec::new();
    let bytes = assert_matches_arrow_avro(
        &mut encoder,
        &mut buffer,
        &widened_avro,
        WireFormat::Plain,
        &widened_schema,
        &widened_row,
    );
    let read_schema = beam_schema(&widened_avro).unwrap();
    let decoded = decode_single(&widened_avro, &read_schema, WireFormat::Plain, &bytes);
    assert_eq!(decoded.get_i64("a").unwrap(), Some(-7));
    assert_eq!(decoded.get_i64("b").unwrap(), Some(-300));
    assert_eq!(decoded.get_i64("c").unwrap(), Some(-100_000));
    assert_eq!(decoded.get_f64("d").unwrap(), Some(3.5));
}

#[test]
fn direct_encoder_matches_arrow_avro_for_both_union_orders_and_nested_containers() {
    let avro: Arc<str> = r#"{
      "type": "record", "name": "Nested",
      "fields": [
        {"name": "null_first", "type": ["null", "string"]},
        {"name": "null_second", "type": ["string", "null"]},
        {"name": "opt_struct", "type": ["null", {
          "type": "record", "name": "Child",
          "fields": [
            {"name": "k", "type": "int"},
            {"name": "v", "type": ["long", "null"]}
          ]
        }]},
        {"name": "rev_struct", "type": ["Child", "null"]},
        {"name": "list_nf", "type": {"type": "array", "items": ["null", "string"]}},
        {"name": "list_ns", "type": {"type": "array", "items": ["int", "null"]}},
        {"name": "map_nf", "type": {"type": "map", "values": ["null", "long"]}},
        {"name": "map_ns", "type": {"type": "map", "values": ["string", "null"]}},
        {"name": "tail_null", "type": ["null", "boolean"]},
        {"name": "tail_rev", "type": ["boolean", "null"]}
      ]
    }"#
    .into();
    let row_schema = beam_schema(&avro).unwrap();
    let TypeInfo::Row(child_schema) = &row_schema.fields[2].field_type.type_info else {
        panic!("child schema");
    };
    let child_present = Row::new(
        Arc::clone(child_schema),
        vec![Some(FieldValue::Int32(9)), Some(FieldValue::Int64(-5))],
    )
    .unwrap();
    let child_null_v = Row::new(
        Arc::clone(child_schema),
        vec![Some(FieldValue::Int32(0)), None],
    )
    .unwrap();

    // Exercises multi-byte null bitmasks (10 fields), empty containers, and both union orders.
    let rows = [
        Row::new(
            Arc::clone(&row_schema),
            vec![
                None,
                Some(FieldValue::String("second".into())),
                None,
                Some(FieldValue::Row(child_present)),
                Some(FieldValue::Array(Vec::new())),
                Some(FieldValue::Array(vec![
                    Some(FieldValue::Int32(1)),
                    None,
                    Some(FieldValue::Int32(-2)),
                ])),
                Some(FieldValue::Map(Vec::new())),
                Some(FieldValue::Map(vec![
                    (
                        FieldValue::String("a".into()),
                        Some(FieldValue::String("x".into())),
                    ),
                    (FieldValue::String("b".into()), None),
                ])),
                None,
                Some(FieldValue::Boolean(true)),
            ],
        )
        .unwrap(),
        Row::new(
            Arc::clone(&row_schema),
            vec![
                Some(FieldValue::String("first".into())),
                None,
                Some(FieldValue::Row(child_null_v)),
                None,
                Some(FieldValue::Array(vec![
                    None,
                    Some(FieldValue::String("hi".into())),
                ])),
                Some(FieldValue::Array(Vec::new())),
                Some(FieldValue::Map(vec![
                    (FieldValue::String("k1".into()), None),
                    (FieldValue::String("k2".into()), Some(FieldValue::Int64(77))),
                ])),
                Some(FieldValue::Map(Vec::new())),
                Some(FieldValue::Boolean(false)),
                None,
            ],
        )
        .unwrap(),
    ];

    for format in [WireFormat::Plain, WireFormat::Confluent(99)] {
        let mut encoder = DatumEncoder::default();
        let mut buffer = Vec::new();
        for row in &rows {
            let bytes = assert_matches_arrow_avro(
                &mut encoder,
                &mut buffer,
                &avro,
                format,
                &row_schema,
                row,
            );
            assert_eq!(decode_single(&avro, &row_schema, format, &bytes), *row);
        }
    }
}

#[test]
fn direct_encoder_handles_nullability_mismatches_between_beam_and_avro() {
    // Non-nullable Beam fields targeting nullable Avro unions (both branch orders).
    let avro_nullable: Arc<str> = r#"{
      "type": "record", "name": "Unions",
      "fields": [
        {"name": "a", "type": ["null", "string"]},
        {"name": "b", "type": ["string", "null"]},
        {"name": "items", "type": {"type": "array", "items": ["string", "null"]}}
      ]
    }"#
    .into();
    let beam_required = Arc::new(Schema::new(vec![
        Field::new("a", FieldType::string()),
        Field::new("b", FieldType::string()),
        Field::new("items", FieldType::array(FieldType::string())),
    ]));
    let row = Row::new(
        Arc::clone(&beam_required),
        vec![
            Some(FieldValue::String("x".into())),
            Some(FieldValue::String("y".into())),
            Some(FieldValue::Array(vec![Some(FieldValue::String(
                "z".into(),
            ))])),
        ],
    )
    .unwrap();
    let mut encoder = DatumEncoder::default();
    let mut buffer = Vec::new();
    let bytes = assert_matches_arrow_avro(
        &mut encoder,
        &mut buffer,
        &avro_nullable,
        WireFormat::Plain,
        &beam_required,
        &row,
    );
    let read_schema = beam_schema(&avro_nullable).unwrap();
    let decoded = decode_single(&avro_nullable, &read_schema, WireFormat::Plain, &bytes);
    assert_eq!(decoded.values(), row.values());

    // Nullable Beam field targeting non-nullable Avro field: non-null encodes, null fails.
    let avro_required: Arc<str> = r#"{
      "type": "record", "name": "Req",
      "fields": [{"name": "a", "type": "string"}]
    }"#
    .into();
    let beam_nullable = Arc::new(Schema::new(vec![Field::nullable("a", FieldType::string())]));
    let present = Row::new(
        Arc::clone(&beam_nullable),
        vec![Some(FieldValue::String("ok".into()))],
    )
    .unwrap();
    let null_row = Row::new(Arc::clone(&beam_nullable), vec![None]).unwrap();
    let mut encoder = DatumEncoder::default();
    let mut buffer = Vec::new();
    assert_matches_arrow_avro(
        &mut encoder,
        &mut buffer,
        &avro_required,
        WireFormat::Plain,
        &beam_nullable,
        &present,
    );
    assert!(
        encoder
            .encode_row(
                &avro_required,
                WireFormat::Plain,
                &beam_nullable,
                &null_row,
                &mut buffer
            )
            .is_err()
    );
    assert!(buffer.is_empty());
    let null_encoded = RowElement::Encoded(null_row.to_row_bytes().unwrap());
    assert!(
        encoder
            .encode_row_element(
                &avro_required,
                WireFormat::Plain,
                &beam_nullable,
                &null_encoded,
                &mut buffer,
            )
            .is_err()
    );
    assert!(buffer.is_empty());
}

#[test]
fn direct_encoder_matches_arrow_avro_for_enums_and_named_types() {
    let avro: Arc<str> = r#"{
      "type": "record", "name": "EnumHost", "namespace": "org.beam.test",
      "fields": [
        {"name": "primary", "type": {
          "type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]
        }},
        {"name": "opt_nf", "type": ["null", "Color"]},
        {"name": "opt_ns", "type": ["Color", "null"]},
        {"name": "nested", "type": {
          "type": "record", "name": "Swatch",
          "fields": [{"name": "c", "type": "Color"}]
        }},
        {"name": "list", "type": {"type": "array", "items": "org.beam.test.Color"}},
        {"name": "map", "type": {"type": "map", "values": ["null", "Color"]}}
      ]
    }"#
    .into();
    let row_schema = beam_schema(&avro).unwrap();
    let TypeInfo::Row(swatch_schema) = &row_schema.fields[3].field_type.type_info else {
        panic!("swatch schema");
    };
    let swatch = Row::new(
        Arc::clone(swatch_schema),
        vec![Some(FieldValue::String("BLUE".into()))],
    )
    .unwrap();
    let good = Row::new(
        Arc::clone(&row_schema),
        vec![
            Some(FieldValue::String("RED".into())),
            None,
            Some(FieldValue::String("GREEN".into())),
            Some(FieldValue::Row(swatch)),
            Some(FieldValue::Array(vec![
                Some(FieldValue::String("BLUE".into())),
                Some(FieldValue::String("RED".into())),
            ])),
            Some(FieldValue::Map(vec![
                (
                    FieldValue::String("a".into()),
                    Some(FieldValue::String("GREEN".into())),
                ),
                (FieldValue::String("b".into()), None),
            ])),
        ],
    )
    .unwrap();

    let mut encoder = DatumEncoder::default();
    let mut buffer = Vec::new();
    let bytes = assert_matches_arrow_avro(
        &mut encoder,
        &mut buffer,
        &avro,
        WireFormat::Confluent(7),
        &row_schema,
        &good,
    );
    assert_eq!(
        decode_single(&avro, &row_schema, WireFormat::Confluent(7), &bytes),
        good
    );

    // Invalid enum symbol fails cleanly for both Row and Encoded, and state recovers.
    let mut bad_vals = good.values().to_vec();
    bad_vals[0] = Some(FieldValue::String("MAGENTA".into()));
    let bad = Row::new(Arc::clone(&row_schema), bad_vals).unwrap();
    buffer.extend_from_slice(&[0xaa; 8]);
    let err = encoder
        .encode_row(
            &avro,
            WireFormat::Confluent(7),
            &row_schema,
            &bad,
            &mut buffer,
        )
        .unwrap_err();
    assert!(err.contains("MAGENTA"), "{err}");
    assert!(buffer.is_empty());

    let bad_encoded = RowElement::Encoded(bad.to_row_bytes().unwrap());
    buffer.extend_from_slice(&[0xbb; 8]);
    let err = encoder
        .encode_row_element(
            &avro,
            WireFormat::Confluent(7),
            &row_schema,
            &bad_encoded,
            &mut buffer,
        )
        .unwrap_err();
    assert!(err.contains("MAGENTA"), "{err}");
    assert!(buffer.is_empty());

    assert_matches_arrow_avro(
        &mut encoder,
        &mut buffer,
        &avro,
        WireFormat::Confluent(7),
        &row_schema,
        &good,
    );
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct DerivedAddr {
    city: String,
    zip: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct DerivedEvent {
    id: i64,
    small: i8,
    medium: i16,
    user: String,
    score: Option<f64>,
    ratio: f32,
    active: bool,
    tags: Vec<String>,
    attrs: BTreeMap<String, Option<i64>>,
    home: DerivedAddr,
    previous: Option<DerivedAddr>,
    day: NaiveDate,
    at: DateTime<Utc>,
    ts_ms: Instant,
    #[beam(bytes)]
    blob: Vec<u8>,
}

#[test]
fn direct_encoder_matches_arrow_avro_for_beam_row_derive() {
    let row_schema = DerivedEvent::beam_schema();
    let derived: Arc<str> = avro_schema(&row_schema).unwrap().into();
    let reversed: Arc<str> = r#"{
      "type": "record", "name": "DerivedEvent",
      "fields": [
        {"name": "id", "type": "long"},
        {"name": "small", "type": "int"},
        {"name": "medium", "type": "int"},
        {"name": "user", "type": "string"},
        {"name": "score", "type": ["double", "null"]},
        {"name": "ratio", "type": "float"},
        {"name": "active", "type": "boolean"},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "attrs", "type": {"type": "map", "values": ["long", "null"]}},
        {"name": "home", "type": {
          "type": "record", "name": "DerivedAddr",
          "fields": [
            {"name": "city", "type": "string"},
            {"name": "zip", "type": ["int", "null"]}
          ]
        }},
        {"name": "previous", "type": ["DerivedAddr", "null"]},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "ts_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "blob", "type": "bytes"}
      ]
    }"#
    .into();

    let events = [
        DerivedEvent {
            id: 42,
            small: -5,
            medium: 1200,
            user: "alice".into(),
            score: Some(98.5),
            ratio: 0.25,
            active: true,
            tags: vec!["x".into(), "yy".into()],
            attrs: BTreeMap::from([("a".into(), Some(1)), ("b".into(), None)]),
            home: DerivedAddr {
                city: "Lisbon".into(),
                zip: Some(1100),
            },
            previous: Some(DerivedAddr {
                city: "Porto".into(),
                zip: None,
            }),
            day: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            at: DateTime::from_timestamp(1_700_000_000, 123_456_000).unwrap(),
            ts_ms: Instant::from_millis(1_700_000_000_789),
            blob: vec![0, 1, 255],
        },
        DerivedEvent {
            id: -1,
            small: 0,
            medium: 0,
            user: String::new(),
            score: None,
            ratio: -1.5,
            active: false,
            tags: Vec::new(),
            attrs: BTreeMap::new(),
            home: DerivedAddr {
                city: String::new(),
                zip: None,
            },
            previous: None,
            day: NaiveDate::from_ymd_opt(1969, 12, 31).unwrap(),
            at: DateTime::from_timestamp(-1, 999_999_000).unwrap(),
            ts_ms: Instant::from_millis(-500),
            blob: Vec::new(),
        },
    ];

    for avro in [&derived, &reversed] {
        for format in [WireFormat::Plain, WireFormat::Confluent(13)] {
            let mut encoder = DatumEncoder::default();
            let mut buffer = Vec::new();
            for event in &events {
                let row = event.to_row().unwrap();
                let expected = assert_matches_arrow_avro(
                    &mut encoder,
                    &mut buffer,
                    avro,
                    format,
                    &row_schema,
                    &row,
                );
                encoder
                    .encode_beam_row(avro, format, &row_schema, event, &mut buffer)
                    .unwrap();
                assert_eq!(buffer.as_slice(), expected.as_slice(), "encode_beam_row");
                let decoded = decode_single(avro, &row_schema, format, &buffer);
                assert_eq!(DerivedEvent::from_row(&decoded).unwrap(), *event);
            }
        }
    }
}

#[test]
fn arrow_fallback_with_fixed_and_enum_reuses_preparsed_symbols() {
    // `fixed` triggers the arrow-avro fallback while the enum fields test pre-parsed symbols.
    let avro: Arc<str> = r#"{
      "type": "record", "name": "WithFixed", "namespace": "fallback",
      "fields": [
        {"name": "tag", "type": {"type": "fixed", "name": "Tag2", "size": 2}},
        {"name": "color", "type": {
          "type": "enum", "name": "Shade", "symbols": ["LIGHT", "DARK"]
        }},
        {"name": "palette", "type": {"type": "array", "items": "Shade"}},
        {"name": "named", "type": {"type": "map", "values": ["null", "Shade"]}}
      ]
    }"#
    .into();
    let row_schema = beam_schema(&avro).unwrap();
    let good1 = Row::new(
        Arc::clone(&row_schema),
        vec![
            Some(FieldValue::Bytes(vec![0xab, 0xcd])),
            Some(FieldValue::String("DARK".into())),
            Some(FieldValue::Array(vec![
                Some(FieldValue::String("LIGHT".into())),
                Some(FieldValue::String("DARK".into())),
            ])),
            Some(FieldValue::Map(vec![
                (
                    FieldValue::String("k".into()),
                    Some(FieldValue::String("LIGHT".into())),
                ),
                (FieldValue::String("n".into()), None),
            ])),
        ],
    )
    .unwrap();
    let good2 = Row::new(
        Arc::clone(&row_schema),
        vec![
            Some(FieldValue::Bytes(vec![0x01, 0x02])),
            Some(FieldValue::String("LIGHT".into())),
            Some(FieldValue::Array(Vec::new())),
            Some(FieldValue::Map(Vec::new())),
        ],
    )
    .unwrap();

    let mut encoder = DatumEncoder::default();
    let mut buffer = Vec::new();
    for row in [&good1, &good2] {
        let bytes = assert_matches_arrow_avro(
            &mut encoder,
            &mut buffer,
            &avro,
            WireFormat::Confluent(5),
            &row_schema,
            row,
        );
        assert_eq!(
            decode_single(&avro, &row_schema, WireFormat::Confluent(5), &bytes),
            *row
        );
    }

    let mut bad_vals = good1.values().to_vec();
    bad_vals[1] = Some(FieldValue::String("NEON".into()));
    let bad = Row::new(Arc::clone(&row_schema), bad_vals).unwrap();
    buffer.extend_from_slice(&[0xff; 4]);
    let err = encoder
        .encode_row(
            &avro,
            WireFormat::Confluent(5),
            &row_schema,
            &bad,
            &mut buffer,
        )
        .unwrap_err();
    assert!(err.contains("NEON"), "{err}");
    assert!(buffer.is_empty());

    // Recovers after fallback error.
    assert_matches_arrow_avro(
        &mut encoder,
        &mut buffer,
        &avro,
        WireFormat::Confluent(5),
        &row_schema,
        &good1,
    );
}

#[test]
fn direct_encoder_rejects_invalid_inputs_without_corrupting_buffer_or_state() {
    let avro: Arc<str> = r#"{
      "type": "record", "name": "Check",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}}
      ]
    }"#
    .into();
    let row_schema = beam_schema(&avro).unwrap();
    let good = Row::new(
        Arc::clone(&row_schema),
        vec![
            Some(FieldValue::String("ok".into())),
            Some(FieldValue::Int64(10)),
        ],
    )
    .unwrap();
    let mut encoder = DatumEncoder::default();
    let mut buffer = Vec::new();
    let expected = assert_matches_arrow_avro(
        &mut encoder,
        &mut buffer,
        &avro,
        WireFormat::Confluent(3),
        &row_schema,
        &good,
    );

    // Wrong field count.
    let short_schema = Arc::new(Schema::new(vec![Field::new("name", FieldType::string())]));
    let short_row = Row::new(short_schema, vec![Some(FieldValue::String("x".into()))]).unwrap();
    buffer.extend_from_slice(&[0xaa; 8]);
    assert!(
        encoder
            .encode_row(
                &avro,
                WireFormat::Confluent(3),
                &row_schema,
                &short_row,
                &mut buffer
            )
            .is_err()
    );
    assert!(buffer.is_empty());

    // Wrong value type (Int64 where String expected).
    let wrong_schema = Arc::new(Schema::new(vec![
        Field::new("name", FieldType::int64()),
        Field::new("day", FieldType::int64()),
    ]));
    let wrong_row = Row::new(
        wrong_schema,
        vec![Some(FieldValue::Int64(1)), Some(FieldValue::Int64(2))],
    )
    .unwrap();
    buffer.extend_from_slice(&[0xaa; 8]);
    assert!(
        encoder
            .encode_row(
                &avro,
                WireFormat::Confluent(3),
                &row_schema,
                &wrong_row,
                &mut buffer
            )
            .is_err()
    );
    assert!(buffer.is_empty());

    // Out-of-range Date32 value.
    let bad_date = Row::new(
        Arc::clone(&row_schema),
        vec![
            Some(FieldValue::String("ok".into())),
            Some(FieldValue::Int64(i64::MAX)),
        ],
    )
    .unwrap();
    buffer.extend_from_slice(&[0xaa; 8]);
    assert!(
        encoder
            .encode_row(
                &avro,
                WireFormat::Confluent(3),
                &row_schema,
                &bad_date,
                &mut buffer
            )
            .is_err()
    );
    assert!(buffer.is_empty());

    // Invalid UTF-8 in RowElement::Encoded.
    let invalid_utf8 = RowElement::Encoded(vec![2, 0, 2, 0xff, 0xfe, 10]);
    buffer.extend_from_slice(&[0xaa; 8]);
    assert!(
        encoder
            .encode_row_element(
                &avro,
                WireFormat::Confluent(3),
                &row_schema,
                &invalid_utf8,
                &mut buffer,
            )
            .is_err()
    );
    assert!(buffer.is_empty());

    // Truncated RowElement::Encoded.
    let truncated = RowElement::Encoded(vec![2, 0, 5, b'a', b'b']);
    buffer.extend_from_slice(&[0xaa; 8]);
    assert!(
        encoder
            .encode_row_element(
                &avro,
                WireFormat::Confluent(3),
                &row_schema,
                &truncated,
                &mut buffer,
            )
            .is_err()
    );
    assert!(buffer.is_empty());

    // Same encoder produces exact bytes on the next valid row.
    encoder
        .encode_row(
            &avro,
            WireFormat::Confluent(3),
            &row_schema,
            &good,
            &mut buffer,
        )
        .unwrap();
    assert_eq!(buffer, expected);
}

#[test]
fn reordered_encoding_positions_fall_back_and_encode_exact_bytes() {
    let avro: Arc<str> = PERSON.into();
    let row_schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()).with_encoding_position(1),
        Field::new("name", FieldType::string()).with_encoding_position(0),
    ]));
    let row = person(&row_schema, 123, "alice");
    let mut encoder = DatumEncoder::default();
    let mut buffer = Vec::new();
    let bytes = assert_matches_arrow_avro(
        &mut encoder,
        &mut buffer,
        &avro,
        WireFormat::Plain,
        &row_schema,
        &row,
    );
    let read_schema = beam_schema(&avro).unwrap();
    let decoded = decode_single(&avro, &read_schema, WireFormat::Plain, &bytes);
    assert_eq!(decoded.get_i64("id").unwrap(), Some(123));
    assert_eq!(decoded.get_string("name").unwrap(), Some("alice"));
}

#[test]
fn flush_error_on_non_last_row_isolates_only_the_bad_frame() {
    // Invalid UTF-8 string bytes fail during `decoder.flush()` after all frames are fed.
    let person_schema: Arc<str> = PERSON.into();
    let person_row_schema = beam_schema(PERSON).unwrap();
    let mut encoder = DatumEncoder::default();
    let good_0 = person(&person_row_schema, 1, "a");
    let good_2 = person(&person_row_schema, 3, "c");
    let good_0_bytes = encode_plain(
        &mut encoder,
        &person_row_schema,
        &RowElement::Row(good_0.clone()),
    );
    let good_2_bytes = encode_plain(
        &mut encoder,
        &person_row_schema,
        &RowElement::Row(good_2.clone()),
    );
    // id = 2 (zigzag 0x04), string len = 2 (zigzag 0x04), invalid UTF-8 bytes 0xff, 0xfe.
    let bad_utf8: &[u8] = &[0x04, 0x04, 0xff, 0xfe];
    let frames = [
        Frame {
            schema_id: PLAIN_ID,
            datum: &good_0_bytes,
        },
        Frame {
            schema_id: PLAIN_ID,
            datum: bad_utf8,
        },
        Frame {
            schema_id: PLAIN_ID,
            datum: &good_2_bytes,
        },
    ];
    let mut decoder = DatumDecoder::default();
    let decoded = decoder.decode_frames(&person_schema, &person_row_schema, &frames, |_| {
        Ok(Arc::clone(&person_schema))
    });
    assert_eq!(decoded[0].as_ref().unwrap(), &good_0);
    assert!(decoded[1].is_err(), "invalid UTF-8 frame must fail");
    assert_eq!(decoded[2].as_ref().unwrap(), &good_2);

    // Out-of-range enum index on index 1 also isolates cleanly without dropping index 2.
    let enum_schema: Arc<str> = r#"{"type": "record", "name": "Status", "fields": [
        {"name": "id", "type": "long"},
        {"name": "color", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}}
    ]}"#
    .into();
    let enum_row_schema = beam_schema(&enum_schema).unwrap();
    // id = 1 (0x02), color = 0 (RED, 0x00); id = 2 (0x04), color = 5 (0x0a, invalid);
    // id = 3 (0x06), color = 1 (GREEN, 0x02).
    let enum_frames = [
        Frame {
            schema_id: PLAIN_ID,
            datum: &[0x02, 0x00],
        },
        Frame {
            schema_id: PLAIN_ID,
            datum: &[0x04, 0x0a],
        },
        Frame {
            schema_id: PLAIN_ID,
            datum: &[0x06, 0x02],
        },
    ];
    let mut enum_decoder = DatumDecoder::default();
    let enum_decoded =
        enum_decoder.decode_frames(&enum_schema, &enum_row_schema, &enum_frames, |_| {
            Ok(Arc::clone(&enum_schema))
        });
    assert_eq!(
        enum_decoded[0]
            .as_ref()
            .unwrap()
            .get_string("color")
            .unwrap(),
        Some("RED")
    );
    assert!(
        enum_decoded[1].is_err(),
        "out-of-range enum index must fail"
    );
    assert_eq!(
        enum_decoded[2]
            .as_ref()
            .unwrap()
            .get_string("color")
            .unwrap(),
        Some("GREEN")
    );

    // Decoder recovers for a subsequent valid batch.
    let next =
        enum_decoder.decode_frames(&enum_schema, &enum_row_schema, &enum_frames[..1], |_| {
            Ok(Arc::clone(&enum_schema))
        });
    assert_eq!(
        next[0].as_ref().unwrap().get_string("color").unwrap(),
        Some("RED")
    );
}

#[test]
fn direct_encoder_writes_chunked_arrays_as_the_arrow_path_does() {
    let avro: Arc<str> = r#"{"type": "record", "name": "Chunks", "fields": [
      {"name": "tags", "type": {"type": "array", "items": "string"}},
      {"name": "scores", "type": {"type": "array", "items": ["null", "int"]}},
      {"name": "empty", "type": {"type": "array", "items": "long"}}
    ]}"#
    .into();
    let row_schema = Arc::new(Schema::new(vec![
        Field::new("tags", FieldType::array(FieldType::string())),
        Field::new(
            "scores",
            FieldType::array(FieldType::int32().with_nullable(true)),
        ),
        Field::new("empty", FieldType::array(FieldType::int64())),
    ]));
    let text = |s: &str| Some(FieldValue::String(s.into()));
    let row = Row::new(
        Arc::clone(&row_schema),
        vec![
            Some(FieldValue::Array(vec![text("a"), text("bb"), text("ccc")])),
            Some(FieldValue::Array(vec![Some(FieldValue::Int32(4)), None])),
            Some(FieldValue::Array(Vec::new())),
        ],
    )
    .unwrap();
    // Each array in chunks: count -1, then varint chunk sizes, and a 0 chunk at the end.
    let chunked = vec![
        3, 0, // 3 fields, no null field.
        0xff, 0xff, 0xff, 0xff, 2, 1, b'a', 2, b'b', b'b', 1, 3, b'c', b'c', b'c', 0, // tags
        0xff, 0xff, 0xff, 0xff, 1, 1, 4, 1, 0, 0, // scores
        0xff, 0xff, 0xff, 0xff, 0, // empty
    ];
    let mut encoder = DatumEncoder::default();
    let mut buffer = Vec::new();
    let expected = assert_matches_arrow_avro(
        &mut encoder,
        &mut buffer,
        &avro,
        WireFormat::Plain,
        &row_schema,
        &row,
    );
    encoder
        .encode_row_element(
            &avro,
            WireFormat::Plain,
            &row_schema,
            &RowElement::Encoded(chunked),
            &mut buffer,
        )
        .unwrap();
    assert_eq!(buffer, expected);
}

/// A `micros_instant` row whose `micros` field is `INT32`.
fn int32_micros_schema() -> (Arc<Schema>, Arc<Schema>) {
    let instant = Arc::new(Schema::new(vec![
        Field::new("seconds", FieldType::int64()),
        Field::new("micros", FieldType::int32()),
    ]));
    let row_schema = Arc::new(Schema::new(vec![Field::new(
        "at",
        FieldType::logical(
            URN_MICROS_INSTANT,
            Vec::new(),
            FieldType::row(Arc::clone(&instant)),
        ),
    )]));
    (instant, row_schema)
}

const AT_MICROS: &str = r#"{"type": "record", "name": "At", "fields": [
  {"name": "at", "type": {"type": "long", "logicalType": "timestamp-micros"}}
]}"#;

#[test]
fn direct_encoder_matches_arrow_avro_for_an_int32_micros_field() {
    let avro: Arc<str> = AT_MICROS.into();
    let (instant, row_schema) = int32_micros_schema();
    let mut encoder = DatumEncoder::default();
    let mut buffer = Vec::new();
    for (seconds, micros) in [(2, -1), (-3, 999_999), (0, i32::MIN)] {
        let value = Row::new(
            Arc::clone(&instant),
            vec![
                Some(FieldValue::Int64(seconds)),
                Some(FieldValue::Int32(micros)),
            ],
        )
        .unwrap();
        let row = Row::new(Arc::clone(&row_schema), vec![Some(FieldValue::Row(value))]).unwrap();
        let expected = assert_matches_arrow_avro(
            &mut encoder,
            &mut buffer,
            &avro,
            WireFormat::Plain,
            &row_schema,
            &row,
        );
        let micros = seconds * 1_000_000 + i64::from(micros);
        assert_eq!(expected, crate::common::long(micros));
    }
}

#[test]
fn direct_encoder_rejects_an_int32_micros_field_outside_the_i32_range() {
    let avro: Arc<str> = AT_MICROS.into();
    let (_, row_schema) = int32_micros_schema();
    // `micros` is 2^32 - 1, the varint of -1 with no sign extension.
    let encoded = RowElement::Encoded(vec![1, 0, 2, 0, 2, 0xff, 0xff, 0xff, 0xff, 0x0f]);
    let mut encoder = DatumEncoder::default();
    let mut buffer = vec![0xaa; 8];
    let result =
        encoder.encode_row_element(&avro, WireFormat::Plain, &row_schema, &encoded, &mut buffer);

    assert!(result.is_err(), "{buffer:?}");
    assert!(buffer.is_empty());
}
