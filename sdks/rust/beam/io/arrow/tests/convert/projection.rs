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

//! Conversions of the fields that a [`Projection`] selects.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::sync::Arc;

use arrow_io::{
    ArrowBridgeError, Projection, RowBatchBuilder, encoded_rows_to_projected_batch,
    encoded_rows_to_record_batch, rows_to_projected_batch, rows_to_record_batch,
};
use arrow_io::{date_type, decimal_type, micros_instant_type, millis_instant_type};
use beam::coders::RowCoder;
use beam::schema::{FieldType, FieldValue, Row, Schema, TypeInfo};

fn inner_schema() -> Schema {
    Schema::builder()
        .field("x", FieldType::int32())
        .nullable_field("y", FieldType::string())
        .build()
}

/// A field of every type that the row coder writes.
fn every_type_schema() -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .field("b", FieldType::byte())
            .field("s", FieldType::int16())
            .nullable_field("i", FieldType::int32())
            .field("l", FieldType::int64())
            .field("f", FieldType::float())
            .field("d", FieldType::double())
            .nullable_field("str", FieldType::string())
            .field("flag", FieldType::boolean())
            .nullable_field("bytes", FieldType::bytes())
            .field(
                "arr",
                FieldType::array(FieldType::int64().with_nullable(true)),
            )
            .field(
                "it",
                FieldType::new(TypeInfo::Iterable(Box::new(FieldType::int32())), false),
            )
            .nullable_field(
                "map",
                FieldType::map(FieldType::string(), FieldType::double().with_nullable(true)),
            )
            .nullable_field("row", FieldType::row(inner_schema()))
            .field(
                "rows",
                FieldType::array(FieldType::row(inner_schema()).with_nullable(true)),
            )
            .field("ts", micros_instant_type())
            .nullable_field("ms", millis_instant_type())
            .field("date", date_type())
            .nullable_field("amount", decimal_type())
            .field(
                "custom",
                FieldType::logical("beam:logical_type:custom:v1", vec![7], FieldType::string()),
            )
            .build(),
    )
}

fn instant(schema: &Schema, seconds: i64, micros: i64) -> FieldValue {
    let TypeInfo::Logical { representation, .. } =
        &schema.field("ts").unwrap().field_type.type_info
    else {
        panic!("ts must be logical");
    };
    let TypeInfo::Row(repr) = &representation.type_info else {
        panic!("micros_instant must be a row");
    };
    let values = vec![
        Some(FieldValue::Int64(seconds)),
        Some(FieldValue::Int64(micros)),
    ];
    FieldValue::Row(Row::new(Arc::clone(repr), values).unwrap())
}

fn inner(x: i32, y: Option<&str>) -> FieldValue {
    let values = vec![Some(FieldValue::Int32(x)), y.map(FieldValue::from)];
    FieldValue::Row(Row::new(Arc::new(inner_schema()), values).unwrap())
}

fn sample_rows(schema: &Arc<Schema>) -> Vec<Row> {
    let full = vec![
        Some(FieldValue::Byte(-3)),
        Some(FieldValue::Int16(300)),
        Some(FieldValue::Int32(70_000)),
        Some(FieldValue::Int64(i64::MAX)),
        Some(FieldValue::Float(1.5)),
        Some(FieldValue::Double(-2.25)),
        Some(FieldValue::String("hello".into())),
        Some(FieldValue::Boolean(true)),
        Some(FieldValue::Bytes(vec![0, 1, 2])),
        Some(FieldValue::Array(vec![
            Some(FieldValue::Int64(1)),
            None,
            Some(FieldValue::Int64(300)),
        ])),
        Some(FieldValue::Array(vec![Some(FieldValue::Int32(-9))])),
        Some(FieldValue::Map(vec![
            (
                FieldValue::String("k1".into()),
                Some(FieldValue::Double(1.0)),
            ),
            (FieldValue::String("k2".into()), None),
        ])),
        Some(inner(1, Some("one"))),
        Some(FieldValue::Array(vec![Some(inner(2, None)), None])),
        Some(instant(schema, -1, 999_999)),
        Some(FieldValue::Int64(1_700_000_000_123)),
        Some(FieldValue::Int64(19_000)),
        // Scale 2, then the unscaled value 12345 in 2 big-endian bytes.
        Some(FieldValue::Bytes(vec![2, 2, 0x30, 0x39])),
        Some(FieldValue::String("custom".into())),
    ];
    let sparse = vec![
        Some(FieldValue::Byte(0)),
        Some(FieldValue::Int16(0)),
        None,
        Some(FieldValue::Int64(0)),
        Some(FieldValue::Float(0.0)),
        Some(FieldValue::Double(0.0)),
        None,
        Some(FieldValue::Boolean(false)),
        None,
        Some(FieldValue::Array(vec![])),
        Some(FieldValue::Array(vec![])),
        None,
        None,
        Some(FieldValue::Array(vec![])),
        Some(instant(schema, 0, 0)),
        None,
        Some(FieldValue::Int64(-1)),
        None,
        Some(FieldValue::String(String::new())),
    ];
    [full, sparse]
        .into_iter()
        .map(|values| Row::new(Arc::clone(schema), values).unwrap())
        .collect()
}

fn encode(rows: &[Row]) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| {
            let mut bytes = Vec::new();
            RowCoder::encode_row(row, &mut bytes).unwrap();
            bytes
        })
        .collect()
}

/// Each field in turn is skipped. The other columns must be the columns of the full batch.
#[test]
fn skipping_any_field_keeps_the_other_columns() {
    let schema = every_type_schema();
    let rows = sample_rows(&schema);
    let encoded = encode(&rows);
    let full = encoded_rows_to_record_batch(&schema, encoded.iter().map(Vec::as_slice)).unwrap();
    for skipped in 0..schema.fields.len() {
        let kept: Vec<usize> = (0..schema.fields.len()).filter(|i| *i != skipped).collect();
        let projection = Projection::Fields(kept.clone());
        let expected = full.project(&kept).unwrap();
        let from_bytes = encoded_rows_to_projected_batch(
            &schema,
            encoded.iter().map(Vec::as_slice),
            &projection,
        )
        .unwrap();
        assert_eq!(from_bytes, expected, "skipped field {skipped}");
        let from_rows = rows_to_projected_batch(&schema, &rows, &projection).unwrap();
        assert_eq!(from_rows, expected, "skipped field {skipped}");
    }
}

/// Only one field is kept, so the reader steps over every other value.
#[test]
fn keeping_one_field_skips_all_the_others() {
    let schema = every_type_schema();
    let rows = sample_rows(&schema);
    let encoded = encode(&rows);
    let full = rows_to_record_batch(&schema, &rows).unwrap();
    for kept in 0..schema.fields.len() {
        let projection = Projection::Fields(vec![kept]);
        let from_bytes = encoded_rows_to_projected_batch(
            &schema,
            encoded.iter().map(Vec::as_slice),
            &projection,
        )
        .unwrap();
        assert_eq!(
            from_bytes,
            full.project(&[kept]).unwrap(),
            "kept field {kept}"
        );
    }
}

#[test]
fn a_projection_follows_the_encoding_positions() {
    let mut schema = Schema::builder()
        .field("a", FieldType::string())
        .field("b", FieldType::int16())
        .field("c", FieldType::int64())
        .build();
    // The encoding puts `c` first, then `a`, then `b`.
    schema.encoding_positions_set = true;
    [1, 2, 0]
        .into_iter()
        .zip(schema.fields.iter_mut())
        .for_each(|(position, field)| field.encoding_position = Some(position));
    let schema = Arc::new(schema);
    let values = vec![
        Some(FieldValue::String("text".into())),
        Some(FieldValue::Int16(-2)),
        Some(FieldValue::Int64(1 << 40)),
    ];
    let rows = vec![Row::new(Arc::clone(&schema), values).unwrap()];
    let encoded = encode(&rows);
    let full = rows_to_record_batch(&schema, &rows).unwrap();
    let projection = Projection::Fields(vec![2, 1]);
    let batch =
        encoded_rows_to_projected_batch(&schema, encoded.iter().map(Vec::as_slice), &projection)
            .unwrap();
    assert_eq!(batch, full.project(&[1, 2]).unwrap());
}

#[test]
fn unnest_makes_the_nested_fields_the_columns() {
    let schema = every_type_schema();
    let rows = sample_rows(&schema);
    let row_index = schema.fields.iter().position(|f| f.name == "row").unwrap();
    let projection = Projection::Unnest {
        field: row_index,
        fields: vec![0, 1],
    };
    let inner = Arc::new(inner_schema());
    let nested = vec![
        Row::new(
            Arc::clone(&inner),
            vec![
                Some(FieldValue::Int32(1)),
                Some(FieldValue::String("one".into())),
            ],
        )
        .unwrap(),
    ];
    let expected = rows_to_record_batch(&inner, &nested).unwrap();

    let encoded = encode(&rows[..1]);
    let from_bytes =
        encoded_rows_to_projected_batch(&schema, encoded.iter().map(Vec::as_slice), &projection)
            .unwrap();
    assert_eq!(from_bytes, expected);
    assert_eq!(
        rows_to_projected_batch(&schema, &rows[..1], &projection).unwrap(),
        expected
    );

    let only_x = Projection::Unnest {
        field: row_index,
        fields: vec![0],
    };
    let batch = rows_to_projected_batch(&schema, &rows[..1], &only_x).unwrap();
    assert_eq!(batch, expected.project(&[0]).unwrap());
}

#[test]
fn unnest_of_a_null_row_does_not_convert() {
    let schema = every_type_schema();
    let rows = sample_rows(&schema);
    let row_index = schema.fields.iter().position(|f| f.name == "row").unwrap();
    let projection = Projection::Unnest {
        field: row_index,
        fields: vec![0],
    };
    // The second sample row has a null `row`.
    let encoded = encode(&rows[1..]);
    let mut builder = RowBatchBuilder::with_projection(&schema, &projection).unwrap();
    assert!(matches!(
        builder.append_encoded(&encoded[0]),
        Err(ArrowBridgeError::UnexpectedNull { field }) if field == "row"
    ));
    assert!(matches!(
        rows_to_projected_batch(&schema, &rows[1..], &projection),
        Err(ArrowBridgeError::UnexpectedNull { field }) if field == "row"
    ));
}

#[test]
fn a_projection_of_a_missing_field_or_a_non_row_is_an_error() {
    let schema = every_type_schema();
    let count = schema.fields.len();
    assert!(matches!(
        RowBatchBuilder::with_projection(&schema, &Projection::Fields(vec![count])),
        Err(ArrowBridgeError::MissingField(_))
    ));
    assert!(matches!(
        RowBatchBuilder::with_projection(
            &schema,
            &Projection::Unnest {
                field: 0,
                fields: vec![0]
            }
        ),
        Err(ArrowBridgeError::TypeMismatch { field, .. }) if field == "b"
    ));
}
