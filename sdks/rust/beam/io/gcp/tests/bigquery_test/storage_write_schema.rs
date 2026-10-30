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

//! Beam to BigQuery types of the native Storage Write connector: the columns of a created
//! table, the Arrow types that a write sends, and the rows that come back from a rejected
//! request. No test calls BigQuery.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::sync::Arc;

use arrow_io::arrow_array::cast::AsArray;
use arrow_io::arrow_array::types::{
    Decimal128Type, Decimal256Type, Int64Type, Time64MicrosecondType, TimestampMicrosecondType,
};
use arrow_io::arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_io::arrow_buffer::i256;
use arrow_io::arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use arrow_io::{date_type, decimal_type, micros_instant_type};
use beam::schema::{FieldType, FieldValue, Row, Schema};
use gcp::bigquery::storage::schema::{
    table_arrow_schema, table_fields, write_array, write_batch_to_rows, write_field,
};
use google_cloud_bigquery_v2::model::TableFieldSchema;

fn column(name: &str, column_type: &str, mode: &str) -> TableFieldSchema {
    TableFieldSchema::new()
        .set_name(name)
        .set_type(column_type)
        .set_mode(mode)
}

/// A Beam `decimal` payload: VarInt scale, VarInt length, big-endian two's complement.
fn decimal(unscaled: i64, scale: u8) -> FieldValue {
    let digits = unscaled.to_be_bytes();
    let start = (0..7)
        .take_while(|&i| {
            let next_negative = digits[i + 1] & 0x80 != 0;
            (digits[i] == 0 && !next_negative) || (digits[i] == 0xFF && next_negative)
        })
        .count();
    let digits = &digits[start..];
    let mut payload = vec![scale, u8::try_from(digits.len()).unwrap()];
    payload.extend_from_slice(digits);
    FieldValue::Bytes(payload)
}

/// Converts `rows` of `schema` into the Arrow batch that a write sends to `columns`.
fn written(
    schema: &Schema,
    rows: &[Row],
    columns: &[TableFieldSchema],
) -> beam::Result<RecordBatch> {
    let table = table_arrow_schema(columns)?;
    let batch = arrow_io::rows_to_record_batch(schema, rows).unwrap();
    let fields = batch
        .schema()
        .fields()
        .iter()
        .zip(table.fields())
        .map(|(source, column)| write_field(source, column))
        .collect::<beam::Result<Vec<_>>>()?;
    let columns = batch
        .columns()
        .iter()
        .zip(&fields)
        .map(|(column, field)| write_array(column, field.data_type()))
        .collect::<beam::Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap())
}

#[test]
fn a_created_table_has_one_column_for_each_field() {
    let nested = Schema::builder().field("x", FieldType::int32()).build();
    let schema = Schema::builder()
        .field("id", FieldType::int64())
        .nullable_field("ratio", FieldType::float())
        .nullable_field("name", FieldType::string())
        .nullable_field("blob", FieldType::bytes())
        .nullable_field("flag", FieldType::boolean())
        .nullable_field("amount", decimal_type())
        .nullable_field("at", micros_instant_type())
        .nullable_field("on", date_type())
        .nullable_field("point", FieldType::row(nested))
        .field("tags", FieldType::array(FieldType::string()))
        .build();
    let columns: Vec<_> = table_fields(&schema)
        .unwrap()
        .into_iter()
        .map(|f| (f.name, f.r#type, f.mode, f.fields.len()))
        .collect();
    let expected = [
        ("id", "INT64", "REQUIRED", 0),
        ("ratio", "FLOAT64", "NULLABLE", 0),
        ("name", "STRING", "NULLABLE", 0),
        ("blob", "BYTES", "NULLABLE", 0),
        ("flag", "BOOL", "NULLABLE", 0),
        ("amount", "NUMERIC", "NULLABLE", 0),
        ("at", "TIMESTAMP", "NULLABLE", 0),
        ("on", "DATE", "NULLABLE", 0),
        ("point", "STRUCT", "NULLABLE", 1),
        ("tags", "STRING", "REPEATED", 0),
    ]
    .map(|(name, column_type, mode, nested)| {
        (
            name.to_string(),
            column_type.to_string(),
            mode.to_string(),
            nested,
        )
    });
    assert_eq!(columns, expected);
}

#[test]
fn maps_and_arrays_of_arrays_have_no_column() {
    let cases = [
        FieldType::map(FieldType::string(), FieldType::int64()),
        FieldType::array(FieldType::array(FieldType::int64())),
    ];
    for field_type in cases {
        let schema = Schema::builder().field("bad", field_type).build();
        let error = table_fields(&schema).unwrap_err().to_string();
        assert!(error.contains("'bad'"), "{error}");
    }
}

#[test]
fn narrow_numbers_widen_to_the_column_types() {
    let schema = Schema::builder()
        .field("small", FieldType::int32())
        .field("ratio", FieldType::float())
        .build();
    let row = Row::new(
        Arc::new(schema.clone()),
        vec![Some(FieldValue::Int32(-7)), Some(FieldValue::Float(0.5))],
    )
    .unwrap();
    let batch = written(
        &schema,
        &[row],
        &[
            column("small", "INTEGER", "REQUIRED"),
            column("ratio", "FLOAT", "REQUIRED"),
        ],
    )
    .unwrap();
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), -7);
    assert_eq!(*batch.column(1).data_type(), DataType::Float64);
}

#[test]
fn decimals_take_the_scale_of_the_column() {
    let schema = Schema::builder()
        .field("numeric", decimal_type())
        .field("bignumeric", decimal_type())
        .build();
    // 12.5 at scale 1, and -0.001 at scale 3.
    let row = Row::new(
        Arc::new(schema.clone()),
        vec![Some(decimal(125, 1)), Some(decimal(-1, 3))],
    )
    .unwrap();
    let batch = written(
        &schema,
        &[row],
        &[
            column("numeric", "NUMERIC", "REQUIRED"),
            column("bignumeric", "BIGNUMERIC", "REQUIRED"),
        ],
    )
    .unwrap();
    let numeric = batch.column(0).as_primitive::<Decimal128Type>();
    assert_eq!(*numeric.data_type(), DataType::Decimal128(38, 9));
    assert_eq!(numeric.value(0), 12_500_000_000);
    let bignumeric = batch.column(1).as_primitive::<Decimal256Type>();
    let expected = i256::from_i128(-10).checked_pow(35).unwrap();
    assert_eq!(bignumeric.value(0), expected);
}

#[test]
fn a_decimal_that_loses_digits_fails() {
    let schema = Schema::builder().field("numeric", decimal_type()).build();
    // 0.0000000001 has scale 10, and NUMERIC keeps 9 digits after the point.
    let row = Row::new(Arc::new(schema.clone()), vec![Some(decimal(1, 10))]).unwrap();
    let error = written(&schema, &[row], &[column("numeric", "NUMERIC", "REQUIRED")])
        .unwrap_err()
        .to_string();
    assert!(error.contains("NUMERIC(38, 9)"), "{error}");
}

#[test]
fn datetime_and_time_parse_from_the_read_format() {
    let schema = Schema::builder()
        .nullable_field("civil", FieldType::string())
        .nullable_field("spaced", FieldType::string())
        .nullable_field("clock", FieldType::string())
        .build();
    let text = |value: &str| Some(FieldValue::String(value.to_string()));
    let rows = [
        Row::new(
            Arc::new(schema.clone()),
            vec![
                text("2024-05-01T12:30:00.000001"),
                text("2024-05-01 12:30:00"),
                text("12:30:00.000001"),
            ],
        )
        .unwrap(),
        Row::new(Arc::new(schema.clone()), vec![None, None, None]).unwrap(),
    ];
    let batch = written(
        &schema,
        &rows,
        &[
            column("civil", "DATETIME", "NULLABLE"),
            column("spaced", "DATETIME", "NULLABLE"),
            column("clock", "TIME", "NULLABLE"),
        ],
    )
    .unwrap();
    let civil = batch.column(0).as_primitive::<TimestampMicrosecondType>();
    assert_eq!(civil.value(0), 1_714_566_600_000_001);
    assert!(civil.is_null(1));
    let spaced = batch.column(1).as_primitive::<TimestampMicrosecondType>();
    assert_eq!(spaced.value(0), 1_714_566_600_000_000);
    let clock = batch.column(2).as_primitive::<Time64MicrosecondType>();
    assert_eq!(clock.value(0), 45_000_000_001);
}

#[test]
fn a_bad_datetime_fails() {
    let schema = Schema::builder()
        .field("civil", FieldType::string())
        .build();
    let row = Row::new(
        Arc::new(schema.clone()),
        vec![Some(FieldValue::String("yesterday".to_string()))],
    )
    .unwrap();
    let error = written(&schema, &[row], &[column("civil", "DATETIME", "REQUIRED")])
        .unwrap_err()
        .to_string();
    assert!(error.contains("yesterday"), "{error}");
}

#[test]
fn a_type_that_does_not_convert_fails_before_any_row() {
    let source = ArrowField::new("flag", DataType::Boolean, true);
    let column = ArrowField::new("flag", DataType::Int64, true);
    let error = write_field(&source, &column).unwrap_err().to_string();
    assert!(error.contains("flag"), "{error}");
}

#[test]
fn nested_fields_match_columns_by_name_in_any_case() {
    let nested = Schema::builder()
        .field("Y", FieldType::string())
        .field("x", FieldType::int32())
        .build();
    let schema = Schema::builder()
        .field("point", FieldType::row(nested.clone()))
        .field("ids", FieldType::array(FieldType::int32()))
        .build();
    let point = Row::new(
        Arc::new(nested),
        vec![
            Some(FieldValue::String("seven".to_string())),
            Some(FieldValue::Int32(7)),
        ],
    )
    .unwrap();
    let row = Row::new(
        Arc::new(schema.clone()),
        vec![
            Some(FieldValue::Row(point)),
            Some(FieldValue::Array(vec![Some(FieldValue::Int32(1))])),
        ],
    )
    .unwrap();
    let batch = written(
        &schema,
        &[row],
        &[
            column("point", "RECORD", "REQUIRED").set_fields([
                column("x", "INT64", "REQUIRED"),
                column("y", "STRING", "REQUIRED"),
            ]),
            column("ids", "INT64", "REPEATED"),
        ],
    )
    .unwrap();
    let DataType::Struct(fields) = batch.schema().field(0).data_type().clone() else {
        panic!("point is not a struct");
    };
    let names: Vec<_> = fields.iter().map(|f| f.name().clone()).collect();
    assert_eq!(
        names,
        ["y", "x"],
        "the write keeps the order of the row fields"
    );
    assert_eq!(
        *batch.schema().field(1).data_type(),
        DataType::List(Arc::new(ArrowField::new("item", DataType::Int64, false)))
    );
}

#[test]
fn a_rejected_row_comes_back_with_its_beam_types() {
    let schema = Schema::builder()
        .field("small", FieldType::int32())
        .field("amount", decimal_type())
        .field("civil", FieldType::string())
        .build();
    let row = Row::new(
        Arc::new(schema.clone()),
        vec![
            Some(FieldValue::Int32(3)),
            Some(decimal(125, 1)),
            Some(FieldValue::String("2024-05-01T12:30:00.000001".to_string())),
        ],
    )
    .unwrap();
    let batch = written(
        &schema,
        std::slice::from_ref(&row),
        &[
            column("small", "INT64", "REQUIRED"),
            column("amount", "NUMERIC", "REQUIRED"),
            column("civil", "DATETIME", "REQUIRED"),
        ],
    )
    .unwrap();
    assert_eq!(
        *batch.schema().field(2).data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    let back = write_batch_to_rows(&batch, &Arc::new(schema)).unwrap();
    let values = back[0].values();
    assert_eq!(values[0], Some(FieldValue::Int32(3)));
    assert_eq!(values[2], row.values()[2]);
    // The decimal comes back at the scale of the column: 12.500000000.
    assert_eq!(values[1], Some(decimal(12_500_000_000, 9)));
}
