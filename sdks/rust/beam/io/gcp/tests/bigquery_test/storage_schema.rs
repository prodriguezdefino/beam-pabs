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

//! Arrow to Beam schemas and rows of the native Storage Read connector. No test calls
//! BigQuery.

use std::sync::Arc;

use arrow_io::arrow_array::{
    ArrayRef, Int64Array, ListArray, RecordBatch, StructArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_io::arrow_buffer::OffsetBuffer;
use arrow_io::arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use arrow_io::{date_type, decimal_type, micros_instant_type};
use beam::schema::{FieldType, FieldValue, Schema};
use gcp::bigquery::storage::schema::{batch_to_rows, beam_schema, table_schema_to_beam_schema};
use google_cloud_bigquery_v2::model::TableFieldSchema;

fn datetime_and_time_fields() -> Vec<Field> {
    vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "civil",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("clock", DataType::Time64(TimeUnit::Microsecond), true),
    ]
}

#[test]
fn datetime_and_time_become_strings_and_timestamp_an_instant() {
    let schema = beam_schema(&ArrowSchema::new(datetime_and_time_fields())).unwrap();
    let types: Vec<_> = schema.fields.iter().map(|f| f.field_type.clone()).collect();
    assert_eq!(
        types,
        vec![
            FieldType::int64(),
            micros_instant_type().with_nullable(true),
            FieldType::string().with_nullable(true),
            FieldType::string().with_nullable(true),
        ]
    );
}

#[test]
fn rows_hold_datetime_and_time_as_text() {
    let arrow_schema = Arc::new(ArrowSchema::new(datetime_and_time_fields()));
    // 2024-05-01T12:30:00.000001 and 12:30:00.000001.
    let micros = 1_714_566_600_000_001;
    let clock = 45_000_000_001;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(micros), None]).with_timezone("UTC")),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(micros), None])),
        Arc::new(Time64MicrosecondArray::from(vec![Some(clock), None])),
    ];
    let batch = RecordBatch::try_new(arrow_schema.clone(), columns).unwrap();
    let schema = Arc::new(beam_schema(&arrow_schema).unwrap());
    let rows = batch_to_rows(&batch, &schema).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get_string("civil").unwrap(),
        Some("2024-05-01T12:30:00.000001")
    );
    assert_eq!(
        rows[0].get_string("clock").unwrap(),
        Some("12:30:00.000001")
    );
    assert!(rows[0].get_value("at").unwrap().is_some());
    assert_eq!(rows[1].get_value("civil").unwrap(), &None);
    assert_eq!(rows[1].get_value("clock").unwrap(), &None);
}

#[test]
fn nested_datetime_columns_become_strings() {
    let civil = Field::new(
        "civil",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    );
    let entry = Fields::from(vec![civil]);
    let item = Field::new("item", DataType::Struct(entry.clone()), true);
    let field = Field::new("events", DataType::List(Arc::new(item.clone())), true);
    let structs = StructArray::try_new(
        entry,
        vec![Arc::new(TimestampMicrosecondArray::from(vec![
            0, 1_000_000,
        ]))],
        None,
    )
    .unwrap();
    let lists = ListArray::try_new(
        Arc::new(item),
        OffsetBuffer::from_lengths([2]),
        Arc::new(structs),
        None,
    )
    .unwrap();
    let arrow_schema = Arc::new(ArrowSchema::new(vec![field]));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(lists)]).unwrap();
    let schema = Arc::new(beam_schema(&arrow_schema).unwrap());
    let rows = batch_to_rows(&batch, &schema).unwrap();

    let Some(Some(FieldValue::Array(events))) = rows[0].get_value("events") else {
        panic!("events is not an array: {:?}", rows[0]);
    };
    let civil: Vec<_> = events
        .iter()
        .map(|event| match event {
            Some(FieldValue::Row(row)) => row.get_string("civil").unwrap().unwrap().to_string(),
            other => panic!("not a row: {other:?}"),
        })
        .collect();
    assert_eq!(
        civil,
        ["1970-01-01T00:00:00.000000", "1970-01-01T00:00:01.000000"]
    );
}

#[test]
fn other_columns_keep_their_arrow_mapping() {
    let schema = beam_schema(&ArrowSchema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("tags", DataType::new_list(DataType::Utf8, true), true),
    ]))
    .unwrap();
    assert_eq!(
        schema,
        Schema::builder()
            .nullable_field("n", FieldType::int64())
            .nullable_field(
                "tags",
                FieldType::array(FieldType::string().with_nullable(true))
            )
            .build()
    );
}

fn column(name: &str, column_type: &str, mode: &str) -> TableFieldSchema {
    TableFieldSchema::new()
        .set_name(name)
        .set_type(column_type)
        .set_mode(mode)
}

#[test]
fn a_dry_run_schema_maps_like_a_session_schema() {
    let record =
        column("rec", "RECORD", "NULLABLE").set_fields([column("x", "INTEGER", "REQUIRED")]);
    let fields = [
        column("i", "INTEGER", "REQUIRED"),
        column("f", "FLOAT", "NULLABLE"),
        column("b", "BOOLEAN", "NULLABLE"),
        column("s", "STRING", "NULLABLE"),
        column("by", "BYTES", "NULLABLE"),
        column("n", "NUMERIC", "NULLABLE"),
        column("bn", "BIGNUMERIC", "NULLABLE"),
        column("d", "DATE", "NULLABLE"),
        column("dt", "DATETIME", "NULLABLE"),
        column("t", "TIME", "NULLABLE"),
        column("ts", "TIMESTAMP", "NULLABLE"),
        column("g", "GEOGRAPHY", "NULLABLE"),
        column("j", "JSON", ""),
        column("arr", "INTEGER", "REPEATED"),
        record,
    ];
    let nullable = |t: FieldType| t.with_nullable(true);
    assert_eq!(
        table_schema_to_beam_schema(&fields).unwrap(),
        Schema::builder()
            .field("i", FieldType::int64())
            .field("f", nullable(FieldType::double()))
            .field("b", nullable(FieldType::boolean()))
            .field("s", nullable(FieldType::string()))
            .field("by", nullable(FieldType::bytes()))
            .field("n", nullable(decimal_type()))
            .field("bn", nullable(decimal_type()))
            .field("d", nullable(date_type()))
            .field("dt", nullable(FieldType::string()))
            .field("t", nullable(FieldType::string()))
            .field("ts", nullable(micros_instant_type()))
            .field("g", nullable(FieldType::string()))
            .field("j", nullable(FieldType::string()))
            .field("arr", FieldType::array(FieldType::int64()))
            .field(
                "rec",
                nullable(FieldType::row(
                    Schema::builder().field("x", FieldType::int64()).build()
                ))
            )
            .build()
    );
}

#[test]
fn an_unknown_dry_run_type_names_the_column() {
    let error = table_schema_to_beam_schema(&[column("r", "RANGE", "NULLABLE")]).unwrap_err();
    assert!(error.to_string().contains("'r'"), "{error}");
}
