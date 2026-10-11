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

//! `#[derive(BeamRow)]` values to and from Arrow with no `Row` between them.

use std::sync::Arc;

use beam::schema::{BeamRow, FieldType, FieldValue, Row, Schema};
use beam_io_arrow::arrow_array::cast::AsArray;
use beam_io_arrow::arrow_array::types::{Int32Type, Int64Type, TimestampMicrosecondType};
use beam_io_arrow::arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray,
};
use beam_io_arrow::arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use beam_io_arrow::{RowBatchBuilder, record_batch_to_beam_rows, rows_to_record_batch};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Address {
    city: String,
    zip: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Resident {
    id: i64,
    home: Address,
    previous: Option<Address>,
}

fn residents() -> Vec<Resident> {
    vec![
        Resident {
            id: 1,
            home: Address {
                city: "Oslo".into(),
                zip: Some(150),
            },
            previous: None,
        },
        Resident {
            id: 2,
            home: Address {
                city: "Lima".into(),
                zip: None,
            },
            previous: Some(Address {
                city: "Quito".into(),
                zip: Some(170_150),
            }),
        },
    ]
}

/// The batch that `append_row` builds from the `Row` of each value.
fn batch_of_rows<T: BeamRow>(values: &[T]) -> RecordBatch {
    let mut builder = RowBatchBuilder::new(&T::beam_schema()).unwrap();
    values
        .iter()
        .for_each(|value| builder.append_row(&value.to_row().unwrap()).unwrap());
    builder.finish().unwrap()
}

/// The batch that `append_beam_row` builds from the values.
fn batch_of_values<T: BeamRow>(values: &[T]) -> RecordBatch {
    let mut builder = RowBatchBuilder::new(&T::beam_schema()).unwrap();
    values
        .iter()
        .for_each(|value| builder.append_beam_row(value).unwrap());
    assert_eq!(builder.len(), values.len());
    builder.finish().unwrap()
}

#[test]
fn append_beam_row_builds_the_batch_of_append_row() {
    let residents = residents();
    let batch = batch_of_values(&residents);
    assert_eq!(batch, batch_of_rows(&residents));
    assert_eq!(
        record_batch_to_beam_rows::<Resident>(&batch).unwrap(),
        residents
    );
}

#[test]
fn append_beam_row_masks_a_null_nested_row() {
    let batch = batch_of_values(&residents());
    let previous = batch.column_by_name("previous").unwrap().as_struct();
    assert!(previous.is_null(0));
    assert!(!previous.is_null(1));
    assert_eq!(previous.column_by_name("city").unwrap().len(), 2);
    let zips = previous
        .column_by_name("zip")
        .unwrap()
        .as_primitive::<Int32Type>();
    assert_eq!(zips.value(1), 170_150);
}

#[test]
fn append_beam_row_starts_again_after_finish() {
    let residents = residents();
    let mut builder = RowBatchBuilder::new(&Resident::beam_schema()).unwrap();
    builder.append_beam_row(&residents[0]).unwrap();
    let first = builder.finish().unwrap();
    assert!(builder.is_empty());
    builder.append_beam_row(&residents[1]).unwrap();
    let second = builder.finish().unwrap();
    assert_eq!(first, batch_of_rows(&residents[..1]));
    assert_eq!(second, batch_of_rows(&residents[1..]));
}

#[test]
fn append_beam_row_fails_for_a_value_of_another_schema() {
    let mut builder = RowBatchBuilder::new(&Address::beam_schema()).unwrap();
    assert!(builder.append_beam_row(&residents()[0]).is_err());
}

/// The wire order is `name`, `count`, `id`, not the field order.
#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Positioned {
    #[beam(encoding_position = 2)]
    id: i64,
    #[beam(encoding_position = 0)]
    name: String,
    #[beam(encoding_position = 1)]
    count: Option<i32>,
}

#[test]
fn derived_encoding_positions_fill_the_columns_of_their_fields() {
    let values = vec![
        Positioned {
            id: 7,
            name: "a".into(),
            count: Some(3),
        },
        Positioned {
            id: -1,
            name: "b".into(),
            count: None,
        },
    ];
    let batch = batch_of_values(&values);
    assert_eq!(batch, batch_of_rows(&values));
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>();
    assert_eq!(ids.values(), &[7, -1]);
    assert_eq!(
        record_batch_to_beam_rows::<Positioned>(&batch).unwrap(),
        values
    );
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Tagged<T> {
    tag: String,
    value: T,
    extra: Option<T>,
}

#[test]
fn a_generic_derived_struct_converts_with_the_schema_of_its_parameter() {
    let values = vec![
        Tagged {
            tag: "x".to_string(),
            value: residents()[1].home.clone(),
            extra: None,
        },
        Tagged {
            tag: "y".to_string(),
            value: residents()[0].home.clone(),
            extra: residents()[1].previous.clone(),
        },
    ];
    let batch = batch_of_values(&values);
    assert_eq!(batch, batch_of_rows(&values));
    assert_eq!(
        record_batch_to_beam_rows::<Tagged<Address>>(&batch).unwrap(),
        values
    );
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Reading {
    at: DateTime<Utc>,
}

#[test]
fn a_derived_instant_reads_a_column_of_an_instant_with_int32_micros() {
    let representation = Schema::builder()
        .field("seconds", FieldType::int64())
        .field("micros", FieldType::int32())
        .build();
    let schema = Schema::builder()
        .field(
            "at",
            FieldType::logical(
                beam::schema::URN_MICROS_INSTANT,
                Vec::new(),
                FieldType::row(representation.clone()),
            ),
        )
        .build();
    let instant = Row::new(
        Arc::new(representation),
        vec![
            Some(FieldValue::Int64(1_714_566_600)),
            Some(FieldValue::Int32(250_000)),
        ],
    )
    .unwrap();
    let row = Row::new(
        Arc::new(schema.clone()),
        vec![Some(FieldValue::Row(instant))],
    )
    .unwrap();
    let batch = rows_to_record_batch(&schema, &[row]).unwrap();
    let expected = Reading {
        at: DateTime::from_timestamp(1_714_566_600, 250_000_000).unwrap(),
    };
    assert_eq!(
        record_batch_to_beam_rows::<Reading>(&batch).unwrap(),
        std::slice::from_ref(&expected)
    );
    // The derived type writes the same instant.
    let micros = |batch: &RecordBatch| {
        batch
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0)
    };
    assert_eq!(micros(&batch_of_values(&[expected])), micros(&batch));
}

#[test]
fn derived_values_read_a_batch_with_reordered_and_extra_columns() {
    let arrow = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("unused", DataType::Int64, false),
        ArrowField::new("zip", DataType::Int32, true),
        ArrowField::new("city", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow,
        vec![
            Arc::new(Int64Array::from(vec![9, 9])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(150), None])),
            Arc::new(StringArray::from(vec!["Oslo", "Lima"])),
        ],
    )
    .unwrap();
    let homes: Vec<Address> = residents().into_iter().map(|r| r.home).collect();
    assert_eq!(record_batch_to_beam_rows::<Address>(&batch).unwrap(), homes);
}
