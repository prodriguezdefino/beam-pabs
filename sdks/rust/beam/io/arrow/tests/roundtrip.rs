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

//! Round trips between Beam rows and Arrow record batches.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_io::arrow_array::cast::AsArray;
use arrow_io::arrow_array::types::{Int32Type, TimestampMicrosecondType};
use arrow_io::arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, RecordBatch, UInt8Array,
};
use arrow_io::arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use arrow_io::schema::{date_type, micros_instant_type, millis_instant_type};
use arrow_io::{
    ArrowBridgeError, arrow_to_beam_schema, beam_rows_to_record_batch, record_batch_to_beam_rows,
    record_batch_to_rows, rows_to_record_batch,
};
use beam::schema::{BeamRow, FieldType, FieldValue, Row, Schema, TypeInfo};
use chrono::{DateTime, NaiveDate, Utc};

fn inner_schema() -> Schema {
    Schema::builder()
        .field("x", FieldType::int32())
        .nullable_field("y", FieldType::string())
        .build()
}

fn full_schema() -> Arc<Schema> {
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
            .nullable_field("strs", FieldType::array(FieldType::string()))
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
    FieldValue::Row(
        Row::new(
            Arc::new(repr.clone()),
            vec![
                Some(FieldValue::Int64(seconds)),
                Some(FieldValue::Int64(micros)),
            ],
        )
        .unwrap(),
    )
}

fn inner(x: i32, y: Option<&str>) -> FieldValue {
    FieldValue::Row(
        Row::new(
            Arc::new(inner_schema()),
            vec![Some(FieldValue::Int32(x)), y.map(FieldValue::from)],
        )
        .unwrap(),
    )
}

fn sample_rows(schema: &Arc<Schema>) -> Vec<Row> {
    let full = Row::new(
        Arc::clone(schema),
        vec![
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
                Some(FieldValue::Int64(3)),
            ])),
            Some(FieldValue::Array(vec![Some(FieldValue::String(
                "a".into(),
            ))])),
            Some(FieldValue::Array(vec![Some(FieldValue::Int32(9))])),
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
            Some(FieldValue::String("custom".into())),
        ],
    )
    .unwrap();
    let sparse = Row::new(
        Arc::clone(schema),
        vec![
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
            None,
            Some(FieldValue::Array(vec![])),
            None,
            None,
            Some(FieldValue::Array(vec![])),
            Some(instant(schema, 0, 0)),
            None,
            Some(FieldValue::Int64(-1)),
            Some(FieldValue::String(String::new())),
        ],
    )
    .unwrap();
    vec![full, sparse]
}

#[test]
fn nested_nullable_round_trip() {
    let schema = full_schema();
    let rows = sample_rows(&schema);
    let batch = rows_to_record_batch(&schema, &rows).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), schema.num_fields());
    assert_eq!(
        batch.column_by_name("ts").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    let ts = batch
        .column_by_name("ts")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    assert_eq!(ts.value(0), -1);

    let back = record_batch_to_rows(&batch, &schema).unwrap();
    assert_eq!(back, rows);

    // The Arrow schema alone reproduces the Beam schema.
    assert_eq!(&arrow_to_beam_schema(&batch.schema()).unwrap(), &*schema);
}

#[test]
fn empty_batch_and_empty_schema() {
    let schema = full_schema();
    let batch = rows_to_record_batch(&schema, &[]).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert!(record_batch_to_rows(&batch, &schema).unwrap().is_empty());

    let empty = Arc::new(Schema::new(vec![]));
    let rows = vec![Row::new(Arc::clone(&empty), vec![]).unwrap(); 3];
    let batch = rows_to_record_batch(&empty, &rows).unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(record_batch_to_rows(&batch, &empty).unwrap(), rows);
}

#[test]
fn null_in_non_nullable_field_is_rejected() {
    let schema = Arc::new(Schema::builder().field("x", FieldType::int32()).build());
    let row = Row::new(Arc::clone(&schema), vec![None]).unwrap();
    assert!(matches!(
        rows_to_record_batch(&schema, &[row]),
        Err(ArrowBridgeError::UnexpectedNull { .. })
    ));

    let arrow = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "x",
        DataType::Int32,
        true,
    )]));
    let batch = RecordBatch::try_new(
        arrow,
        vec![Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef],
    )
    .unwrap();
    assert!(matches!(
        record_batch_to_rows(&batch, &schema),
        Err(ArrowBridgeError::UnexpectedNull { .. })
    ));
}

#[test]
fn type_mismatch_is_reported() {
    let schema = Arc::new(Schema::builder().field("x", FieldType::int32()).build());
    let row = Row::new(Arc::clone(&schema), vec![Some(FieldValue::from("nope"))]).unwrap();
    assert!(matches!(
        rows_to_record_batch(&schema, &[row]),
        Err(ArrowBridgeError::TypeMismatch { .. })
    ));
}

#[test]
fn lenient_reads_widen_and_narrow_with_range_checks() {
    let arrow = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("small", DataType::UInt8, false),
        ArrowField::new("wide", DataType::Int64, false),
        ArrowField::new("name", DataType::LargeUtf8, true),
        ArrowField::new("extra", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow,
        vec![
            Arc::new(UInt8Array::from(vec![200u8])) as ArrayRef,
            Arc::new(Int64Array::from(vec![42i64])),
            Arc::new(LargeStringArray::from(vec![Some("n")])),
            Arc::new(Int32Array::from(vec![0])),
        ],
    )
    .unwrap();

    // Columns matched by name; extra columns ignored; missing nullable -> null.
    let target = Arc::new(
        Schema::builder()
            .field("wide", FieldType::int32())
            .field("small", FieldType::int16())
            .nullable_field("name", FieldType::string())
            .nullable_field("absent", FieldType::double())
            .build(),
    );
    let rows = record_batch_to_rows(&batch, &target).unwrap();
    assert_eq!(
        rows[0].values(),
        &[
            Some(FieldValue::Int32(42)),
            Some(FieldValue::Int16(200)),
            Some(FieldValue::String("n".into())),
            None,
        ]
    );

    // 200 does not fit a BYTE.
    let narrow = Arc::new(Schema::builder().field("small", FieldType::byte()).build());
    assert!(matches!(
        record_batch_to_rows(&batch, &narrow),
        Err(ArrowBridgeError::OutOfRange { .. })
    ));

    // A missing non-nullable field is an error.
    let missing = Arc::new(
        Schema::builder()
            .field("absent", FieldType::int32())
            .build(),
    );
    assert!(matches!(
        record_batch_to_rows(&batch, &missing),
        Err(ArrowBridgeError::MissingField(_))
    ));
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Address {
    city: String,
    zip: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Person {
    id: i64,
    name: String,
    age: Option<i16>,
    score: f64,
    tags: Vec<String>,
    attrs: BTreeMap<String, i64>,
    home: Address,
    previous: Option<Address>,
    born: NaiveDate,
    updated: DateTime<Utc>,
    #[beam(bytes)]
    avatar: Vec<u8>,
}

fn people() -> Vec<Person> {
    vec![
        Person {
            id: 1,
            name: "Ada".into(),
            age: Some(36),
            score: 99.5,
            tags: vec!["math".into(), "code".into()],
            attrs: BTreeMap::from([("k".to_string(), 1)]),
            home: Address {
                city: "London".into(),
                zip: None,
            },
            previous: Some(Address {
                city: "Paris".into(),
                zip: Some(75_000),
            }),
            born: NaiveDate::from_ymd_opt(1815, 12, 10).unwrap(),
            updated: DateTime::from_timestamp(1_700_000_000, 123_456_000).unwrap(),
            avatar: vec![1, 2, 3],
        },
        Person {
            id: 2,
            name: "Grace".into(),
            age: None,
            score: -1.0,
            tags: vec![],
            attrs: BTreeMap::new(),
            home: Address {
                city: "NYC".into(),
                zip: Some(10_001),
            },
            previous: None,
            born: NaiveDate::from_ymd_opt(1906, 12, 9).unwrap(),
            updated: DateTime::from_timestamp(-10, 1_000).unwrap(),
            avatar: vec![],
        },
    ]
}

#[test]
fn derive_beam_row_round_trip() {
    let people = people();
    let batch = beam_rows_to_record_batch(&people).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch.column_by_name("born").unwrap().data_type(),
        &DataType::Date32
    );
    let ages = batch.column_by_name("age").unwrap();
    assert_eq!(ages.null_count(), 1);
    let back: Vec<Person> = record_batch_to_beam_rows(&batch).unwrap();
    assert_eq!(back, people);

    // The derived schema survives an Arrow schema round trip.
    assert_eq!(
        &arrow_to_beam_schema(&batch.schema()).unwrap(),
        &**Person::beam_schema()
    );
}

#[test]
fn nested_struct_under_null_parent_is_masked() {
    let people = people();
    let batch = beam_rows_to_record_batch(&people).unwrap();
    let previous = batch.column_by_name("previous").unwrap().as_struct();
    assert!(previous.is_null(1));
    // The non-nullable `city` child carries a masked slot under the null parent.
    assert_eq!(previous.column_by_name("city").unwrap().len(), 2);
    let zips = previous
        .column_by_name("zip")
        .unwrap()
        .as_primitive::<Int32Type>();
    assert_eq!(zips.value(0), 75_000);
}
