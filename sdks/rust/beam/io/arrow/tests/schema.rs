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

//! Beam schema ⇄ Arrow schema mapping, and logical-type value conversions.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_io::arrow_array::{
    Array, ArrayRef, BinaryArray, Date64Array, Decimal128Array, Decimal256Array, Int64Array,
    RecordBatch, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_io::arrow_buffer::i256;
use arrow_io::arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use arrow_io::schema::{
    META_LOGICAL_PAYLOAD, META_LOGICAL_URN, date_type, decimal_type, micros_instant_type,
    millis_instant_type,
};
use arrow_io::{
    ArrowBridgeError, arrow_to_beam_schema, beam_to_arrow_schema, record_batch_to_rows,
};
use beam::schema::{BeamField, FieldType, FieldValue, Schema, TypeInfo};
use chrono::{DateTime, NaiveDate, Utc};

#[test]
fn beam_schema_maps_to_arrow_and_back() {
    let inner = Schema::builder()
        .field("x", FieldType::int32())
        .nullable_field("y", FieldType::string())
        .build();
    let schema = Schema::builder()
        .field("b", FieldType::byte())
        .field("s", FieldType::int16())
        .field("i", FieldType::int32())
        .field("l", FieldType::int64())
        .field("f", FieldType::float())
        .field("d", FieldType::double())
        .nullable_field("str", FieldType::string())
        .field("bool", FieldType::boolean())
        .field("bytes", FieldType::bytes())
        .field(
            "arr",
            FieldType::array(FieldType::int64().with_nullable(true)),
        )
        .field(
            "it",
            FieldType::new(TypeInfo::Iterable(Box::new(FieldType::string())), false),
        )
        .field(
            "map",
            FieldType::map(FieldType::string(), FieldType::double()),
        )
        .nullable_field("row", FieldType::row(inner))
        .field("ts", micros_instant_type())
        .field("ms", millis_instant_type())
        .nullable_field("date", date_type())
        .field("dec", decimal_type())
        .field(
            "custom",
            FieldType::logical(
                "beam:logical_type:custom:v1",
                vec![1, 0xab],
                FieldType::string(),
            ),
        )
        .build();
    let arrow = beam_to_arrow_schema(&schema).unwrap();
    let atomic: Vec<_> = arrow.fields()[..9]
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    assert_eq!(
        atomic,
        vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Boolean,
            DataType::Binary,
        ]
    );
    assert!(arrow.field(6).is_nullable());
    assert!(!arrow.field(0).is_nullable());
    assert_eq!(
        arrow.field_with_name("ts").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        arrow.field_with_name("ms").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
    );
    assert_eq!(
        arrow.field_with_name("date").unwrap().data_type(),
        &DataType::Date32
    );
    assert_eq!(
        arrow.field_with_name("dec").unwrap().data_type(),
        &DataType::Binary
    );
    assert!(matches!(
        arrow.field_with_name("map").unwrap().data_type(),
        DataType::Map(_, false)
    ));
    let custom = arrow.field_with_name("custom").unwrap();
    assert_eq!(custom.data_type(), &DataType::Utf8);
    assert_eq!(
        custom.metadata().get(META_LOGICAL_URN).map(String::as_str),
        Some("beam:logical_type:custom:v1")
    );
    assert_eq!(
        custom
            .metadata()
            .get(META_LOGICAL_PAYLOAD)
            .map(String::as_str),
        Some("01ab")
    );
    assert_eq!(arrow_to_beam_schema(&arrow).unwrap(), schema);
}

#[test]
fn logical_helpers_match_the_beam_schema_model() {
    assert_eq!(micros_instant_type(), DateTime::<Utc>::beam_field_type());
    assert_eq!(date_type(), NaiveDate::beam_field_type());
}

#[test]
fn widening_from_arrow() {
    let arrow = ArrowSchema::new(vec![
        ArrowField::new("u8", DataType::UInt8, false),
        ArrowField::new("u32", DataType::UInt32, false),
        ArrowField::new("big", DataType::LargeUtf8, true),
        ArrowField::new("ns", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        ArrowField::new("dec", DataType::Decimal128(10, 2), false),
        ArrowField::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]);
    let beam = arrow_to_beam_schema(&arrow).unwrap();
    assert_eq!(beam.fields[0].field_type, FieldType::int16());
    assert_eq!(beam.fields[1].field_type, FieldType::int64());
    assert_eq!(
        beam.fields[2].field_type,
        FieldType::string().with_nullable(true)
    );
    assert_eq!(beam.fields[3].field_type, micros_instant_type());
    assert_eq!(beam.fields[4].field_type, decimal_type());
    assert_eq!(
        beam.fields[5].field_type,
        FieldType::string().with_nullable(true)
    );
}

#[test]
fn unsupported_arrow_type() {
    let arrow = ArrowSchema::new(vec![ArrowField::new("u64", DataType::UInt64, false)]);
    assert!(matches!(
        arrow_to_beam_schema(&arrow),
        Err(ArrowBridgeError::UnsupportedType { .. })
    ));
}

#[test]
fn malformed_logical_payload_is_rejected() {
    let field = ArrowField::new("x", DataType::Utf8, false).with_metadata(HashMap::from([
        (
            META_LOGICAL_URN.to_string(),
            "beam:logical_type:x:v1".to_string(),
        ),
        (META_LOGICAL_PAYLOAD.to_string(), "zzz".to_string()),
    ]));
    assert!(matches!(
        arrow_to_beam_schema(&ArrowSchema::new(vec![field])),
        Err(ArrowBridgeError::UnsupportedType { .. })
    ));
}

fn single_column(name: &str, array: ArrayRef) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        name,
        array.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

#[test]
fn timestamps_of_any_unit_become_instants() {
    let batch = single_column(
        "t",
        Arc::new(TimestampNanosecondArray::from(vec![
            Some(-1),
            Some(1_999),
            None,
        ])),
    );
    let schema = Arc::new(
        Schema::builder()
            .nullable_field("t", micros_instant_type())
            .build(),
    );
    let rows = record_batch_to_rows(&batch, &schema).unwrap();
    let decoded: Vec<Option<DateTime<Utc>>> = rows
        .iter()
        .map(|r| Option::<DateTime<Utc>>::from_field_value(r.values()[0].as_ref()).unwrap())
        .collect();
    // Nanoseconds are floored to microseconds.
    assert_eq!(
        decoded,
        vec![
            Some(DateTime::from_timestamp(-1, 999_999_000).unwrap()),
            Some(DateTime::from_timestamp(0, 1_000).unwrap()),
            None,
        ]
    );

    let batch = single_column("t", Arc::new(TimestampSecondArray::from(vec![Some(2)])));
    let schema = Arc::new(Schema::builder().field("t", millis_instant_type()).build());
    let rows = record_batch_to_rows(&batch, &schema).unwrap();
    assert_eq!(rows[0].values()[0], Some(FieldValue::Int64(2_000)));
}

#[test]
fn date64_becomes_days() {
    let batch = single_column(
        "d",
        Arc::new(Date64Array::from(vec![Some(86_400_000 * 3), Some(-1)])),
    );
    let schema = Arc::new(Schema::builder().field("d", date_type()).build());
    let rows = record_batch_to_rows(&batch, &schema).unwrap();
    assert_eq!(rows[0].values()[0], Some(FieldValue::Int64(3)));
    assert_eq!(rows[1].values()[0], Some(FieldValue::Int64(-1)));
}

#[test]
fn decimal128_becomes_beam_decimal() {
    let array = Decimal128Array::from(vec![Some(12_345i128), Some(-129), Some(0)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let batch = single_column("amount", Arc::new(array));
    let schema = Arc::new(Schema::builder().field("amount", decimal_type()).build());
    let rows = record_batch_to_rows(&batch, &schema).unwrap();
    let bytes: Vec<_> = rows.iter().map(|r| r.values()[0].clone()).collect();
    assert_eq!(
        bytes,
        vec![
            // VarInt(scale=2), VarInt(len=2), 0x3039 = 12345
            Some(FieldValue::Bytes(vec![2, 2, 0x30, 0x39])),
            // -129 = 0xff7f
            Some(FieldValue::Bytes(vec![2, 2, 0xff, 0x7f])),
            Some(FieldValue::Bytes(vec![2, 1, 0x00])),
        ]
    );
}

#[test]
fn decimal256_becomes_beam_decimal() {
    let values = [i256::from_i128(12_345), i256::from_i128(-129), i256::MAX];
    let array = Decimal256Array::from(values.map(Some).to_vec())
        .with_precision_and_scale(76, 38)
        .unwrap();
    let field = ArrowField::new("amount", array.data_type().clone(), false);
    assert_eq!(
        arrow_to_beam_schema(&ArrowSchema::new(vec![field]))
            .unwrap()
            .fields[0]
            .field_type,
        decimal_type()
    );

    let batch = single_column("amount", Arc::new(array));
    let schema = Arc::new(Schema::builder().field("amount", decimal_type()).build());
    let rows = record_batch_to_rows(&batch, &schema).unwrap();
    let bytes: Vec<_> = rows.iter().map(|r| r.values()[0].clone()).collect();
    let mut max = vec![38, 32, 0x7f];
    max.extend([0xff; 31]);
    assert_eq!(
        bytes,
        vec![
            Some(FieldValue::Bytes(vec![38, 2, 0x30, 0x39])),
            Some(FieldValue::Bytes(vec![38, 2, 0xff, 0x7f])),
            Some(FieldValue::Bytes(max)),
        ]
    );
}

fn decode_one(
    array: ArrayRef,
    field_type: FieldType,
) -> Result<Option<FieldValue>, ArrowBridgeError> {
    let batch = single_column("t", array);
    let schema = Arc::new(Schema::builder().nullable_field("t", field_type).build());
    Ok(record_batch_to_rows(&batch, &schema)?[0].values()[0].clone())
}

fn instant(value: Option<FieldValue>) -> DateTime<Utc> {
    DateTime::<Utc>::from_field_value(value.as_ref()).unwrap()
}

#[test]
fn timestamp_units_scale_to_instant_precision() {
    // (array, expected micros_instant in micros, expected millis_instant); negatives floor.
    let cases: [(ArrayRef, i64, i64); 5] = [
        (
            Arc::new(TimestampSecondArray::from(vec![2])),
            2_000_000,
            2_000,
        ),
        (
            Arc::new(TimestampMillisecondArray::from(vec![-1])),
            -1_000,
            -1,
        ),
        (Arc::new(TimestampMicrosecondArray::from(vec![-1])), -1, -1),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![1_500])),
            1_500,
            1,
        ),
        (
            Arc::new(TimestampNanosecondArray::from(vec![1_500_000])),
            1_500,
            1,
        ),
    ];
    for (array, micros, millis) in cases {
        let unit = array.data_type().clone();
        assert_eq!(
            instant(decode_one(Arc::clone(&array), micros_instant_type()).unwrap()),
            DateTime::from_timestamp_micros(micros).unwrap(),
            "{unit:?} -> micros"
        );
        assert_eq!(
            decode_one(array, millis_instant_type()).unwrap(),
            Some(FieldValue::Int64(millis)),
            "{unit:?} -> millis"
        );
    }

    let overflow: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MAX]));
    assert!(matches!(
        decode_one(Arc::clone(&overflow), micros_instant_type()),
        Err(ArrowBridgeError::OutOfRange { .. })
    ));
    assert!(matches!(
        decode_one(overflow, millis_instant_type()),
        Err(ArrowBridgeError::OutOfRange { .. })
    ));
}

#[test]
fn logical_fields_also_accept_their_representation() {
    let seconds = Arc::new(ArrowField::new("seconds", DataType::Int64, false));
    let micros = Arc::new(ArrowField::new("micros", DataType::Int64, false));
    let row: ArrayRef = Arc::new(StructArray::from(vec![
        (seconds, Arc::new(Int64Array::from(vec![-1])) as ArrayRef),
        (
            micros,
            Arc::new(Int64Array::from(vec![999_999])) as ArrayRef,
        ),
    ]));
    assert_eq!(
        instant(decode_one(row, micros_instant_type()).unwrap()),
        DateTime::from_timestamp_micros(-1).unwrap()
    );
    assert_eq!(
        decode_one(Arc::new(Int64Array::from(vec![-7])), millis_instant_type()).unwrap(),
        Some(FieldValue::Int64(-7))
    );
    assert_eq!(
        decode_one(
            Arc::new(BinaryArray::from(vec![&[2u8, 1, 0x05][..]])),
            decimal_type()
        )
        .unwrap(),
        Some(FieldValue::Bytes(vec![2, 1, 0x05]))
    );
}
