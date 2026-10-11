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

//! Arrow layouts that our own writer never produces but files written by other
//! tools do: dictionary-encoded columns, `LargeList`, `FixedSizeList` and
//! unsigned 64-bit integers. Each test builds the Arrow array by hand and checks
//! the exact Beam values (or error) produced.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::sync::Arc;

use arrow_io::arrow_array::types::{Int8Type, Int32Type, Int64Type};
use arrow_io::arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, Int64Array, LargeListArray,
    RecordBatch, StringArray, UInt64Array,
};
use arrow_io::arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use arrow_io::{ArrowBridgeError, array_to_values, arrow_to_beam_schema, record_batch_to_rows};
use beam::schema::{FieldType, FieldValue, Schema};

fn batch_of(name: &str, array: ArrayRef) -> RecordBatch {
    let field = ArrowField::new(name, array.data_type().clone(), array.null_count() > 0);
    RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![field])), vec![array]).unwrap()
}

fn column(schema: Schema, batch: &RecordBatch) -> Vec<Option<FieldValue>> {
    record_batch_to_rows(batch, &Arc::new(schema))
        .unwrap()
        .into_iter()
        .map(|row| row.values()[0].clone())
        .collect()
}

fn s(v: &str) -> Option<FieldValue> {
    Some(FieldValue::String(v.to_string()))
}

#[test]
fn dictionary_encoded_strings_decode_to_their_values() {
    // Keys: 1, 0, null, 1, 2 over the dictionary ["red", "green", "blue"].
    let dict: DictionaryArray<Int32Type> = vec![
        Some("green"),
        Some("red"),
        None,
        Some("green"),
        Some("blue"),
    ]
    .into_iter()
    .collect();
    let batch = batch_of("color", Arc::new(dict));
    // The schema of a dictionary column is that of its values.
    assert_eq!(
        arrow_to_beam_schema(&batch.schema()).unwrap(),
        Schema::builder()
            .nullable_field("color", FieldType::string())
            .build()
    );
    let schema = Schema::builder()
        .nullable_field("color", FieldType::string())
        .build();
    assert_eq!(
        column(schema, &batch),
        vec![s("green"), s("red"), None, s("green"), s("blue")]
    );
}

#[test]
fn dictionary_with_null_value_entry_yields_null() {
    // Key 1 is valid but points at a null dictionary value.
    let keys = Int8Array::from(vec![0, 1, 0, 1]);
    let values = Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef;
    let dict = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();
    let nullable = FieldType::string().with_nullable(true);
    assert_eq!(
        array_to_values("d", &nullable, &dict).unwrap(),
        vec![s("x"), None, s("x"), None]
    );

    // Into a non-nullable field, that null is rejected with the field name.
    let batch = batch_of("d", Arc::new(dict));
    let strict = Arc::new(Schema::builder().field("d", FieldType::string()).build());
    match record_batch_to_rows(&batch, &strict) {
        Err(ArrowBridgeError::UnexpectedNull { field }) => assert_eq!(field, "d"),
        other => panic!("expected UnexpectedNull, got {other:?}"),
    }
}

#[test]
fn dictionary_of_integers_is_narrowed_per_value() {
    let keys = Int8Array::from(vec![Some(2), Some(0), None, Some(1)]);
    let values = Arc::new(Int64Array::from(vec![-7, 300, 40_000])) as ArrayRef;
    let dict = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();

    let int32 = FieldType::int32().with_nullable(true);
    assert_eq!(
        array_to_values("n", &int32, &dict).unwrap(),
        vec![
            Some(FieldValue::Int32(40_000)),
            Some(FieldValue::Int32(-7)),
            None,
            Some(FieldValue::Int32(300)),
        ]
    );

    // 40_000 does not fit an INT16: the dictionary values are range-checked too.
    let int16 = FieldType::int16().with_nullable(true);
    match array_to_values("n", &int16, &dict) {
        Err(ArrowBridgeError::OutOfRange {
            field,
            value,
            target,
        }) => {
            assert_eq!(
                (field.as_str(), value.as_str(), target.as_str()),
                ("n", "40000", "INT16")
            );
        }
        other => panic!("expected OutOfRange, got {other:?}"),
    }
}

#[test]
fn large_list_decodes_like_list() {
    let list = LargeListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        None,
        Some(vec![]),
        Some(vec![Some(-4), None]),
    ]);
    let batch = batch_of("xs", Arc::new(list));
    let elem = FieldType::int64().with_nullable(true);
    assert_eq!(
        arrow_to_beam_schema(&batch.schema()).unwrap(),
        Schema::builder()
            .nullable_field("xs", FieldType::array(elem.clone()))
            .build()
    );
    let schema = Schema::builder()
        .nullable_field("xs", FieldType::array(elem))
        .build();
    let i = |v: i64| Some(FieldValue::Int64(v));
    assert_eq!(
        column(schema, &batch),
        vec![
            Some(FieldValue::Array(vec![i(1), i(2), i(3)])),
            None,
            Some(FieldValue::Array(vec![])),
            Some(FieldValue::Array(vec![i(-4), None])),
        ]
    );
}

#[test]
fn large_list_null_element_into_non_nullable_element_type_fails() {
    let list =
        LargeListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1), None])]);
    let ft = FieldType::array(FieldType::int64());
    match array_to_values("xs", &ft, &list) {
        Err(ArrowBridgeError::UnexpectedNull { field }) => assert_eq!(field, "xs[]"),
        other => panic!("expected UnexpectedNull, got {other:?}"),
    }
}

#[test]
fn fixed_size_list_decodes_including_sliced_arrays() {
    let list = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(5), Some(6)]),
            Some(vec![Some(7), None]),
        ],
        2,
    );
    let elem = FieldType::int32().with_nullable(true);
    let ft = FieldType::array(elem.clone()).with_nullable(true);
    let i = |v: i32| Some(FieldValue::Int32(v));
    assert_eq!(
        array_to_values("pair", &ft, &list).unwrap(),
        vec![
            Some(FieldValue::Array(vec![i(1), i(2)])),
            None,
            Some(FieldValue::Array(vec![i(5), i(6)])),
            Some(FieldValue::Array(vec![i(7), None])),
        ]
    );

    // A slice must start at its own first element, not the parent's.
    let sliced = list.slice(2, 2);
    assert_eq!(
        array_to_values("pair", &ft, &sliced).unwrap(),
        vec![
            Some(FieldValue::Array(vec![i(5), i(6)])),
            Some(FieldValue::Array(vec![i(7), None])),
        ]
    );

    let batch = batch_of("pair", Arc::new(list));
    assert_eq!(
        arrow_to_beam_schema(&batch.schema()).unwrap(),
        Schema::builder()
            .nullable_field("pair", FieldType::array(elem))
            .build()
    );
}

#[test]
fn uint64_within_range_widens_and_overflow_is_reported() {
    let fits = UInt64Array::from(vec![Some(0), None, Some(i64::MAX as u64)]);
    let ft = FieldType::int64().with_nullable(true);
    assert_eq!(
        array_to_values("u", &ft, &fits).unwrap(),
        vec![
            Some(FieldValue::Int64(0)),
            None,
            Some(FieldValue::Int64(i64::MAX)),
        ]
    );

    let overflow = UInt64Array::from(vec![1, u64::MAX]);
    match array_to_values("u", &ft, &overflow) {
        Err(ArrowBridgeError::OutOfRange {
            field,
            value,
            target,
        }) => {
            assert_eq!(field, "u");
            assert_eq!(value, u64::MAX.to_string());
            assert_eq!(target, "INT64");
        }
        other => panic!("expected OutOfRange, got {other:?}"),
    }

    // Even when the target is narrower, the INT64 overflow is reported first.
    let batch = batch_of(
        "u",
        Arc::new(UInt64Array::from(vec![(i64::MAX as u64) + 1])),
    );
    let narrow = Arc::new(Schema::builder().field("u", FieldType::int32()).build());
    match record_batch_to_rows(&batch, &narrow) {
        Err(ArrowBridgeError::OutOfRange { value, target, .. }) => {
            assert_eq!(value, "9223372036854775808");
            assert_eq!(target, "INT64");
        }
        other => panic!("expected OutOfRange, got {other:?}"),
    }
    assert_eq!(batch.column(0).data_type(), &DataType::UInt64);
}
