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

//! Tests for the decoding of a configuration row: integer narrowing and widening, also in
//! nested rows and lists.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::sync::Arc;

use beam::schema::{AtomicType, Field, FieldType, FieldValue, Row, Schema, TypeInfo};
use expansion::config::decode_config;

fn field(name: &str, type_info: TypeInfo) -> Field {
    Field::new(name, FieldType::new(type_info, true))
}

fn int(atomic: AtomicType) -> TypeInfo {
    TypeInfo::Atomic(atomic)
}

fn row_type(fields: Vec<Field>) -> TypeInfo {
    TypeInfo::Row(Schema::new(fields))
}

fn list_of(type_info: TypeInfo) -> TypeInfo {
    TypeInfo::Array(Box::new(FieldType::new(type_info, true)))
}

fn row(schema: &Schema, values: Vec<Option<FieldValue>>) -> Row {
    Row::new(Arc::new(schema.clone()), values).unwrap()
}

/// Encodes `value` under the caller schema `{name: provided}` and decodes it into the
/// target schema `{name: expected}`.
fn decode(
    name: &str,
    expected: TypeInfo,
    provided: TypeInfo,
    value: FieldValue,
) -> Result<Row, String> {
    let target = Schema::new(vec![field(name, expected)]);
    let caller = Schema::new(vec![field(name, provided)]);
    let bytes = row(&caller, vec![Some(value)]).to_row_bytes().unwrap();
    decode_config(target, Some(caller.into()), &bytes)
}

#[test]
fn int64_in_range_narrows_to_int32() {
    let row = decode(
        "num_streams",
        int(AtomicType::Int32),
        int(AtomicType::Int64),
        FieldValue::Int64(42),
    )
    .unwrap();
    assert_eq!(
        row.get_value("num_streams"),
        Some(&Some(FieldValue::Int32(42)))
    );
}

#[test]
fn smaller_integers_widen_to_int64() {
    let row = decode(
        "n",
        int(AtomicType::Int64),
        int(AtomicType::Int16),
        FieldValue::Int16(-7),
    )
    .unwrap();
    assert_eq!(row.get_value("n"), Some(&Some(FieldValue::Int64(-7))));
}

#[test]
fn out_of_range_integer_names_the_field() {
    for (target, value) in [
        (AtomicType::Int32, 3_000_000_000_i64),
        (AtomicType::Int32, i64::from(i32::MIN) - 1),
        (AtomicType::Int16, 40_000),
        (AtomicType::Byte, 128),
    ] {
        let err = decode(
            "num_streams",
            int(target),
            int(AtomicType::Int64),
            FieldValue::Int64(value),
        )
        .unwrap_err();
        assert!(
            err.contains(&format!(
                "field 'num_streams' value {value} is out of range for {target}"
            )),
            "err: {err}"
        );
    }
}

#[test]
fn non_integer_type_for_integer_field_is_rejected() {
    let err = decode(
        "num_streams",
        int(AtomicType::Int32),
        int(AtomicType::String),
        FieldValue::String("4".to_string()),
    )
    .unwrap_err();
    assert!(
        err.contains("field 'num_streams' has type STRING"),
        "err: {err}"
    );
}

#[test]
fn nested_row_narrows_and_names_the_nested_field() {
    let target = row_type(vec![field("num", int(AtomicType::Int32))]);
    let caller_fields = vec![field("num", int(AtomicType::Int64))];
    let caller_schema = Schema::new(caller_fields.clone());
    let nested = |v| FieldValue::Row(row(&caller_schema, vec![Some(FieldValue::Int64(v))]));

    let ok = decode(
        "outer",
        target.clone(),
        row_type(caller_fields.clone()),
        nested(5),
    )
    .unwrap();
    let Some(Some(FieldValue::Row(inner))) = ok.get_value("outer") else {
        panic!("outer is not a row: {ok:?}");
    };
    assert_eq!(inner.get_value("num"), Some(&Some(FieldValue::Int32(5))));

    let err = decode(
        "outer",
        target,
        row_type(caller_fields),
        nested(3_000_000_000),
    )
    .unwrap_err();
    assert!(
        err.contains("field 'outer.num' value 3000000000 is out of range for INT32"),
        "err: {err}"
    );
}

#[test]
fn list_narrows_each_element_and_names_the_field() {
    let ok = decode(
        "sizes",
        list_of(int(AtomicType::Int32)),
        list_of(int(AtomicType::Int64)),
        FieldValue::Array(vec![Some(FieldValue::Int64(1)), None]),
    )
    .unwrap();
    assert_eq!(
        ok.get_value("sizes"),
        Some(&Some(FieldValue::Array(vec![
            Some(FieldValue::Int32(1)),
            None
        ])))
    );

    let err = decode(
        "sizes",
        list_of(int(AtomicType::Int32)),
        list_of(int(AtomicType::Int64)),
        FieldValue::Array(vec![Some(FieldValue::Int64(i64::MAX))]),
    )
    .unwrap_err();
    assert!(
        err.contains(&format!(
            "field 'sizes' value {} is out of range for INT32",
            i64::MAX
        )),
        "err: {err}"
    );
}

#[test]
fn list_of_rows_names_the_nested_field() {
    let caller_fields = vec![field("num", int(AtomicType::Int64))];
    let caller_schema = Schema::new(caller_fields.clone());
    let element = row(&caller_schema, vec![Some(FieldValue::Int64(3_000_000_000))]);
    let err = decode(
        "list",
        list_of(row_type(vec![field("num", int(AtomicType::Int32))])),
        list_of(row_type(caller_fields)),
        FieldValue::Array(vec![Some(FieldValue::Row(element))]),
    )
    .unwrap_err();
    assert!(
        err.contains("field 'list.num' value 3000000000 is out of range for INT32"),
        "err: {err}"
    );
}

#[test]
fn capitalized_caller_field_matches_lowercase_target_field() {
    let target = Schema::new(vec![field("data", TypeInfo::Atomic(AtomicType::String))]);
    let caller = Schema::new(vec![field("Data", TypeInfo::Atomic(AtomicType::String))]);
    let bytes = row(&caller, vec![Some(FieldValue::String("0".to_string()))])
        .to_row_bytes()
        .unwrap();
    let decoded = decode_config(target, Some(caller.into()), &bytes).unwrap();
    assert_eq!(
        decoded.get_value("data"),
        Some(&Some(FieldValue::String("0".to_string())))
    );
}

#[test]
fn ambiguous_field_name_without_case_is_rejected() {
    let target = Schema::new(vec![
        field("Data", TypeInfo::Atomic(AtomicType::String)),
        field("DATA", TypeInfo::Atomic(AtomicType::String)),
    ]);
    let caller = Schema::new(vec![field("data", TypeInfo::Atomic(AtomicType::String))]);
    let bytes = row(&caller, vec![Some(FieldValue::String("0".to_string()))])
        .to_row_bytes()
        .unwrap();
    let err = decode_config(target, Some(caller.into()), &bytes).unwrap_err();
    assert!(
        err.contains("field 'data' matches more than one field without case: 'Data', 'DATA'"),
        "err: {err}"
    );
}

#[test]
fn capitalized_nested_field_matches_lowercase_target_field() {
    let string = || TypeInfo::Atomic(AtomicType::String);
    let caller_fields = vec![field("Data", string())];
    let caller_schema = Schema::new(caller_fields.clone());
    let nested = row(
        &caller_schema,
        vec![Some(FieldValue::String("0".to_string()))],
    );
    let decoded = decode(
        "outer",
        row_type(vec![field("data", string())]),
        row_type(caller_fields),
        FieldValue::Row(nested),
    )
    .unwrap();
    let Some(Some(FieldValue::Row(inner))) = decoded.get_value("outer") else {
        panic!("outer is not a row: {decoded:?}");
    };
    assert_eq!(
        inner.get_value("data"),
        Some(&Some(FieldValue::String("0".to_string())))
    );
}
