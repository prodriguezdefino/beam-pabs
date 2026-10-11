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

//! Decodes the configuration row of a request into the schema of the provider.

use std::sync::Arc;

use beam::schema::{AtomicType, FieldType, FieldValue, Row, Schema, TypeInfo};
use model::pipeline as proto;

/// Decodes `row_bytes` with the caller schema, then maps its fields by name to `target`.
///
/// The caller can omit fields and use any field order. A field that is not in `target`, or
/// that has a different type, is an error. An integer field accepts any integer type when the
/// value fits. A request without a schema and without row bytes gives a row of nulls.
pub fn decode_config(
    target: Schema,
    caller_schema: Option<proto::Schema>,
    row_bytes: &[u8],
) -> Result<Row, String> {
    let target = Arc::new(target);
    let Some(caller_schema) = caller_schema else {
        if !row_bytes.is_empty() {
            return Err("configuration row has no schema".to_string());
        }
        return Row::new(target.clone(), vec![None; target.num_fields()])
            .map_err(|e| e.to_string());
    };
    let caller_schema = Arc::new(Schema::try_from(caller_schema).map_err(|e| e.to_string())?);
    for field in &caller_schema.fields {
        let index = field_position(&target, &field.name)?
            .ok_or_else(|| format!("unknown field '{}'", field.name))?;
        let expected = &target.fields[index];
        if !same_shape(&expected.field_type, &field.field_type)? {
            return Err(format!(
                "field '{}' has type {}, expected {}",
                field.name, field.field_type, expected.field_type
            ));
        }
    }
    let caller_row = Row::from_row_bytes(&caller_schema, row_bytes).map_err(|e| e.to_string())?;
    let values = target
        .fields
        .iter()
        .map(|field| {
            field_position(&caller_schema, &field.name)?
                .and_then(|index| caller_row.values().get(index))
                .cloned()
                .flatten()
                .map(|value| convert(&field.name, &field.field_type, value))
                .transpose()
        })
        .collect::<Result<_, String>>()?;
    Row::new(target, values).map_err(|e| e.to_string())
}

/// Returns the position of the field `name` in `schema`.
///
/// An exact match comes first. Then a field matches when the names are equal without ASCII
/// case, because Go capitalizes exported struct fields in a configuration schema. More than one
/// match without case is an error.
fn field_position(schema: &Schema, name: &str) -> Result<Option<usize>, String> {
    if let Some(index) = schema.field_index(name) {
        return Ok(Some(index));
    }
    let mut matches = schema
        .fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name.eq_ignore_ascii_case(name));
    match (matches.next(), matches.next()) {
        (None, _) => Ok(None),
        (Some((index, _)), None) => Ok(Some(index)),
        (Some((_, first)), Some((_, second))) => Err(format!(
            "field '{name}' matches more than one field without case: '{}', '{}'",
            first.name, second.name
        )),
    }
}

/// Compares two field types without nullability, schema ids and logical type payloads.
///
/// Two integer types match, because [`convert`] narrows the value. Nested rows must have the
/// same fields in the same order, so their values keep the layout. Nested field names match
/// with the rule of [`field_position`].
fn same_shape(expected: &FieldType, provided: &FieldType) -> Result<bool, String> {
    match (&expected.type_info, &provided.type_info) {
        (TypeInfo::Atomic(x), TypeInfo::Atomic(y)) => {
            Ok(x == y || is_integer(*x) && is_integer(*y))
        }
        (TypeInfo::Array(x), TypeInfo::Array(y))
        | (TypeInfo::Iterable(x), TypeInfo::Iterable(y)) => same_shape(x, y),
        (TypeInfo::Map(xk, xv), TypeInfo::Map(yk, yv)) => {
            Ok(same_shape(xk, yk)? && same_shape(xv, yv)?)
        }
        (TypeInfo::Row(x), TypeInfo::Row(y)) if x.fields.len() == y.fields.len() => x
            .fields
            .iter()
            .zip(&y.fields)
            .enumerate()
            .try_fold(true, |same, (index, (xf, yf))| {
                Ok(same
                    && field_position(x, &yf.name)? == Some(index)
                    && same_shape(&xf.field_type, &yf.field_type)?)
            }),
        (TypeInfo::Logical { urn: x, .. }, TypeInfo::Logical { urn: y, .. }) => Ok(x == y),
        _ => Ok(false),
    }
}

fn is_integer(atomic: AtomicType) -> bool {
    match atomic {
        AtomicType::Byte | AtomicType::Int16 | AtomicType::Int32 | AtomicType::Int64 => true,
        AtomicType::Float
        | AtomicType::Double
        | AtomicType::String
        | AtomicType::Boolean
        | AtomicType::Bytes => false,
    }
}

/// Converts a caller value to the type of `expected`, also inside lists, maps and rows.
///
/// `path` names the field in an error, for example `parent.child`. [`same_shape`] has checked
/// the types, so only integers can change.
fn convert(path: &str, expected: &FieldType, value: FieldValue) -> Result<FieldValue, String> {
    match (&expected.type_info, value) {
        (TypeInfo::Atomic(atomic), value) => narrow(path, *atomic, value),
        (TypeInfo::Array(element) | TypeInfo::Iterable(element), FieldValue::Array(items)) => items
            .into_iter()
            .map(|item| item.map(|v| convert(path, element, v)).transpose())
            .collect::<Result<_, _>>()
            .map(FieldValue::Array),
        (TypeInfo::Map(key_type, value_type), FieldValue::Map(entries)) => entries
            .into_iter()
            .map(|(key, value)| {
                Ok((
                    convert(path, key_type, key)?,
                    value.map(|v| convert(path, value_type, v)).transpose()?,
                ))
            })
            .collect::<Result<_, String>>()
            .map(FieldValue::Map),
        (TypeInfo::Row(schema), FieldValue::Row(row)) => {
            // `Row` only lends its values, so each nested value is copied. A config row is
            // decoded once per expansion.
            let values = schema
                .fields
                .iter()
                .zip(row.values())
                .map(|(field, value)| {
                    let path = format!("{path}.{}", field.name);
                    value
                        .clone()
                        .map(|v| convert(&path, &field.field_type, v))
                        .transpose()
                })
                .collect::<Result<_, _>>()?;
            Row::new(Arc::new(schema.clone()), values)
                .map(FieldValue::Row)
                .map_err(|e| e.to_string())
        }
        (_, value) => Ok(value),
    }
}

/// Narrows an integer value to the integer type `target`. Other types keep their value.
fn narrow(path: &str, target: AtomicType, value: FieldValue) -> Result<FieldValue, String> {
    match target {
        AtomicType::Byte => fit(path, target, value, FieldValue::Byte),
        AtomicType::Int16 => fit(path, target, value, FieldValue::Int16),
        AtomicType::Int32 => fit(path, target, value, FieldValue::Int32),
        AtomicType::Int64 => fit(path, target, value, FieldValue::Int64),
        AtomicType::Float
        | AtomicType::Double
        | AtomicType::String
        | AtomicType::Boolean
        | AtomicType::Bytes => Ok(value),
    }
}

/// Wraps an integer `value` as `T`, or returns an error when the value does not fit in `T`.
fn fit<T: TryFrom<i64>>(
    path: &str,
    target: AtomicType,
    value: FieldValue,
    wrap: fn(T) -> FieldValue,
) -> Result<FieldValue, String> {
    let wide = match value {
        FieldValue::Byte(v) => i64::from(v),
        FieldValue::Int16(v) => i64::from(v),
        FieldValue::Int32(v) => i64::from(v),
        FieldValue::Int64(v) => v,
        other => {
            return Err(format!(
                "field '{path}' has value {other}, expected {target}"
            ));
        }
    };
    T::try_from(wide)
        .map(wrap)
        .map_err(|_| format!("field '{path}' value {wide} is out of range for {target}"))
}
