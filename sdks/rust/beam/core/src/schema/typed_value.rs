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

//! Conversion between the [`FieldValue`] of a [`TypedValue`] and its proto form.

use std::sync::Arc;

use model::pipeline as proto;
use proto::atomic_type_value::Value as Atomic;
use proto::field_value::FieldValue as Proto;

use super::error::SchemaError;
use super::row::{FieldValue, Row};
use super::types::{AtomicType, FieldType, TypeInfo, TypedValue};

impl TypedValue {
    /// The value of a logical type is the value of its representation type.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is not a value of `field_type`, also inside arrays, maps
    /// and rows.
    pub fn new(field_type: FieldType, value: Option<FieldValue>) -> Result<Self, SchemaError> {
        let value = nullable_value_to_proto(&field_type, value)?;
        Ok(Self {
            field_type,
            value: value.field_value.is_some().then_some(value),
        })
    }

    /// Returns `None` for a null value.
    ///
    /// # Errors
    ///
    /// Returns an error if the value from the proto is not a value of the type.
    pub fn value(&self) -> Result<Option<FieldValue>, SchemaError> {
        value_from_proto(
            &self.field_type,
            self.value.as_ref().unwrap_or(&proto::FieldValue::default()),
        )
    }
}

/// Converts a value of `field_type`. An empty proto value is null.
fn nullable_value_to_proto(
    field_type: &FieldType,
    value: Option<FieldValue>,
) -> Result<proto::FieldValue, SchemaError> {
    match (value, field_type.nullable) {
        (Some(value), _) => value_to_proto(field_type, value),
        (None, true) => Ok(proto::FieldValue::default()),
        (None, false) => Err(SchemaError::UnexpectedNull {
            expected: field_type.to_string(),
        }),
    }
}

fn mismatch(field_type: &FieldType, actual: &dyn std::fmt::Debug) -> SchemaError {
    SchemaError::ValueTypeMismatch {
        expected: field_type.to_string(),
        actual: format!("{actual:?}"),
    }
}

/// The type picks the proto variant where one model variant has two proto variants: arrays
/// and iterables, and logical type values.
fn value_to_proto(
    field_type: &FieldType,
    value: FieldValue,
) -> Result<proto::FieldValue, SchemaError> {
    let elements = |element: &FieldType, items: Vec<Option<FieldValue>>| {
        items
            .into_iter()
            .map(|item| nullable_value_to_proto(element, item))
            .collect::<Result<Vec<_>, _>>()
    };
    let field_value = match (&field_type.type_info, value) {
        (TypeInfo::Atomic(atomic), value) => Proto::AtomicValue(proto::AtomicTypeValue {
            value: Some(atomic_to_proto(*atomic, value).map_err(|v| mismatch(field_type, &v))?),
        }),
        (TypeInfo::Array(element), FieldValue::Array(items)) => {
            Proto::ArrayValue(proto::ArrayTypeValue {
                element: elements(element, items)?,
            })
        }
        (TypeInfo::Iterable(element), FieldValue::Array(items)) => {
            Proto::IterableValue(proto::IterableTypeValue {
                element: elements(element, items)?,
            })
        }
        (TypeInfo::Map(key_type, value_type), FieldValue::Map(entries)) => {
            Proto::MapValue(proto::MapTypeValue {
                entries: entries
                    .into_iter()
                    .map(|(key, value)| {
                        Ok(proto::MapTypeEntry {
                            key: Some(value_to_proto(key_type, key)?),
                            value: Some(nullable_value_to_proto(value_type, value)?),
                        })
                    })
                    .collect::<Result<_, SchemaError>>()?,
            })
        }
        (TypeInfo::Row(schema), FieldValue::Row(row)) if row.schema() == schema => {
            Proto::RowValue(proto::Row {
                values: schema
                    .fields
                    .iter()
                    .zip(row.into_values())
                    .map(|(field, value)| nullable_value_to_proto(&field.field_type, value))
                    .collect::<Result<_, _>>()?,
            })
        }
        (TypeInfo::Logical { representation, .. }, value) => {
            Proto::LogicalTypeValue(Box::new(proto::LogicalTypeValue {
                value: Some(Box::new(value_to_proto(representation, value)?)),
            }))
        }
        (_, value) => return Err(mismatch(field_type, &value)),
    };
    Ok(proto::FieldValue {
        field_value: Some(field_value),
    })
}

/// Returns the value back if it is not a value of `atomic`.
fn atomic_to_proto(atomic: AtomicType, value: FieldValue) -> Result<Atomic, FieldValue> {
    match (atomic, value) {
        (AtomicType::Byte, FieldValue::Byte(v)) => Ok(Atomic::Byte(i32::from(v))),
        (AtomicType::Int16, FieldValue::Int16(v)) => Ok(Atomic::Int16(i32::from(v))),
        (AtomicType::Int32, FieldValue::Int32(v)) => Ok(Atomic::Int32(v)),
        (AtomicType::Int64, FieldValue::Int64(v)) => Ok(Atomic::Int64(v)),
        (AtomicType::Float, FieldValue::Float(v)) => Ok(Atomic::Float(v)),
        (AtomicType::Double, FieldValue::Double(v)) => Ok(Atomic::Double(v)),
        (AtomicType::String, FieldValue::String(v)) => Ok(Atomic::String(v)),
        (AtomicType::Boolean, FieldValue::Boolean(v)) => Ok(Atomic::Boolean(v)),
        (AtomicType::Bytes, FieldValue::Bytes(v)) => Ok(Atomic::Bytes(v)),
        (_, value) => Err(value),
    }
}

/// Converts a proto value of `field_type`. An empty proto value is null.
fn value_from_proto(
    field_type: &FieldType,
    value: &proto::FieldValue,
) -> Result<Option<FieldValue>, SchemaError> {
    let elements = |element: &FieldType, items: &[proto::FieldValue]| {
        items
            .iter()
            .map(|item| value_from_proto(element, item))
            .collect::<Result<Vec<_>, _>>()
            .map(FieldValue::Array)
    };
    let Some(value) = &value.field_value else {
        return field_type
            .nullable
            .then_some(None)
            .ok_or_else(|| SchemaError::UnexpectedNull {
                expected: field_type.to_string(),
            });
    };
    let value = match (&field_type.type_info, value) {
        (
            TypeInfo::Atomic(atomic),
            Proto::AtomicValue(proto::AtomicTypeValue { value: Some(v) }),
        ) => atomic_from_proto(*atomic, v).ok_or_else(|| mismatch(field_type, v))??,
        (TypeInfo::Array(element), Proto::ArrayValue(array)) => elements(element, &array.element)?,
        (TypeInfo::Iterable(element), Proto::IterableValue(iterable)) => {
            elements(element, &iterable.element)?
        }
        (TypeInfo::Map(key_type, value_type), Proto::MapValue(map)) => FieldValue::Map(
            map.entries
                .iter()
                .map(|entry| {
                    let key = entry
                        .key
                        .as_ref()
                        .map(|key| value_from_proto(key_type, key))
                        .transpose()?
                        .flatten()
                        .ok_or(SchemaError::NullMapKey)?;
                    let value = value_from_proto(
                        value_type,
                        entry
                            .value
                            .as_ref()
                            .unwrap_or(&proto::FieldValue::default()),
                    )?;
                    Ok((key, value))
                })
                .collect::<Result<_, SchemaError>>()?,
        ),
        (TypeInfo::Row(schema), Proto::RowValue(row)) => {
            let values = schema
                .fields
                .iter()
                .zip(&row.values)
                .map(|(field, value)| value_from_proto(&field.field_type, value))
                .collect::<Result<Vec<_>, _>>()?;
            FieldValue::Row(Row::new(Arc::clone(schema), values)?)
        }
        (TypeInfo::Logical { representation, .. }, Proto::LogicalTypeValue(logical)) => {
            return value_from_proto(
                representation,
                logical
                    .value
                    .as_deref()
                    .unwrap_or(&proto::FieldValue::default()),
            );
        }
        (_, other) => return Err(mismatch(field_type, other)),
    };
    Ok(Some(value))
}

/// Returns `None` if `value` is not a value of `atomic`, and an error if it is out of range.
fn atomic_from_proto(
    atomic: AtomicType,
    value: &Atomic,
) -> Option<Result<FieldValue, SchemaError>> {
    let narrow = |v: i32, convert: fn(i32) -> Option<FieldValue>| {
        convert(v).ok_or_else(|| SchemaError::ValueOutOfRange {
            value: v.to_string(),
            target: atomic.to_string(),
        })
    };
    Some(match (atomic, value) {
        (AtomicType::Byte, Atomic::Byte(v)) => {
            narrow(*v, |v| i8::try_from(v).ok().map(FieldValue::Byte))
        }
        (AtomicType::Int16, Atomic::Int16(v)) => {
            narrow(*v, |v| i16::try_from(v).ok().map(FieldValue::Int16))
        }
        (AtomicType::Int32, Atomic::Int32(v)) => Ok(FieldValue::Int32(*v)),
        (AtomicType::Int64, Atomic::Int64(v)) => Ok(FieldValue::Int64(*v)),
        (AtomicType::Float, Atomic::Float(v)) => Ok(FieldValue::Float(*v)),
        (AtomicType::Double, Atomic::Double(v)) => Ok(FieldValue::Double(*v)),
        (AtomicType::String, Atomic::String(v)) => Ok(FieldValue::String(v.clone())),
        (AtomicType::Boolean, Atomic::Boolean(v)) => Ok(FieldValue::Boolean(*v)),
        (AtomicType::Bytes, Atomic::Bytes(v)) => Ok(FieldValue::Bytes(v.clone())),
        _ => return None,
    })
}
