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

//! Rows to JSON objects.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use beam::schema::{FieldType, FieldValue, Row, TypeInfo};
use serde::ser::{Error as _, SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

/// Writes `row` as a JSON object into `buffer`, after clearing it.
pub fn write_json(row: &Row, buffer: &mut Vec<u8>) -> Result<(), String> {
    buffer.clear();
    serde_json::to_writer(buffer, &JsonRow(row)).map_err(|e| e.to_string())
}

/// Serializes a row as a JSON object, with no copy of its values.
struct JsonRow<'a>(&'a Row);

impl Serialize for JsonRow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Self(row) = self;
        let fields = &row.schema().fields;
        let mut map = serializer.serialize_map(Some(fields.len()))?;
        fields
            .iter()
            .zip(row.values())
            .try_for_each(|(field, value)| {
                let value = value
                    .as_ref()
                    .map(|value| JsonValue(value, &field.field_type));
                map.serialize_entry(&field.name, &value)
            })?;
        map.end()
    }
}

/// Serializes one value of a field of type `.1`.
struct JsonValue<'a>(&'a FieldValue, &'a FieldType);

/// Serializes `n`, or fails for NaN and the infinities, which JSON cannot hold.
fn serialize_finite<S: Serializer>(serializer: S, n: f64) -> Result<S::Ok, S::Error> {
    if n.is_finite() {
        serializer.serialize_f64(n)
    } else {
        Err(S::Error::custom(format!("{n} has no JSON number")))
    }
}

impl Serialize for JsonValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Self(value, field_type) = *self;
        match (value, &field_type.type_info) {
            (value, TypeInfo::Logical { representation, .. }) => {
                JsonValue(value, representation).serialize(serializer)
            }
            (FieldValue::Byte(n), _) => serializer.serialize_i8(*n),
            (FieldValue::Int16(n), _) => serializer.serialize_i16(*n),
            (FieldValue::Int32(n), _) => serializer.serialize_i32(*n),
            (FieldValue::Int64(n), _) => serializer.serialize_i64(*n),
            (FieldValue::Float(n), _) => serialize_finite(serializer, f64::from(*n)),
            (FieldValue::Double(n), _) => serialize_finite(serializer, *n),
            (FieldValue::String(s), _) => serializer.serialize_str(s),
            (FieldValue::Boolean(b), _) => serializer.serialize_bool(*b),
            (FieldValue::Bytes(bytes), _) => serializer.serialize_str(&BASE64.encode(bytes)),
            (FieldValue::Row(row), _) => JsonRow(row).serialize(serializer),
            (FieldValue::Array(items), TypeInfo::Array(element) | TypeInfo::Iterable(element)) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                items.iter().try_for_each(|item| {
                    seq.serialize_element(&item.as_ref().map(|item| JsonValue(item, element)))
                })?;
                seq.end()
            }
            (FieldValue::Map(entries), TypeInfo::Map(key_type, value_type)) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                entries.iter().try_for_each(|(key, value)| {
                    let value = value.as_ref().map(|value| JsonValue(value, value_type));
                    map.serialize_entry(&JsonValue(key, key_type), &value)
                })?;
                map.end()
            }
            (value, _) => Err(S::Error::custom(format!(
                "{value} does not match the type {field_type}"
            ))),
        }
    }
}
