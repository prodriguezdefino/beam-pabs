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

//! Mapping between Beam [`Schema`]s and Arrow [`ArrowSchema`]s.
//!
//! | Beam | Arrow |
//! |---|---|
//! | `BYTE` / `INT16` / `INT32` / `INT64` | `Int8` / `Int16` / `Int32` / `Int64` |
//! | `FLOAT` / `DOUBLE` | `Float32` / `Float64` |
//! | `STRING` / `BYTES` / `BOOLEAN` | `Utf8` / `Binary` / `Boolean` |
//! | `ARRAY<T>`, `ITERABLE<T>` | `List<T>` |
//! | `MAP<K, V>` | `Map<entries: Struct<key: K, value: V>>` |
//! | `ROW<...>` | `Struct<...>` |
//! | `micros_instant` | `Timestamp(Microsecond, "UTC")` |
//! | `millis_instant` | `Timestamp(Millisecond, "UTC")` |
//! | `date` | `Date32` |
//! | any other logical type | its representation, tagged in field metadata |
//!
//! Other logical types, including `decimal` (scale per value in Beam, per column in Arrow),
//! are written as their representation, with the URN and payload in [`META_LOGICAL_URN`]
//! and [`META_LOGICAL_PAYLOAD`], so [`arrow_to_beam_schema`] can restore them.
//!
//! Arrow to Beam widens types without an exact match: unsigned integers to the next larger
//! signed type, large/view strings, binaries and lists like their standard forms,
//! timestamps to `micros_instant` (`millis_instant` for second and millisecond units), and
//! `Decimal128` and `Decimal256` to `decimal`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema, TimeUnit};
use beam::schema::{
    AtomicType, Field, FieldType, Schema, TypeInfo, URN_DATE, URN_DECIMAL, URN_MICROS_INSTANT,
    URN_MILLIS_INSTANT,
};

use crate::error::{ArrowBridgeError, Result};

/// Field metadata key holding the URN of a Beam logical type stored as its representation.
pub const META_LOGICAL_URN: &str = "beam:logical_type:urn";

/// Field metadata key holding the hex-encoded payload of a Beam logical type.
pub const META_LOGICAL_PAYLOAD: &str = "beam:logical_type:payload";

/// Field metadata key marking a list as a Beam `ITERABLE` rather than an `ARRAY`.
pub const META_ITERABLE: &str = "beam:iterable";

/// Time zone attached to the Arrow timestamps produced for Beam instants.
pub const UTC: &str = "UTC";

/// Name of the Arrow list child field.
const LIST_ITEM: &str = "item";
/// Name of the Arrow map entries field.
const MAP_ENTRIES: &str = "entries";
/// Name of the Arrow map key field.
const MAP_KEY: &str = "key";
/// Name of the Arrow map value field.
const MAP_VALUE: &str = "value";

/// The Beam field type for `micros_instant`, the same as for `chrono::DateTime<Utc>`.
pub fn micros_instant_type() -> FieldType {
    FieldType::logical(
        URN_MICROS_INSTANT,
        Vec::new(),
        FieldType::row(Schema::new(vec![
            Field::new("seconds", FieldType::int64()),
            Field::new("micros", FieldType::int64()),
        ])),
    )
}

/// The Beam field type for `millis_instant` (`INT64` milliseconds since the epoch).
pub fn millis_instant_type() -> FieldType {
    FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64())
}

/// The Beam field type for `date` (`INT64` days since the epoch).
pub fn date_type() -> FieldType {
    FieldType::logical(URN_DATE, Vec::new(), FieldType::int64())
}

/// The Beam field type for `decimal` (`beam:logical_type:decimal:v1` byte layout).
pub fn decimal_type() -> FieldType {
    FieldType::logical(URN_DECIMAL, Vec::new(), FieldType::bytes())
}

/// Converts a Beam schema to the equivalent Arrow schema.
pub fn beam_to_arrow_schema(schema: &Schema) -> Result<ArrowSchema> {
    Ok(ArrowSchema::new(beam_to_arrow_fields(schema)?))
}

/// Converts the fields of a Beam schema to Arrow fields.
pub fn beam_to_arrow_fields(schema: &Schema) -> Result<Fields> {
    schema
        .fields
        .iter()
        .map(beam_to_arrow_field)
        .collect::<Result<Vec<_>>>()
        .map(Fields::from)
}

/// Converts one Beam field to an Arrow field.
pub fn beam_to_arrow_field(field: &Field) -> Result<ArrowField> {
    field_type_to_arrow(&field.name, &field.field_type)
}

/// Builds an Arrow field named `name` for the Beam `field_type`.
pub fn field_type_to_arrow(name: &str, field_type: &FieldType) -> Result<ArrowField> {
    let (data_type, metadata) = data_type_with_metadata(&field_type.type_info)?;
    let field = ArrowField::new(name, data_type, field_type.nullable);
    Ok(if metadata.is_empty() {
        field
    } else {
        field.with_metadata(metadata)
    })
}

/// The Arrow data type for a Beam type.
pub fn beam_to_arrow_type(type_info: &TypeInfo) -> Result<DataType> {
    data_type_with_metadata(type_info).map(|(data_type, _)| data_type)
}

fn data_type_with_metadata(type_info: &TypeInfo) -> Result<(DataType, HashMap<String, String>)> {
    let data_type = match type_info {
        TypeInfo::Atomic(atomic) => atomic_to_arrow(*atomic),
        TypeInfo::Array(elem) => list_type(elem)?,
        TypeInfo::Iterable(elem) => {
            let metadata = HashMap::from([(META_ITERABLE.to_string(), "true".to_string())]);
            return Ok((list_type(elem)?, metadata));
        }
        TypeInfo::Map(key, value) => map_type(key, value)?,
        TypeInfo::Row(schema) => DataType::Struct(beam_to_arrow_fields(schema)?),
        TypeInfo::Logical {
            urn,
            payload,
            representation,
        } => match urn.as_str() {
            URN_MICROS_INSTANT => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            URN_MILLIS_INSTANT => DataType::Timestamp(TimeUnit::Millisecond, Some(UTC.into())),
            URN_DATE => DataType::Date32,
            _ => {
                let (data_type, mut metadata) = data_type_with_metadata(&representation.type_info)?;
                metadata.insert(META_LOGICAL_URN.to_string(), urn.clone());
                if !payload.is_empty() {
                    metadata.insert(META_LOGICAL_PAYLOAD.to_string(), hex_encode(payload));
                }
                return Ok((data_type, metadata));
            }
        },
    };
    Ok((data_type, HashMap::new()))
}

fn atomic_to_arrow(atomic: AtomicType) -> DataType {
    match atomic {
        AtomicType::Byte => DataType::Int8,
        AtomicType::Int16 => DataType::Int16,
        AtomicType::Int32 => DataType::Int32,
        AtomicType::Int64 => DataType::Int64,
        AtomicType::Float => DataType::Float32,
        AtomicType::Double => DataType::Float64,
        AtomicType::String => DataType::Utf8,
        AtomicType::Boolean => DataType::Boolean,
        AtomicType::Bytes => DataType::Binary,
    }
}

pub(super) fn list_item_field(elem: &FieldType) -> Result<ArrowField> {
    field_type_to_arrow(LIST_ITEM, elem)
}

fn list_type(elem: &FieldType) -> Result<DataType> {
    Ok(DataType::List(Arc::new(list_item_field(elem)?)))
}

/// The key/value fields of a map's entries struct. Arrow map keys are never null.
pub(super) fn map_entry_fields(key: &FieldType, value: &FieldType) -> Result<Fields> {
    let key = field_type_to_arrow(MAP_KEY, &key.clone().with_nullable(false))?;
    let value = field_type_to_arrow(MAP_VALUE, value)?;
    Ok(Fields::from(vec![key, value]))
}

pub(super) fn map_entries_field(key: &FieldType, value: &FieldType) -> Result<ArrowField> {
    Ok(ArrowField::new(
        MAP_ENTRIES,
        DataType::Struct(map_entry_fields(key, value)?),
        false,
    ))
}

fn map_type(key: &FieldType, value: &FieldType) -> Result<DataType> {
    Ok(DataType::Map(
        Arc::new(map_entries_field(key, value)?),
        false,
    ))
}

/// Converts an Arrow schema to the equivalent Beam schema.
pub fn arrow_to_beam_schema(schema: &ArrowSchema) -> Result<Schema> {
    arrow_to_beam_fields(schema.fields())
}

fn arrow_to_beam_fields(fields: &Fields) -> Result<Schema> {
    fields
        .iter()
        .map(|field| arrow_to_beam_field(field))
        .collect::<Result<Vec<_>>>()
        .map(Schema::new)
}

/// Converts one Arrow field to a Beam field.
pub fn arrow_to_beam_field(field: &ArrowField) -> Result<Field> {
    Ok(Field::new(field.name(), arrow_field_type(field)?))
}

fn arrow_field_type(field: &ArrowField) -> Result<FieldType> {
    let metadata = field.metadata();
    let mut field_type = arrow_data_type(field.name(), field.data_type())?;
    if metadata.get(META_ITERABLE).is_some_and(|v| v == "true")
        && let TypeInfo::Array(elem) = field_type.type_info
    {
        field_type = FieldType::new(TypeInfo::Iterable(elem), false);
    }
    if let Some(urn) = metadata.get(META_LOGICAL_URN) {
        let payload = metadata
            .get(META_LOGICAL_PAYLOAD)
            .map(|hex| {
                hex_decode(hex).ok_or_else(|| ArrowBridgeError::UnsupportedType {
                    field: field.name().clone(),
                    detail: format!("malformed logical type payload '{hex}'"),
                })
            })
            .transpose()?
            .unwrap_or_default();
        field_type = FieldType::logical(urn.clone(), payload, field_type);
    }
    Ok(field_type.with_nullable(field.is_nullable()))
}

/// The Beam field type (non-nullable) for an Arrow data type.
pub fn arrow_data_type(name: &str, data_type: &DataType) -> Result<FieldType> {
    let field_type = match data_type {
        DataType::Boolean => FieldType::boolean(),
        DataType::Int8 => FieldType::byte(),
        DataType::Int16 | DataType::UInt8 => FieldType::int16(),
        DataType::Int32 | DataType::UInt16 => FieldType::int32(),
        DataType::Int64 | DataType::UInt32 => FieldType::int64(),
        DataType::Float32 => FieldType::float(),
        DataType::Float64 => FieldType::double(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => FieldType::string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => FieldType::bytes(),
        DataType::Timestamp(TimeUnit::Second | TimeUnit::Millisecond, _) => millis_instant_type(),
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _) => {
            micros_instant_type()
        }
        DataType::Date32 | DataType::Date64 => date_type(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => decimal_type(),
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            FieldType::array(arrow_field_type(item)?)
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(kv) = entries.data_type() else {
                return Err(unsupported(name, data_type));
            };
            let (Some(key), Some(value)) = (kv.first(), kv.get(1)) else {
                return Err(unsupported(name, data_type));
            };
            FieldType::map(
                arrow_field_type(key)?.with_nullable(false),
                arrow_field_type(value)?,
            )
        }
        DataType::Struct(fields) => FieldType::row(arrow_to_beam_fields(fields)?),
        DataType::Dictionary(_, value) => arrow_data_type(name, value)?,
        other => return Err(unsupported(name, other)),
    };
    Ok(field_type)
}

fn unsupported(name: &str, data_type: &DataType) -> ArrowBridgeError {
    ArrowBridgeError::UnsupportedType {
        field: name.to_string(),
        detail: format!("Arrow type {data_type} has no Beam equivalent"),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            hex.get(i..i + 2)
                .and_then(|b| u8::from_str_radix(b, 16).ok())
        })
        .collect()
}
