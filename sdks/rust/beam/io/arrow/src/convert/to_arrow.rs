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

//! Beam rows to an Arrow record batch.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, MapArray, RecordBatch, RecordBatchOptions,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use beam::schema::{
    AtomicType, BeamRow, FieldType, FieldValue, Row, Schema, SchemaError, TypeInfo, URN_DATE,
    URN_MICROS_INSTANT, URN_MILLIS_INSTANT,
};

use super::{MICROS_PER_SECOND, out_of_range};
use crate::error::{ArrowBridgeError, Result};
use crate::schema::{
    UTC, beam_to_arrow_fields, beam_to_arrow_schema, list_item_field, map_entries_field,
    map_entry_fields,
};

type Slot<'a> = Option<&'a FieldValue>;

/// Converts Beam rows sharing `schema` into one Arrow record batch.
///
/// Each row must have as many values as `schema` has fields; the row's own schema is not
/// checked.
pub fn rows_to_record_batch(schema: &Schema, rows: &[Row]) -> Result<RecordBatch> {
    if let Some(row) = rows
        .iter()
        .find(|r| r.values().len() != schema.num_fields())
    {
        return Err(SchemaError::ValueCountMismatch {
            expected: schema.num_fields(),
            actual: row.values().len(),
        }
        .into());
    }
    let arrow_schema = Arc::new(beam_to_arrow_schema(schema)?);
    let columns = schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let slots: Vec<Slot<'_>> = rows
                .iter()
                .map(|row| row.values().get(i).and_then(Option::as_ref))
                .collect();
            build_array(&field.name, &field.field_type, &slots, false)
        })
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    Ok(RecordBatch::try_new_with_options(
        arrow_schema,
        columns,
        &options,
    )?)
}

/// Converts `#[derive(BeamRow)]` values into one Arrow record batch.
pub fn beam_rows_to_record_batch<T: BeamRow>(items: &[T]) -> Result<RecordBatch> {
    let rows = items
        .iter()
        .map(BeamRow::to_row)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    rows_to_record_batch(T::beam_schema(), &rows)
}

/// Builds one Arrow array from the values of a column.
///
/// `masked` is set for children of a struct with null entries: Arrow keeps a child slot
/// under a null parent, so a non-nullable child must accept nulls there. Arrow validation
/// still rejects other nulls in a non-nullable child.
fn build_array(name: &str, ft: &FieldType, slots: &[Slot<'_>], masked: bool) -> Result<ArrayRef> {
    build_type(name, &ft.type_info, ft.nullable || masked, slots)
}

fn null_or_err(name: &str, nullable: bool) -> Result<()> {
    if nullable {
        Ok(())
    } else {
        Err(ArrowBridgeError::UnexpectedNull {
            field: name.to_string(),
        })
    }
}

fn mismatch(name: &str, expected: &str, actual: &FieldValue) -> ArrowBridgeError {
    ArrowBridgeError::TypeMismatch {
        field: name.to_string(),
        expected: expected.to_string(),
        actual: format!("{actual:?}"),
    }
}

/// Collects one native value per slot, rejecting type mismatches and illegal nulls.
fn collect_slots<'a, V>(
    name: &str,
    nullable: bool,
    slots: &[Slot<'a>],
    mut extract: impl FnMut(&'a FieldValue) -> Result<V>,
) -> Result<Vec<Option<V>>> {
    slots
        .iter()
        .map(|slot| match slot {
            Some(value) => extract(value).map(Some),
            None => null_or_err(name, nullable).map(|()| None),
        })
        .collect()
}

macro_rules! primitive_array {
    ($array:ty, $variant:ident, $expected:literal, $name:expr, $nullable:expr, $slots:expr) => {{
        let values = collect_slots($name, $nullable, $slots, |value| match value {
            FieldValue::$variant(v) => Ok(*v),
            other => Err(mismatch($name, $expected, other)),
        })?;
        Arc::new(<$array>::from(values)) as ArrayRef
    }};
}

fn validity(valid: Vec<bool>) -> Option<NullBuffer> {
    if valid.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(valid))
    }
}

fn offset(name: &str, len: usize) -> Result<i32> {
    i32::try_from(len).map_err(|_| out_of_range(name, len, "i32 list offset"))
}

fn build_type(
    name: &str,
    type_info: &TypeInfo,
    nullable: bool,
    slots: &[Slot<'_>],
) -> Result<ArrayRef> {
    let array: ArrayRef = match type_info {
        TypeInfo::Atomic(atomic) => match atomic {
            AtomicType::Byte => primitive_array!(Int8Array, Byte, "BYTE", name, nullable, slots),
            AtomicType::Int16 => {
                primitive_array!(Int16Array, Int16, "INT16", name, nullable, slots)
            }
            AtomicType::Int32 => {
                primitive_array!(Int32Array, Int32, "INT32", name, nullable, slots)
            }
            AtomicType::Int64 => {
                primitive_array!(Int64Array, Int64, "INT64", name, nullable, slots)
            }
            AtomicType::Float => {
                primitive_array!(Float32Array, Float, "FLOAT", name, nullable, slots)
            }
            AtomicType::Double => {
                primitive_array!(Float64Array, Double, "DOUBLE", name, nullable, slots)
            }
            AtomicType::Boolean => {
                primitive_array!(BooleanArray, Boolean, "BOOLEAN", name, nullable, slots)
            }
            AtomicType::String => {
                let values = collect_slots(name, nullable, slots, |value| match value {
                    FieldValue::String(v) => Ok(v.as_str()),
                    other => Err(mismatch(name, "STRING", other)),
                })?;
                Arc::new(StringArray::from(values))
            }
            AtomicType::Bytes => {
                let values = collect_slots(name, nullable, slots, |value| match value {
                    FieldValue::Bytes(v) => Ok(v.as_slice()),
                    other => Err(mismatch(name, "BYTES", other)),
                })?;
                Arc::new(BinaryArray::from(values))
            }
        },
        TypeInfo::Array(elem) | TypeInfo::Iterable(elem) => {
            build_list(name, elem, nullable, slots)?
        }
        TypeInfo::Map(key, value) => build_map(name, key, value, nullable, slots)?,
        TypeInfo::Row(schema) => build_struct(name, schema, nullable, slots)?,
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => match urn.as_str() {
            URN_MICROS_INSTANT => {
                let values =
                    collect_slots(name, nullable, slots, |value| instant_micros(name, value))?;
                Arc::new(TimestampMicrosecondArray::from(values).with_timezone(UTC))
            }
            URN_MILLIS_INSTANT => {
                let values = collect_slots(name, nullable, slots, |value| match value {
                    FieldValue::Int64(v) => Ok(*v),
                    other => Err(mismatch(name, "INT64 (millis_instant)", other)),
                })?;
                Arc::new(TimestampMillisecondArray::from(values).with_timezone(UTC))
            }
            URN_DATE => {
                let values = collect_slots(name, nullable, slots, |value| match value {
                    FieldValue::Int64(days) => {
                        i32::try_from(*days).map_err(|_| out_of_range(name, days, "Date32"))
                    }
                    other => Err(mismatch(name, "INT64 (date)", other)),
                })?;
                Arc::new(Date32Array::from(values))
            }
            _ => build_type(name, &representation.type_info, nullable, slots)?,
        },
    };
    Ok(array)
}

/// Total microseconds of a `micros_instant` representation row.
fn instant_micros(name: &str, value: &FieldValue) -> Result<i64> {
    let expected = "ROW<seconds: INT64, micros: INT64>";
    let FieldValue::Row(row) = value else {
        return Err(mismatch(name, expected, value));
    };
    match row.values() {
        [
            Some(FieldValue::Int64(seconds)),
            Some(FieldValue::Int64(micros)),
        ] => seconds
            .checked_mul(MICROS_PER_SECOND)
            .and_then(|s| s.checked_add(*micros))
            .ok_or_else(|| out_of_range(name, format!("{seconds}s + {micros}us"), "i64 micros")),
        _ => Err(mismatch(name, expected, value)),
    }
}

fn build_list(
    name: &str,
    elem: &FieldType,
    nullable: bool,
    slots: &[Slot<'_>],
) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(slots.len() + 1);
    offsets.push(0i32);
    let mut valid = Vec::with_capacity(slots.len());
    let mut children: Vec<Slot<'_>> = Vec::new();
    for slot in slots {
        match slot {
            Some(FieldValue::Array(items)) => {
                children.extend(items.iter().map(Option::as_ref));
                valid.push(true);
            }
            Some(other) => return Err(mismatch(name, "ARRAY", other)),
            None => {
                null_or_err(name, nullable)?;
                valid.push(false);
            }
        }
        offsets.push(offset(name, children.len())?);
    }
    let child = build_array(&format!("{name}[]"), elem, &children, false)?;
    Ok(Arc::new(ListArray::try_new(
        Arc::new(list_item_field(elem)?),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        validity(valid),
    )?))
}

fn build_map(
    name: &str,
    key: &FieldType,
    value: &FieldType,
    nullable: bool,
    slots: &[Slot<'_>],
) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(slots.len() + 1);
    offsets.push(0i32);
    let mut valid = Vec::with_capacity(slots.len());
    let mut keys: Vec<Slot<'_>> = Vec::new();
    let mut values: Vec<Slot<'_>> = Vec::new();
    for slot in slots {
        match slot {
            Some(FieldValue::Map(entries)) => {
                for (k, v) in entries {
                    keys.push(Some(k));
                    values.push(v.as_ref());
                }
                valid.push(true);
            }
            Some(other) => return Err(mismatch(name, "MAP", other)),
            None => {
                null_or_err(name, nullable)?;
                valid.push(false);
            }
        }
        offsets.push(offset(name, keys.len())?);
    }
    let key_type = key.clone().with_nullable(false);
    let key_array = build_array(&format!("{name}.key"), &key_type, &keys, false)?;
    let value_array = build_array(&format!("{name}.value"), value, &values, false)?;
    let entries = StructArray::try_new(
        map_entry_fields(key, value)?,
        vec![key_array, value_array],
        None,
    )?;
    Ok(Arc::new(MapArray::try_new(
        Arc::new(map_entries_field(key, value)?),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        validity(valid),
        false,
    )?))
}

fn build_struct(
    name: &str,
    schema: &Schema,
    nullable: bool,
    slots: &[Slot<'_>],
) -> Result<ArrayRef> {
    let mut valid = Vec::with_capacity(slots.len());
    let mut rows: Vec<Option<&Row>> = Vec::with_capacity(slots.len());
    for slot in slots {
        match slot {
            Some(FieldValue::Row(row)) => {
                if row.values().len() != schema.num_fields() {
                    return Err(SchemaError::ValueCountMismatch {
                        expected: schema.num_fields(),
                        actual: row.values().len(),
                    }
                    .into());
                }
                rows.push(Some(row));
                valid.push(true);
            }
            Some(other) => return Err(mismatch(name, "ROW", other)),
            None => {
                null_or_err(name, nullable)?;
                rows.push(None);
                valid.push(false);
            }
        }
    }
    let has_nulls = valid.contains(&false);
    let nulls = validity(valid);
    let fields = beam_to_arrow_fields(schema)?;
    if fields.is_empty() {
        return Ok(Arc::new(StructArray::new_empty_fields(slots.len(), nulls)));
    }
    let columns = schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let child_slots: Vec<Slot<'_>> = rows
                .iter()
                .map(|row| row.and_then(|r| r.values().get(i).and_then(Option::as_ref)))
                .collect();
            build_array(
                &format!("{name}.{}", field.name),
                &field.field_type,
                &child_slots,
                has_nulls,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StructArray::try_new(fields, columns, nulls)?))
}
