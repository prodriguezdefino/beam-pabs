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

//! An Arrow record batch to Beam rows of a requested schema. The values come from the
//! [`Reader`]s that the conversion to row coder bytes also reads.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::TimeUnit;
use beam::schema::{BeamRow, FieldType, FieldValue, Row, Schema, SchemaError};

use super::readers::{
    ReadKind, Reader, RowReaders, dictionary_key, offset_range, seconds_and_micros,
};
use crate::error::{ArrowBridgeError, Result};

/// Converts an Arrow record batch into Beam rows of `schema`.
///
/// Columns match fields by name. A missing column gives nulls for a nullable field and an
/// error otherwise; extra columns are ignored.
pub fn record_batch_to_rows(batch: &RecordBatch, schema: &Arc<Schema>) -> Result<Vec<Row>> {
    let readers = RowReaders::for_batch(batch, schema)?;
    (0..batch.num_rows())
        .map(|index| read_row(&readers, index))
        .collect()
}

/// Converts an Arrow record batch into `#[derive(BeamRow)]` values.
pub fn record_batch_to_beam_rows<T: BeamRow>(batch: &RecordBatch) -> Result<Vec<T>> {
    record_batch_to_rows(batch, T::beam_schema())?
        .iter()
        .map(|row| T::from_row(row).map_err(ArrowBridgeError::from))
        .collect()
}

/// Converts an Arrow array into one optional Beam value per element.
pub fn array_to_values(
    name: &str,
    ft: &FieldType,
    array: &dyn Array,
) -> Result<Vec<Option<FieldValue>>> {
    let reader = Reader::new(name.to_string(), ft, array)?;
    (0..array.len())
        .map(|index| read_optional(&reader, index))
        .collect()
}

/// The row at `index`. Fails if a field that is not nullable is null.
fn read_row(readers: &RowReaders<'_>, index: usize) -> Result<Row> {
    let values = readers
        .fields
        .iter()
        .map(|field| match read_optional(field, index)? {
            None if !field.nullable => Err(ArrowBridgeError::UnexpectedNull {
                field: field.name.clone(),
            }),
            value => Ok(value),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Row::new(Arc::clone(&readers.schema), values)?)
}

fn read_optional(reader: &Reader<'_>, index: usize) -> Result<Option<FieldValue>> {
    if reader.is_null(index)? {
        Ok(None)
    } else {
        read_value(reader, index).map(Some)
    }
}

/// An element of a list or a map value. Fails if it is null and its type is not nullable.
fn read_element(reader: &Reader<'_>, index: usize) -> Result<Option<FieldValue>> {
    match read_optional(reader, index)? {
        None if !reader.nullable => Err(ArrowBridgeError::UnexpectedNull {
            field: reader.name.clone(),
        }),
        value => Ok(value),
    }
}

/// The value at `index`, which is not null.
fn read_value(reader: &Reader<'_>, index: usize) -> Result<FieldValue> {
    let name = reader.name.as_str();
    Ok(match &reader.kind {
        ReadKind::Missing => return Err(ArrowBridgeError::MissingField(reader.name.clone())),
        ReadKind::Dictionary { keys, values } => {
            return read_value(values, dictionary_key(keys, index, name)?);
        }
        ReadKind::Byte(values) => FieldValue::Byte(values.get(index, name, "BYTE")?),
        ReadKind::Int16(values) => FieldValue::Int16(values.get(index, name, "INT16")?),
        ReadKind::Int32(values) => FieldValue::Int32(values.get(index, name, "INT32")?),
        ReadKind::Int64(values) => FieldValue::Int64(values.get(index, name, "INT64")?),
        ReadKind::Float(values) => FieldValue::Float(values.value(index)),
        ReadKind::Double(values) => FieldValue::Double(values.get(index)),
        ReadKind::Boolean(values) => FieldValue::Boolean(values.value(index)),
        ReadKind::String(values) => FieldValue::String(values.get(index).to_string()),
        ReadKind::Bytes(values) => FieldValue::Bytes(values.get(index).to_vec()),
        ReadKind::MicrosInstant {
            values,
            representation,
            ..
        } => {
            let (seconds, micros) =
                seconds_and_micros(values.get(index, TimeUnit::Microsecond, name)?);
            FieldValue::Row(Row::new(
                Arc::clone(representation),
                vec![
                    Some(FieldValue::Int64(seconds)),
                    Some(FieldValue::Int64(micros)),
                ],
            )?)
        }
        ReadKind::MillisInstant(values) => {
            FieldValue::Int64(values.get(index, TimeUnit::Millisecond, name)?)
        }
        ReadKind::Date(values) => FieldValue::Int64(values.get(index, name)?),
        ReadKind::Decimal(values) => FieldValue::Bytes(values.payload(index, name)?),
        ReadKind::List { ranges, items } => FieldValue::Array(
            ranges
                .get(index)
                .map(|item| read_element(items, item))
                .collect::<Result<_>>()?,
        ),
        ReadKind::Map {
            offsets,
            keys,
            values,
        } => FieldValue::Map(
            offset_range(offsets, index)
                .map(|entry| {
                    let key = read_optional(keys, entry)?
                        .ok_or(ArrowBridgeError::Schema(SchemaError::NullMapKey))?;
                    Ok((key, read_element(values, entry)?))
                })
                .collect::<Result<_>>()?,
        ),
        ReadKind::Struct(rows) => FieldValue::Row(read_row(rows, index)?),
    })
}
