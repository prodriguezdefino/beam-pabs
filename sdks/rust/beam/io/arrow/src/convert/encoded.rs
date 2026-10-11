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

//! Row coder bytes to and from Arrow, with no Beam [`Row`](beam::schema::Row) between them.
//!
//! - Bytes to a batch: the reader walks the `beam:coder:row:v1` encoding and fills the
//!   [`RowColumns`] that the conversion of Beam rows also fills.
//! - A batch to bytes: the writer reads the values from the [`Reader`]s that the conversion
//!   to Beam rows also reads, and writes the encoding of each row.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema as ArrowSchema, TimeUnit};
use beam::coders::VarIntCoder;
use beam::schema::{Row, RowElement, Schema, SchemaError};

use super::columns::{
    Column, INSTANT_ROW, Kind, RowColumns, Slot, date_days, instant_micros, push_offset,
    record_batch,
};
use super::out_of_range;
use super::projection::Projection;
use super::readers::{
    ReadKind, Reader, RowReaders, Wire, dictionary_key, offset_range, seconds_and_micros,
};
use super::to_arrow::append_whole_row;
use crate::error::{ArrowBridgeError, Result};
use crate::schema::beam_to_arrow_schema;

/// Converts the row coder bytes of rows of `schema` into one Arrow record batch.
pub fn encoded_rows_to_record_batch<'a>(
    schema: &Schema,
    rows: impl IntoIterator<Item = &'a [u8]>,
) -> Result<RecordBatch> {
    let mut builder = RowBatchBuilder::new(schema)?;
    rows.into_iter()
        .try_for_each(|row| builder.append_encoded(row))?;
    builder.finish()
}

/// Converts the row coder bytes of rows of `schema` into one Arrow record batch of the fields
/// that `projection` selects.
pub fn encoded_rows_to_projected_batch<'a>(
    schema: &Schema,
    rows: impl IntoIterator<Item = &'a [u8]>,
    projection: &Projection,
) -> Result<RecordBatch> {
    let mut builder = RowBatchBuilder::with_projection(schema, projection)?;
    rows.into_iter()
        .try_for_each(|row| builder.append_encoded(row))?;
    builder.finish()
}

/// Builds an Arrow record batch from rows of one Beam schema, one row at a time. A row is
/// its row coder bytes or a [`Row`].
///
/// After an append fails, the builder holds part of the bad row. Drop the builder then.
pub struct RowBatchBuilder {
    schema: Arc<ArrowSchema>,
    columns: RowColumns,
    rows: usize,
}

impl RowBatchBuilder {
    /// A builder for rows of `schema`. Fails if a field type has no Arrow type.
    pub fn new(schema: &Schema) -> Result<Self> {
        Ok(Self {
            schema: Arc::new(beam_to_arrow_schema(schema)?),
            columns: RowColumns::new(None, schema)?,
            rows: 0,
        })
    }

    /// A builder for the fields of rows of `schema` that `projection` selects. Fails if a
    /// selected field type has no Arrow type, or if `projection` names no field of `schema`.
    pub fn with_projection(schema: &Schema, projection: &Projection) -> Result<Self> {
        let (columns, schema) = projection.columns(schema)?;
        Ok(Self {
            schema: Arc::new(schema),
            columns,
            rows: 0,
        })
    }

    /// Adds `row`, from its row coder bytes or its values.
    pub fn append(&mut self, row: &RowElement) -> Result<()> {
        match row {
            RowElement::Encoded(bytes) => self.append_encoded(bytes),
            RowElement::Row(row) => self.append_row(row),
        }
    }

    /// Adds the row that `bytes` encodes.
    pub fn append_encoded(&mut self, bytes: &[u8]) -> Result<()> {
        let mut input = bytes;
        read_row(&mut self.columns, &mut input)?;
        self.rows += 1;
        Ok(())
    }

    /// Adds `row`. Fails if `row` does not have one value for each field of the schema.
    pub fn append_row(&mut self, row: &Row) -> Result<()> {
        append_whole_row(&mut self.columns, row)?;
        self.rows += 1;
        Ok(())
    }

    /// The number of rows since the last [`finish`](Self::finish).
    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Returns the batch of the added rows and empties the builder.
    pub fn finish(&mut self) -> Result<RecordBatch> {
        let rows = std::mem::take(&mut self.rows);
        record_batch(&self.schema, &mut self.columns, rows)
    }
}

/// Reads the field count, the null bitmask and the values that are not null.
fn read_row(row: &mut RowColumns, input: &mut &[u8]) -> Result<()> {
    let name = row.name.as_str();
    let count = read_varint(input, name)?;
    if usize::try_from(count).ok() != Some(row.slots.len()) {
        return Err(SchemaError::ValueCountMismatch {
            expected: row.slots.len(),
            actual: usize::try_from(count).unwrap_or_default(),
        }
        .into());
    }
    let bitmask = read_length(input, name)?;
    let nulls = take(input, bitmask, name)?;
    (0..row.slots.len()).try_for_each(|position| {
        let index = row.order.as_ref().map_or(position, |order| order[position]);
        let is_null = nulls
            .get(position / 8)
            .is_some_and(|byte| byte & (1 << (position % 8)) != 0);
        match (&mut row.slots[index], is_null) {
            (Slot::Read(column), true) => column.append_null(false),
            (Slot::Read(column), false) => read_value(column, input),
            (Slot::Skip { .. }, true) => Ok(()),
            (Slot::Skip { name, skip }, false) => skip.skip(input, name),
            (Slot::Unnest(rows), true) => Err(ArrowBridgeError::UnexpectedNull {
                field: rows.name.clone(),
            }),
            (Slot::Unnest(rows), false) => read_row(rows, input),
        }
    })
}

/// Reads one element of a list or a map value. A nullable element starts with a presence
/// byte: 1 means present.
fn read_element(column: &mut Column, input: &mut &[u8]) -> Result<()> {
    if column.nullable && take_array::<1>(input, &column.name)?[0] == 0 {
        column.append_null(false)
    } else {
        read_value(column, input)
    }
}

/// Reads one value that is not null.
fn read_value(column: &mut Column, input: &mut &[u8]) -> Result<()> {
    let name = column.name.as_str();
    match &mut column.kind {
        Kind::Int8(values) => values.append_value(i8::from_be_bytes(take_array(input, name)?)),
        Kind::Int16(values) => values.append_value(i16::from_be_bytes(take_array(input, name)?)),
        // The row coder reads an INT32 as a varint and keeps its low 32 bits.
        Kind::Int32(values) => values.append_value(read_varint(input, name)? as i32),
        Kind::Int64(values) => values.append_value(read_varint(input, name)?),
        Kind::Float32(values) => values.append_value(f32::from_be_bytes(take_array(input, name)?)),
        Kind::Float64(values) => values.append_value(f64::from_be_bytes(take_array(input, name)?)),
        Kind::Boolean(values) => values.append_value(take_array::<1>(input, name)?[0] != 0),
        Kind::Utf8(values) => {
            let length = read_length(input, name)?;
            let text =
                std::str::from_utf8(take(input, length, name)?).map_err(|e| invalid(name, e))?;
            values.append_value(text);
        }
        Kind::Binary(values) => {
            let length = read_length(input, name)?;
            values.append_value(take(input, length, name)?);
        }
        Kind::MicrosInstant { values, order } => {
            values.append_value(read_instant_micros(input, order.as_deref(), name)?);
        }
        Kind::MillisInstant(values) => {
            values.append_value(i64::from_be_bytes(take_array(input, name)?));
        }
        Kind::Date(values) => values.append_value(date_days(name, read_varint(input, name)?)?),
        Kind::Decimal(values) => {
            let start = *input;
            read_varint(input, name)?;
            let length = read_length(input, name)?;
            take(input, length, name)?;
            values.append_value(&start[..start.len() - input.len()]);
        }
        Kind::List {
            offsets,
            nulls,
            items,
            ..
        } => {
            let count = read_count(input, name)?;
            (0..count).try_for_each(|_| read_element(items, input))?;
            push_offset(offsets, count, name)?;
            nulls.append_non_null();
        }
        Kind::Map {
            offsets,
            nulls,
            keys,
            values,
            ..
        } => {
            let count = read_count(input, name)?;
            (0..count).try_for_each(|_| {
                read_value(keys, input)?;
                read_element(values, input)
            })?;
            push_offset(offsets, count, name)?;
            nulls.append_non_null();
        }
        Kind::Struct {
            nulls, rows, len, ..
        } => {
            read_row(rows, input)?;
            nulls.append_non_null();
            *len += 1;
        }
    }
    Ok(())
}

/// Reads a `micros_instant` value: a row with the fields `seconds` and `micros`, both
/// varints and not null. `order` gives the field index at each position of the encoding.
fn read_instant_micros(input: &mut &[u8], order: Option<&[usize]>, name: &str) -> Result<i64> {
    let count = read_varint(input, name)?;
    let bitmask = read_length(input, name)?;
    let nulls = take(input, bitmask, name)?;
    if count != 2 || nulls.iter().any(|byte| *byte != 0) {
        return Err(ArrowBridgeError::TypeMismatch {
            field: name.to_string(),
            expected: format!("{INSTANT_ROW} with no null field"),
            actual: format!("a row of {count} fields with the null bitmask {nulls:?}"),
        });
    }
    let first = read_varint(input, name)?;
    let second = read_varint(input, name)?;
    match order {
        Some([1, 0]) => instant_micros(name, second, first),
        _ => instant_micros(name, first, second),
    }
}

fn invalid(name: &str, detail: impl ToString) -> ArrowBridgeError {
    ArrowBridgeError::InvalidEncoding {
        field: name.to_string(),
        detail: detail.to_string(),
    }
}

pub(super) fn take<'a>(input: &mut &'a [u8], length: usize, name: &str) -> Result<&'a [u8]> {
    if input.len() < length {
        return Err(invalid(
            name,
            format!("{length} bytes expected, {} left", input.len()),
        ));
    }
    let (value, rest) = input.split_at(length);
    *input = rest;
    Ok(value)
}

fn take_array<const N: usize>(input: &mut &[u8], name: &str) -> Result<[u8; N]> {
    let bytes = take(input, N, name)?;
    let mut array = [0; N];
    array.copy_from_slice(bytes);
    Ok(array)
}

/// Reads a `beam:coder:varint:v1` value: the 64 bits of the integer, 7 bits in each byte,
/// low bits first.
pub(super) fn read_varint(input: &mut &[u8], name: &str) -> Result<i64> {
    VarIntCoder::decode_varint(input).map_err(|e| invalid(name, e))
}

pub(super) fn read_length(input: &mut &[u8], name: &str) -> Result<usize> {
    let length = read_varint(input, name)?;
    usize::try_from(length).map_err(|_| invalid(name, format!("negative length {length}")))
}

/// Reads the 32-bit big-endian element count of a list or a map value.
pub(super) fn read_count(input: &mut &[u8], name: &str) -> Result<usize> {
    let count = i32::from_be_bytes(take_array::<4>(input, name)?);
    usize::try_from(count).map_err(|_| invalid(name, format!("negative element count {count}")))
}

/// Converts an Arrow record batch into the row coder bytes of each row, as rows of `schema`.
/// The columns convert as for [`record_batch_to_rows`](super::record_batch_to_rows).
pub fn record_batch_to_encoded_rows(
    batch: &RecordBatch,
    schema: &Arc<Schema>,
) -> Result<Vec<Vec<u8>>> {
    let readers = RowReaders::for_batch(batch, schema)?;
    (0..batch.num_rows())
        .map(|index| {
            let mut bytes = Vec::new();
            write_row(&readers, index, &mut bytes)?;
            Ok(bytes)
        })
        .collect()
}

/// Writes the field count, the null bitmask and the values that are not null of the row at
/// `index`. Fails if a field that is not nullable is null.
fn write_row(readers: &RowReaders<'_>, index: usize, out: &mut Vec<u8>) -> Result<()> {
    let count = readers.fields.len();
    let field_at = |position: usize| readers.order.as_ref().map_or(position, |o| o[position]);
    let nulls = (0..count)
        .map(|position| {
            let field = &readers.fields[field_at(position)];
            let is_null = field.is_null(index)?;
            if is_null && !field.nullable {
                Err(ArrowBridgeError::UnexpectedNull {
                    field: field.name.clone(),
                })
            } else {
                Ok(is_null)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    write_varint(out, count as i64);
    if nulls.iter().any(|is_null| *is_null) {
        let mut bitmask = vec![0u8; count.div_ceil(8)];
        nulls
            .iter()
            .enumerate()
            .filter(|(_, is_null)| **is_null)
            .for_each(|(position, _)| bitmask[position / 8] |= 1 << (position % 8));
        write_varint(out, bitmask.len() as i64);
        out.extend_from_slice(&bitmask);
    } else {
        write_varint(out, 0);
    }
    nulls
        .iter()
        .enumerate()
        .filter(|(_, is_null)| !**is_null)
        .try_for_each(|(position, _)| write_value(&readers.fields[field_at(position)], index, out))
}

/// Writes an element of a list or a map value. A nullable element starts with a presence
/// byte: 1 means present.
fn write_element(reader: &Reader<'_>, index: usize, out: &mut Vec<u8>) -> Result<()> {
    match (reader.is_null(index)?, reader.nullable) {
        (true, true) => {
            out.push(0);
            Ok(())
        }
        (true, false) => Err(ArrowBridgeError::UnexpectedNull {
            field: reader.name.clone(),
        }),
        (false, true) => {
            out.push(1);
            write_value(reader, index, out)
        }
        (false, false) => write_value(reader, index, out),
    }
}

/// Writes the value at `index`, which is not null.
fn write_value(reader: &Reader<'_>, index: usize, out: &mut Vec<u8>) -> Result<()> {
    let name = reader.name.as_str();
    match (&reader.kind, reader.wire) {
        (ReadKind::Missing, _) => return Err(ArrowBridgeError::MissingField(reader.name.clone())),
        (ReadKind::Dictionary { keys, values }, _) => {
            return write_value(values, dictionary_key(keys, index, name)?, out);
        }
        (ReadKind::Byte(values), _) => out.push(values.get::<i8>(index, name, "BYTE")? as u8),
        (ReadKind::Int16(values), _) => {
            out.extend_from_slice(&values.get::<i16>(index, name, "INT16")?.to_be_bytes());
        }
        (ReadKind::Int32(values), _) => {
            write_varint(out, i64::from(values.get::<i32>(index, name, "INT32")?));
        }
        // A `millis_instant` represented as an INT64 column keeps its fixed width.
        (ReadKind::Int64(values), Wire::FixedInt64) => {
            out.extend_from_slice(&values.get::<i64>(index, name, "INT64")?.to_be_bytes());
        }
        (ReadKind::Int64(values), _) => write_varint(out, values.get(index, name, "INT64")?),
        (ReadKind::Float(values), _) => out.extend_from_slice(&values.value(index).to_be_bytes()),
        (ReadKind::Double(values), _) => out.extend_from_slice(&values.get(index).to_be_bytes()),
        (ReadKind::Boolean(values), _) => out.push(u8::from(values.value(index))),
        (ReadKind::String(values), _) => write_bytes(out, values.get(index).as_bytes()),
        // A `decimal` represented as a BYTES column holds the payload, with no length.
        (ReadKind::Bytes(values), Wire::Payload) => out.extend_from_slice(values.get(index)),
        (ReadKind::Bytes(values), _) => write_bytes(out, values.get(index)),
        (ReadKind::MicrosInstant { values, order, .. }, _) => {
            let (seconds, micros) =
                seconds_and_micros(values.get(index, TimeUnit::Microsecond, name)?);
            // A row of two fields with no null field.
            write_varint(out, 2);
            write_varint(out, 0);
            let (first, second) = match order.as_deref() {
                Some([1, 0]) => (micros, seconds),
                _ => (seconds, micros),
            };
            write_varint(out, first);
            write_varint(out, second);
        }
        (ReadKind::MillisInstant(values), _) => {
            let millis = values.get(index, TimeUnit::Millisecond, name)?;
            out.extend_from_slice(&millis.to_be_bytes());
        }
        (ReadKind::Date(values), _) => write_varint(out, values.get(index, name)?),
        (ReadKind::Decimal(values), _) => out.extend_from_slice(&values.payload(index, name)?),
        (ReadKind::List { ranges, items }, _) => {
            let range = ranges.get(index);
            write_count(out, range.len(), name)?;
            range
                .into_iter()
                .try_for_each(|item| write_element(items, item, out))?;
        }
        (
            ReadKind::Map {
                offsets,
                keys,
                values,
            },
            _,
        ) => {
            let range = offset_range(offsets, index);
            write_count(out, range.len(), name)?;
            range.into_iter().try_for_each(|entry| {
                if keys.is_null(entry)? {
                    return Err(ArrowBridgeError::Schema(SchemaError::NullMapKey));
                }
                write_value(keys, entry, out)?;
                write_element(values, entry, out)
            })?;
        }
        (ReadKind::Struct(rows), _) => write_row(rows, index, out)?,
    }
    Ok(())
}

/// Writes a `beam:coder:varint:v1` value: the 64 bits of the integer, 7 bits in each byte,
/// low bits first.
fn write_varint(out: &mut Vec<u8>, value: i64) {
    VarIntCoder::encode_varint(value, out).expect("writing to Vec does not fail");
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    write_varint(out, bytes.len() as i64);
    out.extend_from_slice(bytes);
}

/// Writes the 32-bit big-endian element count of a list or a map value.
fn write_count(out: &mut Vec<u8>, count: usize, name: &str) -> Result<()> {
    let count = i32::try_from(count).map_err(|_| out_of_range(name, count, "i32 element count"))?;
    out.extend_from_slice(&count.to_be_bytes());
    Ok(())
}
