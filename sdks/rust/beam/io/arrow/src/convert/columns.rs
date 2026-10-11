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

//! The Arrow columns of the rows of one Beam schema.
//!
//! A [`RowColumns`] has one builder for each field, and each list, map and struct field has
//! the builders of its children. Beam rows and row coder bytes fill the same columns, so the
//! two conversions give the same batch. Only the reader of the values is different.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int8Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, ListArray, MapArray, RecordBatch, RecordBatchOptions, StructArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{FieldRef, Fields, Schema as ArrowSchema};
use beam::schema::{
    AtomicType, FieldType, Schema, TypeInfo, URN_DATE, URN_DECIMAL, URN_MICROS_INSTANT,
    URN_MILLIS_INSTANT,
};

use super::skip::Skip;
use super::{MICROS_PER_SECOND, out_of_range};
use crate::error::{ArrowBridgeError, Result};
use crate::schema::{
    UTC, beam_to_arrow_fields, list_item_field, map_entries_field, map_entry_fields,
};

/// One slot for each field of a schema, in the order of the fields.
pub(super) struct RowColumns {
    /// The name of the struct field that holds the rows, or `row` at the top.
    pub(super) name: String,
    pub(super) slots: Vec<Slot>,
    /// The field index at each position of the row coder encoding, if the two are not the
    /// same.
    pub(super) order: Option<Vec<usize>>,
}

impl RowColumns {
    /// A column for each field. `parent` is the name of the struct field that holds the rows,
    /// or `None` at the top.
    pub(super) fn new(parent: Option<&str>, schema: &Schema) -> Result<Self> {
        Self::with_slots(parent, schema, |_, name, field_type| {
            Column::new(name, field_type).map(Slot::Read)
        })
    }

    /// The slot that `slot` gives for each field, from the field index, the column name and
    /// the field type.
    pub(super) fn with_slots(
        parent: Option<&str>,
        schema: &Schema,
        mut slot: impl FnMut(usize, String, &FieldType) -> Result<Slot>,
    ) -> Result<Self> {
        let slots = schema
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let name = parent.map_or_else(
                    || field.name.clone(),
                    |parent| format!("{parent}.{}", field.name),
                );
                slot(index, name, &field.field_type)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            name: parent.unwrap_or("row").to_string(),
            slots,
            order: encoding_order(schema),
        })
    }

    /// Adds a slot to each column under a null struct value.
    pub(super) fn append_masked(&mut self) -> Result<()> {
        self.slots.iter_mut().try_for_each(|slot| match slot {
            Slot::Read(column) => column.append_null(true),
            Slot::Skip { .. } => Ok(()),
            Slot::Unnest(rows) => rows.append_masked(),
        })
    }

    /// Returns the arrays of the columns and empties them. The columns of an unnested row
    /// take the place of the row.
    pub(super) fn finish(&mut self) -> Result<Vec<ArrayRef>> {
        self.slots
            .iter_mut()
            .try_fold(Vec::new(), |mut arrays, slot| {
                match slot {
                    Slot::Read(column) => arrays.push(column.finish()?),
                    Slot::Skip { .. } => {}
                    Slot::Unnest(rows) => arrays.extend(rows.finish()?),
                }
                Ok(arrays)
            })
    }
}

/// What a conversion does with one field of a row.
pub(super) enum Slot {
    /// Converts the values into a column.
    Read(Column),
    /// Builds no column. The reader of row coder bytes steps over the value.
    Skip { name: String, skip: Skip },
    /// Converts the fields of a nested row into columns of the parent. A null nested row does
    /// not convert.
    Unnest(RowColumns),
}

/// The batch of `rows` rows with the arrays of `columns`.
pub(super) fn record_batch(
    schema: &Arc<ArrowSchema>,
    columns: &mut RowColumns,
    rows: usize,
) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns.finish()?,
        &options,
    )?)
}

/// The field index at each position of the encoding, as the row coder orders the fields.
pub(super) fn encoding_order(schema: &Schema) -> Option<Vec<usize>> {
    schema.encoding_positions_set.then(|| {
        let mut order: Vec<usize> = (0..schema.fields.len()).collect();
        order.sort_by_key(|&index| schema.fields[index].encoding_position.unwrap_or(0));
        order
    })
}

/// One Arrow column: its builders and the Beam type of its values.
pub(super) struct Column {
    pub(super) name: String,
    pub(super) nullable: bool,
    pub(super) kind: Kind,
}

pub(super) enum Kind {
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    /// `micros_instant`: a row of seconds and micros. `order` is the encoding order of that
    /// row.
    MicrosInstant {
        values: TimestampMicrosecondBuilder,
        order: Option<Vec<usize>>,
    },
    MillisInstant(TimestampMillisecondBuilder),
    Date(Date32Builder),
    /// `decimal`: the scale, the length and the unscaled bytes, as one binary value.
    Decimal(BinaryBuilder),
    List {
        item: FieldRef,
        offsets: Vec<i32>,
        nulls: NullBufferBuilder,
        items: Box<Column>,
    },
    Map {
        entries: FieldRef,
        entry_fields: Fields,
        offsets: Vec<i32>,
        nulls: NullBufferBuilder,
        keys: Box<Column>,
        values: Box<Column>,
    },
    Struct {
        fields: Fields,
        nulls: NullBufferBuilder,
        rows: RowColumns,
        len: usize,
    },
}

impl Column {
    pub(super) fn new(name: String, field_type: &FieldType) -> Result<Self> {
        let kind = Kind::new(&name, &field_type.type_info)?;
        Ok(Self {
            name,
            nullable: field_type.nullable,
            kind,
        })
    }

    /// Adds a null. `masked` is set under a null struct value, where Arrow keeps a slot in
    /// each child, also in a child that is not nullable.
    pub(super) fn append_null(&mut self, masked: bool) -> Result<()> {
        if !self.nullable && !masked {
            return Err(ArrowBridgeError::UnexpectedNull {
                field: self.name.clone(),
            });
        }
        match &mut self.kind {
            Kind::Int8(values) => values.append_null(),
            Kind::Int16(values) => values.append_null(),
            Kind::Int32(values) => values.append_null(),
            Kind::Int64(values) => values.append_null(),
            Kind::Float32(values) => values.append_null(),
            Kind::Float64(values) => values.append_null(),
            Kind::Boolean(values) => values.append_null(),
            Kind::Utf8(values) => values.append_null(),
            Kind::Binary(values) | Kind::Decimal(values) => values.append_null(),
            Kind::MicrosInstant { values, .. } => values.append_null(),
            Kind::MillisInstant(values) => values.append_null(),
            Kind::Date(values) => values.append_null(),
            Kind::List { offsets, nulls, .. } | Kind::Map { offsets, nulls, .. } => {
                offsets.push(offsets.last().copied().unwrap_or_default());
                nulls.append_null();
            }
            Kind::Struct {
                nulls, rows, len, ..
            } => {
                rows.append_masked()?;
                nulls.append_null();
                *len += 1;
            }
        }
        Ok(())
    }

    /// Returns the array of the values and empties the column.
    fn finish(&mut self) -> Result<ArrayRef> {
        Ok(match &mut self.kind {
            Kind::Int8(values) => Arc::new(values.finish()),
            Kind::Int16(values) => Arc::new(values.finish()),
            Kind::Int32(values) => Arc::new(values.finish()),
            Kind::Int64(values) => Arc::new(values.finish()),
            Kind::Float32(values) => Arc::new(values.finish()),
            Kind::Float64(values) => Arc::new(values.finish()),
            Kind::Boolean(values) => Arc::new(values.finish()),
            Kind::Utf8(values) => Arc::new(values.finish()),
            Kind::Binary(values) | Kind::Decimal(values) => Arc::new(values.finish()),
            Kind::MicrosInstant { values, .. } => Arc::new(values.finish().with_timezone(UTC)),
            Kind::MillisInstant(values) => Arc::new(values.finish().with_timezone(UTC)),
            Kind::Date(values) => Arc::new(values.finish()),
            Kind::List {
                item,
                offsets,
                nulls,
                items,
            } => Arc::new(ListArray::try_new(
                Arc::clone(item),
                take_offsets(offsets),
                items.finish()?,
                nulls.finish(),
            )?),
            Kind::Map {
                entries,
                entry_fields,
                offsets,
                nulls,
                keys,
                values,
            } => {
                let pairs = StructArray::try_new(
                    entry_fields.clone(),
                    vec![keys.finish()?, values.finish()?],
                    None,
                )?;
                Arc::new(MapArray::try_new(
                    Arc::clone(entries),
                    take_offsets(offsets),
                    pairs,
                    nulls.finish(),
                    false,
                )?)
            }
            Kind::Struct {
                fields,
                nulls,
                rows,
                len,
            } => {
                let count = std::mem::take(len);
                match fields.is_empty() {
                    true => Arc::new(StructArray::new_empty_fields(count, nulls.finish())),
                    false => Arc::new(StructArray::try_new(
                        fields.clone(),
                        rows.finish()?,
                        nulls.finish(),
                    )?),
                }
            }
        })
    }
}

impl Kind {
    fn new(name: &str, type_info: &TypeInfo) -> Result<Self> {
        Ok(match type_info {
            TypeInfo::Atomic(atomic) => match atomic {
                AtomicType::Byte => Self::Int8(Int8Builder::new()),
                AtomicType::Int16 => Self::Int16(Int16Builder::new()),
                AtomicType::Int32 => Self::Int32(Int32Builder::new()),
                AtomicType::Int64 => Self::Int64(Int64Builder::new()),
                AtomicType::Float => Self::Float32(Float32Builder::new()),
                AtomicType::Double => Self::Float64(Float64Builder::new()),
                AtomicType::Boolean => Self::Boolean(BooleanBuilder::new()),
                AtomicType::String => Self::Utf8(StringBuilder::new()),
                AtomicType::Bytes => Self::Binary(BinaryBuilder::new()),
            },
            TypeInfo::Array(item) | TypeInfo::Iterable(item) => Self::List {
                item: Arc::new(list_item_field(item)?),
                offsets: vec![0],
                nulls: NullBufferBuilder::new(0),
                items: Box::new(Column::new(format!("{name}[]"), item)?),
            },
            TypeInfo::Map(key, value) => {
                // Arrow map keys are never null.
                let key = key.clone().with_nullable(false);
                Self::Map {
                    entries: Arc::new(map_entries_field(&key, value)?),
                    entry_fields: map_entry_fields(&key, value)?,
                    offsets: vec![0],
                    nulls: NullBufferBuilder::new(0),
                    keys: Box::new(Column::new(format!("{name}.key"), &key)?),
                    values: Box::new(Column::new(format!("{name}.value"), value)?),
                }
            }
            TypeInfo::Row(schema) => Self::Struct {
                fields: beam_to_arrow_fields(schema)?,
                nulls: NullBufferBuilder::new(0),
                rows: RowColumns::new(Some(name), schema)?,
                len: 0,
            },
            TypeInfo::Logical {
                urn,
                representation,
                ..
            } => match urn.as_str() {
                URN_MICROS_INSTANT => Self::MicrosInstant {
                    values: TimestampMicrosecondBuilder::new(),
                    order: match &representation.type_info {
                        TypeInfo::Row(schema) => encoding_order(schema),
                        _ => None,
                    },
                },
                URN_MILLIS_INSTANT => Self::MillisInstant(TimestampMillisecondBuilder::new()),
                URN_DATE => Self::Date(Date32Builder::new()),
                URN_DECIMAL => Self::Decimal(BinaryBuilder::new()),
                // Other logical types convert as their representation type.
                _ => Self::new(name, &representation.type_info)?,
            },
        })
    }

    /// The Beam type that the column takes, for a type mismatch error.
    pub(super) fn expected(&self) -> &'static str {
        match self {
            Self::Int8(_) => "BYTE",
            Self::Int16(_) => "INT16",
            Self::Int32(_) => "INT32",
            Self::Int64(_) => "INT64",
            Self::Float32(_) => "FLOAT",
            Self::Float64(_) => "DOUBLE",
            Self::Boolean(_) => "BOOLEAN",
            Self::Utf8(_) => "STRING",
            Self::Binary(_) => "BYTES",
            Self::MicrosInstant { .. } => INSTANT_ROW,
            Self::MillisInstant(_) => "INT64 (millis_instant)",
            Self::Date(_) => "INT64 (date)",
            Self::Decimal(_) => "BYTES (decimal)",
            Self::List { .. } => "ARRAY",
            Self::Map { .. } => "MAP",
            Self::Struct { .. } => "ROW",
        }
    }
}

/// The representation of `micros_instant`. The Java SDK types `micros` as `INT32` and the
/// Python SDK as `INT64`. Both encode as a varint, so both are accepted.
pub(super) const INSTANT_ROW: &str = "ROW<seconds: INT64, micros: INT64 or INT32>";

/// The total microseconds of a `micros_instant` value.
pub(super) fn instant_micros(name: &str, seconds: i64, micros: i64) -> Result<i64> {
    seconds
        .checked_mul(MICROS_PER_SECOND)
        .and_then(|s| s.checked_add(micros))
        .ok_or_else(|| out_of_range(name, format!("{seconds}s + {micros}us"), "i64 micros"))
}

/// The days of a `date` value, as an Arrow `Date32` holds them.
pub(super) fn date_days(name: &str, days: i64) -> Result<i32> {
    i32::try_from(days).map_err(|_| out_of_range(name, days, "Date32"))
}

/// Adds the end offset of a list or map value of `count` elements.
pub(super) fn push_offset(offsets: &mut Vec<i32>, count: usize, name: &str) -> Result<()> {
    let last = offsets.last().copied().unwrap_or_default();
    let next = i32::try_from(count)
        .ok()
        .and_then(|count| last.checked_add(count))
        .ok_or_else(|| out_of_range(name, count, "i32 list offset"))?;
    offsets.push(next);
    Ok(())
}

/// Returns the offsets as an Arrow buffer and starts a new list of offsets.
fn take_offsets(offsets: &mut Vec<i32>) -> OffsetBuffer<i32> {
    OffsetBuffer::new(ScalarBuffer::from(std::mem::replace(offsets, vec![0])))
}
