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

//! Beam rows to an Arrow record batch. Each value goes into the [`RowColumns`] that the
//! conversion of row coder bytes also fills.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use beam::schema::{BeamRow, FieldValue, Row, Schema, SchemaError};

use super::columns::{
    Column, INSTANT_ROW, Kind, RowColumns, Slot, date_days, instant_micros, push_offset,
    record_batch,
};
use super::projection::Projection;
use crate::error::{ArrowBridgeError, Result};
use crate::schema::beam_to_arrow_schema;

/// Converts Beam rows sharing `schema` into one Arrow record batch.
///
/// Each row must have as many values as `schema` has fields; the row's own schema is not
/// checked.
pub fn rows_to_record_batch(schema: &Schema, rows: &[Row]) -> Result<RecordBatch> {
    let arrow_schema = beam_to_arrow_schema(schema)?;
    convert_rows(schema, rows, RowColumns::new(None, schema)?, arrow_schema)
}

/// Converts Beam rows sharing `schema` into one Arrow record batch of the fields that
/// `projection` selects.
pub fn rows_to_projected_batch(
    schema: &Schema,
    rows: &[Row],
    projection: &Projection,
) -> Result<RecordBatch> {
    let (columns, arrow_schema) = projection.columns(schema)?;
    convert_rows(schema, rows, columns, arrow_schema)
}

fn convert_rows(
    schema: &Schema,
    rows: &[Row],
    mut columns: RowColumns,
    arrow_schema: ArrowSchema,
) -> Result<RecordBatch> {
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
    rows.iter()
        .try_for_each(|row| append_row(&mut columns, row.values()))?;
    record_batch(&Arc::new(arrow_schema), &mut columns, rows.len())
}

/// Converts `#[derive(BeamRow)]` values into one Arrow record batch.
pub fn beam_rows_to_record_batch<T: BeamRow>(items: &[T]) -> Result<RecordBatch> {
    let rows = items
        .iter()
        .map(BeamRow::to_row)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    rows_to_record_batch(T::beam_schema(), &rows)
}

/// Adds the values of one row, one to each column. `values` has a value for each slot.
fn append_row(row: &mut RowColumns, values: &[Option<FieldValue>]) -> Result<()> {
    row.slots
        .iter_mut()
        .zip(values)
        .try_for_each(|(slot, value)| match (slot, value) {
            (Slot::Read(column), value) => append(column, value.as_ref()),
            (Slot::Skip { .. }, _) => Ok(()),
            (Slot::Unnest(rows), Some(FieldValue::Row(nested))) => append_whole_row(rows, nested),
            (Slot::Unnest(rows), None) => Err(ArrowBridgeError::UnexpectedNull {
                field: rows.name.clone(),
            }),
            (Slot::Unnest(rows), Some(other)) => Err(mismatch(&rows.name, "ROW", other)),
        })
}

/// Adds the values of `row`, a top-level or a nested row. Fails if it has not a value for
/// each slot.
pub(super) fn append_whole_row(rows: &mut RowColumns, row: &Row) -> Result<()> {
    if row.values().len() != rows.slots.len() {
        return Err(SchemaError::ValueCountMismatch {
            expected: rows.slots.len(),
            actual: row.values().len(),
        }
        .into());
    }
    append_row(rows, row.values())
}

fn append(column: &mut Column, value: Option<&FieldValue>) -> Result<()> {
    match value {
        Some(value) => append_value(column, value),
        None => column.append_null(false),
    }
}

fn append_value(column: &mut Column, value: &FieldValue) -> Result<()> {
    let name = column.name.as_str();
    match (&mut column.kind, value) {
        (Kind::Int8(values), FieldValue::Byte(v)) => values.append_value(*v),
        (Kind::Int16(values), FieldValue::Int16(v)) => values.append_value(*v),
        (Kind::Int32(values), FieldValue::Int32(v)) => values.append_value(*v),
        (Kind::Int64(values), FieldValue::Int64(v)) => values.append_value(*v),
        (Kind::Float32(values), FieldValue::Float(v)) => values.append_value(*v),
        (Kind::Float64(values), FieldValue::Double(v)) => values.append_value(*v),
        (Kind::Boolean(values), FieldValue::Boolean(v)) => values.append_value(*v),
        (Kind::Utf8(values), FieldValue::String(v)) => values.append_value(v),
        (Kind::Binary(values) | Kind::Decimal(values), FieldValue::Bytes(v)) => {
            values.append_value(v);
        }
        (Kind::MicrosInstant { values, .. }, FieldValue::Row(row)) => {
            values.append_value(row_instant_micros(name, row, value)?);
        }
        (Kind::MillisInstant(values), FieldValue::Int64(v)) => values.append_value(*v),
        (Kind::Date(values), FieldValue::Int64(days)) => {
            values.append_value(date_days(name, *days)?);
        }
        (
            Kind::List {
                offsets,
                nulls,
                items,
                ..
            },
            FieldValue::Array(elements),
        ) => {
            elements
                .iter()
                .try_for_each(|element| append(items, element.as_ref()))?;
            push_offset(offsets, elements.len(), name)?;
            nulls.append_non_null();
        }
        (
            Kind::Map {
                offsets,
                nulls,
                keys,
                values,
                ..
            },
            FieldValue::Map(entries),
        ) => {
            entries.iter().try_for_each(|(key, entry)| {
                append_value(keys, key)?;
                append(values, entry.as_ref())
            })?;
            push_offset(offsets, entries.len(), name)?;
            nulls.append_non_null();
        }
        (
            Kind::Struct {
                nulls, rows, len, ..
            },
            FieldValue::Row(row),
        ) => {
            append_whole_row(rows, row)?;
            nulls.append_non_null();
            *len += 1;
        }
        (kind, other) => return Err(mismatch(name, kind.expected(), other)),
    }
    Ok(())
}

/// The total microseconds of a `micros_instant` representation row.
fn row_instant_micros(name: &str, row: &Row, value: &FieldValue) -> Result<i64> {
    match row.values() {
        [
            Some(FieldValue::Int64(seconds)),
            Some(FieldValue::Int64(micros)),
        ] => instant_micros(name, *seconds, *micros),
        [
            Some(FieldValue::Int64(seconds)),
            Some(FieldValue::Int32(micros)),
        ] => instant_micros(name, *seconds, i64::from(*micros)),
        _ => Err(mismatch(name, INSTANT_ROW, value)),
    }
}

fn mismatch(name: &str, expected: &str, actual: &FieldValue) -> ArrowBridgeError {
    ArrowBridgeError::TypeMismatch {
        field: name.to_string(),
        expected: expected.to_string(),
        actual: format!("{actual:?}"),
    }
}
