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

//! Direct `beam:coder:row:v1` encoders and [`Row`] builders for [`RawRow`], in both flattened
//! and nested shapes.

use std::borrow::Cow;
use std::io::Write;
use std::sync::Arc;

use beam::coders::{CoderError, VarIntCoder};
use beam::schema::{FieldValue, Row, Schema, SchemaError};

use super::assembler::{RawCell, RawRow};
use crate::bigtable::{cell_schema, flattened_row_schema, nested_row_schema};

/// Encodes one `beam:coder:row:v1` payload per `(family, qualifier)` column in `row`.
pub fn encode_flattened(row: &RawRow) -> Result<Vec<Vec<u8>>, CoderError> {
    let cells = sorted_cells(&row.cells);
    cells
        .chunk_by(|a, b| a.family == b.family && a.qualifier == b.qualifier)
        .map(|col| {
            let first = &col[0];
            let mut buf =
                Vec::with_capacity(row.key.len() + first.family.len() + first.qualifier.len() + 32);
            // 4 non-null fields: key, family_name, column_qualifier, cells.
            VarIntCoder::encode_varint(4, &mut buf)?;
            VarIntCoder::encode_varint(0, &mut buf)?;
            write_bytes(&row.key, &mut buf)?;
            write_bytes(first.family.as_bytes(), &mut buf)?;
            write_bytes(&first.qualifier, &mut buf)?;
            write_cells(col, &mut buf)?;
            Ok(buf)
        })
        .collect()
}

/// Encodes `row` into one `beam:coder:row:v1` payload matching [`nested_row_schema`].
pub fn encode_nested(row: &RawRow) -> Result<Vec<u8>, CoderError> {
    let cells = sorted_cells(&row.cells);
    let mut buf = Vec::with_capacity(row.key.len() + 64);
    // 2 non-null fields: key, column_families.
    VarIntCoder::encode_varint(2, &mut buf)?;
    VarIntCoder::encode_varint(0, &mut buf)?;
    write_bytes(&row.key, &mut buf)?;
    let family_count = cells.chunk_by(|a, b| a.family == b.family).count();
    write_count(family_count, &mut buf)?;
    for fam_cells in cells.chunk_by(|a, b| a.family == b.family) {
        write_bytes(fam_cells[0].family.as_bytes(), &mut buf)?;
        let col_count = fam_cells
            .chunk_by(|a, b| a.qualifier == b.qualifier)
            .count();
        write_count(col_count, &mut buf)?;
        for col_cells in fam_cells.chunk_by(|a, b| a.qualifier == b.qualifier) {
            write_bytes(&col_cells[0].qualifier, &mut buf)?;
            write_cells(col_cells, &mut buf)?;
        }
    }
    Ok(buf)
}

/// Consumes `row` into one [`Row`] per `(family, qualifier)` column matching
/// [`flattened_row_schema`].
pub fn into_flattened_rows(mut row: RawRow) -> Result<Vec<Row>, SchemaError> {
    sort_columns(&mut row.cells);
    let schema = flattened_row_schema();
    let cell_schema = cell_schema();
    let key_bytes = row.key.as_ref();
    row.cells
        .chunk_by_mut(|a, b| a.family == b.family && a.qualifier == b.qualifier)
        .map(|col| {
            let family = String::from(&*col[0].family);
            let qualifier = Vec::<u8>::from(std::mem::take(&mut col[0].qualifier));
            let cell_values = col
                .iter_mut()
                .map(|cell| into_cell_field(cell, &cell_schema))
                .collect::<Result<Vec<_>, _>>()?;
            Row::new(
                Arc::clone(&schema),
                vec![
                    Some(FieldValue::Bytes(key_bytes.to_vec())),
                    Some(FieldValue::String(family)),
                    Some(FieldValue::Bytes(qualifier)),
                    Some(FieldValue::Array(cell_values)),
                ],
            )
        })
        .collect()
}

/// Consumes `row` into a single [`Row`] matching [`nested_row_schema`].
pub fn into_nested_row(mut row: RawRow) -> Result<Row, SchemaError> {
    sort_columns(&mut row.cells);
    let cell_schema = cell_schema();
    let family_entries = row
        .cells
        .chunk_by_mut(|a, b| a.family == b.family)
        .map(|fam_cells| {
            let family = String::from(&*fam_cells[0].family);
            let col_entries = fam_cells
                .chunk_by_mut(|a, b| a.qualifier == b.qualifier)
                .map(|col_cells| {
                    let qualifier_bytes =
                        Vec::<u8>::from(std::mem::take(&mut col_cells[0].qualifier));
                    let qualifier = String::from_utf8(qualifier_bytes)
                        .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned());
                    let cell_values = col_cells
                        .iter_mut()
                        .map(|cell| into_cell_field(cell, &cell_schema))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok((
                        FieldValue::String(qualifier),
                        Some(FieldValue::Array(cell_values)),
                    ))
                })
                .collect::<Result<Vec<_>, SchemaError>>()?;
            Ok((
                FieldValue::String(family),
                Some(FieldValue::Map(col_entries)),
            ))
        })
        .collect::<Result<Vec<_>, SchemaError>>()?;
    Row::new(
        nested_row_schema(),
        vec![
            Some(FieldValue::Bytes(Vec::<u8>::from(row.key))),
            Some(FieldValue::Map(family_entries)),
        ],
    )
}

fn into_cell_field(
    cell: &mut RawCell,
    cell_schema: &Arc<Schema>,
) -> Result<Option<FieldValue>, SchemaError> {
    let value = Vec::<u8>::from(std::mem::take(&mut cell.value));
    Row::new(
        Arc::clone(cell_schema),
        vec![
            Some(FieldValue::Bytes(value)),
            Some(FieldValue::Int64(cell.timestamp_micros)),
        ],
    )
    .map(|r| Some(FieldValue::Row(r)))
}

fn is_column_sorted(cells: &[RawCell]) -> bool {
    cells
        .windows(2)
        .all(|pair| (&pair[0].family, &pair[0].qualifier) <= (&pair[1].family, &pair[1].qualifier))
}

fn sort_columns(cells: &mut [RawCell]) {
    if !is_column_sorted(cells) {
        cells.sort_by(|a, b| (&a.family, &a.qualifier).cmp(&(&b.family, &b.qualifier)));
    }
}

fn sorted_cells(cells: &[RawCell]) -> Cow<'_, [RawCell]> {
    if is_column_sorted(cells) {
        Cow::Borrowed(cells)
    } else {
        let mut owned = cells.to_vec();
        sort_columns(&mut owned);
        Cow::Owned(owned)
    }
}

fn write_cells(cells: &[RawCell], writer: &mut Vec<u8>) -> Result<(), CoderError> {
    write_count(cells.len(), writer)?;
    cells.iter().try_for_each(|cell| {
        // 2 non-null fields: value, timestamp_micros.
        VarIntCoder::encode_varint(2, writer)?;
        VarIntCoder::encode_varint(0, writer)?;
        write_bytes(&cell.value, writer)?;
        VarIntCoder::encode_varint(cell.timestamp_micros, writer)?;
        Ok(())
    })
}

fn write_bytes(bytes: &[u8], writer: &mut Vec<u8>) -> Result<(), CoderError> {
    VarIntCoder::encode_varint(bytes.len() as i64, writer)?;
    writer.write_all(bytes)?;
    Ok(())
}

fn write_count(len: usize, writer: &mut Vec<u8>) -> Result<(), CoderError> {
    let count = i32::try_from(len)
        .map_err(|_| CoderError::Format(format!("Collection length {len} overflows i32")))?;
    writer.write_all(&count.to_be_bytes())?;
    Ok(())
}
