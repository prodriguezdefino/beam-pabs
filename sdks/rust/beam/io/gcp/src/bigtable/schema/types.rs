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

//! Row domain types (`BigtableCell`, `BigtableColumn`, `BigtableMutation`) and their
//! conversions from and to [`Row`].

use std::sync::Arc;

use beam::schema::{FieldValue, Row, SchemaError};

use super::{
    CELL_SCHEMA, FLATTENED_ROW_SCHEMA, MUTATION_DELETE_FROM_COLUMN, MUTATION_DELETE_FROM_FAMILY,
    MUTATION_DELETE_FROM_ROW, MUTATION_SET_CELL, mutation_schema,
};

/// A single timestamped Bigtable cell value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BigtableCell {
    pub value: Vec<u8>,
    pub timestamp_micros: i64,
}

impl TryFrom<&Row> for BigtableCell {
    type Error = SchemaError;

    fn try_from(cell: &Row) -> Result<Self, Self::Error> {
        if Arc::ptr_eq(cell.schema(), &CELL_SCHEMA)
            && let [
                Some(FieldValue::Bytes(value)),
                Some(FieldValue::Int64(timestamp_micros)),
            ] = cell.values()
        {
            return Ok(Self {
                value: value.clone(),
                timestamp_micros: *timestamp_micros,
            });
        }
        let value = cell
            .get_bytes("value")?
            .ok_or_else(|| SchemaError::UnexpectedNull {
                expected: "BYTES".into(),
            })?
            .to_vec();
        let timestamp_micros =
            cell.get_i64("timestamp_micros")?
                .ok_or_else(|| SchemaError::UnexpectedNull {
                    expected: "INT64".into(),
                })?;
        Ok(Self {
            value,
            timestamp_micros,
        })
    }
}

impl TryFrom<&FieldValue> for BigtableCell {
    type Error = SchemaError;

    fn try_from(value: &FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Row(row) => Self::try_from(row),
            other => Err(SchemaError::ValueTypeMismatch {
                expected: "ROW".into(),
                actual: format!("{other:?}"),
            }),
        }
    }
}

/// One column of a Bigtable row, as produced by a flattened read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BigtableColumn {
    pub key: Vec<u8>,
    pub family_name: String,
    pub column_qualifier: Vec<u8>,
    /// Cell versions, newest first (the Bigtable order).
    pub cells: Vec<BigtableCell>,
}

impl BigtableColumn {
    /// Newest cell value.
    pub fn latest_value(&self) -> Option<&[u8]> {
        self.cells.first().map(|c| c.value.as_slice())
    }
}

impl TryFrom<&Row> for BigtableColumn {
    type Error = SchemaError;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        if Arc::ptr_eq(row.schema(), &FLATTENED_ROW_SCHEMA)
            && let [
                Some(FieldValue::Bytes(key)),
                Some(FieldValue::String(family_name)),
                Some(FieldValue::Bytes(column_qualifier)),
                Some(FieldValue::Array(items)),
            ] = row.values()
        {
            let cells = items
                .iter()
                .map(|c| match c {
                    Some(val) => BigtableCell::try_from(val),
                    None => Err(SchemaError::UnexpectedNull {
                        expected: "cell".into(),
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(Self {
                key: key.clone(),
                family_name: family_name.clone(),
                column_qualifier: column_qualifier.clone(),
                cells,
            });
        }
        let key = row
            .get_bytes("key")?
            .ok_or_else(|| SchemaError::UnexpectedNull {
                expected: "BYTES".into(),
            })?
            .to_vec();
        let family_name = row
            .get_string("family_name")?
            .ok_or_else(|| SchemaError::UnexpectedNull {
                expected: "STRING".into(),
            })?
            .to_string();
        let column_qualifier = row
            .get_bytes("column_qualifier")?
            .ok_or_else(|| SchemaError::UnexpectedNull {
                expected: "BYTES".into(),
            })?
            .to_vec();
        let cells = match row
            .get_value("cells")
            .ok_or_else(|| SchemaError::FieldNotFound("cells".into()))?
        {
            Some(FieldValue::Array(items)) => items
                .iter()
                .map(|c| match c {
                    Some(val) => BigtableCell::try_from(val),
                    None => Err(SchemaError::UnexpectedNull {
                        expected: "cell".into(),
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?,
            Some(other) => {
                return Err(SchemaError::TypeMismatch {
                    field: "cells".into(),
                    expected: "ARRAY<cell>".into(),
                    actual: format!("{other:?}"),
                });
            }
            None => Vec::new(),
        };
        Ok(Self {
            key,
            family_name,
            column_qualifier,
            cells,
        })
    }
}

impl TryFrom<Row> for BigtableColumn {
    type Error = SchemaError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Self::try_from(&row)
    }
}

/// A single Bigtable mutation convertible to a row with [`mutation_schema`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BigtableMutation {
    /// Writes `value` to `family_name:column_qualifier`; a `None` timestamp is server-assigned.
    SetCell {
        key: Vec<u8>,
        family_name: String,
        column_qualifier: Vec<u8>,
        value: Vec<u8>,
        timestamp_micros: Option<i64>,
    },
    /// Deletes cells of one column, optionally restricted to `[start, end)` micros.
    DeleteFromColumn {
        key: Vec<u8>,
        family_name: String,
        column_qualifier: Vec<u8>,
        start_timestamp_micros: Option<i64>,
        end_timestamp_micros: Option<i64>,
    },
    /// Deletes every cell of a column family.
    DeleteFromFamily { key: Vec<u8>, family_name: String },
    /// Deletes an entire row.
    DeleteFromRow { key: Vec<u8> },
}

impl BigtableMutation {
    /// Creates a [`BigtableMutation::SetCell`] with a server timestamp.
    pub fn set_cell(
        key: impl Into<Vec<u8>>,
        family_name: impl Into<String>,
        column_qualifier: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        Self::SetCell {
            key: key.into(),
            family_name: family_name.into(),
            column_qualifier: column_qualifier.into(),
            value: value.into(),
            timestamp_micros: None,
        }
    }

    /// Row key targeted by this mutation.
    pub fn key(&self) -> &[u8] {
        match self {
            Self::SetCell { key, .. }
            | Self::DeleteFromColumn { key, .. }
            | Self::DeleteFromFamily { key, .. }
            | Self::DeleteFromRow { key } => key,
        }
    }

    /// Returns the `type` string that the SchemaTransform provider dispatches on.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::SetCell { .. } => MUTATION_SET_CELL,
            Self::DeleteFromColumn { .. } => MUTATION_DELETE_FROM_COLUMN,
            Self::DeleteFromFamily { .. } => MUTATION_DELETE_FROM_FAMILY,
            Self::DeleteFromRow { .. } => MUTATION_DELETE_FROM_ROW,
        }
    }
}

impl From<BigtableMutation> for Row {
    fn from(mutation: BigtableMutation) -> Self {
        let type_name = mutation.type_name();
        let (key, value, qualifier, family, ts, start, end) = match mutation {
            BigtableMutation::SetCell {
                key,
                family_name,
                column_qualifier,
                value,
                timestamp_micros,
            } => (
                key,
                Some(FieldValue::Bytes(value)),
                Some(FieldValue::Bytes(column_qualifier)),
                Some(FieldValue::String(family_name)),
                timestamp_micros.map(FieldValue::Int64),
                None,
                None,
            ),
            BigtableMutation::DeleteFromColumn {
                key,
                family_name,
                column_qualifier,
                start_timestamp_micros,
                end_timestamp_micros,
            } => (
                key,
                None,
                Some(FieldValue::Bytes(column_qualifier)),
                Some(FieldValue::String(family_name)),
                None,
                start_timestamp_micros.map(FieldValue::Int64),
                end_timestamp_micros.map(FieldValue::Int64),
            ),
            BigtableMutation::DeleteFromFamily { key, family_name } => (
                key,
                None,
                None,
                Some(FieldValue::String(family_name)),
                None,
                None,
                None,
            ),
            BigtableMutation::DeleteFromRow { key } => (key, None, None, None, None, None, None),
        };
        Row::new(
            mutation_schema(),
            vec![
                Some(FieldValue::Bytes(key)),
                Some(FieldValue::String(type_name.to_string())),
                value,
                qualifier,
                family,
                ts,
                start,
                end,
            ],
        )
        .expect("mutation row matches mutation_schema")
    }
}
