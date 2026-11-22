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

//! The fields of the input rows that a write puts in the table.

use std::sync::Arc;

use beam::schema::{FieldValue, Row, Schema, TypeInfo};
use beam_io_arrow::arrow_schema::Fields;
use beam_io_arrow::{FieldSelection, Projection};

use crate::bigquery::schema::find_field;

/// The fields that a write takes from each input row. The write computes it once, when it
/// expands, so the writers map the fields by index.
#[derive(Debug)]
pub struct Selection {
    input: Arc<Schema>,
    /// The index of the row field that holds the fields, or `None` for the fields of the input.
    only: Option<usize>,
    /// The schema that holds the fields: the input, or the schema of the `only` field.
    source: Arc<Schema>,
    /// The indexes in `source` of the selected fields, in schema order.
    fields: Vec<usize>,
    /// The selected fields. A table that the write creates has these columns.
    written: Arc<Schema>,
    /// Input schema for error rows: fields omitted from written rows are nullable.
    error_input: Arc<Schema>,
    /// Source schema for restored error rows: matches `error_input` or its `only` field.
    error_source: Arc<Schema>,
    ignore_unknown_values: bool,
}

impl Selection {
    /// Fails if a name in `selection` is not a field of `input`, or if the `only` field is not
    /// a row.
    pub fn new(
        input: Arc<Schema>,
        selection: &FieldSelection,
        ignore_unknown_values: bool,
    ) -> beam::Result<Self> {
        let (projection, written) = selection.projection(&input).map_err(beam::Error::msg)?;
        let source = Arc::clone(projection.source(&input).map_err(beam::Error::msg)?);
        let (only, fields) = match projection {
            Projection::Fields(fields) => (None, fields),
            Projection::Unnest { field, fields } => (Some(field), fields),
        };
        let (error_input, error_source) = error_schemas(
            &input,
            &source,
            only,
            &fields,
            matches!(selection, FieldSelection::All),
            ignore_unknown_values,
        );
        let written = Arc::new(written);
        Ok(Self {
            input,
            only,
            source,
            fields,
            written,
            error_input,
            error_source,
            ignore_unknown_values,
        })
    }

    pub fn input(&self) -> &Arc<Schema> {
        &self.input
    }

    /// Input schema used for the `failed_row` field of error rows. Unselected fields, outer
    /// fields of `only`, and all source fields under `ignore_unknown_values` are nullable so
    /// restored rows encode with valid nullability.
    pub fn error_input(&self) -> &Arc<Schema> {
        &self.error_input
    }

    pub fn written(&self) -> &Arc<Schema> {
        &self.written
    }

    /// The selection of the fields that `table` has columns for. With `ignore_unknown_values`,
    /// a field that the table lacks is not written. Without it, such a field is an error.
    pub fn for_table(&self, table: &Fields) -> beam::Result<TableSelection> {
        let fields = self
            .fields
            .iter()
            .copied()
            .filter_map(|index| {
                let name = &self.source.fields[index].name;
                match (find_field(table, name), self.ignore_unknown_values) {
                    (Ok(_), _) => Some(Ok(index)),
                    (Err(_), true) => None,
                    (Err(e), false) => Some(Err(e)),
                }
            })
            .collect::<beam::Result<Vec<_>>>()?;
        let unchanged = self.only.is_none() && fields.len() == self.input.fields.len();
        let written = match (unchanged, fields == self.fields) {
            (true, _) => Arc::clone(&self.input),
            (false, true) => Arc::clone(&self.written),
            (false, false) => schema_of(&self.source, &fields),
        };
        let projection = match self.only {
            Some(field) => Projection::Unnest {
                field,
                fields: fields.clone(),
            },
            None => Projection::Fields(fields.clone()),
        };
        Ok(TableSelection {
            projection,
            written,
            restore: (!unchanged).then(|| Restore {
                input: Arc::clone(&self.error_input),
                only: self.only,
                source: Arc::clone(&self.error_source),
                fields,
            }),
        })
    }
}

/// The fields of the input rows that a write puts in one table.
pub struct TableSelection {
    /// The fields that the Arrow conversion takes from each input row.
    pub projection: Projection,
    /// The schema of the fields in the batches.
    pub written: Arc<Schema>,
    /// How to make an input row from a written row, or `None` if the two are the same.
    pub restore: Option<Restore>,
}

impl TableSelection {
    /// A row of the input schema with the values of `row`, a row of the written fields. The
    /// fields that the write does not write are null.
    pub fn input_row(&self, row: Row) -> beam::Result<Row> {
        match &self.restore {
            None => Ok(row),
            Some(restore) => restore.input_row(&row),
        }
    }
}

/// How to make an input row from a written row.
#[derive(Debug)]
pub struct Restore {
    input: Arc<Schema>,
    only: Option<usize>,
    source: Arc<Schema>,
    fields: Vec<usize>,
}

impl Restore {
    fn input_row(&self, row: &Row) -> beam::Result<Row> {
        let mut values = vec![None; self.source.fields.len()];
        self.fields
            .iter()
            .zip(row.values())
            .for_each(|(index, value)| values[*index] = value.clone());
        let source = Row::new(Arc::clone(&self.source), values).map_err(beam::Error::msg)?;
        match self.only {
            None => Ok(source),
            Some(field) => {
                let mut values = vec![None; self.input.fields.len()];
                values[field] = Some(FieldValue::Row(source));
                Row::new(Arc::clone(&self.input), values).map_err(beam::Error::msg)
            }
        }
    }
}

/// The fields of `schema` at `fields`.
fn schema_of(schema: &Schema, fields: &[usize]) -> Arc<Schema> {
    Arc::new(Schema::new(
        fields.iter().map(|i| schema.fields[*i].clone()).collect(),
    ))
}

/// Builds `(error_input, error_source)` with every field that [`Restore::input_row`] can set
/// to null marked nullable.
fn error_schemas(
    input: &Arc<Schema>,
    source: &Arc<Schema>,
    only: Option<usize>,
    fields: &[usize],
    is_all: bool,
    ignore_unknown_values: bool,
) -> (Arc<Schema>, Arc<Schema>) {
    if is_all && !ignore_unknown_values {
        return (Arc::clone(input), Arc::clone(source));
    }
    match only {
        None => {
            let error_input =
                with_nullable_fields(input, |i| ignore_unknown_values || !fields.contains(&i));
            (Arc::clone(&error_input), error_input)
        }
        Some(only_index) => {
            let error_source = if ignore_unknown_values {
                with_nullable_fields(source, |_| true)
            } else {
                Arc::clone(source)
            };
            let mut out = (**input).clone();
            for (index, field) in out.fields.iter_mut().enumerate() {
                if index == only_index {
                    field.field_type.type_info = TypeInfo::Row(Arc::clone(&error_source));
                } else {
                    field.field_type.nullable = true;
                }
            }
            (Arc::new(out), error_source)
        }
    }
}

/// Clones `schema` and marks every field for which `is_nullable` returns `true` as nullable.
fn with_nullable_fields(schema: &Schema, is_nullable: impl Fn(usize) -> bool) -> Arc<Schema> {
    let mut out = schema.clone();
    for (index, field) in out.fields.iter_mut().enumerate() {
        if is_nullable(index) {
            field.field_type.nullable = true;
        }
    }
    Arc::new(out)
}
