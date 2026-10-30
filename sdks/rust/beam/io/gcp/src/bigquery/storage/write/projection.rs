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

use arrow_io::Projection;
use arrow_io::arrow_schema::Fields;
use beam::schema::{FieldValue, Row, Schema, TypeInfo};

use crate::bigquery::storage::schema::find_field;

/// The fields of the input that the options select, before the write knows the table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldSelection {
    All,
    Keep(Vec<String>),
    Drop(Vec<String>),
    /// The fields of the row field with this name.
    Only(String),
}

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
        let index = |schema: &Schema, name: &str, option: &str| {
            schema
                .fields
                .iter()
                .position(|field| field.name == name)
                .ok_or_else(|| {
                    beam::Error::msg(format!("{option}: the input has no field '{name}'"))
                })
        };
        let names = |names: &[String], option: &str| -> beam::Result<Vec<usize>> {
            names
                .iter()
                .map(|name| index(&input, name, option))
                .collect()
        };
        let all = |schema: &Schema| (0..schema.fields.len()).collect::<Vec<_>>();
        let (only, source, mut fields) = match selection {
            FieldSelection::All => (None, Arc::clone(&input), all(&input)),
            FieldSelection::Keep(keep) => (None, Arc::clone(&input), names(keep, "keep")?),
            FieldSelection::Drop(drop) => {
                let dropped = names(drop, "drop")?;
                let kept = all(&input).into_iter().filter(|i| !dropped.contains(i));
                (None, Arc::clone(&input), kept.collect())
            }
            FieldSelection::Only(name) => {
                let field = index(&input, name, "only")?;
                let TypeInfo::Row(nested) = &input.fields[field].field_type.type_info else {
                    return Err(beam::Error::msg(format!(
                        "only: the field '{name}' is not a row"
                    )));
                };
                (Some(field), Arc::clone(nested), all(nested))
            }
        };
        fields.sort_unstable();
        fields.dedup();
        let written = schema_of(&source, &fields);
        Ok(Self {
            input,
            only,
            source,
            fields,
            written,
            ignore_unknown_values,
        })
    }

    pub fn input(&self) -> &Arc<Schema> {
        &self.input
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
                input: Arc::clone(&self.input),
                only: self.only,
                source: Arc::clone(&self.source),
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
