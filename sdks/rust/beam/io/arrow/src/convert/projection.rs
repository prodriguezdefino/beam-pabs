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

//! The fields of a Beam schema that a conversion puts in the batch.

use arrow_schema::Schema as ArrowSchema;
use beam::schema::{Schema, TypeInfo};

use super::columns::{Column, RowColumns, Slot};
use super::skip::Skip;
use crate::error::{ArrowBridgeError, Result};
use crate::schema::beam_to_arrow_schema;

/// The fields of a Beam schema that a conversion puts in the batch, by field index.
///
/// The batch keeps the schema order of the fields. The conversion builds no column for the
/// other fields, and the reader of row coder bytes steps over their values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Projection {
    /// The fields at these indexes.
    Fields(Vec<usize>),
    /// The fields at `fields` of the nested row at index `field`. They become the columns of
    /// the batch. A row where the nested row is null does not convert.
    Unnest { field: usize, fields: Vec<usize> },
}

impl Projection {
    /// The columns of the conversion of rows of `schema`, and the schema of the batch.
    pub(super) fn columns(&self, schema: &Schema) -> Result<(RowColumns, ArrowSchema)> {
        match self {
            Self::Fields(fields) => {
                let selected = selected(schema, fields)?;
                let columns = RowColumns::with_slots(None, schema, slot(&selected))?;
                Ok((columns, arrow_schema(schema, &selected)?))
            }
            Self::Unnest { field, fields } => {
                let nested = schema
                    .fields
                    .get(*field)
                    .ok_or_else(|| missing(schema, *field))?;
                let TypeInfo::Row(nested_schema) = &nested.field_type.type_info else {
                    return Err(ArrowBridgeError::TypeMismatch {
                        field: nested.name.clone(),
                        expected: "ROW".to_string(),
                        actual: nested.field_type.to_string(),
                    });
                };
                let selected = selected(nested_schema, fields)?;
                let columns = RowColumns::with_slots(None, schema, |index, name, field_type| {
                    if index == *field {
                        RowColumns::with_slots(Some(&name), nested_schema, slot(&selected))
                            .map(Slot::Unnest)
                    } else {
                        Ok(Slot::Skip {
                            skip: Skip::new(field_type),
                            name,
                        })
                    }
                })?;
                Ok((columns, arrow_schema(nested_schema, &selected)?))
            }
        }
    }
}

/// Whether each field of `schema` is in `fields`.
fn selected(schema: &Schema, fields: &[usize]) -> Result<Vec<bool>> {
    let mut selected = vec![false; schema.fields.len()];
    fields.iter().try_for_each(|&index| {
        *selected
            .get_mut(index)
            .ok_or_else(|| missing(schema, index))? = true;
        Ok::<_, ArrowBridgeError>(())
    })?;
    Ok(selected)
}

/// A column for each selected field, and a skip for each other field.
fn slot(
    selected: &[bool],
) -> impl FnMut(usize, String, &beam::schema::FieldType) -> Result<Slot> + '_ {
    |index, name, field_type| {
        if selected[index] {
            Column::new(name, field_type).map(Slot::Read)
        } else {
            Ok(Slot::Skip {
                skip: Skip::new(field_type),
                name,
            })
        }
    }
}

/// The Arrow schema of the selected fields of `schema`.
fn arrow_schema(schema: &Schema, selected: &[bool]) -> Result<ArrowSchema> {
    let fields = schema
        .fields
        .iter()
        .zip(selected)
        .filter(|(_, selected)| **selected)
        .map(|(field, _)| field.clone())
        .collect();
    beam_to_arrow_schema(&Schema::new(fields))
}

fn missing(schema: &Schema, index: usize) -> ArrowBridgeError {
    ArrowBridgeError::MissingField(format!(
        "field index {index} of a schema with {} fields",
        schema.fields.len()
    ))
}
