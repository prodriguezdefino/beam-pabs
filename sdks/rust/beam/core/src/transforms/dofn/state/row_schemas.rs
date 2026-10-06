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

//! The row schemas of dynamic `Row` state cells.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use super::UserStateError;
use crate::schema::{Row, Schema};

/// The row schemas of the dynamic [`Row`] values and map keys of the state cells of a
/// function, by state id.
///
/// A dynamic `Row` has no schema at compile time, so give one here for each cell that holds
/// it. The expansion fails without it. A coder of a cell can hold at most one dynamic `Row`.
/// A `#[derive(BeamRow)]` type needs no entry.
#[derive(Clone, Debug, Default)]
pub struct StateRowSchemas {
    values: HashMap<String, Arc<Schema>>,
    keys: HashMap<String, Arc<Schema>>,
}

impl StateRowSchemas {
    /// Creates an empty set of schemas.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the schema of the `Row` values of cell `state_id`.
    pub fn with_value(mut self, state_id: impl Into<String>, schema: Arc<Schema>) -> Self {
        self.values.insert(state_id.into(), schema);
        self
    }

    /// Sets the schema of the `Row` map keys of the map, set or multimap cell `state_id`.
    pub fn with_key(mut self, state_id: impl Into<String>, schema: Arc<Schema>) -> Self {
        self.keys.insert(state_id.into(), schema);
        self
    }

    /// The schema of the `Row` values of cell `state_id`.
    pub fn value(&self, state_id: &str) -> Option<&Arc<Schema>> {
        self.values.get(state_id)
    }

    /// The schema of the `Row` map keys of cell `state_id`.
    pub fn key(&self, state_id: &str) -> Option<&Arc<Schema>> {
        self.keys.get(state_id)
    }

    /// The state ids that have a schema.
    pub(crate) fn state_ids(&self) -> impl Iterator<Item = &str> {
        self.values
            .keys()
            .chain(self.keys.keys())
            .map(String::as_str)
    }
}

/// Returns an error when `value` is a dynamic [`Row`] with a schema other than `cell`, the
/// schema of its state cell. The decode of such a row with `cell` gives wrong values.
pub(super) fn check_row_schema<V: Any>(
    value: &V,
    cell: Option<&Arc<Schema>>,
    state_id: &str,
) -> Result<(), UserStateError> {
    let (Some(row), Some(cell)) = ((value as &dyn Any).downcast_ref::<Row>(), cell) else {
        return Ok(());
    };
    let schema = row.schema();
    if Arc::ptr_eq(schema, cell) || same_layout(schema, cell) {
        return Ok(());
    }
    Err(UserStateError::Format(format!(
        "the schema of a Row written to state '{state_id}' is not the schema of the cell: \
         row fields {:?}, cell fields {:?}",
        names(schema),
        names(cell)
    )))
}

fn names(schema: &Schema) -> Vec<&str> {
    schema.fields.iter().map(|f| f.name.as_str()).collect()
}

/// True if rows of `a` and `b` have the same wire layout and field names.
fn same_layout(a: &Schema, b: &Schema) -> bool {
    a.fields.len() == b.fields.len()
        && a.fields.iter().zip(&b.fields).all(|(x, y)| {
            x.name == y.name
                && x.field_type == y.field_type
                && x.encoding_position == y.encoding_position
        })
}
