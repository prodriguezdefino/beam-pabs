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

//! Decoder from [`RowElement`] (`mutation_schema` or `batched_mutation_schema`) into
//! [`proto::mutate_rows_request::Entry`](crate::bigtable::proto::mutate_rows_request::Entry).

mod batched;
mod flat;
mod spec;

use std::sync::Arc;

use beam::coders::{Coder, Context, RowCoder};
use beam::schema::{FieldValue, Row, RowElement, Schema};
use serde::{Deserialize, Serialize};

use crate::bigtable::proto::{mutate_rows_request::Entry, mutation};

/// Input schema layout for [`WriteMutations`](super::WriteMutations).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationInputKind {
    /// Flat single-mutation row matching [`mutation_schema`](crate::bigtable::mutation_schema).
    Flat,
    /// Batched row-mutations row matching
    /// [`batched_mutation_schema`](crate::bigtable::batched_mutation_schema)
    /// (`key: BYTES, mutations: ARRAY<MAP<STRING, BYTES>>`).
    Batched,
}

impl MutationInputKind {
    /// Detects the mutation schema kind from `schema`.
    pub fn from_schema(schema: &Schema) -> Result<Self, String> {
        let has_key = schema.fields.iter().any(|f| f.name == "key");
        let has_type = schema.fields.iter().any(|f| f.name == "type");
        let has_mutations = schema.fields.iter().any(|f| f.name == "mutations");
        match (has_key, has_type, has_mutations) {
            (true, true, false) => Ok(Self::Flat),
            (true, false, true) => Ok(Self::Batched),
            _ => Err(format!(
                "Unsupported Bigtable write input schema: expected fields (key, type, ...) or (key, mutations), got {:?}",
                schema
                    .fields
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>()
            )),
        }
    }
}

/// One decoded `MutateRows` entry with its estimated wire size.
#[derive(Clone, Debug)]
pub struct PendingEntry {
    pub entry: Entry,
    pub byte_size: usize,
}

impl PendingEntry {
    /// Materializes an error [`Row`] matching [`error_schema`](super::error_schema) from this
    /// rejected entry.
    pub fn into_error_row(
        self,
        kind: MutationInputKind,
        input_schema: &Arc<Schema>,
        err_schema: &Arc<Schema>,
        error_message: impl Into<String>,
    ) -> beam::Result<Row> {
        let failed_row = match kind {
            MutationInputKind::Flat => flat::flat_entry_to_row(self.entry, input_schema)?,
            MutationInputKind::Batched => batched::batched_entry_to_row(self.entry, input_schema)?,
        };
        wrap_error_row(failed_row, err_schema, error_message)
    }
}

/// Converts a [`RowElement`] and error message into an error [`Row`] matching `err_schema`.
pub fn element_to_error_row(
    raw_row: RowElement,
    input_schema: &Arc<Schema>,
    err_schema: &Arc<Schema>,
    error_message: impl Into<String>,
) -> beam::Result<Row> {
    let failed_row = match raw_row {
        RowElement::Row(row) => row,
        RowElement::Encoded(bytes) => decode_row(&bytes, input_schema)?,
    };
    wrap_error_row(failed_row, err_schema, error_message)
}

fn wrap_error_row(
    failed_row: Row,
    err_schema: &Arc<Schema>,
    error_message: impl Into<String>,
) -> beam::Result<Row> {
    Row::new(
        Arc::clone(err_schema),
        vec![
            Some(FieldValue::Row(failed_row)),
            Some(FieldValue::String(error_message.into())),
        ],
    )
    .map_err(beam::Error::from)
}

/// Decodes `element` into a [`PendingEntry`], or returns an error [`Row`] if the mutation is
/// invalid (for example, an empty row key, an unknown mutation type, or missing required fields).
pub fn decode_mutation_row(
    element: RowElement,
    kind: MutationInputKind,
    input_schema: &Arc<Schema>,
    err_schema: &Arc<Schema>,
) -> beam::Result<Result<PendingEntry, Row>> {
    let decoded = match (element, kind) {
        (RowElement::Encoded(vec), MutationInputKind::Flat) if is_mutation_layout(input_schema) => {
            let raw = bytes::Bytes::from(vec);
            match flat::decode_flat_encoded(&raw) {
                Ok(entry) => Ok(entry),
                Err(reason) => Err((decode_row(&raw, input_schema)?, reason)),
            }
        }
        (RowElement::Encoded(bytes), kind) => {
            decode_row_entry(decode_row(&bytes, input_schema)?, kind)
        }
        (RowElement::Row(row), kind) => decode_row_entry(row, kind),
    };

    match decoded {
        Ok(entry) => {
            let byte_size = estimate_entry_bytes(&entry);
            Ok(Ok(PendingEntry { entry, byte_size }))
        }
        Err((failed_row, reason)) => Ok(Err(wrap_error_row(failed_row, err_schema, reason)?)),
    }
}

/// The encoded fast path reads the fields by position, so it needs the field order of
/// [`mutation_schema`](crate::bigtable::mutation_schema).
fn is_mutation_layout(schema: &Schema) -> bool {
    schema.fields.len() == 8 && schema.fields.first().is_some_and(|f| f.name == "key")
}

fn decode_row_entry(row: Row, kind: MutationInputKind) -> Result<Entry, (Row, String)> {
    match kind {
        MutationInputKind::Flat => flat::decode_flat_row(row),
        MutationInputKind::Batched => batched::decode_batched_row(row),
    }
}

fn decode_row(bytes: &[u8], schema: &Arc<Schema>) -> beam::Result<Row> {
    RowCoder::new(Arc::clone(schema))
        .decode(&mut &bytes[..], Context::Nested)
        .map_err(beam::Error::from)
}

fn estimate_entry_bytes(entry: &Entry) -> usize {
    let mutations: usize = entry
        .mutations
        .iter()
        .map(|m| match &m.mutation {
            Some(mutation::Mutation::SetCell(sc)) => {
                sc.family_name.len() + sc.column_qualifier.len() + sc.value.len() + 16
            }
            Some(mutation::Mutation::DeleteFromColumn(dc)) => {
                dc.family_name.len() + dc.column_qualifier.len() + 20
            }
            Some(mutation::Mutation::DeleteFromFamily(df)) => df.family_name.len() + 8,
            Some(mutation::Mutation::DeleteFromRow(_)) | None => 4,
            // The decoders do not build aggregate mutations. Count their encoded size.
            Some(mutation::Mutation::AddToCell(_) | mutation::Mutation::MergeToCell(_)) => {
                prost::Message::encoded_len(m)
            }
        })
        .sum();
    (entry.row_key.len() + 8 + mutations).max(1)
}
