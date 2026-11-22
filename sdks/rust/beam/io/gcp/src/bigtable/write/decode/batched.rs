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

//! Decoding of row mutations with
//! [`batched_mutation_schema`](crate::bigtable::batched_mutation_schema):
//! `key: BYTES, mutations: ARRAY<MAP<STRING, BYTES>>`.

use std::sync::Arc;

use beam::schema::{FieldValue, MapEntry, Row, Schema};
use bytes::Bytes;

use super::spec::{MutationSpec, build_proto_mutation};
use crate::bigtable::proto::{Mutation, mutate_rows_request::Entry, mutation};
use crate::bigtable::{
    MUTATION_DELETE_FROM_COLUMN, MUTATION_DELETE_FROM_FAMILY, MUTATION_DELETE_FROM_ROW,
    MUTATION_SET_CELL,
};

/// Validates `row` and moves its owned byte buffers into an [`Entry`]. Returns
/// `Err((row, reason))` if `row` is invalid so the caller can emit `row` unchanged.
pub(super) fn decode_batched_row(row: Row) -> Result<Entry, (Row, String)> {
    if let Err(reason) = validate_batched_row(&row) {
        return Err((row, reason));
    }
    let schema = Arc::clone(row.schema());
    let mut values = row.into_values();
    let mut take = |name: &str| {
        schema
            .field_index(name)
            .and_then(|idx| values.get_mut(idx))
            .and_then(Option::take)
    };
    let row_key = match take("key") {
        Some(FieldValue::Bytes(vec)) => Bytes::from(vec),
        _ => Bytes::new(),
    };
    let items = match take("mutations") {
        Some(FieldValue::Array(items)) => items,
        _ => Vec::new(),
    };
    let mutations = items
        .into_iter()
        .filter_map(|item| match item {
            Some(FieldValue::Map(entries)) => decode_owned_map(entries).ok(),
            _ => None,
        })
        .collect();

    Ok(Entry {
        row_key,
        mutations,
        idempotency: None,
    })
}

fn validate_batched_row(row: &Row) -> Result<(), String> {
    let key = row
        .get_bytes("key")
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Bigtable mutation 'key' must not be null".to_string())?;
    if key.is_empty() {
        return Err("Bigtable mutation 'key' must not be empty".to_string());
    }

    let mutations_val = row
        .get_value("mutations")
        .ok_or_else(|| "Missing 'mutations' field".to_string())?;
    let Some(FieldValue::Array(items)) = mutations_val else {
        return Err("Bigtable 'mutations' field must be a non-null ARRAY".to_string());
    };
    if items.is_empty() {
        return Err("Bigtable 'mutations' array must not be empty".to_string());
    }

    items.iter().try_for_each(|item| match item {
        Some(FieldValue::Map(entries)) => validate_mutation_map(entries),
        _ => Err("Each element of 'mutations' must be a non-null MAP<STRING, BYTES>".to_string()),
    })
}

fn validate_mutation_map(entries: &[MapEntry]) -> Result<(), String> {
    let mut kind: Option<&str> = None;
    let mut family: Option<&str> = None;
    let mut has_qualifier = false;
    let mut has_value = false;
    for entry in entries {
        let (FieldValue::String(name), Some(FieldValue::Bytes(bytes))) = entry else {
            continue;
        };
        match name.as_str() {
            "type" => {
                kind = Some(
                    std::str::from_utf8(bytes)
                        .map_err(|e| format!("Invalid UTF-8 in mutation 'type': {e}"))?,
                );
            }
            "family_name" => {
                family = Some(
                    std::str::from_utf8(bytes)
                        .map_err(|e| format!("Invalid UTF-8 in 'family_name': {e}"))?,
                );
            }
            "column_qualifier" => has_qualifier = true,
            "value" => has_value = true,
            "timestamp_micros" | "start_timestamp_micros" | "end_timestamp_micros" => {
                let _ = parse_timestamp_bytes(bytes)?;
            }
            _ => {}
        }
    }
    let Some(kind) = kind.filter(|k| !k.is_empty()) else {
        return Err("Mutation map is missing 'type'".to_string());
    };
    match kind {
        MUTATION_SET_CELL => {
            let fam = family.ok_or_else(|| "SetCell requires 'family_name'".to_string())?;
            if fam.is_empty() {
                return Err("SetCell 'family_name' must not be empty".to_string());
            }
            if !has_qualifier {
                return Err("SetCell requires 'column_qualifier'".to_string());
            }
            if !has_value {
                return Err("SetCell requires 'value'".to_string());
            }
        }
        MUTATION_DELETE_FROM_COLUMN => {
            let fam =
                family.ok_or_else(|| "DeleteFromColumn requires 'family_name'".to_string())?;
            if fam.is_empty() {
                return Err("DeleteFromColumn 'family_name' must not be empty".to_string());
            }
            if !has_qualifier {
                return Err("DeleteFromColumn requires 'column_qualifier'".to_string());
            }
        }
        MUTATION_DELETE_FROM_FAMILY => {
            let fam =
                family.ok_or_else(|| "DeleteFromFamily requires 'family_name'".to_string())?;
            if fam.is_empty() {
                return Err("DeleteFromFamily 'family_name' must not be empty".to_string());
            }
        }
        MUTATION_DELETE_FROM_ROW => {}
        other => {
            return Err(format!(
                "Unknown Bigtable mutation type '{other}': expected SetCell, DeleteFromColumn, DeleteFromFamily, or DeleteFromRow"
            ));
        }
    }
    Ok(())
}

/// Builds one mutation by moving the byte buffers out of `entries`.
fn decode_owned_map(entries: Vec<MapEntry>) -> Result<Mutation, String> {
    let spec =
        entries
            .into_iter()
            .try_fold(MutationSpec::default(), |spec, entry| match entry {
                (FieldValue::String(name), Some(FieldValue::Bytes(bytes))) => {
                    with_owned_field(spec, &name, bytes)
                }
                _ => Ok(spec),
            })?;
    build_proto_mutation(spec)
}

fn with_owned_field(
    spec: MutationSpec,
    name: &str,
    bytes: Vec<u8>,
) -> Result<MutationSpec, String> {
    Ok(match name {
        "type" => MutationSpec {
            kind: String::from_utf8(bytes)
                .map_err(|e| format!("Invalid UTF-8 in mutation 'type': {e}"))?,
            ..spec
        },
        "family_name" => MutationSpec {
            family: Some(
                String::from_utf8(bytes)
                    .map_err(|e| format!("Invalid UTF-8 in 'family_name': {e}"))?,
            ),
            ..spec
        },
        "column_qualifier" => MutationSpec {
            qualifier: Some(Bytes::from(bytes)),
            ..spec
        },
        "value" => MutationSpec {
            value: Some(Bytes::from(bytes)),
            ..spec
        },
        "timestamp_micros" => MutationSpec {
            timestamp_micros: Some(parse_timestamp_bytes(&bytes)?),
            ..spec
        },
        "start_timestamp_micros" => MutationSpec {
            start_timestamp_micros: Some(parse_timestamp_bytes(&bytes)?),
            ..spec
        },
        "end_timestamp_micros" => MutationSpec {
            end_timestamp_micros: Some(parse_timestamp_bytes(&bytes)?),
            ..spec
        },
        _ => spec,
    })
}

/// Reads an 8-byte big-endian integer, or else a decimal integer string.
fn parse_timestamp_bytes(bytes: &[u8]) -> Result<i64, String> {
    if let Ok(arr) = <[u8; 8]>::try_from(bytes) {
        return Ok(i64::from_be_bytes(arr));
    }
    let s = std::str::from_utf8(bytes)
        .map_err(|_| format!("Invalid timestamp bytes of length {}", bytes.len()))?;
    s.parse::<i64>()
        .map_err(|e| format!("Invalid timestamp integer '{s}': {e}"))
}

/// Rebuilds a batched mutation [`Row`] matching `schema` from a rejected [`Entry`].
pub(super) fn batched_entry_to_row(entry: Entry, schema: &Arc<Schema>) -> beam::Result<Row> {
    let mut key = Some(Vec::from(entry.row_key));
    let mut items: Option<Vec<Option<FieldValue>>> = Some(
        entry
            .mutations
            .into_iter()
            .filter_map(|m| m.mutation.map(mutation_to_map))
            .map(|entries| Some(FieldValue::Map(entries)))
            .collect(),
    );
    let values = schema
        .fields
        .iter()
        .map(|f| match f.name.as_str() {
            "key" => key.take().map(FieldValue::Bytes),
            "mutations" => items.take().map(FieldValue::Array),
            _ => None,
        })
        .collect();
    Row::new(Arc::clone(schema), values).map_err(beam::Error::from)
}

fn map_entry(key: &str, value: Vec<u8>) -> MapEntry {
    (
        FieldValue::String(key.to_string()),
        Some(FieldValue::Bytes(value)),
    )
}

fn mutation_to_map(inner: mutation::Mutation) -> Vec<MapEntry> {
    match inner {
        mutation::Mutation::SetCell(sc) => {
            let mut out = vec![
                map_entry("type", MUTATION_SET_CELL.as_bytes().to_vec()),
                map_entry("family_name", sc.family_name.into_bytes()),
                map_entry("column_qualifier", Vec::from(sc.column_qualifier)),
                map_entry("value", Vec::from(sc.value)),
            ];
            if sc.timestamp_micros != -1 {
                out.push(map_entry(
                    "timestamp_micros",
                    sc.timestamp_micros.to_be_bytes().to_vec(),
                ));
            }
            out
        }
        mutation::Mutation::DeleteFromColumn(dc) => {
            let mut out = vec![
                map_entry("type", MUTATION_DELETE_FROM_COLUMN.as_bytes().to_vec()),
                map_entry("family_name", dc.family_name.into_bytes()),
                map_entry("column_qualifier", Vec::from(dc.column_qualifier)),
            ];
            if let Some(range) = dc.time_range {
                out.push(map_entry(
                    "start_timestamp_micros",
                    range.start_timestamp_micros.to_be_bytes().to_vec(),
                ));
                if range.end_timestamp_micros != 0 {
                    out.push(map_entry(
                        "end_timestamp_micros",
                        range.end_timestamp_micros.to_be_bytes().to_vec(),
                    ));
                }
            }
            out
        }
        mutation::Mutation::DeleteFromFamily(df) => vec![
            map_entry("type", MUTATION_DELETE_FROM_FAMILY.as_bytes().to_vec()),
            map_entry("family_name", df.family_name.into_bytes()),
        ],
        mutation::Mutation::DeleteFromRow(_)
        | mutation::Mutation::AddToCell(_)
        | mutation::Mutation::MergeToCell(_) => {
            vec![map_entry(
                "type",
                MUTATION_DELETE_FROM_ROW.as_bytes().to_vec(),
            )]
        }
    }
}
