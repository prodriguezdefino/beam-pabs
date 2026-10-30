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

use beam::schema::{FieldValue, MapEntry, Row};
use bytes::Bytes;

use super::spec::{MutationSpec, build_proto_mutation};
use crate::bigtable::proto::{Mutation, mutate_rows_request::Entry};

pub(super) fn decode_batched_row(row: &Row) -> Result<Entry, String> {
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

    let mutations = items
        .iter()
        .map(|item| match item {
            Some(FieldValue::Map(entries)) => decode_mutation_map(entries),
            _ => {
                Err("Each element of 'mutations' must be a non-null MAP<STRING, BYTES>".to_string())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Entry {
        row_key: Bytes::copy_from_slice(key),
        mutations,
        idempotency: None,
    })
}

/// Builds one mutation from its map. Entries with a key that is not a string or a value that
/// is not bytes, and unknown keys, have no effect.
fn decode_mutation_map(entries: &[MapEntry]) -> Result<Mutation, String> {
    let spec = entries
        .iter()
        .try_fold(MutationSpec::default(), |spec, entry| match entry {
            (FieldValue::String(name), Some(FieldValue::Bytes(bytes))) => {
                with_field(spec, name, bytes)
            }
            _ => Ok(spec),
        })?;
    if spec.kind.is_empty() {
        return Err("Mutation map is missing 'type'".to_string());
    }
    build_proto_mutation(spec)
}

/// Sets the field `name` of `spec` from `bytes`. The row keeps its values, so the strings and
/// byte buffers of the mutation are copies.
fn with_field(spec: MutationSpec, name: &str, bytes: &[u8]) -> Result<MutationSpec, String> {
    Ok(match name {
        "type" => MutationSpec {
            kind: utf8(bytes).map_err(|e| format!("Invalid UTF-8 in mutation 'type': {e}"))?,
            ..spec
        },
        "family_name" => MutationSpec {
            family: Some(utf8(bytes).map_err(|e| format!("Invalid UTF-8 in 'family_name': {e}"))?),
            ..spec
        },
        "column_qualifier" => MutationSpec {
            qualifier: Some(Bytes::copy_from_slice(bytes)),
            ..spec
        },
        "value" => MutationSpec {
            value: Some(Bytes::copy_from_slice(bytes)),
            ..spec
        },
        "timestamp_micros" => MutationSpec {
            timestamp_micros: Some(parse_timestamp_bytes(bytes)?),
            ..spec
        },
        "start_timestamp_micros" => MutationSpec {
            start_timestamp_micros: Some(parse_timestamp_bytes(bytes)?),
            ..spec
        },
        "end_timestamp_micros" => MutationSpec {
            end_timestamp_micros: Some(parse_timestamp_bytes(bytes)?),
            ..spec
        },
        _ => spec,
    })
}

fn utf8(bytes: &[u8]) -> Result<String, std::str::Utf8Error> {
    std::str::from_utf8(bytes).map(str::to_string)
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
