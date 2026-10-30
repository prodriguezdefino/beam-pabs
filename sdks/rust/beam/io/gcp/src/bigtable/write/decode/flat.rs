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

//! Decoding of single-mutation rows with
//! [`mutation_schema`](crate::bigtable::mutation_schema), from row coder bytes or a [`Row`].

use beam::coders::VarIntCoder;
use beam::schema::Row;
use bytes::Bytes;

use super::spec::{MutationSpec, build_single_entry};
use crate::bigtable::proto::mutate_rows_request::Entry;

/// Decodes `RowElement::Encoded` bytes with the field order of `mutation_schema`, with no
/// intermediate [`Row`].
pub(super) fn decode_flat_encoded(bytes: &[u8]) -> Result<Entry, String> {
    let mut cursor = bytes;
    let field_count = VarIntCoder::decode_varint(&mut cursor)
        .map_err(|e| format!("Failed to decode row field count: {e}"))?;
    if field_count != 8 {
        return Err(format!(
            "Expected 8 fields in mutation_schema row, got {field_count}"
        ));
    }
    let bitmask_len = VarIntCoder::decode_varint(&mut cursor)
        .map_err(|e| format!("Failed to decode null bitmask length: {e}"))?
        as usize;
    let bitmask = beam::coders::take_bytes(&mut cursor, bitmask_len)
        .map_err(|_| "Truncated null bitmask in mutation row".to_string())?;
    let null_mask = bitmask.first().copied().unwrap_or(0);

    let is_null = |idx: usize| -> bool { (null_mask & (1u8 << idx)) != 0 };

    if is_null(0) {
        return Err("Bigtable mutation 'key' must not be null".to_string());
    }
    let key = read_bytes_slice(&mut cursor)?;

    if is_null(1) {
        return Err("Bigtable mutation 'type' must not be null".to_string());
    }
    let type_bytes = read_bytes_slice(&mut cursor)?;
    let type_str = std::str::from_utf8(&type_bytes)
        .map_err(|e| format!("Invalid UTF-8 in mutation 'type': {e}"))?;

    let value = match is_null(2) {
        true => None,
        false => Some(read_bytes_slice(&mut cursor)?),
    };
    let qualifier = match is_null(3) {
        true => None,
        false => Some(read_bytes_slice(&mut cursor)?),
    };
    let family = match is_null(4) {
        true => None,
        false => {
            let fam_bytes = read_bytes_slice(&mut cursor)?;
            Some(
                std::str::from_utf8(&fam_bytes)
                    .map_err(|e| format!("Invalid UTF-8 in 'family_name': {e}"))?
                    .to_string(),
            )
        }
    };
    let mut read_i64 = |idx: usize, field: &str| -> Result<Option<i64>, String> {
        match is_null(idx) {
            true => Ok(None),
            false => VarIntCoder::decode_varint(&mut cursor)
                .map(Some)
                .map_err(|e| format!("Invalid '{field}': {e}")),
        }
    };
    let timestamp_micros = read_i64(5, "timestamp_micros")?;
    let start_timestamp_micros = read_i64(6, "start_timestamp_micros")?;
    let end_timestamp_micros = read_i64(7, "end_timestamp_micros")?;

    build_single_entry(
        key,
        MutationSpec {
            kind: type_str.to_string(),
            family,
            qualifier,
            value,
            timestamp_micros,
            start_timestamp_micros,
            end_timestamp_micros,
        },
    )
}

fn read_bytes_slice(cursor: &mut &[u8]) -> Result<Bytes, String> {
    let len = VarIntCoder::decode_varint(cursor)
        .map_err(|e| format!("Failed to decode byte length: {e}"))?;
    let len = usize::try_from(len).map_err(|_| format!("Negative byte length {len}"))?;
    beam::coders::take_bytes(cursor, len)
        .map(Bytes::copy_from_slice)
        .map_err(|_| "Truncated byte field in mutation row".to_string())
}

pub(super) fn decode_flat_row(row: &Row) -> Result<Entry, String> {
    let key = row
        .get_bytes("key")
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Bigtable mutation 'key' must not be null".to_string())?;
    let type_str = row
        .get_string("type")
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Bigtable mutation 'type' must not be null".to_string())?;
    let value = row
        .get_bytes("value")
        .map_err(|e| e.to_string())?
        .map(Bytes::copy_from_slice);
    let qualifier = row
        .get_bytes("column_qualifier")
        .map_err(|e| e.to_string())?
        .map(Bytes::copy_from_slice);
    let family = row
        .get_string("family_name")
        .map_err(|e| e.to_string())?
        .map(str::to_string);
    let timestamp_micros = row.get_i64("timestamp_micros").map_err(|e| e.to_string())?;
    let start_timestamp_micros = row
        .get_i64("start_timestamp_micros")
        .map_err(|e| e.to_string())?;
    let end_timestamp_micros = row
        .get_i64("end_timestamp_micros")
        .map_err(|e| e.to_string())?;

    build_single_entry(
        Bytes::copy_from_slice(key),
        MutationSpec {
            kind: type_str.to_string(),
            family,
            qualifier,
            value,
            timestamp_micros,
            start_timestamp_micros,
            end_timestamp_micros,
        },
    )
}
