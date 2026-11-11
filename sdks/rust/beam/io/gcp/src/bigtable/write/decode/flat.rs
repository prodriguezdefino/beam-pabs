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

use std::sync::Arc;

use beam::coders::VarIntCoder;
use beam::schema::{FieldValue, Row, Schema};
use bytes::Bytes;

use super::spec::{MutationSpec, build_single_entry};
use crate::bigtable::proto::{mutate_rows_request::Entry, mutation};
use crate::bigtable::{
    MUTATION_DELETE_FROM_COLUMN, MUTATION_DELETE_FROM_FAMILY, MUTATION_DELETE_FROM_ROW,
    MUTATION_SET_CELL,
};

/// Decodes `RowElement::Encoded` bytes with the field order of `mutation_schema`, slicing
/// byte fields directly from `raw` with no intermediate [`Row`] or buffer copy.
pub(super) fn decode_flat_encoded(raw: &Bytes) -> Result<Entry, String> {
    let mut cursor = raw.as_ref();
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
    let key = read_bytes(raw, &mut cursor)?;

    if is_null(1) {
        return Err("Bigtable mutation 'type' must not be null".to_string());
    }
    let kind = read_utf8(&mut cursor, "type")?;

    let value = if is_null(2) {
        None
    } else {
        Some(read_bytes(raw, &mut cursor)?)
    };
    let qualifier = if is_null(3) {
        None
    } else {
        Some(read_bytes(raw, &mut cursor)?)
    };
    let family = if is_null(4) {
        None
    } else {
        Some(read_utf8(&mut cursor, "family_name")?)
    };
    let mut read_i64 = |idx: usize, field: &str| -> Result<Option<i64>, String> {
        if is_null(idx) {
            Ok(None)
        } else {
            VarIntCoder::decode_varint(&mut cursor)
                .map(Some)
                .map_err(|e| format!("Invalid '{field}': {e}"))
        }
    };
    let timestamp_micros = read_i64(5, "timestamp_micros")?;
    let start_timestamp_micros = read_i64(6, "start_timestamp_micros")?;
    let end_timestamp_micros = read_i64(7, "end_timestamp_micros")?;

    build_single_entry(
        key,
        MutationSpec {
            kind,
            family,
            qualifier,
            value,
            timestamp_micros,
            start_timestamp_micros,
            end_timestamp_micros,
        },
    )
}

fn read_bytes(raw: &Bytes, cursor: &mut &[u8]) -> Result<Bytes, String> {
    let len = VarIntCoder::decode_varint(cursor)
        .map_err(|e| format!("Failed to decode byte length: {e}"))?;
    let len = usize::try_from(len).map_err(|_| format!("Negative byte length {len}"))?;
    let offset = raw.len() - cursor.len();
    let _ = beam::coders::take_bytes(cursor, len)
        .map_err(|_| "Truncated byte field in mutation row".to_string())?;
    Ok(raw.slice(offset..offset + len))
}

fn read_utf8(cursor: &mut &[u8], field: &str) -> Result<String, String> {
    let len = VarIntCoder::decode_varint(cursor)
        .map_err(|e| format!("Failed to decode byte length: {e}"))?;
    let len = usize::try_from(len).map_err(|_| format!("Negative byte length {len}"))?;
    let slice = beam::coders::take_bytes(cursor, len)
        .map_err(|_| "Truncated byte field in mutation row".to_string())?;
    std::str::from_utf8(slice)
        .map(str::to_string)
        .map_err(|e| format!("Invalid UTF-8 in '{field}': {e}"))
}

/// Validates `row` and moves its owned byte and string fields into an [`Entry`]. Returns
/// `Err((row, reason))` if `row` is invalid so the caller can emit `row` unchanged.
pub(super) fn decode_flat_row(row: Row) -> Result<Entry, (Row, String)> {
    if let Err(reason) = validate_flat_row(&row) {
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
    let kind = match take("type") {
        Some(FieldValue::String(s)) => s,
        _ => String::new(),
    };
    let value = match take("value") {
        Some(FieldValue::Bytes(vec)) => Bytes::from(vec),
        _ => Bytes::new(),
    };
    let column_qualifier = match take("column_qualifier") {
        Some(FieldValue::Bytes(vec)) => Bytes::from(vec),
        _ => Bytes::new(),
    };
    let family_name = match take("family_name") {
        Some(FieldValue::String(s)) => s,
        _ => String::new(),
    };
    let take_i64 = |val: Option<FieldValue>| match val {
        Some(FieldValue::Int64(n)) => Some(n),
        Some(FieldValue::Int32(n)) => Some(i64::from(n)),
        _ => None,
    };
    let timestamp_micros = take_i64(take("timestamp_micros"));
    let start_timestamp_micros = take_i64(take("start_timestamp_micros"));
    let end_timestamp_micros = take_i64(take("end_timestamp_micros"));

    let inner = match kind.as_str() {
        MUTATION_SET_CELL => mutation::Mutation::SetCell(mutation::SetCell {
            family_name,
            column_qualifier,
            timestamp_micros: timestamp_micros.unwrap_or(-1),
            value,
        }),
        MUTATION_DELETE_FROM_COLUMN => {
            let time_range = match (start_timestamp_micros, end_timestamp_micros) {
                (None, None) => None,
                (start, end) => Some(crate::bigtable::proto::TimestampRange {
                    start_timestamp_micros: start.unwrap_or(0),
                    end_timestamp_micros: end.unwrap_or(0),
                }),
            };
            mutation::Mutation::DeleteFromColumn(mutation::DeleteFromColumn {
                family_name,
                column_qualifier,
                time_range,
            })
        }
        MUTATION_DELETE_FROM_FAMILY => {
            mutation::Mutation::DeleteFromFamily(mutation::DeleteFromFamily { family_name })
        }
        _ => mutation::Mutation::DeleteFromRow(mutation::DeleteFromRow {}),
    };
    Ok(Entry {
        row_key,
        mutations: vec![crate::bigtable::proto::Mutation {
            mutation: Some(inner),
            timestamp_origin: mutation::TimestampOrigin::Unspecified.into(),
        }],
        idempotency: None,
    })
}

fn validate_flat_row(row: &Row) -> Result<(), String> {
    let key = row
        .get_bytes("key")
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Bigtable mutation 'key' must not be null".to_string())?;
    if key.is_empty() {
        return Err("Bigtable mutation 'key' must not be empty".to_string());
    }
    let kind = row
        .get_string("type")
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Bigtable mutation 'type' must not be null".to_string())?;
    let value = row.get_bytes("value").ok().flatten();
    let qualifier = row.get_bytes("column_qualifier").ok().flatten();
    let family = row.get_string("family_name").ok().flatten();
    if row.schema().field_index("value").is_some() {
        row.get_bytes("value").map_err(|e| e.to_string())?;
    }
    if row.schema().field_index("column_qualifier").is_some() {
        row.get_bytes("column_qualifier")
            .map_err(|e| e.to_string())?;
    }
    if row.schema().field_index("family_name").is_some() {
        row.get_string("family_name").map_err(|e| e.to_string())?;
    }
    for field in [
        "timestamp_micros",
        "start_timestamp_micros",
        "end_timestamp_micros",
    ] {
        if row.schema().field_index(field).is_some() {
            row.get_i64(field).map_err(|e| e.to_string())?;
        }
    }
    match kind {
        MUTATION_SET_CELL => {
            let fam = family.ok_or_else(|| "SetCell requires 'family_name'".to_string())?;
            if fam.is_empty() {
                return Err("SetCell 'family_name' must not be empty".to_string());
            }
            qualifier.ok_or_else(|| "SetCell requires 'column_qualifier'".to_string())?;
            value.ok_or_else(|| "SetCell requires 'value'".to_string())?;
        }
        MUTATION_DELETE_FROM_COLUMN => {
            let fam =
                family.ok_or_else(|| "DeleteFromColumn requires 'family_name'".to_string())?;
            if fam.is_empty() {
                return Err("DeleteFromColumn 'family_name' must not be empty".to_string());
            }
            qualifier.ok_or_else(|| "DeleteFromColumn requires 'column_qualifier'".to_string())?;
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

/// Rebuilds a flat mutation [`Row`] matching `schema` from a rejected [`Entry`].
pub(super) fn flat_entry_to_row(entry: Entry, schema: &Arc<Schema>) -> beam::Result<Row> {
    let key = Vec::from(entry.row_key);
    let inner = entry.mutations.into_iter().next().and_then(|m| m.mutation);
    let (kind, family, qualifier, value, ts, start_ts, end_ts) = match inner {
        Some(mutation::Mutation::SetCell(sc)) => (
            MUTATION_SET_CELL,
            Some(sc.family_name),
            Some(Vec::from(sc.column_qualifier)),
            Some(Vec::from(sc.value)),
            (sc.timestamp_micros != -1).then_some(sc.timestamp_micros),
            None,
            None,
        ),
        Some(mutation::Mutation::DeleteFromColumn(dc)) => {
            let (start, end) = match dc.time_range {
                Some(r) => (
                    Some(r.start_timestamp_micros),
                    (r.end_timestamp_micros != 0).then_some(r.end_timestamp_micros),
                ),
                None => (None, None),
            };
            (
                MUTATION_DELETE_FROM_COLUMN,
                Some(dc.family_name),
                Some(Vec::from(dc.column_qualifier)),
                None,
                None,
                start,
                end,
            )
        }
        Some(mutation::Mutation::DeleteFromFamily(df)) => (
            MUTATION_DELETE_FROM_FAMILY,
            Some(df.family_name),
            None,
            None,
            None,
            None,
            None,
        ),
        Some(
            mutation::Mutation::DeleteFromRow(_)
            | mutation::Mutation::AddToCell(_)
            | mutation::Mutation::MergeToCell(_),
        )
        | None => (MUTATION_DELETE_FROM_ROW, None, None, None, None, None, None),
    };

    let mut key = Some(key);
    let mut family = family;
    let mut qualifier = qualifier;
    let mut value = value;
    let values = schema
        .fields
        .iter()
        .map(|f| match f.name.as_str() {
            "key" => key.take().map(FieldValue::Bytes),
            "type" => Some(FieldValue::String(kind.to_string())),
            "value" => value.take().map(FieldValue::Bytes),
            "column_qualifier" => qualifier.take().map(FieldValue::Bytes),
            "family_name" => family.take().map(FieldValue::String),
            "timestamp_micros" => ts.map(FieldValue::Int64),
            "start_timestamp_micros" => start_ts.map(FieldValue::Int64),
            "end_timestamp_micros" => end_ts.map(FieldValue::Int64),
            _ => None,
        })
        .collect();
    Row::new(Arc::clone(schema), values).map_err(beam::Error::from)
}
