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

//! Validation of the decoded mutation fields and conversion into a proto [`Mutation`].

use bytes::Bytes;

use crate::bigtable::proto::{Mutation, TimestampRange, mutate_rows_request::Entry, mutation};
use crate::bigtable::{
    MUTATION_DELETE_FROM_COLUMN, MUTATION_DELETE_FROM_FAMILY, MUTATION_DELETE_FROM_ROW,
    MUTATION_SET_CELL,
};

/// The fields of one mutation, before the checks of its `kind`.
#[derive(Default)]
pub(super) struct MutationSpec {
    pub(super) kind: String,
    pub(super) family: Option<String>,
    pub(super) qualifier: Option<Bytes>,
    pub(super) value: Option<Bytes>,
    pub(super) timestamp_micros: Option<i64>,
    pub(super) start_timestamp_micros: Option<i64>,
    pub(super) end_timestamp_micros: Option<i64>,
}

pub(super) fn build_single_entry(row_key: Bytes, spec: MutationSpec) -> Result<Entry, String> {
    if row_key.is_empty() {
        return Err("Bigtable mutation 'key' must not be empty".to_string());
    }
    let mutation = build_proto_mutation(spec)?;
    Ok(Entry {
        row_key,
        mutations: vec![mutation],
        idempotency: None,
    })
}

pub(super) fn build_proto_mutation(spec: MutationSpec) -> Result<Mutation, String> {
    let inner = match spec.kind.as_str() {
        MUTATION_SET_CELL => {
            let family_name = spec
                .family
                .ok_or_else(|| "SetCell requires 'family_name'".to_string())?;
            if family_name.is_empty() {
                return Err("SetCell 'family_name' must not be empty".to_string());
            }
            let column_qualifier = spec
                .qualifier
                .ok_or_else(|| "SetCell requires 'column_qualifier'".to_string())?;
            let cell_value = spec
                .value
                .ok_or_else(|| "SetCell requires 'value'".to_string())?;
            mutation::Mutation::SetCell(mutation::SetCell {
                family_name,
                column_qualifier,
                timestamp_micros: spec.timestamp_micros.unwrap_or(-1),
                value: cell_value,
            })
        }
        MUTATION_DELETE_FROM_COLUMN => {
            let family_name = spec
                .family
                .ok_or_else(|| "DeleteFromColumn requires 'family_name'".to_string())?;
            if family_name.is_empty() {
                return Err("DeleteFromColumn 'family_name' must not be empty".to_string());
            }
            let column_qualifier = spec
                .qualifier
                .ok_or_else(|| "DeleteFromColumn requires 'column_qualifier'".to_string())?;
            let time_range = match (spec.start_timestamp_micros, spec.end_timestamp_micros) {
                (None, None) => None,
                (start, end) => Some(TimestampRange {
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
            let family_name = spec
                .family
                .ok_or_else(|| "DeleteFromFamily requires 'family_name'".to_string())?;
            if family_name.is_empty() {
                return Err("DeleteFromFamily 'family_name' must not be empty".to_string());
            }
            mutation::Mutation::DeleteFromFamily(mutation::DeleteFromFamily { family_name })
        }
        MUTATION_DELETE_FROM_ROW => mutation::Mutation::DeleteFromRow(mutation::DeleteFromRow {}),
        other => {
            return Err(format!(
                "Unknown Bigtable mutation type '{other}': expected SetCell, DeleteFromColumn, DeleteFromFamily, or DeleteFromRow"
            ));
        }
    };
    Ok(Mutation {
        mutation: Some(inner),
        timestamp_origin: mutation::TimestampOrigin::Unspecified.into(),
    })
}
