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

//! Unit tests for the write path: mutation decoding (both `RowElement::Encoded` and
//! `RowElement::Row`), [`BatchBuffer`], [`partition_outcomes`], and [`rate_limit_backoff`].

use std::sync::Arc;
use std::time::Duration;

use beam::schema::{FieldValue, Row, RowElement};
use gcp::bigtable::proto::mutate_rows_response::Entry as RespEntry;
use gcp::bigtable::proto::{Duration as ProtoDuration, MutateRowsResponse, RateLimitInfo, Status};
use gcp::bigtable::write::{
    BatchBuffer, BatchOutcome, MutationInputKind, decode_mutation_row, error_schema,
    partition_outcomes, rate_limit_backoff,
};
use gcp::bigtable::{BigtableMutation, batched_mutation_schema, mutation_schema};

#[test]
fn decode_mutation_row_supports_encoded_fast_path_and_row_values() {
    let schema = mutation_schema();
    let err_schema = Arc::new(error_schema(&schema));

    let m = BigtableMutation::SetCell {
        key: b"row-1".to_vec(),
        family_name: "cf".to_string(),
        column_qualifier: b"q1".to_vec(),
        value: b"v1".to_vec(),
        timestamp_micros: Some(42),
    };
    let row = Row::from(m);
    let encoded_bytes = row.to_row_bytes().unwrap();

    let from_encoded = decode_mutation_row(
        RowElement::Encoded(encoded_bytes),
        MutationInputKind::Flat,
        &schema,
        &err_schema,
    )
    .unwrap()
    .expect("valid encoded mutation");
    let from_row = decode_mutation_row(
        RowElement::Row(row),
        MutationInputKind::Flat,
        &schema,
        &err_schema,
    )
    .unwrap()
    .expect("valid row mutation");

    assert_eq!(from_encoded.entry, from_row.entry);
    assert_eq!(from_encoded.entry.row_key.as_ref(), b"row-1");
    assert_eq!(from_encoded.entry.mutations.len(), 1);
}

#[test]
fn decode_mutation_row_supports_batched_mutation_schema_and_routes_invalid_rows_to_errors() {
    let batched_schema = batched_mutation_schema();
    let err_schema = Arc::new(error_schema(&batched_schema));

    let map_entry = vec![
        (
            FieldValue::String("type".to_string()),
            Some(FieldValue::Bytes(b"SetCell".to_vec())),
        ),
        (
            FieldValue::String("family_name".to_string()),
            Some(FieldValue::Bytes(b"cf".to_vec())),
        ),
        (
            FieldValue::String("column_qualifier".to_string()),
            Some(FieldValue::Bytes(b"q".to_vec())),
        ),
        (
            FieldValue::String("value".to_string()),
            Some(FieldValue::Bytes(b"val".to_vec())),
        ),
        (
            FieldValue::String("timestamp_micros".to_string()),
            Some(FieldValue::Bytes(12345i64.to_be_bytes().to_vec())),
        ),
    ];
    let valid_row = Row::new(
        batched_schema.clone(),
        vec![
            Some(FieldValue::Bytes(b"batch-key".to_vec())),
            Some(FieldValue::Array(vec![Some(FieldValue::Map(map_entry))])),
        ],
    )
    .unwrap();

    let encoded = decode_mutation_row(
        RowElement::Encoded(valid_row.to_row_bytes().unwrap()),
        MutationInputKind::Batched,
        &batched_schema,
        &err_schema,
    )
    .unwrap()
    .expect("valid encoded batched row");
    let decoded = decode_mutation_row(
        RowElement::Row(valid_row),
        MutationInputKind::Batched,
        &batched_schema,
        &err_schema,
    )
    .unwrap()
    .expect("valid batched row");
    assert_eq!(encoded.entry, decoded.entry);
    assert_eq!(decoded.entry.row_key.as_ref(), b"batch-key");
    assert_eq!(decoded.entry.mutations.len(), 1);

    // Empty row key is rejected into an error Row rather than failing the bundle.
    let bad_row = Row::from(BigtableMutation::DeleteFromRow { key: Vec::new() });
    let flat_schema = mutation_schema();
    let flat_err_schema = Arc::new(error_schema(&flat_schema));
    let err = decode_mutation_row(
        RowElement::Row(bad_row),
        MutationInputKind::Flat,
        &flat_schema,
        &flat_err_schema,
    )
    .unwrap()
    .expect_err("empty key must produce error row");
    assert!(
        err.get_string("error_message")
            .unwrap()
            .unwrap()
            .contains("must not be empty")
    );
}

#[test]
fn batch_buffer_and_partition_outcomes_separate_succeeded_retryable_and_rejected_entries() {
    let schema = mutation_schema();
    let err_schema = Arc::new(error_schema(&schema));
    let make_entry = |k: &[u8]| {
        let row = Row::from(BigtableMutation::set_cell(k, "cf", "q", "v"));
        decode_mutation_row(
            RowElement::Row(row),
            MutationInputKind::Flat,
            &schema,
            &err_schema,
        )
        .unwrap()
        .unwrap()
    };

    let mut buffer = BatchBuffer::new(2, 1024 * 1024);
    assert!(buffer.push(make_entry(b"k1")).is_none());
    assert!(!buffer.is_ready());
    assert!(buffer.push(make_entry(b"k2")).is_none());
    assert!(buffer.is_ready());
    let flushed = buffer.push(make_entry(b"k3")).expect("flushes k1, k2");
    assert_eq!(flushed.entries.len(), 2);
    let tail = buffer.flush().expect("flushes k3");
    assert_eq!(tail.entries.len(), 1);

    // Partition 3 entries: index 0 -> OK (0), index 1 -> UNAVAILABLE (14, retryable),
    // index 2 -> INVALID_ARGUMENT (3, permanent).
    let entries = vec![make_entry(b"k1"), make_entry(b"k2"), make_entry(b"k3")];
    let resp = MutateRowsResponse {
        entries: vec![
            RespEntry {
                index: 0,
                status: Some(Status {
                    code: 0,
                    message: String::new(),
                    ..Default::default()
                }),
            },
            RespEntry {
                index: 1,
                status: Some(Status {
                    code: 14,
                    message: "tablet busy".to_string(),
                    ..Default::default()
                }),
            },
            RespEntry {
                index: 2,
                status: Some(Status {
                    code: 3,
                    message: "bad family".to_string(),
                    ..Default::default()
                }),
            },
        ],
        rate_limit_info: Some(RateLimitInfo {
            period: Some(ProtoDuration {
                seconds: 2,
                nanos: 0,
            }),
            factor: 0.7,
        }),
    };

    let outcome = partition_outcomes(entries, vec![resp], true);
    let BatchOutcome::Partial {
        retryable,
        rejected,
        server_backoff,
    } = outcome
    else {
        panic!("expected Partial outcome");
    };
    assert_eq!(retryable.len(), 1);
    assert_eq!(retryable[0].entry.row_key.as_ref(), b"k2");
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].0.entry.row_key.as_ref(), b"k3");
    assert_eq!(rejected[0].1.code, 3);
    assert!(server_backoff.unwrap() >= Duration::from_millis(500));

    // RateLimitInfo with factor >= 1.0 produces no backoff.
    assert!(
        rate_limit_backoff(&RateLimitInfo {
            period: Some(ProtoDuration {
                seconds: 5,
                nanos: 0,
            }),
            factor: 1.2,
        })
        .is_none()
    );
}
