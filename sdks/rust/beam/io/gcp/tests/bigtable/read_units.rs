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

//! Unit tests for the read path: [`RowAssembler`], direct `beam:coder:row:v1` encoders,
//! [`SampleCoalescer`], and [`KeyRangeRestriction`] / [`KeyRangeTracker`].

use std::sync::Arc;

use bytes::Bytes;

use beam::coders::DefaultCoder;
use beam::schema::Row;
use beam::transforms::sdf::RestrictionTracker;
use gcp::bigtable::proto::read_rows_response::CellChunk;
use gcp::bigtable::proto::read_rows_response::cell_chunk::RowStatus;
use gcp::bigtable::proto::{BytesValue, ReadRowsResponse, SampleRowKeysResponse, StringValue};
use gcp::bigtable::read::assembler::{RawCell, RawRow, RowAssembler, ScanEvent};
use gcp::bigtable::read::encode::{
    encode_flattened, encode_nested, into_flattened_rows, into_nested_row,
};
use gcp::bigtable::read::restriction::choose_split;
use gcp::bigtable::read::{
    KeyRangeRestriction, KeyRangeTracker, SampleCoalescer, SplitTarget, TabletBoundary,
};
use gcp::bigtable::{flattened_row_schema, nested_row_schema};

#[test]
fn row_assembler_handles_chunked_cells_reset_row_and_scanned_heartbeats() {
    let mut asm = RowAssembler::new();

    // First response starts a row, then resets it, then starts a 2-chunk cell and commits.
    let resp1 = ReadRowsResponse {
        chunks: vec![
            CellChunk {
                row_key: Bytes::from_static(b"discarded"),
                family_name: Some(StringValue {
                    value: "cf".to_string(),
                }),
                qualifier: Some(BytesValue {
                    value: Bytes::from_static(b"q"),
                }),
                timestamp_micros: 1,
                labels: Vec::new(),
                value: Bytes::from_static(b"bad"),
                value_size: 0,
                row_status: None,
            },
            CellChunk {
                row_key: Bytes::new(),
                family_name: None,
                qualifier: None,
                timestamp_micros: 0,
                labels: Vec::new(),
                value: Bytes::new(),
                value_size: 0,
                row_status: Some(RowStatus::ResetRow(true)),
            },
            CellChunk {
                row_key: Bytes::from_static(b"row-1"),
                family_name: Some(StringValue {
                    value: "cf1".to_string(),
                }),
                qualifier: Some(BytesValue {
                    value: Bytes::from_static(b"col-a"),
                }),
                timestamp_micros: 100,
                labels: Vec::new(),
                value: Bytes::from_static(b"hello "),
                value_size: 11,
                row_status: None,
            },
            CellChunk {
                row_key: Bytes::new(),
                family_name: None,
                qualifier: None,
                timestamp_micros: 100,
                labels: Vec::new(),
                value: Bytes::from_static(b"world"),
                value_size: 0,
                row_status: Some(RowStatus::CommitRow(true)),
            },
        ],
        last_scanned_row_key: Bytes::from_static(b"row-999"),
        ..Default::default()
    };

    let events = asm.feed(resp1).unwrap();
    asm.finish_stream().unwrap();
    assert_eq!(
        events,
        vec![
            ScanEvent::Row(RawRow {
                key: Bytes::from_static(b"row-1"),
                cells: vec![RawCell {
                    family: Arc::from("cf1"),
                    qualifier: Bytes::from_static(b"col-a"),
                    timestamp_micros: 100,
                    labels: Vec::new(),
                    value: Bytes::from_static(b"hello world"),
                }],
            }),
            ScanEvent::Scanned(Bytes::from_static(b"row-999")),
        ]
    );
}

#[test]
fn row_assembler_rejects_uncommitted_stream_end() {
    let mut asm = RowAssembler::new();
    asm.feed(ReadRowsResponse {
        chunks: vec![CellChunk {
            row_key: Bytes::from_static(b"row-uncommitted"),
            family_name: Some(StringValue {
                value: "cf".to_string(),
            }),
            qualifier: Some(BytesValue {
                value: Bytes::from_static(b"q"),
            }),
            timestamp_micros: 10,
            labels: Vec::new(),
            value: Bytes::from_static(b"val"),
            value_size: 0,
            row_status: None,
        }],
        last_scanned_row_key: Bytes::new(),
        ..Default::default()
    })
    .unwrap();
    assert!(asm.finish_stream().is_err());
}

#[test]
fn direct_flattened_and_nested_row_encoders_match_portable_row_coder() {
    let sorted = RawRow {
        key: Bytes::from_static(b"user#42"),
        cells: vec![
            RawCell {
                family: Arc::from("cf_meta"),
                qualifier: Bytes::from_static(b"name"),
                timestamp_micros: 2000,
                labels: Vec::new(),
                value: Bytes::from_static(b"Alice"),
            },
            RawCell {
                family: Arc::from("cf_meta"),
                qualifier: Bytes::from_static(b"name"),
                timestamp_micros: 1000,
                labels: Vec::new(),
                value: Bytes::from_static(b"Alicia"),
            },
            RawCell {
                family: Arc::from("cf_metrics"),
                qualifier: Bytes::from_static(b"clicks"),
                timestamp_micros: 3000,
                labels: Vec::new(),
                value: Bytes::from_static(b"42"),
            },
        ],
    };
    let unsorted = RawRow {
        key: sorted.key.clone(),
        cells: sorted.cells.iter().rev().cloned().collect(),
    };

    for raw in [sorted, unsorted] {
        let flattened_bytes = encode_flattened(&raw).unwrap();
        let flattened_rows = into_flattened_rows(raw.clone()).unwrap();
        assert_eq!(flattened_bytes.len(), 2);
        assert_eq!(flattened_rows.len(), 2);
        for (bytes, expected_row) in flattened_bytes.iter().zip(&flattened_rows) {
            assert_eq!(bytes, &expected_row.to_row_bytes().unwrap());
            let decoded = Row::from_row_bytes(&flattened_row_schema(), bytes).unwrap();
            assert_eq!(&decoded, expected_row);
        }

        let nested_bytes = encode_nested(&raw).unwrap();
        let nested_row = into_nested_row(raw).unwrap();
        assert_eq!(nested_bytes, nested_row.to_row_bytes().unwrap());
        let decoded_nested = Row::from_row_bytes(&nested_row_schema(), &nested_bytes).unwrap();
        assert_eq!(decoded_nested, nested_row);
    }
}

#[test]
fn sample_coalescer_compacts_thousands_of_tablets_into_super_ranges_with_interior_boundaries() {
    let mut coalescer = SampleCoalescer::new(Vec::new(), Vec::new(), 16, true);
    for idx in 1..=2000u32 {
        let key = if idx == 2000 {
            Bytes::new()
        } else {
            Bytes::from(format!("tablet-{idx:06}"))
        };
        coalescer.feed(SampleRowKeysResponse {
            row_key: key,
            offset_bytes: (idx as i64) * 1024 * 1024,
        });
    }
    let super_ranges = coalescer.finish();
    assert!(
        super_ranges.len() <= 16,
        "expected <= 16 super-ranges, got {}",
        super_ranges.len()
    );

    // Every super-range retains interior tablet boundaries and expands into individual tablets.
    let total_expanded: usize = super_ranges.iter().map(|r| r.expand_tablets().len()).sum();
    assert!(
        total_expanded > super_ranges.len(),
        "expected interior tablets to expand, got {total_expanded}"
    );

    // Round-trip `KeyRangeRestriction` through `DefaultCoder`.
    let first = &super_ranges[0];
    let encoded = first.encode().unwrap();
    assert_eq!(&KeyRangeRestriction::decode(&encoded).unwrap(), first);
}

#[test]
fn key_range_tracker_advances_on_scanned_keys_and_splits_at_tablet_boundaries_first() {
    let restriction =
        KeyRangeRestriction::new(b"a".to_vec(), b"z".to_vec(), 300).with_tablets(vec![
            TabletBoundary {
                end_key: b"h".to_vec(),
                offset_bytes: 100,
            },
            TabletBoundary {
                end_key: b"p".to_vec(),
                offset_bytes: 200,
            },
        ]);

    // `choose_split` snaps to tablet boundary `b"h"` or `b"p"` before interpolating.
    let target = choose_split(b"a", b"z", 300, &restriction.tablets, 0.35).unwrap();
    assert_eq!(
        target,
        SplitTarget::Tablet {
            index: 0,
            key: b"h".to_vec(),
            primary_bytes: 100,
        }
    );

    let tracker = KeyRangeTracker::new(restriction);
    assert_eq!(tracker.current_progress().work_completed, 0.0);

    // Claiming a scanned filter heartbeat advances progress.
    assert!(tracker.try_claim(&b"c".to_vec()));
    assert!(tracker.current_progress().work_completed > 0.0);

    // Splitting at 0.6 snaps to the second interior tablet boundary (`b"p"`, 200/300).
    let (primary, residual) = tracker.try_split(0.6).unwrap();
    assert_eq!(primary.end_key, b"p");
    assert_eq!(residual.start_key, b"p");
    assert_eq!(residual.end_key, b"z");

    // Once no interior tablets remain in `(c\0, p)` after claiming `h`, splitting falls back to
    // key-space interpolation.
    assert!(tracker.try_claim(&b"h".to_vec()));
    let (primary2, residual2) = tracker.try_split(0.5).unwrap();
    assert!(primary2.end_key.as_slice() > b"h\0".as_slice());
    assert!(primary2.end_key.as_slice() < b"p".as_slice());
    assert_eq!(primary2.end_key, residual2.start_key);

    // Checkpointing at 0.0 stops primary immediately at `next_key("h")` = `"h\0"`.
    let (cp_primary, cp_residual) = tracker.try_split(0.0).unwrap();
    assert_eq!(cp_primary.end_key, b"h\0");
    assert_eq!(cp_residual.start_key, b"h\0");
    assert!(!tracker.try_claim(&b"i".to_vec()));
    tracker.check_done().unwrap();
}
