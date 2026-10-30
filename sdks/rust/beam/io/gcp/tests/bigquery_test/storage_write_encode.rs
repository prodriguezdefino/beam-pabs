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

//! Splitting encoded row batches into appends that fit a request, and the rows of a request
//! that BigQuery accepted.

use std::sync::Arc;

use arrow_io::arrow_array::{RecordBatch, StringArray};
use arrow_io::arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use gcp::bigquery::storage::write::encode::{
    APPEND_BYTES, EncodedBatch, MAX_REQUEST_BYTES, accepted, split,
};

/// A batch of `rows` strings of `bytes` bytes each.
fn strings(rows: usize, bytes: usize) -> RecordBatch {
    let schema = ArrowSchema::new(vec![ArrowField::new("text", DataType::Utf8, false)]);
    let column = StringArray::from_iter_values(std::iter::repeat_n("x".repeat(bytes), rows));
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(column)]).expect("a valid string batch")
}

fn split_all(batch: RecordBatch, max_bytes: usize) -> (Vec<EncodedBatch>, Vec<RecordBatch>) {
    let (mut batches, mut too_large) = (Vec::new(), Vec::new());
    split(batch, max_bytes, &mut batches, &mut too_large).expect("split encodes the batch");
    (batches, too_large)
}

#[test]
fn a_batch_larger_than_an_append_but_smaller_than_a_request_is_one_batch() {
    let (batches, too_large) = split_all(strings(3_000, 1_000), MAX_REQUEST_BYTES);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].rows, 3_000);
    assert!(batches[0].bytes.len() > APPEND_BYTES);
    assert!(too_large.is_empty());
}

#[test]
fn a_batch_larger_than_a_request_splits_into_batches_that_fit() {
    let (batches, too_large) = split_all(strings(4_000, 1_000), 1_000_000);
    assert!(batches.len() > 1);
    assert!(batches.iter().all(|batch| batch.bytes.len() <= 1_000_000));
    assert_eq!(batches.iter().map(|batch| batch.rows).sum::<i64>(), 4_000);
    assert!(too_large.is_empty());
}

#[test]
fn a_row_larger_than_a_request_is_too_large() {
    let (batches, too_large) = split_all(strings(3, 1_000), 500);
    assert!(batches.is_empty());
    assert_eq!(too_large.len(), 3);
    assert!(too_large.iter().all(|batch| batch.num_rows() == 1));
}

/// The ranges that `accepted` returns, as `(start, end)` pairs.
fn accepted_pairs(rejected: &[usize], rows: usize) -> Vec<(usize, usize)> {
    accepted(rejected, rows)
        .map(|range| (range.start, range.end))
        .collect()
}

/// The rejected rows of a batch of 5 rows, and the accepted ranges.
type AcceptedCase = (&'static [usize], &'static [(usize, usize)]);

#[test]
fn accepted_gives_the_ranges_between_the_rejected_rows() {
    let cases: [AcceptedCase; 7] = [
        (&[], &[(0, 5)]),
        (&[0], &[(1, 5)]),
        (&[4], &[(0, 4)]),
        (&[1, 3], &[(0, 1), (2, 3), (4, 5)]),
        (&[1, 2, 3], &[(0, 1), (4, 5)]),
        (&[0, 1, 2, 3, 4], &[]),
        // BigQuery can name an index after the last row.
        (&[2, 9], &[(0, 2), (3, 5)]),
    ];
    for (rejected, expected) in cases {
        assert_eq!(
            accepted_pairs(rejected, 5),
            expected,
            "rejected {rejected:?}"
        );
    }
}

#[test]
fn accepted_and_rejected_rows_cover_every_row_once() {
    // Every set of rejected rows of a batch of 8 rows.
    let rows = 8;
    for mask in 0u32..1 << rows {
        let rejected: Vec<usize> = (0..rows).filter(|i| mask & (1 << i) != 0).collect();
        let mut seen: Vec<usize> = accepted(&rejected, rows).flatten().collect();
        assert!(
            seen.iter().all(|row| !rejected.contains(row)),
            "rejected {rejected:?}: {seen:?}"
        );
        seen.extend(&rejected);
        seen.sort_unstable();
        assert_eq!(seen, (0..rows).collect::<Vec<_>>(), "rejected {rejected:?}");
    }
}
