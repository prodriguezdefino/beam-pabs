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

//! Splitting encoded row batches into appends that fit a request.

use std::sync::Arc;

use arrow_io::arrow_array::{RecordBatch, StringArray};
use arrow_io::arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use gcp::bigquery::storage::write::encode::{APPEND_BYTES, EncodedBatch, MAX_REQUEST_BYTES, split};

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
