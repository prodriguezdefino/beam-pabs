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

//! Elements that a writer buffers for one append, and the append in-flight limit.

use beam::schema::{FieldValue, Row, RowElement};
use beam_io_arrow::EncodedBatch;

/// Target bytes per append.
pub const APPEND_BYTES: usize = 2 * 1024 * 1024;

/// Maximum rows per append.
const APPEND_ROWS: usize = 150_000;

/// Rows in the first batch of a typed writer copy, before it measures bytes per row.
const FIRST_BATCH_ROWS: usize = 1_024;

/// Maximum append request bytes. A single row larger than a request, with its writer schema,
/// becomes an error row.
pub const MAX_REQUEST_BYTES: usize = 10_000_000;

/// Elements that a writer converts together, up to [`APPEND_ROWS`] and about [`APPEND_BYTES`].
#[derive(Debug)]
pub(super) struct RowBuffer<T> {
    rows: Vec<T>,
    bytes: usize,
    max_rows: usize,
}

impl<T> Default for RowBuffer<T> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            bytes: 0,
            max_rows: FIRST_BATCH_ROWS,
        }
    }
}

impl<T> RowBuffer<T> {
    /// Adds `row` with `estimated_bytes` (`None` when the codec learns row size from batches),
    /// and returns whether the buffer is full.
    pub(super) fn push(&mut self, row: T, estimated_bytes: Option<usize>) -> bool {
        self.rows.push(row);
        if let Some(bytes) = estimated_bytes {
            self.bytes += bytes;
            self.rows.len() >= APPEND_ROWS || self.bytes >= APPEND_BYTES
        } else {
            self.rows.len() >= self.max_rows
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(super) fn rows(&self) -> &[T] {
        &self.rows
    }

    pub(super) fn clear(&mut self) {
        self.rows.clear();
        self.bytes = 0;
    }

    /// Empties the buffer for the next append and updates the row limit from `batches`.
    pub(super) fn finish_batch(&mut self, batches: &[EncodedBatch]) {
        self.clear();
        let bytes: usize = batches.iter().map(|b| b.bytes.len()).sum();
        let count: usize = batches
            .iter()
            .filter_map(|b| usize::try_from(b.rows).ok())
            .sum();
        if let Some(per_row) = bytes.checked_div(count) {
            self.max_rows = (APPEND_BYTES / per_row.max(1)).clamp(1, APPEND_ROWS);
        }
    }
}

/// Estimated Arrow batch bytes of `element`.
pub(super) fn row_element_bytes(element: &RowElement) -> usize {
    match element {
        RowElement::Encoded(bytes) => bytes.len(),
        RowElement::Row(row) => row_bytes(row),
    }
}

fn row_bytes(row: &Row) -> usize {
    row.values().iter().map(opt_value_bytes).sum()
}

fn opt_value_bytes(value: &Option<FieldValue>) -> usize {
    match value {
        None => 1,
        Some(value) => field_value_bytes(value),
    }
}

fn field_value_bytes(value: &FieldValue) -> usize {
    match value {
        FieldValue::Byte(_) | FieldValue::Boolean(_) => 1,
        FieldValue::Int16(_) => 2,
        FieldValue::Int32(_) | FieldValue::Float(_) => 4,
        FieldValue::Int64(_) | FieldValue::Double(_) => 8,
        FieldValue::String(text) => text.len() + 4,
        FieldValue::Bytes(bytes) => bytes.len() + 4,
        FieldValue::Array(items) => items.iter().map(opt_value_bytes).sum::<usize>() + 4,
        FieldValue::Map(entries) => entries
            .iter()
            .map(|(key, value)| field_value_bytes(key) + opt_value_bytes(value))
            .sum(),
        FieldValue::Row(row) => row_bytes(row),
    }
}

/// Appends that a writer sends before it waits for the oldest one: at most 1,000 requests and
/// 20 MiB. A writer keeps the rows of each append until BigQuery accepts them, so the byte limit
/// bounds the memory of each bundle.
pub(super) const fn in_flight_limit() -> beam::internals::InFlightLimit {
    beam::internals::InFlightLimit::new(1_000, 20 * 1024 * 1024)
}
