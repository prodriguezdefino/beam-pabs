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

//! The rows that a writer converts together, and the append requests that it waits for.

use beam::schema::{FieldValue, Row, RowElement};

use super::encoded_batch::Request;

/// The bytes of rows for one append. A writer converts its buffer when the rows reach about
/// this size.
#[doc(hidden)]
pub const APPEND_BYTES: usize = 2 * 1024 * 1024;

/// The most rows for one append.
const APPEND_ROWS: usize = 150_000;

/// The largest append request, as the Java SDK default `storageWriteApiMaxRequestSize`. A row
/// larger than a request, with its writer schema, is an error row.
#[doc(hidden)]
pub const MAX_REQUEST_BYTES: usize = 10_000_000;

/// The rows that a writer converts together, until they reach [`APPEND_ROWS`] or about
/// [`APPEND_BYTES`]. The size of encoded rows is the size of their bytes. The size of a row
/// is an estimate from its values.
#[derive(Debug, Default)]
pub(super) struct RowBuffer {
    rows: Buffered,
    bytes: usize,
}

/// The rows of a buffer: the row coder bytes of the rows that arrived encoded, and the rows
/// that arrived as rows.
#[derive(Debug, Default)]
pub(super) struct Buffered {
    pub(super) encoded: Vec<Vec<u8>>,
    pub(super) rows: Vec<Row>,
}

impl Buffered {
    pub(super) fn is_empty(&self) -> bool {
        self.encoded.is_empty() && self.rows.is_empty()
    }
}

impl RowBuffer {
    /// Adds `row`, and returns whether the buffer is full.
    pub(super) fn push(&mut self, row: RowElement) -> bool {
        match row {
            RowElement::Encoded(bytes) => {
                self.bytes += bytes.len();
                self.rows.encoded.push(bytes);
            }
            RowElement::Row(row) => {
                self.bytes += row.values().iter().map(value_bytes).sum::<usize>();
                self.rows.rows.push(row);
            }
        }
        self.rows.encoded.len() + self.rows.rows.len() >= APPEND_ROWS || self.bytes >= APPEND_BYTES
    }

    /// Returns the rows and empties the buffer.
    pub(super) fn take(&mut self) -> Buffered {
        self.bytes = 0;
        std::mem::take(&mut self.rows)
    }
}

/// About the bytes of a value in an Arrow batch.
fn value_bytes(value: &Option<FieldValue>) -> usize {
    match value {
        None | Some(FieldValue::Byte(_) | FieldValue::Boolean(_)) => 1,
        Some(FieldValue::Int16(_)) => 2,
        Some(FieldValue::Int32(_) | FieldValue::Float(_)) => 4,
        Some(FieldValue::Int64(_) | FieldValue::Double(_)) => 8,
        Some(FieldValue::String(text)) => text.len() + 4,
        Some(FieldValue::Bytes(bytes)) => bytes.len() + 4,
        Some(FieldValue::Array(items)) => items.iter().map(value_bytes).sum::<usize>() + 4,
        Some(FieldValue::Map(entries)) => entries
            .iter()
            .map(|(key, value)| value_bytes(&Some(key.clone())) + value_bytes(value))
            .sum(),
        Some(FieldValue::Row(row)) => row.values().iter().map(value_bytes).sum(),
    }
}

/// Appends that a writer sends before it waits for the oldest one: at most 1,000 requests and
/// 20 MiB. A writer keeps the rows of each append until BigQuery accepts them, so the byte limit
/// bounds the memory of each bundle.
#[derive(Debug, Default)]
pub(super) struct InFlightLimit {
    requests: usize,
    bytes: usize,
}

impl InFlightLimit {
    const MAX_REQUESTS: usize = 1_000;
    const MAX_BYTES: usize = 20 * 1024 * 1024;

    pub(super) fn add(&mut self, request: &Request) {
        self.requests += 1;
        self.bytes += request.rows.serialized_record_batch.len();
    }

    pub(super) fn remove(&mut self, request: &Request) {
        self.requests = self.requests.saturating_sub(1);
        self.bytes = self
            .bytes
            .saturating_sub(request.rows.serialized_record_batch.len());
    }

    pub(super) fn is_full(&self) -> bool {
        self.requests > Self::MAX_REQUESTS || self.bytes > Self::MAX_BYTES
    }
}
