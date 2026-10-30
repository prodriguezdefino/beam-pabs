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

//! Pure state machine that assembles `ReadRowsResponse` chunks into committed rows and
//! server-side filter scan heartbeats.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::bigtable::proto::ReadRowsResponse;
use crate::bigtable::proto::read_rows_response::CellChunk;
use crate::bigtable::proto::read_rows_response::cell_chunk::RowStatus;

/// One timestamped cell in a committed [`RawRow`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawCell {
    /// Shared by the cells of one family run, so a cell does not copy the name.
    pub family: Arc<str>,
    pub qualifier: Bytes,
    pub timestamp_micros: i64,
    pub labels: Vec<String>,
    pub value: Bytes,
}

/// A committed Bigtable row assembled from one or more [`CellChunk`] messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawRow {
    pub key: Bytes,
    pub cells: Vec<RawCell>,
}

/// An event produced by [`RowAssembler::feed`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScanEvent {
    /// A complete row committed by the server.
    Row(RawRow),
    /// A server scan heartbeat (`last_scanned_row_key`) indicating that all keys up to and
    /// including this key have been filtered out.
    Scanned(Bytes),
}

/// State of an in-progress multi-chunk cell value.
#[derive(Debug)]
struct InProgressCell {
    family: Arc<str>,
    qualifier: Bytes,
    timestamp_micros: i64,
    labels: Vec<String>,
    buffer: BytesMut,
}

/// Assembles streaming [`ReadRowsResponse`] messages into [`ScanEvent`] values.
#[derive(Debug, Default)]
pub struct RowAssembler {
    row_key: Bytes,
    family: Option<Arc<str>>,
    qualifier: Option<Bytes>,
    cells: Vec<RawCell>,
    partial_cell: Option<InProgressCell>,
}

impl RowAssembler {
    /// Creates an empty assembler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns whether the assembler holds uncommitted chunks of a row.
    pub fn has_in_progress_row(&self) -> bool {
        !self.row_key.is_empty() || !self.cells.is_empty() || self.partial_cell.is_some()
    }

    /// Discards any uncommitted row state, for example before reconnecting a broken stream.
    pub fn reset(&mut self) {
        self.row_key = Bytes::new();
        self.family = None;
        self.qualifier = None;
        self.cells.clear();
        self.partial_cell = None;
    }

    /// Feeds one [`ReadRowsResponse`] into the state machine and returns all resulting events.
    pub fn feed(&mut self, response: ReadRowsResponse) -> Result<Vec<ScanEvent>, String> {
        let ReadRowsResponse {
            chunks,
            last_scanned_row_key,
            ..
        } = response;
        let mut events = Vec::with_capacity(chunks.len() / 4 + 1);
        for chunk in chunks {
            if let Some(row) = self.feed_chunk(chunk)? {
                events.push(ScanEvent::Row(row));
            }
        }
        if !last_scanned_row_key.is_empty() {
            events.push(ScanEvent::Scanned(last_scanned_row_key));
        }
        Ok(events)
    }

    /// Checks that no partial row remains uncommitted when the `ReadRows` stream ends.
    pub fn finish_stream(&self) -> Result<(), String> {
        if self.has_in_progress_row() {
            Err("ReadRows stream ended with an uncommitted row".into())
        } else {
            Ok(())
        }
    }

    fn feed_chunk(&mut self, chunk: CellChunk) -> Result<Option<RawRow>, String> {
        let CellChunk {
            row_key,
            family_name,
            qualifier,
            timestamp_micros,
            labels,
            value,
            value_size,
            row_status,
        } = chunk;

        if matches!(row_status, Some(RowStatus::ResetRow(true))) {
            self.reset();
            return Ok(None);
        }

        if !row_key.is_empty() {
            if !self.row_key.is_empty() && self.row_key != row_key {
                return Err("ReadRows sent a new row key before committing the active row".into());
            }
            self.row_key = row_key;
        }
        if self.row_key.is_empty() {
            return Err("ReadRows sent a chunk without a row key".into());
        }

        if let Some(fam) = family_name {
            self.family = Some(Arc::from(fam.value));
            self.qualifier = None;
        }
        if let Some(qual) = qualifier {
            self.qualifier = Some(qual.value);
        }

        match (&mut self.partial_cell, value_size > 0) {
            (Some(partial), true) => {
                partial.buffer.extend_from_slice(&value);
            }
            (Some(_), false) => {
                let Some(mut partial) = self.partial_cell.take() else {
                    unreachable!("checked Some above")
                };
                partial.buffer.extend_from_slice(&value);
                self.cells.push(RawCell {
                    family: partial.family,
                    qualifier: partial.qualifier,
                    timestamp_micros: partial.timestamp_micros,
                    labels: partial.labels,
                    value: partial.buffer.freeze(),
                });
            }
            (None, true) => {
                let (family, qualifier) = self.current_column()?;
                let capacity = usize::try_from(value_size).unwrap_or(value.len());
                let mut buffer = BytesMut::with_capacity(capacity);
                buffer.extend_from_slice(&value);
                self.partial_cell = Some(InProgressCell {
                    family,
                    qualifier,
                    timestamp_micros,
                    labels,
                    buffer,
                });
            }
            (None, false) => {
                let (family, qualifier) = self.current_column()?;
                self.cells.push(RawCell {
                    family,
                    qualifier,
                    timestamp_micros,
                    labels,
                    value,
                });
            }
        }

        match row_status {
            Some(RowStatus::CommitRow(true)) => {
                if self.partial_cell.is_some() {
                    return Err(
                        "ReadRows committed a row while a chunked cell was incomplete".into(),
                    );
                }
                let next_cap = self.cells.len();
                let key = std::mem::take(&mut self.row_key);
                let cells = std::mem::replace(&mut self.cells, Vec::with_capacity(next_cap));
                self.family = None;
                self.qualifier = None;
                Ok(Some(RawRow { key, cells }))
            }
            Some(RowStatus::CommitRow(false) | RowStatus::ResetRow(_)) | None => Ok(None),
        }
    }

    fn current_column(&self) -> Result<(Arc<str>, Bytes), String> {
        let family = self
            .family
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| "ReadRows chunk is missing a column family".to_string())?;
        let qualifier = self
            .qualifier
            .clone()
            .ok_or_else(|| "ReadRows chunk is missing a column qualifier".to_string())?;
        Ok((family, qualifier))
    }
}
