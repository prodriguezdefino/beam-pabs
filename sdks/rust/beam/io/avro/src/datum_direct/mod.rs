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

//! Direct encoding of Beam rows and row coder bytes into Avro binary datums.
//!
//! Compiles an Avro record schema and a Beam [`Schema`] into a plan that writes straight to
//! the caller buffer with no Arrow batch between them. Schemas outside the supported subset
//! return `None` so [`DatumEncoder`](crate::datum::DatumEncoder) falls back to `arrow-avro`.

mod encoder;
mod row_stream;
mod schema;

use beam::internals::RowWalk;
use beam::schema::{BeamRow, Row, RowElement, Schema};

use crate::datum::{CONFLUENT_MAGIC, WireFormat};
use schema::{ValuePlan, WriteOp};

/// Direct single-record Avro datum writer for one `(avro_schema, format, row_schema)` triple.
pub(crate) struct DirectWriter {
    header: WireHeader,
    root: ValuePlan,
    /// The row schema and `root`, compiled for row coder bytes.
    walk: RowWalk<WriteOp>,
    /// Reused row-bytes buffer so `encode_beam_row` allocates nothing per element.
    scratch: Vec<u8>,
}

#[derive(Clone, Copy)]
enum WireHeader {
    Plain,
    Confluent([u8; 5]),
}

impl DirectWriter {
    /// Compiles a direct writer, or returns `None` when the schema needs the `arrow-avro` path.
    pub(crate) fn try_new(
        avro_schema: &str,
        format: WireFormat,
        row_schema: &Schema,
    ) -> Option<Self> {
        let root = schema::compile(avro_schema, row_schema)?;
        let walk = RowWalk::compile(row_schema, &root).ok()?;
        let header = match format {
            WireFormat::Plain => WireHeader::Plain,
            WireFormat::Confluent(id) => {
                let [a, b, c, d] = id.to_be_bytes();
                WireHeader::Confluent([CONFLUENT_MAGIC, a, b, c, d])
            }
        };
        Some(Self {
            header,
            root,
            walk,
            scratch: Vec::new(),
        })
    }

    pub(crate) fn encode_row_element(
        &self,
        element: &RowElement,
        buffer: &mut Vec<u8>,
    ) -> Result<(), String> {
        match element {
            RowElement::Row(row) => self.encode_row(row, buffer),
            RowElement::Encoded(bytes) => self.encode_encoded(bytes, buffer),
        }
    }

    pub(crate) fn encode_row(&self, row: &Row, buffer: &mut Vec<u8>) -> Result<(), String> {
        encode_into(self.header, buffer, |out| {
            encoder::write_row(&self.root, row, out)
        })
    }

    pub(crate) fn encode_beam_row<T: BeamRow>(
        &mut self,
        value: &T,
        buffer: &mut Vec<u8>,
    ) -> Result<(), String> {
        self.scratch.clear();
        if let Err(err) = value.encode_row_bytes(&mut self.scratch) {
            buffer.clear();
            return Err(err.to_string());
        }
        self.encode_encoded(&self.scratch, buffer)
    }

    fn encode_encoded(&self, bytes: &[u8], buffer: &mut Vec<u8>) -> Result<(), String> {
        encode_into(self.header, buffer, |out| {
            row_stream::stream_row(&self.walk, bytes, out)
        })
    }
}

fn encode_into(
    header: WireHeader,
    buffer: &mut Vec<u8>,
    write: impl FnOnce(&mut Vec<u8>) -> Result<(), String>,
) -> Result<(), String> {
    buffer.clear();
    if let WireHeader::Confluent(prefix) = header {
        buffer.extend_from_slice(&prefix);
    }
    let result = write(buffer);
    if result.is_err() {
        buffer.clear();
    }
    result
}
