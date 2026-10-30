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

//! Rows in Arrow IPC format, and the append requests that hold them.

use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV};
use bytes::Bytes;
use google_cloud_bigquery::model::ArrowRecordBatch;

/// Rows in Arrow IPC format, for one append request. The shuffle of an exactly-once write
/// moves these, so [`EncodedBatchCoder`] writes the bytes with no copy into a row.
#[derive(Clone, Debug, PartialEq)]
#[doc(hidden)]
pub struct EncodedBatch {
    pub bytes: Vec<u8>,
    pub rows: i64,
}

/// Encodes an [`EncodedBatch`] as `KV<varint, bytes>`. Both parts take the nested context,
/// as [`DefaultCoder::encode_element`] writes them.
#[derive(Clone, Debug, Default)]
#[doc(hidden)]
pub struct EncodedBatchCoder;

impl Coder<EncodedBatch> for EncodedBatchCoder {
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        value: &EncodedBatch,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<EncodedBatch, CoderError> {
        EncodedBatch::decode_element(reader)
    }
}

impl DefaultCoder for EncodedBatch {
    type Coder = EncodedBatchCoder;

    fn coder() -> Self::Coder {
        EncodedBatchCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        self.rows.encode_element(writer)?;
        self.bytes.encode_element(writer)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let rows = i64::decode_element(reader)?;
        let bytes = Vec::<u8>::decode_element(reader)?;
        Ok(Self { bytes, rows })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let rows = i64::register_coder(registry);
        let bytes = Vec::<u8>::register_coder(registry);
        registry.register_coder(URN_KV, vec![rows, bytes])
    }
}

/// The rows of one append request. A clone shares the bytes, so a writer keeps the rows of a
/// request that it sent with no copy.
#[derive(Clone, Debug)]
pub(super) struct Request {
    pub(super) rows: ArrowRecordBatch,
    pub(super) count: i64,
}

impl From<EncodedBatch> for Request {
    /// Moves the bytes into the request, with no copy.
    fn from(batch: EncodedBatch) -> Self {
        Self {
            rows: ArrowRecordBatch::new().set_serialized_record_batch(Bytes::from(batch.bytes)),
            count: batch.rows,
        }
    }
}
