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

//! Arrow [`RecordBatch`] elements with an Arrow IPC stream coder, and converters that batch
//! Beam rows into them.

use std::fmt;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, VarIntCoder};
use beam::schema::{BeamRow, Row, Schema};
use beam::transforms::BatchConverter;

use crate::convert::{
    beam_rows_to_record_batch, record_batch_to_beam_rows, record_batch_to_rows,
    rows_to_record_batch,
};

/// A [`RecordBatch`] whose [`DefaultCoder`] uses the Arrow IPC streaming format.
#[derive(Clone, Debug, PartialEq)]
pub struct ArrowRecordBatch(pub RecordBatch);

impl ArrowRecordBatch {
    pub fn new(batch: RecordBatch) -> Self {
        Self(batch)
    }

    /// Extracts the inner `RecordBatch`.
    pub fn into_inner(self) -> RecordBatch {
        self.0
    }
}

impl Deref for ArrowRecordBatch {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ArrowRecordBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<RecordBatch> for ArrowRecordBatch {
    fn from(batch: RecordBatch) -> Self {
        Self(batch)
    }
}

impl From<ArrowRecordBatch> for RecordBatch {
    fn from(wrapper: ArrowRecordBatch) -> Self {
        wrapper.0
    }
}

/// Serializes and deserializes [`ArrowRecordBatch`]es using the Arrow IPC streaming format.
#[derive(Clone, Debug, Default)]
pub struct ArrowRecordBatchCoder;

impl Coder<ArrowRecordBatch> for ArrowRecordBatchCoder {
    fn urn(&self) -> &'static str {
        "beam:coder:arrow_record_batch:v1"
    }

    fn encode(
        &self,
        value: &ArrowRecordBatch,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        let mut buffer = Vec::new();
        {
            let mut stream_writer =
                StreamWriter::try_new(&mut buffer, &value.schema()).map_err(|e| {
                    CoderError::Format(format!("Failed to create Arrow IPC writer: {e}"))
                })?;
            stream_writer.write(&value.0).map_err(|e| {
                CoderError::Format(format!("Failed to write RecordBatch to Arrow IPC: {e}"))
            })?;
            stream_writer.finish().map_err(|e| {
                CoderError::Format(format!("Failed to finish Arrow IPC stream: {e}"))
            })?;
        }
        VarIntCoder::encode_varint(buffer.len() as i64, writer)?;
        writer.write_all(&buffer)?;
        Ok(())
    }

    fn decode(
        &self,
        reader: &mut dyn Read,
        _context: Context,
    ) -> Result<ArrowRecordBatch, CoderError> {
        let len = VarIntCoder::decode_varint(reader)? as usize;
        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer)?;
        let mut stream_reader = StreamReader::try_new(std::io::Cursor::new(buffer), None)
            .map_err(|e| CoderError::Format(format!("Failed to create Arrow IPC reader: {e}")))?;
        let batch = stream_reader
            .next()
            .ok_or_else(|| {
                CoderError::Format("Arrow IPC stream contained no RecordBatch".to_string())
            })?
            .map_err(|e| {
                CoderError::Format(format!("Failed to read RecordBatch from Arrow IPC: {e}"))
            })?;
        Ok(ArrowRecordBatch(batch))
    }
}

impl DefaultCoder for ArrowRecordBatch {
    type Coder = ArrowRecordBatchCoder;

    fn coder() -> Self::Coder {
        ArrowRecordBatchCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        Self::coder().encode(self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        Self::coder().decode(reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder("beam:coder:arrow_record_batch:v1", vec![])
    }
}

/// Batch converter accumulating dynamically typed Beam [`Row`]s into [`ArrowRecordBatch`]es.
#[derive(Clone, Debug)]
pub struct ArrowRowBatchConverter {
    schema: Arc<Schema>,
}

impl ArrowRowBatchConverter {
    /// Creates a converter targeting the given Beam schema.
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

impl BatchConverter<Row, ArrowRecordBatch> for ArrowRowBatchConverter {
    type Buffer = Vec<Row>;

    fn create_buffer(&self) -> Self::Buffer {
        Vec::new()
    }

    fn push(&self, buffer: &mut Self::Buffer, element: Row) -> beam::Result {
        buffer.push(element);
        Ok(())
    }

    fn buffer_len(&self, buffer: &Self::Buffer) -> usize {
        buffer.len()
    }

    fn finish_batch(&self, buffer: Self::Buffer) -> beam::Result<ArrowRecordBatch> {
        let batch = rows_to_record_batch(&self.schema, &buffer)?;
        Ok(ArrowRecordBatch(batch))
    }

    fn explode(&self, batch: ArrowRecordBatch) -> beam::Result<Vec<Row>> {
        Ok(record_batch_to_rows(&batch.0, &self.schema)?)
    }
}

/// Batch converter accumulating strongly-typed `#[derive(BeamRow)]` structures into [`ArrowRecordBatch`]es.
pub struct ArrowBeamRowBatchConverter<T> {
    _marker: PhantomData<T>,
}

impl<T> ArrowBeamRowBatchConverter<T> {
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Clone for ArrowBeamRowBatchConverter<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ArrowBeamRowBatchConverter<T> {}

impl<T> Default for ArrowBeamRowBatchConverter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for ArrowBeamRowBatchConverter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrowBeamRowBatchConverter").finish()
    }
}

impl<T: BeamRow + Send + Sync + 'static> BatchConverter<T, ArrowRecordBatch>
    for ArrowBeamRowBatchConverter<T>
{
    type Buffer = Vec<T>;

    fn create_buffer(&self) -> Self::Buffer {
        Vec::new()
    }

    fn push(&self, buffer: &mut Self::Buffer, element: T) -> beam::Result {
        buffer.push(element);
        Ok(())
    }

    fn buffer_len(&self, buffer: &Self::Buffer) -> usize {
        buffer.len()
    }

    fn finish_batch(&self, buffer: Self::Buffer) -> beam::Result<ArrowRecordBatch> {
        let batch = beam_rows_to_record_batch(&buffer)?;
        Ok(ArrowRecordBatch(batch))
    }

    fn explode(&self, batch: ArrowRecordBatch) -> beam::Result<Vec<T>> {
        Ok(record_batch_to_beam_rows(&batch.0)?)
    }
}
