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

//! [`FileSink`] writing Avro object container files with arrow-avro.

use std::io::Write;
use std::sync::Arc;

use arrow_avro::compression::CompressionCodec;
use arrow_avro::writer::format::AvroOcfFormat;
use arrow_avro::writer::{Writer, WriterBuilder};
use arrow_io::{
    BeamRowCodec, RowCodec, SchemaRowCodec, beam_to_arrow_schema, rows_to_record_batch,
};
use beam::schema::{BeamRow, Row, Schema};
use file::sink::{FileSink, FileSinkWriter};

/// Default number of elements per Avro data block.
pub const DEFAULT_BLOCK_SIZE: usize = 4096;

/// Writes elements of type `T` as Avro object container files.
///
/// Every [`block size`](Self::with_block_size) elements (default 4096) become one data
/// block, Snappy-compressed by default. The header, with the Avro schema derived from the
/// Beam schema, is written on open, so a file with no elements is still valid.
pub struct AvroSink<T> {
    codec: Arc<dyn RowCodec<T>>,
    compression: Option<CompressionCodec>,
    block_size: usize,
}

impl<T> Clone for AvroSink<T> {
    fn clone(&self) -> Self {
        Self {
            codec: Arc::clone(&self.codec),
            compression: self.compression,
            block_size: self.block_size,
        }
    }
}

impl<T: BeamRow + 'static> AvroSink<T> {
    /// A sink for `#[derive(BeamRow)]` values, using the derived schema.
    pub fn new() -> Self {
        Self::with_codec(Arc::new(BeamRowCodec::<T>::new()))
    }
}

impl<T: BeamRow + 'static> Default for AvroSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl AvroSink<Row> {
    /// A sink for [`Row`]s of `schema`.
    pub fn for_rows(schema: impl Into<Arc<Schema>>) -> Self {
        Self::with_codec(Arc::new(SchemaRowCodec::new(schema)))
    }
}

impl<T: 'static> AvroSink<T> {
    /// A sink converting elements with a custom codec.
    pub fn with_codec(codec: Arc<dyn RowCodec<T>>) -> Self {
        Self {
            codec,
            compression: Some(CompressionCodec::Snappy),
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }

    /// Sets the block codec: Deflate, Snappy or ZStandard; `None` writes uncompressed blocks.
    pub fn with_compression(mut self, compression: Option<CompressionCodec>) -> Self {
        self.compression = compression;
        self
    }

    /// Sets how many elements are written per data block (default 4096).
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size.max(1);
        self
    }

    /// The Beam schema of the rows written.
    pub fn schema(&self) -> &Arc<Schema> {
        self.codec.schema()
    }
}

impl<T: 'static> FileSink<T> for AvroSink<T> {
    fn open(&self, out: Box<dyn Write + Send>) -> beam::Result<Box<dyn FileSinkWriter<T>>> {
        let schema = Arc::clone(self.codec.schema());
        let arrow_schema = beam_to_arrow_schema(&schema)?;
        let writer = WriterBuilder::new(arrow_schema)
            .with_compression(self.compression)
            .build::<_, AvroOcfFormat>(out)
            .map_err(|e| beam::Error::from(e).context("Failed to start Avro file"))?;
        Ok(Box::new(AvroSinkWriter {
            codec: Arc::clone(&self.codec),
            schema,
            writer,
            rows: Vec::with_capacity(self.block_size),
            block_size: self.block_size,
        }))
    }
}

struct AvroSinkWriter<T> {
    codec: Arc<dyn RowCodec<T>>,
    schema: Arc<Schema>,
    writer: Writer<Box<dyn Write + Send>, AvroOcfFormat>,
    rows: Vec<Row>,
    block_size: usize,
}

impl<T> AvroSinkWriter<T> {
    fn flush_rows(&mut self) -> beam::Result {
        if self.rows.is_empty() {
            return Ok(());
        }
        let batch = rows_to_record_batch(&self.schema, &self.rows)?;
        self.rows.clear();
        self.writer
            .write(&batch)
            .map_err(|e| beam::Error::from(e).context("Failed to write Avro block"))
    }
}

impl<T: 'static> FileSinkWriter<T> for AvroSinkWriter<T> {
    fn write(&mut self, element: &T) -> beam::Result {
        self.rows.push(self.codec.to_row(element)?);
        if self.rows.len() >= self.block_size {
            self.flush_rows()?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> beam::Result {
        self.flush_rows()?;
        self.writer
            .finish()
            .map_err(|e| beam::Error::from(e).context("Failed to finish Avro file"))?;
        Ok(self.writer.into_inner().flush()?)
    }
}
