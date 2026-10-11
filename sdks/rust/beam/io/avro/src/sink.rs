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
use arrow_io::{BeamRowCodec, RowBatchBuilder, RowCodec, SchemaRowCodec, beam_to_arrow_schema};
use beam::schema::{BeamRow, RowElement, Schema};
use file::{FileSink, FileSinkWriter};

/// Default number of elements per Avro data block.
pub const DEFAULT_BLOCK_SIZE: usize = 4096;

/// The settings of an [`AvroSink`] that do not depend on the element type.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AvroOptions {
    pub(crate) compression: Option<CompressionCodec>,
    pub(crate) block_size: usize,
}

impl Default for AvroOptions {
    fn default() -> Self {
        Self {
            compression: Some(CompressionCodec::Snappy),
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}

/// Writes elements of type `T` as Avro object container files.
///
/// Every [`block size`](Self::with_block_size) elements (default 4096) become one data
/// block, Snappy-compressed by default. The header, with the Avro schema derived from the
/// Beam schema, is written on open, so a file with no elements is still valid.
pub struct AvroSink<T> {
    codec: Arc<dyn RowCodec<T>>,
    options: AvroOptions,
}

impl<T> Clone for AvroSink<T> {
    fn clone(&self) -> Self {
        Self {
            codec: Arc::clone(&self.codec),
            options: self.options,
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

impl AvroSink<RowElement> {
    /// A sink for rows of `schema`, as row coder bytes or as [`Row`](beam::schema::Row)s.
    pub fn for_rows(schema: impl Into<Arc<Schema>>) -> Self {
        Self::with_codec(Arc::new(SchemaRowCodec::new(schema)))
    }
}

impl<T: 'static> AvroSink<T> {
    /// A sink converting elements with a custom codec.
    fn with_codec(codec: Arc<dyn RowCodec<T>>) -> Self {
        Self::with_options(codec, AvroOptions::default())
    }

    pub(crate) fn with_options(codec: Arc<dyn RowCodec<T>>, options: AvroOptions) -> Self {
        Self { codec, options }
    }

    /// Sets the block codec: Deflate, Snappy or ZStandard; `None` writes uncompressed blocks.
    ///
    /// `arrow_avro::CompressionCodec` represents uncompressed blocks as `None`, unlike Parquet
    /// which defines `Compression::UNCOMPRESSED`.
    pub fn with_compression(mut self, compression: Option<CompressionCodec>) -> Self {
        self.options.compression = compression;
        self
    }

    /// Sets how many elements are written per data block (default 4096).
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.options.block_size = block_size.max(1);
        self
    }

    /// The Beam schema of the rows written.
    pub fn schema(&self) -> &Arc<Schema> {
        self.codec.schema()
    }
}

impl<T: 'static> FileSink<T> for AvroSink<T> {
    fn open(&self, out: Box<dyn Write + Send>) -> beam::Result<Box<dyn FileSinkWriter<T>>> {
        let schema = self.codec.schema();
        let arrow_schema = beam_to_arrow_schema(schema)?;
        let builder = RowBatchBuilder::new(schema)?;
        let writer = WriterBuilder::new(arrow_schema)
            .with_compression(self.options.compression)
            .build::<_, AvroOcfFormat>(out)
            .map_err(|e| beam::Error::from(e).context("Failed to start Avro file"))?;
        Ok(Box::new(AvroSinkWriter {
            codec: Arc::clone(&self.codec),
            writer,
            builder,
            block_size: self.options.block_size,
        }))
    }
}

struct AvroSinkWriter<T> {
    codec: Arc<dyn RowCodec<T>>,
    writer: Writer<Box<dyn Write + Send>, AvroOcfFormat>,
    builder: RowBatchBuilder,
    block_size: usize,
}

impl<T> AvroSinkWriter<T> {
    fn flush_rows(&mut self) -> beam::Result {
        if self.builder.is_empty() {
            return Ok(());
        }
        let batch = self.builder.finish()?;
        self.writer
            .write(&batch)
            .map_err(|e| beam::Error::from(e).context("Failed to write Avro block"))
    }
}

impl<T: 'static> FileSinkWriter<T> for AvroSinkWriter<T> {
    fn write(&mut self, element: &T) -> beam::Result {
        self.codec.append(element, &mut self.builder)?;
        if self.builder.len() >= self.block_size {
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
