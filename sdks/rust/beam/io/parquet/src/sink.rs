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

//! [`FileSink`] writing Parquet files with arrow-rs's [`ArrowWriter`].

use std::io::Write;
use std::sync::Arc;

use arrow_io::{
    BeamRowCodec, RowCodec, SchemaRowCodec, beam_to_arrow_schema, rows_to_record_batch,
};
use beam::schema::{BeamRow, Row, Schema};
use file::sink::{FileSink, FileSinkWriter};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Default number of elements buffered before they are encoded as one Arrow batch.
pub const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;

/// Writes elements of type `T` as Parquet files. Every [`batch size`](Self::with_batch_size)
/// elements (default 1024) go to an [`ArrowWriter`], which flushes a row group at the
/// [row group limits](Self::with_row_group_size) and writes the footer on finish.
/// Snappy-compressed by default. An empty file is still a valid Parquet file.
pub struct ParquetSink<T> {
    codec: Arc<dyn RowCodec<T>>,
    compression: Compression,
    max_row_group_rows: Option<usize>,
    max_row_group_bytes: Option<usize>,
    properties: Option<WriterProperties>,
    batch_size: usize,
}

impl<T> Clone for ParquetSink<T> {
    fn clone(&self) -> Self {
        Self {
            codec: Arc::clone(&self.codec),
            compression: self.compression,
            max_row_group_rows: self.max_row_group_rows,
            max_row_group_bytes: self.max_row_group_bytes,
            properties: self.properties.clone(),
            batch_size: self.batch_size,
        }
    }
}

impl<T: BeamRow + 'static> ParquetSink<T> {
    /// A sink for `#[derive(BeamRow)]` values, using the derived schema.
    pub fn new() -> Self {
        Self::with_codec(Arc::new(BeamRowCodec::<T>::new()))
    }
}

impl<T: BeamRow + 'static> Default for ParquetSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetSink<Row> {
    /// A sink for [`Row`]s of `schema`.
    pub fn for_rows(schema: impl Into<Arc<Schema>>) -> Self {
        Self::with_codec(Arc::new(SchemaRowCodec::new(schema)))
    }
}

impl<T: 'static> ParquetSink<T> {
    /// A sink converting elements with a custom codec.
    pub fn with_codec(codec: Arc<dyn RowCodec<T>>) -> Self {
        Self {
            codec,
            compression: Compression::SNAPPY,
            max_row_group_rows: None,
            max_row_group_bytes: None,
            properties: None,
            batch_size: DEFAULT_WRITE_BATCH_SIZE,
        }
    }

    /// Sets the column codec (default [`Compression::SNAPPY`]). Only `snap` and `zstd` are
    /// enabled; others fail when the file is opened unless enabled downstream.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Caps the number of rows per row group.
    pub fn with_row_group_size(mut self, rows: usize) -> Self {
        self.max_row_group_rows = Some(rows.max(1));
        self
    }

    /// Caps the encoded size of a row group, bounding the writer's memory use.
    pub fn with_row_group_bytes(mut self, bytes: usize) -> Self {
        self.max_row_group_bytes = Some(bytes.max(1));
        self
    }

    /// Uses custom writer properties, overriding compression and row group limits.
    pub fn with_writer_properties(mut self, properties: WriterProperties) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Sets how many elements are buffered before being encoded (default 1024).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// The Beam schema of the rows written.
    pub fn schema(&self) -> &Arc<Schema> {
        self.codec.schema()
    }

    fn writer_properties(&self) -> WriterProperties {
        self.properties.clone().unwrap_or_else(|| {
            let mut builder = WriterProperties::builder().set_compression(self.compression);
            if let Some(rows) = self.max_row_group_rows {
                builder = builder.set_max_row_group_row_count(Some(rows));
            }
            if let Some(bytes) = self.max_row_group_bytes {
                builder = builder.set_max_row_group_bytes(Some(bytes));
            }
            builder.build()
        })
    }
}

impl<T: 'static> FileSink<T> for ParquetSink<T> {
    fn open(&self, out: Box<dyn Write + Send>) -> beam::Result<Box<dyn FileSinkWriter<T>>> {
        let schema = Arc::clone(self.codec.schema());
        let arrow_schema = beam_to_arrow_schema(&schema)?;
        let writer =
            ArrowWriter::try_new(out, Arc::new(arrow_schema), Some(self.writer_properties()))
                .map_err(|e| beam::Error::from(e).context("Failed to start Parquet file"))?;
        Ok(Box::new(ParquetSinkWriter {
            codec: Arc::clone(&self.codec),
            schema,
            writer,
            rows: Vec::with_capacity(self.batch_size),
            batch_size: self.batch_size,
        }))
    }
}

struct ParquetSinkWriter<T> {
    codec: Arc<dyn RowCodec<T>>,
    schema: Arc<Schema>,
    writer: ArrowWriter<Box<dyn Write + Send>>,
    rows: Vec<Row>,
    batch_size: usize,
}

impl<T> ParquetSinkWriter<T> {
    fn flush_rows(&mut self) -> beam::Result {
        if self.rows.is_empty() {
            return Ok(());
        }
        let batch = rows_to_record_batch(&self.schema, &self.rows)?;
        self.rows.clear();
        self.writer
            .write(&batch)
            .map_err(|e| beam::Error::from(e).context("Failed to write Parquet batch"))
    }
}

impl<T: 'static> FileSinkWriter<T> for ParquetSinkWriter<T> {
    fn write(&mut self, element: &T) -> beam::Result {
        self.rows.push(self.codec.to_row(element)?);
        if self.rows.len() >= self.batch_size {
            self.flush_rows()?;
        }
        Ok(())
    }

    /// Encoded size of the in-progress row group, without rows not yet batched.
    fn buffered_bytes(&self) -> u64 {
        self.writer.in_progress_size() as u64
    }

    fn finish(mut self: Box<Self>) -> beam::Result {
        self.flush_rows()?;
        // `into_inner` flushes the last row group and writes the footer.
        let mut out = self
            .writer
            .into_inner()
            .map_err(|e| beam::Error::from(e).context("Failed to finish Parquet file"))?;
        Ok(out.flush()?)
    }
}
