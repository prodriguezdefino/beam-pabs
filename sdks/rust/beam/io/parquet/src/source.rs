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

//! Splittable Parquet reads.
//!
//! A file's restriction is its byte range `[0, size)`, split like any [`FileBasedSource`].
//! A **row group belongs to the split that contains its first byte** (the start of its
//! first column chunk). The split claims that offset and reads that row group. No row
//! group is read twice or skipped. Dynamic splits happen between row groups.

use std::ops::ControlFlow;
use std::sync::Arc;

use arrow_io::{BeamRowCodec, RowCodec, RowOutput, SchemaRowCodec};
use beam::coders::DefaultCoder;
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam::transforms::PTransform;
use beam::transforms::ProcessContext;
use beam::transforms::sdf::{OffsetRangeTracker, RestrictionTracker};
use beam::values::{PBegin, PCollection};
use file::FileSystem;
use file::{DEFAULT_SPLIT_SIZE, FileBasedSource, FileRecordReader, ReadAllViaFileBasedSource};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::RowGroupMetaData;

use crate::chunk::FileSystemChunkReader;
use crate::error::ParquetIoError;

/// Default number of rows decoded per Arrow batch while reading.
pub const DEFAULT_READ_BATCH_SIZE: usize = 1024;

/// The file offset at which a row group starts: its first column chunk.
fn row_group_start(row_group: &RowGroupMetaData) -> i64 {
    row_group
        .columns()
        .iter()
        .map(|column| i64::try_from(column.byte_range().0).unwrap_or(i64::MAX))
        .min()
        .or_else(|| row_group.file_offset())
        .unwrap_or(0)
}

/// Reads the Beam schema of a Parquet file from its footer, for example to build a
/// [`ReadRows`] at pipeline construction.
pub fn schema_of(path: &str) -> Result<Schema, ParquetIoError> {
    let chunk = FileSystemChunkReader::open(path)?;
    let metadata = ArrowReaderMetadata::load(&chunk, ArrowReaderOptions::new())?;
    Ok(arrow_io::arrow_to_beam_schema(metadata.schema())?)
}

/// [`FileRecordReader`] decoding Parquet row groups into elements of type `T`.
pub struct ParquetRecordReader<T> {
    codec: Arc<dyn RowCodec<T>>,
    batch_size: usize,
}

impl<T> Clone for ParquetRecordReader<T> {
    fn clone(&self) -> Self {
        Self {
            codec: Arc::clone(&self.codec),
            batch_size: self.batch_size,
        }
    }
}

impl<T: 'static> ParquetRecordReader<T> {
    /// Creates a new record reader with `codec` and record `batch_size`.
    ///
    /// Clamps batch sizes less than 1 to 1.
    pub fn new(codec: Arc<dyn RowCodec<T>>, batch_size: usize) -> Self {
        Self {
            codec,
            batch_size: batch_size.max(1),
        }
    }

    /// Decodes the row groups of `file` whose start offsets `tracker` accepts.
    #[doc(hidden)]
    pub fn read_with_tracker(
        &self,
        file: &str,
        tracker: &OffsetRangeTracker,
        mut emit: impl FnMut(T) -> beam::Result,
    ) -> beam::Result {
        let restriction = tracker.current_restriction();
        if restriction.is_empty() {
            return Ok(());
        }
        let chunk = FileSystemChunkReader::open(file).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to open Parquet file '{file}'"))
        })?;
        let metadata =
            ArrowReaderMetadata::load(&chunk, ArrowReaderOptions::new()).map_err(|e| {
                beam::Error::from(e).context(format!("Failed to read Parquet footer of '{file}'"))
            })?;

        // Read only the top-level columns the element schema asks for.
        let wanted = self.codec.schema();
        let descriptor = metadata.metadata().file_metadata().schema_descr();
        let roots: Vec<usize> = descriptor
            .root_schema()
            .get_fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| wanted.field(field.name()).is_some())
            .map(|(i, _)| i)
            .collect();
        let projection = ProjectionMask::roots(descriptor, roots);

        let mut groups: Vec<(i64, usize)> = metadata
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .map(|(i, row_group)| (row_group_start(row_group), i))
            .collect();
        groups.sort_unstable();

        let flow = groups
            .into_iter()
            .filter(|(s, _)| *s >= restriction.start)
            .try_for_each(|(start, index)| {
                if !tracker.try_claim(&start) {
                    return ControlFlow::Break(Ok(()));
                }
                if let Err(e) =
                    self.read_row_group(&chunk, &metadata, &projection, index, file, &mut emit)
                {
                    return ControlFlow::Break(Err(e));
                }
                ControlFlow::Continue(())
            });

        match flow {
            ControlFlow::Continue(()) => {
                // Mark the remainder of the range, which holds no row group start, done.
                tracker.try_claim(&tracker.current_restriction().end);
                Ok(())
            }
            ControlFlow::Break(result) => result,
        }
    }

    /// Reads and emits all elements from one row group.
    fn read_row_group(
        &self,
        chunk: &FileSystemChunkReader,
        metadata: &ArrowReaderMetadata,
        projection: &ProjectionMask,
        index: usize,
        file: &str,
        emit: &mut impl FnMut(T) -> beam::Result,
    ) -> beam::Result {
        let reader =
            ParquetRecordBatchReaderBuilder::new_with_metadata(chunk.clone(), metadata.clone())
                .with_row_groups(vec![index])
                .with_projection(projection.clone())
                .with_batch_size(self.batch_size)
                .build()
                .map_err(|e| {
                    beam::Error::from(e)
                        .context(format!("Failed to read row group {index} of '{file}'"))
                })?;
        for batch in reader {
            let batch = batch.map_err(|e| {
                beam::Error::from(e)
                    .context(format!("Failed to decode row group {index} of '{file}'"))
            })?;
            let elements = self.codec.decode_batch(&batch).map_err(|e| {
                beam::Error::from(e).context(format!("Failed to convert rows of '{file}'"))
            })?;
            elements.into_iter().try_for_each(&mut *emit)?;
        }
        Ok(())
    }
}

impl<T: DefaultCoder> FileRecordReader<T> for ParquetRecordReader<T> {
    fn read_records(
        &self,
        _fs: &dyn FileSystem,
        file: &str,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, T>,
    ) -> beam::Result {
        // The FileRecordReader trait passes a borrowed FileSystem, but parquet
        // ChunkReader requires an owned static reader.
        self.read_with_tracker(file, tracker, |element| ctx.emit(element))
    }
}

/// Settings shared by every read transform.
struct ReadConfig<T> {
    name: String,
    split_size: u64,
    batch_size: usize,
    codec: Arc<dyn RowCodec<T>>,
}

impl<T> Clone for ReadConfig<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            split_size: self.split_size,
            batch_size: self.batch_size,
            codec: Arc::clone(&self.codec),
        }
    }
}

impl<T: 'static> ReadConfig<T> {
    fn new(name: String, codec: Arc<dyn RowCodec<T>>) -> Self {
        Self {
            name,
            split_size: DEFAULT_SPLIT_SIZE,
            batch_size: DEFAULT_READ_BATCH_SIZE,
            codec,
        }
    }

    fn reader(&self) -> ParquetRecordReader<T> {
        ParquetRecordReader::new(Arc::clone(&self.codec), self.batch_size)
    }
}

macro_rules! read_builders {
    () => {
        /// Sets the byte size of initial splits (default 8 MiB). Splits smaller than a
        /// row group can be empty.
        pub fn with_split_size(mut self, split_size: u64) -> Self {
            self.config.split_size = split_size;
            self
        }

        /// Sets how many rows are decoded per Arrow batch (default 1024).
        ///
        /// Clamps values less than 1 to 1.
        pub fn with_batch_size(mut self, batch_size: usize) -> Self {
            self.config.batch_size = batch_size.max(1);
            self
        }
    };
}

/// Reads Parquet files that match a pattern into `#[derive(BeamRow)]` values. Columns match
/// by name; extra columns are not read, and nullable fields can be missing.
pub struct Read<T> {
    pattern: String,
    config: ReadConfig<T>,
}

impl<T> Clone for Read<T> {
    fn clone(&self) -> Self {
        Self {
            pattern: self.pattern.clone(),
            config: self.config.clone(),
        }
    }
}

impl<T: BeamRow + DefaultCoder> Read<T> {
    /// Reads every file that matches `pattern`, a path or glob on any registered filesystem.
    pub fn new(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::with_row_codec(name, pattern, Arc::new(BeamRowCodec::<T>::new()))
    }
}

impl<T: DefaultCoder> Read<T> {
    /// Reads with a custom element codec.
    fn with_row_codec(
        name: impl Into<String>,
        pattern: impl Into<String>,
        codec: Arc<dyn RowCodec<T>>,
    ) -> Self {
        Self {
            pattern: pattern.into(),
            config: ReadConfig::new(name.into(), codec),
        }
    }

    read_builders!();
}

impl<T: DefaultCoder> PTransform<PBegin> for Read<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PBegin) -> PCollection<T> {
        FileBasedSource::new(
            self.config.name.clone(),
            self.pattern.clone(),
            self.config.reader(),
        )
        .with_split_size(self.config.split_size)
        .expand(input)
    }
}

/// Reads each Parquet file named in a `PCollection<String>` into `#[derive(BeamRow)]` values.
pub struct ReadFiles<T> {
    config: ReadConfig<T>,
}

impl<T> Clone for ReadFiles<T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
        }
    }
}

impl<T: BeamRow + DefaultCoder> ReadFiles<T> {
    /// Creates a transform that reads Parquet files from input file paths into `T` records.
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_row_codec(name, Arc::new(BeamRowCodec::<T>::new()))
    }
}

impl<T: DefaultCoder> ReadFiles<T> {
    /// Reads with a custom element codec.
    fn with_row_codec(name: impl Into<String>, codec: Arc<dyn RowCodec<T>>) -> Self {
        Self {
            config: ReadConfig::new(name.into(), codec),
        }
    }

    read_builders!();
}

impl<T: DefaultCoder> PTransform<PCollection<String>> for ReadFiles<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<T> {
        ReadAllViaFileBasedSource::new(self.config.name.clone(), self.config.reader())
            .with_split_size(self.config.split_size)
            .expand(input)
    }
}

/// Reads Parquet files that match a pattern into [`Row`]s of `schema`, which can cross into
/// other SDKs. [`schema_of`] reads the schema of an existing file.
#[derive(Clone)]
pub struct ReadRows {
    schema: Arc<Schema>,
    inner: Read<RowElement>,
}

impl ReadRows {
    /// Creates a transform that reads Parquet files matching `pattern` into [`Row`]s of `schema`.
    pub fn new(
        name: impl Into<String>,
        pattern: impl Into<String>,
        schema: impl Into<Arc<Schema>>,
    ) -> Self {
        let schema = schema.into();
        let codec = Arc::new(SchemaRowCodec::new(Arc::clone(&schema)));
        Self {
            schema,
            inner: Read::with_row_codec(name, pattern, codec),
        }
    }

    /// Gives each row as its row coder bytes, with no [`Row`] between. Use it when the rows
    /// go to another SDK or cross a fusion break. A fused Rust step decodes each row again.
    pub fn with_encoded_rows(mut self) -> Self {
        self.inner.config.codec =
            Arc::new(SchemaRowCodec::new(Arc::clone(&self.schema)).with_output(RowOutput::Encoded));
        self
    }

    /// Sets the byte size of initial splits (default 8 MiB). Splits smaller than a
    /// row group can be empty.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.inner = self.inner.with_split_size(split_size);
        self
    }

    /// Sets how many rows are decoded per Arrow batch (default 1024).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.inner = self.inner.with_batch_size(batch_size);
        self
    }
}

impl PTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> PCollection<Row> {
        self.inner
            .expand(input)
            .as_rows()
            .with_row_schema(&self.schema)
    }
}

/// Reads each Parquet file named in a `PCollection<String>` into schema-aware [`Row`]s.
#[derive(Clone)]
pub struct ReadRowFiles {
    schema: Arc<Schema>,
    inner: ReadFiles<RowElement>,
}

impl ReadRowFiles {
    /// Creates a transform that reads Parquet files from input paths into [`Row`]s of `schema`.
    pub fn new(name: impl Into<String>, schema: impl Into<Arc<Schema>>) -> Self {
        let schema = schema.into();
        let codec = Arc::new(SchemaRowCodec::new(Arc::clone(&schema)));
        Self {
            schema,
            inner: ReadFiles::with_row_codec(name, codec),
        }
    }

    /// Gives each row as its row coder bytes, with no [`Row`] between. Use it when the rows
    /// go to another SDK or cross a fusion break. A fused Rust step decodes each row again.
    pub fn with_encoded_rows(mut self) -> Self {
        self.inner.config.codec =
            Arc::new(SchemaRowCodec::new(Arc::clone(&self.schema)).with_output(RowOutput::Encoded));
        self
    }

    /// Sets the byte size of initial splits (default 8 MiB). Splits smaller than a
    /// row group can be empty.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.inner = self.inner.with_split_size(split_size);
        self
    }

    /// Sets how many rows are decoded per Arrow batch (default 1024).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.inner = self.inner.with_batch_size(batch_size);
        self
    }
}

impl PTransform<PCollection<String>> for ReadRowFiles {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<Row> {
        self.inner
            .expand(input)
            .as_rows()
            .with_row_schema(&self.schema)
    }
}
