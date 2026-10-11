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

//! Avro write transforms: an [`AvroSink`] driven by [`WriteFiles`].
//!
//! [`Write<T>`] writes `#[derive(BeamRow)]` values with the schema of `T`. [`WriteRows`]
//! writes [`Row`]s with the schema of the input PCollection.

use std::sync::Arc;

use arrow_avro::compression::CompressionCodec;
use arrow_io::{BeamRowCodec, RowCodec, SchemaRowCodec};
use beam::coders::DefaultCoder;
use beam::internals::PipelineInternals;
use beam::pipeline::PipelineError;
use beam::schema::{BeamRow, Row, RowElement, Schema};
use beam::transforms::PTransform;
use beam::values::PCollection;
use file::{FilenamePolicy, WriteFiles, WriteFilesOptions};

use crate::sink::{AvroOptions, AvroSink};

/// Writes a `PCollection<T>` of `#[derive(BeamRow)]` values as Avro container files and
/// outputs the final file names.
///
/// [`WriteFiles`] with an [`AvroSink`]; file names default to `prefix-SSSSS-of-NNNNN.avro`.
///
/// ```no_run
/// # use beam::values::PCollection;
/// # use beam::schema::BeamRow;
/// # fn f<T: BeamRow + beam::coders::DefaultCoder>(events: PCollection<T>) {
/// use avro_io::avroio::{self, CompressionCodec};
///
/// let files = events.apply(
///     "WriteEvents",
///     avroio::Write::<T>::new("/tmp/out/events")
///         .with_num_shards(4)
///         .with_compression(Some(CompressionCodec::ZStandard)),
/// );
/// # }
/// ```
pub struct Write<T> {
    prefix: String,
    options: AvroOptions,
    file_options: WriteFilesOptions,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Clone for Write<T> {
    fn clone(&self) -> Self {
        Self {
            prefix: self.prefix.clone(),
            options: self.options,
            file_options: self.file_options.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for Write<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("prefix", &self.prefix)
            .field("options", &self.options)
            .field("file_options", &self.file_options)
            .finish()
    }
}

impl<T: BeamRow + DefaultCoder> Write<T> {
    /// Writes files that start with `prefix`.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self::create(prefix.into())
    }

    /// The sink every file is written with.
    #[doc(hidden)]
    pub fn sink(&self) -> AvroSink<T> {
        self.sink_with(Arc::new(BeamRowCodec::<T>::new()))
    }

    /// The [`WriteFiles`] transform this expands to.
    #[doc(hidden)]
    pub fn write_files(&self) -> WriteFiles<T> {
        self.write_files_with(Arc::new(BeamRowCodec::<T>::new()))
    }
}

impl<T: DefaultCoder> Write<T> {
    fn create(prefix: String) -> Self {
        Self {
            prefix,
            options: AvroOptions::default(),
            file_options: WriteFilesOptions::with_default_suffix(".avro"),
            _marker: std::marker::PhantomData,
        }
    }

    /// Sets the file name suffix (default `.avro`).
    pub fn with_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.file_options.suffix = suffix.into();
        self
    }

    /// Writes exactly `num_shards` files per window (zero: runner-chosen).
    pub fn with_num_shards(mut self, num_shards: u32) -> Self {
        self.file_options.num_shards = num_shards;
        self
    }

    /// Overrides the shard template (default `-SSSSS-of-NNNNN`).
    pub fn with_shard_template(mut self, template: impl Into<String>) -> Self {
        self.file_options.shard_template = template.into();
        self
    }

    /// Writes a single file named exactly `prefix` + suffix.
    pub fn without_sharding(mut self) -> Self {
        self.file_options.num_shards = 1;
        self.file_options.shard_template = String::new();
        self
    }

    /// Starts a new file once the current one holds `max` records.
    pub fn with_max_records_per_file(mut self, max: u64) -> Self {
        self.file_options.max_records_per_file = Some(max);
        self
    }

    /// Starts a new file at about `max` bytes. Bytes count when their block is written, so
    /// a file can exceed the limit by one block.
    pub fn with_max_bytes_per_file(mut self, max: u64) -> Self {
        self.file_options.max_bytes_per_file = Some(max);
        self
    }

    /// Writes separate files per window and pane. Required for unbounded input.
    pub fn with_windowed_writes(mut self) -> Self {
        self.file_options.windowed_writes = true;
        self
    }

    /// Names output files with `policy` instead of the shard template.
    pub fn with_filename_policy<P: FilenamePolicy>(mut self, policy: P) -> Self {
        self.file_options.policy = Arc::new(policy);
        self
    }

    /// Places temporary files under `dir` (same filesystem as the output).
    pub fn with_temp_directory(mut self, dir: impl Into<String>) -> Self {
        self.file_options.temp_directory = Some(dir.into());
        self
    }

    /// Sets the block compression codec (default Snappy); `None` disables it.
    ///
    /// `arrow_avro::CompressionCodec` represents uncompressed blocks as `None`, unlike Parquet
    /// which defines `Compression::UNCOMPRESSED`.
    pub fn with_compression(mut self, compression: Option<CompressionCodec>) -> Self {
        self.options.compression = compression;
        self
    }

    /// Sets how many elements are written per Avro data block (default 4096).
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.options.block_size = block_size.max(1);
        self
    }

    fn sink_with(&self, codec: Arc<dyn RowCodec<T>>) -> AvroSink<T> {
        AvroSink::with_options(codec, self.options)
    }

    fn write_files_with(&self, codec: Arc<dyn RowCodec<T>>) -> WriteFiles<T> {
        WriteFiles::new(self.prefix.clone(), self.sink_with(codec))
            .with_options(self.file_options.clone())
    }
}

impl<T: BeamRow + DefaultCoder> PTransform<PCollection<T>> for Write<T> {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<String> {
        self.write_files().expand(input)
    }
}

/// Writes a `PCollection<Row>` as Avro container files, with the row schema of the input,
/// and outputs the final file names.
///
/// The writer reads the row coder bytes of an element from another worker or SDK, with no
/// [`Row`] between. Set the schema of the input with `with_row_schema` if its producer does
/// not set it.
///
/// ```no_run
/// # use beam::values::PCollection;
/// # use beam::schema::Row;
/// # fn f(rows: PCollection<Row>) {
/// use avro_io::avroio;
///
/// let files = rows.apply("WriteRows", avroio::WriteRows::new("/tmp/out/rows"));
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct WriteRows {
    inner: Write<RowElement>,
}

/// Forwards builder methods of [`WriteRows`] to its inner [`Write`].
macro_rules! forward {
    ($($(#[$doc:meta])* $method:ident($($arg:ident: $ty:ty),*);)*) => {
        $(
            $(#[$doc])*
            pub fn $method(self, $($arg: $ty),*) -> Self {
                Self { inner: self.inner.$method($($arg),*) }
            }
        )*
    };
}

impl WriteRows {
    /// Writes files that start with `prefix`.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            inner: Write::create(prefix.into()),
        }
    }

    forward! {
        /// Sets the file name suffix (default `.avro`).
        with_suffix(suffix: impl Into<String>);
        /// Writes exactly `num_shards` files per window (zero: runner-chosen).
        with_num_shards(num_shards: u32);
        /// Overrides the shard template (default `-SSSSS-of-NNNNN`).
        with_shard_template(template: impl Into<String>);
        /// Writes a single file named exactly `prefix` + suffix.
        without_sharding();
        /// Starts a new file once the current one holds `max` records.
        with_max_records_per_file(max: u64);
        /// Starts a new file at about `max` bytes.
        with_max_bytes_per_file(max: u64);
        /// Writes separate files per window and pane. Required for unbounded input.
        with_windowed_writes();
        /// Places temporary files under `dir` (same filesystem as the output).
        with_temp_directory(dir: impl Into<String>);
        /// Sets the block compression codec (default Snappy); `None` disables it.
        with_compression(compression: Option<CompressionCodec>);
        /// Sets how many rows are written per Avro data block (default 4096).
        with_block_size(block_size: usize);
    }

    /// Names output files with `policy` instead of the shard template.
    pub fn with_filename_policy<P: FilenamePolicy>(self, policy: P) -> Self {
        Self {
            inner: self.inner.with_filename_policy(policy),
        }
    }
}

/// An input without a row schema is a construction error: [`Pipeline::validate`] returns it,
/// so the pipeline is not run.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl PTransform<PCollection<Row>> for WriteRows {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<Row>) -> PCollection<String> {
        // Without a schema, record the error and build the write with an empty schema, so the
        // later transforms still attach to its output.
        let schema = input.row_schema().unwrap_or_else(|| {
            let pipeline = input.pipeline();
            pipeline.record_construction_error(PipelineError::InvalidConstruction {
                transform_id: pipeline.scope_name().unwrap_or_default(),
                reason: "the input has no row schema".to_string(),
            });
            Arc::new(Schema::builder().build())
        });
        let codec = Arc::new(SchemaRowCodec::new(schema));
        self.inner
            .write_files_with(codec)
            .expand(&input.as_row_elements())
    }
}
