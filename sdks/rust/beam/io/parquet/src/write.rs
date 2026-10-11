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

//! The `ParquetIO.Write` transforms: a [`ParquetSink`] driven by [`WriteFiles`].
//!
//! [`Write<T>`] writes `#[derive(BeamRow)]` values with the schema of `T`. [`WriteRows`]
//! writes [`Row`]s with the schema of the input PCollection.

use std::sync::Arc;

use arrow_io::{BeamRowCodec, RowCodec, SchemaRowCodec};
use beam::coders::DefaultCoder;
use beam::schema::{BeamRow, Row, RowElement};
use beam::transforms::PTransform;
use beam::values::PCollection;
use file::{FileNamingContext, FilenamePolicy, WriteFiles};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::sink::{ParquetOptions, ParquetSink};

type Configure<T> = Arc<dyn Fn(WriteFiles<T>) -> WriteFiles<T> + Send + Sync>;

/// Writes a `PCollection<T>` of `#[derive(BeamRow)]` values as Parquet files and outputs
/// the final file names.
///
/// [`WriteFiles`] with a [`ParquetSink`]; file names default to
/// `prefix-SSSSS-of-NNNNN.parquet`.
///
/// ```no_run
/// # use beam::values::PCollection;
/// # use beam::schema::BeamRow;
/// # fn f<T: BeamRow + beam::coders::DefaultCoder>(events: PCollection<T>) {
/// use parquet_io::parquetio::{self, Compression, ZstdLevel};
///
/// let files = events.apply(
///     parquetio::Write::<T>::new("WriteEvents", "/tmp/out/events")
///         .with_num_shards(4)
///         .with_compression(Compression::ZSTD(ZstdLevel::default())),
/// );
/// # }
/// ```
pub struct Write<T> {
    name: String,
    prefix: String,
    options: ParquetOptions,
    configure: Configure<T>,
}

impl<T> Clone for Write<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            prefix: self.prefix.clone(),
            options: self.options.clone(),
            configure: Arc::clone(&self.configure),
        }
    }
}

impl<T: BeamRow + DefaultCoder> Write<T> {
    /// Writes files that start with `prefix`.
    pub fn new(name: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self::create(name.into(), prefix.into())
    }

    /// The sink every file is written with.
    pub fn sink(&self) -> ParquetSink<T> {
        self.sink_with(Arc::new(BeamRowCodec::<T>::new()))
    }

    /// The [`WriteFiles`] transform this expands to.
    pub fn write_files(&self) -> WriteFiles<T> {
        self.write_files_with(Arc::new(BeamRowCodec::<T>::new()))
    }
}

impl<T: DefaultCoder> Write<T> {
    fn create(name: String, prefix: String) -> Self {
        Self {
            name,
            prefix,
            options: ParquetOptions::default(),
            configure: Arc::new(|files: WriteFiles<T>| files.with_suffix(".parquet")),
        }
    }

    /// Adds a [`WriteFiles`] setting after those already set.
    fn configure(
        mut self,
        f: impl Fn(WriteFiles<T>) -> WriteFiles<T> + Send + Sync + 'static,
    ) -> Self {
        let previous = Arc::clone(&self.configure);
        self.configure = Arc::new(move |files| f(previous(files)));
        self
    }

    /// Sets the file name suffix (default `.parquet`).
    pub fn with_suffix(self, suffix: impl Into<String>) -> Self {
        let suffix = suffix.into();
        self.configure(move |files| files.with_suffix(suffix.clone()))
    }

    /// Writes exactly `num_shards` files per window (zero: runner-chosen).
    pub fn with_num_shards(self, num_shards: u32) -> Self {
        self.configure(move |files| files.with_num_shards(num_shards))
    }

    /// Overrides the shard template (default `-SSSSS-of-NNNNN`).
    pub fn with_shard_template(self, template: impl Into<String>) -> Self {
        let template = template.into();
        self.configure(move |files| files.with_shard_template(template.clone()))
    }

    /// Writes a single file named exactly `prefix` + suffix.
    pub fn without_sharding(self) -> Self {
        self.configure(WriteFiles::without_sharding)
    }

    /// Starts a new file once the current one holds `max` records.
    pub fn with_max_records_per_file(self, max: u64) -> Self {
        self.configure(move |files| files.with_max_records_per_file(max))
    }

    /// Starts a new file at about `max` bytes, counting the in-progress row group.
    pub fn with_max_bytes_per_file(self, max: u64) -> Self {
        self.configure(move |files| files.with_max_bytes_per_file(max))
    }

    /// Writes separate files per window and pane. Required for unbounded input.
    pub fn with_windowed_writes(self) -> Self {
        self.configure(WriteFiles::with_windowed_writes)
    }

    pub fn with_filename_policy<P: FilenamePolicy>(self, policy: P) -> Self {
        let policy = Arc::new(policy);
        self.configure(move |files| {
            let policy = Arc::clone(&policy);
            files.with_filename_policy(move |base: &str, ctx: &FileNamingContext<'_>| {
                policy.file_path(base, ctx)
            })
        })
    }

    /// Places temporary files under `dir` (same filesystem as the output).
    pub fn with_temp_directory(self, dir: impl Into<String>) -> Self {
        let dir = dir.into();
        self.configure(move |files| files.with_temp_directory(dir.clone()))
    }

    /// Sets the column compression codec (default Snappy).
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.options.compression = compression;
        self
    }

    /// Caps the number of rows per row group.
    pub fn with_row_group_size(mut self, rows: usize) -> Self {
        self.options.max_row_group_rows = Some(rows.max(1));
        self
    }

    /// Caps the encoded size of a row group.
    pub fn with_row_group_bytes(mut self, bytes: usize) -> Self {
        self.options.max_row_group_bytes = Some(bytes.max(1));
        self
    }

    /// Uses fully custom Parquet writer properties.
    pub fn with_writer_properties(mut self, properties: WriterProperties) -> Self {
        self.options.properties = Some(properties);
        self
    }

    /// Sets how many elements are buffered before being encoded (default 1024).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.options.batch_size = batch_size.max(1);
        self
    }

    fn sink_with(&self, codec: Arc<dyn RowCodec<T>>) -> ParquetSink<T> {
        ParquetSink::with_options(codec, self.options.clone())
    }

    fn write_files_with(&self, codec: Arc<dyn RowCodec<T>>) -> WriteFiles<T> {
        (self.configure)(WriteFiles::new(
            self.name.clone(),
            self.prefix.clone(),
            self.sink_with(codec),
        ))
    }
}

impl<T: BeamRow + DefaultCoder> PTransform<PCollection<T>> for Write<T> {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<String> {
        self.write_files().expand(input)
    }
}

/// Writes a `PCollection<Row>` as Parquet files, with the row schema of the input,
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
/// use parquet_io::parquetio;
///
/// let files = rows.apply(parquetio::WriteRows::new("WriteRows", "/tmp/out/rows"));
/// # }
/// ```
#[derive(Clone)]
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
    pub fn new(name: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            inner: Write::create(name.into(), prefix.into()),
        }
    }

    forward! {
        /// Sets the file name suffix (default `.parquet`).
        with_suffix(suffix: impl Into<String>);
        /// Writes exactly `num_shards` files per window (zero: runner-chosen).
        with_num_shards(num_shards: u32);
        /// Overrides the shard template (default `-SSSSS-of-NNNNN`).
        with_shard_template(template: impl Into<String>);
        /// Writes a single file named exactly `prefix` + suffix.
        without_sharding();
        /// Starts a new file once the current one holds `max` records.
        with_max_records_per_file(max: u64);
        /// Starts a new file at about `max` bytes, counting the in-progress row group.
        with_max_bytes_per_file(max: u64);
        /// Writes separate files per window and pane. Required for unbounded input.
        with_windowed_writes();
        /// Places temporary files under `dir` (same filesystem as the output).
        with_temp_directory(dir: impl Into<String>);
        /// Sets the column compression codec (default Snappy).
        with_compression(compression: Compression);
        /// Caps the number of rows per row group.
        with_row_group_size(rows: usize);
        /// Caps the encoded size of a row group.
        with_row_group_bytes(bytes: usize);
        /// Uses fully custom Parquet writer properties.
        with_writer_properties(properties: WriterProperties);
        /// Sets how many rows are buffered before being encoded (default 1024).
        with_batch_size(batch_size: usize);
    }

    pub fn with_filename_policy<P: FilenamePolicy>(self, policy: P) -> Self {
        Self {
            inner: self.inner.with_filename_policy(policy),
        }
    }

    /// Fallible form of [`PTransform::expand`]: fails if the input has no row schema.
    pub fn try_expand(&self, input: &PCollection<Row>) -> beam::Result<PCollection<String>> {
        let schema = input.row_schema().ok_or_else(|| {
            beam::Error::msg(format!("{}: the input has no row schema", self.inner.name))
        })?;
        let codec = Arc::new(SchemaRowCodec::new(schema));
        Ok(self
            .inner
            .write_files_with(codec)
            .expand(&input.as_row_elements()))
    }
}

impl PTransform<PCollection<Row>> for WriteRows {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<Row>) -> PCollection<String> {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.inner.name))
    }
}
