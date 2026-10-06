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

//! The `ParquetIO.Write` transform: a [`ParquetSink`] driven by [`WriteFiles`].

use std::sync::Arc;

use beam::coders::DefaultCoder;
use beam::transforms::PTransform;
use beam::values::PCollection;
use file::{FileNamingContext, FilenamePolicy, WriteFiles};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::sink::ParquetSink;

type Configure<T> = Arc<dyn Fn(WriteFiles<T>) -> WriteFiles<T> + Send + Sync>;

/// Writes a `PCollection<T>` as Parquet files and outputs the final file names.
///
/// [`WriteFiles`] with a [`ParquetSink`]; file names default to `prefix-SSSSS-of-NNNNN.parquet`.
///
/// ```no_run
/// # use beam::values::PCollection;
/// # use beam::schema::BeamRow;
/// # fn f<T: BeamRow + beam::coders::DefaultCoder>(events: PCollection<T>) {
/// use parquet_io::parquetio;
/// use parquet_io::parquet::basic::{Compression, ZstdLevel};
///
/// let files = events.apply(
///     parquetio::Write::new("WriteEvents", "/tmp/out/events", parquetio::ParquetSink::<T>::new())
///         .with_num_shards(4)
///         .with_compression(Compression::ZSTD(ZstdLevel::default())),
/// );
/// # }
/// ```
pub struct Write<T> {
    name: String,
    prefix: String,
    sink: ParquetSink<T>,
    configure: Configure<T>,
}

impl<T> Clone for Write<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            prefix: self.prefix.clone(),
            sink: self.sink.clone(),
            configure: Arc::clone(&self.configure),
        }
    }
}

impl<T: DefaultCoder> Write<T> {
    /// Writes files that start with `prefix` through `sink`: [`ParquetSink::new`] for
    /// `#[derive(BeamRow)]` values or [`ParquetSink::for_rows`] for [`Row`](beam::schema::Row)s.
    pub fn new(name: impl Into<String>, prefix: impl Into<String>, sink: ParquetSink<T>) -> Self {
        Self {
            name: name.into(),
            prefix: prefix.into(),
            sink,
            configure: Arc::new(|files: WriteFiles<T>| files.with_suffix(".parquet")),
        }
    }

    /// Applies an arbitrary [`WriteFiles`] customization, after those already set.
    pub fn with_write_files(
        mut self,
        f: impl Fn(WriteFiles<T>) -> WriteFiles<T> + Send + Sync + 'static,
    ) -> Self {
        let previous = Arc::clone(&self.configure);
        self.configure = Arc::new(move |files| f(previous(files)));
        self
    }

    fn map_sink(mut self, f: impl FnOnce(ParquetSink<T>) -> ParquetSink<T>) -> Self {
        self.sink = f(self.sink);
        self
    }

    /// Sets the file name suffix (default `.parquet`).
    pub fn with_suffix(self, suffix: impl Into<String>) -> Self {
        let suffix = suffix.into();
        self.with_write_files(move |files| files.with_suffix(suffix.clone()))
    }

    /// Writes exactly `num_shards` files per window (zero: runner-chosen).
    pub fn with_num_shards(self, num_shards: u32) -> Self {
        self.with_write_files(move |files| files.with_num_shards(num_shards))
    }

    /// Overrides the shard template (default `-SSSSS-of-NNNNN`).
    pub fn with_shard_template(self, template: impl Into<String>) -> Self {
        let template = template.into();
        self.with_write_files(move |files| files.with_shard_template(template.clone()))
    }

    /// Writes a single file named exactly `prefix` + suffix.
    pub fn without_sharding(self) -> Self {
        self.with_write_files(WriteFiles::without_sharding)
    }

    /// Starts a new file once the current one holds `max` records.
    pub fn with_max_records_per_file(self, max: u64) -> Self {
        self.with_write_files(move |files| files.with_max_records_per_file(max))
    }

    /// Starts a new file at about `max` bytes, counting the in-progress row group.
    pub fn with_max_bytes_per_file(self, max: u64) -> Self {
        self.with_write_files(move |files| files.with_max_bytes_per_file(max))
    }

    /// Writes separate files per window and pane. Required for unbounded input.
    pub fn with_windowed_writes(self) -> Self {
        self.with_write_files(WriteFiles::with_windowed_writes)
    }

    pub fn with_filename_policy<P: FilenamePolicy>(self, policy: P) -> Self {
        let policy = Arc::new(policy);
        self.with_write_files(move |files| {
            let policy = Arc::clone(&policy);
            files.with_filename_policy(move |base: &str, ctx: &FileNamingContext<'_>| {
                policy.file_path(base, ctx)
            })
        })
    }

    /// Places temporary files under `dir` (same filesystem as the output).
    pub fn with_temp_directory(self, dir: impl Into<String>) -> Self {
        let dir = dir.into();
        self.with_write_files(move |files| files.with_temp_directory(dir.clone()))
    }

    /// Sets the column compression codec (default Snappy).
    pub fn with_compression(self, compression: Compression) -> Self {
        self.map_sink(|sink| sink.with_compression(compression))
    }

    /// Caps the number of rows per row group.
    pub fn with_row_group_size(self, rows: usize) -> Self {
        self.map_sink(|sink| sink.with_row_group_size(rows))
    }

    /// Caps the encoded size of a row group.
    pub fn with_row_group_bytes(self, bytes: usize) -> Self {
        self.map_sink(|sink| sink.with_row_group_bytes(bytes))
    }

    /// Uses fully custom Parquet writer properties.
    pub fn with_writer_properties(self, properties: WriterProperties) -> Self {
        self.map_sink(|sink| sink.with_writer_properties(properties))
    }

    /// Sets how many elements are buffered before being encoded (default 1024).
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        self.map_sink(|sink| sink.with_batch_size(batch_size))
    }

    /// The sink every file is written with.
    pub fn sink(&self) -> &ParquetSink<T> {
        &self.sink
    }

    /// The [`WriteFiles`] transform this expands to.
    pub fn write_files(&self) -> WriteFiles<T> {
        (self.configure)(WriteFiles::new(
            self.name.clone(),
            self.prefix.clone(),
            self.sink.clone(),
        ))
    }
}

impl<T: DefaultCoder> PTransform<PCollection<T>> for Write<T> {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<String> {
        self.write_files().expand(input)
    }
}
