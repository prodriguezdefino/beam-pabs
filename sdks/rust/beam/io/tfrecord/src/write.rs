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

//! The transform that writes TFRecord files, built on `WriteFiles`.

use beam::transforms::PTransform;
use beam::values::PCollection;
use file::{Compression, FileFormat, FilenamePolicy, FormatSink, WriteFiles};

use crate::format::write_record;

/// Writes each element as one TFRecord, as [`FileFormat`].
#[derive(Clone, Copy, Debug, Default)]
pub struct TFRecordFormat;

impl FileFormat<Vec<u8>> for TFRecordFormat {
    fn write_element(&self, element: &Vec<u8>, writer: &mut dyn std::io::Write) -> beam::Result {
        write_record(writer, element)
            .map_err(|e| beam::Error::msg(format!("Failed to write a TFRecord: {e}")))
    }
}

/// Writes each element of a `PCollection<Vec<u8>>` as one TFRecord.
///
/// Built on [`WriteFiles`]; outputs the written file paths. By default the runner chooses
/// the shard count, and the shard goes before the extension: `"/out/data.tfrecord"` produces
/// `/out/data-00000-of-00003.tfrecord`.
#[derive(Clone, Debug)]
pub struct Write {
    inner: WriteFiles<Vec<u8>>,
}

impl Write {
    /// Writes files named after `prefix`, e.g. `/out/data.tfrecord` or `gs://b/out/part`.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            inner: WriteFiles::new(prefix, FormatSink::new(TFRecordFormat)),
        }
    }

    fn map(mut self, f: impl FnOnce(WriteFiles<Vec<u8>>) -> WriteFiles<Vec<u8>>) -> Self {
        self.inner = f(self.inner);
        self
    }

    /// Appends `suffix` (e.g. `.tfrecord`) after the shard in each file name.
    pub fn with_suffix(self, suffix: impl Into<String>) -> Self {
        self.map(|w| w.with_suffix(suffix))
    }

    /// Writes exactly `num_shards` files per window and pane.
    pub fn with_num_shards(self, num_shards: u32) -> Self {
        self.map(|w| w.with_num_shards(num_shards))
    }

    /// Overrides the shard template; see [`file::format_shard_template`].
    pub fn with_shard_template(self, template: impl Into<String>) -> Self {
        self.map(|w| w.with_shard_template(template))
    }

    /// Writes a single file named exactly as given, through a single worker.
    pub fn without_sharding(self) -> Self {
        self.map(WriteFiles::without_sharding)
    }

    /// Rolls to a new file after `max` records.
    pub fn with_max_records_per_file(self, max: u64) -> Self {
        self.map(|w| w.with_max_records_per_file(max))
    }

    /// Rolls to a new file after roughly `max` bytes.
    pub fn with_max_bytes_per_file(self, max: u64) -> Self {
        self.map(|w| w.with_max_bytes_per_file(max))
    }

    /// Writes separate files per window and pane. Required for unbounded input. See
    /// [`WriteFiles::with_windowed_writes`].
    pub fn with_windowed_writes(self) -> Self {
        self.map(WriteFiles::with_windowed_writes)
    }

    /// Configures how final file names are derived.
    pub fn with_filename_policy<P: FilenamePolicy>(self, policy: P) -> Self {
        self.map(|w| w.with_filename_policy(policy))
    }

    /// Places temporary files under `dir`, on the output's filesystem. See
    /// [`WriteFiles::with_temp_directory`].
    pub fn with_temp_directory(self, dir: impl Into<String>) -> Self {
        self.map(|w| w.with_temp_directory(dir))
    }

    /// Compresses each output shard with `compression` and appends its file suffix.
    pub fn with_compression(self, compression: Compression) -> Self {
        self.map(|w| w.with_compression(compression))
    }
}

impl PTransform<PCollection<Vec<u8>>> for Write {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<Vec<u8>>) -> PCollection<String> {
        self.inner.expand(input)
    }
}
