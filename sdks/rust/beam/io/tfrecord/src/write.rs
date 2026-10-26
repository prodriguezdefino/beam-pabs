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
use beam_io_file::{Compression, FileFormat, FormatSink, WriteFiles};

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

    beam_io_file::forward_write_files_options!(inner);

    /// Compresses each output shard with `compression` and appends its file suffix.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.inner = self.inner.with_compression(compression);
        self
    }
}

impl PTransform<PCollection<Vec<u8>>> for Write {
    type Output = PCollection<String>;

    fn expand(self, input: &PCollection<Vec<u8>>) -> PCollection<String> {
        self.inner.expand(input)
    }
}
