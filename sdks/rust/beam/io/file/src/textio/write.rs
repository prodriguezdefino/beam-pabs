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

//! Sink transform for writing text files.

use crate::compression::Compression;
use crate::sink::{FormatSink, TextFormat};
use crate::write_files::WriteFiles;
use beam::transforms::PTransform;
use beam::values::PCollection;

/// Writes each element of a `PCollection<String>` as one line of text.
///
/// Built on [`WriteFiles`]; outputs the written file paths. By default the runner chooses
/// the shard count, and the shard goes before the extension: `"/out/words.txt"` produces
/// `/out/words-00000-of-00003.txt`.
#[derive(Clone, Debug)]
pub struct Write {
    inner: WriteFiles<String>,
}

impl Write {
    /// Writes files named after `prefix`, e.g. `/out/words.txt` or `gs://b/out/part`.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            inner: WriteFiles::new(prefix, FormatSink::new(TextFormat)),
        }
    }

    crate::forward_write_files_options!(inner);

    /// Compresses each output file with `compression` and appends its extension.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.inner = self.inner.with_compression(compression);
        self
    }
}

impl PTransform<PCollection<String>> for Write {
    type Output = PCollection<String>;

    fn expand(self, input: &PCollection<String>) -> PCollection<String> {
        self.inner.expand(input)
    }
}
