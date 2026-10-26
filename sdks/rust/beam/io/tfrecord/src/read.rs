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

//! Transforms that read TFRecord files, built on the file-based sources of
//! `apache-beam-io-file`.

use std::io::BufReader;

use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::sdf::{OffsetRangeTracker, RestrictionTracker};
use beam::transforms::{PTransform, ProcessContext};
use beam::values::{PBegin, PCollection};
use file::{Compression, FileBasedSource, FileRecordReader, FileSystem, ReadAllViaFileBasedSource};

use crate::format::RecordReader;

/// A TFRecord file has no markers to find a record from an arbitrary byte, so each file is
/// one split.
const WHOLE_FILE: u64 = u64::MAX;

/// Reads the records of a TFRecord file as [`FileRecordReader`].
///
/// It reads from the start of the file and emits the records that start in the tracked byte
/// range, so a runner can still split a file while it reads it.
#[derive(Clone, Copy, Debug, Default)]
pub struct TFRecordReader;

impl FileRecordReader<Vec<u8>> for TFRecordReader {
    fn read_records(
        &self,
        fs: &dyn FileSystem,
        file: &str,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, Vec<u8>>,
    ) -> beam::Result {
        let range = tracker.current_restriction();
        let input = fs
            .open_read(file)
            .map_err(|e| beam::Error::msg(format!("Failed to open '{file}': {e}")))?;
        for record in RecordReader::new(BufReader::new(input)) {
            let (offset, data) = record.map_err(|e| beam::Error::msg(format!("{file}: {e}")))?;
            let offset = i64::try_from(offset).unwrap_or(i64::MAX);
            if offset < range.start {
                continue;
            }
            if !tracker.try_claim(&offset) {
                return Ok(());
            }
            ctx.emit(data)?;
        }
        // Claims the end of the range, so the tracker knows that no record is left.
        tracker.try_claim(&range.end);
        Ok(())
    }
}

/// Reads the records of the TFRecord files that match a path or glob pattern, as
/// `Vec<u8>`.
#[derive(Clone)]
pub struct Read {
    inner: FileBasedSource<Vec<u8>, TFRecordReader>,
}

impl Read {
    /// Reads the files that match `pattern`, for example `gs://bucket/data/*.tfrecord`.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            inner: FileBasedSource::new(pattern, TFRecordReader).with_split_size(WHOLE_FILE),
        }
    }

    /// Sets the decompression mode for matched files (defaults to [`Compression::Auto`]).
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.inner = self.inner.with_compression(compression);
        self
    }
}

impl HasDisplayData for Read {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.inner.populate_display_data(builder);
    }
}

impl PTransform<PBegin> for Read {
    type Output = PCollection<Vec<u8>>;

    fn expand(&self, input: &PBegin) -> PCollection<Vec<u8>> {
        self.inner.expand(input)
    }
}

/// Reads the records of each TFRecord file path in an input `PCollection<String>`.
#[derive(Clone)]
pub struct ReadFiles {
    inner: ReadAllViaFileBasedSource<Vec<u8>, TFRecordReader>,
}

impl Default for ReadFiles {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadFiles {
    /// Creates a transform that reads the records of each input path.
    pub fn new() -> Self {
        Self {
            inner: ReadAllViaFileBasedSource::new(TFRecordReader).with_split_size(WHOLE_FILE),
        }
    }

    /// Sets the decompression mode for input files (defaults to [`Compression::Auto`]).
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.inner = self.inner.with_compression(compression);
        self
    }
}

impl HasDisplayData for ReadFiles {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.inner.populate_display_data(builder);
    }
}

impl PTransform<PCollection<String>> for ReadFiles {
    type Output = PCollection<Vec<u8>>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<Vec<u8>> {
        self.inner.expand(input)
    }
}
