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

//! Transforms that read newline-delimited text files, built on [`FileBasedSource`] and
//! [`ReadAllViaFileBasedSource`].
use super::split::{TextLineReader, TextLineWithFilenameReader};
use crate::filebasedsource::{FileBasedSource, ReadAllViaFileBasedSource};
use beam::transforms::PTransform;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::values::{PBegin, PCollection};

/// Reads lines from the text files that match a path or glob pattern. Large files are split
/// into byte ranges, so the runner can rebalance work.
#[derive(Clone)]
pub struct Read {
    inner: FileBasedSource<String, TextLineReader>,
}

impl Read {
    /// Reads a file path or glob pattern, with the default split size (8 MB).
    pub fn new(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            inner: FileBasedSource::new(name, pattern, TextLineReader),
        }
    }

    /// Overrides the maximum byte size for individual splits.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.inner = self.inner.with_split_size(split_size);
        self
    }
}

impl HasDisplayData for Read {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.inner.populate_display_data(builder);
    }
}

impl PTransform<PBegin> for Read {
    type Output = PCollection<String>;

    fn expand(&self, input: &PBegin) -> PCollection<String> {
        self.inner.expand(input)
    }
}

/// Reads lines from each file path in an input [`PCollection<String>`].
#[derive(Clone)]
pub struct ReadFiles {
    inner: ReadAllViaFileBasedSource<String, TextLineReader>,
}

impl ReadFiles {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: ReadAllViaFileBasedSource::new(name, TextLineReader),
        }
    }

    /// Overrides the maximum byte size for individual splits.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.inner = self.inner.with_split_size(split_size);
        self
    }
}

impl PTransform<PCollection<String>> for ReadFiles {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<String> {
        self.inner.expand(input)
    }
}

/// Reads text files matching a pattern, pairing each line with its source filename.
#[derive(Clone)]
pub struct ReadWithFilename {
    inner: FileBasedSource<(String, String), TextLineWithFilenameReader>,
}

impl ReadWithFilename {
    /// Reads a file path or glob pattern, tagging lines with their filename.
    pub fn new(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            inner: FileBasedSource::new(name, pattern, TextLineWithFilenameReader),
        }
    }

    /// Overrides the maximum byte size for individual splits.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.inner = self.inner.with_split_size(split_size);
        self
    }
}

impl HasDisplayData for ReadWithFilename {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.inner.populate_display_data(builder);
    }
}

impl PTransform<PBegin> for ReadWithFilename {
    type Output = PCollection<(String, String)>;

    fn expand(&self, input: &PBegin) -> PCollection<(String, String)> {
        self.inner.expand(input)
    }
}

/// Reads lines, paired with their filename, from each file path in a [`PCollection<String>`].
#[derive(Clone)]
pub struct ReadFilesWithFilename {
    inner: ReadAllViaFileBasedSource<(String, String), TextLineWithFilenameReader>,
}

impl ReadFilesWithFilename {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: ReadAllViaFileBasedSource::new(name, TextLineWithFilenameReader),
        }
    }

    /// Overrides the maximum byte size for individual splits.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.inner = self.inner.with_split_size(split_size);
        self
    }
}

impl PTransform<PCollection<String>> for ReadFilesWithFilename {
    type Output = PCollection<(String, String)>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<(String, String)> {
        self.inner.expand(input)
    }
}
