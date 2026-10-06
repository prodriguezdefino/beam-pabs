/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Transforms for matching and processing files across filesystems.
//!
//! [`Match`] emits one [`FileMetadata`] per file, so runners can spread the work across
//! workers. [`ReadMatches`] turns each into a [`ReadableFile`], for formats read whole.
//!
//! ```no_run
//! use file::fileio::{self, ReadableFile};
//! use beam::pipeline::Pipeline;
//! use beam::transforms::Map;
//!
//! let p = Pipeline::new();
//! p.apply("FileIO.MatchMetadata", fileio::Match::new("/data/*.json"))
//!     .apply("FileIO.ReadMatches", fileio::ReadMatches::new())
//!     .apply("Sizes", Map::new(|file: ReadableFile| {
//!         format!("{}: {} bytes", file.path(), file.size_bytes())
//!     }));
//! ```

use std::io::{self, Read};
use std::time::UNIX_EPOCH;

use super::compression::Compression;
use super::filesystem::{get_filesystem, has_wildcard};
use beam::schema::BeamRow;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::{DoFn, PTransform, ParDo, ProcessContext};
use beam::values::{PBegin, PCollection};

/// Policy applied when a filepattern matches no files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EmptyMatchTreatment {
    /// An empty match is not an error.
    Allow,
    /// An empty match fails the pipeline.
    #[default]
    Disallow,
    /// An empty match is an error only if the pattern has no wildcard.
    AllowIfWildcard,
}

impl EmptyMatchTreatment {
    fn check(self, pattern: &str, matched: usize) -> beam::Result {
        let allowed = match self {
            Self::Allow => true,
            Self::Disallow => false,
            Self::AllowIfWildcard => has_wildcard(pattern),
        };
        if matched == 0 && !allowed {
            Err(format!("No files matched pattern '{pattern}'").into())
        } else {
            Ok(())
        }
    }
}

/// Metadata about a matched file. A schema'd row, so it can cross language boundaries.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
#[beam(crate = "::beam")]
pub struct FileMetadata {
    /// Full path, including any scheme such as `gs://`.
    pub path: String,
    /// Size of the file in bytes.
    pub size_bytes: i64,
    /// Last modification time in milliseconds since the epoch, or 0 if unknown.
    pub last_modified_millis: i64,
}

impl FileMetadata {
    /// Looks up the metadata of the file at `path`.
    pub fn of(path: &str) -> io::Result<Self> {
        let fs = get_filesystem(path)?;
        let size = fs.size(path)?;
        let last_modified_millis = fs
            .last_modified(path)
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
        Ok(Self {
            path: path.to_string(),
            size_bytes: i64::try_from(size).unwrap_or(i64::MAX),
            last_modified_millis,
        })
    }
}

/// A matched file from [`ReadMatches`]. It is opened only by a read method, so the element
/// is cheap to shuffle.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
#[beam(crate = "::beam")]
pub struct ReadableFile {
    /// Metadata of the matched file.
    pub metadata: FileMetadata,
    /// Compression of the file content, resolved from [`Compression::Auto`] when matched.
    pub compression: Compression,
}

impl ReadableFile {
    /// Creates a readable file from its metadata, detecting compression from the path.
    pub fn new(metadata: FileMetadata) -> Self {
        let compression = Compression::detect(&metadata.path);
        Self {
            metadata,
            compression,
        }
    }

    /// Sets the compression mode, resolving [`Compression::Auto`] from the file path.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression.resolve(&self.metadata.path);
        self
    }

    /// Returns the file path.
    pub fn path(&self) -> &str {
        &self.metadata.path
    }

    /// Size of the file in bytes, as recorded when it was matched.
    pub fn size_bytes(&self) -> i64 {
        self.metadata.size_bytes
    }

    /// Opens the file for streaming reads, decompressing when configured.
    pub fn open(&self) -> io::Result<Box<dyn Read + Send>> {
        let raw = get_filesystem(self.path())?.open_read(self.path())?;
        self.compression.resolve(self.path()).reader(raw)
    }

    /// Opens `length` bytes starting at `offset` on an uncompressed file; a `length` of 0
    /// reads to the end. Fails with [`io::ErrorKind::Unsupported`] on compressed files.
    pub fn open_range(&self, offset: u64, length: u64) -> io::Result<Box<dyn Read + Send>> {
        match self.compression.resolve(self.path()) {
            Compression::Uncompressed => {
                get_filesystem(self.path())?.open_read_range(self.path(), offset, length)
            }
            codec @ (Compression::Auto
            | Compression::Gzip
            | Compression::Zstd
            | Compression::Deflate
            | Compression::Snappy) => Err(codec.byte_range_error(self.path())),
        }
    }

    /// Reads the whole file into memory, decompressing when configured.
    pub fn read_fully_as_bytes(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(usize::try_from(self.size_bytes()).unwrap_or(0));
        self.open()?.read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Reads the whole file into memory as UTF-8 text.
    pub fn read_fully_as_utf8_string(&self) -> io::Result<String> {
        String::from_utf8(self.read_fully_as_bytes()?)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[derive(Clone)]
pub(crate) struct MatchFilesFn {
    pattern: String,
    empty_match: EmptyMatchTreatment,
}

impl MatchFilesFn {
    pub(crate) fn new(pattern: String) -> Self {
        Self {
            pattern,
            empty_match: EmptyMatchTreatment::Disallow,
        }
    }

    fn with_empty_match(mut self, treatment: EmptyMatchTreatment) -> Self {
        self.empty_match = treatment;
        self
    }
}

impl DoFn for MatchFilesFn {
    type In = Vec<u8>;
    type Out = String;

    fn process_element(
        &mut self,
        _impulse: Vec<u8>,
        out: &mut ProcessContext<String>,
    ) -> beam::Result {
        tracing::info!("FileIO.Match: matching pattern '{}'", self.pattern);
        let fs = get_filesystem(&self.pattern)
            .map_err(|e| format!("Failed to get filesystem for '{}': {e}", self.pattern))?;

        let matched = fs
            .match_files(&self.pattern)
            .map_err(|e| format!("Failed to match files for pattern '{}': {e}", self.pattern))?;

        tracing::info!(
            "FileIO.Match: matched {} files for pattern '{}'",
            matched.len(),
            self.pattern
        );

        self.empty_match.check(&self.pattern, matched.len())?;
        matched.into_iter().try_for_each(|path| out.emit(path))
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("filePattern", &self.pattern);
    }
}

/// Emits the [`FileMetadata`] of every file that matches a filepattern, reshuffled so
/// downstream reads spread across workers.
#[derive(Clone, Debug)]
pub struct Match {
    pattern: String,
    empty_match: EmptyMatchTreatment,
}

impl Match {
    /// Matches `pattern`, for example `gs://bucket/logs/*.json`.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            empty_match: EmptyMatchTreatment::Disallow,
        }
    }

    /// Sets what happens when nothing matches. Defaults to [`EmptyMatchTreatment::Disallow`].
    pub fn with_empty_match_treatment(mut self, treatment: EmptyMatchTreatment) -> Self {
        self.empty_match = treatment;
        self
    }
}

impl HasDisplayData for Match {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("filePattern", &self.pattern);
        builder.add_text("emptyMatchTreatment", format!("{:?}", self.empty_match));
    }
}

impl PTransform<PBegin> for Match {
    type Output = PCollection<FileMetadata>;

    fn expand(&self, input: &PBegin) -> PCollection<FileMetadata> {
        crate::composite::expand(input.pipeline(), self, None, |pipeline| {
            let impulse = pipeline.apply("Impulse", beam::transforms::Impulse::new());
            impulse
                .apply(
                    "Match",
                    ParDo::new(
                        MatchFilesFn::new(self.pattern.clone()).with_empty_match(self.empty_match),
                    ),
                )
                .apply("Reshuffle", beam::transforms::Reshuffle::new())
                .apply("Metadata", ParDo::new(MetadataFn))
        })
    }
}

/// Looks up the metadata of each path.
#[derive(Clone)]
struct MetadataFn;

impl DoFn for MetadataFn {
    type In = String;
    type Out = FileMetadata;

    fn process_element(
        &mut self,
        path: String,
        out: &mut ProcessContext<FileMetadata>,
    ) -> beam::Result {
        let metadata = FileMetadata::of(&path)
            .map_err(|e| beam::Error::from(e).context(format!("Failed to stat '{path}'")))?;
        out.emit(metadata)
    }
}

/// Turns the [`FileMetadata`] from [`Match`], or plain paths, into [`ReadableFile`]s.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadMatches {
    compression: Compression,
}

impl ReadMatches {
    /// Creates a transform that converts metadata or paths into readable files.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overrides the compression mode. Defaults to [`Compression::Auto`].
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }
}

impl HasDisplayData for ReadMatches {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("stream_compression", self.compression.to_string());
    }
}

impl PTransform<PCollection<FileMetadata>> for ReadMatches {
    type Output = PCollection<ReadableFile>;

    fn expand(&self, input: &PCollection<FileMetadata>) -> PCollection<ReadableFile> {
        let compression = self.compression;
        crate::composite::expand(input.pipeline(), self, None, |_pipeline| {
            input.apply(
                "ToReadableFile",
                beam::transforms::Map::new(move |metadata| {
                    ReadableFile::new(metadata).with_compression(compression)
                }),
            )
        })
    }
}

impl PTransform<PCollection<String>> for ReadMatches {
    type Output = PCollection<ReadableFile>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<ReadableFile> {
        let compression = self.compression;
        crate::composite::expand(input.pipeline(), self, None, |_pipeline| {
            input.apply("Metadata", ParDo::new(MetadataFn)).apply(
                "ToReadableFile",
                beam::transforms::Map::new(move |metadata| {
                    ReadableFile::new(metadata).with_compression(compression)
                }),
            )
        })
    }
}
