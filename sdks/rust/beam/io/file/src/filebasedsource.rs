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

//! Generic file-based source: matches files with [`Match`](crate::fileio::Match), splits them
//! into byte ranges with a [`SplittableDoFn`] and [`OffsetRangeTracker`], and decodes records
//! with a format-specific [`FileRecordReader`].

use crate::compression::Compression;
use crate::fileio::MatchFilesFn;
use crate::filesystem::{FileSystem, get_filesystem};
use beam::coders::DefaultCoder;
use beam::transforms::ProcessContext;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    SplittableParDo,
};
use beam::transforms::{PTransform, ParDo};
use beam::values::{PBegin, PCollection};
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::SystemTime;

/// Default split size in bytes (8 MB).
pub const DEFAULT_SPLIT_SIZE: u64 = 8 * 1024 * 1024;

/// Reads the records of a format from a byte range of a file.
pub trait FileRecordReader<T: DefaultCoder>: Send + Sync + 'static {
    /// Reads records within the byte range tracked by `tracker` from `file`.
    fn read_records(
        &self,
        fs: &dyn FileSystem,
        file: &str,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, T>,
    ) -> beam::Result;

    /// Whether the reader reads byte ranges of the file (default false). Such a reader
    /// cannot read a compressed file, so the read fails with a byte-range error.
    fn needs_byte_ranges(&self) -> bool {
        false
    }
}

/// Allows closures to act as a [`FileRecordReader`].
impl<T, F> FileRecordReader<T> for F
where
    T: DefaultCoder,
    F: Fn(&dyn FileSystem, &str, &OffsetRangeTracker, &mut ProcessContext<'_, T>) -> beam::Result
        + Send
        + Sync
        + 'static,
{
    fn read_records(
        &self,
        fs: &dyn FileSystem,
        file: &str,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, T>,
    ) -> beam::Result {
        self(fs, file, tracker, ctx)
    }
}

/// Computes the initial offset range restriction `[0, size)` for a file.
///
/// A file that cannot be found or sized is an error, so the bundle fails and the runner
/// retries it. It is not read as empty.
pub fn file_initial_restriction(file: &str) -> beam::Result<OffsetRange> {
    let fs = get_filesystem(file)
        .map_err(|e| beam::Error::msg(format!("Failed to get filesystem for '{file}': {e}")))?;
    let size = fs
        .size(file)
        .map_err(|e| beam::Error::msg(format!("Failed to get size for '{file}': {e}")))?;
    Ok(OffsetRange::new(0, i64::try_from(size).unwrap_or(i64::MAX)))
}

/// Splits an offset range restriction into chunks of `split_size` bytes.
pub fn file_split_restriction(restriction: &OffsetRange, split_size: u64) -> Vec<OffsetRange> {
    let size = if split_size == 0 {
        DEFAULT_SPLIT_SIZE as i64
    } else {
        i64::try_from(split_size).unwrap_or(i64::MAX)
    };
    restriction.sized_splits(size)
}

/// Generic Splittable DoFn that executes byte-range reads with a [`FileRecordReader`].
pub struct FileBasedSourceFn<T, R> {
    split_size: u64,
    compression: Compression,
    reader: Arc<R>,
    _marker: PhantomData<T>,
}

/// Copies share the stateless reader; each read opens its own file handle.
impl<T, R> Clone for FileBasedSourceFn<T, R> {
    fn clone(&self) -> Self {
        Self {
            split_size: self.split_size,
            compression: self.compression,
            reader: Arc::clone(&self.reader),
            _marker: PhantomData,
        }
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> FileBasedSourceFn<T, R> {
    /// Creates a splittable DoFn that reads records from file splits of `split_size`
    /// bytes.
    pub fn new(split_size: u64, reader: Arc<R>) -> Self {
        Self {
            split_size,
            compression: Compression::Auto,
            reader,
            _marker: PhantomData,
        }
    }

    /// Overrides the compression mode. Defaults to [`Compression::Auto`].
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Returns the maximum byte size for individual splits.
    pub fn split_size(&self) -> u64 {
        self.split_size
    }

    /// Returns the configured compression mode.
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// Returns a reference to the record reader.
    pub fn reader(&self) -> &Arc<R> {
        &self.reader
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> SplittableDoFn for FileBasedSourceFn<T, R> {
    type In = String;
    type Out = T;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, file: &Self::In) -> beam::Result<Self::Restriction> {
        file_initial_restriction(file)
    }

    fn split_restriction(
        &self,
        file: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        if self.compression.is_compressed(file) {
            vec![*restriction]
        } else {
            file_split_restriction(restriction, self.split_size)
        }
    }

    fn restriction_size(&self, _file: &Self::In, restriction: &Self::Restriction) -> f64 {
        restriction.size()
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        file: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result<ProcessContinuation> {
        let fs = get_filesystem(&file).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to get filesystem for '{file}'"))
        })?;
        match self.compression.resolve(&file) {
            Compression::Uncompressed => {
                self.reader.read_records(fs.as_ref(), &file, tracker, ctx)?;
            }
            compression @ (Compression::Auto
            | Compression::Gzip
            | Compression::Zstd
            | Compression::Deflate
            | Compression::Snappy) => {
                if self.reader.needs_byte_ranges() {
                    return Err(compression.byte_range_error(&file).into());
                }
                // A compressed file is one stream: only the restriction at offset 0 reads it.
                let range = tracker.current_restriction();
                if range.start == 0 && !range.is_empty() && tracker.try_claim(&(range.end - 1)) {
                    let stream_tracker = OffsetRangeTracker::new(OffsetRange::new(0, i64::MAX));
                    let decompressing = DecompressingFileSystem {
                        inner: fs.as_ref(),
                        compression,
                    };
                    self.reader
                        .read_records(&decompressing, &file, &stream_tracker, ctx)?;
                } else {
                    tracker.try_claim(&range.end);
                }
            }
        }
        Ok(ProcessContinuation::Stop)
    }
}

/// Decompresses the reads of `inner`. Readers take a [`FileSystem`], so this is the seam.
#[derive(Debug)]
struct DecompressingFileSystem<'a> {
    inner: &'a dyn FileSystem,
    compression: Compression,
}

impl FileSystem for DecompressingFileSystem<'_> {
    fn open_read(&self, path: &str) -> io::Result<Box<dyn Read + Send>> {
        self.compression.reader(self.inner.open_read(path)?)
    }

    fn open_read_range(
        &self,
        path: &str,
        start: u64,
        length: u64,
    ) -> io::Result<Box<dyn Read + Send>> {
        match (start, length) {
            (0, 0) => self.open_read(path),
            _ => Err(self.compression.byte_range_error(path)),
        }
    }

    fn match_files(&self, pattern: &str) -> io::Result<Vec<String>> {
        self.inner.match_files(pattern)
    }

    fn open_write(&self, path: &str) -> io::Result<Box<dyn Write + Send>> {
        self.inner.open_write(path)
    }

    fn open_append(&self, path: &str) -> io::Result<Box<dyn Write + Send>> {
        self.inner.open_append(path)
    }

    fn size(&self, path: &str) -> io::Result<u64> {
        self.inner.size(path)
    }

    fn last_modified(&self, path: &str) -> io::Result<SystemTime> {
        self.inner.last_modified(path)
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn remove_dir(&self, path: &str) -> io::Result<()> {
        self.inner.remove_dir(path)
    }

    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        self.inner.rename(old_path, new_path)
    }

    fn copy(&self, from: &str, to: &str) -> io::Result<()> {
        self.inner.copy(from, to)
    }

    fn exists(&self, path: &str) -> io::Result<bool> {
        self.inner.exists(path)
    }
}

/// Generic file-based source reading files matching a pattern from [`PBegin`].
#[derive(Clone)]
pub struct FileBasedSource<T, R> {
    pattern: String,
    split_size: u64,
    compression: Compression,
    reader: Arc<R>,
    _marker: PhantomData<T>,
}

impl<T: DefaultCoder, R: FileRecordReader<T>> FileBasedSource<T, R> {
    /// Reads files that match `pattern` with `reader`.
    pub fn new(pattern: impl Into<String>, reader: R) -> Self {
        Self {
            pattern: pattern.into(),
            split_size: DEFAULT_SPLIT_SIZE,
            compression: Compression::Auto,
            reader: Arc::new(reader),
            _marker: PhantomData,
        }
    }

    /// Overrides the maximum split size in bytes.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.split_size = split_size;
        self
    }

    /// Overrides the compression mode. Defaults to [`Compression::Auto`].
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> HasDisplayData for FileBasedSource<T, R> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("filePattern", &self.pattern);
        builder.add_integer(
            "splitSize",
            i64::try_from(self.split_size).unwrap_or(i64::MAX),
        );
        builder.add_text("stream_compression", self.compression.to_string());
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> PTransform<PBegin> for FileBasedSource<T, R> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PBegin) -> PCollection<T> {
        crate::composite::expand(input.pipeline(), self, None, |pipeline| {
            let impulse = pipeline.apply("Impulse", beam::transforms::Impulse::new());
            let match_pcoll =
                impulse.apply("Match", ParDo::new(MatchFilesFn::new(self.pattern.clone())));
            let read_sdf = FileBasedSourceFn::new(self.split_size, Arc::clone(&self.reader))
                .with_compression(self.compression);
            match_pcoll.apply("Read", SplittableParDo::new(read_sdf))
        })
    }
}

/// Generic source reading each file in an input [`PCollection<String>`].
#[derive(Clone)]
pub struct ReadAllViaFileBasedSource<T, R> {
    split_size: u64,
    compression: Compression,
    reader: Arc<R>,
    _marker: PhantomData<T>,
}

impl<T: DefaultCoder, R: FileRecordReader<T>> ReadAllViaFileBasedSource<T, R> {
    /// Creates a source transform that reads all matching files.
    pub fn new(reader: R) -> Self {
        Self {
            split_size: DEFAULT_SPLIT_SIZE,
            compression: Compression::Auto,
            reader: Arc::new(reader),
            _marker: PhantomData,
        }
    }

    /// Sets the maximum byte size for individual splits.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.split_size = split_size;
        self
    }

    /// Overrides the compression mode. Defaults to [`Compression::Auto`].
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> PTransform<PCollection<String>>
    for ReadAllViaFileBasedSource<T, R>
{
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<T> {
        crate::composite::expand(input.pipeline(), self, None, |_pipeline| {
            let read_sdf = FileBasedSourceFn::new(self.split_size, Arc::clone(&self.reader))
                .with_compression(self.compression);
            input.apply("Read", SplittableParDo::new(read_sdf))
        })
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> HasDisplayData for ReadAllViaFileBasedSource<T, R> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_integer("splitSize", self.split_size as i64);
        builder.add_text("stream_compression", self.compression.to_string());
    }
}
