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

use crate::fileio::MatchFilesFn;
use crate::filesystem::{FileSystem, get_filesystem};
use beam::coders::DefaultCoder;
use beam::transforms::ProcessContext;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, SplittableDoFn, SplittableParDo,
};
use beam::transforms::{PTransform, ParDo};
use beam::values::{PBegin, PCollection};
use std::marker::PhantomData;
use std::sync::Arc;

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
    reader: Arc<R>,
    _marker: PhantomData<T>,
}

/// Copies share the stateless reader; each read opens its own file handle.
impl<T, R> Clone for FileBasedSourceFn<T, R> {
    fn clone(&self) -> Self {
        Self {
            split_size: self.split_size,
            reader: Arc::clone(&self.reader),
            _marker: PhantomData,
        }
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> FileBasedSourceFn<T, R> {
    /// Creates a splittable DoFn that reads records from file splits of split_size
    /// bytes.
    pub fn new(split_size: u64, reader: Arc<R>) -> Self {
        Self {
            split_size,
            reader,
            _marker: PhantomData,
        }
    }

    /// Returns the maximum byte size for individual splits.
    pub fn split_size(&self) -> u64 {
        self.split_size
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
        _file: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        file_split_restriction(restriction, self.split_size)
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
        self.reader.read_records(fs.as_ref(), &file, tracker, ctx)?;
        Ok(ProcessContinuation::Stop)
    }
}

/// Generic file-based source reading files matching a pattern from [`PBegin`].
#[derive(Clone)]
pub struct FileBasedSource<T, R> {
    name: String,
    pattern: String,
    split_size: u64,
    reader: Arc<R>,
    _marker: PhantomData<T>,
}

impl<T: DefaultCoder, R: FileRecordReader<T>> FileBasedSource<T, R> {
    /// Reads files that match `pattern` with `reader`.
    pub fn new(name: impl Into<String>, pattern: impl Into<String>, reader: R) -> Self {
        Self {
            name: name.into(),
            pattern: pattern.into(),
            split_size: DEFAULT_SPLIT_SIZE,
            reader: Arc::new(reader),
            _marker: PhantomData,
        }
    }

    /// Overrides the maximum split size in bytes.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.split_size = split_size;
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
        builder.add_text("transform", &self.name);
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> PTransform<PBegin> for FileBasedSource<T, R> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PBegin) -> PCollection<T> {
        crate::composite::expand_root(input, &self.name, self, |pipeline, name| {
            let (_, impulse) = pipeline.add_impulse(&format!("{name}/Impulse"));
            let match_pcoll = impulse.apply(ParDo::new(
                format!("{name}/Match"),
                MatchFilesFn::new(self.pattern.clone()),
            ));
            let read_sdf = FileBasedSourceFn::new(self.split_size, Arc::clone(&self.reader));
            match_pcoll.apply(SplittableParDo::new(format!("{name}/Read"), read_sdf))
        })
    }
}

/// Generic source reading each file in an input [`PCollection<String>`].
#[derive(Clone)]
pub struct ReadAllViaFileBasedSource<T, R> {
    name: String,
    split_size: u64,
    reader: Arc<R>,
    _marker: PhantomData<T>,
}

impl<T: DefaultCoder, R: FileRecordReader<T>> ReadAllViaFileBasedSource<T, R> {
    /// Creates a source transform that reads all matching files.
    pub fn new(name: impl Into<String>, reader: R) -> Self {
        Self {
            name: name.into(),
            split_size: DEFAULT_SPLIT_SIZE,
            reader: Arc::new(reader),
            _marker: PhantomData,
        }
    }

    /// Sets the maximum byte size for individual splits.
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.split_size = split_size;
        self
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> PTransform<PCollection<String>>
    for ReadAllViaFileBasedSource<T, R>
{
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<T> {
        crate::composite::expand(input, &self.name, self, |_pipeline, name| {
            let read_sdf = FileBasedSourceFn::new(self.split_size, Arc::clone(&self.reader));
            input.apply(SplittableParDo::new(format!("{name}/Read"), read_sdf))
        })
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> HasDisplayData for ReadAllViaFileBasedSource<T, R> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_integer("splitSize", self.split_size as i64);
        builder.add_text("transform", &self.name);
    }
}
