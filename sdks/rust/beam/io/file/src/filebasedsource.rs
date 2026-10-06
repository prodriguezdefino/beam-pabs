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
use std::collections::HashMap;
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
pub fn file_initial_restriction(file: &str) -> OffsetRange {
    match get_filesystem(file) {
        Ok(fs) => match fs.size(file) {
            Ok(size) => OffsetRange::new(0, size as i64),
            Err(e) => {
                tracing::warn!("Failed to get size for '{file}': {e}");
                OffsetRange::new(0, 0)
            }
        },
        Err(e) => {
            tracing::warn!("Failed to get filesystem for '{file}': {e}");
            OffsetRange::new(0, 0)
        }
    }
}

/// Splits an offset range restriction into chunks of `split_size` bytes.
pub fn file_split_restriction(restriction: &OffsetRange, split_size: u64) -> Vec<OffsetRange> {
    let size = if split_size == 0 {
        DEFAULT_SPLIT_SIZE as i64
    } else {
        split_size as i64
    };
    restriction.sized_splits(size)
}

/// Generic Splittable DoFn executing byte-range reading using a [`FileRecordReader`].
pub struct FileBasedSourceFn<T, R> {
    pub split_size: u64,
    pub reader: Arc<R>,
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
    pub fn new(split_size: u64, reader: Arc<R>) -> Self {
        Self {
            split_size,
            reader,
            _marker: PhantomData,
        }
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> SplittableDoFn for FileBasedSourceFn<T, R> {
    type In = String;
    type Out = T;
    type Restriction = OffsetRange;
    type Position = i64;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, file: &Self::In) -> Self::Restriction {
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
        &self,
        file: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result<ProcessContinuation> {
        let fs = get_filesystem(&file).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to get filesystem for '{file}'"))
        })?;
        self.reader.read_records(fs.as_ref(), &file, tracker, ctx)?;
        Ok(ProcessContinuation::stop())
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
        builder.add_integer("splitSize", self.split_size as i64);
        builder.add_text("transform", &self.name);
    }
}

impl<T: DefaultCoder, R: FileRecordReader<T>> PTransform<PBegin> for FileBasedSource<T, R> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PBegin) -> PCollection<T> {
        let pipeline = input.pipeline();
        let name = pipeline.unique_transform_name(&self.name);
        let (impulse_id, impulse) = pipeline.add_impulse(&format!("{name}/Impulse"));
        let match_pcoll = impulse.apply(ParDo::new(
            format!("{name}/Match"),
            MatchFilesFn::new(self.pattern.clone()),
        ));
        let match_id = pipeline
            .producer_transform_id(match_pcoll.id())
            .expect("Match transform must exist in graph");

        let read_sdf = FileBasedSourceFn::new(self.split_size, Arc::clone(&self.reader));
        let pcoll = match_pcoll.apply(SplittableParDo::new(format!("{name}/Read"), read_sdf));
        let read_id = pipeline
            .producer_transform_id(pcoll.id())
            .expect("Read transform must exist in graph");

        let outputs = HashMap::from([("out".to_string(), pcoll.id().to_string())]);
        let transform_id = pipeline.add_composite_transform(
            &name,
            None,
            Vec::new(),
            HashMap::new(),
            outputs,
            vec![impulse_id, match_id, read_id],
        );

        let mut builder = DisplayDataBuilder::with_namespace(&name);
        self.populate_display_data(&mut builder);
        pipeline.set_transform_display_data(&transform_id, builder.into_proto());

        pcoll
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
    pub fn new(name: impl Into<String>, reader: R) -> Self {
        Self {
            name: name.into(),
            split_size: DEFAULT_SPLIT_SIZE,
            reader: Arc::new(reader),
            _marker: PhantomData,
        }
    }

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
        let pipeline = input.pipeline();
        let name = pipeline.unique_transform_name(&self.name);
        let read_sdf = FileBasedSourceFn::new(self.split_size, Arc::clone(&self.reader));
        input.apply(SplittableParDo::new(format!("{name}/Read"), read_sdf))
    }
}
