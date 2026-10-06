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

//! Text record readers and newline alignment. Generic splitting is in
//! [`crate::filebasedsource`].

use crate::filesystem::FileSystem;
use beam::transforms::ProcessContext;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, ProcessContinuation, SplittableDoFn};
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

pub use crate::filebasedsource::{DEFAULT_SPLIT_SIZE, FileBasedSourceFn, FileRecordReader};

/// Reads newline-delimited text records using a byte-range tracker.
#[derive(Clone, Copy, Debug, Default)]
pub struct TextLineReader;

impl FileRecordReader<String> for TextLineReader {
    fn read_records(
        &self,
        fs: &dyn FileSystem,
        file: &str,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, String>,
    ) -> beam::Result {
        read_file_lines_with_tracker(fs, file, tracker, |line| ctx.emit(line))
    }
}

/// Reads newline-delimited text records paired with filename using a byte-range tracker.
#[derive(Clone, Copy, Debug, Default)]
pub struct TextLineWithFilenameReader;

impl FileRecordReader<(String, String)> for TextLineWithFilenameReader {
    fn read_records(
        &self,
        fs: &dyn FileSystem,
        file: &str,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, (String, String)>,
    ) -> beam::Result {
        let filename = file.to_string();
        read_file_lines_with_tracker(fs, file, tracker, |line| ctx.emit((filename.clone(), line)))
    }
}

/// Default buffer size for text file reads (4 MiB).
pub const TEXTIO_BUFFER_SIZE: usize = 4 * 1024 * 1024;

/// An iterator-like reader yielding lines with their starting byte offsets.
pub struct OffsetLines<R> {
    reader: R,
    current_offset: u64,
    buf: Vec<u8>,
}

impl<R: BufRead> OffsetLines<R> {
    pub fn new(reader: R, start_offset: u64) -> Self {
        Self {
            reader,
            current_offset: start_offset,
            buf: Vec::with_capacity(1024),
        }
    }

    /// Read the next line, returning the line's starting offset and line content.
    pub fn next_line(&mut self) -> beam::Result<Option<(i64, String)>> {
        let line_offset = self.current_offset as i64;
        self.buf.clear();

        let mut found_delimiter = false;
        let mut consumed_any = false;

        loop {
            let available = self
                .reader
                .fill_buf()
                .map_err(|e| beam::Error::from(e).context("Failed to read line"))?;
            if available.is_empty() {
                break;
            }
            consumed_any = true;

            let delim = available
                .iter()
                .copied()
                .enumerate()
                .find(|&(_, b)| b == b'\r' || b == b'\n');

            if let Some((idx, b)) = delim {
                self.buf.extend_from_slice(&available[..idx]);
                self.reader.consume(idx + 1);
                self.current_offset += (idx + 1) as u64;
                found_delimiter = true;

                if b == b'\r' {
                    let next = self
                        .reader
                        .fill_buf()
                        .map_err(|e| beam::Error::from(e).context("Failed to read line"))?;
                    if next.first() == Some(&b'\n') {
                        self.reader.consume(1);
                        self.current_offset += 1;
                    }
                }
                break;
            } else {
                let len = available.len();
                self.buf.extend_from_slice(available);
                self.reader.consume(len);
                self.current_offset += len as u64;
            }
        }

        if (!consumed_any || !found_delimiter) && self.buf.is_empty() {
            return Ok(None);
        }

        std::str::from_utf8(&self.buf)
            .map_err(|e| beam::Error::from(e).context("UTF-8 decode error"))
            .map(|line| Some((line_offset, line.to_string())))
    }
}

fn skip_to_next_record(reader: &mut BufReader<Box<dyn Read + Send>>) -> beam::Result<u64> {
    let mut skipped = 0;
    loop {
        let available = reader
            .fill_buf()
            .map_err(|e| beam::Error::from(e).context("Failed to skip partial line"))?;
        if available.is_empty() {
            break;
        }
        let delim = available
            .iter()
            .copied()
            .enumerate()
            .find(|&(_, b)| b == b'\r' || b == b'\n');

        if let Some((idx, b)) = delim {
            reader.consume(idx + 1);
            skipped += (idx + 1) as u64;
            if b == b'\r' {
                let next = reader
                    .fill_buf()
                    .map_err(|e| beam::Error::from(e).context("Failed to skip partial line"))?;
                if next.first() == Some(&b'\n') {
                    reader.consume(1);
                    skipped += 1;
                }
            }
            break;
        } else {
            let len = available.len();
            reader.consume(len);
            skipped += len as u64;
        }
    }
    Ok(skipped)
}

fn open_offset_reader(
    fs: &dyn FileSystem,
    file: &str,
    start: u64,
) -> beam::Result<OffsetLines<BufReader<Box<dyn Read + Send>>>> {
    let open_offset = if start > 0 {
        start.saturating_sub(1)
    } else {
        0
    };
    let mut reader = BufReader::with_capacity(
        TEXTIO_BUFFER_SIZE,
        fs.open_read_range(file, open_offset, 0).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to open range for '{file}'"))
        })?,
    );

    let mut current_offset = open_offset;
    if start > 0 {
        let mut first_byte = [0u8; 1];
        let n = reader.read(&mut first_byte).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to read prefix byte from '{file}'"))
        })?;
        if n > 0 {
            current_offset += 1;
            if first_byte[0] == b'\r' {
                let next = reader.fill_buf().map_err(|e| {
                    beam::Error::from(e).context(format!("Failed to read '{file}'"))
                })?;
                if !next.is_empty() && next[0] == b'\n' {
                    reader.consume(1);
                    current_offset += 1;
                }
            } else if first_byte[0] != b'\n' {
                let bytes_skipped = skip_to_next_record(&mut reader)?;
                current_offset += bytes_skipped;
            }
        }
    }

    Ok(OffsetLines::new(reader, current_offset))
}

pub fn read_file_lines_with_tracker<F>(
    fs: &dyn FileSystem,
    file: &str,
    tracker: &OffsetRangeTracker,
    mut on_line: F,
) -> beam::Result
where
    F: FnMut(String) -> beam::Result,
{
    let rest = tracker.current_restriction();
    if rest.is_empty() {
        return Ok(());
    }

    let mut lines = open_offset_reader(fs, file, rest.start.max(0) as u64)?;
    while let Some((offset, line)) = lines.next_line()? {
        if offset >= rest.end || !tracker.try_claim(&offset) {
            break;
        }
        on_line(line)?;
    }

    if lines.current_offset >= rest.end as u64 {
        tracker.try_claim(&rest.end);
    }

    Ok(())
}

/// Splittable DoFn for reading newline-delimited text lines from file paths.
#[derive(Clone)]
pub struct ReadFileLinesFn {
    inner: FileBasedSourceFn<String, TextLineReader>,
}

impl ReadFileLinesFn {
    pub fn new(split_size: u64) -> Self {
        Self {
            inner: FileBasedSourceFn::new(split_size, Arc::new(TextLineReader)),
        }
    }
}

impl Default for ReadFileLinesFn {
    fn default() -> Self {
        Self::new(DEFAULT_SPLIT_SIZE)
    }
}

impl SplittableDoFn for ReadFileLinesFn {
    type In = String;
    type Out = String;
    type Restriction = OffsetRange;
    type Position = i64;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, file: &Self::In) -> Self::Restriction {
        self.inner.initial_restriction(file)
    }

    fn split_restriction(
        &self,
        file: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        self.inner.split_restriction(file, restriction)
    }

    fn restriction_size(&self, file: &Self::In, restriction: &Self::Restriction) -> f64 {
        self.inner.restriction_size(file, restriction)
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        self.inner.create_tracker(restriction)
    }

    fn process_element(
        &self,
        file: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result<ProcessContinuation> {
        self.inner.process_element(file, tracker, ctx)
    }
}

/// Splittable DoFn for reading lines from file paths, pairing each line with its source filename.
#[derive(Clone)]
pub struct ReadFileLinesWithFilenameFn {
    inner: FileBasedSourceFn<(String, String), TextLineWithFilenameReader>,
}

impl ReadFileLinesWithFilenameFn {
    pub fn new(split_size: u64) -> Self {
        Self {
            inner: FileBasedSourceFn::new(split_size, Arc::new(TextLineWithFilenameReader)),
        }
    }
}

impl Default for ReadFileLinesWithFilenameFn {
    fn default() -> Self {
        Self::new(DEFAULT_SPLIT_SIZE)
    }
}

impl SplittableDoFn for ReadFileLinesWithFilenameFn {
    type In = String;
    type Out = (String, String);
    type Restriction = OffsetRange;
    type Position = i64;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, file: &Self::In) -> Self::Restriction {
        self.inner.initial_restriction(file)
    }

    fn split_restriction(
        &self,
        file: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        self.inner.split_restriction(file, restriction)
    }

    fn restriction_size(&self, file: &Self::In, restriction: &Self::Restriction) -> f64 {
        self.inner.restriction_size(file, restriction)
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        self.inner.create_tracker(restriction)
    }

    fn process_element(
        &self,
        file: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result<ProcessContinuation> {
        self.inner.process_element(file, tracker, ctx)
    }
}
