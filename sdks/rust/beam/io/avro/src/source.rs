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

//! Splittable reads of Avro object container files.
//!
//! An object container file is a header followed by data blocks, each ending in
//! the file's 16-byte sync marker:
//!
//! ```text
//! header (magic, metadata, sync) | count size data sync | count size data sync | ...
//! ```
//!
//! A **block belongs to the split that contains its first byte**. A split that starts inside
//! the file begins at the block after the first sync marker at or after `start - 16`. Each
//! block offset is claimed before the block is read, so no block is skipped or duplicated
//! and dynamic splitting works per block. Claimed blocks are re-framed behind the file
//! header and decoded by arrow-avro, a few megabytes at a time.

use std::io::{BufRead, BufReader, Cursor, Read as _};
use std::sync::Arc;

use arrow_avro::reader::{ReaderBuilder, read_header_info};
use arrow_io::{BeamRowCodec, RowCodec, SchemaRowCodec};
use beam::coders::DefaultCoder;
use beam::schema::{BeamRow, Row, Schema};
use beam::transforms::PTransform;
use beam::transforms::ProcessContext;
use beam::transforms::sdf::OffsetRangeTracker;
use beam::values::{PBegin, PCollection};
use file::filebasedsource::{
    DEFAULT_SPLIT_SIZE, FileBasedSource, FileRecordReader, ReadAllViaFileBasedSource,
};
use file::filesystem::{FileSystem, get_filesystem};

use crate::error::AvroIoError;

/// Default number of rows decoded per Arrow batch while reading.
pub const DEFAULT_READ_BATCH_SIZE: usize = 1024;

/// Length of an Avro sync marker.
const SYNC_LEN: usize = 16;

/// Claimed block bytes are decoded once this many have accumulated.
const DECODE_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Scan buffer size used while looking for a sync marker.
const SCAN_CHUNK_BYTES: usize = 64 * 1024;

/// Reads the Beam schema of an Avro container file from its header.
pub fn schema_of(path: &str) -> Result<Schema, AvroIoError> {
    let fs = get_filesystem(path)?;
    let reader = ReaderBuilder::new().build(BufReader::new(fs.open_read(path)?))?;
    Ok(arrow_io::arrow_to_beam_schema(&reader.schema())?)
}

/// The parsed header of a container file, plus its raw bytes for re-framing.
struct Header {
    bytes: Vec<u8>,
    sync: [u8; SYNC_LEN],
}

fn read_header(fs: &dyn FileSystem, file: &str) -> beam::Result<Header> {
    let open = |start, len| {
        fs.open_read_range(file, start, len)
            .map_err(|e| beam::Error::from(e).context(format!("Failed to open Avro file '{file}'")))
    };
    let info = read_header_info(BufReader::new(open(0, 0)?)).map_err(|e| {
        beam::Error::from(e).context(format!("Failed to read Avro header of '{file}'"))
    })?;
    let len = usize::try_from(info.header_len())
        .map_err(|_| format!("Avro header of '{file}' is too large"))?;
    let mut bytes = Vec::with_capacity(len);
    open(0, info.header_len())?
        .read_to_end(&mut bytes)
        .map_err(|e| {
            beam::Error::from(e).context(format!("Failed to read Avro header of '{file}'"))
        })?;
    if bytes.len() != len {
        return Err(format!("Truncated Avro header in '{file}'").into());
    }
    Ok(Header {
        bytes,
        sync: info.sync(),
    })
}

/// Offset of the first block at or after `start`: just past the first sync marker at or
/// after `start - 16`. `None` if no marker follows.
fn first_block_at_or_after(
    fs: &dyn FileSystem,
    file: &str,
    start: u64,
    sync: &[u8; SYNC_LEN],
) -> beam::Result<Option<u64>> {
    let scan_from = start.saturating_sub(SYNC_LEN as u64);
    let mut reader = fs
        .open_read_range(file, scan_from, 0)
        .map_err(|e| beam::Error::from(e).context(format!("Failed to open Avro file '{file}'")))?;
    // `window` holds up to 15 carried-over bytes followed by the newest chunk;
    // `window_start` is the file offset of `window[0]`.
    let mut window: Vec<u8> = Vec::with_capacity(SCAN_CHUNK_BYTES + SYNC_LEN);
    let mut window_start = scan_from;
    let mut chunk = vec![0u8; SCAN_CHUNK_BYTES];
    loop {
        let n = reader.read(&mut chunk).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to scan Avro file '{file}'"))
        })?;
        if n == 0 {
            return Ok(None);
        }
        window.extend_from_slice(&chunk[..n]);
        if let Some(pos) = window.windows(SYNC_LEN).position(|w| w == sync) {
            return Ok(Some(window_start + (pos + SYNC_LEN) as u64));
        }
        let keep = window.len().min(SYNC_LEN - 1);
        let drop = window.len() - keep;
        window.drain(..drop);
        window_start += drop as u64;
    }
}

/// Reads one Avro `long` (zig-zag varint) and appends its raw bytes to `raw`; `None` at EOF.
fn read_long(reader: &mut impl BufRead, raw: &mut Vec<u8>) -> beam::Result<Option<i64>> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        let n = reader
            .read(&mut byte)
            .map_err(|e| beam::Error::from(e).context("Failed to read Avro block header"))?;
        if n == 0 {
            return if shift == 0 {
                Ok(None)
            } else {
                Err("Truncated varint in Avro block header".into())
            };
        }
        raw.push(byte[0]);
        value |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            let decoded = ((value >> 1) as i64) ^ -((value & 1) as i64);
            return Ok(Some(decoded));
        }
        shift += 7;
        if shift > 63 {
            return Err("Overlong varint in Avro block header".into());
        }
    }
}

/// Appends the next block (header, data and sync) to `out` and returns its length; `None` at EOF.
fn read_block(
    reader: &mut impl BufRead,
    sync: &[u8; SYNC_LEN],
    out: &mut Vec<u8>,
) -> beam::Result<Option<u64>> {
    let begin = out.len();
    let Some(_count) = read_long(reader, out)? else {
        return Ok(None);
    };
    let size = read_long(reader, out)?.ok_or("Truncated Avro block header")?;
    let size = usize::try_from(size).map_err(|_| format!("Invalid Avro block size {size}"))?;
    let data_start = out.len();
    out.resize(data_start + size + SYNC_LEN, 0);
    reader
        .read_exact(&mut out[data_start..])
        .map_err(|e| beam::Error::from(e).context("Truncated Avro block"))?;
    if &out[out.len() - SYNC_LEN..] != sync {
        return Err("Avro block sync marker does not match the file header".into());
    }
    Ok(Some((out.len() - begin) as u64))
}

/// [`FileRecordReader`] decoding Avro container file blocks into elements of type `T`.
pub struct AvroRecordReader<T> {
    codec: Arc<dyn RowCodec<T>>,
    batch_size: usize,
}

impl<T> Clone for AvroRecordReader<T> {
    fn clone(&self) -> Self {
        Self {
            codec: Arc::clone(&self.codec),
            batch_size: self.batch_size,
        }
    }
}

impl<T: 'static> AvroRecordReader<T> {
    pub fn new(codec: Arc<dyn RowCodec<T>>, batch_size: usize) -> Self {
        Self {
            codec,
            batch_size: batch_size.max(1),
        }
    }

    /// Decodes `blocks` (complete, consecutive blocks) and emits their elements.
    fn decode(
        &self,
        file: &str,
        header: &Header,
        blocks: &mut Vec<u8>,
        emit: &mut impl FnMut(T) -> beam::Result,
    ) -> beam::Result {
        if blocks.is_empty() {
            return Ok(());
        }
        let mut framed = Vec::with_capacity(header.bytes.len() + blocks.len());
        framed.extend_from_slice(&header.bytes);
        framed.append(blocks);
        let reader = ReaderBuilder::new()
            .with_batch_size(self.batch_size)
            .build(Cursor::new(framed))
            .map_err(|e| {
                beam::Error::from(e).context(format!("Failed to decode Avro file '{file}'"))
            })?;
        for batch in reader {
            let batch = batch.map_err(|e| {
                beam::Error::from(e).context(format!("Failed to decode Avro file '{file}'"))
            })?;
            let elements = self.codec.decode_batch(&batch).map_err(|e| {
                beam::Error::from(e).context(format!("Failed to convert rows of '{file}'"))
            })?;
            elements.into_iter().try_for_each(&mut *emit)?;
        }
        Ok(())
    }

    /// Decodes the blocks of `file` whose start offsets `tracker` lets us claim.
    pub fn read_with_tracker(
        &self,
        fs: &dyn FileSystem,
        file: &str,
        tracker: &OffsetRangeTracker,
        mut emit: impl FnMut(T) -> beam::Result,
    ) -> beam::Result {
        let restriction = tracker.current_restriction();
        if restriction.is_empty() {
            return Ok(());
        }
        let header = read_header(fs, file)?;
        let header_len = header.bytes.len() as u64;
        let start = u64::try_from(restriction.start.max(0)).unwrap_or(0);
        let first = if start <= header_len {
            Some(header_len)
        } else {
            first_block_at_or_after(fs, file, start, &header.sync)?
        };
        let Some(mut position) = first else {
            // No block starts in or after this range.
            tracker.try_claim(&restriction.end);
            return Ok(());
        };

        let mut reader = BufReader::new(fs.open_read_range(file, position, 0).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to open Avro file '{file}'"))
        })?);
        let mut blocks = Vec::new();
        loop {
            let claim = i64::try_from(position).unwrap_or(i64::MAX);
            if !tracker.try_claim(&claim) {
                break;
            }
            match read_block(&mut reader, &header.sync, &mut blocks)
                .map_err(|e| format!("{e} (in '{file}' at offset {position})"))?
            {
                Some(len) => position += len,
                None => {
                    // End of file inside the range: nothing left to claim.
                    tracker.try_claim(&tracker.current_restriction().end);
                    break;
                }
            }
            if blocks.len() >= DECODE_CHUNK_BYTES {
                self.decode(file, &header, &mut blocks, &mut emit)?;
            }
        }
        self.decode(file, &header, &mut blocks, &mut emit)
    }
}

impl<T: DefaultCoder> FileRecordReader<T> for AvroRecordReader<T> {
    fn read_records(
        &self,
        fs: &dyn FileSystem,
        file: &str,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, T>,
    ) -> beam::Result {
        self.read_with_tracker(fs, file, tracker, |element| ctx.emit(element))
    }
}

/// Settings shared by every read transform.
struct ReadConfig<T> {
    name: String,
    split_size: u64,
    batch_size: usize,
    codec: Arc<dyn RowCodec<T>>,
}

impl<T> Clone for ReadConfig<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            split_size: self.split_size,
            batch_size: self.batch_size,
            codec: Arc::clone(&self.codec),
        }
    }
}

impl<T: 'static> ReadConfig<T> {
    fn new(name: String, codec: Arc<dyn RowCodec<T>>) -> Self {
        Self {
            name,
            split_size: DEFAULT_SPLIT_SIZE,
            batch_size: DEFAULT_READ_BATCH_SIZE,
            codec,
        }
    }

    fn reader(&self) -> AvroRecordReader<T> {
        AvroRecordReader::new(Arc::clone(&self.codec), self.batch_size)
    }
}

macro_rules! read_builders {
    () => {
        /// Sets the byte size of initial splits (default 8 MiB).
        pub fn with_split_size(mut self, split_size: u64) -> Self {
            self.config.split_size = split_size;
            self
        }

        /// Sets how many rows are decoded per Arrow batch (default 1024).
        pub fn with_batch_size(mut self, batch_size: usize) -> Self {
            self.config.batch_size = batch_size.max(1);
            self
        }
    };
}

/// Reads Avro container files that match a pattern into `#[derive(BeamRow)]` values. Fields
/// match by name; Avro `int` is narrowed, with range checks, for `BYTE`/`INT16` fields.
pub struct Read<T> {
    pattern: String,
    config: ReadConfig<T>,
}

impl<T> Clone for Read<T> {
    fn clone(&self) -> Self {
        Self {
            pattern: self.pattern.clone(),
            config: self.config.clone(),
        }
    }
}

impl<T: BeamRow + DefaultCoder> Read<T> {
    /// Reads every file that matches `pattern`, a path or glob on any registered filesystem.
    pub fn new(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::with_row_codec(name, pattern, Arc::new(BeamRowCodec::<T>::new()))
    }
}

impl<T: DefaultCoder> Read<T> {
    /// Reads with a custom element codec.
    fn with_row_codec(
        name: impl Into<String>,
        pattern: impl Into<String>,
        codec: Arc<dyn RowCodec<T>>,
    ) -> Self {
        Self {
            pattern: pattern.into(),
            config: ReadConfig::new(name.into(), codec),
        }
    }

    read_builders!();
}

impl<T: DefaultCoder> PTransform<PBegin> for Read<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PBegin) -> PCollection<T> {
        FileBasedSource::new(
            self.config.name.clone(),
            self.pattern.clone(),
            self.config.reader(),
        )
        .with_split_size(self.config.split_size)
        .expand(input)
    }
}

/// Reads each Avro file named in a `PCollection<String>` into `#[derive(BeamRow)]` values.
pub struct ReadFiles<T> {
    config: ReadConfig<T>,
}

impl<T> Clone for ReadFiles<T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
        }
    }
}

impl<T: BeamRow + DefaultCoder> ReadFiles<T> {
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_row_codec(name, Arc::new(BeamRowCodec::<T>::new()))
    }
}

impl<T: DefaultCoder> ReadFiles<T> {
    /// Reads with a custom element codec.
    fn with_row_codec(name: impl Into<String>, codec: Arc<dyn RowCodec<T>>) -> Self {
        Self {
            config: ReadConfig::new(name.into(), codec),
        }
    }

    read_builders!();
}

impl<T: DefaultCoder> PTransform<PCollection<String>> for ReadFiles<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<T> {
        ReadAllViaFileBasedSource::new(self.config.name.clone(), self.config.reader())
            .with_split_size(self.config.split_size)
            .expand(input)
    }
}

/// Reads Avro files that match a pattern into [`Row`]s of `schema`. [`schema_of`] reads the
/// schema of an existing file.
#[derive(Clone)]
pub struct ReadRows {
    schema: Arc<Schema>,
    inner: Read<Row>,
}

impl ReadRows {
    pub fn new(
        name: impl Into<String>,
        pattern: impl Into<String>,
        schema: impl Into<Arc<Schema>>,
    ) -> Self {
        let schema = schema.into();
        let codec = Arc::new(SchemaRowCodec::new(Arc::clone(&schema)));
        Self {
            schema,
            inner: Read::with_row_codec(name, pattern, codec),
        }
    }

    /// Sets the byte size of initial splits (default 8 MiB).
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.inner = self.inner.with_split_size(split_size);
        self
    }

    /// Sets how many rows are decoded per Arrow batch (default 1024).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.inner = self.inner.with_batch_size(batch_size);
        self
    }
}

impl PTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> PCollection<Row> {
        self.inner.expand(input).with_row_schema(&self.schema)
    }
}

/// Reads each Avro file named in a `PCollection<String>` into schema-aware [`Row`]s.
#[derive(Clone)]
pub struct ReadRowFiles {
    schema: Arc<Schema>,
    inner: ReadFiles<Row>,
}

impl ReadRowFiles {
    pub fn new(name: impl Into<String>, schema: impl Into<Arc<Schema>>) -> Self {
        let schema = schema.into();
        let codec = Arc::new(SchemaRowCodec::new(Arc::clone(&schema)));
        Self {
            schema,
            inner: ReadFiles::with_row_codec(name, codec),
        }
    }

    /// Sets the byte size of initial splits (default 8 MiB).
    pub fn with_split_size(mut self, split_size: u64) -> Self {
        self.inner = self.inner.with_split_size(split_size);
        self
    }

    /// Sets how many rows are decoded per Arrow batch (default 1024).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.inner = self.inner.with_batch_size(batch_size);
        self
    }
}

impl PTransform<PCollection<String>> for ReadRowFiles {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<Row> {
        self.inner.expand(input).with_row_schema(&self.schema)
    }
}
