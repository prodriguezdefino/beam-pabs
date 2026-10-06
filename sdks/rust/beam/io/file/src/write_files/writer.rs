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

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::io::{self, BufWriter, Write as IoWrite};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use beam::coders::DefaultCoder;
use sync_wrapper::SyncWrapper;

use crate::filesystem::get_filesystem;
use crate::sink::{FileSink, FileSinkWriter};

/// A written temporary file: `(temp_path, (shard, sequence))`.
///
/// `shard` is the fixed shard, or `-1` under runner-chosen sharding; `sequence` orders the
/// rolled files of one writer. Both only make the finalization order deterministic.
#[doc(hidden)]
pub type FileResult = (String, (i32, i32));

/// Configuration of the steps that write temporary files. Public only for tests.
#[doc(hidden)]
pub struct WriterConfig<T> {
    pub sink: Arc<dyn FileSink<T>>,
    pub temp_dir: String,
    pub max_records: Option<u64>,
    pub max_bytes: Option<u64>,
}

impl<T: 'static> WriterConfig<T> {
    /// Opens a new, uniquely named temporary file.
    pub(super) fn open(&self, shard: i32, sequence: i32) -> beam::Result<OpenFile<T>> {
        let temp_path = format!("{}/{}", self.temp_dir, unique_id());
        let fs = get_filesystem(&temp_path).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to get filesystem for '{temp_path}'"))
        })?;
        let raw = fs.open_write(&temp_path).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to create temporary file '{temp_path}'"))
        })?;
        let bytes = Arc::new(AtomicU64::new(0));
        let out = CountingWriter {
            inner: BufWriter::new(raw),
            count: Arc::clone(&bytes),
        };
        let writer = self
            .sink
            .open(Box::new(out))
            .map_err(|e| e.context(format!("Failed to open sink on '{temp_path}'")))?;
        Ok(OpenFile {
            writer: SyncWrapper::new(writer),
            temp_path,
            bytes,
            records: 0,
            shard,
            sequence,
        })
    }

    /// Whether `file` has reached a rolling limit.
    pub(super) fn is_full(&self, file: &mut OpenFile<T>) -> bool {
        self.max_records.is_some_and(|max| file.records >= max)
            || self
                .max_bytes
                .is_some_and(|max| file.size_estimate() >= max)
    }
}

/// A temporary file being written. DoFn state must be `Sync`, but sink writers are only
/// `Send`; [`SyncWrapper`] allows access only through `&mut`, so no lock is needed.
pub(super) struct OpenFile<T> {
    writer: SyncWrapper<Box<dyn FileSinkWriter<T>>>,
    temp_path: String,
    bytes: Arc<AtomicU64>,
    records: u64,
    shard: i32,
    sequence: i32,
}

impl<T> OpenFile<T> {
    pub(super) fn write(&mut self, element: &T) -> beam::Result {
        self.writer
            .get_mut()
            .write(element)
            .map_err(|e| format!("Failed to write to '{}': {e}", self.temp_path))?;
        self.records += 1;
        Ok(())
    }

    fn size_estimate(&mut self) -> u64 {
        self.bytes.load(Ordering::Relaxed) + self.writer.get_mut().buffered_bytes()
    }

    /// Closes the file and describes it for finalization.
    pub(super) fn finish(self) -> beam::Result<Vec<u8>> {
        let Self {
            writer,
            temp_path,
            shard,
            sequence,
            ..
        } = self;
        writer
            .into_inner()
            .finish()
            .map_err(|e| e.context(format!("Failed to close '{temp_path}'")))?;
        encode_result((temp_path, (shard, sequence)))
    }
}

/// Counts bytes on their way to the underlying file, for size-based rolling.
struct CountingWriter<W> {
    inner: W,
    count: Arc<AtomicU64>,
}

impl<W: IoWrite> IoWrite for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.count.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn encode_result(result: FileResult) -> beam::Result<Vec<u8>> {
    result
        .encode()
        .map_err(|e| beam::Error::from(e).context("Failed to encode file result"))
}

pub(super) fn decode_result(bytes: &[u8]) -> beam::Result<FileResult> {
    FileResult::decode(bytes)
        .map_err(|e| beam::Error::from(e).context("Failed to decode file result"))
}

/// A 128-bit random hex id from `RandomState` (OS-seeded, varies per call), the clock and
/// the process id. Enough for temporary file names without a `rand` dependency.
pub(super) fn unique_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut halves = [0u64; 2];
    for (salt, half) in (0u8..).zip(halves.iter_mut()) {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(nanos);
        hasher.write_u32(std::process::id());
        hasher.write_u8(salt);
        *half = hasher.finish();
    }
    format!("{:016x}{:016x}", halves[0], halves[1])
}
