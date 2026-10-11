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

//! Random access to Parquet files through the Beam [`FileSystem`] abstraction.

use std::io::Read;
use std::sync::Arc;

use bytes::Bytes;
use file::filesystem::{FileSystem, get_filesystem};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};

/// A Parquet [`ChunkReader`] over any Beam [`FileSystem`].
///
/// Every [`get_bytes`](ChunkReader::get_bytes) call is one
/// [`FileSystem::open_read_range`], so only the footer and the selected column chunks of a
/// large `gs://` object are fetched.
#[derive(Clone, Debug)]
pub struct FileSystemChunkReader {
    fs: Arc<dyn FileSystem>,
    path: String,
    len: u64,
}

impl FileSystemChunkReader {
    /// Wraps `path` on `fs`, whose size is `len` bytes.
    pub fn new(fs: Arc<dyn FileSystem>, path: impl Into<String>, len: u64) -> Self {
        Self {
            fs,
            path: path.into(),
            len,
        }
    }

    /// Resolves the filesystem for `path` and looks up its size.
    pub fn open(path: &str) -> std::io::Result<Self> {
        let fs = get_filesystem(path)?;
        let len = fs.size(path)?;
        Ok(Self::new(fs, path, len))
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

fn external(err: std::io::Error) -> ParquetError {
    ParquetError::External(Box::new(err))
}

impl Length for FileSystemChunkReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for FileSystemChunkReader {
    type T = Box<dyn Read + Send>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        let length = self.len.saturating_sub(start);
        self.fs
            .open_read_range(&self.path, start, length)
            .map_err(external)
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let mut reader = self
            .fs
            .open_read_range(&self.path, start, length as u64)
            .map_err(external)?;
        let mut buffer = Vec::with_capacity(length);
        reader.read_to_end(&mut buffer).map_err(external)?;
        if buffer.len() != length {
            return Err(ParquetError::EOF(format!(
                "expected {length} bytes at offset {start} of '{}', read {}",
                self.path,
                buffer.len()
            )));
        }
        Ok(Bytes::from(buffer))
    }
}
