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

//! Shared fixtures: a temporary directory, a file writer and a counting file system.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use beam_io_file::{Compression, FileOutput, FileSink, FileSystem, LocalFileSystem};
use beam_io_parquet::parquetio::ParquetSink;

pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(name: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "beam_parquet_footer_cache_{name}_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub(crate) fn file(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_string()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn write_file<T: 'static>(sink: &ParquetSink<T>, path: &str, items: &[T]) {
    let out = FileOutput::new(
        Box::new(fs::File::create(path).unwrap()),
        Compression::Uncompressed,
    )
    .unwrap();
    let mut writer = sink.open(out).unwrap();
    for item in items {
        writer.write(item).unwrap();
    }
    writer.finish().unwrap().close().unwrap();
}
/// Scheme of [`CountingFileSystem`].
pub(crate) const COUNTING_SCHEME: &str = "countfs";

/// A local file system that records each size lookup and each ranged read, per path.
#[derive(Debug, Default)]
pub(crate) struct CountingFileSystem {
    sizes: Mutex<HashMap<String, usize>>,
    reads: Mutex<HashMap<String, Vec<(u64, u64)>>>,
}

static COUNTING: LazyLock<Arc<CountingFileSystem>> = LazyLock::new(|| {
    let fs = Arc::new(CountingFileSystem::default());
    beam_io_file::register_filesystem(COUNTING_SCHEME, Arc::clone(&fs) as _).unwrap();
    fs
});

impl CountingFileSystem {
    /// The registered instance.
    pub(crate) fn get() -> Arc<Self> {
        Arc::clone(&COUNTING)
    }

    /// The `countfs://` URI of a local `path`. Registers the file system on first use.
    pub(crate) fn uri(path: &str) -> String {
        LazyLock::force(&COUNTING);
        format!("{COUNTING_SCHEME}://{path}")
    }

    /// The number of size lookups of `uri`.
    pub(crate) fn sizes(&self, uri: &str) -> usize {
        self.sizes.lock().unwrap().get(uri).copied().unwrap_or(0)
    }

    /// The `(start, length)` of each ranged read of `uri`.
    pub(crate) fn reads(&self, uri: &str) -> Vec<(u64, u64)> {
        self.reads
            .lock()
            .unwrap()
            .get(uri)
            .cloned()
            .unwrap_or_default()
    }

    fn local(uri: &str) -> &str {
        uri.strip_prefix("countfs://").unwrap_or(uri)
    }
}

impl FileSystem for CountingFileSystem {
    fn match_files(&self, pattern: &str) -> io::Result<Vec<String>> {
        LocalFileSystem::new().match_files(Self::local(pattern))
    }
    fn open_read(&self, path: &str) -> io::Result<Box<dyn io::Read + Send>> {
        self.open_read_range(path, 0, 0)
    }
    fn open_read_range(
        &self,
        path: &str,
        start: u64,
        length: u64,
    ) -> io::Result<Box<dyn io::Read + Send>> {
        self.reads
            .lock()
            .unwrap()
            .entry(path.to_string())
            .or_default()
            .push((start, length));
        LocalFileSystem::new().open_read_range(Self::local(path), start, length)
    }
    fn open_write(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        LocalFileSystem::new().open_write(Self::local(path))
    }
    fn open_append(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        LocalFileSystem::new().open_append(Self::local(path))
    }
    fn size(&self, path: &str) -> io::Result<u64> {
        *self
            .sizes
            .lock()
            .unwrap()
            .entry(path.to_string())
            .or_default() += 1;
        LocalFileSystem::new().size(Self::local(path))
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        LocalFileSystem::new().remove(Self::local(path))
    }
    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        LocalFileSystem::new().rename(Self::local(old_path), Self::local(new_path))
    }
    fn copy(&self, from: &str, to: &str) -> io::Result<()> {
        LocalFileSystem::new().copy(Self::local(from), Self::local(to))
    }
    fn last_modified(&self, path: &str) -> io::Result<std::time::SystemTime> {
        LocalFileSystem::new().last_modified(Self::local(path))
    }
}
