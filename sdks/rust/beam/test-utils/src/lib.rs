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

//! Test utilities, mocks, and test doubles for the Apache Beam Rust SDK.

pub mod mock_expansion;
pub mod timeout;

use std::collections::HashMap;
use std::io::{self, Write as _};
use std::sync::{Arc, RwLock};

use file::filesystem::{FileSystem, GlobMatcher};

pub use mock_expansion::{MockExpansionServer, MockExpansionService};
pub use timeout::{
    DEFAULT_TEST_TIMEOUT, TEST_TIMEOUT_ENV, test_timeout, with_custom_timeout, with_timeout,
};

/// Reads and concatenates all output shards that match `prefix`, in shard order (for
/// example `output.txt` matches `output-00000-of-00002.txt`, ...).
pub fn read_shards(prefix: impl AsRef<std::path::Path>) -> String {
    let prefix = prefix.as_ref();
    let dir = prefix.parent().expect("output has a parent directory");
    let file = prefix
        .file_name()
        .expect("output has a file name")
        .to_string_lossy();
    let (stem, ext) = file.rsplit_once('.').unwrap_or((&file, ""));
    let mut shards: Vec<_> = std::fs::read_dir(dir)
        .expect("output directory must exist")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| {
            let name = p.file_name().unwrap_or_default().to_string_lossy();
            name.starts_with(&format!("{stem}-")) && name.ends_with(&format!(".{ext}"))
        })
        .collect();
    shards.sort();
    assert!(!shards.is_empty(), "no output shards for {prefix:?}");
    shards
        .iter()
        .map(|p| std::fs::read_to_string(p).expect("read shard"))
        .collect()
}

/// Thread-safe in-memory file system for tests.
///
/// Files are keyed by the exact path string, including any scheme prefix.
#[derive(Clone, Debug, Default)]
pub struct InMemoryFileSystem {
    files: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl InMemoryFileSystem {
    /// Creates an empty in-memory file system.
    pub fn new() -> Self {
        Self::default()
    }

    fn read_files(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, Vec<u8>>> {
        self.files
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_files(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, Vec<u8>>> {
        self.files
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Inserts or replaces a file in memory.
    pub fn insert_file(&self, path: impl Into<String>, content: impl Into<Vec<u8>>) {
        self.write_files().insert(path.into(), content.into());
    }

    /// Retrieves a copy of the file content if it exists.
    pub fn get_file(&self, path: &str) -> Option<Vec<u8>> {
        self.read_files().get(path).cloned()
    }

    /// Checks if a file exists in memory.
    pub fn contains_file(&self, path: &str) -> bool {
        self.read_files().contains_key(path)
    }

    /// Returns a sorted list of all file paths currently stored.
    pub fn all_paths(&self) -> Vec<String> {
        let mut paths: Vec<String> = self.read_files().keys().cloned().collect();
        paths.sort();
        paths
    }

    /// Clears all files in memory.
    pub fn clear(&self) {
        self.write_files().clear();
    }
}

impl FileSystem for InMemoryFileSystem {
    fn match_files(&self, pattern: &str) -> io::Result<Vec<String>> {
        let matcher = GlobMatcher::new(pattern)?;
        let files = self.read_files();
        let mut matches: Vec<String> = files
            .keys()
            .filter(|path| matcher.is_match(path))
            .cloned()
            .collect();
        matches.sort();
        Ok(matches)
    }

    fn open_read(&self, path: &str) -> io::Result<Box<dyn io::Read + Send>> {
        self.get_file(path)
            .map(|bytes| Box::new(io::Cursor::new(bytes)) as Box<dyn io::Read + Send>)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("file not found: {path}"))
            })
    }

    fn open_read_range(
        &self,
        path: &str,
        start_offset: u64,
        length: u64,
    ) -> io::Result<Box<dyn io::Read + Send>> {
        let bytes = self.get_file(path).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("file not found: {path}"))
        })?;
        let start = usize::try_from(start_offset)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let end = if length == 0 {
            bytes.len()
        } else {
            start
                .saturating_add(usize::try_from(length).unwrap_or(usize::MAX))
                .min(bytes.len())
        };
        Ok(Box::new(io::Cursor::new(bytes[start..end].to_vec())))
    }

    fn open_write(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        Ok(Box::new(MemoryWriter {
            path: path.to_string(),
            files: self.files.clone(),
            buf: Vec::new(),
        }))
    }

    fn open_append(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        let initial_buf = self.get_file(path).unwrap_or_default();
        Ok(Box::new(MemoryWriter {
            path: path.to_string(),
            files: self.files.clone(),
            buf: initial_buf,
        }))
    }

    fn size(&self, path: &str) -> io::Result<u64> {
        self.read_files()
            .get(path)
            .map(|b| b.len() as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("file not found: {path}"))
            })
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        if self.write_files().remove(path).is_some() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {path}"),
            ))
        }
    }

    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        let mut files = self.write_files();
        let bytes = files.remove(old_path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {old_path}"),
            )
        })?;
        files.insert(new_path.to_string(), bytes);
        Ok(())
    }

    fn copy(&self, from: &str, to: &str) -> io::Result<()> {
        let mut files = self.write_files();
        let bytes = files.get(from).cloned().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("file not found: {from}"))
        })?;
        files.insert(to.to_string(), bytes);
        Ok(())
    }

    fn last_modified(&self, path: &str) -> io::Result<std::time::SystemTime> {
        if self.read_files().contains_key(path) {
            Ok(std::time::SystemTime::UNIX_EPOCH)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {path}"),
            ))
        }
    }

    fn exists(&self, path: &str) -> io::Result<bool> {
        Ok(self.contains_file(path))
    }
}

struct MemoryWriter {
    path: String,
    files: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    buf: Vec<u8>,
}

impl io::Write for MemoryWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.files
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(self.path.clone(), self.buf.clone());
        Ok(())
    }
}

impl Drop for MemoryWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
