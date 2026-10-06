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

//! File system abstraction and scheme registry for local disk (`file://`), in-memory
//! storage and object stores such as `gs://`.

use globset::GlobBuilder;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, OnceLock, RwLock};
use thiserror::Error;

/// File system interface that Beam I/O sources and sinks use for every storage system.
pub trait FileSystem: Send + Sync + std::fmt::Debug {
    /// Expands a pattern/glob to a list of matching file paths.
    fn match_files(&self, pattern: &str) -> io::Result<Vec<String>>;

    /// Opens a file for reading.
    fn open_read(&self, path: &str) -> io::Result<Box<dyn io::Read + Send>>;

    /// Opens a byte range [start_offset, start_offset + length) for reading.
    fn open_read_range(
        &self,
        path: &str,
        start_offset: u64,
        length: u64,
    ) -> io::Result<Box<dyn io::Read + Send>> {
        use io::Read;
        let mut reader = self.open_read(path)?;
        if start_offset > 0 {
            io::copy(
                &mut Read::by_ref(&mut reader).take(start_offset),
                &mut io::sink(),
            )?;
        }
        if length == 0 {
            Ok(reader)
        } else {
            Ok(Box::new(reader.take(length)))
        }
    }

    /// Opens a file for writing; truncates or creates it.
    fn open_write(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>>;

    /// Opens a file for appending (creating if missing, preserving existing contents).
    fn open_append(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>>;

    fn size(&self, path: &str) -> io::Result<u64>;

    fn remove(&self, path: &str) -> io::Result<()>;

    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()>;

    fn copy(&self, from: &str, to: &str) -> io::Result<()>;

    fn last_modified(&self, path: &str) -> io::Result<std::time::SystemTime>;

    fn exists(&self, path: &str) -> io::Result<bool> {
        self.size(path).map(|_| true).or_else(|e| match e.kind() {
            io::ErrorKind::NotFound => Ok(false),
            _ => Err(e),
        })
    }

    /// Removes an empty directory. The default does nothing, for object stores.
    fn remove_dir(&self, _path: &str) -> io::Result<()> {
        Ok(())
    }
}

pub fn strip_file_prefix(path: &str) -> &str {
    path.strip_prefix("file://").unwrap_or(path)
}

/// Splits a URI into `(scheme, path)`. Without a scheme, returns `("", path)`.
pub fn parse_scheme(uri: &str) -> (&str, &str) {
    uri.find("://")
        .map_or(("", uri), |idx| (&uri[..idx], &uri[idx + 3..]))
}

/// Error returned when a glob pattern cannot be compiled.
#[derive(Debug, Error)]
#[error("invalid glob pattern '{pattern}'")]
pub struct GlobError {
    pattern: String,
    #[source]
    source: globset::Error,
}

impl From<GlobError> for io::Error {
    fn from(err: GlobError) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, err)
    }
}

/// A compiled glob pattern with `*`, `**`, `?`, `[a-z]` and `{a,b}`. `*` and `?` stay in
/// one path segment; `**` spans segments. Matching is linear in the input length (no
/// backtracking).
///
/// ```
/// use file::filesystem::GlobMatcher;
///
/// let matcher: GlobMatcher = "data/**/*.txt".parse()?;
/// assert!(matcher.is_match("data/2024/jan/events.txt"));
/// assert!(!matcher.is_match("data/2024/jan/events.csv"));
/// # Ok::<(), file::filesystem::GlobError>(())
/// ```
#[derive(Clone, Debug)]
pub struct GlobMatcher {
    inner: globset::GlobMatcher,
}

impl GlobMatcher {
    /// Compiles a glob pattern. Fails on a malformed pattern, such as an unclosed `[`.
    pub fn new(pattern: &str) -> Result<Self, GlobError> {
        GlobBuilder::new(pattern)
            // Confine `*` and `?` to a single path segment; `**` still spans them.
            .literal_separator(true)
            .build()
            .map(|glob| Self {
                inner: glob.compile_matcher(),
            })
            .map_err(|source| GlobError {
                pattern: pattern.to_string(),
                source,
            })
    }

    /// Reports whether `text` matches the whole pattern.
    pub fn is_match(&self, text: &str) -> bool {
        self.inner.is_match(text)
    }
}

impl FromStr for GlobMatcher {
    type Err = GlobError;

    fn from_str(pattern: &str) -> Result<Self, Self::Err> {
        Self::new(pattern)
    }
}

/// Matches a path against a glob pattern. Use [`GlobMatcher`] to reuse a compiled pattern.
pub fn glob_match(pattern: &str, text: &str) -> Result<bool, GlobError> {
    Ok(GlobMatcher::new(pattern)?.is_match(text))
}

/// Creates the parent directory of `path` if it has one that does not yet exist.
fn ensure_parent_dir(path: &str) -> io::Result<()> {
    Path::new(path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or(Ok(()), fs::create_dir_all)
}

/// Local file system through `std::fs`.
#[derive(Clone, Debug, Default)]
pub struct LocalFileSystem;

impl LocalFileSystem {
    pub fn new() -> Self {
        Self
    }
}

impl FileSystem for LocalFileSystem {
    fn match_files(&self, pattern: &str) -> io::Result<Vec<String>> {
        let clean_path = strip_file_prefix(pattern);

        // Fast-path: a literal path either exists or matches nothing.
        let Some(wildcard_pos) = clean_path.find(['*', '?']) else {
            let exists = Path::new(clean_path).exists();
            return Ok(exists.then(|| clean_path.to_string()).into_iter().collect());
        };

        // Search from the deepest directory that contains no wildcard.
        let base_dir_str = clean_path[..wildcard_pos]
            .rfind('/')
            .map_or("", |slash_pos| &clean_path[..slash_pos]);

        let base_path = Path::new(if base_dir_str.is_empty() {
            "."
        } else {
            base_dir_str
        });

        if !base_path.exists() {
            return Ok(Vec::new());
        }

        // The two patterns differ only in a leading `./`, which `read_dir` may produce.
        let clean_pat = clean_path.strip_prefix("./").unwrap_or(clean_path);
        let pattern_matcher = GlobMatcher::new(clean_pat)?;
        let raw_matcher = GlobMatcher::new(clean_path)?;

        let mut matches = Vec::new();
        let mut dirs_to_visit = vec![base_path.to_path_buf()];

        while let Some(current_dir) = dirs_to_visit.pop() {
            let Ok(entries) = fs::read_dir(&current_dir) else {
                continue;
            };

            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    dirs_to_visit.push(path);
                } else if path.is_file() {
                    let path_str = path.to_string_lossy();
                    let normalized = path_str.strip_prefix("./").unwrap_or(&path_str);

                    if pattern_matcher.is_match(normalized) || raw_matcher.is_match(&path_str) {
                        matches.push(normalized.to_string());
                    }
                }
            }
        }

        matches.sort();
        Ok(matches)
    }

    fn open_read(&self, path: &str) -> io::Result<Box<dyn io::Read + Send>> {
        let clean_path = strip_file_prefix(path);
        let file = fs::File::open(clean_path)?;
        Ok(Box::new(file))
    }

    fn open_read_range(
        &self,
        path: &str,
        start_offset: u64,
        length: u64,
    ) -> io::Result<Box<dyn io::Read + Send>> {
        use io::{Read, Seek};
        let clean_path = strip_file_prefix(path);
        let mut file = fs::File::open(clean_path)?;
        if start_offset > 0 {
            file.seek(io::SeekFrom::Start(start_offset))?;
        }
        if length == 0 {
            Ok(Box::new(file))
        } else {
            Ok(Box::new(file.take(length)))
        }
    }

    fn open_write(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        let clean_path = strip_file_prefix(path);
        ensure_parent_dir(clean_path)?;
        let file = fs::File::create(clean_path)?;
        Ok(Box::new(file))
    }

    fn open_append(&self, path: &str) -> io::Result<Box<dyn io::Write + Send>> {
        let clean_path = strip_file_prefix(path);
        ensure_parent_dir(clean_path)?;
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(clean_path)?;
        Ok(Box::new(file))
    }

    fn size(&self, path: &str) -> io::Result<u64> {
        let clean_path = strip_file_prefix(path);
        let metadata = fs::metadata(clean_path)?;
        Ok(metadata.len())
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        let clean_path = strip_file_prefix(path);
        fs::remove_file(clean_path)
    }

    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        let clean_old = strip_file_prefix(old_path);
        let clean_new = strip_file_prefix(new_path);
        ensure_parent_dir(clean_new)?;
        fs::rename(clean_old, clean_new)
    }

    fn copy(&self, from: &str, to: &str) -> io::Result<()> {
        let clean_from = strip_file_prefix(from);
        let clean_to = strip_file_prefix(to);
        ensure_parent_dir(clean_to)?;
        fs::copy(clean_from, clean_to)?;
        Ok(())
    }

    fn last_modified(&self, path: &str) -> io::Result<std::time::SystemTime> {
        let clean_path = strip_file_prefix(path);
        let metadata = fs::metadata(clean_path)?;
        metadata.modified()
    }

    fn exists(&self, path: &str) -> io::Result<bool> {
        let clean_path = strip_file_prefix(path);
        Ok(Path::new(clean_path).exists())
    }

    fn remove_dir(&self, path: &str) -> io::Result<()> {
        match fs::remove_dir(strip_file_prefix(path)) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }
}

/// Reads a whole file as a UTF-8 string.
pub fn read_to_string(path: &str) -> io::Result<String> {
    use std::io::Read;
    let fs = get_filesystem(path)?;
    let mut reader = fs.open_read(path)?;
    let mut s = String::new();
    reader.read_to_string(&mut s)?;
    Ok(s)
}

/// Reads a whole file as bytes.
pub fn read_to_bytes(path: &str) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let fs = get_filesystem(path)?;
    let mut reader = fs.open_read(path)?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Writes bytes to a file.
pub fn write_bytes(path: &str, data: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let fs = get_filesystem(path)?;
    let mut writer = fs.open_write(path)?;
    writer.write_all(data)?;
    writer.flush()?;
    Ok(())
}

/// Checks if a file exists.
pub fn exists(path: &str) -> io::Result<bool> {
    let fs = get_filesystem(path)?;
    fs.exists(path)
}

/// A registration entry for a filesystem scheme discovered at link time.
pub struct FileSystemRegistration {
    pub scheme: &'static str,
    pub factory: fn() -> Arc<dyn FileSystem>,
}

inventory::collect!(FileSystemRegistration);

static REGISTRY: OnceLock<RwLock<HashMap<String, Arc<dyn FileSystem>>>> = OnceLock::new();

fn get_registry() -> &'static RwLock<HashMap<String, Arc<dyn FileSystem>>> {
    REGISTRY.get_or_init(|| {
        let local: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
        let mut map: HashMap<String, Arc<dyn FileSystem>> = HashMap::from([
            (String::new(), Arc::clone(&local)),
            ("file".to_string(), local),
        ]);
        for reg in inventory::iter::<FileSystemRegistration> {
            map.insert(reg.scheme.to_string(), (reg.factory)());
        }
        RwLock::new(map)
    })
}

/// Retrieves the registered [`FileSystem`] implementation for the scheme in `uri_or_path`.
pub fn get_filesystem(uri_or_path: &str) -> io::Result<Arc<dyn FileSystem>> {
    let (scheme, _) = parse_scheme(uri_or_path);
    let reg = get_registry()
        .read()
        .map_err(|_| io::Error::other("FileSystem registry lock poisoned"))?;

    reg.get(scheme).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "No FileSystem registered for scheme '{scheme}' (path: '{uri_or_path}'). \
                If this is a cloud storage path (e.g. gs://), register the corresponding filesystem first."
            ),
        )
    })
}

/// Registers a [`FileSystem`] backend for a given scheme (e.g. `"gs"`, `"s3"`, `"memfs"`).
pub fn register_filesystem(
    scheme: impl Into<String>,
    fs: Arc<dyn FileSystem>,
) -> Result<(), io::Error> {
    let mut reg = get_registry()
        .write()
        .map_err(|_| io::Error::other("FileSystem registry lock poisoned"))?;
    reg.insert(scheme.into(), fs);
    Ok(())
}
