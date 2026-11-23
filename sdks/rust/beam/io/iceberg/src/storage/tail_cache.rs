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

//! A worker-level cache of Parquet tail bytes, so row-group discovery and Arrow readers share
//! one footer read for each data file.

use std::ops::Range;
use std::sync::LazyLock;

use bytes::{Bytes, BytesMut};
use iceberg::io::FileRead;
use moka::sync::Cache;

/// Tail bytes read on the first footer fetch of a Parquet file.
pub(crate) const TAIL_CACHE_BYTES: u64 = 64 * 1024;

/// Maximum cached tail bytes for one file.
const MAX_ENTRY_BYTES: u64 = 1024 * 1024;

/// Total tail bytes kept across all cached files on a worker.
const MAX_TOTAL_BYTES: u64 = 8 * 1024 * 1024;

/// Estimated per-entry overhead apart from path and tail payload bytes.
const ENTRY_OVERHEAD_BYTES: usize = 128;

/// Cached tail bytes `[start, file_size)` of one file.
#[derive(Clone)]
struct Entry {
    file_size: u64,
    start: u64,
    bytes: Bytes,
}

static TAILS: LazyLock<Cache<String, Entry, foldhash::fast::RandomState>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(MAX_TOTAL_BYTES)
        .weigher(|path: &String, entry: &Entry| -> u32 {
            let bytes = ENTRY_OVERHEAD_BYTES
                .saturating_add(path.len())
                .saturating_add(entry.bytes.len());
            u32::try_from(bytes).unwrap_or(u32::MAX)
        })
        .build_with_hasher(foldhash::fast::RandomState::default())
});

/// Result of looking up a requested byte range in the tail cache.
enum Lookup {
    /// The full requested range lies inside the cached tail.
    Hit(Bytes),
    /// Only `[cached_start, range.end)` is cached; fetch `[range.start, cached_start)`.
    Suffix {
        file_size: u64,
        cached_start: u64,
        suffix: Bytes,
    },
    /// None of the requested range is cached.
    Miss,
}

fn lookup(path: &str, range: &Range<u64>) -> Lookup {
    let Some(entry) = TAILS.get(path) else {
        return Lookup::Miss;
    };
    if range.end > entry.file_size || range.end <= entry.start {
        return Lookup::Miss;
    }
    if range.start >= entry.start {
        let Ok(offset) = usize::try_from(range.start - entry.start) else {
            return Lookup::Miss;
        };
        let Ok(len) = usize::try_from(range.end - range.start) else {
            return Lookup::Miss;
        };
        return Lookup::Hit(entry.bytes.slice(offset..offset + len));
    }
    let Ok(suffix_len) = usize::try_from(range.end - entry.start) else {
        return Lookup::Miss;
    };
    Lookup::Suffix {
        file_size: entry.file_size,
        cached_start: entry.start,
        suffix: entry.bytes.slice(..suffix_len),
    }
}

/// Reports whether `[start, file_size)` of `path` is cached for `file_size`.
pub(crate) fn has_tail(path: &str, file_size: u64, start: u64) -> bool {
    TAILS
        .get(path)
        .is_some_and(|entry| entry.file_size == file_size && entry.start <= start)
}

/// Stores the tail `[start, file_size)` of `path`, for a caller that knows the file size.
pub(crate) fn insert(path: &str, file_size: u64, start: u64, bytes: &Bytes) {
    let len = bytes.len() as u64;
    if len == 0 || len > MAX_ENTRY_BYTES || start.checked_add(len) != Some(file_size) {
        return;
    }
    TAILS
        .entry_by_ref(path)
        .and_upsert_with(|maybe_entry| match maybe_entry {
            Some(existing)
                if existing.value().file_size == file_size && existing.value().start <= start =>
            {
                existing.into_value()
            }
            Some(_) | None => Entry {
                file_size,
                start,
                bytes: bytes.clone(),
            },
        });
}

/// Removes any cached tail of `path`.
pub(super) fn invalidate(path: &str) {
    TAILS.invalidate(path);
}

/// Removes any cached tail of `path` whose cached size differs from `file_size`.
pub(crate) fn invalidate_if_size_differs(path: &str, file_size: u64) {
    if TAILS
        .get(path)
        .is_some_and(|entry| entry.file_size != file_size)
    {
        TAILS.invalidate(path);
    }
}

/// Removes any cached tail whose path lies under `prefix`.
pub(super) fn invalidate_prefix(prefix: &str) {
    let trimmed = prefix.trim_end_matches('/');
    for (path, _) in &*TAILS {
        let key: &str = &path;
        if key
            .strip_prefix(trimmed)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        {
            TAILS.invalidate(key);
        }
    }
}

/// Wraps a [`FileRead`] so sub-ranges inside a cached Parquet tail serve from memory.
pub struct CachedTailRead<R> {
    path: String,
    inner: R,
}

impl<R> CachedTailRead<R> {
    /// Wraps `inner` for `path`.
    pub fn new(path: impl Into<String>, inner: R) -> Self {
        Self {
            path: path.into(),
            inner,
        }
    }
}

#[async_trait::async_trait]
impl<R: FileRead> FileRead for CachedTailRead<R> {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        if range.end <= range.start {
            return Ok(Bytes::new());
        }
        match lookup(&self.path, &range) {
            Lookup::Hit(bytes) => Ok(bytes),
            Lookup::Suffix {
                file_size,
                cached_start,
                suffix,
            } => {
                let prefix = self.inner.read(range.start..cached_start).await?;
                if prefix.len() as u64 != cached_start - range.start {
                    // The file is shorter than the cached tail says, so the entry is stale.
                    invalidate(&self.path);
                    return Err(iceberg::Error::new(
                        iceberg::ErrorKind::Unexpected,
                        format!(
                            "Read {} of {} bytes before the cached tail of '{}'",
                            prefix.len(),
                            cached_start - range.start,
                            self.path
                        ),
                    )
                    .with_source(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)));
                }
                let mut combined = BytesMut::with_capacity(prefix.len() + suffix.len());
                combined.extend_from_slice(&prefix);
                combined.extend_from_slice(&suffix);
                let bytes = combined.freeze();
                if range.end == file_size {
                    insert(&self.path, file_size, range.start, &bytes);
                }
                Ok(bytes)
            }
            // The reader does not know the file size, so a miss does not fill the cache. The
            // readers that know it call `insert`.
            Lookup::Miss => self.inner.read(range).await,
        }
    }
}
