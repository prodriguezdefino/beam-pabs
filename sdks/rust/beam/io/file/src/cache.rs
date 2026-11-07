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

//! Byte-bounded worker cache keyed by file path and byte length.

use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use moka::sync::Cache;

/// Estimated per-entry overhead apart from path and value payload bytes.
const ENTRY_OVERHEAD_BYTES: usize = 128;

#[derive(Clone, Debug, Eq)]
struct FileKey {
    path: Arc<str>,
    size: u64,
}

trait FileKeyParts {
    fn parts(&self) -> (&str, u64);
}

impl FileKeyParts for FileKey {
    fn parts(&self) -> (&str, u64) {
        (&self.path, self.size)
    }
}

impl FileKeyParts for (&str, u64) {
    fn parts(&self) -> (&str, u64) {
        *self
    }
}

impl PartialEq for FileKey {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Hash for FileKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (path, size) = self.parts();
        path.hash(state);
        size.hash(state);
    }
}

impl<'a> Borrow<dyn FileKeyParts + 'a> for FileKey {
    fn borrow(&self) -> &(dyn FileKeyParts + 'a) {
        self
    }
}

impl Hash for dyn FileKeyParts + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (path, size) = self.parts();
        path.hash(state);
        size.hash(state);
    }
}

impl PartialEq for dyn FileKeyParts + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn FileKeyParts + '_ {}

/// A byte-bounded worker cache keyed by `(path, size)`.
///
/// Overwriting a path with a different byte size misses any entry from the older file.
pub struct FileCache<V: Clone + Send + Sync + 'static> {
    entries: Cache<FileKey, V, foldhash::fast::RandomState>,
}

impl<V: Clone + Send + Sync + 'static> FileCache<V> {
    /// Creates a cache that keeps at most `max_bytes`, weighing values with `value_bytes`.
    pub fn new(max_bytes: u64, value_bytes: fn(&V) -> usize) -> Self {
        let entries = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(move |key: &FileKey, value: &V| -> u32 {
                let bytes = ENTRY_OVERHEAD_BYTES
                    .saturating_add(key.path.len())
                    .saturating_add(value_bytes(value));
                u32::try_from(bytes).unwrap_or(u32::MAX)
            })
            .build_with_hasher(foldhash::fast::RandomState::default());
        Self { entries }
    }

    /// Looks up the cached value of `path` with `size` bytes without allocating a key.
    pub fn get(&self, path: &str, size: u64) -> Option<V> {
        self.entries.get(&(path, size) as &dyn FileKeyParts)
    }

    /// Inserts `value` for `path` with `size` bytes.
    pub fn insert(&self, path: &str, size: u64, value: V) {
        self.entries.insert(
            FileKey {
                path: Arc::from(path),
                size,
            },
            value,
        );
    }
}
