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

//! A worker-level cache of Parquet footers, so that the splits of one file read its footer
//! once.

use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

/// Footers kept per worker. A footer of a wide file can be large, so the cache stays small.
const CAPACITY: usize = 16;

/// The footers of the files read last, newest first.
static FOOTERS: LazyLock<ArcSwap<Vec<Entry>>> = LazyLock::new(|| ArcSwap::from_pointee(Vec::new()));

/// A footer and the file it belongs to. The size is part of the key, so a file that is
/// written again with another size does not get the old footer.
#[derive(Clone)]
struct Entry {
    path: Arc<str>,
    size: u64,
    metadata: Arc<ArrowReaderMetadata>,
}

/// Returns the cached footer of `path` with `size` bytes.
pub(crate) fn get(path: &str, size: u64) -> Option<Arc<ArrowReaderMetadata>> {
    FOOTERS
        .load()
        .iter()
        .find(|entry| entry.size == size && &*entry.path == path)
        .map(|entry| Arc::clone(&entry.metadata))
}

/// Adds the footer of `path` with `size` bytes and drops the oldest footer when full.
pub(crate) fn insert(path: &str, size: u64, metadata: &Arc<ArrowReaderMetadata>) {
    let path: Arc<str> = Arc::from(path);
    // The update copies the small list, so a lookup never waits.
    FOOTERS.rcu(|entries| {
        let newest = Entry {
            path: Arc::clone(&path),
            size,
            metadata: Arc::clone(metadata),
        };
        std::iter::once(newest)
            .chain(
                entries
                    .iter()
                    .filter(|entry| entry.size != size || entry.path != path)
                    .cloned(),
            )
            .take(CAPACITY)
            .collect::<Vec<_>>()
    });
}
