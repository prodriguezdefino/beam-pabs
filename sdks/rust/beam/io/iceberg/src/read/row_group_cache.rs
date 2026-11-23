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

//! A worker-level cache of Parquet row-group byte ranges, so the splits of one file decode
//! its footer once.

use std::sync::{Arc, LazyLock};

use beam_io_file::FileCache;

/// Maximum row-group range bytes kept per worker.
const MAX_BYTES: u64 = 4 * 1024 * 1024;

type RowGroupRanges = Arc<[(u64, u64)]>;

static ROW_GROUPS: LazyLock<FileCache<RowGroupRanges>> = LazyLock::new(|| {
    FileCache::new(MAX_BYTES, |ranges| {
        ranges.len().saturating_mul(size_of::<(u64, u64)>())
    })
});

/// Returns the cached row-group byte ranges of `path` with `size` bytes.
pub(crate) fn get(path: &str, size: u64) -> Option<RowGroupRanges> {
    ROW_GROUPS.get(path, size)
}

/// Stores the row-group ranges of `path` with `size` bytes.
pub(crate) fn insert(path: &str, size: u64, ranges: &RowGroupRanges) {
    ROW_GROUPS.insert(path, size, Arc::clone(ranges));
}
