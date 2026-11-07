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

use beam_io_file::FileCache;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

/// Maximum footer bytes kept per worker.
const MAX_BYTES: u64 = 16 * 1024 * 1024;

static FOOTERS: LazyLock<FileCache<Arc<ArrowReaderMetadata>>> = LazyLock::new(|| {
    FileCache::new(MAX_BYTES, |metadata| {
        metadata
            .metadata()
            .memory_size()
            .saturating_add(metadata.schema().fields.size())
    })
});

/// Returns the cached footer of `path` with `size` bytes.
pub(crate) fn get(path: &str, size: u64) -> Option<Arc<ArrowReaderMetadata>> {
    FOOTERS.get(path, size)
}

/// Adds the footer of `path` with `size` bytes.
pub(crate) fn insert(path: &str, size: u64, metadata: &Arc<ArrowReaderMetadata>) {
    FOOTERS.insert(path, size, Arc::clone(metadata));
}
