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

use std::sync::Arc;

use beam_io_file::FileCache;

#[test]
fn file_cache_matches_path_and_size() {
    let cache: FileCache<Arc<[u8]>> = FileCache::new(64 * 1024, |buf| buf.len());
    let payload: Arc<[u8]> = Arc::from([1_u8, 2, 3, 4]);
    cache.insert("gs://bucket/part-0.parquet", 100, Arc::clone(&payload));

    let hit = cache.get("gs://bucket/part-0.parquet", 100).unwrap();
    assert!(Arc::ptr_eq(&payload, &hit));
    assert!(cache.get("gs://bucket/part-0.parquet", 101).is_none());
    assert!(cache.get("gs://bucket/part-1.parquet", 100).is_none());
}

#[test]
fn file_cache_evicts_entries_above_byte_budget() {
    let cache: FileCache<Arc<[u8]>> = FileCache::new(4 * 1024, |buf| buf.len());
    for i in 0..128_u8 {
        let block: Arc<[u8]> = Arc::from(vec![i; 1024]);
        cache.insert(&format!("gs://bucket/file-{i}"), 1024, block);
    }
    let kept = (0..128_u8)
        .filter(|i| cache.get(&format!("gs://bucket/file-{i}"), 1024).is_some())
        .count();
    assert!(
        kept < 64,
        "expected byte budget to evict entries, kept {kept}"
    );
}
