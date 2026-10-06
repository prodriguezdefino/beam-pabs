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

//! Unit tests for [`WorkerModelCache`] and atomic model hot-swapping.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use beam_ml::WorkerModelCache;

#[test]
fn test_worker_model_cache_get_or_load() {
    let cache = WorkerModelCache::global();
    cache.clear();

    static LOAD_COUNT: AtomicUsize = AtomicUsize::new(0);

    let load_fn = || {
        LOAD_COUNT.fetch_add(1, Ordering::SeqCst);
        beam::Result::Ok("model_v1".to_string())
    };

    let model1 = cache.get_or_load("model_key", load_fn).expect("first load");
    assert_eq!(*model1, "model_v1");
    assert_eq!(LOAD_COUNT.load(Ordering::SeqCst), 1);

    // The second load reads from the cache and does not call `load_fn`.
    let model2 = cache
        .get_or_load("model_key", load_fn)
        .expect("second load");
    assert_eq!(*model2, "model_v1");
    assert_eq!(LOAD_COUNT.load(Ordering::SeqCst), 1);

    cache
        .update_model("model_key", "model_v2".to_string())
        .expect("update model");

    let model3: Arc<String> = cache
        .get_or_load("model_key", load_fn)
        .expect("third load after update");
    assert_eq!(*model3, "model_v2");
    assert_eq!(LOAD_COUNT.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "dynamic-refresh")]
#[test]
fn test_swappable_model_atomic_hot_swap() {
    use beam_ml::SwappableModel;

    let initial = Arc::new("model_version_1".to_string());
    let swappable = SwappableModel::new(initial);

    let m1 = swappable.load();
    assert_eq!(*m1, "model_version_1");

    let updated = Arc::new("model_version_2".to_string());
    swappable.update(updated);

    let m2 = swappable.load();
    assert_eq!(*m2, "model_version_2");

    // A reference loaded before the swap stays valid and keeps the old model.
    assert_eq!(*m1, "model_version_1");
}
