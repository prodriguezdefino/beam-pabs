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

//! [`WorkerModelCache::clear`] tests, in their own binary so clearing the process-wide
//! cache cannot race other cache tests.

use std::sync::atomic::{AtomicUsize, Ordering};

use beam_ml::WorkerModelCache;

#[test]
fn test_clear_forces_reload_and_type_mismatch_reloads() {
    let cache = WorkerModelCache::global();
    let loads = AtomicUsize::new(0);
    let load = |value: &'static str| {
        loads.fetch_add(1, Ordering::SeqCst);
        beam::Result::Ok(value.to_string())
    };

    assert_eq!(*cache.get_or_load("k", || load("v1")).unwrap(), "v1");
    assert_eq!(*cache.get_or_load("k", || load("unused")).unwrap(), "v1");
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    cache.clear();
    assert_eq!(*cache.get_or_load("k", || load("v2")).unwrap(), "v2");
    assert_eq!(loads.load(Ordering::SeqCst), 2);

    // A different model type under the same key misses the downcast and reloads.
    let other: std::sync::Arc<u32> = cache.get_or_load("k", || Ok(7u32)).unwrap();
    assert_eq!(*other, 7);

    let err = cache
        .get_or_load::<String, _>("failing", || Err("load failed".into()))
        .expect_err("loader error propagates");
    assert!(err.to_string().contains("load failed"), "{err}");
}
