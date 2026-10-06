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

//! Unit tests for [`WorkerModelCache`].

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use beam_ml::WorkerModelCache;

#[test]
fn test_worker_model_cache_get_or_load() {
    let cache = WorkerModelCache::new();
    const KEY: &str = "model_key";

    static LOAD_COUNT: AtomicUsize = AtomicUsize::new(0);

    let load_fn = || {
        LOAD_COUNT.fetch_add(1, Ordering::SeqCst);
        beam::Result::Ok("model_v1".to_string())
    };

    let model1 = cache.get_or_load(KEY, load_fn).expect("first load");
    assert_eq!(*model1, "model_v1");
    assert_eq!(LOAD_COUNT.load(Ordering::SeqCst), 1);

    // The second load reads from the cache and does not call `load_fn`.
    let model2 = cache.get_or_load(KEY, load_fn).expect("second load");
    assert_eq!(*model2, "model_v1");
    assert_eq!(LOAD_COUNT.load(Ordering::SeqCst), 1);

    cache.update_model(KEY, "model_v2".to_string());

    let model3: Arc<String> = cache
        .get_or_load(KEY, load_fn)
        .expect("third load after update");
    assert_eq!(*model3, "model_v2");
    assert_eq!(LOAD_COUNT.load(Ordering::SeqCst), 1);
}

/// Threads that ask for the same key at the same time run the loader once.
#[test]
fn test_concurrent_loads_of_one_key_load_once() {
    let cache = WorkerModelCache::new();
    let loads = AtomicUsize::new(0);
    let models: Vec<Arc<String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    cache
                        .get_or_load("shared", || {
                            loads.fetch_add(1, Ordering::SeqCst);
                            std::thread::sleep(std::time::Duration::from_millis(20));
                            Ok("model".to_string())
                        })
                        .expect("load")
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("join"))
            .collect()
    });
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert!(models.iter().all(|m| Arc::ptr_eq(m, &models[0])));
}

/// A load of one key does not block a load of another key.
#[test]
fn test_slow_load_does_not_block_other_keys() {
    let cache = WorkerModelCache::new();
    let (loading, started) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let cache = &cache;
    std::thread::scope(|scope| {
        let slow = scope.spawn(move || {
            cache.get_or_load("slow", || {
                loading.send(()).expect("send");
                released.recv().expect("release");
                Ok(1u32)
            })
        });
        started.recv().expect("slow load started");
        // The slow load still holds its own key while this load finishes.
        assert_eq!(*cache.get_or_load("fast", || Ok(2u32)).expect("fast"), 2);
        release.send(()).expect("release");
        assert_eq!(*slow.join().expect("join").expect("slow"), 1);
    });
}

/// A key that holds a model of another type gives an error and does not reload.
#[test]
fn test_type_mismatch_is_an_error() {
    let cache = WorkerModelCache::new();
    cache
        .get_or_load("key", || Ok("model".to_string()))
        .expect("load");
    let err = cache
        .get_or_load::<u32, _>("key", || panic!("a type mismatch must not reload"))
        .expect_err("type mismatch");
    let message = err.to_string();
    assert!(
        message.contains("key") && message.contains("String") && message.contains("u32"),
        "{message}"
    );
}

#[derive(Clone)]
struct CountingHandler {
    model_id: Option<&'static str>,
    loads: Arc<AtomicUsize>,
}

impl beam_ml::ModelHandler<i64, i64> for CountingHandler {
    type Model = i64;
    type Batch = Vec<i64>;
    type Converter = beam::transforms::VecBatchConverter<i64>;

    fn load_model(&self) -> beam::Result<Self::Model> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(10)
    }

    fn run_inference(
        &self,
        batch: &Self::Batch,
        model: &Self::Model,
        _args: Option<&beam_ml::InferenceArgs>,
    ) -> beam::Result<Vec<i64>> {
        Ok(batch.iter().map(|v| v + *model).collect())
    }

    fn get_batch_converter(&self) -> Self::Converter {
        beam::transforms::VecBatchConverter::new()
    }

    fn model_id(&self) -> Option<String> {
        self.model_id.map(str::to_string)
    }
}

#[test]
fn test_run_inference_uses_worker_model_cache_when_model_id_is_set() {
    use beam::internals::PipelineInternals;
    use beam::prelude::*;
    use beam_ml::RunInference;

    let cached_loads = Arc::new(AtomicUsize::new(0));
    let p = Pipeline::new();
    let input = p.apply("In", Create::new([1_i64, 2]));
    let _ = input.apply(
        "CachedSingle",
        RunInference::new(CountingHandler {
            model_id: Some("run_inference_cached_single_v1"),
            loads: Arc::clone(&cached_loads),
        }),
    );
    let _ = input.apply(
        "CachedMulti",
        RunInference::new(CountingHandler {
            model_id: Some("run_inference_cached_single_v1"),
            loads: Arc::clone(&cached_loads),
        })
        .with_exception_handling(),
    );
    let uncached_loads = Arc::new(AtomicUsize::new(0));
    let _ = input.apply(
        "UncachedSingle",
        RunInference::new(CountingHandler {
            model_id: None,
            loads: Arc::clone(&uncached_loads),
        }),
    );

    let components = p.to_proto().components.expect("pipeline components");
    let transform_id = |unique_name: &str| {
        components
            .transforms
            .iter()
            .find_map(|(id, t)| (t.unique_name == unique_name).then_some(id.as_str()))
            .unwrap_or_else(|| panic!("transform '{unique_name}' must exist"))
    };

    let mut handlers = p.transform_handlers();
    let single_factory = handlers
        .remove(transform_id("CachedSingle/Predict"))
        .expect("single handler");
    let multi_factory = handlers
        .remove(transform_id("CachedMulti/PredictWithDLQ"))
        .expect("multi handler");
    let uncached_factory = handlers
        .remove(transform_id("UncachedSingle/Predict"))
        .expect("uncached handler");

    single_factory.instantiate().setup().expect("setup 1");
    single_factory.instantiate().setup().expect("setup 2");
    multi_factory.instantiate().setup().expect("multi setup");
    assert_eq!(cached_loads.load(Ordering::SeqCst), 1);

    uncached_factory
        .instantiate()
        .setup()
        .expect("uncached setup 1");
    uncached_factory
        .instantiate()
        .setup()
        .expect("uncached setup 2");
    assert_eq!(uncached_loads.load(Ordering::SeqCst), 2);
}
