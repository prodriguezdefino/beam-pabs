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

//! Process-wide model instance cache. Worker threads share one loaded model.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

/// Loads each model artifact once per worker process, so threads do not duplicate
/// multi-gigabyte allocations or exhaust GPU memory.
pub struct WorkerModelCache {
    entries: RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>>,
}

impl WorkerModelCache {
    pub fn global() -> &'static Self {
        static CACHE: OnceLock<WorkerModelCache> = OnceLock::new();
        CACHE.get_or_init(|| Self {
            entries: RwLock::new(HashMap::new()),
        })
    }

    /// Gets a cached model or loads and inserts it with `loader`. Fails if the cache lock
    /// is poisoned or `loader` fails.
    pub fn get_or_load<M, F>(&self, key: &str, loader: F) -> beam::Result<Arc<M>>
    where
        M: Send + Sync + 'static,
        F: FnOnce() -> beam::Result<M>,
    {
        {
            let read = self
                .entries
                .read()
                .map_err(|e| format!("Cache lock poisoned: {e}"))?;
            if let Some(downcasted) = read
                .get(key)
                .and_then(|entry| Arc::clone(entry).downcast::<M>().ok())
            {
                return Ok(downcasted);
            }
        }

        // Check again under the write lock: another thread can insert the model
        // between the two locks.
        let mut write = self
            .entries
            .write()
            .map_err(|e| format!("Cache lock poisoned: {e}"))?;
        if let Some(downcasted) = write
            .get(key)
            .and_then(|entry| Arc::clone(entry).downcast::<M>().ok())
        {
            return Ok(downcasted);
        }

        let model = loader()?;
        let arc_model = Arc::new(model);
        write.insert(
            key.to_string(),
            arc_model.clone() as Arc<dyn Any + Send + Sync>,
        );
        Ok(arc_model)
    }

    /// Updates or replaces a model in the cache. Fails if the cache lock is poisoned.
    pub fn update_model<M: Send + Sync + 'static>(&self, key: &str, new_model: M) -> beam::Result {
        let mut write = self
            .entries
            .write()
            .map_err(|e| format!("Cache lock poisoned: {e}"))?;
        let arc_model = Arc::new(new_model);
        write.insert(key.to_string(), arc_model as Arc<dyn Any + Send + Sync>);
        Ok(())
    }

    pub fn clear(&self) {
        if let Ok(mut write) = self.entries.write() {
            write.clear();
        }
    }
}

/// Model container that threads can hot-swap lock-free, through `arc_swap::ArcSwap`.
#[cfg(feature = "dynamic-refresh")]
pub struct SwappableModel<M: Send + Sync + 'static> {
    current: arc_swap::ArcSwap<M>,
}

#[cfg(feature = "dynamic-refresh")]
impl<M: Send + Sync + 'static> SwappableModel<M> {
    pub fn new(initial_model: Arc<M>) -> Self {
        Self {
            current: arc_swap::ArcSwap::new(initial_model),
        }
    }

    /// Loads the active model pointer. The load is wait-free.
    pub fn load(&self) -> Arc<M> {
        self.current.load_full()
    }

    /// Atomically replaces the active model. In-flight batches finish with their own
    /// `Arc`; later batches see `new_model`.
    pub fn update(&self, new_model: Arc<M>) {
        self.current.store(new_model);
    }
}
