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

use std::any::{Any, type_name};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use once_cell::sync::OnceCell;

/// A loaded model and the name of its type, for the type mismatch error.
struct Entry {
    model: Arc<dyn Any + Send + Sync>,
    type_name: &'static str,
}

impl Entry {
    fn new<M: Send + Sync + 'static>(model: Arc<M>) -> Self {
        Self {
            model,
            type_name: type_name::<M>(),
        }
    }

    fn get<M: Send + Sync + 'static>(&self, key: &str) -> beam::Result<Arc<M>> {
        Arc::clone(&self.model).downcast::<M>().map_err(|_| {
            format!(
                "Model cache key '{key}' holds a {}, not a {}",
                self.type_name,
                type_name::<M>()
            )
            .into()
        })
    }
}

/// One cache key. It keeps the model of the first load that succeeds. A failed load leaves
/// it empty, so the next call loads again.
type Slot = OnceCell<Entry>;

/// Loads each model artifact once per worker process, so threads do not duplicate
/// multi-gigabyte allocations or exhaust GPU memory.
///
/// The map is copy-on-write: a lookup reads it without a lock, and a new key replaces it.
/// Concurrent loads of one key run the loader once. A load never blocks other keys.
pub struct WorkerModelCache {
    slots: ArcSwap<HashMap<String, Arc<Slot>>>,
}

impl Default for WorkerModelCache {
    fn default() -> Self {
        Self {
            slots: ArcSwap::from_pointee(HashMap::new()),
        }
    }
}

impl WorkerModelCache {
    /// Creates an empty cache. `RunInference` uses [`global`](Self::global).
    pub fn new() -> Self {
        Self::default()
    }

    /// The cache of this process, which `RunInference` uses.
    pub fn global() -> &'static Self {
        static CACHE: OnceLock<WorkerModelCache> = OnceLock::new();
        CACHE.get_or_init(Self::new)
    }

    /// The slot of `key`, created if it is missing.
    fn slot(&self, key: &str) -> Arc<Slot> {
        // Loop because a concurrent `clear` can remove the slot that this call inserted.
        loop {
            if let Some(slot) = self.slots.load().get(key) {
                return Arc::clone(slot);
            }
            // `rcu` runs again if another thread changed the map, so a slot that another
            // thread inserted first is kept. The copy runs once for each new key.
            self.slots.rcu(|slots| {
                let mut slots = HashMap::clone(slots);
                slots.entry(key.to_string()).or_default();
                slots
            });
        }
    }

    /// Gets the model of `key`, or loads it with `loader` and caches it. Concurrent calls
    /// for the same key run `loader` once.
    ///
    /// # Errors
    ///
    /// Fails if `loader` fails, or if `key` holds a model of a type other than `M`.
    pub fn get_or_load<M, F>(&self, key: &str, loader: F) -> beam::Result<Arc<M>>
    where
        M: Send + Sync + 'static,
        F: FnOnce() -> beam::Result<M>,
    {
        self.slot(key)
            .get_or_try_init(|| loader().map(|model| Entry::new(Arc::new(model))))?
            .get(key)
    }

    /// Replaces the model of `key`. Threads that hold the previous model keep it.
    pub fn update_model<M: Send + Sync + 'static>(&self, key: &str, new_model: M) {
        let slot = Arc::new(Slot::with_value(Entry::new(Arc::new(new_model))));
        self.slots.rcu(|slots| {
            let mut slots = HashMap::clone(slots);
            slots.insert(key.to_string(), Arc::clone(&slot));
            slots
        });
    }

    /// Removes every model. Threads that hold a model keep it.
    pub fn clear(&self) {
        self.slots.store(Arc::new(HashMap::new()));
    }
}
