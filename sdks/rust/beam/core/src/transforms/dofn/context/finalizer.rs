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

//! Bundle finalization: callbacks that run after the runner durably commits a bundle, for
//! side effects such as message acks or file promotion.

use std::sync::Mutex;

use super::ProcessContext;

/// Runs once the runner confirms that the bundle was committed.
pub type FinalizationCallback = Box<dyn FnOnce() -> crate::Result + Send + 'static>;

/// Collects the finalization callbacks of a bundle. The harness runs them after commit.
#[derive(Default)]
pub struct BundleFinalizerCollector {
    callbacks: Mutex<Vec<FinalizationCallback>>,
}

impl BundleFinalizerCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_callback(&self, callback: FinalizationCallback) {
        if let Ok(mut guard) = self.callbacks.lock() {
            guard.push(callback);
        }
    }

    pub fn has_callbacks(&self) -> bool {
        let guard = self
            .callbacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !guard.is_empty()
    }

    pub fn drain(&self) -> Vec<FinalizationCallback> {
        if let Ok(mut guard) = self.callbacks.lock() {
            std::mem::take(&mut *guard)
        } else {
            Vec::new()
        }
    }
}

impl<T> ProcessContext<'_, T> {
    /// Registers `callback` to run after the runner commits the outputs of this bundle. Fails
    /// unless the function returns `true` from
    /// [`DoFn::requests_finalization`](crate::transforms::DoFn::requests_finalization): the
    /// runner would never finalize the bundle, and the callback would never run.
    pub fn register_finalizer<F>(&self, callback: F) -> crate::Result
    where
        F: FnOnce() -> crate::Result + Send + 'static,
    {
        let finalizer = self.bundle_finalizer.ok_or_else(|| {
            crate::Error::from(
                "register_finalizer needs bundle finalization: return true from \
                 `requests_finalization()`"
                    .to_string(),
            )
        })?;
        finalizer.register_callback(Box::new(callback));
        Ok(())
    }
}
