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
//! side effects such as message acknowledgements or offset commits.
//!
//! A function declares finalization with `type Out = WithFinalizer<O>`. Only its contexts
//! have [`finalizer`](ProcessContext::finalizer).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{FinishBundleContext, ProcessContext};
use crate::transforms::dofn::outputs::{Outputs, WithFinalizer};

/// The time after registration when a callback expires, unless
/// [`Finalizer::expire_after`] sets another. An expired callback does not run.
pub const DEFAULT_FINALIZER_EXPIRY: Duration = Duration::from_secs(10 * 60);

/// Runs once the runner confirms that the bundle was committed.
pub type FinalizationCallback = Box<dyn FnOnce() -> crate::Result + Send + 'static>;

/// A callback of a bundle and the wall-clock time after which it must not run.
pub struct PendingFinalizer {
    pub deadline: std::time::Instant,
    pub callback: FinalizationCallback,
}

impl PendingFinalizer {
    pub fn is_expired(&self, now: std::time::Instant) -> bool {
        now > self.deadline
    }
}

/// Collects the finalization callbacks of a bundle. The harness runs them after commit.
#[derive(Default)]
pub struct BundleFinalizerCollector {
    callbacks: Mutex<Vec<PendingFinalizer>>,
}

impl BundleFinalizerCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, finalizer: PendingFinalizer) {
        let mut guard = self
            .callbacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.push(finalizer);
    }

    pub fn has_callbacks(&self) -> bool {
        let guard = self
            .callbacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !guard.is_empty()
    }

    pub fn drain(&self) -> Vec<PendingFinalizer> {
        let mut guard = self
            .callbacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *guard)
    }
}

/// A finalization callback under construction. [`register`](Self::register) adds it to the
/// bundle.
#[must_use = "a finalizer does nothing until `register` is called"]
pub struct Finalizer<'a> {
    collector: Option<&'a Arc<BundleFinalizerCollector>>,
    callback: FinalizationCallback,
    expiry: Duration,
}

impl Finalizer<'_> {
    /// Sets the time after registration when the callback expires. The default is
    /// [`DEFAULT_FINALIZER_EXPIRY`]. Use the longest time that the effect of the callback
    /// can wait, for example the time that an acknowledgement stays valid.
    pub fn expire_after(mut self, expiry: Duration) -> Self {
        self.expiry = expiry;
        self
    }

    /// Adds the callback to the bundle. It runs at most once, after the runner commits the
    /// outputs and the residuals of the bundle, and only if that happens before it expires.
    pub fn register(self) -> crate::Result {
        let Self {
            collector,
            callback,
            expiry,
        } = self;
        let collector = collector.ok_or_else(|| {
            crate::Error::msg(
                "This bundle has no finalizer: the runner did not ask the SDK to finalize it",
            )
        })?;
        collector.register(PendingFinalizer {
            deadline: std::time::Instant::now() + expiry,
            callback: Box::new(callback),
        });
        Ok(())
    }
}

fn finalizer<'a, F>(
    collector: Option<&'a Arc<BundleFinalizerCollector>>,
    callback: F,
) -> Finalizer<'a>
where
    F: FnOnce() -> crate::Result + Send + 'static,
{
    Finalizer {
        collector,
        callback: Box::new(callback),
        expiry: DEFAULT_FINALIZER_EXPIRY,
    }
}

impl<'a, O: Outputs> ProcessContext<'a, WithFinalizer<O>> {
    /// Starts a callback that runs after the runner commits this bundle. Use it to
    /// acknowledge or release what the bundle read, never for work that the output needs:
    /// the callback might never run, for example when the commit fails or the callback
    /// expires. It runs on another thread, possibly while later bundles run, so make its
    /// effect safe to apply out of order.
    pub fn finalizer<F>(&self, callback: F) -> Finalizer<'a>
    where
        F: FnOnce() -> crate::Result + Send + 'static,
    {
        finalizer(self.bundle_finalizer, callback)
    }
}

impl<'a, O: Outputs> FinishBundleContext<'a, WithFinalizer<O>> {
    /// Starts a callback that runs after the runner commits this bundle. See
    /// [`ProcessContext::finalizer`].
    pub fn finalizer<F>(&self, callback: F) -> Finalizer<'a>
    where
        F: FnOnce() -> crate::Result + Send + 'static,
    {
        finalizer(self.inner.bundle_finalizer, callback)
    }
}
