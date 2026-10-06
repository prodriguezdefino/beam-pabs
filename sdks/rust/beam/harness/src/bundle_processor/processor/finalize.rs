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

//! Bundle finalization callbacks: storage, expiry and the runner's finalize request.

use std::collections::HashMap;
use std::fmt;
use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval_at};
use tracing::{debug, warn};

use super::BundleProcessor;
use beam::internals::PendingFinalizer;

/// The pending finalization callbacks of each bundle, by instruction id.
pub(super) type PendingFinalizations = HashMap<String, Vec<PendingFinalizer>>;
/// Starts a task that drops the expired finalization callbacks of `bundles` at intervals of
/// `every`. The first sweep is one interval after the start. The task stops at the first tick
/// after the processor is dropped.
#[doc(hidden)]
pub fn spawn_finalizer_sweeper(bundles: Weak<BundleProcessor>, every: Duration) -> JoinHandle<()> {
    let mut ticker = interval_at(tokio::time::Instant::now() + every, every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tokio::spawn(async move {
        loop {
            ticker.tick().await;
            let Some(processor) = bundles.upgrade() else {
                return;
            };
            processor.sweep_expired_finalizations();
        }
    })
}
/// Removes the callbacks in `map` that expired at `now`, and the bundles with no callback left.
fn take_expired(map: &mut PendingFinalizations, now: Instant) -> Vec<PendingFinalizer> {
    let expired = map
        .values_mut()
        .flat_map(|pending| pending.extract_if(.., |finalizer| finalizer.is_expired(now)))
        .collect();
    map.retain(|_, pending| !pending.is_empty());
    expired
}

impl BundleProcessor {
    /// Stores the finalization callbacks of a completed bundle. A runner does not finalize every
    /// bundle, for example when the commit of a bundle fails. The finalizer sweeper drops such
    /// callbacks when they expire.
    pub fn store_finalization(&self, instruction_id: &str, callbacks: Vec<PendingFinalizer>) {
        if let Ok(mut map) = self.pending_finalizations.lock() {
            map.insert(instruction_id.to_string(), callbacks);
        }
    }
    /// Drops the expired callbacks of all pending bundles and returns their number. The task of
    /// [`spawn_finalizer_sweeper`] calls this function at each tick.
    #[doc(hidden)]
    pub fn sweep_expired_finalizations(&self) -> usize {
        // The callbacks drop after the guard, so the state that they own drops outside the lock.
        let expired = self
            .pending_finalizations
            .lock()
            .map(|mut map| take_expired(&mut map, Instant::now()))
            .unwrap_or_default();
        self.record_dropped(
            expired.len(),
            "bundles that the runner did not finalize before the deadline",
        );
        expired.len()
    }
    /// Counts the finalization callbacks that expired before they could run. The count includes
    /// the callbacks that the runner did not finalize and the callbacks that it finalized late.
    #[doc(hidden)]
    pub fn expired_finalization_count(&self) -> u64 {
        self.expired_finalizations.load(Ordering::Relaxed)
    }
    /// Counts and logs `dropped` expired callbacks of `scope`.
    fn record_dropped(&self, dropped: usize, scope: impl fmt::Display) {
        if dropped > 0 {
            self.expired_finalizations
                .fetch_add(dropped as u64, Ordering::Relaxed);
            warn!(
                "Dropped {dropped} expired bundle finalization callback(s) of {scope}. These \
                 callbacks do not run."
            );
        }
    }
    /// Runs the callbacks of the bundle `instruction_id` that have not expired. On failure,
    /// returns the joined messages of the failed callbacks or a poisoned-lock message.
    pub fn finalize_bundle(&self, instruction_id: &str) -> Result<(), String> {
        let callbacks = self
            .pending_finalizations
            .lock()
            .map_err(|e| format!("Lock poisoned: {e}"))?
            .remove(instruction_id);

        let Some(callbacks) = callbacks else {
            debug!(
                "No pending finalizations for instruction '{}' (may already be finalized)",
                instruction_id
            );
            return Ok(());
        };

        let now = Instant::now();
        let (expired, live): (Vec<_>, Vec<_>) = callbacks
            .into_iter()
            .partition(|finalizer| finalizer.is_expired(now));
        self.record_dropped(
            expired.len(),
            format_args!("instruction '{instruction_id}'"),
        );
        let count = live.len();
        let errors: Vec<String> = live
            .into_iter()
            .filter_map(|finalizer| (finalizer.callback)().err().map(|e| e.to_string()))
            .collect();

        if errors.is_empty() {
            debug!(
                "Successfully executed {} bundle finalization callback(s) for instruction '{}'",
                count, instruction_id
            );
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}
