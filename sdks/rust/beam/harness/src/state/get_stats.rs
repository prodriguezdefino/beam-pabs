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

//! The State API GET requests of one bundle: pages, bytes and the time that the bundle thread
//! waits for the replies.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use beam::metrics::Metrics;

/// Namespace of the counters that the harness records for its own work.
pub const HARNESS_METRICS_NAMESPACE: &str = "beam-harness";
/// Counter of the State API GET requests. Each request returns one page.
pub const STATE_GET_PAGES: &str = "state_get_pages";
/// Counter of the bytes in the State API GET replies.
pub const STATE_GET_BYTES: &str = "state_get_bytes";
/// Counter of the microseconds that a caller waits for State API GET replies.
pub const STATE_GET_WAIT_US: &str = "state_get_wait_us";

/// The totals of the State API GET requests of one [`StateChannel`](super::StateChannel)
/// and its clones.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StateGetTotals {
    /// GET requests, one for each page, also the requests that failed.
    pub pages: u64,
    /// Bytes of state data in the replies.
    pub bytes: u64,
    /// Time that the callers block for the replies. A page that arrived before the caller
    /// asks for it adds no time.
    pub wait: Duration,
}

/// Counts the GET requests of one bundle. The clones of a channel share one value.
#[derive(Debug, Default)]
pub(crate) struct StateGetStats {
    pages: AtomicU64,
    bytes: AtomicU64,
    wait_nanos: AtomicU64,
}

impl StateGetStats {
    /// Adds one GET reply of `bytes` that took `waited`. Also adds to the `beam-harness`
    /// counters of the transform that runs on this thread.
    pub(crate) fn record(&self, bytes: usize, waited: Duration) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let wait_nanos = u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX);
        // The totals are read only after the reads, so relaxed adds are enough.
        self.pages.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.wait_nanos.fetch_add(wait_nanos, Ordering::Relaxed);
        // Microseconds, not milliseconds: a page often arrives in less than 1 ms.
        Metrics::counter(HARNESS_METRICS_NAMESPACE, STATE_GET_PAGES).inc();
        Metrics::counter(HARNESS_METRICS_NAMESPACE, STATE_GET_BYTES)
            .inc_by(i64::try_from(bytes).unwrap_or(i64::MAX));
        Metrics::counter(HARNESS_METRICS_NAMESPACE, STATE_GET_WAIT_US)
            .inc_by(i64::try_from(waited.as_micros()).unwrap_or(i64::MAX));
    }

    pub(crate) fn totals(&self) -> StateGetTotals {
        StateGetTotals {
            pages: self.pages.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            wait: Duration::from_nanos(self.wait_nanos.load(Ordering::Relaxed)),
        }
    }
}
