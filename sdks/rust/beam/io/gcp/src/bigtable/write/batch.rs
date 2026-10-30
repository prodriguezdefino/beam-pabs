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

//! Contiguous mutation batch buffer, in-flight concurrency limiter, and pure partial-failure
//! partitioning for `Bigtable.MutateRows`.

use std::time::Duration;

use super::decode::PendingEntry;
use crate::bigtable::client::is_transient_code;
use crate::bigtable::proto::{MutateRowsResponse, RateLimitInfo, Status};

/// Default maximum number of row entries per `MutateRows` RPC (`500`).
pub const DEFAULT_MAX_ELEMENTS_PER_BATCH: usize = 500;

/// Default maximum byte size per `MutateRows` RPC (`4 MiB`).
pub const DEFAULT_MAX_BYTES_PER_BATCH: usize = 4 * 1024 * 1024;

/// Default maximum number of concurrent in-flight `MutateRows` RPCs per bundle (`8`).
pub const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 8;

/// Default maximum total in-flight bytes per bundle (`32 MiB`).
pub const DEFAULT_MAX_IN_FLIGHT_BYTES: usize = 32 * 1024 * 1024;

/// One ready-to-send `MutateRows` batch.
#[derive(Clone, Debug)]
pub struct BatchRequest {
    pub entries: Vec<PendingEntry>,
    pub total_bytes: usize,
}

/// Accumulates [`PendingEntry`] items into size- and count-bounded [`BatchRequest`]s.
#[derive(Clone, Debug)]
pub struct BatchBuffer {
    entries: Vec<PendingEntry>,
    total_bytes: usize,
    max_elements: usize,
    max_bytes: usize,
}

impl BatchBuffer {
    /// Creates a new buffer with `max_elements` and `max_bytes` thresholds.
    pub fn new(max_elements: usize, max_bytes: usize) -> Self {
        let cap = max_elements.clamp(1, 100_000);
        Self {
            entries: Vec::with_capacity(cap.min(1024)),
            total_bytes: 0,
            max_elements: cap,
            max_bytes: max_bytes.max(1),
        }
    }

    /// Appends `entry` to the buffer. If `entry` would overflow the current non-empty batch,
    /// flushes and returns the full [`BatchRequest`] first.
    pub fn push(&mut self, entry: PendingEntry) -> Option<BatchRequest> {
        let would_exceed_bytes =
            !self.entries.is_empty() && self.total_bytes + entry.byte_size > self.max_bytes;
        let would_exceed_count = self.entries.len() >= self.max_elements;

        let flushed = if would_exceed_bytes || would_exceed_count {
            self.flush()
        } else {
            None
        };

        self.total_bytes += entry.byte_size;
        self.entries.push(entry);
        flushed
    }

    /// Returns whether the buffer currently has reached `max_elements` or `max_bytes`.
    pub fn is_ready(&self) -> bool {
        self.entries.len() >= self.max_elements || self.total_bytes >= self.max_bytes
    }

    /// Takes all buffered entries as a [`BatchRequest`], if non-empty.
    pub fn flush(&mut self) -> Option<BatchRequest> {
        if self.entries.is_empty() {
            return None;
        }
        let total_bytes = std::mem::replace(&mut self.total_bytes, 0);
        let entries = std::mem::take(&mut self.entries);
        Some(BatchRequest {
            entries,
            total_bytes,
        })
    }
}

/// Bounds the number and byte weight of concurrent in-flight `MutateRows` RPCs per worker bundle.
#[derive(Clone, Copy, Debug)]
pub struct InFlightLimit {
    active_requests: usize,
    active_bytes: usize,
    max_requests: usize,
    max_bytes: usize,
}

impl InFlightLimit {
    pub fn new(max_requests: usize, max_bytes: usize) -> Self {
        Self {
            active_requests: 0,
            active_bytes: 0,
            max_requests: max_requests.max(1),
            max_bytes: max_bytes.max(1),
        }
    }

    pub fn add(&mut self, bytes: usize) {
        self.active_requests += 1;
        self.active_bytes = self.active_bytes.saturating_add(bytes);
    }

    pub fn remove(&mut self, bytes: usize) {
        self.active_requests = self.active_requests.saturating_sub(1);
        self.active_bytes = self.active_bytes.saturating_sub(bytes);
    }

    pub fn is_full(&self) -> bool {
        self.active_requests >= self.max_requests || self.active_bytes >= self.max_bytes
    }
}

/// Result of partitioning per-entry statuses from a `MutateRows` response stream.
#[derive(Clone, Debug)]
pub enum BatchOutcome {
    /// Every entry in the batch succeeded (`status.code == 0`).
    AllSucceeded,
    /// Some entries failed with transient or permanent statuses.
    Partial {
        retryable: Vec<PendingEntry>,
        rejected: Vec<(PendingEntry, Status)>,
        server_backoff: Option<Duration>,
    },
}

/// Pure function that partitions `batch` into succeeded, retryable, and permanently rejected
/// entries based on the `MutateRowsResponse` messages received from Bigtable.
///
/// Any entry whose index is absent from `responses` (for example, if the server closed the
/// stream after reporting partial results) is treated as `retryable`.
pub fn partition_outcomes(
    batch: Vec<PendingEntry>,
    responses: Vec<MutateRowsResponse>,
    enable_flow_control: bool,
) -> BatchOutcome {
    let mut statuses: Vec<Option<Status>> = vec![None; batch.len()];
    let mut server_backoff: Option<Duration> = None;

    for MutateRowsResponse {
        entries,
        rate_limit_info,
    } in responses
    {
        if enable_flow_control
            && let Some(info) = &rate_limit_info
            && let Some(delay) = rate_limit_backoff(info)
        {
            server_backoff = Some(server_backoff.map_or(delay, |prev| prev.max(delay)));
        }
        for entry in entries {
            if let Some(slot) = usize::try_from(entry.index)
                .ok()
                .and_then(|idx| statuses.get_mut(idx))
            {
                *slot = Some(entry.status.unwrap_or_default());
            }
        }
    }

    let mut retryable = Vec::new();
    let mut rejected = Vec::new();

    for (entry, status_opt) in batch.into_iter().zip(statuses) {
        match status_opt {
            Some(status) if status.code == 0 => {}
            Some(status) if is_transient_code(status.code) => {
                retryable.push(entry);
            }
            Some(status) => {
                rejected.push((entry, status));
            }
            None => {
                retryable.push(entry);
            }
        }
    }

    if retryable.is_empty() && rejected.is_empty() {
        BatchOutcome::AllSucceeded
    } else {
        BatchOutcome::Partial {
            retryable,
            rejected,
            server_backoff,
        }
    }
}

/// Computes a client backoff delay when Bigtable's [`RateLimitInfo`] indicates tablet
/// overload (`factor < 1.0`).
pub fn rate_limit_backoff(info: &RateLimitInfo) -> Option<Duration> {
    if !(0.0..1.0).contains(&info.factor) {
        return None;
    }
    let period = info.period.as_ref()?;
    let base_secs = (period.seconds.max(0) as f64) + (period.nanos.max(0) as f64) / 1_000_000_000.0;
    if base_secs <= 0.0 {
        return None;
    }
    // Scale delay inversely with `factor` when `factor < 1.0`, capped at 10 seconds.
    let scale = (1.0 - info.factor).clamp(0.05, 0.9);
    let delay_secs = (base_secs * scale).min(10.0);
    Some(Duration::from_secs_f64(delay_secs))
}
