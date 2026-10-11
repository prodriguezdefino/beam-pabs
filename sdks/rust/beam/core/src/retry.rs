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

//! Retries with exponential backoff and jitter, for calls to external services from I/O
//! clients and `DoFn` code.

use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::metrics::Metrics;

/// How often a failed call runs again, and how long each retry waits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Backoff {
    /// Calls in total, the first call included.
    pub attempts: u32,
    /// The base wait before the first retry.
    pub initial: Duration,
    /// The largest wait.
    pub max: Duration,
}

impl Backoff {
    /// The base wait before retry `attempt` (0 is the first retry): `initial · 2^attempt`, at
    /// most `max`.
    pub fn base(self, attempt: u32) -> Duration {
        let factor = 1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX);
        self.initial.saturating_mul(factor).min(self.max)
    }

    /// The wait before retry `attempt`: a random time from half the [base](Self::base) to the
    /// base, so that clients that fail together do not retry together.
    pub fn delay(self, attempt: u32) -> Duration {
        let base = self.base(attempt);
        let half = base / 2;
        let spread = u64::try_from((base - half).as_nanos()).unwrap_or(u64::MAX);
        let random = RandomState::new().hash_one(attempt);
        half + Duration::from_nanos(random.checked_rem(spread + 1).unwrap_or(0))
    }

    /// Whether a call that failed on `attempt` (0 is the first call) can run again.
    pub fn allows_retry(self, attempt: u32) -> bool {
        attempt.saturating_add(1) < self.attempts
    }

    /// The number of retries after the first call.
    pub fn retries(self) -> u32 {
        self.attempts.saturating_sub(1)
    }
}

/// What [`with_retries`] does after a failed call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Return the error.
    Stop,
    /// Call again after the [`Backoff::delay`] of the attempt. The wait is a retry wait.
    Backoff,
    /// The server pushes back without a delay, for example with `RESOURCE_EXHAUSTED`. Call
    /// again after the [`Backoff::delay`] of the attempt. The wait is throttling.
    Throttled,
    /// The server sends this delay. Call again after it, at most [`Backoff::max`]. The wait is
    /// throttling.
    Wait(Duration),
}

impl Decision {
    /// The wait before retry `attempt` (0 is the first retry), or `None` for
    /// [`Decision::Stop`].
    pub fn wait(self, backoff: Backoff, attempt: u32) -> Option<Duration> {
        match self {
            Self::Stop => None,
            Self::Backoff | Self::Throttled => Some(backoff.delay(attempt)),
            Self::Wait(wait) => Some(wait.min(backoff.max)),
        }
    }

    /// Whether the wait is server push-back. Runners read throttling time to hold back
    /// autoscaling, so count only these waits as throttling.
    pub fn is_throttling(self) -> bool {
        match self {
            Self::Throttled | Self::Wait(_) => true,
            Self::Stop | Self::Backoff => false,
        }
    }
}

/// Runs `call`, and runs it again after an error as `decide` and `backoff` allow. The calls
/// stop after `backoff.attempts` calls, whatever `decide` returns.
pub async fn with_retries<T, E, F, Fut>(
    backoff: Backoff,
    decide: impl Fn(&E) -> Decision,
    call: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    retry_loop(backoff, decide, |_, _| {}, call).await
}

/// Runs `call` like [`with_retries`] and counts each completed wait in milliseconds.
///
/// A [`Decision::Throttled`] or [`Decision::Wait`] wait goes to
/// [`Metrics::throttling_msecs`], which runners read to hold back autoscaling. A
/// [`Decision::Backoff`] wait goes to [`Metrics::retry_wait_msecs`] in `namespace`.
pub async fn with_throttled_retries<T, E, F, Fut>(
    backoff: Backoff,
    namespace: &'static str,
    decide: impl Fn(&E) -> Decision,
    call: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    retry_loop(
        backoff,
        decide,
        |decision, wait| {
            if decision.is_throttling() {
                inc_ms(&Metrics::throttling_msecs(), millis(wait));
            } else {
                inc_ms(&Metrics::retry_wait_msecs(namespace), millis(wait));
            }
        },
        call,
    )
    .await
}

async fn retry_loop<T, E, F, Fut>(
    backoff: Backoff,
    decide: impl Fn(&E) -> Decision,
    mut on_wait: impl FnMut(Decision, Duration),
    mut call: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 0;
    loop {
        let error = match call().await {
            Ok(value) => return Ok(value),
            Err(error) if !backoff.allows_retry(attempt) => return Err(error),
            Err(error) => error,
        };
        let decision = decide(&error);
        let Some(wait) = decision.wait(backoff, attempt) else {
            return Err(error);
        };
        tokio::time::sleep(wait).await;
        // Count the wait after the sleep, so a dropped future does not count it.
        on_wait(decision, wait);
        attempt += 1;
    }
}

/// Runs `call` like [`with_retries`] and adds each completed wait to `waits`. Use it when the
/// calls run on a background task: then [`RetryWaits::record`] reports the waits on the
/// transform thread.
pub async fn with_retry_waits<T, E, F, Fut>(
    backoff: Backoff,
    waits: &RetryWaits,
    decide: impl Fn(&E) -> Decision,
    call: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    retry_loop(
        backoff,
        decide,
        |decision, wait| waits.add(decision, wait),
        call,
    )
    .await
}

/// Retry wait totals in milliseconds. A background task adds the waits, and the transform
/// records them on its own thread, so that the metrics go to the transform.
///
/// Each class (throttling, retry wait) counts the union of its wait intervals. Waits of
/// concurrent tasks that overlap count once, so a total never passes the wall time.
#[derive(Debug)]
pub struct RetryWaits {
    /// The instant that the interval bounds count from.
    origin: tokio::time::Instant,
    throttled: WaitTotal,
    retry: WaitTotal,
}

impl Default for RetryWaits {
    fn default() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
            throttled: WaitTotal::default(),
            retry: WaitTotal::default(),
        }
    }
}

/// The counted milliseconds of one wait class, and the end of the last counted interval.
#[derive(Debug, Default)]
struct WaitTotal {
    ms: AtomicU64,
    end_ms: AtomicU64,
}

impl WaitTotal {
    /// Adds the part of `start_ms..end_ms` that starts after the last counted interval. An
    /// interval that ends before it counts nothing, so the total can be low but never high.
    fn add(&self, start_ms: u64, end_ms: u64) {
        let previous_end = self.end_ms.fetch_max(end_ms, Ordering::Relaxed);
        let from = start_ms.max(previous_end);
        if end_ms > from {
            self.ms.fetch_add(end_ms - from, Ordering::Relaxed);
        }
    }
}

impl RetryWaits {
    /// Adds a completed `wait` that `decision` gave. The wait ends now.
    pub fn add(&self, decision: Decision, wait: Duration) {
        let end = self.now_ms();
        self.total(decision)
            .add(end.saturating_sub(millis(wait)), end);
    }

    /// Adds a `wait` that `decision` gave and that starts now, before the sleep. Use it when
    /// the caller cannot see the end of the sleep. A cancelled sleep still counts.
    pub fn add_upcoming(&self, decision: Decision, wait: Duration) {
        let start = self.now_ms();
        self.total(decision)
            .add(start, start.saturating_add(millis(wait)));
    }

    fn total(&self, decision: Decision) -> &WaitTotal {
        if decision.is_throttling() {
            &self.throttled
        } else {
            &self.retry
        }
    }

    /// Milliseconds since `origin`, plus an offset so that a wait that started before
    /// `origin` keeps its start.
    fn now_ms(&self) -> u64 {
        const OFFSET_MS: u64 = 1 << 40;
        OFFSET_MS + millis(tokio::time::Instant::now().saturating_duration_since(self.origin))
    }

    /// Moves the totals to [`Metrics::throttling_msecs`] and to
    /// [`Metrics::retry_wait_msecs`] of `namespace`.
    pub fn record(&self, namespace: &'static str) {
        inc_ms(
            &Metrics::throttling_msecs(),
            self.throttled.ms.swap(0, Ordering::Relaxed),
        );
        inc_ms(
            &Metrics::retry_wait_msecs(namespace),
            self.retry.ms.swap(0, Ordering::Relaxed),
        );
    }
}

fn millis(wait: Duration) -> u64 {
    u64::try_from(wait.as_millis()).unwrap_or(u64::MAX)
}

fn inc_ms(counter: &crate::metrics::Counter, ms: u64) {
    if ms > 0 {
        counter.inc_by(i64::try_from(ms).unwrap_or(i64::MAX));
    }
}
