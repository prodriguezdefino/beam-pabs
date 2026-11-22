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

//! Transient-error classification, status names and retry wait observers for
//! `google-cloud-gax` RPC and transport errors.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use beam::metrics::{grpc_status, http_status};
use beam::retry::{Decision, RetryWaits};
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::{Code, StatusDetails};
use google_cloud_gax::retry_policy::RetryPolicy;
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;

/// The time limit of the client retries of one Google Cloud call.
pub const RETRY_TIME_LIMIT: Duration = Duration::from_secs(300);

/// Returns how to retry `error`: the [`push_back`] decision, else [`Decision::Backoff`] when
/// `error` is transient, else [`Decision::Stop`].
pub fn transient_decision(error: &Error) -> Decision {
    if is_transient(error) {
        push_back(error).unwrap_or(Decision::Backoff)
    } else {
        Decision::Stop
    }
}

/// Returns the decision for server push-back: [`Decision::Wait`] for a `RetryInfo` delay,
/// [`Decision::Throttled`] for `RESOURCE_EXHAUSTED` or HTTP 429, else `None`.
pub fn push_back(error: &Error) -> Option<Decision> {
    if let Some(status) = error.status() {
        let delay = status.details.iter().find_map(|detail| match detail {
            StatusDetails::RetryInfo(info) => info
                .retry_delay
                .and_then(|delay| Duration::try_from(delay).ok()),
            _ => None,
        });
        match (delay, status.code) {
            (Some(delay), _) => return Some(Decision::Wait(delay)),
            (None, Code::ResourceExhausted) => return Some(Decision::Throttled),
            (None, _) => {}
        }
    }
    (error.http_status_code() == Some(429)).then_some(Decision::Throttled)
}

/// Returns whether a `google-cloud-gax` error is transient and safe to retry.
pub fn is_transient(error: &Error) -> bool {
    // HTTP status errors in `google-cloud-gax` also report `is_transport()`, so classify
    // errors with a service or HTTP status before falling back to transport errors.
    match (error.status(), error.http_status_code()) {
        (Some(status), _) => is_transient_code(status.code),
        (None, Some(code)) => is_transient_http(code),
        (None, None) => error.is_transport() || error.is_io() || error.is_timeout(),
    }
}

fn is_transient_http(code: u16) -> bool {
    matches!(code, 408 | 429 | 500 | 502 | 503 | 504)
}

/// Returns whether an RPC [`Code`] is transient.
pub fn is_transient_code(code: Code) -> bool {
    matches!(
        code,
        Code::Aborted
            | Code::Cancelled
            | Code::DeadlineExceeded
            | Code::Internal
            | Code::ResourceExhausted
            | Code::Unavailable
            | Code::Unknown
    )
}

/// Returns the status name of `error` for the I/O request metrics.
pub fn status_name(error: &Error) -> &'static str {
    match (error.status(), error.http_status_code()) {
        // The discriminants of `Code` are the gRPC status codes.
        (Some(status), _) => grpc_status(status.code as i32),
        (None, Some(code)) => http_status(code),
        (None, None) if error.is_timeout() => "deadline_exceeded",
        (None, None) => "unknown",
    }
}

/// The wait class of an approved retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WaitClass {
    Throttling,
    RetryWait,
}

impl WaitClass {
    fn of(error: &Error) -> Self {
        if push_back(error).is_some() {
            Self::Throttling
        } else {
            Self::RetryWait
        }
    }

    fn decision(self) -> Decision {
        match self {
            Self::Throttling => Decision::Throttled,
            Self::RetryWait => Decision::Backoff,
        }
    }
}

/// The approved retries of each wait class whose backoff has not run yet. Each approved
/// retry gives one backoff, so concurrent retry loops that share one policy keep their counts.
#[derive(Debug)]
struct PendingRetries {
    waits: Arc<RetryWaits>,
    throttling: AtomicU32,
    retry_wait: AtomicU32,
}

impl PendingRetries {
    fn new(waits: Arc<RetryWaits>) -> Self {
        Self {
            waits,
            throttling: AtomicU32::new(0),
            retry_wait: AtomicU32::new(0),
        }
    }

    fn count(&self, class: WaitClass) -> &AtomicU32 {
        match class {
            WaitClass::Throttling => &self.throttling,
            WaitClass::RetryWait => &self.retry_wait,
        }
    }

    fn approve(&self, error: &Error) {
        self.count(WaitClass::of(error))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Takes back an approval that gives no backoff, for a throttle that ends the loop.
    fn withdraw(&self, error: &Error) {
        take_one(self.count(WaitClass::of(error)));
    }

    /// Records `delay` in the class of one pending approval, throttling first. A backoff
    /// without an approval records nothing.
    fn record_wait(&self, delay: Duration) {
        if let Some(class) = [WaitClass::Throttling, WaitClass::RetryWait]
            .into_iter()
            .find(|&class| take_one(self.count(class)))
        {
            self.waits.add_upcoming(class.decision(), delay);
        }
    }
}

/// Decrements `count` unless it is zero. Returns whether it did.
fn take_one(count: &AtomicU32) -> bool {
    count
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
        .is_ok()
}

/// Wraps a `google-cloud-gax` [`RetryPolicy`] to classify approved retries as throttling or
/// backoff waits.
#[derive(Debug)]
struct ObservedRetryPolicy<P> {
    inner: P,
    pending: Arc<PendingRetries>,
}

impl<P: RetryPolicy> RetryPolicy for ObservedRetryPolicy<P> {
    fn on_error(&self, state: &RetryState, error: Error) -> RetryResult {
        let result = self.inner.on_error(state, error);
        match &result {
            RetryResult::Continue(err) => self.pending.approve(err),
            RetryResult::Permanent(_) | RetryResult::Exhausted(_) => {}
        }
        result
    }

    fn on_throttle(&self, state: &RetryState, error: Error) -> ThrottleResult {
        let result = self.inner.on_throttle(state, error);
        match &result {
            ThrottleResult::Continue(err) => self.pending.approve(err),
            ThrottleResult::Exhausted(err) => self.pending.withdraw(err),
        }
        result
    }

    fn remaining_time(&self, state: &RetryState) -> Option<Duration> {
        self.inner.remaining_time(state)
    }
}

/// Wraps a `google-cloud-gax` [`BackoffPolicy`] to record each retry delay into [`RetryWaits`].
#[derive(Debug)]
struct ObservedBackoffPolicy<B> {
    inner: B,
    pending: Arc<PendingRetries>,
}

impl<B: BackoffPolicy> BackoffPolicy for ObservedBackoffPolicy<B> {
    fn on_failure(&self, state: &RetryState) -> Duration {
        let delay = self.inner.on_failure(state);
        self.pending.record_wait(delay);
        delay
    }
}

/// Wraps `retry` and `backoff` so retry delays accumulate into `waits` as throttling or retry
/// waits. A delay counts when the backoff gives it, before the sleep. Call
/// [`RetryWaits::record`] on the transform thread to report the waits.
pub fn observe_retries<P, B>(
    retry: P,
    backoff: B,
    waits: Arc<RetryWaits>,
) -> (impl RetryPolicy, impl BackoffPolicy)
where
    P: RetryPolicy,
    B: BackoffPolicy,
{
    let pending = Arc::new(PendingRetries::new(waits));
    (
        ObservedRetryPolicy {
            inner: retry,
            pending: Arc::clone(&pending),
        },
        ObservedBackoffPolicy {
            inner: backoff,
            pending,
        },
    )
}
