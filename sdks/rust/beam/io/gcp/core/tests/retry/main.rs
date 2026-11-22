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

//! Tests for GCP retry classification and `google_cloud_gax` retry-wait observation.

use std::sync::Arc;
use std::time::Duration;

use beam::internals::{MetricsContainer, MetricsScope};
use beam::metrics::{
    MetricResults, RETRY_WAIT_COUNTER_NAME, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE,
};
use beam::retry::{Decision, RetryWaits};
use beam_gcp_core::retry::{is_transient, observe_retries, push_back, transient_decision};
use bytes::Bytes;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::retry_policy::{Aip194Strict, AlwaysRetry, RetryPolicy, RetryPolicyExt as _};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;
use http::HeaderMap;

#[derive(Debug)]
struct FixedBackoff(Duration);

impl BackoffPolicy for FixedBackoff {
    fn on_failure(&self, _state: &RetryState) -> Duration {
        self.0
    }
}

#[derive(Debug)]
struct ExhaustOnThrottle;

impl RetryPolicy for ExhaustOnThrottle {
    fn on_error(&self, _state: &RetryState, error: GaxError) -> RetryResult {
        RetryResult::Continue(error)
    }

    fn on_throttle(&self, _state: &RetryState, error: GaxError) -> ThrottleResult {
        ThrottleResult::Exhausted(error)
    }
}

fn http_error(status: u16) -> GaxError {
    GaxError::http(status, HeaderMap::new(), Bytes::new())
}

fn rpc_error(code: Code) -> GaxError {
    GaxError::service(Status::default().set_code(code).set_message("err"))
}

#[test]
fn http_429_is_transient_and_classified_as_throttled() {
    let err = http_error(429);
    assert!(is_transient(&err));
    assert_eq!(push_back(&err), Some(Decision::Throttled));
    assert_eq!(transient_decision(&err), Decision::Throttled);

    let exhausted = rpc_error(Code::ResourceExhausted);
    assert!(is_transient(&exhausted));
    assert_eq!(push_back(&exhausted), Some(Decision::Throttled));
    assert_eq!(transient_decision(&exhausted), Decision::Throttled);
}

#[test]
fn http_transient_and_permanent_statuses_map_to_backoff_and_stop() {
    for status in [408, 500, 502, 503, 504] {
        let err = http_error(status);
        assert!(is_transient(&err), "status {status}");
        assert_eq!(push_back(&err), None, "status {status}");
        assert_eq!(
            transient_decision(&err),
            Decision::Backoff,
            "status {status}"
        );
    }
    for status in [400, 401, 403, 404, 409] {
        let err = http_error(status);
        assert!(!is_transient(&err), "status {status}");
        assert_eq!(push_back(&err), None, "status {status}");
        assert_eq!(transient_decision(&err), Decision::Stop, "status {status}");
    }
}

/// Sleeps for `delay` on the paused clock, as the retry loop of the client does.
async fn sleep(delay: Duration) -> Duration {
    tokio::time::sleep(delay).await;
    delay
}

#[tokio::test(start_paused = true)]
async fn observe_retries_records_throttling_and_retry_waits() {
    let waits = Arc::new(RetryWaits::default());
    let (retry, backoff) = observe_retries(
        AlwaysRetry,
        FixedBackoff(Duration::from_millis(25)),
        Arc::clone(&waits),
    );
    let state = RetryState::new(true);

    // HTTP 429 and gRPC RESOURCE_EXHAUSTED report as throttling.
    assert!(retry.on_error(&state, http_error(429)).is_continue());
    assert_eq!(
        sleep(backoff.on_failure(&state)).await,
        Duration::from_millis(25)
    );
    assert!(
        retry
            .on_error(&state, rpc_error(Code::ResourceExhausted))
            .is_continue()
    );
    assert_eq!(
        sleep(backoff.on_failure(&state)).await,
        Duration::from_millis(25)
    );

    // Client-side retry throttling after a 429 stays classified as throttling.
    assert!(matches!(
        retry.on_throttle(&state, http_error(429)),
        ThrottleResult::Continue(_)
    ));
    assert_eq!(
        sleep(backoff.on_failure(&state)).await,
        Duration::from_millis(25)
    );

    // HTTP 503 and gRPC UNAVAILABLE report as non-throttling retry waits.
    assert!(retry.on_error(&state, http_error(503)).is_continue());
    assert_eq!(
        sleep(backoff.on_failure(&state)).await,
        Duration::from_millis(25)
    );
    assert!(
        retry
            .on_error(&state, rpc_error(Code::Unavailable))
            .is_continue()
    );
    assert_eq!(
        sleep(backoff.on_failure(&state)).await,
        Duration::from_millis(25)
    );

    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("GcpStep"));
        waits.record("gcs");
    }
    let metrics = MetricResults::from_container(&container);
    assert_eq!(
        metrics.counter_for_transform(
            "GcpStep",
            THROTTLE_TIME_NAMESPACE,
            THROTTLE_TIME_COUNTER_NAME,
        ),
        Some(75)
    );
    assert_eq!(
        metrics.counter_for_transform("GcpStep", "gcs", RETRY_WAIT_COUNTER_NAME),
        Some(50)
    );
}

#[tokio::test(start_paused = true)]
async fn observe_retries_ignores_backoff_after_permanent_or_exhausted() {
    let waits = Arc::new(RetryWaits::default());
    let (retry, backoff) = observe_retries(
        Aip194Strict.with_attempt_limit(1),
        FixedBackoff(Duration::from_millis(30)),
        Arc::clone(&waits),
    );
    let exhausted = RetryState::new(true).set_attempt_count(1_u32);
    assert!(retry.on_error(&exhausted, http_error(429)).is_permanent());

    // A backoff query after a permanent or exhausted decision records no wait.
    assert_eq!(
        backoff.on_failure(&RetryState::new(true)),
        Duration::from_millis(30)
    );

    let (throttler, throttler_backoff) = observe_retries(
        ExhaustOnThrottle,
        FixedBackoff(Duration::from_millis(20)),
        Arc::clone(&waits),
    );
    let state = RetryState::new(true);
    assert!(throttler.on_error(&state, http_error(429)).is_continue());
    assert!(matches!(
        throttler.on_throttle(&state, http_error(429)),
        ThrottleResult::Exhausted(_)
    ));
    assert_eq!(
        throttler_backoff.on_failure(&state),
        Duration::from_millis(20)
    );

    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("GcpStep"));
        waits.record("gcs");
    }
    let metrics = MetricResults::from_container(&container);
    assert_eq!(
        metrics.counter_for_transform(
            "GcpStep",
            THROTTLE_TIME_NAMESPACE,
            THROTTLE_TIME_COUNTER_NAME,
        ),
        None
    );
    assert_eq!(
        metrics.counter_for_transform("GcpStep", "gcs", RETRY_WAIT_COUNTER_NAME),
        None
    );
}

/// Two retry loops share one policy, as the appends of a multiplexed writer do. Each backoff
/// counts in the class of its own approved retry.
#[tokio::test(start_paused = true)]
async fn interleaved_retry_loops_keep_their_wait_classes() {
    let waits = Arc::new(RetryWaits::default());
    let (retry, backoff) = observe_retries(
        AlwaysRetry,
        FixedBackoff(Duration::from_millis(25)),
        Arc::clone(&waits),
    );
    let state = RetryState::new(true);
    assert!(retry.on_error(&state, http_error(429)).is_continue());
    assert!(retry.on_error(&state, http_error(503)).is_continue());
    let throttled_delay = backoff.on_failure(&state);
    let retry_delay = backoff.on_failure(&state);
    sleep(throttled_delay.max(retry_delay)).await;

    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("GcpStep"));
        waits.record("gcs");
    }
    let metrics = MetricResults::from_container(&container);
    assert_eq!(
        metrics.counter_for_transform(
            "GcpStep",
            THROTTLE_TIME_NAMESPACE,
            THROTTLE_TIME_COUNTER_NAME,
        ),
        Some(25)
    );
    assert_eq!(
        metrics.counter_for_transform("GcpStep", "gcs", RETRY_WAIT_COUNTER_NAME),
        Some(25)
    );
}
