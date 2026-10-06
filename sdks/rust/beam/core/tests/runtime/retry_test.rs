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

use std::cell::Cell;
use std::time::Duration;

use beam::internals::block_on_io;
use beam::retry::{Backoff, Decision, with_retries};

const BACKOFF: Backoff = Backoff {
    attempts: 3,
    initial: Duration::from_millis(1),
    max: Duration::from_millis(3),
};

#[test]
fn test_the_base_wait_doubles_up_to_the_max() {
    let bases: Vec<_> = (0..4).map(|attempt| BACKOFF.base(attempt)).collect();
    assert_eq!(bases, [1, 2, 3, 3].map(Duration::from_millis));
    assert_eq!(
        BACKOFF.base(u32::MAX),
        BACKOFF.max,
        "a large attempt does not overflow"
    );
}

#[test]
fn test_a_delay_is_from_half_the_base_to_the_base() {
    for attempt in 0..4 {
        let base = BACKOFF.base(attempt);
        let delay = BACKOFF.delay(attempt);
        assert!(
            (base / 2..=base).contains(&delay),
            "{delay:?} for base {base:?}"
        );
    }
}

#[test]
fn test_attempts_count_the_first_call() {
    assert!(BACKOFF.allows_retry(0));
    assert!(BACKOFF.allows_retry(1));
    assert!(!BACKOFF.allows_retry(2), "3 calls in total");
    assert_eq!(BACKOFF.retries(), 2);
    let none = Backoff {
        attempts: 0,
        ..BACKOFF
    };
    assert_eq!(none.retries(), 0, "no underflow");
}

#[test]
fn test_a_server_wait_is_at_most_the_max() {
    let long = Decision::Wait(Duration::from_secs(60));
    assert_eq!(long.wait(BACKOFF, 0), Some(BACKOFF.max));
    assert_eq!(Decision::Stop.wait(BACKOFF, 0), None);
    assert!(long.is_throttling() && Decision::Throttled.is_throttling());
    assert!(!Decision::Backoff.is_throttling() && !Decision::Stop.is_throttling());
}

#[test]
fn test_with_retries_stops_on_success_on_a_permanent_error_and_after_the_attempts() {
    let calls = Cell::new(0);
    let call = |fail_until: u32, error: &'static str| {
        calls.set(0);
        block_on_io(with_retries(
            BACKOFF,
            |e: &&str| match *e {
                "transient" => Decision::Backoff,
                _ => Decision::Stop,
            },
            || {
                calls.set(calls.get() + 1);
                let result = if calls.get() > fail_until {
                    Ok(calls.get())
                } else {
                    Err(error)
                };
                async move { result }
            },
        ))
        .expect("the async I/O runtime starts")
    };
    assert_eq!(call(1, "transient"), Ok(2), "one retry, then success");
    assert_eq!(call(5, "permanent"), Err("permanent"));
    assert_eq!(calls.get(), 1, "a permanent error is not retried");
    assert_eq!(call(5, "transient"), Err("transient"));
    assert_eq!(calls.get(), 3, "no call after the attempts");
}

#[test]
fn test_a_wait_decision_uses_its_own_delay() {
    let started = std::time::Instant::now();
    let calls = Cell::new(0);
    let result: Result<u32, &str> = block_on_io(with_retries(
        BACKOFF,
        |_| Decision::Wait(Duration::from_millis(3)),
        || {
            calls.set(calls.get() + 1);
            let result = match calls.get() {
                1 => Err("busy"),
                n => Ok(n),
            };
            async move { result }
        },
    ))
    .expect("the async I/O runtime starts");
    assert_eq!(result, Ok(2));
    assert!(
        started.elapsed() >= Duration::from_millis(3),
        "the wait is the server delay"
    );
}

/// Runs `with_throttled_retries` with two failures that `decide` classifies, and returns the
/// throttling and retry wait counters of the transform.
fn throttled_retry_counters(decide: fn(&&str) -> Decision) -> (Option<i64>, Option<i64>) {
    use std::sync::Arc;

    use beam::internals::{MetricsContainer, MetricsScope};
    use beam::metrics::{
        MetricResults, RETRY_WAIT_COUNTER_NAME, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE,
    };
    use beam::retry::with_throttled_retries;

    let container = Arc::new(MetricsContainer::new());
    let calls = Cell::new(0);
    let result: Result<u32, &str> = {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("Write"));
        block_on_io(with_throttled_retries(BACKOFF, "connector", decide, || {
            calls.set(calls.get() + 1);
            let out = match calls.get() {
                1 | 2 => Err("busy"),
                n => Ok(n),
            };
            async move { out }
        }))
        .expect("the async I/O runtime starts")
    };
    assert_eq!(result, Ok(3));
    let results = MetricResults::from_container(&container);
    (
        results.counter_for_transform("Write", THROTTLE_TIME_NAMESPACE, THROTTLE_TIME_COUNTER_NAME),
        results.counter_for_transform("Write", "connector", RETRY_WAIT_COUNTER_NAME),
    )
}

#[test]
fn test_with_throttled_retries_records_server_waits_as_throttling() {
    let (throttled, retry_wait) =
        throttled_retry_counters(|_| Decision::Wait(Duration::from_millis(3)));
    assert_eq!(throttled, Some(6), "two waits of 3 ms, at most the max");
    assert_eq!(retry_wait, None);
}

#[test]
fn test_with_throttled_retries_records_backoff_waits_as_retry_waits() {
    let (throttled, retry_wait) = throttled_retry_counters(|_| Decision::Backoff);
    assert_eq!(throttled, None, "a backoff wait is not server push-back");
    assert!(
        retry_wait.is_some_and(|ms| (1..=5).contains(&ms)),
        "{retry_wait:?}"
    );
}

#[test]
fn test_retry_waits_record_and_reset() {
    use std::sync::Arc;

    use beam::internals::{MetricsContainer, MetricsScope};
    use beam::metrics::{
        MetricResults, RETRY_WAIT_COUNTER_NAME, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE,
    };
    use beam::retry::RetryWaits;

    let container = Arc::new(MetricsContainer::new());
    let waits = RetryWaits::default();
    waits.add(Decision::Throttled, Duration::from_millis(7));
    waits.add(Decision::Backoff, Duration::from_millis(5));
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("Read"));
        waits.record("connector");
        waits.record("connector");
    }
    let results = MetricResults::from_container(&container);
    assert_eq!(
        results.counter_for_transform("Read", THROTTLE_TIME_NAMESPACE, THROTTLE_TIME_COUNTER_NAME),
        Some(7)
    );
    assert_eq!(
        results.counter_for_transform("Read", "connector", RETRY_WAIT_COUNTER_NAME),
        Some(5),
        "a second record adds nothing"
    );
}
