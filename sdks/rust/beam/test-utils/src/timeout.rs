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

//! Wall-clock deadlines for tests.
//!
//! The Rust test harness has no per-test timeout. If a test blocks forever, `main`
//! never returns, and the test binary keeps its open sockets and files. This can occur
//! in integration tests where a blocking Fn API client waits on a mock server that
//! stops without an error.
//!
//! [`with_timeout`] runs a test with a deadline. If the test does not finish in time,
//! it fails with a clear message, so the binary exits and releases its resources.

use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

/// Default wall-clock deadline of [`with_timeout`]. [`TEST_TIMEOUT_ENV`] overrides it.
///
/// It is long enough for a debug-build integration test on a loaded CI machine. It is
/// short enough to report a deadlock before the job-level timeout.
pub const DEFAULT_TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Environment variable holding whole seconds that override [`DEFAULT_TEST_TIMEOUT`].
pub const TEST_TIMEOUT_ENV: &str = "BEAM_TEST_TIMEOUT_SECS";

/// Returns the timeout duration from [`TEST_TIMEOUT_ENV`] if set, or [`DEFAULT_TEST_TIMEOUT`].
///
/// # Panics
///
/// Panics if [`TEST_TIMEOUT_ENV`] is set but is not a positive whole number, so that
/// a typo does not go unnoticed.
pub fn test_timeout() -> Duration {
    match std::env::var(TEST_TIMEOUT_ENV) {
        Err(_) => DEFAULT_TEST_TIMEOUT,
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(secs) if secs > 0 => Duration::from_secs(secs),
            _ => panic!("{TEST_TIMEOUT_ENV}={raw:?} is not a positive whole number of seconds"),
        },
    }
}

/// Runs `body` within [`test_timeout`].
///
/// See [`with_custom_timeout`] for behavior.
pub fn with_timeout<F, T>(name: &str, body: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    with_custom_timeout(name, test_timeout(), body)
}

/// Runs `body` on a worker thread and panics if it does not finish within `timeout`.
///
/// `name` is used only in the failure message. Use the name of the test function to
/// keep the report clear when many timed tests run in parallel.
///
/// Panics in `body` are re-raised on the calling thread. So `#[should_panic]` and
/// assertion failures behave as they do without the wrapper.
///
/// On timeout, the worker thread is abandoned, not killed, because Rust has no safe way
/// to cancel a thread. The panic fails the test. The abandoned thread stops when the
/// process exits.
pub fn with_custom_timeout<F, T>(name: &str, timeout: Duration, body: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (result_tx, result_rx) = mpsc::channel();
    let worker = thread::Builder::new()
        .name(format!("timed-test-{name}"))
        .spawn(move || {
            // If `body` panics, `result_tx` is dropped without a send. The receiver then
            // sees `Disconnected` immediately and does not wait for the full timeout.
            let _ = result_tx.send(body());
        })
        .unwrap_or_else(|e| panic!("could not spawn worker thread for test `{name}`: {e}"));

    match result_rx.recv_timeout(timeout) {
        Ok(value) => {
            // The worker already sent its result, so this join returns immediately.
            if let Err(panic) = worker.join() {
                std::panic::resume_unwind(panic);
            }
            value
        }
        Err(RecvTimeoutError::Disconnected) => match worker.join() {
            // Re-raise on the test thread, so that the harness reports the original payload.
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(()) => unreachable!("worker thread finished without sending a result"),
        },
        Err(RecvTimeoutError::Timeout) => panic!(
            "test `{name}` did not complete within {timeout:?}; it is most likely blocked on a \
             dependency that never answered (a mock server, a lock, or a channel). Failing here \
             so the test binary exits instead of hanging and leaking its listening sockets."
        ),
    }
}
