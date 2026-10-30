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

//! Retry policy of the Bigtable calls and the wait before each retry.
//!
//! When the error of a call has a `google.rpc.RetryInfo` detail, the client waits for the
//! delay in it. Else it waits for the exponential backoff of [`RetryPolicy`]. Each retry logs
//! a warning.

use std::time::Duration;

use googleapis::google::rpc::RetryInfo;
use prost::Message as _;
use tonic::{Code, Status};

/// Type URL of the `google.rpc.RetryInfo` detail in a `google.rpc.Status`.
const RETRY_INFO_TYPE: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// Exponential backoff settings for Bigtable gRPC calls.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl RetryPolicy {
    /// Returns the backoff delay after `attempt` (0-indexed).
    pub fn backoff(self, attempt: u32) -> Duration {
        let factor = 1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX);
        self.initial_backoff
            .saturating_mul(factor)
            .min(self.max_backoff)
    }

    /// Returns the wait before retry `attempt`: the delay of the server when `status` has a
    /// `RetryInfo` detail, else [`backoff`](Self::backoff).
    pub fn delay(self, attempt: u32, status: Option<&Status>) -> Duration {
        status
            .and_then(server_retry_delay)
            .unwrap_or_else(|| self.backoff(attempt))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        DEFAULT_RETRY
    }
}

/// Default retry policy for read and write RPCs.
pub const DEFAULT_RETRY: RetryPolicy = RetryPolicy {
    attempts: 10,
    initial_backoff: Duration::from_millis(200),
    max_backoff: Duration::from_secs(60),
};

/// Returns whether a gRPC status code is transient and safe to retry on Bigtable.
pub fn is_transient(code: Code) -> bool {
    matches!(
        code,
        Code::Aborted
            | Code::DeadlineExceeded
            | Code::Internal
            | Code::ResourceExhausted
            | Code::Unavailable
    )
}

/// Returns whether a raw `google.rpc.Status.code` is transient and safe to retry on Bigtable.
pub fn is_transient_code(code: i32) -> bool {
    is_transient(Code::from_i32(code))
}

/// Returns the delay of the `google.rpc.RetryInfo` detail of `status`, if it has one.
pub fn server_retry_delay(status: &Status) -> Option<Duration> {
    let details = googleapis::google::rpc::Status::decode(status.details()).ok()?;
    details
        .details
        .iter()
        .find(|any| any.type_url == RETRY_INFO_TYPE)
        .and_then(|any| RetryInfo::decode(any.value.as_ref()).ok())
        .and_then(|info| info.retry_delay)
        .and_then(|delay| {
            let seconds = u64::try_from(delay.seconds).ok()?;
            let nanos = u32::try_from(delay.nanos).ok()?;
            Some(Duration::new(seconds, nanos))
        })
}

/// Logs a warning for retry `attempt` of `call` on `resource`, then waits for
/// [`RetryPolicy::delay`]. `cause` describes the error. `status` is the gRPC error, if any.
pub async fn wait_before_retry(
    policy: RetryPolicy,
    attempt: u32,
    call: &str,
    resource: &str,
    cause: &str,
    status: Option<&Status>,
) {
    let delay = policy.delay(attempt, status);
    tracing::warn!(
        "Retrying Bigtable {call} on {resource} (attempt {} of {}) in {} ms: {cause}",
        attempt + 1,
        policy.attempts,
        delay.as_millis()
    );
    tokio::time::sleep(delay).await;
}
