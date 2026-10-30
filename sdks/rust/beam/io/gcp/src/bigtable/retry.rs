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
//! delay in it. Else it waits for the exponential backoff of [`DEFAULT_RETRY`]. Each retry logs
//! a warning.

use std::time::Duration;

use beam::retry::Backoff;
use beam_io_gcp_googleapis::google::rpc::RetryInfo;
use prost::Message as _;
use tonic::{Code, Status};

/// Type URL of the `google.rpc.RetryInfo` detail in a `google.rpc.Status`.
const RETRY_INFO_TYPE: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// Default retry settings for read and write RPCs: 10 retries.
pub const DEFAULT_RETRY: Backoff = Backoff {
    attempts: 11,
    initial: Duration::from_millis(200),
    max: Duration::from_secs(60),
};

/// Returns the wait before retry `attempt`: the delay of the server when `status` has a
/// `RetryInfo` detail, else the [`Backoff::delay`] of `backoff`.
pub fn retry_delay(backoff: Backoff, attempt: u32, status: Option<&Status>) -> Duration {
    status
        .and_then(server_retry_delay)
        .unwrap_or_else(|| backoff.delay(attempt))
}

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
    let details = beam_io_gcp_googleapis::google::rpc::Status::decode(status.details()).ok()?;
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
/// [`retry_delay`]. `cause` describes the error. `status` is the gRPC error, if any.
pub async fn wait_before_retry(
    policy: Backoff,
    attempt: u32,
    call: &str,
    resource: &str,
    cause: &str,
    status: Option<&Status>,
) {
    let delay = retry_delay(policy, attempt, status);
    tracing::warn!(
        "Retrying Bigtable {call} on {resource} (retry {} of {}) in {} ms: {cause}",
        attempt + 1,
        policy.attempts - 1,
        delay.as_millis()
    );
    tokio::time::sleep(delay).await;
}
