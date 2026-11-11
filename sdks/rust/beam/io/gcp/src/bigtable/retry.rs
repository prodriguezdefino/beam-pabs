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
//! [`decision`] reads the status of a failed call. A `google.rpc.RetryInfo` delay or
//! `RESOURCE_EXHAUSTED` is server push-back, which counts as throttling. Another transient
//! error waits for the backoff of [`DEFAULT_RETRY`]. Each retry logs a warning.
//!
//! The codes differ from `beam_gcp_core::retry`, which works on `google-cloud-gax` errors and
//! retries every `CANCELLED` and `UNKNOWN` status.

use std::error::Error as _;
use std::time::Duration;

use beam::retry::{Backoff, Decision, RetryWaits};
use beam_io_gcp_googleapis::google::rpc::RetryInfo;
use prost::Message as _;
use tonic::{Code, Status};

/// Type URL of the `google.rpc.RetryInfo` detail in a `google.rpc.Status`.
const RETRY_INFO_TYPE: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// Default retry settings for read and write RPCs: 11 retries.
pub const DEFAULT_RETRY: Backoff = Backoff {
    attempts: 12,
    initial: Duration::from_millis(200),
    max: Duration::from_secs(60),
};

/// Returns how to retry a call that failed with `status`: [`Decision::Wait`] for a
/// `RetryInfo` delay, [`Decision::Throttled`] for `RESOURCE_EXHAUSTED`, [`Decision::Backoff`]
/// for another transient status or a [closed connection](is_closed_connection), else
/// [`Decision::Stop`].
pub fn decision(status: &Status) -> Decision {
    if !is_transient(status.code()) && !is_closed_connection(status) {
        return Decision::Stop;
    }
    if let Some(delay) = server_retry_delay(status) {
        return Decision::Wait(delay);
    }
    if status.code() == Code::ResourceExhausted {
        Decision::Throttled
    } else {
        Decision::Backoff
    }
}

/// Returns whether `status` is `CANCELLED` because the transport connection closed. Only a
/// local transport error gives a status a source, so a `CANCELLED` from the server stays final.
pub fn is_closed_connection(status: &Status) -> bool {
    status.code() == Code::Cancelled && status.source().is_some()
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

/// Logs a warning for retry `attempt` of `call` on `resource`, waits as `decision` gives,
/// and adds the wait to `waits`. [`Decision::Stop`] does not wait.
pub async fn wait_before_retry(
    policy: Backoff,
    attempt: u32,
    decision: Decision,
    waits: &RetryWaits,
    call: &str,
    resource: &str,
    cause: &str,
) {
    let Some(wait) = decision.wait(policy, attempt) else {
        return;
    };
    tracing::warn!(
        "Retrying Bigtable {call} on {resource} (retry {} of {}) in {} ms: {cause}",
        attempt + 1,
        policy.retries(),
        wait.as_millis()
    );
    tokio::time::sleep(wait).await;
    waits.add(decision, wait);
}
