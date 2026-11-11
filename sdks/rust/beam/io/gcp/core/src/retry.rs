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

//! Transient-error classification for `google-cloud-gax` RPC and transport errors.

use std::time::Duration;

use beam::retry::Decision;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::{Code, StatusDetails};

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
/// [`Decision::Throttled`] for `RESOURCE_EXHAUSTED`, else `None`.
pub fn push_back(error: &Error) -> Option<Decision> {
    let status = error.status()?;
    let delay = status.details.iter().find_map(|detail| match detail {
        StatusDetails::RetryInfo(info) => info
            .retry_delay
            .and_then(|delay| Duration::try_from(delay).ok()),
        _ => None,
    });
    match (delay, status.code) {
        (Some(delay), _) => Some(Decision::Wait(delay)),
        (None, Code::ResourceExhausted) => Some(Decision::Throttled),
        (None, _) => None,
    }
}

/// Returns whether a `google-cloud-gax` error is transient and safe to retry.
pub fn is_transient(error: &Error) -> bool {
    error.is_transport()
        || error.is_io()
        || error.is_timeout()
        || error
            .status()
            .is_some_and(|status| is_transient_code(status.code))
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
