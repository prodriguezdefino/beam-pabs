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

//! Tests for the retry wait: the `RetryInfo` delay of the server and the backoff fallback.

use std::time::Duration;

use gcp::bigtable::proto::{Any, Duration as ProtoDuration, RetryInfo, Status as RpcStatus};
use gcp::bigtable::retry::{DEFAULT_RETRY, server_retry_delay};
use prost::Message as _;
use tonic::{Code, Status};

const RETRY_INFO_TYPE: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// Builds an `UNAVAILABLE` status whose details hold `details`.
fn status_with(details: Vec<Any>) -> Status {
    let rpc = RpcStatus {
        code: Code::Unavailable as i32,
        message: "try later".to_string(),
        details,
    };
    Status::with_details(Code::Unavailable, "try later", rpc.encode_to_vec().into())
}

fn retry_info(seconds: i64, nanos: i32) -> Any {
    let info = RetryInfo {
        retry_delay: Some(ProtoDuration { seconds, nanos }),
    };
    Any {
        type_url: RETRY_INFO_TYPE.to_string(),
        value: info.encode_to_vec().into(),
    }
}

#[test]
fn server_delay_comes_from_retry_info() {
    let status = status_with(vec![retry_info(2, 500_000_000)]);
    assert_eq!(
        server_retry_delay(&status),
        Some(Duration::from_millis(2_500))
    );
    assert_eq!(
        DEFAULT_RETRY.delay(3, Some(&status)),
        Duration::from_millis(2_500)
    );
}

#[test]
fn retry_info_is_found_among_other_details() {
    let other = Any {
        type_url: "type.googleapis.com/google.rpc.ErrorInfo".to_string(),
        value: vec![0x0a, 0x01, b'x'].into(),
    };
    let status = status_with(vec![other, retry_info(1, 0)]);
    assert_eq!(server_retry_delay(&status), Some(Duration::from_secs(1)));
}

#[test]
fn backoff_applies_without_retry_info() {
    let plain = Status::unavailable("try later");
    assert_eq!(server_retry_delay(&plain), None);
    assert_eq!(
        DEFAULT_RETRY.delay(2, Some(&plain)),
        DEFAULT_RETRY.backoff(2)
    );
    assert_eq!(DEFAULT_RETRY.delay(2, None), DEFAULT_RETRY.backoff(2));
}

#[test]
fn malformed_details_fall_back_to_backoff() {
    let garbage = Status::with_details(Code::Unavailable, "x", vec![0xff, 0xff, 0xff].into());
    assert_eq!(server_retry_delay(&garbage), None);
    let negative = status_with(vec![retry_info(-1, 0)]);
    assert_eq!(server_retry_delay(&negative), None);
    assert_eq!(
        DEFAULT_RETRY.delay(0, Some(&negative)),
        DEFAULT_RETRY.backoff(0)
    );
}
