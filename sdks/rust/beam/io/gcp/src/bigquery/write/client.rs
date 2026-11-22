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

//! Retry policies and client helpers shared across BigQuery Storage Write streams.
//!
//! The module is public only for the open tests: they check `open_decision` against the
//! errors of a fake Write service, and no transform opens a stream without a real table.

use std::time::Duration;

use beam::retry::{Backoff, Decision};
pub(super) use beam_gcp_core::retry::transient_decision;
use google_cloud_bigquery::error::WriterBuilderError;
use google_cloud_gax::error::rpc::Code;

pub(super) use crate::bigquery::clients::{table_client, write_client};

/// Appends and the opening of a stream: 500 retries, with waits of at most 20 seconds. A new
/// table can answer `NOT_FOUND` for a while, and quota errors pass with time.
pub(super) const APPEND_RETRY: Backoff = Backoff {
    attempts: 501,
    initial: Duration::from_secs(1),
    max: Duration::from_secs(20),
};

/// Finalize, flush and commit: 3 retries, with waits of at most a minute.
pub(super) const CALL_RETRY: Backoff = Backoff {
    attempts: 4,
    initial: Duration::from_secs(1),
    max: Duration::from_secs(60),
};

/// Retries the open of a stream after a transient error or `NOT_FOUND`. A new table can
/// answer `NOT_FOUND` for a short time, which is not throttling.
pub fn open_decision(error: &WriterBuilderError) -> Decision {
    match error {
        WriterBuilderError::Rpc { source, .. } => match transient_decision(source) {
            Decision::Stop if is_not_found(error) => Decision::Backoff,
            decision => decision,
        },
        _ => Decision::Stop,
    }
}

/// Whether the open of a stream failed with `NOT_FOUND`, as for a table that is new.
pub fn is_not_found(error: &WriterBuilderError) -> bool {
    match error {
        // A gRPC error carries the code in its status and has no HTTP status.
        WriterBuilderError::Rpc { source, .. } => {
            source.http_status_code() == Some(404)
                || matches!(
                    source.status().map(|status| status.code),
                    Some(Code::NotFound)
                )
        }
        _ => false,
    }
}
