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

//! The process-wide clients of the write, and the retry rules that all its calls share.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use beam::retry::{Backoff, Decision};
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::error::WriterBuilderError;
use google_cloud_bigquery_v2::client::TableService;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use tokio::sync::OnceCell;

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

static WRITE_CLIENT: OnceCell<Arc<Write>> = OnceCell::const_new();
static TABLE_CLIENT: OnceCell<TableService> = OnceCell::const_new();

/// Returns `given`, or the process-wide Storage Write client. Call it on the client runtime
/// only. The client multiplexes the default streams of all writers on shared connections.
pub(super) async fn write_client(given: Option<Arc<Write>>) -> io::Result<Arc<Write>> {
    match given {
        Some(client) => Ok(client),
        None => WRITE_CLIENT
            .get_or_try_init(|| async {
                Write::builder()
                    .build()
                    .await
                    .map(Arc::new)
                    .map_err(io::Error::other)
            })
            .await
            .cloned(),
    }
}

/// Returns `given`, or the process-wide tables client. Call it on the client runtime only.
pub(super) async fn table_client(given: Option<TableService>) -> io::Result<TableService> {
    match given {
        Some(client) => Ok(client),
        None => TABLE_CLIENT
            .get_or_try_init(|| async {
                TableService::builder()
                    .build()
                    .await
                    .map_err(io::Error::other)
            })
            .await
            .cloned(),
    }
}

/// Retries an error that [`is_transient`] accepts.
pub(super) fn transient_decision(error: &Error) -> Decision {
    match is_transient(error) {
        true => Decision::Backoff,
        false => Decision::Stop,
    }
}

/// Retries the open of a stream after a transient error or `NOT_FOUND`. A new table can
/// answer `NOT_FOUND` for a short time.
pub(super) fn open_decision(error: &WriterBuilderError) -> Decision {
    match error {
        WriterBuilderError::Rpc { source, .. }
            if is_transient(source) || source.http_status_code() == Some(404) =>
        {
            Decision::Backoff
        }
        _ => Decision::Stop,
    }
}

/// Whether a later call can succeed.
pub(super) fn is_transient(error: &Error) -> bool {
    error.is_transport()
        || error.is_io()
        || error.is_timeout()
        || error
            .status()
            .is_some_and(|status| is_transient_code(status.code))
}

pub(super) fn is_transient_code(code: Code) -> bool {
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
