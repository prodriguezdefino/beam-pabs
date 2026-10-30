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

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use google_cloud_bigquery::client::Write;
use google_cloud_bigquery_v2::client::TableService;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use tokio::sync::OnceCell;

/// How many times a call runs, and the longest wait between two calls. The waits start at one
/// second and double.
#[derive(Clone, Copy, Debug)]
pub(super) struct Retry {
    pub(super) attempts: u32,
    pub(super) max_backoff: Duration,
}

/// Appends and the opening of a stream: 500 retries, with waits of at most 20 seconds. A new
/// table can answer `NOT_FOUND` for a while, and quota errors pass with time.
pub(super) const APPEND_RETRY: Retry = Retry {
    attempts: 501,
    max_backoff: Duration::from_secs(20),
};

/// Finalize, flush and commit: 3 retries, with waits of at most a minute.
pub(super) const CALL_RETRY: Retry = Retry {
    attempts: 4,
    max_backoff: Duration::from_secs(60),
};

impl Retry {
    /// The wait before attempt `attempt + 1`.
    pub(super) fn backoff(&self, attempt: u32) -> Duration {
        Duration::from_secs(1 << attempt.min(16)).min(self.max_backoff)
    }
}

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

/// Calls `call` again after a failure that `retry` accepts, as `policy` allows.
pub(super) async fn with_retries<T, E, F, Fut>(
    policy: Retry,
    retry: impl Fn(&E) -> bool,
    call: F,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 0;
    loop {
        match call().await {
            Err(error) if attempt + 1 < policy.attempts && retry(&error) => {
                tokio::time::sleep(policy.backoff(attempt)).await;
                attempt += 1;
            }
            result => return result,
        }
    }
}
