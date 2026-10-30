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

//! One classification of append failures for all writers, and the append at an offset that
//! the pending and buffered writers share.

use std::future::Future;
use std::time::Duration;

use google_cloud_bigquery::error::AppendError;
use google_cloud_bigquery::model::ArrowRecordBatch;
use google_cloud_gax::error::rpc::Code;

use super::client::{APPEND_RETRY, is_transient};

/// What a failed append means for the writer.
#[derive(Debug)]
pub enum Failure {
    /// BigQuery rejected these rows, by index in the request with the reason, and appended no
    /// row of the request.
    Rows(Vec<(usize, String)>),
    /// The offset of the request is already written.
    Exists(String),
    /// The offset of the request is after the end of the stream.
    OutOfRange(String),
    /// The stream is finalized or does not exist.
    Stream(String),
    /// The connection of the writer gave no answer in time or closed. A call on a new
    /// connection can succeed.
    Connection(String),
    /// A later call can succeed.
    Transient(String),
    /// No later call can succeed.
    Fatal(String),
}

impl Failure {
    pub fn message(&self) -> String {
        match self {
            Self::Rows(rows) => format!("BigQuery rejected {} rows", rows.len()),
            Self::Exists(message)
            | Self::OutOfRange(message)
            | Self::Stream(message)
            | Self::Connection(message)
            | Self::Transient(message)
            | Self::Fatal(message) => message.clone(),
        }
    }

    /// Whether a call on a new connection of the writer can succeed.
    pub fn needs_new_connection(&self) -> bool {
        matches!(self, Self::Connection(_) | Self::Transient(_))
    }
}

impl From<AppendError> for Failure {
    fn from(error: AppendError) -> Self {
        let message = error.to_string();
        match error {
            AppendError::RowErrors { row_errors, .. } => Self::Rows(
                row_errors
                    .into_iter()
                    .map(|row| {
                        (
                            usize::try_from(row.index).unwrap_or(usize::MAX),
                            row.message,
                        )
                    })
                    .collect(),
            ),
            AppendError::UnexpectedEndOfStream => Self::Connection(message),
            AppendError::Rpc { source, .. } => match source.status().map(|status| status.code) {
                Some(Code::AlreadyExists) => Self::Exists(message),
                Some(Code::OutOfRange) => Self::OutOfRange(message),
                Some(Code::NotFound | Code::FailedPrecondition) => Self::Stream(message),
                _ if is_transient(&source) => Self::Transient(message),
                _ => Self::Fatal(message),
            },
            _ => Self::Fatal(message),
        }
    }
}

/// Appends `rows` at `offset` through `send`, and calls again after a transient failure. An
/// `Exists` failure after a transient one means that the first call wrote the rows, so it
/// counts as success. Set `retried` if an earlier call for the same rows failed as transient.
///
/// A call that has no answer after `timeout`, or a closed connection, returns
/// [`Failure::Connection`] at once: a call on the same connection cannot succeed.
pub async fn append_at<Call, Fut, Response>(
    send: Call,
    rows: &ArrowRecordBatch,
    offset: i64,
    mut retried: bool,
    timeout: Duration,
) -> Result<(), Failure>
where
    Call: Fn(ArrowRecordBatch, i64) -> Fut,
    Fut: Future<Output = Result<Response, AppendError>>,
{
    let mut attempt = 0;
    loop {
        // A clone of the request shares its bytes.
        let result = match tokio::time::timeout(timeout, send(rows.clone(), offset)).await {
            Ok(result) => result.map_err(Failure::from),
            Err(_) => Err(Failure::Connection(format!(
                "no answer to the append at offset {offset} in {} s",
                timeout.as_secs()
            ))),
        };
        match result {
            Ok(_) => return Ok(()),
            Err(Failure::Exists(_)) if retried => return Ok(()),
            Err(Failure::Transient(_)) if attempt + 1 < APPEND_RETRY.attempts => {
                retried = true;
                tokio::time::sleep(APPEND_RETRY.backoff(attempt)).await;
                attempt += 1;
            }
            Err(failure) => return Err(failure),
        }
    }
}
