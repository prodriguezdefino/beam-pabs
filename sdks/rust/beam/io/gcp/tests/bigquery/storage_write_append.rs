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

//! The append at an offset that the pending and buffered writers share: its time limit and the
//! failures that ask for a new connection.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use gcp::bigquery::storage::write::append::{Failure, append_at};
use google_cloud_bigquery::error::AppendError;
use google_cloud_bigquery::model::ArrowRecordBatch;

const TIMEOUT: Duration = Duration::from_millis(200);

#[tokio::test]
async fn an_append_without_answer_fails_as_connection() {
    let result = append_at(
        |_, _| std::future::pending::<Result<(), AppendError>>(),
        &ArrowRecordBatch::default(),
        7,
        false,
        TIMEOUT,
    )
    .await;
    assert!(matches!(&result, Err(Failure::Connection(_))), "{result:?}");
    assert!(result.is_err_and(|failure| failure.needs_new_connection()));
}

#[tokio::test]
async fn a_closed_connection_fails_at_once() {
    let calls = AtomicU32::new(0);
    let result = append_at(
        |_, _| {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err::<(), _>(AppendError::UnexpectedEndOfStream) }
        },
        &ArrowRecordBatch::default(),
        0,
        false,
        TIMEOUT,
    )
    .await;
    assert!(matches!(result, Err(Failure::Connection(_))), "{result:?}");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn an_answer_in_time_succeeds_at_the_given_offset() {
    let result = append_at(
        |_, offset| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(offset, 3);
            Ok::<_, AppendError>(())
        },
        &ArrowRecordBatch::default(),
        3,
        false,
        TIMEOUT,
    )
    .await;
    assert!(result.is_ok(), "{result:?}");
}
