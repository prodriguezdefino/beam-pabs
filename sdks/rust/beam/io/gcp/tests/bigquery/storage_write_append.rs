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

use beam_io_gcp::bigquery::write::append::{Failure, append_at};
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

/// Runs an append that fails twice with `error` and then succeeds, and returns the metrics of
/// the transform.
async fn append_after_two_failures(
    error: impl Fn() -> google_cloud_gax::error::Error,
) -> beam::metrics::MetricResults {
    use std::sync::Arc;

    use beam::internals::{MetricsContainer, MetricsScope};
    use beam::metrics::MetricResults;

    let calls = AtomicU32::new(0);
    let container = Arc::new(MetricsContainer::new());
    let result = {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("Write"));
        append_at(
            |_, _| {
                let failed = calls.fetch_add(1, Ordering::Relaxed) < 2;
                let result = if failed {
                    Err(AppendError::from(error()))
                } else {
                    Ok(())
                };
                async move { result }
            },
            &ArrowRecordBatch::default(),
            0,
            false,
            Duration::from_secs(5),
        )
        .await
    };
    assert!(result.is_ok(), "{result:?}");
    MetricResults::from_container(&container)
}

#[tokio::test(start_paused = true)]
async fn transient_failures_record_append_retries_and_retry_waits() {
    use beam::metrics::{
        RETRY_WAIT_COUNTER_NAME, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE,
    };
    use google_cloud_gax::error::Error as GaxError;

    let metrics = append_after_two_failures(|| GaxError::io("transient socket reset")).await;
    assert_eq!(
        metrics.counter_for_transform("Write", "bigquery", "append_retries"),
        Some(2)
    );
    let waited = metrics
        .counter_for_transform("Write", "bigquery", RETRY_WAIT_COUNTER_NAME)
        .expect("retry_wait_msecs recorded");
    assert!((1_500..=3_000).contains(&waited), "{waited}");
    assert_eq!(
        metrics.counter_for_transform("Write", THROTTLE_TIME_NAMESPACE, THROTTLE_TIME_COUNTER_NAME),
        None,
        "a socket reset is not server push-back"
    );
}

#[tokio::test(start_paused = true)]
async fn resource_exhausted_records_throttling_msecs() {
    use beam::metrics::{
        RETRY_WAIT_COUNTER_NAME, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE,
    };
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status};

    let metrics = append_after_two_failures(|| {
        GaxError::service(Status::default().set_code(Code::ResourceExhausted))
    })
    .await;
    let throttled = metrics
        .counter_for_transform("Write", THROTTLE_TIME_NAMESPACE, THROTTLE_TIME_COUNTER_NAME)
        .expect("throttling_msecs recorded");
    assert!((1_500..=3_000).contains(&throttled), "{throttled}");
    assert_eq!(
        metrics.counter_for_transform("Write", "bigquery", RETRY_WAIT_COUNTER_NAME),
        None
    );
}
