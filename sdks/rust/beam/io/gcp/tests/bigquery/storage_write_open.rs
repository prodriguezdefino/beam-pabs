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

//! The retry decision for the open of a stream, against a local fake of the BigQuery Write
//! service that answers `NOT_FOUND` as for a table that does not exist yet.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use beam::internals::{MetricsContainer, MetricsScope};
use beam::metrics::{
    MetricResults, RETRY_WAIT_COUNTER_NAME, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE,
};
use beam::retry::{Decision, RetryWaits};
use beam_gcp_core::retry::observe_retries;
use beam_io_arrow::arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use beam_io_gcp::bigquery::write::client::{is_not_found, open_decision};
use beam_io_gcp::bigquery::write::encode::schema_bytes;
use beam_io_gcp::runtime::{block_on_async, client_runtime};
use bytes::Bytes;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::error::WriterBuilderError;
use google_cloud_bigquery::model::ArrowSchema as WriterSchema;
use google_cloud_bigquery::write::retry_policy::RetryableErrors;
use google_cloud_gax::backoff_policy::BackoffPolicy;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};
use google_cloud_gax::retry_policy::{RetryPolicy, RetryPolicyExt as _};
use google_cloud_gax::retry_state::RetryState;
use tonic::Status;
use tonic::codegen::{BoxFuture, http};
use tonic::server::NamedService;

const TABLE: &str = "projects/p/datasets/d/tables/t";
const SERVICE: &str = "google.cloud.bigquery.storage.v1.BigQueryWrite";

/// A fake of the BigQuery Write service that answers `status` to each call and counts calls.
#[derive(Clone)]
struct FixedStatus {
    calls: Arc<AtomicU32>,
    status: Status,
}

impl NamedService for FixedStatus {
    const NAME: &'static str = SERVICE;
}

impl<B> tonic::codegen::Service<http::Request<B>> for FixedStatus
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Infallible>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: http::Request<B>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let status = self.status.clone();
        Box::pin(async move { Ok(status.into_http()) })
    }
}

/// Opens the default stream of [`TABLE`] against the fake with the retry policy of the default
/// writer and a short time limit. Returns the error of the open and the count of calls that
/// the fake received.
fn open_default_stream(status: Status) -> (WriterBuilderError, u32) {
    let calls = Arc::new(AtomicU32::new(0));
    let runtime = client_runtime().expect("starts the client runtime");
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("binds a local port");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("the listener has an address")
    );
    let server = runtime.spawn(
        tonic::transport::Server::builder()
            .add_service(FixedStatus {
                calls: Arc::clone(&calls),
                status,
            })
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let columns = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int64, true)]);
    let schema = WriterSchema::new().set_serialized_schema(Bytes::from(
        schema_bytes(&columns).expect("encodes the schema"),
    ));
    let waits = Arc::new(RetryWaits::default());
    let opened = block_on_async(async move {
        let client = Write::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .map_err(std::io::Error::other)?;
        let (retry, backoff) = observe_retries(
            RetryableErrors.with_time_limit(Duration::from_secs(1)),
            FixedBackoff(Duration::from_millis(10)),
            waits,
        );
        let open = client
            .open_default_stream(TABLE)
            .with_multiplexing(true)
            .with_retry_policy(retry)
            .with_backoff_policy(backoff)
            .with_attempt_timeout(Duration::from_secs(5))
            .build_arrow(schema);
        tokio::time::timeout(Duration::from_secs(30), open)
            .await
            .map_err(std::io::Error::other)
    })
    .expect("the open ends in time");
    server.abort();
    match opened {
        Ok(_) => panic!("the open of a table that does not exist succeeds"),
        Err(error) => (error, calls.load(Ordering::Relaxed)),
    }
}

/// A new table can answer `NOT_FOUND` to the open of its default stream for a short time. The
/// open waits and tries again.
#[test]
fn not_found_on_the_open_of_a_stream_is_retried() {
    let (error, calls) = open_default_stream(Status::not_found("no table"));
    assert_eq!(
        calls, 1,
        "only the open loop of the writer retries the open"
    );
    assert!(is_not_found(&error), "{error:?}");
    let codes = match &error {
        WriterBuilderError::Rpc { source, .. } => Some((
            source.http_status_code(),
            source.status().map(|status| status.code),
        )),
        _ => None,
    };
    assert_eq!(
        open_decision(&error),
        Decision::Backoff,
        "{error:?} with (HTTP status, gRPC code) {codes:?}"
    );
}

#[test]
fn resource_exhausted_on_open_is_classified_as_throttling() {
    let (error, calls) = open_default_stream(Status::resource_exhausted("quota exceeded"));
    assert_eq!(
        calls, 1,
        "only the open loop of the writer retries the open"
    );
    assert!(!is_not_found(&error), "{error:?}");
    assert_eq!(open_decision(&error), Decision::Throttled, "{error:?}");
}

#[derive(Debug)]
struct FixedBackoff(Duration);

impl BackoffPolicy for FixedBackoff {
    fn on_failure(&self, _state: &RetryState) -> Duration {
        self.0
    }
}

#[test]
fn default_stream_retry_policy_observes_resource_exhausted_as_throttling() {
    let waits = Arc::new(RetryWaits::default());
    let (retry, backoff) = observe_retries(
        RetryableErrors.with_time_limit(Duration::from_secs(30)),
        FixedBackoff(Duration::from_millis(45)),
        Arc::clone(&waits),
    );
    let state = RetryState::new(true);

    let exhausted = GaxError::service(
        RpcStatus::default()
            .set_code(Code::ResourceExhausted)
            .set_message("quota"),
    );
    assert!(retry.on_error(&state, exhausted).is_continue());
    assert_eq!(backoff.on_failure(&state), Duration::from_millis(45));

    let unavailable = GaxError::service(
        RpcStatus::default()
            .set_code(Code::Unavailable)
            .set_message("transient"),
    );
    assert!(retry.on_error(&state, unavailable).is_continue());
    assert_eq!(backoff.on_failure(&state), Duration::from_millis(45));

    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("WriteDefault"));
        waits.record("bigquery");
    }
    let metrics = MetricResults::from_container(&container);
    assert_eq!(
        metrics.counter_for_transform(
            "WriteDefault",
            THROTTLE_TIME_NAMESPACE,
            THROTTLE_TIME_COUNTER_NAME,
        ),
        Some(45)
    );
    assert_eq!(
        metrics.counter_for_transform("WriteDefault", "bigquery", RETRY_WAIT_COUNTER_NAME),
        Some(45)
    );
}
