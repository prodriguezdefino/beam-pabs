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

//! I/O request counts and latencies. They record into the process-wide container, which all
//! tests of this binary share, so each test uses its own resource.

use std::sync::Arc;
use std::time::Duration;

use beam_core::coders::VarIntCoder;
use beam_core::internals::{MetricsContainer, MetricsScope, process_wide_container};
use beam_core::metrics::{
    HistogramBuckets, HistogramValue, LABEL_BIGQUERY_TABLE, LABEL_METHOD, LABEL_PTRANSFORM,
    LABEL_RESOURCE, LABEL_SERVICE, LABEL_STATUS, MetricResults, Metrics, TYPE_HISTOGRAM,
    TYPE_SUM_INT64, URN_API_REQUEST_COUNT, URN_API_REQUEST_LATENCIES, grpc_status, http_status,
};
use beam_model::pipeline::MonitoringInfo;

/// The process-wide `MonitoringInfo`s of `resource`.
fn infos_of(resource: &str) -> Vec<MonitoringInfo> {
    process_wide_container()
        .to_monitoring_infos()
        .into_iter()
        .filter(|info| info.labels.get(LABEL_RESOURCE).map(String::as_str) == Some(resource))
        .collect()
}

/// The count of `status` in `infos`.
fn count(infos: &[MonitoringInfo], status: &str) -> Option<i64> {
    infos
        .iter()
        .find(|info| {
            info.urn == URN_API_REQUEST_COUNT
                && info.labels.get(LABEL_STATUS).map(String::as_str) == Some(status)
        })
        .map(|info| VarIntCoder::decode_varint(&mut info.payload.as_slice()).unwrap())
}

fn latencies(infos: &[MonitoringInfo]) -> HistogramValue {
    let info = infos
        .iter()
        .find(|info| info.urn == URN_API_REQUEST_LATENCIES)
        .expect("a latency histogram");
    assert_eq!(info.r#type, TYPE_HISTOGRAM);
    assert!(!info.labels.contains_key(LABEL_STATUS));
    HistogramValue::decode_payload(&info.payload).unwrap()
}

#[test]
fn calls_count_by_status_in_the_process_wide_container_with_the_transform_label() {
    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("ReadStep"));
        let request = Metrics::api_request("BigQuery", "ReadRows", "proj.ds.calls")
            .with_label(LABEL_BIGQUERY_TABLE, "calls");
        request.record("ok", Duration::from_millis(5));
        request.record("ok", Duration::from_millis(30));
        request.record("unavailable", Duration::from_millis(70));
    }

    assert!(
        container.to_monitoring_infos().is_empty(),
        "no bundle metric"
    );
    let infos = infos_of("proj.ds.calls");
    assert_eq!(infos.len(), 3, "one histogram and two counts: {infos:?}");
    assert_eq!(count(&infos, "ok"), Some(2));
    assert_eq!(count(&infos, "unavailable"), Some(1));
    let ok = infos
        .iter()
        .find(|info| info.labels.get(LABEL_STATUS).map(String::as_str) == Some("ok"))
        .unwrap();
    assert_eq!(ok.r#type, TYPE_SUM_INT64);
    let label = |key: &str| ok.labels.get(key).map(String::as_str);
    assert_eq!(label(LABEL_PTRANSFORM), Some("ReadStep"));
    assert_eq!(label(LABEL_SERVICE), Some("BigQuery"));
    assert_eq!(label(LABEL_METHOD), Some("ReadRows"));
    assert_eq!(label(LABEL_BIGQUERY_TABLE), Some("calls"));

    let histogram = latencies(&infos);
    assert_eq!(histogram.total(), 3);
    assert_eq!(
        histogram.buckets(),
        HistogramBuckets::linear(0.0, 20.0, 3000).unwrap()
    );
    assert_eq!(&histogram.bucket_counts()[..4], &[1, 1, 0, 1]);
}

#[test]
fn a_handle_keeps_the_transform_when_another_thread_records() {
    let request = {
        let _scope = MetricsScope::enter(Arc::new(MetricsContainer::new()), Arc::from("Write"));
        Metrics::api_request("Storage", "WriteObject", "bucket-threads")
    };
    std::thread::spawn(move || request.record("not_found", Duration::from_millis(1)))
        .join()
        .unwrap();

    let infos = infos_of("bucket-threads");
    assert_eq!(count(&infos, "not_found"), Some(1));
    assert!(
        infos
            .iter()
            .all(|info| info.labels.get(LABEL_PTRANSFORM).map(String::as_str) == Some("Write"))
    );
}

#[test]
fn a_handle_outside_a_transform_has_an_empty_transform_label() {
    Metrics::api_request("SchemaRegistry", "GET", "registry-outside").record("ok", Duration::ZERO);

    let infos = infos_of("registry-outside");
    assert_eq!(count(&infos, "ok"), Some(1));
    assert_eq!(
        infos[0].labels.get(LABEL_PTRANSFORM).map(String::as_str),
        Some("")
    );
}

#[test]
fn metric_results_skip_io_metrics() {
    Metrics::api_request("Kafka", "ListOffsets", "broker-results").record("ok", Duration::ZERO);

    let infos = infos_of("broker-results");
    assert_eq!(infos.len(), 2);
    assert!(MetricResults::from_monitoring_infos(&infos, &infos).is_empty());
}

#[tokio::test]
async fn time_records_ok_or_the_status_of_the_error() {
    let request = Metrics::api_request("BigTable", "MutateRows", "table-time");
    let ok = request.time(async { Ok::<_, u16>(7) }, |_| "unknown").await;
    let failed = request
        .time(async { Err::<i32, _>(404) }, |e| http_status(*e))
        .await;

    assert_eq!((ok, failed), (Ok(7), Err(404)));
    let infos = infos_of("table-time");
    assert_eq!(count(&infos, "ok"), Some(1));
    assert_eq!(count(&infos, "not_found"), Some(1));
}

#[test]
fn status_names_follow_the_canonical_codes() {
    let grpc = [
        (0, "ok"),
        (4, "deadline_exceeded"),
        (5, "not_found"),
        (8, "resource_exhausted"),
        (14, "unavailable"),
        (16, "unauthenticated"),
        (17, "unknown"),
        (-1, "unknown"),
    ];
    grpc.into_iter()
        .for_each(|(code, name)| assert_eq!(grpc_status(code), name, "gRPC {code}"));
    let http = [
        (200, "ok"),
        (204, "ok"),
        (400, "out_of_range"),
        (404, "not_found"),
        (429, "resource_exhausted"),
        (501, "not_implemented"),
        (503, "unavailable"),
        (418, "unknown"),
    ];
    http.into_iter()
        .for_each(|(code, name)| assert_eq!(http_status(code), name, "HTTP {code}"));
}
