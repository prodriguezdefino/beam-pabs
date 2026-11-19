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

//! Calls of connectors to external services: a count by status and a latency histogram for
//! each method and resource. The model marks both metrics as process metrics, so they record
//! into the process-wide container, with the transform as a label.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beam_model::pipeline::MonitoringInfo;

use super::scope::{process_wide_container, with_running_transform};
use super::values::{HistogramBuckets, HistogramValue, MetricCell};
use super::{
    LABEL_METHOD, LABEL_PTRANSFORM, LABEL_RESOURCE, LABEL_SERVICE, LABEL_STATUS, TYPE_HISTOGRAM,
    TYPE_SUM_INT64, URN_API_REQUEST_COUNT, URN_API_REQUEST_LATENCIES,
};

/// Latency buckets in milliseconds: 20 ms wide, up to 60 s.
const LATENCY_BUCKETS: HistogramBuckets = HistogramBuckets::Linear {
    start: 0.0,
    width: 20.0,
    count: 3000,
};

/// The canonical gRPC status names, by code.
const GRPC_STATUSES: [&str; 17] = [
    "ok",
    "cancelled",
    "unknown",
    "invalid_argument",
    "deadline_exceeded",
    "not_found",
    "already_exists",
    "permission_denied",
    "resource_exhausted",
    "failed_precondition",
    "aborted",
    "out_of_range",
    "unimplemented",
    "internal",
    "unavailable",
    "data_loss",
    "unauthenticated",
];

/// The labels of the calls of one transform to one method on one resource.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RequestLabels {
    transform_id: String,
    service: Cow<'static, str>,
    method: Cow<'static, str>,
    resource: String,
    extra: Vec<(&'static str, String)>,
}

impl RequestLabels {
    fn to_map(&self) -> HashMap<String, String> {
        [
            (LABEL_PTRANSFORM, self.transform_id.as_str()),
            (LABEL_SERVICE, &self.service),
            (LABEL_METHOD, &self.method),
            (LABEL_RESOURCE, &self.resource),
        ]
        .into_iter()
        .chain(self.extra.iter().map(|(key, value)| (*key, value.as_str())))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
    }
}

/// Counts the calls to one method of an external service on one resource, and their latency.
///
/// The handle keeps the transform that runs when it is made, so a task that the transform
/// spawns records with the same transform label.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiRequest {
    labels: Arc<RequestLabels>,
}

impl ApiRequest {
    pub(crate) fn new(
        service: impl Into<Cow<'static, str>>,
        method: impl Into<Cow<'static, str>>,
        resource: impl Into<String>,
    ) -> Self {
        let transform_id = with_running_transform(|id| id.unwrap_or_default().to_string());
        Self {
            labels: Arc::new(RequestLabels {
                transform_id,
                service: service.into(),
                method: method.into(),
                resource: resource.into(),
                extra: Vec::new(),
            }),
        }
    }

    /// Adds a label of the resource, such as [`LABEL_BIGQUERY_DATASET`].
    ///
    /// [`LABEL_BIGQUERY_DATASET`]: super::LABEL_BIGQUERY_DATASET
    pub fn with_label(mut self, key: &'static str, value: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.labels)
            .extra
            .push((key, value.into()));
        self
    }

    /// Records one call that ended with `status` after `latency`. [`grpc_status`] and
    /// [`http_status`] give the status names.
    pub fn record(&self, status: &'static str, latency: Duration) {
        process_wide_container().record_api_request(
            &self.labels,
            status,
            latency.as_secs_f64() * 1_000.0,
        );
    }

    /// Waits for `call` and records its latency, with `ok` or the `status` of its error.
    pub async fn time<T, E>(
        &self,
        call: impl Future<Output = Result<T, E>>,
        status: impl FnOnce(&E) -> &'static str,
    ) -> Result<T, E> {
        let started = Instant::now();
        let result = call.await;
        self.record(
            result.as_ref().map_or_else(status, |_| "ok"),
            started.elapsed(),
        );
        result
    }
}

/// The canonical status name of the gRPC status `code`, or `unknown`.
pub fn grpc_status(code: i32) -> &'static str {
    usize::try_from(code)
        .ok()
        .and_then(|index| GRPC_STATUSES.get(index))
        .copied()
        .unwrap_or("unknown")
}

/// The canonical status name of the HTTP status `code`, or `unknown`.
pub fn http_status(code: u16) -> &'static str {
    match code {
        200..=299 => "ok",
        400 => "out_of_range",
        401 => "unauthenticated",
        403 => "permission_denied",
        404 => "not_found",
        409 => "already_exists",
        429 => "resource_exhausted",
        499 => "cancelled",
        500 => "internal",
        501 => "not_implemented",
        503 => "unavailable",
        504 => "deadline_exceeded",
        _ => "unknown",
    }
}

/// The calls with one set of labels: a count for each status, and the latencies.
#[derive(Debug)]
pub(crate) struct RequestCell {
    counts: Vec<(&'static str, i64)>,
    latencies: HistogramValue,
}

impl Default for RequestCell {
    fn default() -> Self {
        Self {
            counts: Vec::new(),
            latencies: HistogramValue::new(LATENCY_BUCKETS),
        }
    }
}

impl RequestCell {
    pub(crate) fn record(&mut self, status: &'static str, latency_ms: f64) {
        match self.counts.iter_mut().find(|(name, _)| *name == status) {
            Some((_, count)) => *count = count.saturating_add(1),
            None => self.counts.push((status, 1)),
        }
        self.latencies.record(latency_ms);
    }

    /// The latency histogram and one count for each status, as `MonitoringInfo`s.
    pub(crate) fn monitoring_infos(
        &self,
        labels: &RequestLabels,
    ) -> impl Iterator<Item = MonitoringInfo> + '_ {
        let labels = labels.to_map();
        let latencies = MonitoringInfo {
            urn: URN_API_REQUEST_LATENCIES.to_string(),
            r#type: TYPE_HISTOGRAM.to_string(),
            payload: self.latencies.encode_cell(),
            labels: labels.clone(),
            start_time: None,
        };
        let counts = self.counts.iter().map(move |(status, count)| {
            let mut labels = labels.clone();
            labels.insert(LABEL_STATUS.to_string(), status.to_string());
            MonitoringInfo {
                urn: URN_API_REQUEST_COUNT.to_string(),
                r#type: TYPE_SUM_INT64.to_string(),
                payload: count.encode_cell(),
                labels,
                start_time: None,
            }
        });
        std::iter::once(latencies).chain(counts)
    }
}
