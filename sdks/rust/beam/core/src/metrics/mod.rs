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

//! Beam metric URNs, labels and the types that collect and query metrics. The Fn API sends
//! metrics as `MonitoringInfo` messages: system metrics that the SDK harness collects, user
//! metrics that DoFn code declares, and I/O metrics that connectors record.

pub mod context;
pub mod query;
pub mod scope;
pub mod short_ids;
pub mod system;
pub mod user;

pub use context::{DistributionValue, GaugeValue, MetricKey, MetricsContainer};
pub use query::{
    MetricFilter, MetricPhase, MetricReading, MetricResult, MetricResults, MetricValue,
};
pub use scope::{MetricsScope, TransformSelection};
pub use short_ids::ShortIdCache;
pub use system::{
    data_channel_read_index, element_count, process_bundle_msecs, sampled_byte_size,
    work_completed, work_remaining,
};
pub use user::{Counter, Distribution, Gauge, Metrics};

/// Standard Fn API protocol capability for short monitoring info IDs.
pub const PROTOCOL_MONITORING_INFO_SHORT_IDS: &str = "beam:protocol:monitoring_info_short_ids:v1";

// System and execution metric URNs.

/// The total number of elements output to a PCollection by a PTransform.
pub const URN_ELEMENT_COUNT: &str = "beam:metric:element_count:v1";

// The SDK reports only `SUPPORTED_METRIC_URNS`. The others identify foreign monitoring infos.
pub const URN_SAMPLED_BYTE_SIZE: &str = "beam:metric:sampled_byte_size:v1";
pub const URN_START_BUNDLE_MSECS: &str = "beam:metric:pardo_execution_time:start_bundle_msecs:v1";
pub const URN_PROCESS_BUNDLE_MSECS: &str =
    "beam:metric:pardo_execution_time:process_bundle_msecs:v1";
pub const URN_FINISH_BUNDLE_MSECS: &str = "beam:metric:pardo_execution_time:finish_bundle_msecs:v1";
pub const URN_TOTAL_MSECS: &str = "beam:metric:ptransform_execution_time:total_msecs:v1";
pub const URN_DATA_CHANNEL_READ_INDEX: &str = "beam:metric:data_channel:read_index:v1";
pub const URN_WORK_REMAINING: &str = "beam:metric:ptransform_progress:remaining:v1";
pub const URN_WORK_COMPLETED: &str = "beam:metric:ptransform_progress:completed:v1";

// User metric URNs.

pub const URN_USER_SUM_INT64: &str = "beam:metric:user:sum_int64:v1";
pub const URN_USER_SUM_DOUBLE: &str = "beam:metric:user:sum_double:v1";
pub const URN_USER_DISTRIBUTION_INT64: &str = "beam:metric:user:distribution_int64:v1";
pub const URN_USER_DISTRIBUTION_DOUBLE: &str = "beam:metric:user:distribution_double:v1";
pub const URN_USER_LATEST_INT64: &str = "beam:metric:user:latest_int64:v1";
pub const URN_USER_LATEST_DOUBLE: &str = "beam:metric:user:latest_double:v1";
pub const URN_USER_TOP_N_INT64: &str = "beam:metric:user:top_n_int64:v1";
pub const URN_USER_TOP_N_DOUBLE: &str = "beam:metric:user:top_n_double:v1";
pub const URN_USER_BOTTOM_N_INT64: &str = "beam:metric:user:bottom_n_int64:v1";
pub const URN_USER_BOTTOM_N_DOUBLE: &str = "beam:metric:user:bottom_n_double:v1";
pub const URN_USER_SET_STRING: &str = "beam:metric:user:set_string:v1";
pub const URN_USER_BOUNDED_TRIE: &str = "beam:metric:user:bounded_trie:v1";
pub const URN_USER_HISTOGRAM: &str = "beam:metric:user:histogram_int64:v1";

// I/O and service metric URNs.

pub const URN_API_REQUEST_COUNT: &str = "beam:metric:io:api_request_count:v1";
pub const URN_API_REQUEST_LATENCIES: &str = "beam:metric:io:api_request_latencies:v1";

// Metric type URNs, which name the payload encoding.

pub const TYPE_SUM_INT64: &str = "beam:metrics:sum_int64:v1";
pub const TYPE_SUM_DOUBLE: &str = "beam:metrics:sum_double:v1";
pub const TYPE_DISTRIBUTION_INT64: &str = "beam:metrics:distribution_int64:v1";
pub const TYPE_DISTRIBUTION_DOUBLE: &str = "beam:metrics:distribution_double:v1";
pub const TYPE_LATEST_INT64: &str = "beam:metrics:latest_int64:v1";
pub const TYPE_LATEST_DOUBLE: &str = "beam:metrics:latest_double:v1";
pub const TYPE_TOP_N_INT64: &str = "beam:metrics:top_n_int64:v1";
pub const TYPE_TOP_N_DOUBLE: &str = "beam:metrics:top_n_double:v1";
pub const TYPE_BOTTOM_N_INT64: &str = "beam:metrics:bottom_n_int64:v1";
pub const TYPE_BOTTOM_N_DOUBLE: &str = "beam:metrics:bottom_n_double:v1";
pub const TYPE_PROGRESS: &str = "beam:metrics:progress:v1";
pub const TYPE_SET_STRING: &str = "beam:metrics:set_string:v1";
pub const TYPE_BOUNDED_TRIE: &str = "beam:metrics:bounded_trie:v1";
pub const TYPE_HISTOGRAM: &str = "beam:metrics:histogram_int64:v1";

// `MonitoringInfo` label keys.

pub const LABEL_PCOLLECTION: &str = "PCOLLECTION";
pub const LABEL_PTRANSFORM: &str = "PTRANSFORM";
pub const LABEL_NAMESPACE: &str = "NAMESPACE";
pub const LABEL_NAME: &str = "NAME";
pub const LABEL_SERVICE: &str = "SERVICE";
pub const LABEL_METHOD: &str = "METHOD";
pub const LABEL_RESOURCE: &str = "RESOURCE";
pub const LABEL_STATUS: &str = "STATUS";

/// Metric URNs that this SDK reports to runners.
pub const SUPPORTED_METRIC_URNS: &[&str] = &[
    URN_ELEMENT_COUNT,
    URN_DATA_CHANNEL_READ_INDEX,
    URN_SAMPLED_BYTE_SIZE,
    URN_PROCESS_BUNDLE_MSECS,
    URN_USER_SUM_INT64,
    URN_USER_DISTRIBUTION_INT64,
    URN_USER_LATEST_INT64,
];
