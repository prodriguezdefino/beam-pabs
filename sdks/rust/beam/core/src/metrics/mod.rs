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
//! metrics as `MonitoringInfo` messages: system metrics that the SDK harness collects and user
//! metrics that DoFn code declares.

pub(crate) mod context;
pub(crate) mod lineage;
pub(crate) mod query;
pub(crate) mod scope;
pub(crate) mod short_ids;
pub(crate) mod system;
pub(crate) mod user;
pub(crate) mod values;

pub use context::MetricKey;
pub use lineage::{LINEAGE_NAMESPACE, LINEAGE_SINKS, LINEAGE_SOURCES, Lineage};
pub use query::{
    MetricFilter, MetricPhase, MetricReading, MetricResult, MetricResults, MetricValue,
};
pub use user::{
    BoundedTrie, Counter, Distribution, Gauge, Histogram, Metrics, RETRY_WAIT_COUNTER_NAME,
    StringSet, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE,
};
pub use values::{
    BOUNDED_TRIE_DEFAULT_BOUND, BoundedTrieValue, DistributionValue, GaugeValue, HistogramBuckets,
    HistogramValue, STRING_SET_MAX_BYTES, StringSetValue,
};

/// Standard Fn API protocol capability for short monitoring info IDs.
pub const PROTOCOL_MONITORING_INFO_SHORT_IDS: &str = "beam:protocol:monitoring_info_short_ids:v1";

// System and execution metric URNs.

/// The total number of elements output to a PCollection by a PTransform.
pub const URN_ELEMENT_COUNT: &str = "beam:metric:element_count:v1";

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
pub const URN_USER_DISTRIBUTION_INT64: &str = "beam:metric:user:distribution_int64:v1";
pub const URN_USER_LATEST_INT64: &str = "beam:metric:user:latest_int64:v1";
pub const URN_USER_SET_STRING: &str = "beam:metric:user:set_string:v1";
pub const URN_USER_BOUNDED_TRIE: &str = "beam:metric:user:bounded_trie:v1";
pub const URN_USER_HISTOGRAM: &str = "beam:metric:user:histogram_int64:v1";

// Metric type URNs, which name the payload encoding.

pub const TYPE_SUM_INT64: &str = "beam:metrics:sum_int64:v1";
pub const TYPE_DISTRIBUTION_INT64: &str = "beam:metrics:distribution_int64:v1";
pub const TYPE_LATEST_INT64: &str = "beam:metrics:latest_int64:v1";
pub const TYPE_PROGRESS: &str = "beam:metrics:progress:v1";
pub const TYPE_SET_STRING: &str = "beam:metrics:set_string:v1";
pub const TYPE_BOUNDED_TRIE: &str = "beam:metrics:bounded_trie:v1";
pub const TYPE_HISTOGRAM: &str = "beam:metrics:histogram_int64:v1";

// `MonitoringInfo` label keys.

pub const LABEL_PCOLLECTION: &str = "PCOLLECTION";
pub const LABEL_PTRANSFORM: &str = "PTRANSFORM";
pub const LABEL_NAMESPACE: &str = "NAMESPACE";
pub const LABEL_NAME: &str = "NAME";
