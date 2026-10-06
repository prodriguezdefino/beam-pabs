<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Metrics and Telemetry

This page describes how the Rust SDK implements the Beam telemetry protocol and which metrics it reports.

The SDK uses the Fn API [`MonitoringInfo`](https://github.com/apache/beam/blob/master/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/metrics.proto) structures and short-ID caching (`beam:protocol:monitoring_info_short_ids:v1`).

## Architecture

```text
DoFn / Operator Execution
   │ (emits elements / updates counters)
   ▼
Bundle Processor Accumulator
   │ (on bundle finish or progress poll)
   ▼
Short ID Resolution (ShortIdCache)
   │ (compact 64-bit integer mappings)
   ▼
ProcessBundleResponse / ProcessBundleProgressResponse
   ├── monitoring_infos (first occurrence descriptors)
   └── monitoring_data  (short_id -> varint/distribution payload)
```

## Metric Types & Status

**System and harness execution metrics.** `apache-beam-harness` tracks these automatically.

- `beam:metric:element_count:v1` (**Implemented**): output element count per PCollection (`sum_int64`).
- `beam:metric:data_channel:read_index:v1` (**Implemented**): read progress of the data channel (`sum_int64`).
- `beam:metric:sampled_byte_size:v1` (**Implemented**): byte size distribution per PCollection.
- `beam:metric:pardo_execution_time:process_bundle_msecs:v1` (**Implemented**): wall-clock execution time of DoFn processing, in milliseconds.
- `beam:metric:ptransform_progress:completed:v1` and `beam:metric:ptransform_progress:remaining:v1` (**Implemented**): work completed and work remaining in the current element of a splittable DoFn, reported in `ProcessBundleProgressResponse`.

**User-defined metrics** (**Implemented**). Call these directly in a DoFn.

- Counters: `beam:metric:user:sum_int64:v1` (`Metrics::counter(namespace, name)`, then `.inc()`, `.inc_by(n)`, `.dec()` or `.dec_by(n)`).
- Distributions: `beam:metric:user:distribution_int64:v1` (`Metrics::distribution(namespace, name).update(value)`).
- Gauges: `beam:metric:user:latest_int64:v1` (`Metrics::gauge(namespace, name).set(value)`).
- These types are in `beam::metrics`. The harness makes a `MetricsContainer` and the running transform current on the worker thread (`MetricsScope`). A metric records into that container. Outside a scope, it records nothing. The container drains into `ProcessBundleResponse.monitoring_data` at the end of the bundle, and into progress responses.
- **Post-Execution Query API** (**Implemented**): call `result.metrics()` on the `PipelineResult`. It returns `Option<&MetricResults>`, because a runner can report no metrics. It supports aggregated queries (`metrics.counter("ns", "name")`, `metrics.distribution(...)`, `metrics.gauge(...)`, and `*_for_transform` variants). It also supports scoped filter queries (`query_counters`, `query_distributions`, `query_gauges`) through [`MetricFilter`](../beam/core/src/metrics/query.rs).

```rust
let result = pipeline.run().await?;
if let Some(metrics) = result.metrics() {
    let words = metrics.counter("wordcount", "words");
}
```

**I/O and service metrics.**

- `beam:metric:io:api_request_count:v1` and `beam:metric:io:api_request_latencies:v1` (*Planned*): standard connector metrics for Cloud Storage, BigQuery and HTTP services. The SDK defines the URNs, but no connector emits them.
