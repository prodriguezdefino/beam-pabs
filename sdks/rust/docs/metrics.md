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

**System and harness execution metrics.** `apache-beam-harness` tracks these without user code.

- `beam:metric:element_count:v1` (**Implemented**): output element count per PCollection (`sum_int64`).
- `beam:metric:data_channel:read_index:v1` (**Implemented**): read progress of the data channel (`sum_int64`).
- `beam:metric:sampled_byte_size:v1` (**Implemented**): byte size distribution per PCollection.
- `beam:metric:pardo_execution_time:process_bundle_msecs:v1` (**Implemented**): wall-clock execution time of DoFn processing, in milliseconds.
- `beam:metric:ptransform_progress:completed:v1` and `beam:metric:ptransform_progress:remaining:v1` (**Implemented**): work completed and work remaining in the current element of a splittable DoFn. `ProcessBundleProgressResponse` reports them.

**User-defined metrics** (**Implemented**). Call these directly in a DoFn.

- Counters: `beam:metric:user:sum_int64:v1` (`Metrics::counter(namespace, name)`, then `.inc()`, `.inc_by(n)`, `.dec()` or `.dec_by(n)`).
- Distributions: `beam:metric:user:distribution_int64:v1` (`Metrics::distribution(namespace, name).update(value)`).
- Gauges: `beam:metric:user:latest_int64:v1` (`Metrics::gauge(namespace, name).set(value)`).
- String sets: `beam:metric:user:set_string:v1` (`Metrics::string_set(namespace, name).add(value)`).
- Bounded tries: `beam:metric:user:bounded_trie:v1` (`Metrics::bounded_trie(namespace, name).add(segments)`).
- Histograms: `beam:metric:user:histogram_int64:v1` (`Metrics::histogram(namespace, name, buckets).update(value)`). `HistogramBuckets::linear` and `HistogramBuckets::exponential` make the buckets.
- **Dataflow**: the job results have string sets only if the job sets `--experiments=enable_lineage`. Dataflow does not return gauges or bounded tries.
- **Prism**: the job results have no bounded tries.
- These types are in `beam::metrics`. On the worker thread, the harness sets a current `MetricsContainer` and a current transform (`MetricsScope`). Both types are in `beam::internals`. A metric records into the current container. Outside a scope, it records nothing. At the end of the bundle, the container drains into `ProcessBundleResponse.monitoring_data`. Progress responses also carry its values.
- **Post-Execution Query API** (**Implemented**): call `result.metrics()` on the `PipelineResult`. It returns `Option<&MetricResults>`, because a runner can report no metrics. Aggregated queries are `metrics.counter("ns", "name")`, `metrics.distribution(...)`, `metrics.gauge(...)`, `metrics.bounded_trie(...)` and `metrics.histogram(...)`. Each has a `*_for_transform` variant. Filter queries take a [`MetricFilter`](../beam/core/src/metrics/query.rs): `query_counters`, `query_distributions`, `query_gauges`, `query_bounded_tries` and `query_histograms`.

```rust
let result = pipeline.run().await?;
if let Some(metrics) = result.metrics() {
    let words = metrics.counter("wordcount", "words");
}
```

- **Live Query API** (**Implemented**): `pipeline.submit()` returns a `JobHandle`. While the job runs, `JobHandle::metrics()` returns the `MetricResults` that the runner has. Prism and Dataflow give attempted and committed values.

**I/O and service metrics.**

- `beam:metric:io:api_request_count:v1` and `beam:metric:io:api_request_latencies:v1` (*Planned*): standard connector metrics for Cloud Storage, BigQuery and HTTP services. The SDK defines the URNs, but no connector emits them.
