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

# SDK Feature Parity

This page compares the Rust SDK with the other Beam SDKs.

**Legend:** ✅ supported (native or `✅ xlang`) · 🟡 partial · 🔜 planned for Rust · ❌ not supported

> [!NOTE]
> Each cell is verified against the source code in this repository, not
> against documentation. If the source does not show a capability, the cell is
> "?".

---

## Core model

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| Batch execution | ✅ | ✅ | ✅ | ✅ | ✅ |
| Streaming execution | ✅ | ✅ | ✅ | 🟡 | ✅ |
| ParDo / DoFn | ✅ | ✅ | ✅ | ✅ | ✅ |
| GroupByKey | ✅ | ✅ | ✅ | ✅ | ✅ |
| CoGroupByKey | ✅ | ✅ | ✅ | ✅ | ✅ |
| Combine (global + per-key) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Flatten / Partition | ✅ | ✅ | ✅ | ✅ | ✅ |
| Composite transforms | ✅ | ✅ | ✅ | ✅ | ✅ |
| Side inputs (singleton / iterable / multimap) | ✅ | ✅ | ✅ | 🟡 | ✅ |
| Display data | ✅ | ✅ | 🟡 | ❌ | ✅ |

## Windowing

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| Fixed / Sliding / Sessions / Global | ✅ | ✅ | ✅ | ✅ | ✅ |
| Custom user-defined `WindowFn` | ✅ | ✅ | ❌ | ✅ | 🟡 |
| Triggers | ✅ | ✅ | ✅ | ❌ | ✅ |
| Accumulation modes | ✅ | ✅ | ✅ | ❌ | ✅ |
| Allowed lateness | ✅ | ✅ | ✅ | ❌ | ✅ |

Notes:
- **Go** has no user-extensible `WindowFn`. `window.Fn` is a closed struct over
  the four built-in kinds.
- **TypeScript** hardcodes the default trigger, `DISCARDING`, and zero lateness.
- **Rust** has a public `WindowFn` trait for any assignment logic (e.g. fiscal
  calendars, business shifts). The produced window must be an `IntervalWindow`
  or a `GlobalWindow`. Rust does not support new window data structures (e.g.
  2D spatial windows or custom metadata payloads). There are three causes:
  - `BoundedWindow` is an enum of Global/Interval.
  - The Fn API harness decodes only the standard window coder URNs.
  - Portable runners have no general callback protocol to merge non-standard windows.

## State and timers

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| ValueState / BagState | ✅ | ✅ | ✅ | ❌ | ✅ |
| MapState | ✅ | ❌ | ✅ | ❌ | ✅ |
| SetState | ✅ | ✅ | ✅ | ❌ | ✅ |
| CombiningState | ✅ | ✅ | ✅ | ❌ | ❌ |
| OrderedListState | ✅ | ✅ | ✅ | ❌ | ❌ |
| Event-time timers | ✅ | ✅ | ✅ | ❌ | ✅ |
| Processing-time timers | ✅ | ✅ | ✅ | ❌ | ✅ |
| Timer families (dynamic tags) | ✅ | ✅ | ✅ | ❌ | ✅ |

Notes:
- **Python** has `BagStateSpec`, `SetStateSpec`, `ReadModifyWriteStateSpec`,
  `CombiningValueStateSpec` and `OrderedListStateSpec`, but no `MapState`.
- **TypeScript** has no state or timer support.
- **`CombiningState` in Rust**: no protocol blocks it. `CombiningState` is not
  an Fn API state type. It is an SDK-side composition of a `CombineFn` over
  `ValueState` or `BagState`. `beam::transforms::CombineFn` exists, but a
  `CombiningStateSpec` wrapper does not.
- **`OrderedListState` in Rust**: it needs a third Fn API state wire protocol
  (`beam:user_state:ordered_list:v1`, `StateKey.OrderedListUserState`). That protocol
  uses `KV<varint, length_prefix<value>>` encoding, sort-range pushdown
  (`OrderedListRange` `[start, end)`) and continuation-token paging bounded by range.
  An emulation over bag or multimap state loses the sort-range pushdown to the runner
  storage engine. Runner support is also fragmented: Prism has no range queries, and
  Dataflow supports `OrderedListState` only on Streaming Engine, not on Batch.

## Splittable DoFn

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| Splittable DoFn | ✅ | ✅ | ✅ | ❌ | ✅ |
| Watermark estimators | ✅ | ✅ | ✅ | ❌ | ✅ |
| Dynamic splitting / bundle finalization | ✅ | ✅ | ✅ | ❌ | ✅ |

Notes:
- **Dynamic splitting in Rust** has three forms:
  - channel splits at element boundaries of the data stream
  - self-checkpoints through `ProcessContinuation::resume()`
  - runner-initiated splits of active restrictions. The harness evaluates a
    `ProcessBundleSplitRequest` against the live `RestrictionTracker`. It
    returns `primary_roots`, `residual_roots` and channel splits.

  The SDF types are in `beam::transforms::sdf`.
- **Bundle finalization in Rust**: DoFns and sinks register post-commit
  callbacks with `ProcessContext::register_finalizer`. The callbacks run only if
  the DoFn returns `true` from `DoFn::requests_finalization`. The harness sets
  `requires_finalization` in `ProcessBundleResponse`. It runs the callbacks on
  `FinalizeBundleRequest`.


## Schemas and types

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| Beam Schemas / Row coder | ✅ | ✅ | ✅ | ✅ | ✅ |
| Schema inference from native types | ✅ | ✅ | ✅ | 🟡 | ✅ |
| Logical types | ✅ | ✅ | ✅ | ✅ | ✅ |
| Beam SQL | ✅ native | ✅ xlang | ✅ xlang | ✅ xlang | 🔜 |
| DataFrame / relational API | ✅ xlang | ✅ | ✅ xlang | ❌ | ❌ |

Rust infers schemas through `#[derive(BeamRow)]`. Beam SQL is native only in
Java. All other SDKs use it through cross-language.

## Portability and deployment

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| Portable Fn API harness | ✅ | ✅ | ✅ | ✅ | ✅ |
| Prism runner | ✅ | ✅ | ✅ | ✅ | ✅ |
| Dataflow runner | ✅ | ✅ | ✅ | ❌ | ✅ |
| Flink / Spark runners | ✅ | ✅ | ✅ | 🟡 | 🟡 untested |
| **Consume** cross-language transforms | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Provide** transforms to other SDKs | ✅ | ✅ | ❌ | ❌ | ✅ |
| Automated Java expansion service download | ❌ | ✅ | ✅ | ✅ | ✅ |
| Custom containers | ✅ | ✅ | ✅ | ✅ | ✅ |
| Dataflow Flex Templates | ✅ | ✅ | ? | ❌ | 🟡 experimental |
| Resource hints, per-transform | ✅ | ✅ | ❌ | ❌ | ✅ |
| Resource hints, pipeline-level | ✅ | ✅ | ✅ | ❌ | ✅ |
| Managed I/O | ✅ | ✅ | ❌ | ❌ | ✅ xlang |
| Lineage reporting | ✅ | ✅ | ❌ | ❌ | ❌ |
| Worker runs user code without rebuilding the pipeline | ✅ | ✅ | ✅ | ✅ | 🔜 |

Notes:
- **Worker without rebuilding the pipeline**: Java and Python serialize DoFns
  into the pipeline proto. Go looks up functions that are registered by name.
  Rust closures cannot be serialized, so a Rust worker rebuilds the pipeline
  to find the code of each transform.
  [static-registration.md](static-registration.md) describes the gap and the
  work to close it.
- **Providing** transforms means that the SDK ships an expansion service, so
  other SDKs can call its transforms. Go and TypeScript have only the generated
  gRPC stubs, not a server implementation. Rust ships `apache-beam-expansion`
  and the `beam-expansion-service` binary.
- Java does not download its *own* expansion service, because the downloaded
  service is the Java expansion service.
- For all SDKs, Flex Template support is mostly tooling outside this
  repository, so the source does not show the Go value.
- **Managed I/O** has the same model as Python.
  `beam::io::managed::{ManagedRead, ManagedWrite}` expand Java's
  `beam:transform:managed:v1` with the connector config as YAML (Iceberg,
  Iceberg CDC read, Kafka, BigQuery, Postgres, MySQL, SQL Server, Delta Lake;
  constants in `beam::io::managed`, e.g. `managed::ICEBERG`). The typed Kafka and
  BigQuery transforms also expand through Managed, so runners such as Dataflow
  can manage and upgrade them. The BigQuery Storage Write v2 and File Loads
  methods have no Managed equivalent, so they expand directly.
- **Resource hints**: Rust supports pipeline-level and per-transform hints
  (`beam:resources:min_ram_bytes:v1`, `beam:resources:cpu_count:v1`,
  `beam:resources:accelerator:v1`, `beam:resources:max_active_bundles_per_worker:v1`,
  and custom hints). Set them on `Pipeline` or with `--resource_hints`, or per
  transform with `.with_resource_hints(...)` / `Pipeline::enter_resource_hints_scope()`.
  Dataflow uses them to provision worker pools per stage.


## Testing and observability

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| User counters / distributions / gauges | ✅ | ✅ | ✅ | 🟡 | ✅ |
| Post-run metrics query API | ✅ | ✅ | ✅ | ✅ | ✅ |
| Worker status / diagnostics endpoint | ✅ | ✅ | ✅ | ❌ | ✅ |
| Structured worker logging over Fn API | ✅ | ✅ | ✅ | 🟡 | ✅ |
| In-graph assertions | ✅ | ✅ | ✅ | ✅ | ✅ |
| `TestPipeline`-style harness | ✅ | ✅ | ✅ | 🟡 | ✅ |
| `TestStream` | ✅ | ✅ | ✅ | ❌ | ✅ |
| ValidatesRunner suite | ✅ | ✅ | ✅ | 🟡 | ✅ |

TypeScript has counters and distributions, but no gauge.

In Rust, the `apache-beam-testing` crate contains the assertions
(`beam::testing::passert`), `TestStream` and `TestPipeline`. The `testing`
feature enables this crate. `TestPipeline` reads its options from
`BEAM_TEST_PIPELINE_OPTIONS`, which is the equivalent of Java's
`beamTestPipelineOptions`. After the run, `TestPipeline` verifies that each
assertion ran and passed. It panics if you drop it with transforms but without
a run (run enforcement, on by default). `TestStream`
needs a runner that executes `beam:transform:teststream:v1`, such as Prism.
Dataflow does not execute it.

## I/O connectors

| Connector | Java | Python | Go | **Rust** |
|---|:--:|:--:|:--:|:--:|
| TextIO / FileIO | ✅ | ✅ | ✅ | ✅ |
| Local filesystem | ✅ | ✅ | ✅ | ✅ |
| GCS (`gs://`) | ✅ | ✅ | ✅ | ✅ |
| S3 (`s3://`) | ✅ | ✅ | ✅ | 🔜 |
| Azure Blob | ✅ | ✅ | 🟡 | ❌ |
| HDFS | ✅ | ✅ | ❌ | ❌ |
| Pub/Sub | ✅ | ✅ | ✅ xlang | ✅ xlang |
| BigQuery | ✅ | ✅ xlang | ✅ xlang | ✅ xlang |
| Bigtable | ✅ | ✅ | ✅ | ✅ xlang |
| Kafka | ✅ | ✅ xlang | ✅ xlang | ✅ xlang |
| JDBC (Postgres, MySQL, SQL Server) | ✅ | ✅ xlang | ✅ xlang | ✅ xlang |
| Iceberg | ✅ | ✅ xlang | ❌ | ✅ xlang |
| Delta Lake (read) | ✅ | ✅ xlang | ❌ | ✅ xlang |
| Avro | ✅ | ✅ | ✅ | ✅ |
| Parquet | ✅ | ✅ | ✅ | ✅ |
| Elasticsearch | ✅ | ✅ | ❌ | 🔜 |

Notes:
- **Pub/Sub**: native in Java and Python (Dataflow streaming). Go and Rust use cross-language SchemaTransforms through an expansion service (`beam:schematransform:org.apache.beam:pubsub_read:v1` / `write:v1`).
- **BigQuery**: native in Java (`BigQueryIO`). Python, Go and Rust use cross-language Storage Read API and Storage Write API transforms from the Java SchemaTransform providers (`beam:schematransform:org.apache.beam:bigquery_storage_read:v1` / `bigquery_write:v1`).
- **Bigtable**: native in Java, Python and Go. Rust uses the Java SchemaTransform providers (`beam:schematransform:org.apache.beam:bigtable_read:v1` / `bigtable_write:v1`) through `beam::io::gcp::bigtable::{BigtableRead, BigtableWrite}`. These providers accept only project, instance and table, plus `flatten` on read. Until the Java providers expose them, Rust and the Python cross-language wrapper cannot set app profiles, emulator host, row filters, key ranges or write flow control.
- **Kafka**: native in Java. Python, Go and Rust use the Java SchemaTransform providers (`beam:schematransform:org.apache.beam:kafka_read:v1` / `kafka_write:v1`). Rust exposes them as `beam::io::kafka::{KafkaRead, KafkaWrite}` behind the `kafka` feature, and they expand through Managed I/O. You can configure consumer/producer properties (security, SASL, group id, …), RAW/STRING/JSON/AVRO/PROTO formats, Confluent schema registry, bounded reads and redistribution. The providers do not expose record keys and headers. On `KafkaRead` and `KafkaWrite`, `with_error_handling` sends failed records to an error output. The plain transforms do not return this output. To get the errors from `KafkaRead`, expand `to_managed()?.with_all_outputs()` and take the error output tag. For `KafkaWrite`, expand `to_managed()?.with_outputs()`. `KafkaRead` does not support error handling together with the Confluent schema registry.
- **Iceberg, Delta Lake & JDBC**: native in Java. Python and Rust use them through Managed I/O (`beam.managed.Read/Write` in Python, `beam::io::managed::{ManagedRead, ManagedWrite}` in Rust), which expands the Java SchemaTransforms cross-language. Iceberg and the JDBC databases support read and write. Delta Lake is read-only. Go has no Managed API.

## Ecosystem

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| `RunInference` / ML | ✅ xlang | ✅ | ✅ xlang | ❌ | ✅ |
| Beam YAML | ✅ | ✅ | ❌ | ❌ | 🔜 |
| Interactive / notebooks | ❌ | ✅ | ❌ | ❌ | ❌ |
| Package registry | Maven | PyPI | pkg.go.dev | npm | ❌ none yet |

Notes:
- **RunInference**: native in Python and Rust. Java and Go use the Python
  implementation through cross-language. The Java wrapper is
  `sdks/java/extensions/python/.../RunInference.java`. In Rust, `beam::ml`
  (`ml` feature) contains:
  - `RunInference` and `RunInferenceMulti`
  - the `ModelHandler` trait and `KeyedModelHandler`
  - micro-batching (`BatchBounds`) and a per-worker model cache
  - handlers for ONNX Runtime (`OnnxModelHandler`, `ml-onnx*`), Candle
    (`CandleModelHandler`, `BertEmbeddingModelHandler`, `ml-candle*`) and
    remote endpoints (`RemoteModelHandler` with a Vertex AI Gemini adapter, `ml-remote`)

  See [accelerated-workloads.md](accelerated-workloads.md).
