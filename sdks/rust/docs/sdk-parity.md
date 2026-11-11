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
> Each cell comes from the source code in this repository, not from
> documentation. If the source does not show a capability, the cell is "?".

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
| Reshuffle (random keys, fixed number of key buckets) | ✅ | ✅ | 🟡 (no buckets) | ❌ | ✅ |
| Redistribute (standalone transform) | ✅ | ❌ | ❌ | ❌ | 🔜 |
| WithTimestamps (standalone transform) | ✅ | ❌ | ❌ | ✅ | 🔜 |
| Flatten / Partition | ✅ | ✅ | ✅ | ✅ | ✅ |
| Distinct | ✅ | ✅ | ✅ | ❌ | ✅ |
| Deduplicate (in a time interval) | ✅ | ✅ | ❌ | ❌ | 🔜 |
| GroupIntoBatches (size, byte size, buffering time, sharded keys) | ✅ | ✅ | ✅ | ❌ | ✅ |
| Set operations (union / intersect / except, distinct and all) | ✅ | ❌ | ❌ | ❌ | ✅ |
| Composite transforms | ✅ | ✅ | ✅ | ✅ | ✅ |
| Hierarchical transform names (composite scopes, duplicate sibling names rejected) | ✅ | ✅ | ✅ (Scope) | ✅ | ✅ |
| Side inputs (singleton / iterable / multimap) | ✅ | ✅ | ✅ | 🟡 | ✅ |
| Map side input that rejects duplicate keys (`as_map`) | ✅ | ✅ | ❌ | ❌ | 🔜 |
| Multimap keys side input (`MultimapKeysSideInput`) | ✅ | ❌ | ❌ | ❌ | ❌ |
| Per-key element order (`PerKeyOrdering` tests) | ✅ | ? | ? | ? | ❌ |
| Display data | ✅ | ✅ | 🟡 | ❌ | ✅ |

Notes:
- **Combine**: Java, Python and Rust also split skewed keys across intermediate shards
  (`withHotKeyFanout` / `withFanout`, `with_hot_key_fanout` / `with_fanout`).
- **Distinct**: Java and Rust also keep one element per representative value
  (`Distinct.withRepresentativeValueFn`, `DistinctBy`).
  Python `Distinct` and Go `filter.Distinct` compare whole elements.
- **Redistribute and WithTimestamps**: Rust uses `Reshuffle` to spread elements across
  workers. It sets event time in a `ParDo` with `ctx.output(v).at(ts).emit()`. TypeScript
  has `assignTimestamps`.
- **Deduplicate**: Java and Python `Deduplicate` drop a repeated element during a
  processing-time or event-time duration. `Distinct` keeps one element per window.
- **`as_map`**: Java `View.asMap` and Python `AsDict` fail on a duplicate key. Rust
  `MultimapView` accepts duplicate keys.
- **Multimap keys side input**: a function reads only the keys of a multimap side input.
  The Java harness reads them with the `MultimapKeysSideInput` state key.
- **Per-key element order**: Java `PerKeyOrderingTest` checks that a runner keeps the order
  of the elements of a key between steps. The other SDKs have no such test.
- **Set operations**: Java `Sets` and Rust `Sets` (`union_distinct`, `intersect_all`,
  `except_distinct`, ...). Rust takes two collections; combine more by chaining.
- **Hierarchical transform names**: the children of a composite get the name of the
  composite as a prefix (`Outer/Inner/Leaf`). All SDKs name a transform at the call site:
  Java `apply(name, t)`, Python `"Label" >> t`, Go `Scope`, TypeScript `withName(name, t)` and
  Rust `apply(name, t)`. Two siblings with the same full name fail `Pipeline::validate`;
  Java warns by default, Python and TypeScript raise an error.

## Combiners

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| Sum / Min / Max / Mean / Count | ✅ | ✅ | ✅ | ✅ | ✅ |
| Top (largest / smallest / custom order) | ✅ | ✅ | ✅ | ❌ | ✅ |
| Sample (uniform fixed size, any) | ✅ | ✅ | ❌ | ❌ | ✅ |
| Composed combine (several combiners in one pass) | ✅ | ✅ | ❌ | ✅ | ✅ |
| ApproximateUnique | ✅ | ✅ | ❌ | ❌ | ✅ |
| ApproximateQuantiles | ✅ | ✅ | ✅ | ❌ | ✅ |
| HLL++ sketches (BigQuery `HLL_COUNT` compatible) | ✅ ext | ❌ | ❌ | ❌ | 🔜 |
| Other sketches (t-digest, count-min, HLL+) | ✅ ext | ❌ | ❌ | ❌ | ❌ |

Notes:
- **Java** keeps the sketch combiners in extension modules (`extensions/zetasketch`,
  `extensions/sketching`), because they depend on sketch libraries. The other
  combiners are in the core SDK in all SDKs.
- **Composed combine**: Java `CombineFns.compose`, Python `TupleCombineFn`,
  TypeScript `groupBy(..).combining(..)`. Rust `Compose` takes 2 to 6 parts and
  nests for more; each part reads its value from the element with `on`.
- **Quantiles**: Java, Python and Rust use the deterministic Munro-Paterson
  algorithm, with the same buffer sizes, so they give the same quantiles. Go uses
  randomized compactors.
- **ApproximateUnique**: Java, Python and Rust keep the largest distinct hashes.
  Each SDK uses its own hash function, so their estimates differ.
- **HLL++ in Rust**: the plan is a cross-language wrapper of the Java `HllCount`, or
  a separate crate that reads and writes ZetaSketch sketches.

## Windowing

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| Fixed / Sliding / Sessions / Global | ✅ | ✅ | ✅ | ✅ | ✅ |
| Custom user-defined `WindowFn` | ✅ | ✅ | ❌ | ✅ | 🟡 |
| Custom window types (window encodings other than global and interval) | ✅ | ✅ | ❌ | ✅ | ❌ |
| Triggers | ✅ | ✅ | ✅ | ❌ | ✅ |
| Accumulation modes | ✅ | ✅ | ✅ | ❌ | ✅ |
| Allowed lateness | ✅ | ✅ | ✅ | ❌ | ✅ |

Notes:
- **Go** has no user-extensible `WindowFn`. `window.Fn` is a closed struct over
  the four built-in kinds.
- **TypeScript** hardcodes the default trigger, `DISCARDING`, and zero lateness.
- **Rust** has a public `WindowFn` trait for any assignment logic, for example fiscal
  calendars or business shifts. Each window must be an `IntervalWindow` or a
  `GlobalWindow`, and the function must not merge. Rust does not support other window
  types, for example 2D spatial windows or windows with custom metadata. There are three causes:
  - `BoundedWindow` is an enum of Global/Interval.
  - The Fn API harness decodes only the standard window coder URNs.
  - Portable runners have no general callback protocol to merge non-standard windows.
- **Rust** `Instant::MIN`, `Instant::MAX` and `Instant::END_OF_GLOBAL_WINDOW` have the
  values of `BeamConstants` in the Runner API, the same values as the other SDKs.

## State and timers

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| ValueState / BagState | ✅ | ✅ | ✅ | ❌ | ✅ |
| MapState | ✅ | ❌ | ✅ | ❌ | ✅ |
| MultimapState | ✅ | ❌ | ❌ | ❌ | ✅ |
| SetState | ✅ | ✅ | ✅ | ❌ | ✅ |
| CombiningState | ✅ | ✅ | ✅ | ❌ | ✅ |
| OrderedListState | ✅ | ✅ | ✅ | ❌ | ✅ |
| Event-time timers | ✅ | ✅ | ✅ | ❌ | ✅ |
| Processing-time timers | ✅ | ✅ | ✅ | ❌ | ✅ |
| Timer families (dynamic tags) | ✅ | ✅ | ✅ | ❌ | ✅ |
| Timer `align` / `offset` | ✅ | ❌ | ❌ | ❌ | 🔜 |
| OnWindowExpiration | ✅ | ❌ | ❌ | ❌ | ✅ |

Notes:
- **Python** has `BagStateSpec`, `SetStateSpec`, `ReadModifyWriteStateSpec`,
  `CombiningValueStateSpec` and `OrderedListStateSpec`, but no `MapState`.
- **TypeScript** has no state or timer support.
- **Timer `align` / `offset`**: Java `Timer.align(period)` and `Timer.offset(duration)` move
  the target time of `setRelative` to a period boundary. In Rust, compute the instant and
  call `at`.
- **OnWindowExpiration**: Java and Rust declare an event-time timer family and name it in
  `ParDoPayload.on_window_expiration_timer_family_spec`. The runner fires it once per key
  and window. Java marks the method with `@OnWindowExpiration`; Rust sets
  `const ON_WINDOW_EXPIRATION: bool = true` and implements `on_window_expiration`.
- **MultimapState**: Java and Rust use `MultimapStateSpec` and the multimap state keys of
  the Fn API. In Rust, `entries` skips keys with no values, because Prism keeps a removed
  key as an empty entry. On Dataflow streaming, Streaming Engine returns no values for a key
  that earlier bundles only appended to, for Java and Rust alike (see [validates-runner.md](validates-runner.md)).
- **`CombiningState`**: every SDK declares a `CombiningStateSpec` with the
  accumulator coder and the bag protocol, and combines in the SDK harness.
  Java and Rust keep one accumulator: an add reads it, adds the input and writes
  it back. Python sometimes appends a one-input accumulator instead and merges
  on read. Go keeps the accumulator as a value. Rust builds the `CombineFn` with
  `Default`, because a `#[derive(State)]` cell has no instance; Java, Python and
  Go take a `CombineFn` instance in the spec.
- **Dynamic `Row` state**: Java needs a `RowCoder` with an explicit schema in the state
  spec, and fails at construction without it. Rust needs the schema from
  `StatefulDoFn::state_row_schemas` and fails at `validate` without it. Rust also rejects a
  write of a `Row` with another schema at run time.
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
- **Bundle finalization in Rust**: a `DoFn`, `StatefulDoFn` or `SplittableDoFn`
  declares it in its output type (`type Out = WithFinalizer<O>`). Only then does it
  have `ctx.finalizer(..).register()`, so a missing declaration is a compile error.
  Java and Python check at run time. Each callback has an expiry, 10 minutes by
  default, as Java's `BundleFinalizer.afterBundleCommit(expiry, ..)`. The harness
  gives the finalizer to the stages of an expanded splittable ParDo. It sets
  `requires_finalization` in `ProcessBundleResponse`, runs the callbacks on
  `FinalizeBundleRequest` and drops expired callbacks.


## Schemas and types

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| Beam Schemas / Row coder | ✅ | ✅ | ✅ | ✅ | ✅ |
| Schema inference from native types | ✅ | ✅ | ✅ | 🟡 | ✅ |
| Logical types | ✅ | ✅ | ✅ | ✅ | ✅ |
| Beam SQL | ✅ native | ✅ xlang | ✅ xlang | ✅ xlang | 🔜 |
| DataFrame / relational API | ✅ xlang | ✅ | ✅ xlang | ❌ | ❌ |
| Tuple coders (3+ values) | ❌ | ✅ | ❌ | ❌ | ✅ |
| Nullable elements across stages (`beam:coder:nullable:v1`) | ✅ | ✅ | ✅ | ✅ | ✅ |

Rust infers schemas through `#[derive(BeamRow)]`, including generic structs whose type
parameters satisfy `BeamField` and `DefaultCoder`. `ToRows` and `FromRows` convert between
`PCollection<T>` and `PCollection<Row>`, matching fields by Beam name when schemas differ.
User state cells (`ValueState<Row>`, `BagState<Row>`, `MapState`, `MultimapState`,
`OrderedListState`, `CombiningState`) support dynamic `Row` values without extra state
attributes. Beam SQL is native only in Java. All other SDKs use it through cross-language.

Rust encodes `Option<T>` as `beam:coder:nullable:v1`. Inside it, a runner can wrap a
component that it does not know (for example a `Row`) in `beam:coder:length_prefix:v1`. The
harness removes that length on input and writes it on output.

Tuples:
- **Java, Go and TypeScript** have only `KV`. They use `Row` for records.
- **Python** registers a tuple of 3 or more values as `beam:coder:tuple:v1`. This
  URN is not a standard coder, so runners and other SDKs see a length-prefixed blob.
- **Rust** registers `(A, B, C)` as `KV<A, KV<B, C>>` (`beam:coder:kv:v1`), for
  arities 3 to 6. Runners know the coder and add no length prefix. The bytes are
  the same as the Python tuple bytes, but Python decodes them as `(A, (B, C))`.
  Across languages, use `Row` for records.

Decimals:
- Beam has no standard decimal coder. **Java** `BigDecimalCoder` and **Python**
  `DecimalCoder` write a varint scale and then the two's-complement unscaled value,
  but neither has a portable URN.
- **Rust** registers `Decimal` as `beam:coder:bytes:v1`. The bytes payload is the
  whole-stream `BigDecimalCoder` encoding, so runners add no length prefix. In rows,
  all SDKs use the `beam:logical_type:decimal:v1` logical type.

## Portability and deployment

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| Portable Fn API harness | ✅ | ✅ | ✅ | ✅ | ✅ |
| Prism runner | ✅ | ✅ | ✅ | ✅ | ✅ |
| Dataflow runner | ✅ | ✅ | ✅ | ❌ | ✅ |
| Flink / Spark runners | ✅ | ✅ | ✅ | 🟡 | 🟡 untested |
| Live job handle (state, metrics, cancel, wait with timeout) | ✅ | ✅ | 🟡 | 🟡 | ✅ Prism, Dataflow |
| **Consume** cross-language transforms | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Provide** transforms to other SDKs | ✅ | ✅ | ❌ | ❌ | ✅ |
| Automated Java expansion service download | ❌ | ✅ | ✅ | ✅ | ✅ |
| Custom containers | ✅ | ✅ | ✅ | ✅ | ✅ |
| Dataflow Flex Templates | ✅ | ✅ | ? | ❌ | 🟡 experimental |
| Resource hints, per-transform | ✅ | ✅ | ❌ | ❌ | ✅ |
| Resource hints, pipeline-level | ✅ | ✅ | ✅ | ❌ | ✅ |
| Managed I/O | ✅ | ✅ | ❌ | ❌ | ✅ xlang |
| Lineage reporting | ✅ | ✅ | ❌ | ❌ | ✅ |
| Worker runs user code without rebuilding the pipeline | ✅ | ✅ | ✅ | ✅ | 🟡 |

Notes:
- **Worker without rebuilding the pipeline**: Java and Python serialize DoFns
  into the pipeline proto. Go looks up functions that the user registers by name.
  Rust uses both paths. A `DoFn`, `StatefulDoFn`, `SplittableDoFn` or
  `CombineFn` type with `#[derive(Register)]` travels in the pipeline graph as
  an id and its serialized fields. `Create` carries its elements. The SDK cannot
  serialize closures and the generic SDK transforms. While a pipeline has any of
  them, the worker rebuilds it.
  [static-registration.md](static-registration.md) describes both paths.
- **Providing** transforms means that the SDK ships an expansion service, so
  other SDKs can call its transforms. Go and TypeScript have only the generated
  gRPC stubs, not a server implementation. Rust ships `apache-beam-expansion`
  and the `beam-expansion-service`, `beam-gcp-expansion-service`, and
  `beam-kafka-expansion-service` binaries.
- **Consuming** cross-language transforms: the `beam:transforms:xlang:test:*` cases of
  `ValidateRunnerXlangTest.java` run as Rust pipelines on Prism. They run against the Java
  test expansion service (`validatesCrossLanguageRunnerJavaServicePrism`). They also run
  against the Python test expansion service (`validatesCrossLanguageRunnerPythonServicePrism`,
  which also runs `python_bs4`). See [validates-runner.md](validates-runner.md#cross-language-suites).
- Java does not download its *own* expansion service, because the downloaded
  service is the Java expansion service.
- For all SDKs, Flex Template support is mostly tooling outside this
  repository, so the source does not show the Go value.
- **Lineage reporting**: `beam::metrics::Lineage` (`Lineage::sources()`, `Lineage::sinks()`)
  records source and sink identifiers as string-set metrics (`lineage:sources`,
  `lineage:sinks`). The BigQuery, Bigtable, Kafka and Iceberg connectors report them.
- **Managed I/O** has the same model as Python.
  `beam::io::managed::{ManagedRead, ManagedWrite}` expand Java's
  `beam:transform:managed:v1` with the connector config as YAML. The connectors are
  Iceberg, Iceberg CDC read, Kafka, BigQuery, Postgres, MySQL, SQL Server and Delta Lake.
  Their constants are in `beam::io::managed`, for example `managed::ICEBERG`.
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
| TextIO / FileIO dynamic destinations | ✅ | ✅ | ❌ | ❌ |
| Local filesystem | ✅ | ✅ | ✅ | ✅ |
| GCS (`gs://`) | ✅ | ✅ | ✅ | ✅ |
| S3 (`s3://`) | ✅ | ✅ | ✅ | 🔜 |
| Azure Blob | ✅ | ✅ | 🟡 | ❌ |
| HDFS | ✅ | ✅ | ❌ | ❌ |
| Pub/Sub | ✅ | ✅ | ✅ xlang | ✅ xlang |
| BigQuery | ✅ | ✅ xlang | ✅ xlang | ✅ / xlang (Managed) |
| Bigtable | ✅ | ✅ | ✅ | ✅ |
| Kafka | ✅ | ✅ xlang | ✅ xlang | ✅ / xlang |
| JDBC (Postgres, MySQL, SQL Server) | ✅ | ✅ xlang | ✅ xlang | ✅ xlang |
| Iceberg | ✅ | ✅ xlang | ❌ | ✅ (write) / xlang |
| Delta Lake (read) | ✅ | ✅ xlang | ❌ | ✅ xlang |
| Avro | ✅ | ✅ | ✅ | ✅ |
| Parquet | ✅ | ✅ | ✅ | ✅ |
| TFRecord | ✅ | ✅ | ❌ | ✅ |
| Elasticsearch | ✅ | ✅ | ❌ | 🔜 |

Notes:
- **Pub/Sub**: native in Java and Python (Dataflow streaming). Go and Rust use
  cross-language SchemaTransforms through an expansion service
  (`beam:schematransform:org.apache.beam:pubsub_read:v1` / `write:v1`).
- **File compression**: Rust file sources and sinks support Gzip, Zstd, Deflate (zlib) and
  Snappy (framing format, `.sz`). They detect the codec from the file extension. Rust reads
  a compressed file whole and does not split it. Rust does not support Bzip2, xz/LZMA, Zip
  or LZO. Java `SNAPPY` (`.snappy`) uses a different Snappy format, so these files do not
  interoperate.
- **TFRecord**: Rust `beam::io::tfrecord::tfrecordio` (feature `tfrecord`) reads and writes
  uncompressed and compressed files. Uncompressed, Gzip and Deflate files are byte
  compatible with TensorFlow and the Java and Python SDKs. TensorFlow does not read Zstd or
  Snappy files.
- **BigQuery**: native in Java (`BigQueryIO`). Python and Go use cross-language Storage Read
  and Storage Write transforms from the Java SchemaTransform providers
  (`beam:schematransform:org.apache.beam:bigquery_storage_read:v1` / `bigquery_write:v1`).
  Rust has Storage Read and Storage Write connectors in `beam::io::gcp::bigquery::{read, write}`
  (feature `gcp-bigquery`), including `ReadRows::typed::<T>()` and `WriteRows::typed::<T>()` for
  `#[derive(BeamRow)]` structs. The Storage Write API cannot replace the rows of a table. For
  `WRITE_TRUNCATE`, load jobs, CDC writes and dynamic destinations, Rust uses `ManagedWrite`
  with `managed::BIGQUERY`.
- **Bigtable**: native in Java, Python and Go. Rust has its own connectors in
  `beam::io::gcp::bigtable::{read, write}` (feature `gcp-bigtable`). They support app
  profiles, materialized and authorized views, row filters, key ranges, write flow control
  and error outputs.
- **Kafka**: native in Java (`KafkaIO`) and Rust (`beam::io::kafka::{read, write}`, feature
  `kafka`, on librdkafka). Python and Go use cross-language SchemaTransform providers
  (`beam:schematransform:org.apache.beam:kafka_read:v1` / `kafka_write:v1`).
  `beam-kafka-expansion-service` serves the Rust connector to other SDKs. The Rust connector
  supports:
  - consumer and producer properties, and Google Cloud Managed Kafka authentication
  - bounded and unbounded reads, a reshuffle after the read (the `Redistribute` read
    option), and dead-letter error outputs
  - codecs in `beam::io::kafka::codec`: `Raw`, `Utf8`, `Json` (`beam::io::json`), `Avro`
    (`beam::io::avro::datum`, plain or with a Confluent `SchemaRegistry`), `Proto`
    (`beam::io::proto` on `prost-reflect`, with maps and groups), `BeamRow`, `Payload`
    (rows of one field), `Typed` (`Typed::new` on any row codec, also a codec of your own)
    and `WithMetadata` (keys, headers and timestamps)

  For dynamic topic writes or inline `.proto` schema text, Rust uses `ManagedRead` or
  `ManagedWrite` with `managed::KAFKA`.
- **Iceberg**: Rust `beam::io::iceberg::{Write, WriteRows}` (feature `iceberg`) appends a
  `#[derive(BeamRow)]` struct or a schema row to one table of a REST catalog. It works in
  batch and streaming. It writes Parquet data files through the SDK file systems and
  supports vended credentials and BigLake. It commits one snapshot for each table and
  triggering period. Dynamic destinations, upserts and the read go through Managed I/O.
- **Iceberg, Delta Lake & JDBC**: native in Java. Python and Rust use them through Managed
  I/O (`beam.managed.Read/Write` in Python, `beam::io::managed::{ManagedRead, ManagedWrite}`
  in Rust). Managed I/O expands the Java SchemaTransforms cross-language. Iceberg and the
  JDBC databases support read and write. Delta Lake is read-only. Go has no Managed API.
- **Dynamic destinations**: Java `FileIO.writeDynamic` and Python `WriteToFiles(destination=..)`
  write each element to a destination that the element selects. Rust file sinks write all
  elements to one destination.

## Ecosystem

| Capability | Java | Python | Go | TypeScript | **Rust** |
|---|:--:|:--:|:--:|:--:|:--:|
| `RunInference` / ML | ✅ xlang | ✅ | ✅ xlang | ❌ | ✅ |
| Beam YAML | ✅ | ✅ | ❌ | ❌ | 🔜 |
| Interactive / notebooks | ❌ | ✅ | ❌ | ❌ | ❌ |
| Package registry | Maven | PyPI | pkg.go.dev | npm | ❌ none |

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
