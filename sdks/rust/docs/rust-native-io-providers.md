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

# Rust-native I/O for other SDKs (cross-language providers)

**Status:** draft.

| Piece | State |
|---|---|
| Expansion service, worker and image for other SDKs | Works on Prism and Dataflow ([§2](#2-how-it-works)) |
| BigQuery Storage Read provider | Works, with snapshot reads, a stream count hint and an error output ([§7.2](#72-bigquery-storage-read)). Decision after the first benchmark: **go** ([§9](#9-benchmarks)) |
| BigQuery Storage Write provider | Works: at least once, exactly once in batch and in streaming, field selection (`keep`, `drop`, `only`, `ignore_unknown_values`) ([§7.3](#73-bigquery-storage-write)) |
| Java harness fix for a Java step between two Rust steps | Validated on Dataflow, 6 of 6 runs; PR [#40606](https://github.com/apache/beam/pull/40606) ([§4.1](#41-a-java-step-between-two-rust-steps-can-stop-the-job)) |
| Kafka, Bigtable, Pub/Sub providers | Kafka dependencies validated ([§7.6](#76-kafka-dependencies)); providers not started ([§8](#8-plan)) |

This document compares the Rust SDK with the other SDKs on purpose: the work is about how Python, Java and Go pipelines use Rust transforms. All work follows the [guiding principles](rfc-followups.md#guiding-principles) and [development.md](development.md).

---

## 1. Motivation and scope

The Rust SDK uses the Java I/O connectors through cross-language (xlang): `KafkaRead`, `BigQueryRead`, `BigtableRead` and the Pub/Sub transforms expand on the Java expansion service. This work goes the other way: connectors **in Rust**, for Python, Java and Go pipelines, through the Rust expansion service. The expected value:

- Flat memory: the I/O container has no JVM heap and no GC pauses.
- Fast startup: the Rust expansion service starts in milliseconds.
- Higher throughput, mostly where the wire format is columnar. BigQuery Storage Read returns Arrow, and the SDK uses `arrow`.

Priorities:

1. **Rust-native xlang I/O.** A Python user applies a Rust connector with `SchemaAwareExternalTransform`, pointed at the Rust expansion service.
2. **Later: Managed I/O.** The same connector can serve the Managed URNs, so a Managed transform can run in Rust instead of Java. [§6](#6-keeping-the-managed-door-open) keeps this possible.

### 1.1 Drop-in constraint

All changes stay in `sdks/rust`. Other SDKs use a Rust connector through the cross-language APIs that they have, as they use a Java connector:

| Step in the caller | Caller code | What Rust supplies |
|---|---|---|
| Lists providers and their config schemas | Python `SchemaAwareExternalTransform` calls `DiscoverSchemaTransform` ([external.py](../../python/apache_beam/transforms/external.py)) | `discover_schema_transform` in [lib.rs](../beam/expansion/src/lib.rs) |
| Sends the expansion request | Python `SchemaAwareExternalTransform` and `ExternalTransform`, Java `External`, Go `xlangx` | Both payload forms ([§2.1](#21-expansion-requests)) |
| Resolves the artifacts of new environments | Python, Java and Go call `ArtifactRetrievalService` only for environments with `dependencies` | Environments with no dependencies, so the server needs no artifact service |
| Uses the output coders | The caller decodes the PCollections of the expansion | Standard coder URNs at the boundary, mostly `beam:coder:row:v1` |
| Merges equal environments (Python only) | `merge_common_environments` in [pipeline_utils.py](../../python/apache_beam/runners/pipeline_utils.py) | The same environment proto from every expansion ([§4](#4-workers-and-scaling)) |
| Starts the worker | The runner starts the image that the environment names | The expansion service image ([§2.3](#23-image)) |

Rules:

- Phases 1 and 2 change nothing in `sdks/python`, `sdks/java`, `sdks/go`, `runners/` or `buildSrc`, tests included.
- If a phase needs a change outside `sdks/rust`, it stops and records the change in [§3](#3-gaps) first.

---

## 2. How it works

| Piece | Where |
|---|---|
| Expansion service | [`apache-beam-expansion`](../beam/expansion/src/lib.rs): the `SchemaTransformProvider` trait, `register_schema_transform!` (`inventory`) and the `beam-expansion-service` binary |
| GCP providers | `beam:schematransform:org.apache.beam:rust:bigquery_storage_read:v1` and `...:bigquery_storage_write:v1` in [`apache-beam-io-gcp-expansion-service`](../beam/io/gcp/expansion-service/src/lib.rs), binary `beam-gcp-expansion-service` |
| Connectors | [`gcp::bigquery::storage::read`](../beam/io/gcp/src/bigquery/storage/read/mod.rs) and [`write`](../beam/io/gcp/src/bigquery/storage/write/mod.rs), crate `apache-beam-io-gcp`, feature `bigquery-storage` |
| Worker features the connectors use | Splittable DoFn, self-checkpoint, bundle finalization, state and timers, concurrent bundles ([worker.rs](../beam/harness/src/worker.rs)) |

### 2.1 Expansion requests

The server decodes both payload forms ([lib.rs](../beam/expansion/src/lib.rs), `resolve_request`):

| Form | `spec.urn` | `spec.payload` | Sent by |
|---|---|---|---|
| SchemaTransform | `beam:expansion:payload:schematransform:v1` | `SchemaTransformPayload` | Python `SchemaAwareExternalTransform`, Python and Java Managed, the Rust client |
| External configuration | The URN of the transform | `ExternalConfigurationPayload` | Python `ExternalTransform`, the cross-language ValidatesRunner suites |

- The server decodes the row with the schema of the caller, then maps fields to the config schema of the provider by name. A caller can omit fields or change their order. An unknown field or another type is an error.
- Transforms get names under the transform of the caller (`<caller name>/<name>`), so names stay unique when a pipeline expands one provider many times.

### 2.2 Workers for expanded transforms

A Rust worker finds its handlers when it runs the pipeline construction again ([static-registration.md](static-registration.md)). A transform from an expansion has no user `main` on the worker, so the server and the worker use an **expansion replay**:

- The server rewrites each Rust `do_fn` of an expansion to `beam:dofn:rust:expanded:v1`. The payload holds the handler key and a `ReplayEntry`: provider, config, namespace, input components and an expansion id ([replay.rs](../beam/harness/src/replay.rs)). Runners pass the `do_fn` bytes through unchanged.
- The server builds the provider in a pipeline that holds only the input PCollections. The worker builds the same pipeline from the entry and gets the same keys, so `build_transform` must be deterministic.
- Each request gets its own expansion id, because caller namespaces repeat (`external_<N>` in Python, `External_<N>` in Java, random in Go).
- The worker replays each entry once for each expansion id and caches the handlers ([handlers.rs](../beam/harness/src/bundle_processor/handlers.rs)).
- The environment holds nothing expansion-specific, so all environments of one service are equal and callers can merge them ([§4](#4-workers-and-scaling)).
- Static registration can add a registered spec URN to the same resolve function. A provider with only registered DoFns then needs no replay.
- Not covered: custom window functions that use `window_into_handler_key`.

### 2.3 Image

One image is both the expansion service and the worker ([containers.md](containers.md)):

- `./gradlew :sdks:rust:gcpExpansionServiceImage` bakes `beam-gcp-expansion-service` into the SDK base image at `/opt/apache/beam/worker_binary` ([Dockerfile.expansion](../container/Dockerfile.expansion)). `./gradlew :sdks:rust:expansionServiceImage` does the same for `beam-expansion-service`.
- With `--port`, the container is the expansion service. It names itself with `--docker-image`, so the runner starts the same image as the worker through `boot`, which passes `--worker`.
- A job from a driver of another SDK has no Rust options snapshot, so the worker starts with default options.

### 2.4 Cross-language suites

The Python, Java and Go XVR suites pass on Prism and Dataflow against the Rust test expansion service ([xlang_transforms.rs](../beam/runner-tests/src/xlang_transforms.rs)): `./gradlew :sdks:rust:validatesCrossLanguageRunner{Python,Java,Go}{Prism,Dataflow}` ([validates-runner.md](validates-runner.md)).

- `-Ptests=prefix,group_by_key` selects test methods by name (pytest node id in Python, JUnit `--tests` in Java, `go test -run` in Go).
- The server matches test URNs by name when the caller sends an unprefixed URN (as Go `TestXLang_Multi` does with `multifn`).
- A Rust combine runs as written: `PartialCombine`, `GroupByKey`, `MergeAccumulators`. The server removes the `combine_per_key` spec, because Dataflow lifts only composites that it can rebuild. `CombineGlobally` adds a default value on empty input, so Go `TestXLang_CombineGlobally` receives `[0]` when the input is empty.

---

## 3. Gaps

### 3.1 Python usability

Python needs nothing new: `SchemaAwareExternalTransform` works against any expansion service with discovery ([bigquery_storage_test.py](../beam/io/gcp/expansion-service/tests/python/bigquery_storage_test.py)):

```python
import apache_beam as beam
from apache_beam.transforms.external import SchemaAwareExternalTransform

URN = 'beam:schematransform:org.apache.beam:rust:bigquery_storage_read:v1'

with beam.Pipeline() as p:
  rows = p | SchemaAwareExternalTransform(
      URN,
      'localhost:8097',
      table_spec='my-project.my_dataset.my_table',
      billing_project='my-project',
      selected_fields=['id', 'name'],
      row_restriction='id > 0')
```

Optional additions: a `RustExpansionService` helper that starts the container, as `BeamJarExpansionService` does for Java, and typed wrappers generated from discovery.

### 3.2 Decimal precision in Python

The Python `DecimalCoderImpl` ([coder_impl.py](../../python/apache_beam/coders/coder_impl.py)) decodes `beam:logical_type:decimal:v1` in the default context, with 28 significant digits. Python rounds a longer `NUMERIC` or `BIGNUMERIC` value from any SDK. The fix is in Python, outside `sdks/rust`, so the Python type test uses values with at most 28 digits.

### 3.3 Row decoding in Python

Python decodes the rows of every cross-language read, and that limits the end-to-end throughput ([§9.3](#93-where-the-time-goes)). The row coder decodes each `STRING` field with a Python-level call, and the gRPC thread of the data plane shares the GIL with that work. A faster string path in the Python row coder helps every cross-language source. The fix is outside `sdks/rust`.

---

## 4. Workers and scaling

The runner starts one SDK worker for each **environment**, not for each transform, and fuses the transforms of one environment into stages. Every VM runs one container for each environment, so all environments scale together. Elements between environments cross the Fn API data plane.

- The Rust worker declares `beam:protocol:multi_core_bundle_processing:v1`, so each VM runs one multi-threaded Rust container.
- All environments of one Rust expansion service are equal. Python merges them into one Rust worker. Java and Go callers do not merge environments, so a Java pipeline with a Rust read and a Rust write runs two Rust containers on each VM.
- One worker for each VM is better than many: one tokio runtime, one set of client connections and one control stream.

### 4.1 A Java step between two Rust steps can stop the job

**Summary.** A Java pipeline that reads with Rust, changes the rows in a Java step and writes with Rust can stop on Dataflow with Beam 2.76.0. The cause is a flow-control deadlock between the Dataflow runner and the Java SDK harness (internal bug b/280697034). The Rust harness is not the cause. PR [#40606](https://github.com/apache/beam/pull/40606) ([`java/fn-data-elastic-inbound`](https://github.com/prodriguezdefino/beam-pabs/tree/java/fn-data-elastic-inbound)) fixes the deadlock in the Java harness without workarounds or experiment flags.

**Workaround on Beam 2.76.0.** Add `--experiments=data_buffer_size_limit=10000000` to the pipeline options. A `Reshuffle` after the Java step is not enough: with autoscaling, the job stops before the `Reshuffle`.

**When it occurs.** Dataflow fuses the Rust read, the Java step and the Rust write into one stage that uses three containers. The runner moves the rows of each work item from container to container in one sequence. The deadlock needs a Java step that receives many rows from the runner and also sends many rows to the runner.

| Pipeline | 4 fixed workers | Autoscaling from 1 to 10 |
|---|---|---|
| Rust read → Java count | Completes | Completes |
| `BigQueryIO` read → Java step → Rust write | Completes | Completes |
| Rust read → Java step → `BigQueryIO` write | Completes | Completes |
| Python pipeline: Rust read → Python step → Rust write | Completes | Completes |
| Rust read → Java step → Rust write (Beam 2.76.0) | Stops in about 8 of 10 runs | Stops |
| Same, with a `Reshuffle` after the Java step (Beam 2.76.0) | Completes (1 run) | Stops after a resize (1 run) |
| Same, with `data_buffer_size_limit=10000000` (Beam 2.76.0) | Completes | Completes |
| Same, with the Java harness fix (PR [#40606](https://github.com/apache/beam/pull/40606)) | Completes (4 of 4 runs) | Completes (5 of 5 runs, `exactly_once` and `at_least_once`) |
| Same, with larger inbound buffers instead (rejected design) | Completes, Java peak 11 GiB | Stops |

**How the deadlock occurs.** `DirectStreamObserver` sends 100 messages (about 100 MB) before it checks `isReady()`. When `isReady()` is false, the bundle thread waits for the `onReady` callback. At the same time, the input queue of the bundle (100 messages) becomes full, and the gRPC thread waits in `CancellableQueue.put()`. In gRPC-Java, `onNext` and `onReady` share one sequential executor for each call. Because `onNext` blocks that executor, `onReady` cannot run when the output stream becomes ready. The bundle thread then waits up to 60 seconds for a timeout while the runner fills its shared byte limit with upstream data.

The Java step, its gRPC thread and the runner wait for each other. Every 2 minutes the runner sends one message ("Ignoring data element byte limit … to avoid stuckness"). As a result, the job is very slow or stops. With 10 MB output chunks, 100 messages equal 1 GB, so the Java step rarely reaches the `isReady()` check.

**How the Java harness fix works.** PR [#40606](https://github.com/apache/beam/pull/40606) ([`java/fn-data-elastic-inbound`](https://github.com/prodriguezdefino/beam-pabs/tree/java/fn-data-elastic-inbound)) changes two classes in `sdks/java/core`:

- `BeamFnDataGrpcMultiplexer` uses manual gRPC flow control (`disableAutoRequestWithInitial(16)`) and non-blocking queue handoff (`tryAcceptWithCallback`). Each bundle queue keeps its 100-message bound, and `onNext` never blocks the sequential executor that runs `onReady`. When an outbound write waits 500 ms on flow control, the multiplexer requests one extra inbound message up to 100 messages per active bundle.
- `DirectStreamObserver` checks `isReady()` before every message (`DEFAULT_MAX_MESSAGES_BEFORE_CHECK = 0`) and polls readiness every 50 ms.

**Validation of the fix (2026-10-14).** Commit `20d81c987b` on [`java/fn-data-elastic-inbound`](https://github.com/prodriguezdefino/beam-pabs/tree/java/fn-data-elastic-inbound) (PR [#40606](https://github.com/apache/beam/pull/40606)), Java copy of §9 (Rust read, Java step, Rust write, `exactly_once`), Beam 2.76.0 with the patched classes. Every run completes, with no "to avoid stuckness" message, and the copy check passes.

| Workers | Runs | Wall (s) | Processing (s) | vCPU·s | Java peak (MiB) |
|---|---|---|---|---|---|
| 4 fixed | 3 | 476–580 | 327–386 | 5,879–6,363 | 4,178–4,196 |
| 1–10 (peak 4–6) | 3 | 680–696 | 504–529 | 6,980–7,761 | 4,181–4,207 |

A design that queues more inbound data while output waits, up to a fifth of the heap, completes on 4 fixed workers with an 11 GiB Java peak, but stops with autoscaling (12 "to avoid stuckness" messages, the pool shrinks to 1 worker). More buffering only moves the point where the deadlock starts.

<a id="how-to-test-a-java-harness-change-on-dataflow"></a>**How to test a Java harness change on Dataflow.** `BeamFnDataGrpcMultiplexer`, `BeamFnDataInboundObserver`, `DirectStreamObserver` and `CancellableQueue` are in `beam-sdks-java-core`, not in the harness jar. The Dataflow worker loads them from the jars that the job stages, which are the released 2.76.0 jars of the benchmark. A Java SDK container built from the tree does not change them, so such a run tests stock 2.76.0. Use one of these:

- Add the compiled `org/apache/beam/sdk/fn` classes of the change to `/opt/apache/beam/jars/beam-sdks-java-harness.jar` of `gcr.io/cloud-dataflow/v1beta3/beam_java21_sdk:2.76.0`, push the image, and pass `-PextraArgs="--sdkHarnessContainerImageOverrides=.*beam_java.*_sdk:.*,<image>"`. The harness jar comes before the staged core jar on the worker classpath. The changed classes must differ from 2.76.0 only in the change.
- The route of the Beam [code change guide](../../../contributor-docs/code-change-guide.md): publish the harness and core to Maven Local, build the pipeline against the snapshot, and run with `--experiments=use_runner_v2,use_staged_dataflow_worker_jar`.

Before a result counts, check that the pushed jar holds the changed classes, and that the worker logs show the image (`ContainerCreate`).

**Evidence.**

- Rust write: the data stream is open, the input queues are empty, and no input arrives for 60 to 240 seconds. The runner sends no progress or split requests to these bundles.
- Rust read: it waits until the runner reads its output.
- Java step: the lull report shows the bundle thread in `DirectStreamObserver.onNext`, called from `TouchFn.process`. The gRPC thread waits in `CancellableQueue.put`.
- Runner: "Ignoring data element byte limit … to avoid stuckness" for the output of the Java step.
- Two versions of the Rust harness and every set of declared capabilities give the same result.
- A local test with in-process gRPC and a fully serial runner causes the same deadlock with the Java data plane classes. The test runner writes a whole batch before it reads any output, which is stricter than Dataflow, and it also stops the fix. So the test reproduces the problem but does not validate a fix. Validate on Dataflow ([below](#how-to-test-a-java-harness-change-on-dataflow)).

**References.**

- b/280697034 (internal): the same deadlock with two Java containers in one fused stage. It was closed with smaller elements in the user pipeline, not with a change to Dataflow or Beam.
- Apache Beam PRs #26631 and #33419 change `DirectStreamObserver` for a stream that ends. They do not prevent this deadlock. The jobs used the Java container `beam_java21_sdk:2.76.0`, which includes both.

**Follow-ups.**

- Merge Apache Beam PR [#40606](https://github.com/apache/beam/pull/40606) ([`java/fn-data-elastic-inbound`](https://github.com/prodriguezdefino/beam-pabs/tree/java/fn-data-elastic-inbound)), which references #20028.
- A local test of the fix needs a fake runner that reads output while it writes input and holds a byte limit, as Dataflow does.

---

## 5. Validation

The other SDKs run XVR suites against each expansion service (`createCrossLanguageValidatesRunnerTask` in [BeamModulePlugin.groovy](../../../buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy)) and I/O suites by marker ([test-suites/xlang/build.gradle](../../python/test-suites/xlang/build.gradle)). All are Gradle tasks; CI only calls them.

For Rust:

- The Python, Java and Go XVR suites run on Prism and Dataflow ([§2.4](#24-cross-language-suites)), defined in [`gradle/xlang.gradle`](../gradle/xlang.gradle) and its language scripts (`xlang-python.gradle`, `xlang-java.gradle`, `xlang-go.gradle`), with no change to the other SDKs.
- Each expansion service crate keeps its connector tests in `tests`:
  - `tests/<transform>_test.rs`: configuration and registration, with no cloud calls.
  - `tests/python/<connector>_test.py`: Python pipelines apply the transforms of one connector and check the Python type of every value. The transforms of one connector share one module and its test tables, for example [bigquery_storage_test.py](../beam/io/gcp/expansion-service/tests/python/bigquery_storage_test.py).
- `./gradlew :sdks:rust:gcpExpansionServicePythonTest` runs the modules on Prism. `-Ptests=<connector>` selects modules, `-PtestFilter=<expression>` selects tests (pytest `-k`).
- `gcpExpansionServicePythonTestDataflow` runs the same modules on Dataflow. It pushes a linux/amd64 image to `dataflowRepositoryRoot`, and gives the options to the tests in `PIPELINE_OPTIONS`. The settings come from `sdks/rust/.local/sdk.properties`.
- A test that cannot pass on one runner is skipped there, with the reason in the skip.

---

## 6. Keeping the Managed door open

- **Own URNs.** Rust connectors register their own identifiers (`beam:schematransform:org.apache.beam:rust:...`), so nothing resolves to Rust by accident.
- **Config like the Java provider.** The same field names and types, for example `table_spec`, `query`, `row_restriction`, `selected_fields`. Rust-only options are extra nullable fields.
- **Output tags like Java.** `output`, and `errors` with the standard `error_handling` config.
- **Aliases later.** One provider can get more identifiers. A Managed dispatcher then parses the YAML config and calls the provider.
- **Discovery diff as a CI check** for providers that claim compatibility.

---

## 7. Connectors

| Connector | Rust client | What Python uses | Where Rust can do better | Effort |
|---|---|---|---|---|
| **BigQuery Storage Read** | Official `google-cloud-bigquery` 0.18 (`Read` client) | Native, or Java xlang through Managed | Arrow responses go into Rust Arrow. Read streams map to SDF restrictions with dynamic splits | Done |
| **BigQuery Storage Write** | Same crate (`Write` client) | Java xlang (`STORAGE_WRITE_API`) | Arrow appends, no JVM | Done |
| **Kafka** | `rdkafka` 0.39 (static `librdkafka` 2.12.1, [§7.6](#76-kafka-dependencies)) | Java xlang only | No JVM, flat memory in streaming | Write S. Read M–L |
| **Bigtable** | No official data-plane crate. Own stubs need `ReadRows`, `MutateRows`, `SampleRowKeys` | Native Python and Java xlang | Options that the Java provider does not expose: app profile, row filters, flow control | M |
| **Pub/Sub** | Official `google-cloud-pubsub` 1.6 | Native. Dataflow replaces both with its own implementation | Runners other than Dataflow ([§7.1](#71-pubsub-caveats)) | S–M |
| Postgres / MySQL | `sqlx` 0.9 | Java xlang (JDBC) | No JVM and no JDBC driver | S |
| Iceberg | `iceberg` 0.10 | Java xlang | Strategic, write path not mature | L |

**Client library policy.** Use the client that Google or the project supports when it covers the API. Otherwise generate stubs with `tonic-prost-build` (the default for Bigtable). Check the choice again when the phase starts.

### 7.1 Pub/Sub caveats

- **Dataflow replaces Pub/Sub I/O** with its own implementation. A Rust connector serves other runners, and pipelines that choose it on purpose.
- **Backlog for autoscaling.** Dataflow reads the oldest unacknowledged message age from an internal API. A Rust connector can use Cloud Monitoring `subscription/oldest_unacked_message_age` (about 60 s late; poll once for each subscription), or the oldest unacknowledged `publish_time` that the worker holds.
- **SDF backlog.** Check that the Dataflow streaming autoscaler uses the backlog that a Rust worker reports. The same question applies to Kafka.
- **Acks** in the bundle finalizer, with `modifyAckDeadline` while the runner commits. **Watermark** from `publish_time`.

### 7.2 BigQuery Storage Read

API: [read/mod.rs](../beam/io/gcp/src/bigquery/storage/read/mod.rs) (`ReadOptions`, `ReadRows`, `ReadRows::typed`). Provider config: `BigQueryStorageReadConfig` in [lib.rs](../beam/io/gcp/expansion-service/src/lib.rs).

- **Sources.** Table reads (`table_spec`, `row_restriction`, `selected_fields`) and query reads (`query`, `query_location`, `query_priority`, `temp_dataset`, `use_legacy_sql`, `flatten_results`, `kms_key`). A query read runs a job into a temporary table, reads it, and deletes it after the last stream ([query.rs](../beam/io/gcp/src/bigquery/storage/read/query.rs)).
- **Splitting.** `CreateReadSession` divides the table into streams, and a reshuffle spreads them. Each stream is one SDF restriction, `[offset, i64::MAX)`. A checkpoint resumes after the rows read. A dynamic split calls `SplitReadStream`: the reader continues in the primary stream, and the residual reads the remainder.
- **Size and progress** in bytes: the estimated rows of a stream times the average row size. The worker reports the progress of the current element, so Dataflow sizes the pool from the real backlog.
- **Schema.** The provider looks up the schema when it expands: a read session with one stream, or a dry-run job. So the expansion service needs credentials, and a worker that replays the expansion looks it up again. `ReadRows::with_schema` skips the lookup.
- **Types.** `DATETIME` and `TIME` become strings, because no logical type for them decodes in every SDK. `TIMESTAMP` becomes `micros_instant`. `NUMERIC` and `BIGNUMERIC` become `decimal` ([schema.rs](../beam/io/gcp/src/bigquery/storage/schema.rs)).
- **Options.** Arrow buffer compression (`NONE`, `LZ4_FRAME`, `ZSTD`), the billing project from `--project` or `billing_project`, typed reads into `#[derive(BeamRow)]` types that select only their fields.
- **Table reads.** `snapshot_time` (RFC 3339, checked before any call) and `preferred_min_stream_count` (at most `max_stream_count` when that is set).
- **Error handling.** Without `error_handling`, the provider calls `ReadRows::try_expand`: the stream reader emits the rows, with no step after it and no work for each row. The first row that does not convert fails the bundle. With `error_handling`, the provider calls `ReadRows::try_expand_with_failures`. Its stream reader declares `type Out = Result<RowElement, Row>`, so it emits rows to `output` and error rows to the named output itself, with no step after it, and a row stays row coder bytes ([§7.5](#75-moving-the-bigquery-connectors-to-the-typed-outputs-of-the-sdk)). An error row has `failed_row` (the Arrow values as text), `error_message`, `stream` and `offset`. Only a batch with a bad row converts again, one row at a time. A bad row is in a claimed range, so the restriction counts it as read.
- **Which conversions can fail.**

  | Failure | Provider (session schema) | `with_schema` | `typed` |
  |---|---|---|---|
  | Column type does not match the field type, or a required field has no column | no | yes, every row of the batch | yes, fails the pipeline |
  | Integer out of range for a narrower field (`INT64` column into `INT16`) | no | yes, that row | yes, fails the pipeline |
  | Null in a field that is not nullable | no | yes, that row | yes, fails the pipeline |
  | `DATETIME` or `TIME` outside the range of the conversion to text | no: BigQuery values are in range | no | no |

  The provider reads with the schema of the session, so no value from BigQuery fails. Its error output is for parity with the write provider and stays empty. `typed` has no error output: `ReadTyped` uses `try_expand` and then converts each row into the type.

- **Not covered:** lineage metrics (needs a Lineage API in core), and `sample_percentage`.

### 7.3 BigQuery Storage Write

API: [write/mod.rs](../beam/io/gcp/src/bigquery/storage/write/mod.rs) (`WriteOptions`, `WriteRows`). Provider: `BigQueryStorageWriteProvider` in [lib.rs](../beam/io/gcp/expansion-service/src/lib.rs).

**Fields.** `keep`, `drop` or `only` selects the input fields that the write puts in the table. Set at most one of them. `only` names a row field, and the fields of that row become the columns. With `ignore_unknown_values`, the write does not write a selected field that the table has no column for. Without it, such a field fails the write and the error names the field. The write computes the selection once. The Arrow conversion steps over the other fields in the row coder bytes and builds no column for them. A row that does not convert goes to the error output as the input row. A row that BigQuery rejects, or that is larger than a request, holds only the written fields, and the other fields are null.

`num_streams` is `INT32`, as in the Java configuration. Python sends every integer as `INT64`, so the expansion service accepts any integer type for an integer field when the value fits. A value that does not fit is an error that names the field.

The crate gives `DefaultWriter` (at least once), `PendingWriter` (offsets, `finalize`) and `BufferedWriter` (offsets, `flush`, `finalize`), all with Arrow. [commit.rs](../beam/io/gcp/src/bigquery/storage/write/commit.rs) sends `BatchCommitWriteStreams` with `tonic`, because the crate commits only one stream.

One step creates or checks the table before any row, so the `WRITE_EMPTY` check runs once. The writers read the column types as a side input. Each field takes the Arrow type that a read session gives for its column, so a write accepts what the read returns.

| Guarantee | Input | Shape |
|---|---|---|
| At least once | any | No shuffle. Each bundle appends Arrow batches to the default stream, and `finish_bundle` waits for all appends. |
| Exactly once | bounded | No shuffle. Each bundle appends to its own pending stream at offsets, finalizes it and checks the row count. A `GroupByKey` by table gives all streams to one `BatchCommitWriteStreams` call. |
| Exactly once | unbounded or `--streaming` | Arrow batches with a shard key (one shard for each DoFn copy, or `num_streams`), then a stateful step that keeps the buffered stream and next offset of each shard. A processing-time timer flushes. |

Recovery:

- **Bounded.** A failed bundle leaves a pending stream that nobody commits, so BigQuery drops its rows. The retry writes a new stream. The commit is atomic for the table.
- **Unbounded.** The runner commits the offset with the bundle, and the flush timer fires only after that commit, so a flush makes only committed rows visible. A retried bundle finds its offset written: it flushes the rows before the offset, finalizes the stream and continues in a new one. At the end of the input, a timer flushes and finalizes each stream and clears its flush timer, so a drained job ends.
- **Flush and finalize** count only when BigQuery confirms every committed row. Any other answer fails the bundle with the stream and offsets.
- **Errors.** Rows that do not convert and rows that BigQuery rejects go to the error output (`failed_row`, `error_message`; see [§7.5](#75-moving-the-bigquery-connectors-to-the-typed-outputs-of-the-sdk)), and the other rows of the request go again. Without `error_handling`, a rejected row fails the pipeline, and the empty output `post_write` is the output, because the Java `External` transform fails on an expansion with no output.

Defaults, the same as the Java SDK:

| Setting | Value |
|---|---|
| Rows for one append | 2 MiB or 150,000 rows. A buffer converts once and splits only above one request |
| Largest request | 10 MB. A larger row is an error row |
| Appends in flight for each writer | 1,000 requests and 20 MiB |
| Append retries | 500, waits from 1 s to 20 s |
| Finalize, flush and commit retries | 3, waits of at most 1 minute |
| Flush interval (unbounded) | 5 s (`triggering_frequency_seconds`; ignored for bounded input) |
| Idle time before a stream is finalized | 1 hour |
| Shards (unbounded) | One for each DoFn copy, or `num_streams` (needed with `auto_sharding: false`) |

**Not covered:** dynamic destinations, CDC writes, schema changes of the table during the write, and `WRITE_TRUNCATE`, which the Storage Write API cannot do.

**Tests.** [storage_write.rs](../beam/io/gcp/tests/bigquery_test/storage_write.rs), [storage_write_schema.rs](../beam/io/gcp/tests/bigquery_test/storage_write_schema.rs) and [bigquery_storage_write_test.rs](../beam/io/gcp/expansion-service/tests/bigquery_storage_write_test.rs) need no cloud calls. [bigquery_storage_test.py](../beam/io/gcp/expansion-service/tests/python/bigquery_storage_test.py) copies an all-types table through the Rust read and write in each shape, creates a table and checks the error output.

The unbounded test runs on Prism and on Dataflow: `./gradlew :sdks:rust:gcpExpansionServicePythonTest -Ptests=bigquery_storage -PtestFilter=unbounded` (Prism) or `:sdks:rust:gcpExpansionServicePythonTestDataflow` with the same options.

### 7.4 Row data path

Rows cross the Fn API as `beam:coder:row:v1` bytes. The connectors convert between those bytes and Arrow with no Beam `Row` in between:

- **Into Arrow.** A writer takes [`RowElement`](../beam/core/src/schema/row.rs): the row coder bytes of an element from another worker or SDK, or the `Row` of a fused Rust step, with no encode (`PCollection::as_row_elements`). [`EncodedRowsBuilder`](../beam/io/arrow/src/convert/encoded.rs) and [`encoded_rows_to_record_batch`](../beam/io/arrow/src/convert/encoded.rs) read the bytes into the same Arrow builders that [`rows_to_record_batch`](../beam/io/arrow/src/convert/to_arrow.rs) fills from rows ([columns.rs](../beam/io/arrow/src/convert/columns.rs)). [`ParquetSink::for_rows`](../beam/io/parquet/src/sink.rs) and [`AvroSink::for_rows`](../beam/io/avro/src/sink.rs) stream `RowElement`s into `EncodedRowsBuilder` (`parquetio::Write` and `avroio::Write` accept both `PCollection<Row>` and `PCollection<RowElement>`).
- **Out of Arrow.** [`record_batch_to_encoded_rows`](../beam/io/arrow/src/convert/encoded.rs) writes the row coder bytes of each row from the same readers that [`record_batch_to_rows`](../beam/io/arrow/src/convert/from_arrow.rs) uses ([readers.rs](../beam/io/arrow/src/convert/readers.rs)). `ReadRows::with_encoded_rows` (in BigQuery Storage Read, Parquet and Avro) and `ReadRowFiles::with_encoded_rows` (in Parquet and Avro) emit them; the read provider uses it. A Rust read emits rows by default.
- Tests check that both paths give the same batches and the same bytes ([roundtrip.rs](../beam/io/arrow/tests/roundtrip.rs), [parquetio_test.rs](../beam/runner-tests/tests/parquetio_test.rs), [avroio_test.rs](../beam/runner-tests/tests/avroio_test.rs)).

Follow-ups, in their own work:

| Connector | Change |
|---|---|
| `#[derive(BeamRow)]` types | Convert a struct to and from Arrow with no `Row`, for Rust pipelines |

### 7.5 Moving the BigQuery connectors to the typed outputs of the SDK

**Status: done** on `rust/native-io-providers`, as described below. The read reports its failures under the output name `failures`, and the write under `errors`.

The SDK declares the outputs of a `DoFn` or a `SplittableDoFn` with its `Out` type ([rfc-followups.md, F4](rfc-followups.md#f4-dofn-feature-detection)). `Out = Result<T, F>` sends `Ok` to `output` and `Err` to `errors`, and `ParDo` or `SplittableParDo` returns `WithFailures<T, F>`. Each output gets the coder of its own type. The SDK has no `ParDoMulti`, `output_to` or string tags. `finish_bundle` gets a `FinishBundleContext` that requires a window and a timestamp for each output. The steps below move both connectors from tagged outputs to typed outputs, which makes them simpler.

**Read.** The stream reader emits `ReadOutput` with a hand-written `KV<varint, bytes>` coder. A `SplitFailures` `ParDoMulti` then splits it, `two_outputs` unpacks the list by position, and `retype` fixes the coder of the error collection. With typed outputs, the reader emits both kinds itself:

```rust
// reader.rs: one `StreamOutput` per shape of the read
impl StreamOutput for Result<RowElement, Row> {
    fn emit_batch(batch: &RecordBatch, schema: &Arc<Schema>, output: RowOutput,
                  first_row: (&str, i64), ctx: &mut ProcessContext<'_, Self>) -> beam::Result {
        read_outputs(batch, schema, output, first_row)?   // Vec<Result<RowElement, Row>>
            .into_iter()
            .try_for_each(|output| ctx.emit(output))
    }
}

// mod.rs, try_expand_with_failures
let WithFailures { output, failures } = streams.apply(SplittableParDo::new(
    format!("{name}/ReadStreams"),
    ReadStreamFn::<Result<RowElement, Row>>::new(schema, client, encoded),
));
let output = output.as_rows().with_row_schema(&streams.schema);
let failures = failures.with_row_schema(error_schema());
```

- Remove `ReadOutput`, `ReadOutputCoder`, `SplitFailuresFn`, the `ROW_TAG`/`FAILURE_TAG` constants, `two_outputs` and `retype`. `read_outputs` returns `Ok(row)` or `Err(error_row(..))`.
- One fewer step and no wrapping: a row stays in its row coder bytes with no `KV` around it, and the error collection has the row coder from the start.
- Without `error_handling`, `ReadStreamFn<RowElement>` stays as it is: a single output, with no work for each row.
- `ReadTyped` can get an error output the same way, with `Result<T, Row>`, instead of failing the pipeline on the first row that does not convert.
- The reader also needs the smaller SDF changes: drop `type Position`, and if it reports boundedness, return `IsBounded`.

**Write.** `EncodeFn` and `WritePendingFn` send rejected rows with `ctx.output_to(ERRORS_TAG, row)` from a `ParDoMulti::new(.., ["out", ERRORS_TAG], ..)`, then use `two_outputs` and `retype`. Declare the error output in the type instead:

| Step | `type Out` with tagged outputs | `type Out` with typed outputs |
|---|---|---|
| `EncodeFn` | `(i64, EncodedBatch)` + `output_to(ERRORS_TAG, row)` | `Result<(i64, EncodedBatch), Row>` |
| `WritePendingFn` | `(String, String)` + `output_to(ERRORS_TAG, row)` | `Result<(String, String), Row>` |
| `WriteDefaultFn`, `WriteBufferedFn` | `Row` (the errors only) | Unchanged |

```rust
// encode.rs, flush
errors.into_iter().try_for_each(|row| ctx.emit(Err(row)))?;
batches.into_iter().try_for_each(|batch| ctx.emit(Ok((self.shard, batch))))

// mod.rs
let WithFailures { output: batches, failures } =
    rows.apply(ParDo::new(format!("{name}/Encode"), encoder));
```

- Remove `two_outputs` and `retype`. The SDK gives the error output the `errors` tag, which is the tag of the Java connector. `add_composite` still names the output of the composite. Keep `with_row_schema(&errors)` on the flattened error collection.
- **Windows in `finish_bundle`.** `EncodeFn`, `WritePendingFn` and `WriteDefaultFn` flush buffered rows in `finish_bundle`. Without a window, the outputs take the last header of the bundle. With `FinishBundleContext`, they must name the window. Rewindow the rows into the global window at the start of the write. The steps then emit with `ctx.emit(Ok(batch), BoundedWindow::Global(GlobalWindow), Instant::END_OF_GLOBAL_WINDOW)`, and error rows the same way. The table and the commit do not depend on the window of a row, so nothing else changes.
- **Timers of `WriteBufferedFn`.**
  - `set_timer_after(ShardTimer::Flush, d)` becomes `ctx.timer(ShardTimer::Flush).after(d)?`.
  - `set_timer(ShardTimer::Expiry, BoundedWindow::TIMESTAMP_MAX_VALUE)` becomes `ctx.timer(ShardTimer::Expiry).at(ctx.window()?.max_timestamp())?`.
  - `clear_timer` becomes `.clear()`.
  - The processing-time flush timer holds the watermark at the timestamp of the element, not at the wall-clock firing time. The comment about a pending flush timer holding the watermark until a drain still applies.

**Order.** Rebase the connectors onto the typed outputs before they gain more outputs: dynamic destinations and CDC writes ([§7.3](#73-bigquery-storage-write)) can then declare them as types too. For outputs that differ in more than success and failure, use an enum with `#[derive(Outputs)]`. An example is a write that returns written rows, failed rows and committed streams, each of its own type.

### 7.6 Kafka dependencies

Validated in `beam_rust_builder` (`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`) and in the runtime base image `gcr.io/distroless/cc-debian12:latest`:

| Crate | Version | License | Compression | TLS & Auth | Verdict |
|---|---|---|---|---|---|
| `rdkafka` (`rdkafka-sys` + bundled `librdkafka` 2.12.1) | 0.39.0 | MIT (`rdkafka`), BSD-2-Clause (`librdkafka`, ASF Category A) | `none`, `gzip`, `snappy`, `lz4`, `zstd` | `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER` with custom token callback) | **Selected** |
| `rskafka` (pure Rust) | 0.6.0 | MIT OR Apache-2.0 | `gzip`, `lz4`, `snappy`, `zstd` | TLS (`rustls`), no SASL (`PLAIN`, `SCRAM`, `OAUTHBEARER`), no consumer group offset management or `consumer_config_updates` / `producer_config_updates` map | Not sufficient for Managed Kafka or full provider config |

Build and runtime requirements for `rdkafka`:

- **Cargo features:** `rdkafka = { version = "0.39", default-features = false, features = ["tokio", "libz-static", "zstd", "ssl-vendored"] }`.
- **Distroless runtime:** `gcr.io/distroless/cc-debian12:latest` includes `libc.so.6`, `libm.so.6`, `libgcc_s.so.1`, `libstdc++.so.6` and `libssl.so.3`, but does not include `libz.so.1`, `libzstd.so` or `libsasl2.so`. With `libz-static`, `zstd` and `ssl-vendored`, the worker binary links only `libc.so.6`, `libm.so.6` and `libgcc_s.so.1`.
- **Cross-compilation in `Dockerfile.builder`:** `rdkafka-sys` without `cmake-build` runs `./configure`, which reads `CC` and `CXX` rather than `CC_<target>` and `CXX_<target>`. Cross-compiling to `x86_64-unknown-linux-gnu` or `aarch64-unknown-linux-gnu` needs `CC=<arch>-linux-gnu-gcc` and `CXX=<arch>-linux-gnu-g++` set for the target build. Do not enable `cmake-build` without `curl-static`, because `librdkafka`'s `CMakeLists.txt` enables OIDC (`#include <curl/curl.h>`) when `WITH_SSL` and `WITH_SASL_OAUTHBEARER` are on.
- **Record formats and auth in the workspace:** `serde_json` (JSON), `arrow-avro` (Avro), `prost` / `prost-types` (Proto) and `google-cloud-auth` (OAuth2 tokens for Google Cloud Managed Service for Apache Kafka with `OAUTHBEARER`) are already workspace dependencies.

---

## 8. Plan

1. **Phase 1: BigQuery Storage Read and a go or no-go decision.** The decision rule: go if the Rust provider does clearly better than the Java provider on at least one metric and is not clearly worse on the others. Result: **go** ([§9](#9-benchmarks)).
2. **Phase 2: more SDKs and connectors.**
   - BigQuery Storage Write ([§7.3](#73-bigquery-storage-write)). Done.
   - BigQuery Storage Read options (`snapshot_time`, `preferred_min_stream_count`) and an error output ([§7.2](#72-bigquery-storage-read)). Done.
   - The Java harness fix for a Java step between two Rust steps ([§4.1](#41-a-java-step-between-two-rust-steps-can-stop-the-job)). Validated; PR [#40606](https://github.com/apache/beam/pull/40606).
   - The Java and Go XVR suites against the Rust test expansion service ([§5](#5-validation)). Done.
   - Kafka (streaming, flat memory; dependencies validated in [§7.6](#76-kafka-dependencies)), Bigtable (more options than the Java provider) and Pub/Sub (runners other than Dataflow).
3. **Later: Managed compatibility** ([§6](#6-keeping-the-managed-door-open)).

---

## 9. Benchmarks

`./gradlew :sdks:rust:bigQueryBenchmark` runs one job on Dataflow ([benchmark.gradle](../scripts/benchmarks/bigquery/benchmark.gradle), [bigquery_benchmark.py](../scripts/benchmarks/bigquery/python/bigquery_benchmark.py), [BigQueryBenchmark.java](../scripts/benchmarks/bigquery/java/BigQueryBenchmark.java)). [job_metrics.py](../scripts/benchmarks/tools/job_metrics.py) prints the tables from the job ids.

- **Data:** a copy of `bigquery-public-data.samples.wikipedia` in the benchmark project: 313,797,035 rows, 15 columns, 38 GB.
- **Shapes:** `count` reads and counts. `copy` reads, passes each row through a step in the pipeline language (the `e2e/rows` counter), and writes a new table. The step keeps the rows of a Rust reader from fusing with a Rust writer, so they cross the data plane to that worker and back, as in a real pipeline.
- **Readers and writers** (`-Preader`, `-Pwriter`):

  | Pipeline | Readers | Writers |
  |---|---|---|
  | Python | `python` (native `DIRECT_READ`), `java` (Managed), `rust` | `java` (`WriteToBigQuery` with `STORAGE_WRITE_API`, on the Java expansion service), `rust` |
  | Java | `java-native` (`BigQueryIO` `DIRECT_READ`), `java-managed`, `rust` | `java-native` (`BigQueryIO` Storage Write), `java-managed` (file loads for bounded input, a reference only), `rust` |

- **Scaling:** 4 fixed workers (`-PnumWorkers=4`), or autoscaling from 1 to 10 (`-PnumWorkers=1 -PmaxNumWorkers=10`). `n2-standard-4`, `us-central1`, Runner v2.
- **Java copy, Rust → Rust:** runs with the Java harness fix of [§4.1](#41-a-java-step-between-two-rust-steps-can-stop-the-job) (PR [#40606](https://github.com/apache/beam/pull/40606), [`java/fn-data-elastic-inbound`](https://github.com/prodriguezdefino/beam-pabs/tree/java/fn-data-elastic-inbound)), without workarounds or experiment flags. The fix is in a patched 2.76.0 harness image that `-PextraArgs="--sdkHarnessContainerImageOverrides=..."` selects ([how](#how-to-test-a-java-harness-change-on-dataflow)). On stock Beam 2.76.0, pass `-PextraArgs="--experiments=data_buffer_size_limit=10000000"`.
- **Check:** every job checks the row count. A `copy` job compares the rows and distinct `revision_id` values with the source table, then drops the table. Exactly once is the default (`-Psemantics`).
- **Metrics:** vCPU·s, rows per vCPU·s, processing time (first worker with work to pool teardown, so no VM or container startup), peak memory of each container, step time.
- Each cell runs once, in sequence. Take differences under about 10% as noise.

### 9.1 Results: copy

| Pipeline | Read → write | Workers | Wall (s) | Processing (s) | vCPU·s | Rows per vCPU·s |
|---|---|---|---|---|---|---|
| Java | Java native → Java native | 4 fixed | 861 | 726 | 12,408 | 25,290 |
| Java | **Rust → Rust** | 4 fixed | **441** | **278** | **5,444** | **57,641** |
| Java | Java (Managed) → Java (Managed) | 4 fixed | 1,414 | 858 | 14,872 | 21,100 |
| Python | **Rust → Rust** | 4 fixed | **714** | **527** | **10,174** | **30,843** |
| Python | Java → Java | 4 fixed | 3,096 | 2,513 | 42,039 | 7,464 |
| Python | Python → Java | 4 fixed | 2,799 | 2,617 | 43,596 | 7,198 |
| Java | Java native → Java native | 1–10, peak 9 | 696 | 556 | 14,757 | 21,264 |
| Java | **Rust → Rust** | 1–10, peak 5 | **596** | **443** | **6,116** | **51,308** |
| Java | Java (Managed) → Java (Managed) | 1–10 | failed | | | |
| Python | **Rust → Rust** | 1–10, peak 8 | **704** | **528** | **11,833** | **26,519** |
| Python | Java → Java | 1–10, peak 10 | 1,590 | 1,014 | 34,501 | 9,095 |
| Python | Python → Java | 1–10, peak 10 | 1,455 | 1,268 | 46,570 | 6,738 |

- In a Java pipeline, Rust read and write need 56% less compute and 62% less processing time than `BigQueryIO` on 4 workers. With autoscaling, they need 59% less compute. Step time: the Rust write 2,090 s against 7,526 s (`Write Records` and `Convert to message`), the Rust read 1,831 s against 2,734 s.
- In a Python pipeline, Rust read and write need 4.1 times less compute than the Java provider on 4 workers. With autoscaling, they need 2.9 times less compute.
- A Java pipeline runs the Rust read and the Rust write in two Rust containers, because Dataflow does not merge their environments. A Python pipeline runs them in one Rust container.
- Memory of each container, per worker:

  | Pipeline | Read → write | Workers | Container | Peak (MiB) | Average (MiB) |
  |---|---|---|---|---|---|
  | Java | Rust → Rust | 4 fixed | Rust write | 732 | 484 |
  | Java | Rust → Rust | 4 fixed | Rust read | 545 | 230 |
  | Java | Rust → Rust | 4 fixed | Java (`Touch`) | 4,190 | 3,346 |
  | Java | Rust → Rust | 1–10 | Rust write | 709 | 503 |
  | Java | Rust → Rust | 1–10 | Rust read | 536 | 254 |
  | Java | Rust → Rust | 1–10 | Java (`Touch`) | 4,185 | 3,649 |
  | Java | Java native → Java native | 4 fixed | Java | 5,745 | 4,846 |
  | Python | Rust → Rust | 4 fixed | Rust read and write | 1,315 | 989 |
  | Python | Rust → Rust | 4 fixed | Python (`Touch`) | 1,749 | 1,478 |
  | Python | Rust → Rust | 1–10 | Rust read and write | 1,307 | 885 |
  | Python | Java → Java | 4 fixed | Java (read, write) | 10,420, 5,151 | 6,511, 3,140 |

  The Rust write uses most of the Rust memory, and its average is near its peak. Each bundle keeps its appends in flight (at most 1,000 requests and 100 MiB) until BigQuery accepts them. With the Java harness fix ([§4.1](#41-a-java-step-between-two-rust-steps-can-stop-the-job)), the Java container (`Touch`) also uses less memory than `BigQueryIO` in one process.
- **Java, Rust → Rust, other variants** ([§4.1](#41-a-java-step-between-two-rust-steps-can-stop-the-job)):

  | Variant | Workers | Wall (s) | Processing (s) | vCPU·s | Rows per vCPU·s | Java peak / average (MiB) |
  |---|---|---|---|---|---|---|
  | Java harness fix (PR [#40606](https://github.com/apache/beam/pull/40606)), `exactly_once` | 4 fixed | 441 | 278 | 5,444 | 57,641 | 4,190 / 3,346 |
  | Java harness fix (PR [#40606](https://github.com/apache/beam/pull/40606)), `exactly_once` | 1–10, peak 5 | 596 | 443 | 6,116 | 51,308 | 4,185 / 3,649 |
  | Java harness fix (PR [#40606](https://github.com/apache/beam/pull/40606)), `at_least_once` | 1–10, peak 7 | 658 | 485 | 10,082 | 31,124 | 4,199 / 3,778 |
  | Java harness fix, `exactly_once`, 3 runs on 2026-10-14 | 4 fixed | 476–580 | 327–386 | 5,879–6,363 | 49,316–53,376 | 4,178–4,196 / 3,691–3,917 |
  | Java harness fix, `exactly_once`, 3 runs on 2026-10-14 | 1–10, peak 4–6 | 680–696 | 504–529 | 6,980–7,761 | 40,433–44,957 | 4,181–4,207 / 3,322–3,784 |
  | Beam 2.76.0 + `data_buffer_size_limit=10000000` | 4 fixed | 434 | 287 | 5,680 | 55,246 | 7,512 / 6,110 |
  | Beam 2.76.0 + `data_buffer_size_limit=10000000` | 1–10, peak 3 | 585 | 436 | 4,915 | 63,845 | 8,616 / 6,290 |
  | Beam 2.76.0 + `Reshuffle` | 4 fixed | 1,420 | 1,146 | 19,229 | 16,319 | 9,872 / 6,648 |
  | Beam 2.76.0 + `Reshuffle` | 1–10, peak 4 | stopped | | | | 11,404 / 8,762 |

  The Java harness fix keeps each bundle queue at 100 messages and cuts peak Java memory from 7.5–8.6 GiB to 4.2 GiB. The runs of 2026-10-14 use 8–17% more compute than the first runs of the same fix, with the same memory; take that as the spread between days. A `Reshuffle` writes all 38 GB to shuffle, uses 3.5 times the compute on 4 fixed workers, and still stops with autoscaling on stock Beam 2.76.0.
- **Java, Managed → Managed, autoscaling:** Dataflow failed the job after one work item timed out 4 times waiting for a worker update. The job runs no Rust code.

### 9.2 Results: count

Beam 2.76.0. The Rust read sends each row to the consumer as encoded bytes ([§7.4](#74-row-data-path)).

| Pipeline | Reader | Workers | Wall (s) | Processing (s) | vCPU·s | Rows per vCPU·s | Reader peak / average (MiB) |
|---|---|---|---|---|---|---|---|
| Java | **Rust** | 4 fixed | **262** | **117** | **2,943** | **106,625** | 85 / 78 |
| Java | Java native (`TableRow`, Avro) | 4 fixed | 286 | 149 | 3,238 | 96,911 | 1,683 / 1,495 |
| Java | Java (Managed) | 4 fixed | 1,071 | 535 | 9,488 | 33,073 | 3,636 / 3,213 |
| Java | **Rust** | 1–10, peak 2 | **408** | **264** | **2,109** | **148,789** | 96 / 88 |
| Java | Java native (`TableRow`, Avro) | 1–10, peak 2 | 485 | 344 | 2,678 | 117,176 | 1,746 / 1,387 |
| Java | Java (Managed) | 1–10, peak 6 | 1,098 | 547 | 10,551 | 29,741 | 5,147 / 3,544 |
| Python | **Rust** | 4 fixed | **481** | **312** | **6,638** | **47,273** | 519 / 224 |
| Python | Python native | 4 fixed | 835 | 665 | 12,158 | 25,810 | 770 / 693 |
| Python | Java (Managed) | 4 fixed | 1,298 | 717 | 13,373 | 23,465 | 3,617 / 3,144 |
| Python | **Rust** | 1–10, peak 4 | **622** | **435** | **6,449** | **48,658** | 418 / 217 |
| Python | Python native | 1–10 | not run | | | | |
| Python | Java (Managed) | 1–10, peak 9 | 1,171 | 602 | 16,143 | 19,439 | 5,112 / 3,550 |

- Against the Java provider, the Rust read needs 2 to 5 times less compute and a tenth of the memory or less. In a Java pipeline it needs 9% less compute than `BigQueryIO` on 4 workers and 21% less with autoscaling, while `BigQueryIO` reads and counts in one JVM.
- The encoded rows lowered the processing time of the Rust read in a Java pipeline from 128 s to 117 s, and its compute from 3,109 to 2,943 vCPU·s.
- The Rust reader uses more memory with a Python consumer (519 MiB peak, 224 MiB average) than with a Java consumer (85 MiB peak, 78 MiB average). The Python step reads slower, so the read responses and the outbound data wait in their bounded buffers.
- `Managed.read` converts each `TableRow` to a Beam row in a `MapElements` step that costs twice the read, and Dataflow adds about 6.5 minutes before the first worker to upgrade the transform.
- `java-native` uses `TableRow` with Avro: Beam `Row` costs 42% more, and both Arrow variants fail with `OutOfMemoryError` on direct memory in Beam 2.76.0 (`BigQueryStorageArrowReader` does not release its buffers).
- Arrow buffer compression (`-Pcompression`) does not change throughput, compute or memory: bytes from BigQuery do not limit the read. `NONE` is the default.
- The autoscaling Python native read did not run: the credentials of the benchmark run expired.

### 9.3 Where the time goes

With a Python consumer, Python sets the pace. In the `count` jobs, the Python step that decodes the rows takes 4,011–4,217 s and the Rust read 2,704–2,898 s, summed over workers. In the Python bundle threads, about 60% of the time waits on the GIL and the data plane, and about 21% is `StrUtf8Coder.decode`, 1.19 billion calls ([§3.3](#33-row-decoding-in-python)). A Java consumer more than doubles the rows per vCPU·s of the Rust read.

---

## 10. Open questions

- Image scopes: one scope for each I/O crate, with a sibling crate for the providers and the binary (`beam/io/gcp/expansion-service`), as the Java `google-cloud-platform/expansion-service` project does. Confirm with a second scope.
- Does the Dataflow streaming autoscaler use SDF backlog from a Rust worker ([§7.1](#71-pubsub-caveats))?
- A CPU profiler for Rust workers on Dataflow: the worker samples itself when an option names a location (`--profile_location=gs://...`) and writes pprof profiles. Cloud Profiler has no Rust agent. Local profiles on Prism use `./gradlew :sdks:rust:profilePrism` ([development.md](development.md#profiling)).
- An IO validation suite that runs every connector module on Prism or Dataflow by option, with per-runner skips that show their reason.
