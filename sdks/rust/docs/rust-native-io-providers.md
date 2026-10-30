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

# Native I/O Connectors and Cross-language Providers

This document describes the native I/O connectors of the Rust SDK, the libraries and runtime
patterns they use, how to benchmark them, and how other SDKs call them as
`SchemaTransform` providers.

## 1. Connector Status and Layout

| Connector | Rust module (`beam::io::*`) | Expansion service URN | Supported capabilities | Not supported |
|---|---|---|---|---|
| **BigQuery Storage Read** | `gcp::bigquery::read` (`gcp-bigquery`) | `beam:schematransform:org.apache.beam:rust:bigquery_storage_read:v1` | Table and query reads, `row_restriction`, `selected_fields`, `snapshot_time`, stream count hints, Arrow compression, `SplitReadStream` dynamic splits, typed `#[derive(BeamRow)]` reads, dead-letter `failures` output | `sample_percentage` |
| **BigQuery Storage Write** | `gcp::bigquery::write` (`gcp-bigquery`) | `beam:schematransform:org.apache.beam:rust:bigquery_storage_write:v1` | Default stream (at least once), pending streams + `BatchCommitWriteStreams` (bounded exactly once), sharded buffered streams + timers (unbounded exactly once), field projection (`keep`, `drop`, `only`, `ignore_unknown_values`), dead-letter `errors` output | `WRITE_TRUNCATE`, dynamic destinations, CDC writes: use `ManagedWrite` |
| **Bigtable Read & Write** | `gcp::bigtable::{read, write}` (`gcp-bigtable`) | `beam:schematransform:org.apache.beam:rust:bigtable_read:v1`, `...:bigtable_write:v1` | `ReadRows` with `SampleRowKeys` initial splits, dynamic split notifications, materialized and authorized views, app profiles, `RowFilter`s; `MutateRows` batching, partial-failure retries, `RateLimitInfo` flow control, flat and batched schemas, dead-letter `errors` output | A server-assigned `SetCell` timestamp (`-1`) can write duplicate cells on stream retry |
| **Kafka Read & Write** | `kafka::{read, write, codec}` (`kafka`) | `beam:schematransform:org.apache.beam:rust:kafka_read:v1`, `...:kafka_write:v1` | Bounded and unbounded SDF reads, consumer pool, parked partition leases, bundle-finalizer offset commits, `Redistribute` (`Off`, `Random`, `Keys`), Managed Kafka `SASL/OAUTHBEARER` ADC auth, `Raw`, `Utf8`, `Json`, `Avro` (plain and Confluent `SchemaRegistry`), `Proto`, `BeamRow`, `WithMetadata`, dead-letter outputs | Exactly-once transactional writes, multi-topic patterns, dynamic destination topics |
| **Iceberg Write** | `iceberg` (`iceberg`) | — (native Rust; Managed I/O for xlang) | REST catalog (`BigLake` and standard REST), `BearerAuth` per-request token refresh, vended GCS credentials, partitioned Parquet data files, bounded and periodic (`triggering_frequency`) snapshot commits | Native Iceberg read, dynamic table routing, upserts (use `ManagedRead` / `ManagedWrite`) |
| **Cloud Storage (`gs://`)** | `gcp::gcs` (`gcp-gcs`) | — (`FileSystem` registry) | Async reads and writes (`GcsFileSystem`, `GcsAsyncWriter`), 8 MiB buffered single-shot uploads, resumable streaming uploads above 8 MiB | — |
| **Pub/Sub** | `gcp::pubsub` (`gcp-pubsub`) | — (consumes Java expansion service) | Cross-language `PubsubRead` / `PubsubWrite` (`try_apply`) | Native read and write: the transforms need the Java expansion service |

### Workspace crates

Format codecs live in standalone crates under `beam/io/` so file, messaging, and table
connectors share one implementation:

| Crate | Library name | Role |
|---|---|---|
| `apache-beam-gcp-core` | `beam_gcp_core` | Shared Google ADC credentials, shared Tokio I/O runtime, gRPC channel builder, transient-error classification |
| `apache-beam-io-gcp` | `beam_io_gcp` | GCS, Secret Manager, BigQuery, Bigtable, and cross-language Pub/Sub |
| `apache-beam-io-gcp-expansion-service` | — (`beam-gcp-expansion-service`) | Serves BigQuery and Bigtable `SchemaTransform` providers |
| `apache-beam-io-kafka` | `beam_io_kafka` | Native Kafka read, write, auth, and codecs |
| `apache-beam-io-kafka-expansion-service` | — (`beam-kafka-expansion-service`) | Serves Kafka `SchemaTransform` providers |
| `apache-beam-io-iceberg` | `beam_io_iceberg` | Native Iceberg REST catalog write |
| `apache-beam-io-arrow` | `beam_io_arrow` | Beam `Schema` / `Row` / `RowElement` ⇄ Arrow `RecordBatch` |
| `apache-beam-io-avro` | `beam_io_avro` | Block-splittable Avro OCF files and single-record `datum` codecs |
| `apache-beam-io-parquet` | `beam_io_parquet` | Row-group-splittable Parquet reads and sharded writes |
| `apache-beam-io-json` | `beam_io_json` | JSON Schema mapping and direct `Row` reader and writer |
| `apache-beam-io-proto` | `beam_io_proto` | Protobuf descriptor mapping and dynamic `Row` wire codec |

---

## 2. Library and Dependency Choices

Each connector reuses maintained client and format crates before adding generated stubs.

### Google Cloud clients (`google-cloud-*` and `google-cloud-gax-internal`)

- **BigQuery (`google-cloud-bigquery` `0.18`):** Uses the official `Read` and `Write` Storage
  API clients for `CreateReadSession`, `ReadRows`, `SplitReadStream`, and Arrow `AppendRows`
  streams. Because `google-cloud-bigquery` commits one stream at a time, multi-stream
  `BatchCommitWriteStreams` runs `tonic` over a `google-cloud-gax-internal` `0.7.21` gRPC
  channel with percent-encoded `x-goog-request-params` routing headers.
- **Bigtable (`google-cloud-gax-internal` `0.7.21` + `tonic-prost-build`):** Google does not
  publish an official Rust Bigtable data-plane crate yet. `apache-beam-io-gcp-googleapis`
  compiles the `google.bigtable.v2` protos, and `BigtableClient` pools balanced
  `google-cloud-gax-internal` gRPC subchannels keyed on `(uri, tls)` with
  `concurrency_limit(100)` per subchannel. `ClientConfig` sets no HTTP/2 keepalive, so
  `bigtable::retry` treats local transport `CANCELLED` errors from closed idle connections
  (`hyper::Error(Canceled, "connection closed")`) as retryable backoff while keeping
  server-issued `CANCELLED` statuses fatal.
- **Cloud Storage (`google-cloud-storage` `1.20`):** `GcsAsyncWriter` owns a single async
  upload state machine (`GcsWriter` is a thin synchronous wrapper over it). Objects below
  8 MiB upload in one buffered call; larger objects stream over a bounded channel. Both paths
  share `with_upload_retries` (`with_idempotency(true)`, `RetryableErrors` up to 5 attempts,
  0.25–5 s backoff).
- **Auth caching (`beam_gcp_core::auth`):** Global `OnceLock` cells cache only successful
  credential or principal lookups, so a transient metadata-server failure at worker startup
  does not poison the process.

### Kafka (`rdkafka` `0.39` / bundled `librdkafka` `2.12.1`)

| Candidate | Verdict | Reason |
|---|---|---|
| `rdkafka` `0.39` (`librdkafka` `2.12.1`, MIT / BSD-2-Clause) | **Selected** | Supports `gzip`, `snappy`, `lz4`, `zstd`, `SSL`, `SASL_SSL` (`PLAIN`, `SCRAM`, custom `OAUTHBEARER` callbacks), consumer groups, and pass-through client properties. |
| `rskafka` `0.6` (pure Rust) | Rejected | Lacks `SASL` (`PLAIN`, `SCRAM`, `OAUTHBEARER`), consumer-group offset management, and `librdkafka` property compatibility. |

#### Kafka dependencies and cross-compilation

- **Static runtime linking:** `rdkafka` enables `libz-static`, `zstd`, and `ssl-vendored`
  (`default-features = false`). The worker binary links only `libc.so.6`, `libm.so.6`, and
  `libgcc_s.so.1`, which `gcr.io/distroless/cc-debian12:latest` provides.
- **Cross-compilation:** Without `cmake-build`, `rdkafka-sys` runs `./configure`, which reads
  `CC` and `CXX` rather than `CC_<target>`. `compileWorkers` in
  [`gradle/containers.gradle`](../gradle/containers.gradle) sets `CC`, `CXX`, `HOST_CC`, and
  `HOST_CXX` when cross-compiling. Do not enable `cmake-build` without `curl-static`.
- **Managed Kafka auth (`SASL/OAUTHBEARER`):** `with_google_auth()` (or `with_gcp_adc` in the expansion service config) builds
  three-part `GOOG_OAUTH2_TOKEN` JWTs signed with Google ADC tokens (refreshing access tokens
  4 minutes before expiry) and polls idle shared consumers every 100 ms so `librdkafka`
  receives token refreshes on long-lived jobs.

### Format codecs (`json`, `arrow-avro`, `prost-reflect`)

- **JSON (`beam_io_json`):** Decodes UTF-8 JSON bytes straight into `Row` fields and formats
  `Row` values into a reused byte buffer, without building `serde_json::Value`.
- **Avro (`beam_io_avro::datum` on `arrow-avro` `58.4.1` and `arrow-cast`):** Replaces custom
  datum parsing with `arrow-avro` single-record framing (raw Avro and 5-byte Confluent wire
  framing). Each reader owns a `DatumDecoder` that borrows `librdkafka` payload slices and
  decodes a poll batch as one `RecordBatch`, falling back to per-record isolation only when a
  batch contains a bad record. Each writer owns a `DatumEncoder` with a cached `Conform` plan
  that casts top-level and nested string columns to Arrow enum dictionaries. `SchemaRegistry`
  (`ureq`) deduplicates concurrent schema-ID fetches with per-ID locks, caches negative
  lookups with exponential backoff (1–30 s), and bounds read-path HTTP calls to 5 s.
- **Protobuf (`beam_io_proto` on `prost-reflect` `0.16`):** Maps `MessageDescriptor` schemas
  (including `map<K, V>` fields) to Beam `Schema`s and converts wire payloads to and from
  `Row`s.

### Iceberg (`iceberg` `0.11` and `iceberg-catalog-rest` `0.11`)

- **Arrow version:** `iceberg` `0.11` pins Arrow and Parquet to `58.4.1` across the
  workspace.
- **Isolated `reqwest 0.12`:** `iceberg-catalog-rest` `0.11` requires a `reqwest` `0.12`
  `Client` (`RestCatalogBuilder::with_client`), while the rest of the workspace uses
  `reqwest` `0.13`. Keep `reqwest = "0.12"` confined to `apache-beam-io-iceberg`.
- **Token refresh and storage:** `BearerAuth` implements Iceberg's `AuthManager` to attach a
  fresh ADC bearer token to every catalog, credential, and metrics call. Vended GCS tokens
  look up clients in a bounded `moka` cache (`TOKEN_GCS_CACHE`) and report hit/miss counters
  (`iceberg/gcs_client_cache_hits`, `iceberg/gcs_client_cache_misses`). Prefix deletes bound
  concurrency with `buffer_unordered(16)`.

---

## 3. Runtime and Data-path Design

- **Calling-thread `block_on_io`:** Bundle threads run synchronous `DoFn` methods, while GCP
  and Iceberg clients are async. [`beam::runtime::block_on_io`](../beam/core/src/runtime.rs)
  enters the shared Tokio runtime context and polls the future on the calling thread with
  `std::thread::park` (restoring outer park tokens through a thread-local depth guard when
  nested). It spawns no OS thread per batch and keeps the current step's `MetricsContext`
  active while polling; work spawned onto Tokio worker threads with `tokio::spawn` runs
  outside that step context.
- **Encoded-row pass-through (`RowElement`):** Across stage and language boundaries, rows
  arrive as `beam:coder:row:v1` bytes. BigQuery, Parquet, Avro, and Bigtable writers read
  `RowElement` bytes straight into Arrow column builders (`RowBatchBuilder`) or `Bytes`
  slices (`Bytes::slice` on flat Bigtable mutations, `Row::into_values` move on decoded
  rows), avoiding intermediate `Row` allocations and duplicate raw-row copies.
- **Construction errors and `BundleWindows`:** Invalid connector options record errors on the
  `Pipeline` via `Pipeline::invalid_construction_output` instead of panicking during `apply`
  (`External*`, `Managed*`, and `Pubsub*` implement `TryPTransform` only and use
  `try_apply`). Setting `triggering_frequency` on a bounded input is a construction error in
  BigQuery and Iceberg. Writers track input windows with `BundleWindows` and emit
  `finish_bundle` batches and dead-letter rows at the earliest input timestamp in the bundle.
- **Retries, throttling, and lineage:** All connectors share `beam::retry::Backoff` (with
  jitter; Bigtable uses 12 attempts / 11 retries for ~122 s mean wait budget). Only server
  push-back (`RESOURCE_EXHAUSTED`, `RetryInfo`, Bigtable `RateLimitInfo`) returns
  `Decision::Throttled` and increments `throttling-msecs` for the Dataflow autoscaler;
  transport retries increment `<connector>/retry_wait_msecs`. Step counters use the
  namespaces `bigquery`, `bigtable`, `kafka`, and `iceberg`, and every source and sink
  reports `Lineage::sources()` or `Lineage::sinks()` plus `HasDisplayData`.
- **In-flight memory and streaming overlap:**
  - BigQuery Storage Write caps in-flight appends per bundle at `1,000` requests and `20 MiB`
    (`InFlightLimit`), and verifies `Fetched::End` on Storage Read streams so a panicked
    fetch task never looks like end-of-stream.
  - Kafka read keeps a pool of `librdkafka` consumers per read transform with sticky
    partition slots and parked partition leases (`parking.rs`), and emits records immediately
    when `!decoder.decodes_batches()` so downstream fused writers overlap with polling.
  - Iceberg `WriteDataFilesFn` sizes batches with `GroupIntoBatches::with_byte_size_fn`
    (without double-encoding values) and flushes groups at `128 MiB` of Arrow IPC bytes.

---

## 4. Benchmarks

### Commands

Dataflow benchmarks run on Dataflow Runner v2 in `us-central1` with `n2-standard-4` workers
(4 vCPUs, 16 GiB) and write `report.md` and `results.json` under `build/reports/benchmarks/`
(`vCPU·s` and `rows per used vCPU·s` lead every report):

```bash
# Local worker-profile harness and Kafka copy benchmarks
./gradlew :sdks:rust:harnessBenchmark -PcargoProfile=worker
./gradlew :sdks:rust:kafkaCopyBenchmark

# Build all four Linux I/O benchmark worker binaries (shared .rlib cache, sequential LTO link)
./gradlew :sdks:rust:buildIoBenchmarkWorkers

# Dataflow native Rust benchmarks (-Ppipeline=rust)
./gradlew :sdks:rust:bigQueryBenchmark -Ppipeline=rust -Pshape=count -PnumWorkers=1 -PmaxNumWorkers=10
./gradlew :sdks:rust:bigQueryBenchmark -Ppipeline=rust -Pshape=copy -Pwriter=rust -PnumWorkers=1 -PmaxNumWorkers=10
./gradlew :sdks:rust:bigtableBenchmark -Ppipeline=rust -Pshape=bt_to_gcs -Preader=rust -PnumWorkers=1 -PmaxNumWorkers=10
./gradlew :sdks:rust:bigtableBenchmark -Ppipeline=rust -Pshape=gcs_to_bt -Pwriter=rust -Pinput=gs://<bucket>/part-*.tsv
./gradlew :sdks:rust:kafkaBenchmark -Ppipeline=rust -Pshape=copy -Preader=rust -Pwriter=rust \
  -Ptopic=<src> -PdestinationTopic=<dst> -Prate=200000 -PloadSeconds=1800
./gradlew :sdks:rust:icebergBenchmark -Ppipeline=rust -Pwriter=rust -Ptopic=<topic> \
  -Prate=100000 -PloadSeconds=1800 -PidleAfterLoadSeconds=270
```

The `report.md` of a run gives vCPU time, element counts and rates, memory, shuffle, step CPU,
peak workers and wall time for each pipeline.

---

## 5. Cross-language Providers

### Expansion and worker execution

`beam-gcp-expansion-service` and `beam-kafka-expansion-service` expose the native connectors
to Python (`SchemaAwareExternalTransform`), Java (`External`), and Go (`xlangx`) with no
changes outside `sdks/rust`:

- **Payloads:** [`beam_expansion`](../beam/expansion/src/lib.rs) accepts both
  `SchemaTransformPayload` and `ExternalConfigurationPayload`, maps fields by name, and
  widens integer config fields (such as Python `INT64` into `INT32`) when values fit.
- **Deterministic replay (`ReplayEntry`):** Foreign drivers do not run a Rust `main` binary
  on the worker. The expansion server rewrites expanded `DoFn`s to `beam:dofn:rust:expanded:v1`
  with a `ReplayEntry` (`provider`, `config`, `namespace`, `expansion_id`), and the Rust
  worker replays `build_transform` once per `expansion_id` to populate its handler map
  ([`replay.rs`](../beam/harness/src/replay.rs)).
- **Single container image:** `gcpExpansionServiceImage` and `kafkaExpansionServiceImage`
  bake the service binary into the Rust SDK base image (`--port` runs the expansion server;
  `--worker` via `boot` runs the Fn API worker). All expansions from one service emit
  identical environment protos, so Python merges them into one Rust container per VM.
- **Java step between Rust steps:** When a Java step sits between a Rust source and a Rust
  sink in one fused Dataflow stage (`Rust → Java → Rust`), the Java harness can stall. Pass
  `--experiments=data_buffer_size_limit=10000000` to the job.

### Validation tasks

```bash
# Prism (uses testcontainers for Kafka; GCP Python tests use configured tables)
./gradlew :sdks:rust:kafkaExpansionServicePythonTest
./gradlew :sdks:rust:gcpExpansionServicePythonTest -Ptests=bigquery_storage

# Dataflow
./gradlew :sdks:rust:gcpExpansionServicePythonTestDataflow -Ptests=bigquery_storage
```
