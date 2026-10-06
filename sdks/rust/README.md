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

# Rust SDK

A library for writing [Apache Beam](https://beam.apache.org/) pipelines in Rust.

The SDK is under active development. [Roadmap](#roadmap) lists the features
that are not available. [SDK parity](docs/sdk-parity.md) compares the SDK with
the Java, Python, Go and TypeScript SDKs. The design and its review are in the
[RFC](https://docs.google.com/document/d/1M4ZsHNobSt526__rtcwp7K0ULvL9sAKnKBt39CcA7Vw/edit).

## Quickstart

Add one dependency. Select a runner as a Cargo feature: the runner feature adds
the worker harness. The default features add the connectors and the schema
derive. The SDK is not on crates.io. Depend on this repository by path. The
crates follow the Beam repository version, use edition 2024 and need
Rust 1.98 or later:

```toml
[dependencies]
beam = { package = "apache-beam", path = "../beam/sdks/rust/beam", features = ["prism"] }
tokio = { version = "1", features = ["full"] }
```

WordCount with `beam::prelude::*`:

```rust
use beam::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let options = PipelineOptions::from_args();
    let p = Pipeline::create(&options);

    p.apply(textio::Read::new("ReadLines", "gs://apache-beam-samples/shakespeare/kinglear.txt"))
        .flat_map("ExtractWords", |line: String| {
            line.split_whitespace().map(String::from).collect::<Vec<_>>()
        })
        .count_per_element("CountWords")
        .map("FormatCounts", |(word, count): (String, i64)| format!("{word}: {count}"))
        .apply(textio::Write::new("WriteLines", "/tmp/kinglear_counts.txt"));

    p.run().await?; // runs on --runner (default: prism)
    Ok(())
}
```

Full example: [examples/minimal_wordcount](examples/minimal_wordcount/src/main.rs)
(it uses `Pipeline::new()`, which has the default options).

For a pipeline with its own flags, use `beam::options::parse::<MyArgs>()`. It
returns the core `PipelineOptions` and the user option group. See
[examples/wordcount](examples/wordcount/src/main.rs).

### Feature flags

| Feature | Default | Brings in |
|---|---|---|
| `fluent` | yes | Method-style transforms (`map`, `filter`, `group_by_key`, `inner_join`, …) |
| `io-file` | yes | `beam::io`: local filesystem, `textio`, `fileio`, `WriteFiles` |
| `gcs` | yes | `gs://` filesystem; Pub/Sub, BigQuery and Bigtable I/O (cross-language); implies `io-file` |
| `derive` | yes | `#[derive(BeamRow)]` / `#[derive(BeamEnum)]` schema support |
| `external` | yes | `beam::external`: cross-language expansion client and `expansionx` |
| `mimalloc` | yes | mimalloc as the process-wide `#[global_allocator]`; disable default features to opt out |
| `prism` | no | Local portable runner, plus the worker harness |
| `dataflow` | no | Cloud Dataflow runner, plus the worker harness |
| `harness` | no | `beam::harness`: Fn API worker harness; enabled by `prism` and `dataflow` |
| `expansion` | no | `beam::expansion`: expansion server that serves Rust transforms to other SDKs |
| `testing` | no | `beam::testing`: in-graph assertions (`passert`), `TestStream`, `TestPipeline` |
| `arrow` | no | `beam::io::arrow`: Beam schema/`Row` ⇄ Arrow `RecordBatch` bridge |
| `parquet` | no | `beam::io::parquet::parquetio` (splittable Parquet read, sharded write); implies `arrow` |
| `avro` | no | `beam::io::avro::avroio` (splittable Avro OCF read, sharded write); implies `arrow` |
| `kafka` | no | `beam::io::kafka`: cross-language Kafka read/write through the Java expansion service |
| `managed` | yes | `beam::io::managed`: Managed I/O (Iceberg, Kafka, BigQuery, JDBC, Delta Lake), cross-language |
| `ml` | no | `beam::ml`: `RunInference`, `ModelHandler`, micro-batching |
| `ml-candle` | no | Hugging Face Candle transformer and embedding inference (pure Rust, CPU) |
| `ml-candle-cuda` | no | Candle with NVIDIA CUDA |
| `ml-candle-metal` | no | Candle with Apple Silicon Metal |
| `ml-onnx` | no | ONNX Runtime inference |
| `ml-onnx-dynamic` | no | ONNX Runtime loaded dynamically at run time (`ort/load-dynamic`) |
| `ml-onnx-download` | no | ONNX Runtime with downloaded prebuilt binaries (`ort/download-binaries`) |
| `ml-onnx-cuda` | no | ONNX Runtime with NVIDIA CUDA |
| `ml-onnx-tensorrt` | no | ONNX Runtime with NVIDIA TensorRT |
| `ml-onnx-coreml` | no | ONNX Runtime with Apple Silicon CoreML |
| `ml-remote` | no | Micro-batched remote LLM inference (Vertex AI Gemini) |

**No runner is enabled by default.** Select one. Cargo features are additive.
If you list two runners, the binary links both, so one binary can use
`--runner=prism` locally and `--runner=dataflow` in production:

```toml
beam = { package = "apache-beam", path = "../beam/sdks/rust/beam", features = ["prism", "dataflow"] }
```

## Documentation

| Document | Contents |
|---|---|
| [Programming guide](docs/programming-guide.md) | Transforms, side inputs, state, timers, SDF, windowing, schemas, resource hints, testing |
| [Machine learning and vectorized execution](docs/accelerated-workloads.md) | `RunInference`, `ModelHandler`, `WorkerModelCache`, Candle, ONNX Runtime, remote LLMs, `BatchElements`, Arrow batches |
| [Examples](examples/README.md) | Runnable example pipelines and their commands |
| [Running pipelines](docs/runners.md) | Prism and Dataflow submission, worker binaries |
| [Worker containers](docs/containers.md) | Base image, staged binaries, pre-baked images, Classic and Flex Templates |
| [ValidatesRunner conformance](docs/validates-runner.md) | Conformance test architecture, registry, and Prism / Dataflow Runner V2 results |
| [SDK parity](docs/sdk-parity.md) | Feature-by-feature comparison against the other Beam SDKs |
| [Metrics and telemetry](docs/metrics.md) | Fn API monitoring protocol and supported metrics |
| [WordCount benchmark](docs/wordcount_mini_benchmark.md) | Dataflow Runner V2 benchmark comparing Java, Python, and Rust |
| [Static registration (future work)](docs/static-registration.md) | Gap analysis for running user code on workers without rebuilding the pipeline |
| [Developing the SDK](docs/development.md) | Gradle tasks, testing, linting, coverage, mutation testing, complexity scorecard |

The API reference is in the rustdoc: `cargo doc --open -p apache-beam`.

## Where the Rust SDK stands

This table is a summary of [docs/sdk-parity.md](docs/sdk-parity.md).
✅ supported · 🟡 partial · 🔜 on the roadmap · ❌ not supported.

| Area | Rust | Notes |
|---|:--:|---|
| Core transforms (ParDo, GBK, CoGBK, Combine, Flatten, Partition) | ✅ | |
| Windowing, triggers, allowed lateness | ✅ | Custom `WindowFn` limited to the two standard window encodings |
| State (Value, Bag, Map, Set) and timers | ✅ | No `CombiningState` or `OrderedListState` |
| Splittable DoFn + watermark estimators | ✅ | |
| Schemas, Row coder, logical types | ✅ | `#[derive(BeamRow)]` infers schemas from native types |
| Metrics, worker status, structured logging | ✅ | |
| Portable Fn API harness | ✅ | |
| Prism and Dataflow runners | ✅ | Flink/Spark are not tested; they can work through portability |
| Cross-language: consume *and* provide | ✅ | Ships a bidirectional expansion service |
| I/O: TextIO, FileIO, Parquet, Avro, local FS, GCS | ✅ | Sharded, rolling writes; `ReadMatches`; splittable columnar reads |
| I/O through cross-language: Pub/Sub, BigQuery, Bigtable, Kafka | ✅ | Java SchemaTransforms through the auto-started expansion service |
| Managed I/O: Iceberg, JDBC, Delta Lake, Kafka, BigQuery | ✅ | `ManagedRead` and `ManagedWrite`; the Kafka and BigQuery transforms expand through them |
| In-graph assertions (`beam::testing::passert`) | ✅ | `testing` feature |
| `TestStream` | ✅ | Runner-executed; supported on Prism |
| `TestPipeline` | ✅ | Options from `BEAM_TEST_PIPELINE_OPTIONS`; verifies assertions ran |
| Resource hints (`beam:resources:*`) | ✅ | Pipeline and transform level |
| ML & Hardware Acceleration (Candle, ONNX, Remote LLMs, Arrow) | ✅ | CUDA, Metal, CoreML, Vertex AI Gemini, RunInference ([guide](docs/accelerated-workloads.md)) |
| Lineage reporting | ❌ | |
| Published to crates.io | ❌ | Build from this repo |

## Crate Layout

The SDK is a Cargo workspace with its root at `sdks/rust`:

| Crate | Path | Contents |
| :--- | :--- | :--- |
| `apache-beam` | `beam/` | Facade crate exposing the unified `beam::prelude::*`. |
| `apache-beam-model` | `beam/model/` | Protobuf and gRPC bindings generated from the standard Beam model protos. |
| `apache-beam-core` | `beam/core/` | The pipeline model: `DoFn` and `ProcessContext`, core transforms, coders, windowing, state and timers, schemas, options, and the runner/filesystem registries. |
| `apache-beam-fluent` | `beam/fluent/` | Fluent combinators over the core transforms (`map`, `filter`, `group_by_key`, `co_group_by_key`, the join family, side-input helpers). |
| `apache-beam-derive` | `beam/derive/` | `#[derive(BeamRow)]` and `#[derive(BeamEnum)]` proc-macros. Re-exported through `apache-beam-core`; not depended on directly. |
| `apache-beam-harness` | `beam/harness/` | Worker harness: Fn API control/data/state/logging dispatch and bundle execution. |
| `apache-beam-io-file` | `beam/io/file/` | Generic file-based sources and sinks (`FileBasedSource`, `WriteFiles`, `FileSink`, `TextIO`, `FileIO`). |
| `apache-beam-io-gcp` | `beam/io/gcp/` | GCS filesystem, GCP authentication, and cross-language Pub/Sub, BigQuery and Bigtable. |
| `apache-beam-io-arrow` | `beam/io/arrow/` | Beam schema/`Row` ⇄ Arrow `Schema`/`RecordBatch` conversion and the `RowCodec` abstraction shared by columnar I/O. |
| `apache-beam-io-parquet` | `beam/io/parquet/` | `parquetio`: row-group-splittable Parquet reads and sharded Parquet writes on arrow-rs. |
| `apache-beam-io-avro` | `beam/io/avro/` | `avroio`: block-splittable Avro OCF reads and sharded Avro writes on `arrow-avro`. |
| `apache-beam-io-kafka` | `beam/io/kafka/` | Cross-language `KafkaRead` and `KafkaWrite` over the Java `kafka_read`/`kafka_write` SchemaTransforms, expanded through Managed. Exposed as `beam::io::kafka` by the `kafka` feature. |
| `apache-beam-io-managed` | `beam/io/managed/` | Managed I/O: `ManagedRead` and `ManagedWrite` over Java's `beam:transform:managed:v1`, plus the `Row`-to-config bridge typed connectors use. Exposed as `beam::io::managed` by the `managed` feature. |
| `apache-beam-ml` | `beam/ml/` | Machine learning inference framework (`RunInference`, `ModelHandler`, micro-batching, DLQ) and acceleration backends (Candle, ONNX Runtime, Vertex AI remote LLMs). Exposed as `beam::ml` by the `ml` feature. |
| `apache-beam-external` | `beam/external/` | Cross-language expansion *client* and automated Java expansion service management (`expansionx`). |
| `apache-beam-expansion` | `beam/expansion/` | Cross-language expansion *server*, so other SDKs can call Rust transforms. |
| `apache-beam-runner-prism` | `beam/runners/prism/` | Portable runner backed by the Prism job service. |
| `apache-beam-runner-dataflow` | `beam/runners/dataflow/` | Google Cloud Dataflow runner. |
| `apache-beam-testing` | `beam/testing/` | Pipeline test tooling: in-graph assertions (`passert`), `TestStream` and `TestPipeline`. Exposed as `beam::testing` by the `testing` feature. |
| `apache-beam-test-utils` | `beam/test-utils/` | Test doubles such as `InMemoryFileSystem`. Not published. |
| `apache-beam-runner-tests` | `beam/runner-tests/` | ValidatesRunner suite executed against each runner. Not published. |
| — | `container/` | The SDK base container image, which holds only `boot`. |
| — | `examples/*` | Example pipelines; see [examples/README.md](examples/README.md). |

### Layering

Dependencies go in one direction only. The diagram shows the normal
dependencies from each `Cargo.toml`. It does not show dev-dependencies.

```text
apache-beam-derive (proc-macro, optional) ─┐
apache-beam-model ─────────────────────────┴─> apache-beam-core
apache-beam-core ─> apache-beam-fluent, apache-beam-harness, apache-beam-io-file,
                    apache-beam-io-arrow, apache-beam-external, apache-beam-expansion,
                    apache-beam-ml, apache-beam-testing
apache-beam-harness                       ─> apache-beam-runner-prism, apache-beam-expansion,
                                             apache-beam-testing (optional, with prism)
apache-beam-external                      ─> apache-beam-io-managed ─> apache-beam-io-kafka
apache-beam-io-file + external + managed  ─> apache-beam-io-gcp
apache-beam-io-file + io-gcp              ─> apache-beam-runner-dataflow
apache-beam-io-file + io-arrow            ─> apache-beam-io-parquet, apache-beam-io-avro
all of the above (optional features)      ─> apache-beam (facade) ─> apache-beam-runner-tests
```

`apache-beam-core` and `apache-beam-fluent` do not depend on a runner or an
I/O implementation. Runners and filesystems register themselves with the
registries in `apache-beam-core` through the `inventory` crate. A runner or a
filesystem is available when its crate is *linked* into the binary.

Rust drops rlibs that nothing references, so some code must name each crate
to keep its registrations. The `apache-beam` facade does this: it re-exports
each optional crate that its enabled features add, and its private `link`
module holds a `use ... as _;` for each crate that registers through
`inventory` (file, GCS, harness, Prism, Dataflow, expansion, ML). Pipeline
authors depend only on `beam` and do not write these imports.

## Building and Testing

Run builds through Gradle. Gradle caches incremental builds, and the rest of
the Beam repository uses it too:

```bash
./gradlew :sdks:rust:build   # Compile all crates
./gradlew :sdks:rust:test    # Run all tests (one crate: -Ppkg=<crate>)
./gradlew :sdks:rust:check   # rustfmt --check + clippy -D warnings
./gradlew :sdks:rust:fmt     # Apply formatting
```

The tasks are defined in [gradle/cargo.gradle](gradle/cargo.gradle).

For the full task reference, coverage and caching, see [docs/development.md](docs/development.md).

## Roadmap

The guides above document the available features. This table shows the work that remains.

| Area | Item | Status |
|---|---|---|
| I/O | S3 filesystem | Planned |
| Transforms | `stats` and `top` combinators (`mean`, `quantiles`, `top_per_key`) | Planned |
| Relational | Beam SQL through cross-language, Beam YAML | Exploring |
| Platform | Lineage reporting | Unscheduled |
| Release | Publish to crates.io | Unscheduled |

## Contributing

See the [Beam Contribution Guide](https://beam.apache.org/contribute/). Each new file must have the standard Apache 2.0 license header; the repository-wide `./gradlew rat` task checks it. Before you open a PR, run `./gradlew :sdks:rust:check` (formatting and clippy).
