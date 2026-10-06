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

# Rust SDK Examples

Each example below is a standalone crate under `sdks/rust/examples/`. The Cargo package name
matches the directory name. Run each example on the portable Prism runner or submit it to
Google Cloud Dataflow. Dataflow launches require a worker container image, provided by
`-PsdkContainerImage` or `sdkContainerImage` in `sdks/rust/.local/sdk.properties`.

The Gradle launch tasks run `cargo run --release -p <example> -- <runner flags> <extraArgs>`
(see [`prism.gradle`](../gradle/prism.gradle) and [`dataflow.gradle`](../gradle/dataflow.gradle)):

- `-Pexample=<name>` selects the package (default `wordcount`).
- `-PextraArgs="..."` appends pipeline flags. Values are split on spaces.
- `-PexampleFeatures=<features>` passes `--features` to `cargo run`.
- `:sdks:rust:prism` runs with `--environment_type=LOOPBACK`. Add `-Pdocker` to run the
  workers in the SDK container instead.

Unit and pipeline tests live in each example's `tests/` directory. `minimal_wordcount` and
`side_inputs` have no tests:

```bash
./gradlew :sdks:rust:test -Ppkg=<example>
```

| Example | Topic |
|---|---|
| [`minimal_wordcount`](#minimal-wordcount) | Simplest pipeline, hardcoded paths |
| [`wordcount`](#wordcount) | Pipeline options, composite transforms, pre-baked image, Flex Template |
| [`streaming`](#streaming-continuous-ingestion-with-periodicimpulse) | `PeriodicImpulse`, fixed windows |
| [`side_inputs`](#side-inputs--broadcast-joins) | Singleton and iterable side inputs, broadcast join |
| [`join`](#relational-joins) | `inner_join`, `left_join`, `CoGroupByKey` |
| [`gaming`](#mobile-gaming-stateful-processing--metrics) | Stateful DoFn, user metrics |
| [`partition`, `forest`](#partition--flatten) | `Partition`, `Flatten`, recursive construction |
| [`bigquery_tornadoes`](#bigquery-tornadoes-cross-language) | Java BigQuery read and write |
| [`bigtable_wordcount`](#bigtable-wordcount-cross-language-bigtable_wordcount) | Java Bigtable read and write |
| [`kafka_ouroboros`](#kafka-ouroboros-cross-language-streaming-kafka_ouroboros) | Java Kafka read and write, Managed error handling |
| [`pubsub_ouroboros`](#pubsub-ouroboros-cross-language-streaming-pubsub_ouroboros) | Java Pub/Sub read and write |
| [`iceberg_wordcount`](#iceberg-wordcount-managed-io-iceberg_wordcount) | Managed I/O Iceberg |
| [`windowed_wordcount`](#windowed-wordcount-fixed-tumbling-windows) | Event timestamps, fixed windows |
| [`leaderboard`](#mobile-gaming-leaderboard-triggers-speculative-panes--allowed-lateness) | Triggers, accumulation, allowed lateness |
| [`sessions`](#user-session-analytics-dynamic-sessionization--window-merging) | Session windows |
| [`sliding_window`](#sliding-window-moving-average-overlapping-hopping-windows) | Sliding windows |
| [`row_schemas`](#type-driven-row-schemas--logical-types-row_schemas) | `#[derive(BeamRow)]`, logical types |
| [`state_conformance`](#cross-bundle-state-conformance-state_conformance) | `MapState`, `SetState`, timers |
| [`fold_state_backed`](#fold-combinators--state-backed-iterables-fold_state_backed) | Folds, state-backed iterables |
| [`nyc_taxi`](#nyc-taxi--rideshare-analytics-nyc_taxi) | Parquet read and write |
| [`traffic_routes`](#traffic-routes-avro-analytics-traffic_routes) | CSV read, Avro write |
| [`remote_llm_inference`](#remote-llm-inference-remote_llm_inference) | `RunInference` with Gemini, dead-letter output |
| [`cv_onnx_classification`](#image-classification-with-onnx-runtime-cv_onnx_classification) | `RunInference` with ONNX Runtime |
| [`text_embedding_candle`](#text-embeddings-with-candle-text_embedding_candle) | `RunInference` with Candle |
| [`columnar_feature_engineering`](#table-row-inference-columnar_feature_engineering) | ONNX RandomForest, port of a Python benchmark |

## Minimal WordCount

The simplest word count: it reads `gs://apache-beam-samples/shakespeare/kinglear.txt`, counts
the words, and writes them to `/tmp/minimal_wordcount.txt`. The paths are hardcoded and
the example has no options of its own:

```bash
./gradlew :sdks:rust:prism -Pexample=minimal_wordcount
```

## WordCount

Count occurrences of words from local files or Google Cloud Storage (`gs://`). Flags:
`--input` (default `gs://apache-beam-samples/shakespeare/kinglear.txt`) and `--output`
(default `/tmp/output.txt`):

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=wordcount

# Custom input/output with extraArgs
./gradlew :sdks:rust:prism -Pexample=wordcount -PextraArgs="--input=/path/to/input.txt --output=/tmp/counts.txt"

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=wordcount
```

The example includes a [`Dockerfile`](wordcount/Dockerfile) to build an image with a pre-baked binary:

```bash
./gradlew :sdks:rust:prebakedImage -Pexample=wordcount -Pdocker-repository-root=<registry> -Ppush-containers
./gradlew :sdks:rust:dataflow -Pexample=wordcount -PsdkContainerImage=<registry>/beam_rust_example_wordcount:<sdk version> -PworkerBinary=none
```

Add `-Pflex` to build the `flex` stage instead (`beam_rust_example_wordcount_flex`), which
also holds the Flex Template launcher. [`metadata.json`](wordcount/metadata.json) describes the
template parameters. See [Flex Templates](../docs/containers.md#flex-templates-experimental).

## Streaming (Continuous Ingestion with PeriodicImpulse)

This pipeline generates continuous heartbeat data with `PeriodicImpulse` and splittable DoFns. It assigns elements to fixed tumbling event-time windows and aggregates window metrics. It then writes window summaries to sink files.

Flags: `--impulse_interval_ms` (default `500`), `--window_size_secs` (default `5`), `--limit`
(number of impulses; unbounded if omitted), `--max_read_time_secs` (default `600`), and
`--output` (default `/tmp/streaming_output.txt`). `--streaming` is a standard pipeline option:

```bash
# Portable Prism Runner (local streaming execution)
./gradlew :sdks:rust:prism \
  -Pexample=streaming \
  -PextraArgs="--streaming --max_read_time_secs=300 --impulse_interval_ms=1000 --window_size_secs=10 --output=/tmp/streaming_output.txt"

# Google Cloud Dataflow (Streaming Engine)
./gradlew :sdks:rust:dataflow \
  -Pexample=streaming \
  -PextraArgs="--streaming --max_read_time_secs=600 --impulse_interval_ms=1000 --window_size_secs=10"
```

## Side Inputs & Broadcast Joins

This pipeline filters Shakespeare word counts with two side inputs: a singleton side input (`as_singleton`) for minimum word length and an iterable side input (`as_iter`) for a stopword list. It then enriches counts with character roles using a shuffle-free `broadcast_left_join`:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=side_inputs

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=side_inputs
```

The `apache-beam-runner-tests` suite tests multimap side inputs (`as_multimap`).

## Relational Joins

This pipeline joins user profiles with orders using keyed joins (`inner_join`, `left_join`) and
groups both collections with `CoGroupByKey` over a `KeyedPCollectionTuple`. `--output` is
optional:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=join

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=join
```

## Mobile Gaming (Stateful Processing & Metrics)

This pipeline parses gaming events from Google Cloud Storage, tracks user metrics (`Counter` and `Distribution`), and aggregates team scores. It uses a stateful DoFn with both [`ValueStateSpec`](../beam/core/src/transforms/dofn/state.rs) and [`BagStateSpec`](../beam/core/src/transforms/dofn/state.rs) to detect and award team milestones (`--threshold`, default `500`):

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=gaming -PextraArgs="--output=/tmp/gaming_output.txt"

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=gaming
```

## Partition & Flatten

`partition` divides student scores into three tiers with `Partition`, processes each tier, and
merges the tiers with `Flatten`. `forest` builds disconnected trees of transforms recursively and
flattens their leaves (`--count`, default `2`; `--depth`, default `3`):

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=partition
./gradlew :sdks:rust:prism -Pexample=forest
```

## BigQuery Tornadoes (Cross-Language)

This pipeline mixes native Rust processing with Java cross-language I/O transforms through the Beam Expansion Service:

1. Reads weather station observations with the Java BigQuery Storage Read API (`--input`, default `apache-beam-testing.samples.weather_stations`, or `--input_query`).
2. Filters for tornado events and extracts the month in Rust.
3. Computes monthly tornado counts using `CountPerElement`.
4. Writes aggregated results to the `--output` table with the Java BigQuery Write transform. `--write_method` selects `storage_write_api` (default), `file_loads`, `at_least_once`, or `auto`. Without `--output`, the pipeline only reads and counts.

```bash
# Run with Portable Prism runner:
./gradlew :sdks:rust:prism \
  -Pexample=bigquery_tornadoes \
  -PextraArgs="--output=<project>:<dataset>.tornadoes"

# Run on Google Cloud Dataflow:
./gradlew :sdks:rust:dataflow -Pexample=bigquery_tornadoes \
  -PextraArgs="--output=<project>:<dataset>.tornadoes"
```

## Bigtable WordCount (Cross-Language, `bigtable_wordcount`)

This pipeline transfers word counts to and from Cloud Bigtable using the Java `bigtable_write` and `bigtable_read` SchemaTransforms:

- `--mode=write` (default): Counts King Lear words in Rust and writes one row per word (`<column_family>:count`) with `BigtableWrite`.
- `--mode=read`: Reads the table with `BigtableRead`, decodes each column in Rust, and writes `word: count` lines to `--output`.

`--column_family` defaults to `counts`. The table and its column family must exist first:

```bash
gcloud bigtable instances tables create <table> --instance=<instance> --column-families=counts

# Google Cloud Dataflow: write, then read back
./gradlew :sdks:rust:dataflow -Pexample=bigtable_wordcount \
  -PextraArgs="--bigtable_project=<project> --bigtable_instance=<instance> --bigtable_table=<table>"
./gradlew :sdks:rust:dataflow -Pexample=bigtable_wordcount \
  -PextraArgs="--bigtable_project=<project> --bigtable_instance=<instance> --bigtable_table=<table> --mode=read --output=gs://<bucket>/counts"
```

## Kafka Ouroboros (Cross-Language Streaming, `kafka_ouroboros`)

This streaming pipeline reads from and writes to the same Kafka topic:

1. Reads the loop topic unboundedly with the Java `kafka_read` SchemaTransform (`KafkaRead`, earliest offset, expanded through Managed I/O).
2. Injects `--num_seeds` seed messages (default `1`) to start the cycle.
3. Modifies each JSON message in Rust.
4. Writes messages with remaining cycles back to the same topic with `KafkaWrite`. After `--max_cycles` hops (default `5`), messages exit the loop.

Progress appears in `ouroboros` user counters (`seeded`, `evolved`, `ascended`, `malformed`). Both Kafka transforms use Managed error handling (`with_error_handling`, expanded with `to_managed()?.with_all_outputs()` or `with_outputs()`): records that Java cannot decode or serialize increment `kafka_read_errors` or `kafka_write_errors` instead of failing the bundle. The job runs until cancelled.

```bash
# Google Cloud Dataflow (streaming)
./gradlew :sdks:rust:dataflow -Pexample=kafka_ouroboros \
  -PextraArgs="--bootstrap_servers=<broker>:9092 --loop_topic=<topic> --num_seeds=3 --streaming=true"

# Against a Google Cloud Managed Service for Apache Kafka cluster (workers need roles/managedkafka.client)
./gradlew :sdks:rust:dataflow -Pexample=kafka_ouroboros \
  -PextraArgs="--bootstrap_servers=<bootstrap-host>:9092 --gmk=true --loop_topic=<topic> --num_seeds=3 --streaming=true"
```

`--gmk` applies `KafkaRead`/`KafkaWrite::with_google_managed_kafka_auth()` (SASL_SSL and OAUTHBEARER with worker Google credentials). Run workers on a network attached to the cluster (`--network` and `--subnetwork`). Use the `bootstrap-*` host from the private Cloud DNS zone created by Google Cloud Managed Service for Apache Kafka (`gcloud dns record-sets list --zone=gmk-...`).

## Pub/Sub Ouroboros (Cross-Language Streaming, `pubsub_ouroboros`)

This streaming pipeline reads from and writes to Cloud Pub/Sub through the Java Pub/Sub
SchemaTransforms (`PubsubRead`, `PubsubWrite`, RAW format):

1. Reads `--input_subscription`, or `--input_topic`, or else `--loop_topic`.
2. Injects one seed message to start the cycle.
3. Evolves each JSON message in Rust.
4. Writes messages with remaining cycles to `--loop_topic` (or `--input_topic` if
   `--loop_topic` is not set). After `--max_cycles` (default `5`), messages exit the loop.

`--expansion_service` defaults to the GCP expansion service of `beam::io::gcp::pubsub`:

```bash
./gradlew :sdks:rust:dataflow -Pexample=pubsub_ouroboros \
  -PextraArgs="--input_subscription=projects/<project>/subscriptions/<sub> --loop_topic=projects/<project>/topics/<topic> --streaming"
```

## Iceberg WordCount (Managed I/O, `iceberg_wordcount`)

This pipeline transfers word counts to and from an Apache Iceberg table using `ManagedWrite::new("Managed Write(ICEBERG)", managed::ICEBERG)` and `ManagedRead::new("Managed Read(ICEBERG)", managed::ICEBERG)`, which expand `beam:transform:managed:v1`:

- `--mode=write` (default): Counts words in Rust and writes `(word, count)` rows. Iceberg creates the table from the row schema. The write expands with `with_outputs()`. Every committed snapshot (`snapshots` output) is logged and increments `iceberg_wordcount/snapshots_committed`.
- `--mode=read`: Reads the table back and writes `word: count` lines to `--output`.

`--warehouse` is required. `--table` defaults to `rust_sdk.wordcount` and `--catalog_name` to
`rust_sdk`. The catalog defaults to a Hadoop catalog (`--catalog_type=hadoop`) at `--warehouse`. To configure another catalog (such as an Iceberg REST catalog), pass `--catalog_type` plus one `--catalog_property=key=value` per catalog property (endpoint, auth manager, `FileIO`, headers...).

```bash
./gradlew :sdks:rust:dataflow -Pexample=iceberg_wordcount \
  -PextraArgs="--warehouse=gs://<bucket>/warehouse"
./gradlew :sdks:rust:dataflow -Pexample=iceberg_wordcount \
  -PextraArgs="--warehouse=gs://<bucket>/warehouse --mode=read --output=gs://<bucket>/iceberg_counts"
```

## Windowed WordCount (Fixed Tumbling Windows)

This pipeline demonstrates fixed event-time windowing with custom event timestamps and formatted window boundaries. `--window_size` is in seconds (default `60`); `--base_timestamp` sets the first synthetic event time in milliseconds:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism \
  -Pexample=windowed_wordcount \
  -PextraArgs="--output=/tmp/windowed_wordcount.txt --window_size=60"
```

## Mobile Gaming Leaderboard (Triggers, Speculative Panes, & Allowed Lateness)

This pipeline processes game activity logs with triggers and lateness:
- Fixed team windows of `--team_window_duration` seconds (default `60`). The trigger is `Trigger::after_end_of_window()` with speculative early firings (`Trigger::after_count(--early_count)`, default `5`) and late firings for each late element (`Trigger::repeatedly(Trigger::after_count(1))`). Panes accumulate (`AccumulationMode::Accumulating`) and late data is accepted for `--allowed_lateness` seconds (default `120`).
- Global user windows triggered after every `--user_trigger_count` score events (default `3`, `Trigger::repeatedly(Trigger::after_count(n))`).

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=leaderboard

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=leaderboard
```

## User Session Analytics (Dynamic Sessionization & Window Merging)

This pipeline groups user interactions into dynamic sessions (`Sessions::with_gap_duration`) separated by an inactivity gap of `--gap_duration` seconds (default `300`). It merges overlapping windows per user key:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=sessions

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=sessions
```

## Sliding Window Moving Average (Overlapping Hopping Windows)

This pipeline partitions time-series readings into overlapping sliding windows (`SlidingWindows::of(size).every(period)`, set by `--window_size` and `--window_period` in seconds, defaults `30` and `10`). It tracks rolling statistics: reading count, moving average, minimum, and maximum. The default input is the gaming CSV, which it reads as `user,team,score,timestamp_ms`:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=sliding_window

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=sliding_window
```

## Type-Driven Row Schemas & Logical Types (`row_schemas`)

This pipeline demonstrates type-driven Beam Row Schemas with `#[derive(BeamRow)]` and `#[derive(BeamEnum)]`. It supports nested structs, string-backed unit enums, logical decimals (`rust_decimal::Decimal`), dates (`chrono::NaiveDate`), UTC timestamps (`chrono::DateTime<Utc>`), and global aggregations (`combine_globally`):

```bash
# Portable Prism Runner (zero network, self-contained)
./gradlew :sdks:rust:prism -Pexample=row_schemas

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=row_schemas
```

## Cross-Bundle State Conformance (`state_conformance`)

This pipeline verifies that `MapState` and `SetState` mutations (`clear`, `remove` of a present or absent key) persist across bundles. Event-time timers stage seed, mutate, and verify operations in separate bundles. Each key reports `MATCH` or `DIVERGENCE` against the Beam model (`--strict` fails the pipeline instead):

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=state_conformance

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=state_conformance
```

## Fold Combinators & State-Backed Iterables (`fold_state_backed`)

This pipeline contrasts a combiner-backed fold (`fold_per_key` with combiner lifting) against a `GroupByKey`-backed fold (`fold_values` sequential reduction over grouped iterables). It exercises runner continuation tokens and lazy paging over the Fn API State channel (`beam:coder:state_backed_iterable:v1`) under skewed hot keys (`--num_elements`, default `50000`; `--payload_bytes`, default `128`), validating numerical parity against Gauss's summation formula:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=fold_state_backed

# Google Cloud Dataflow (validating State API continuation tokens)
./gradlew :sdks:rust:dataflow -Pexample=fold_state_backed -PextraArgs="--num_elements=50000"
```

## NYC Taxi & Rideshare Analytics (`nyc_taxi`)

This pipeline demonstrates columnar data processing on Apache Parquet datasets:
- Splittable Parquet reading with column projection with `beam::io::parquet::parquetio::Read::<NycTripRecord>`.
- Type-driven schemas derived with `#[derive(BeamRow)]`.
- Filtering and key extraction mapping TLC license numbers to service providers (Uber, Lyft, Via, Juno) and day of week.
- Combiner-backed partial aggregation (`combine_per_key`) computing revenue, driver earnings, trip miles, duration, averages, and speeds.
- Sharded, distributed Parquet writing with `beam::io::parquet::parquetio::Write` with a `ParquetSink` of `DailyServiceSummary` rows.

`--output` is required. `--input` defaults to `gs://apache-beam-samples/nyc_trip/parquet/fhvhv_tripdata_2023-02.parquet`:

```bash
# Portable Prism Runner (reading from public GCS sample or local file)
./gradlew :sdks:rust:prism \
  -Pexample=nyc_taxi \
  -PextraArgs="--output=/tmp/nyc_taxi_stats"

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow \
  -Pexample=nyc_taxi \
  -PextraArgs="--output=gs://<YOUR_BUCKET>/output/nyc_taxi_stats"
```

## Traffic Routes Avro Analytics (`traffic_routes`)

This pipeline demonstrates batch event processing reading CSV telemetry and writing Apache Avro datasets:
- Splittable CSV ingestion of Caltrans PeMS freeway traffic telemetry with `textio::Read`.
- Strongly typed records derived with `#[derive(BeamRow)]`.
- Route corridor classification and traffic metric calculation (flow, occupancy, speed, congestion incidents).
- Keyed combiner partial aggregation (`combine_per_key`) with functional accumulator logic.
- Sharded, Deflate-compressed Avro writing with `beam::io::avro::avroio::Write` with an `AvroSink::<RouteTrafficSummary>`.

`--output` is required. `--input` defaults to a public sample under `gs://apache-beam-samples/traffic_sensor/`:

```bash
# Portable Prism Runner (reading from public GCS sample or local file)
./gradlew :sdks:rust:prism \
  -Pexample=traffic_routes \
  -PextraArgs="--output=/tmp/traffic_stats"

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow \
  -Pexample=traffic_routes \
  -PextraArgs="--output=gs://<YOUR_BUCKET>/output/traffic_stats"
```

## Machine Learning Inference

These examples use `RunInference` from `beam::ml`. See
[Machine Learning and Vectorized Execution](../docs/accelerated-workloads.md).

> [!NOTE]
> The model and prompt files have no default location. Pass `--input` to
> `remote_llm_inference`, `--model_path` to `cv_onnx_classification` and
> `columnar_feature_engineering`, and `--weights_path`, `--config_path` and `--tokenizer_path`
> to `text_embedding_candle`. Each section tells how to make the files. Put them in your own
> bucket (`gs://<bucket>/...`) for Dataflow.

### Remote LLM Inference (`remote_llm_inference`)

Sends one prompt per input line to Gemini with `RemoteModelHandler` and `GeminiAdapter`, writes
`Input: ..., Output: ...` lines to `--output` (default `/tmp/gemini_predictions.txt`), and writes
failed prompts to `--dlq_output` (default `/tmp/gemini_dlq.txt`) through
`RunInference::with_exception_handling()`.

- Endpoint: `--endpoint_url`, else Vertex AI in `--cloud_project`/`--cloud_region` (default
  `us-central1`), else the Gemini Developer API. `--model_name` defaults to `gemini-2.5-flash`.
- Credentials: `--api_key` or `--bearer_token` take a secret reference (`env:<VAR>`,
  `file:<PATH>`, or `gcp:projects/<p>/secrets/<s>/versions/<v>`) that workers resolve. Without
  either, workers use their service account (`RemoteAuth::ApplicationDefault`).
- Tuning: `--batch_size`, `--max_retries`, `--retry_backoff_secs`, `--max_concurrent_requests`,
  `--temperature`, `--max_output_tokens`, `--thinking_budget`.

```bash
./gradlew :sdks:rust:prism -Pexample=remote_llm_inference \
  -PextraArgs="--input=gs://<bucket>/datasets/llm_prompts_1k.txt --api_key=env:GEMINI_API_KEY"
```

[`scripts/make_prompts.py`](remote_llm_inference/scripts/make_prompts.py) builds the benchmark
prompt set from the public Shakespeare corpus, and
[`python/gemini_baseline.py`](remote_llm_inference/python/gemini_baseline.py) is the Python
baseline.

### Image Classification with ONNX Runtime (`cv_onnx_classification`)

Reads a manifest of image paths (`--input`, default
`gs://apache-beam-ml/testing/inputs/openimage_50k_benchmark.txt`), resizes and normalizes each
image, runs MobileNetV2 with `OnnxModelHandler` in batches of `--min_batch_size` to
`--max_batch_size` (defaults `10` and `100`), and writes `path,argmax` lines to `--output`
(default `/tmp/onnx_predictions.txt`). Images that fail go to `--dlq_output` (default
`<output>_failures`). `--model_path` is required: export the model with
[`export_model.py`](cv_onnx_classification/export_model.py). `OnnxDeviceOptions` adds
`--device` (`cpu`, `cuda`, `tensorrt`, `coreml`), `--device_id`, `--allow_cpu_fallback`, and
`--dylib_path`. GPU providers need the matching crate feature (`-PexampleFeatures=cuda`,
`tensorrt` or `coreml`):

```bash
./gradlew :sdks:rust:prism -Pexample=cv_onnx_classification \
  -PextraArgs="--model_path=/path/to/mobilenet_v2_torchvision.onnx --dylib_path=/path/to/libonnxruntime.dylib"
```

The [`Dockerfile`](cv_onnx_classification/Dockerfile) builds a self-contained CUDA worker image
with ONNX Runtime GPU from the repository root (`docker build -f
sdks/rust/examples/cv_onnx_classification/Dockerfile .`). It is not a `prebakedImage` layout.

### Text Embeddings with Candle (`text_embedding_candle`)

Embeds text lines (`--input`, default `gs://apache-beam-ml/testing/inputs/sentences_50k.txt`)
with `sentence-transformers/all-MiniLM-L6-v2` through `BertEmbeddingModelHandler`, and writes
sharded `*.jsonl` files to `--output` (default `/tmp/candle_embeddings`). Model artifacts come from
`--weights_path`, `--config_path`, and `--tokenizer_path`, which are required. They are the
`model.safetensors`, `config.json` and `tokenizer.json` files of the model on the Hugging Face Hub.
`CandleDeviceOptions` adds `--device` (`cpu`, `cuda`, `metal`), `--device_id`, and
`--allow_cpu_fallback`. GPU devices need the `cuda` or `metal` crate feature:

```bash
./gradlew :sdks:rust:prism -Pexample=text_embedding_candle \
  -PextraArgs="--input=/path/to/sentences.txt --output=/tmp/embeddings --weights_path=/path/to/model.safetensors --config_path=/path/to/config.json --tokenizer_path=/path/to/tokenizer.json"

# GPU: add the crate feature and the device flag
./gradlew :sdks:rust:prism -Pexample=text_embedding_candle -PexampleFeatures=cuda \
  -PextraArgs="--device=cuda --weights_path=... --config_path=... --tokenizer_path=..."
```

The [`Dockerfile`](text_embedding_candle/Dockerfile) builds a self-contained CUDA worker image
from the repository root, like `cv_onnx_classification`.

### Table Row Inference (`columnar_feature_engineering`)

This is the Rust port of the Python batch benchmark
[`table_row_inference.py`](../../python/apache_beam/examples/inference/table_row_inference.py),
configured by
[`beam_Inference_Python_Benchmarks_Dataflow_Table_Row_Inference_Batch.txt`](../../../.github/workflows/load-tests-pipeline-options/beam_Inference_Python_Benchmarks_Dataflow_Table_Row_Inference_Batch.txt).
Both pipelines run the same workload for side-by-side benchmarking.

| Step | Python | Rust |
|---|---|---|
| Read | `ReadFromText(--input_file)` | `textio::Read::new("ReadLines", --input)`, then `reshuffle` |
| Expand | `FlatMap([line] * --input_expand_factor)` when > 1 | the same |
| Parse | `json.loads`, `beam.Row(feature1..feature5)` | `serde_json` into `TableRow` |
| Features | `np.array([...], dtype=float32)`, `--feature_columns` order | `[f64; 5] as f32`, `FEATURE_COLUMNS` order |
| Batch | `BatchElements`, dynamic 1..10000 | `RunInference` `--min_batch_size`..`--max_batch_size` (1..10000), flushed per bundle |
| Model | sklearn `RandomForestClassifier.predict` (pickle) | same model exported to ONNX, ONNX Runtime CPU |
| Output | `json.dumps` of `row_key, prediction, model_id, input_*` to `<output_file>.jsonl` | byte-identical lines to `<output>.jsonl` (one file, or `--num_shards` shards) |

The only difference in the output is `model_id`, which names the model file that each side loads.
The Python benchmark configuration writes to BigQuery (`--output_table`); the Rust pipeline
writes only the JSONL file, which matches the Python `--output_file` mode.

Inputs:

- Data (`--input`, the default): `gs://apache-beam-ml/testing/inputs/table_rows_100k_benchmark.jsonl`
  (100k rows of `{"id", "feature1".."feature5"}`). The benchmark uses
  `--input_expand_factor=100` for 10M rows.
- Model (`--model_path`, required): the ONNX export of
  `gs://apache-beam-ml/models/sklearn_table_classifier.pkl`, made by
  [`export_model.py`](columnar_feature_engineering/export_model.py). The model has 12 trees, a
  maximum depth of 8, 5 features, and classes `[0, 1]`. It is exported with scikit-learn 1.5.2
  and skl2onnx at opset 17 / ai.onnx.ml 3, with `zipmap=False`. The graph maps
  `input: float32[N, 5]` to `label: int64[N]` and `probabilities: float32[N, 2]`.

Export the model, upload it, and regenerate the test fixtures:

```bash
python3.12 -m venv venv && . venv/bin/activate
pip install 'scikit-learn==1.5.2' 'numpy>=2,<2.5' skl2onnx onnx onnxruntime
python sdks/rust/examples/columnar_feature_engineering/export_model.py \
  --upload=gs://<bucket>/models/table_row_rf.onnx \
  --fixture_dir sdks/rust/examples/columnar_feature_engineering/tests/fixtures
```

The binary loads ONNX Runtime dynamically (`ort/load-dynamic`) from `ORT_DYLIB_PATH`, or from
`--dylib_path` if set. The `onnxruntime` pip wheel provides the library, for example at
`venv/lib/python3.12/site-packages/onnxruntime/capi/libonnxruntime.*.dylib`.

```bash
ORT_DYLIB_PATH=/path/to/libonnxruntime.dylib ./gradlew :sdks:rust:prism \
  -Pexample=columnar_feature_engineering \
  -PextraArgs="--input=rows.jsonl --model_path=table_row_rf.onnx --output=/tmp/predictions"
```

Other flags: `--input_expand_factor` (default `1`), `--min_batch_size` / `--max_batch_size`
(defaults `1` / `10000`), `--intra_op_threads` (default `1`), `--num_shards` (default `0`, a
single file), and the `OnnxDeviceOptions` flags `--device`, `--device_id`, `--allow_cpu_fallback`.

Dataflow workers also need ONNX Runtime. The
[`Dockerfile`](columnar_feature_engineering/Dockerfile) puts the CPU build and the pipeline
binary into an image and sets `ORT_DYLIB_PATH`:

```bash
./gradlew :sdks:rust:prebakedImage -Pexample=columnar_feature_engineering \
  -Pdocker-repository-root=us-central1-docker.pkg.dev/<project>/<repo> -Ppush-containers
./gradlew :sdks:rust:dataflow -Pexample=columnar_feature_engineering \
  -PsdkContainerImage=us-central1-docker.pkg.dev/<project>/<repo>/beam_rust_example_columnar_feature_engineering:<sdk version> \
  -PworkerBinary=none -PworkerMachineType=n1-standard-4 -PnumWorkers=10 -PmaxNumWorkers=10 \
  "-PextraArgs=--input_expand_factor=100 --model_path=gs://<bucket>/models/table_row_rf.onnx --output=gs://<bucket>/output/rs_table_rows"
```

The ignored test runs the whole pipeline with ONNX Runtime and compares it with the Python output
for the fixture rows:

```bash
./gradlew :sdks:rust:test -Ppkg=columnar_feature_engineering
ORT_DYLIB_PATH=/path/to/libonnxruntime.dylib TABLE_ROW_ONNX_MODEL=/path/to/table_row_rf.onnx \
  ./gradlew :sdks:rust:test -Ppkg=columnar_feature_engineering -PtestArgs=--ignored
```

## Building worker binaries

Submitting to Dataflow or Docker requires a Linux build of the pipeline binary. See [Running Rust Pipelines](../docs/runners.md#building-linux-worker-binaries).
