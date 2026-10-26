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

Unit and pipeline tests live in each example's `tests/` directory. `minimal_wordcount` has no
tests:

```bash
./gradlew :sdks:rust:test -Ppkg=<example>
```

| Example | Topic |
|---|---|
| [`minimal_wordcount`](#minimal-wordcount) | Simplest pipeline, hardcoded paths |
| [`wordcount`](#wordcount) | Pipeline options, composite transforms, metrics, pre-baked image, Flex Template |
| [`streaming`](#streaming-continuous-ingestion-with-periodicimpulse) | `PeriodicImpulse`, fixed windows, `sum_per_key` |
| [`side_inputs`](#side-inputs) | Singleton and iterable side inputs in a `DoFn` struct |
| [`join`](#relational-joins) | `co_group_by_key`, inner, left, right and full outer joins |
| [`tfidf`](#tf-idf-tfidf) | `distinct`, counts, joins, singleton side input, TFRecord of `tf.train.Example` |
| [`gaming`](#mobile-gaming-stateful-processing--metrics) | Stateful DoFn, user metrics |
| [`partition`](#multiple-outputs--flatten-partition) | Typed outputs with `#[derive(Outputs)]`, `Flatten` |
| [`windowing`](#sliding-windows--sessions-windowing) | Sliding windows, session windows |
| [`stateful_buffering`](#stateful-buffering-with-timers-stateful_buffering) | `BagState`, `CombiningState`, event-time and processing-time timers |
| [`splittable_dofn`](#splittable-dofn-splittable_dofn) | `OffsetRange` restriction, initial splits, self-checkpoints |
| [`bigquery_tornadoes`](#bigquery-tornadoes-cross-language) | Java BigQuery read and write |
| [`bigtable_wordcount`](#bigtable-wordcount-cross-language-bigtable_wordcount) | Java Bigtable read and write |
| [`ouroboros`](#ouroboros-cross-language-streaming-ouroboros) | Java Kafka or Pub/Sub read and write, Managed error handling |
| [`iceberg_wordcount`](#iceberg-wordcount-managed-io-iceberg_wordcount) | Managed I/O Iceberg, Managed outputs |
| [`windowed_wordcount`](#windowed-wordcount-fixed-tumbling-windows) | Event timestamps, fixed windows |
| [`leaderboard`](#mobile-gaming-leaderboard-triggers-speculative-panes--allowed-lateness) | Triggers, accumulation, allowed lateness |
| [`row_schemas`](#type-driven-row-schemas--logical-types-row_schemas) | `#[derive(BeamRow)]`, logical types |
| [`nyc_taxi`](#nyc-taxi--rideshare-analytics-nyc_taxi) | Parquet read and write |
| [`remote_llm_inference`](#remote-llm-inference-remote_llm_inference) | `RunInference` with Gemini, dead-letter output |
| [`cv_onnx_classification`](#image-classification-with-onnx-runtime-cv_onnx_classification) | `RunInference` with ONNX Runtime |
| [`text_embedding_candle`](#text-embeddings-with-candle-text_embedding_candle) | `RunInference` with Candle |
| [`table_row_inference`](#table-row-inference-table_row_inference) | `RunInference` with an ONNX RandomForest on JSONL rows |

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
(default `/tmp/output.txt`). The `CountWords` composite is public, and other examples
reuse it:

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

This pipeline counts the ticks of a `PeriodicImpulse` in fixed event-time windows. Each tick
is even or odd, and the `CountTicks` composite counts both kinds per window with
`sum_per_key`.

Flags: `--impulse_interval_ms` (default `500`), `--window_size_secs` (default `5`), `--limit`
(number of ticks; without it, the source stops after `--max_read_time_secs`, default `600`),
and `--output` (default `/tmp/streaming_output.txt`). `--streaming` is a standard pipeline
option:

```bash
# Portable Prism Runner (local streaming execution)
./gradlew :sdks:rust:prism \
  -Pexample=streaming \
  -PextraArgs="--streaming --limit=20 --impulse_interval_ms=1000 --window_size_secs=10 --output=/tmp/streaming_output.txt"

# Google Cloud Dataflow (Streaming Engine)
./gradlew :sdks:rust:dataflow \
  -Pexample=streaming \
  -PextraArgs="--streaming --max_read_time_secs=600 --impulse_interval_ms=1000 --window_size_secs=10"
```

## Side Inputs

This pipeline counts the words of King Lear that are longer than the mean word length and
are not stopwords. It computes the mean length from the words themselves and reads it as a
singleton side input (`mean_globally().as_singleton()`). The stopwords are an iterable side
input (`as_iter`). A registered `DoFn` struct keeps both views as fields. Flags: `--input`,
`--output` (default `/tmp/side_inputs.txt`) and `--stopwords` (comma-separated):

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=side_inputs -PextraArgs="--stopwords=should,would,though"

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=side_inputs
```

The `apache-beam-runner-tests` suite tests multimap side inputs (`as_multimap`).

## Relational Joins

This pipeline joins a CSV file of users (`user_id,name`) with a CSV file of orders
(`user_id,item`). The `JoinOrders` composite groups both with `co_group_by_key` and applies
the inner, left, right and full outer joins, then merges the results with `Flatten`.
`--users` and `--orders` are required. `--output` defaults to `/tmp/join.txt`:

```bash
./gradlew :sdks:rust:prism -Pexample=join \
  -PextraArgs="--users=/path/to/users.csv --orders=/path/to/orders.csv --output=/tmp/join.txt"
```

## TF-IDF (`tfidf`)

This pipeline reads a collection of documents (one file per document, by default the works
of Shakespeare) and computes the TF-IDF of each word in each document. It uses `distinct`
for the documents and the `(document, word)` pairs, `count_per_element` and `count_globally`,
a singleton side input for the number of documents, and `inner_join` to bring the counts
together.

The output is a TFRecord of `tf.train.Example`s, one per document, with the features
`document`, `words` and `tfidf`, ready for TensorFlow (`tf.data.TFRecordDataset` and
`tf.io.parse_single_example`):

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=tfidf \
  -PextraArgs="--input=/path/to/docs/* --output=/tmp/tfidf.tfrecord"

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=tfidf
```

## Mobile Gaming (Stateful Processing & Metrics)

This pipeline parses gaming events (`--input`, default
`gs://apache-beam-samples/game/small/gaming_data.csv`) with a `Counter` and a `Distribution`.
It writes two typed results with the `--output` prefix (default `/tmp/gaming`):

- `<output>-totals*.txt`: the total score of each team, from `sum_per_key`.
- `<output>-milestones*.txt`: one report each time a team total passes a multiple of
  `--threshold` (default `500`). A registered `StatefulDoFn` keeps the total in a
  [`CombiningState`](../beam/core/src/transforms/dofn/state.rs) and the contributors in a
  [`SetState`](../beam/core/src/transforms/dofn/state.rs).

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=gaming -PextraArgs="--output=/tmp/gaming"

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=gaming
```

## Multiple Outputs & Flatten (`partition`)

This pipeline reads student scores from a CSV file (`name,score`) and sends each student to
one of three typed outputs: `Honours`, `Pass` or `Support`. The tiers are the variants of a
`#[derive(Outputs)]` enum. Each tier is processed on its own, and `Flatten` merges the tiers
again. `--input` is required. `--output` defaults to `/tmp/partition.txt`:

```bash
./gradlew :sdks:rust:prism -Pexample=partition \
  -PextraArgs="--input=/path/to/scores.csv --output=/tmp/partition.txt"
```

## Sliding Windows & Sessions (`windowing`)

This pipeline reads the gaming CSV (`user,team,score,timestamp_ms`) and windows the same
events in two ways:

- `SlidingWindows` of `--window_minutes` (default `30`) every `--period_minutes` (default
  `10`). Each window gets the count, mean, min and max score of each team from one
  `Compose` combine function.
- `Sessions` with a gap of `--gap_minutes` (default `5`). Each session gets the number of
  events of the user.

Bad lines increment the `windowing/bad_lines` counter. `--output` is a prefix (default
`/tmp/windowing`): the team statistics go to `<output>-teams.txt` and the sessions to
`<output>-sessions.txt`:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=windowing -PextraArgs="--output=/tmp/windowing"

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=windowing
```

## Stateful Buffering with Timers (`stateful_buffering`)

This pipeline generates sensor readings at a fixed rate and buffers the values of each
sensor in fixed windows. The stateful DoFn keeps the values in a `BagState` and their running
sum in a `CombiningState`. The first value of a batch sets two timers: an event-time timer
at the end of the window and a processing-time timeout. The timer that fires first emits the
batch with its reason and clears the other timer.

Flags: `--readings` (default `100`), `--per_second` (default `20`), `--sensors` (default
`3`), `--window_secs` (default `2`), `--timeout_millis` (default `500`) and `--output`
(default `/tmp/stateful_buffering_output.txt`):

```bash
./gradlew :sdks:rust:prism -Pexample=stateful_buffering -PextraArgs="--output=/tmp/batches.txt"
```

## Splittable DoFn (`splittable_dofn`)

This pipeline reads whole text files with `fileio::Match` and `fileio::ReadMatches` and
splits each text into lines with a splittable DoFn. The restriction is the `OffsetRange` of
the bytes of the text. The DoFn splits it into chunks of `--chunk_bytes` (default `65536`)
and claims the start offset of each line with an `OffsetRangeTracker`. After
`--lines_per_call` lines (default `1000`), it returns `ProcessContinuation::Resume` (a
self-checkpoint). `--input` (a path or glob) is required. `--output` defaults to
`/tmp/splittable_dofn_output.txt`:

```bash
./gradlew :sdks:rust:prism -Pexample=splittable_dofn \
  -PextraArgs="--input=/path/to/texts/*.txt --output=/tmp/lines.txt"
```

## BigQuery Tornadoes (Cross-Language)

This pipeline mixes native Rust processing with Java cross-language I/O transforms through the Beam Expansion Service:

1. Reads weather station observations with the Java BigQuery Storage Read API (`--input`, default `apache-beam-testing.samples.weather_stations`, or `--input_query`).
2. Filters for tornado events and extracts the month in Rust.
3. Computes monthly tornado counts with the `CountTornadoes` composite.
4. Writes `(month, tornado_count)` rows to the `--output` table with the Java BigQuery Write transform. `--output` is required.

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

- `--mode=write` (default): Counts the words of `--input` with `wordcount::CountWords` and writes one row per word (`<column_family>:count`) with `BigtableWrite`.
- `--mode=read`: Reads the table with `BigtableRead`, decodes each column in Rust, and writes `word: count` lines to `--output`. `--output` is required in this mode.

`--column_family` defaults to `counts`. The table and its column family must exist first:

```bash
gcloud bigtable instances tables create <table> --instance=<instance> --column-families=counts

# Google Cloud Dataflow: write, then read back
./gradlew :sdks:rust:dataflow -Pexample=bigtable_wordcount \
  -PextraArgs="--bigtable_project=<project> --bigtable_instance=<instance> --bigtable_table=<table>"
./gradlew :sdks:rust:dataflow -Pexample=bigtable_wordcount \
  -PextraArgs="--bigtable_project=<project> --bigtable_instance=<instance> --bigtable_table=<table> --mode=read --output=gs://<bucket>/counts"
```

## Ouroboros (Cross-Language Streaming, `ouroboros`)

This streaming pipeline reads from and writes to the same topic (`--loop_topic`).
`--transport` selects the messaging system:

- `kafka`: the Java `kafka_read` and `kafka_write` SchemaTransforms, expanded through
  Managed I/O. The read starts at the earliest offset. `--bootstrap_servers` is required,
  and `--group_id` is optional.
- `pubsub`: the Java Pub/Sub SchemaTransforms (`PubsubRead`, `PubsubWrite`, RAW format). The
  read uses `--subscription` if it is set, else `--loop_topic`.

The pipeline adds `--num_seeds` new messages (default `1`) to the loop. Rust changes each
JSON message on each pass. After `--max_cycles` passes (default `5`), a message leaves the
loop. The `ouroboros` counters `seeded`, `evolved`, `ascended` and `malformed` show the
progress. The job runs until cancelled.

On Kafka, both transforms use Managed error handling (`with_error_handling`, expanded with
`to_managed()?.with_all_outputs()` or `with_outputs()`). Records that Java cannot decode or
serialize increment `kafka_read_errors` or `kafka_write_errors` and do not fail the bundle.

```bash
# Kafka on Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=ouroboros \
  -PextraArgs="--transport=kafka --bootstrap_servers=<broker>:9092 --loop_topic=<topic> --num_seeds=3 --streaming=true"

# Pub/Sub on Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=ouroboros \
  -PextraArgs="--transport=pubsub --loop_topic=projects/<project>/topics/<topic> --subscription=projects/<project>/subscriptions/<sub> --streaming=true"

# Kafka on a Google Cloud Managed Service for Apache Kafka cluster (workers need roles/managedkafka.client)
./gradlew :sdks:rust:dataflow -Pexample=ouroboros \
  -PextraArgs="--transport=kafka --bootstrap_servers=<bootstrap-host>:9092 --gmk --loop_topic=<topic> --num_seeds=3 --streaming=true"
```

`--gmk` applies `KafkaRead`/`KafkaWrite::with_google_managed_kafka_auth()` (SASL_SSL and OAUTHBEARER with worker Google credentials). Run workers on a network attached to the cluster (`--network` and `--subnetwork`). Use the `bootstrap-*` host from the private Cloud DNS zone created by Google Cloud Managed Service for Apache Kafka (`gcloud dns record-sets list --zone=gmk-...`).

## Iceberg WordCount (Managed I/O, `iceberg_wordcount`)

This pipeline transfers word counts to and from an Apache Iceberg table using `apply("Managed Write(ICEBERG)", ManagedWrite::new(managed::ICEBERG))` and `apply("Managed Read(ICEBERG)", ManagedRead::new(managed::ICEBERG))`, which expand `beam:transform:managed:v1`:

- `--mode=write` (default): Counts the words of `--input` with `wordcount::CountWords` and writes `(word, count)` rows. Iceberg creates the table from the row schema. The write expands with `with_outputs()`, and the pipeline logs each committed snapshot (`snapshots` output). The Iceberg write accepts `error_handling` only in its `merge-on-read` mode, so this append write has no error output.
- `--mode=read`: Reads the table back and writes `word: count` lines to `--output`. `--output` is required in this mode.

`--warehouse` is required. `--table` defaults to `rust_sdk.wordcount` and `--catalog_name` to
`rust_sdk`. The catalog defaults to a Hadoop catalog (`--catalog_type=hadoop`) at `--warehouse`. To configure another catalog (such as an Iceberg REST catalog), pass `--catalog_type` plus one `--catalog_property=key=value` per catalog property (endpoint, auth manager, `FileIO`, headers...).

```bash
./gradlew :sdks:rust:dataflow -Pexample=iceberg_wordcount \
  -PextraArgs="--warehouse=gs://<bucket>/warehouse"
./gradlew :sdks:rust:dataflow -Pexample=iceberg_wordcount \
  -PextraArgs="--warehouse=gs://<bucket>/warehouse --mode=read --output=gs://<bucket>/iceberg_counts"
```

## Windowed WordCount (Fixed Tumbling Windows)

This pipeline counts words in fixed event-time windows. It reuses `wordcount::CountWords` on
windowed data. `--input` defaults to King Lear, `--window_size` is in seconds (default `60`),
and `--base_timestamp` sets the first synthetic event time in milliseconds:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism \
  -Pexample=windowed_wordcount \
  -PextraArgs="--output=/tmp/windowed_wordcount.txt --window_size=60"
```

## Mobile Gaming Leaderboard (Triggers, Speculative Panes, & Allowed Lateness)

This pipeline parses the game events with `gaming::ParseGameEvents` and computes scores with
triggers and lateness:

- Team scores (`CalculateTeamScores`): fixed windows of `--team_window_duration` seconds (default `60`). The trigger is `Trigger::after_end_of_window()` with speculative early firings (`Trigger::after_count(--early_count)`, default `5`) and late firings for each late element (`Trigger::repeatedly(Trigger::after_count(1))`). Panes accumulate (`AccumulationMode::Accumulating`) and late data is accepted for `--allowed_lateness` seconds (default `120`).
- User scores (`CalculateUserScores`): global windows triggered after every `--user_trigger_count` score events (default `3`, `Trigger::repeatedly(Trigger::after_count(n))`).

The pipeline writes `<output>-team*.txt` and `<output>-user*.txt`. The `--output` prefix
defaults to `/tmp/leaderboard`:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=leaderboard

# Google Cloud Dataflow
./gradlew :sdks:rust:dataflow -Pexample=leaderboard
```

## Type-Driven Row Schemas & Logical Types (`row_schemas`)

This pipeline reads customer records from JSON lines and summarizes them with type-driven Beam Row Schemas (`#[derive(BeamRow)]` and `#[derive(BeamEnum)]`). The records have nested structs, string-backed unit enums, logical decimals (`rust_decimal::Decimal`), dates (`chrono::NaiveDate`) and UTC timestamps (`chrono::DateTime<Utc>`). The `SummarizeAccounts` composite combines them in one pass with `Compose(...).map_output`. [`main.rs`](row_schemas/src/main.rs) shows a sample input line. `--input` is required. `--output` defaults to `/tmp/row_schemas.txt`:

```bash
./gradlew :sdks:rust:prism -Pexample=row_schemas \
  -PextraArgs="--input=/path/to/customers.jsonl --output=/tmp/row_schemas.txt"
```

## NYC Taxi & Rideshare Analytics (`nyc_taxi`)

This pipeline demonstrates columnar data processing on Apache Parquet datasets:
- Splittable Parquet reading with column projection with `beam::io::parquet::parquetio::Read::<NycTripRecord>`.
- Type-driven schemas derived with `#[derive(BeamRow)]`.
- Key extraction that maps TLC license numbers to service providers (Uber, Lyft, Via, Juno) and day of week.
- One keyed `Compose` combine (`SummarizeTrips`) that the runner can lift. It computes revenue, driver earnings, trip miles, duration, averages, and speeds.
- Sharded Parquet writing of `DailyServiceSummary` rows with ZSTD compression through `beam::io::parquet::parquetio::Write::<DailyServiceSummary>`.

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

## Machine Learning Inference

These examples use `RunInference` from `beam::ml`. See
[Machine Learning and Vectorized Execution](../docs/accelerated-workloads.md).

> [!NOTE]
> The model and prompt files have no default location. Pass `--input` to
> `remote_llm_inference`, `--model_path` to `cv_onnx_classification` and
> `table_row_inference`, and `--weights_path`, `--config_path` and `--tokenizer_path`
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
prompt set from the public Shakespeare corpus.

### Image Classification with ONNX Runtime (`cv_onnx_classification`)

Reads a manifest of image paths (`--input`, default
`gs://apache-beam-ml/testing/inputs/openimage_50k_benchmark.txt`), resizes and normalizes each
image, runs MobileNetV2 with `OnnxModelHandler` in batches of `--min_batch_size` to
`--max_batch_size` (defaults `10` and `100`), and writes `path,class` lines to `--output`
(default `/tmp/onnx_predictions.txt`). Images that fail go to `--dlq_output` (default
`/tmp/onnx_failures.txt`). `--model_path` is required: export the model with
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

`--inference_accelerator` puts an accelerator resource hint on `RunInference` only, for example
`--inference_accelerator="type:nvidia-tesla-t4;count:1;install-nvidia-driver"` with
`--device=cuda`. On Dataflow the image reads then run on CPU workers that autoscale with the
backlog, and only the inference stage gets GPU workers. Do not also set the
`worker_accelerator` service option, which puts a GPU on every worker.

### Text Embeddings with Candle (`text_embedding_candle`)

Embeds text lines (`--input`, default `gs://apache-beam-ml/testing/inputs/sentences_50k.txt`)
with `sentence-transformers/all-MiniLM-L6-v2` through `BertEmbeddingModelHandler`, and writes
sharded `*.jsonl` files to `--output` (default `/tmp/candle_embeddings`). Texts that fail to
embed go to `--dlq_output` (default `/tmp/candle_failures.txt`). Model artifacts come from
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

### Table Row Inference (`table_row_inference`)

This pipeline runs batch model inference on tabular data.

| Step | Operation |
|---|---|
| Read | `apply("ReadLines", textio::Read::new(--input))`, then `reshuffle` |
| Expand | `flat_map` that repeats each line `--input_expand_factor` times, when > 1 |
| Parse | `try_map` into `TableRow`; bad lines go to `--dlq_output` |
| Features | `feature1`..`feature5` as `[f32; 5]` (`TableRow::features`) |
| Batch | `RunInference` `--min_batch_size`..`--max_batch_size` (1..10000), flushed per bundle |
| Model | ONNX export of tabular classifier, ONNX Runtime CPU |
| Output | Serialized lines with prediction and model ID to `<output>.jsonl` |

The pipeline writes results to JSONL files (one file, or `--num_shards` shards). Lines that
are not valid rows go to `--dlq_output` (default `/tmp/table_row_failures.txt`).

Inputs:

- Data (`--input`, the default): `gs://apache-beam-ml/testing/inputs/table_rows_100k_benchmark.jsonl`
  (100k rows of `{"id", "feature1".."feature5"}`). The benchmark uses
  `--input_expand_factor=100` for 10M rows.
- Model (`--model_path`, required): the ONNX export of
  `gs://apache-beam-ml/models/sklearn_table_classifier.pkl`, made by
  [`export_model.py`](table_row_inference/export_model.py). The model has 12 trees, a
  maximum depth of 8, 5 features, and classes `[0, 1]`. It is exported with scikit-learn 1.5.2
  and skl2onnx at opset 17 / ai.onnx.ml 3, with `zipmap=False`. The graph maps
  `input: float32[N, 5]` to `label: int64[N]` and `probabilities: float32[N, 2]`.

Export the model, upload it, and regenerate the test fixtures:

```bash
python3.12 -m venv venv && . venv/bin/activate
pip install 'scikit-learn==1.5.2' 'numpy>=2,<2.5' skl2onnx onnx onnxruntime
python sdks/rust/examples/table_row_inference/export_model.py \
  --upload=gs://<bucket>/models/table_row_rf.onnx \
  --fixture_dir sdks/rust/examples/table_row_inference/tests/fixtures
```

The binary loads ONNX Runtime dynamically (`ort/load-dynamic`) from `ORT_DYLIB_PATH`, or from
`--dylib_path` if set. The `onnxruntime` pip wheel provides the library, for example at
`venv/lib/python3.12/site-packages/onnxruntime/capi/libonnxruntime.*.dylib`.

```bash
ORT_DYLIB_PATH=/path/to/libonnxruntime.dylib ./gradlew :sdks:rust:prism \
  -Pexample=table_row_inference \
  -PextraArgs="--input=rows.jsonl --model_path=table_row_rf.onnx --output=/tmp/predictions"
```

Other flags: `--input_expand_factor` (default `1`), `--min_batch_size` / `--max_batch_size`
(defaults `1` / `10000`), `--intra_op_threads` (default `1`), `--num_shards` (default `0`, a
single file), and the `OnnxDeviceOptions` flags `--device`, `--device_id`, `--allow_cpu_fallback`.

Dataflow workers also need ONNX Runtime. The
[`Dockerfile`](table_row_inference/Dockerfile) puts the CPU build and the pipeline
binary into an image and sets `ORT_DYLIB_PATH`:

```bash
./gradlew :sdks:rust:prebakedImage -Pexample=table_row_inference \
  -Pdocker-repository-root=us-central1-docker.pkg.dev/<project>/<repo> -Ppush-containers
./gradlew :sdks:rust:dataflow -Pexample=table_row_inference \
  -PsdkContainerImage=us-central1-docker.pkg.dev/<project>/<repo>/beam_rust_example_table_row_inference:<sdk version> \
  -PworkerBinary=none -PworkerMachineType=n1-standard-4 -PnumWorkers=10 -PmaxNumWorkers=10 \
  "-PextraArgs=--input_expand_factor=100 --model_path=gs://<bucket>/models/table_row_rf.onnx --output=gs://<bucket>/output/rs_table_rows"
```

The ignored test runs the whole pipeline with ONNX Runtime and compares it with the Python output
for the fixture rows:

```bash
./gradlew :sdks:rust:test -Ppkg=table_row_inference
ORT_DYLIB_PATH=/path/to/libonnxruntime.dylib TABLE_ROW_ONNX_MODEL=/path/to/table_row_rf.onnx \
  ./gradlew :sdks:rust:test -Ppkg=table_row_inference -PtestArgs=--ignored
```

## Building worker binaries

Submitting to Dataflow or Docker requires a Linux build of the pipeline binary. See [Running Rust Pipelines](../docs/runners.md#building-linux-worker-binaries).
