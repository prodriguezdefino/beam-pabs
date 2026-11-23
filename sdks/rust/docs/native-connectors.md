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

# Native I/O Connectors

This guide shows how to read from and write to external storage and messaging services with the native Rust connectors. Every connector on this page runs inside the Rust worker process without starting a cross-language expansion service. For file formats (`textio`, `parquetio`, `avroio`, `json`, `tfrecordio`), see [File Formats and Managed I/O](#file-formats-and-managed-io). For connector internals, benchmarks, and cross-language `SchemaTransform` providers, see [rust-native-io-providers.md](rust-native-io-providers.md).

## Dependencies and Cargo Features

Depend on `apache-beam` (library name `beam`) and enable the Cargo feature for each connector your pipeline uses. Enable `mimalloc` in pipeline binaries, and add `dataflow` when you submit jobs to Google Cloud Dataflow:

```toml
[dependencies]
apache-beam = { path = "../beam/sdks/rust/beam", features = [
    "dataflow",
    "gcp-bigquery",
    "gcp-bigtable",
    "gcp-gcs",
    "kafka",
    "iceberg",
    "mimalloc",
] }
tokio = { version = "1", features = ["full"] }
```

`beam::io` re-exports each enabled connector crate. Libraries that do not link a runner can depend on the standalone connector crates directly:

| Service | `apache-beam` feature | `beam::io` path | Standalone crate (library) | Example crate |
|---|---|---|---|---|
| BigQuery Storage | `gcp-bigquery` | `beam::io::gcp::bigquery` | `apache-beam-io-gcp` (`beam_io_gcp`, feature `bigquery`) | [`bigquery_tornadoes`](../examples/bigquery_tornadoes/src/lib.rs) |
| Cloud Bigtable | `gcp-bigtable` | `beam::io::gcp::bigtable` | `apache-beam-io-gcp` (`beam_io_gcp`, feature `bigtable`) | [`bigtable_wordcount`](../examples/bigtable_wordcount/src/lib.rs) |
| Apache Kafka | `kafka` | `beam::io::kafka` | `apache-beam-io-kafka` (`beam_io_kafka`) | [`ouroboros`](../examples/ouroboros/src/kafka.rs) |
| Apache Iceberg | `iceberg` | `beam::io::iceberg` | `apache-beam-io-iceberg` (`beam_io_iceberg`) | — |
| Cloud Storage (`gs://`) | `gcp-gcs` | `beam::io::gcp::gcs` | `apache-beam-io-gcp` (`beam_io_gcp`, feature `gcs`) | [`wordcount`](../examples/wordcount/src/lib.rs) |

## Shared Patterns

### Typed Structs and Dynamic Rows

BigQuery, Bigtable, and Iceberg provide typed transforms (`Read<T>`, `Write<T>`) for `#[derive(BeamRow)]` structs and dynamic transforms (`ReadRows`, `WriteRows`) for `beam::schema::Row`. Kafka `Read` and `Write` take codecs from `beam::io::kafka::codec`, and `Typed::new(codec)` adapts any `Row` codec to a `#[derive(BeamRow)]` struct.

- Use `Read<T>` and `Write<T>` when your pipeline knows the schema at compile time. `Read<T>` projects the fields of `T` on BigQuery and Iceberg table reads unless you set explicit columns.
- Use `ReadRows` and `WriteRows` when the schema comes from a table, a catalog, or a runtime configuration.
- Give the input `PCollection<Row>` of `WriteRows` a row schema. Call `.with_row_schema(&schema)` after a `Map` or `ParDo` that builds `Row` values.
- Call `.with_encoded_rows()` on `ReadRows` when rows pass straight to `WriteRows`, across a fusion break, or to another SDK. That mode keeps each row as `beam:coder:row:v1` bytes without building `Row` values.

### Construction Errors and Dead-Letter Outputs

Every native `Read`, `ReadRows`, `Write`, and `WriteRows` implements both `PTransform` and `TryPTransform` (`Error = beam::Error`):

- `p.apply("Name", transform)` records invalid options or schema lookup failures on the `Pipeline`. `Pipeline::validate` and `Pipeline::run` return that construction error before execution starts.
- `p.try_apply("Name", transform)?` returns the expansion error immediately as `beam::Result`.

Reads fail the bundle on a bad record by default. Call `.with_failures()` on `Read` or `ReadRows` to get `WithFailures<Out, Row>`, which holds `.output` and `.failures` (`PCollection<Row>`). Chain `.failures_to("WriteDlq", sink)` to attach a sink to `.failures` and keep chaining `.output`.

Writes return a `WriteResult` struct with `pub failures: PCollection<Row>` (Iceberg also returns `pub snapshots: PCollection<SnapshotInfo>`). Rows that fail schema conversion or per-row server checks go to `written.failures` with an `error_message` field instead of failing the bundle. Transport errors and unrecoverable server errors retry with backoff and fail the bundle when retries run out.

## BigQuery Storage (`beam::io::gcp::bigquery`)

Enable the `gcp-bigquery` feature. `Read` and `ReadRows` read tables and SQL queries in Arrow format with the BigQuery Storage Read API. `Write` and `WriteRows` append Arrow batches with the BigQuery Storage Write API.

### Authentication and Project Configuration

Workers and launchers authenticate with Google Application Default Credentials (ADC). Linking any `gcp-*` feature registers `GcpOptions` (`--project`, `--region`, `--temp_location`, `--staging_location`, `--service_account_email`).

- A table spec takes the form `project:dataset.table`, `project.dataset.table`, or `dataset.table` (which uses `--project`).
- `ReadOptions::with_billing_project(project)` sets the project that pays for Storage Read sessions and query jobs. When unset, the connector uses `--project`, then the project of the table. Public tables and query reads require `--project` or `with_billing_project`.
- Call `with_client` (and `with_query_clients` on reads or `with_table_client` on writes) to supply preconfigured Google Cloud clients. Workers rebuild the pipeline graph, so build custom clients deterministically on every machine.

### Reading Tables and Queries

Build `ReadOptions::new(table_spec)` for a table read or `ReadOptions::from_query(sql)` for a GoogleSQL query read. A query read runs a query job into a temporary table, reads it through Storage Read streams, and deletes the temporary table at completion.

| `ReadOptions` builder | Scope | Effect |
|---|---|---|
| `with_row_restriction(filter)` | Table only | Pushes down a GoogleSQL `WHERE` predicate |
| `with_selected_fields(fields)` | Table only | Reads only the listed columns (`Read<T>` defaults to the fields of `T`) |
| `with_snapshot_time(rfc3339)` | Table only | Reads the table as of an RFC 3339 timestamp (`"2024-05-01T12:30:00Z"`) |
| `with_sample_percentage(pct)` | Table only | Samples `(0.0, 100.0]` percent of storage blocks |
| `with_billing_project(project)` | Both | Project billed for read sessions and query jobs |
| `with_compression(ArrowCompression)` | Both | `None` (default), `Lz4Frame`, or `Zstd` on Arrow responses |
| `with_max_stream_count(count)` | Both | Upper bound on Storage Read streams (`0` lets BigQuery choose) |
| `with_preferred_min_stream_count(count)` | Both | Minimum stream hint (`0` lets BigQuery choose) |
| `with_query_priority(QueryPriority)` | Query only | `QueryPriority::Batch` (default) or `QueryPriority::Interactive` |
| `with_query_location(location)` | Query only | Execution location of the query job |
| `with_temp_dataset(dataset)` | Query only | Existing dataset for the temporary query destination table |
| `with_kms_key(kms_key)` | Query only | Cloud KMS key for the temporary query table |
| `with_use_legacy_sql(bool)` | Query only | Uses legacy SQL (`false` by default) |
| `with_flatten_results(bool)` | Query only | Flattens repeated legacy SQL fields (`true` by default) |

`ReadRows` looks up the table or query schema during expansion unless you pass `.with_schema(schema)` (for example from `options.schema()?`).

### Writing to Tables

Build `WriteOptions::new(table_spec)` and pass it to `Write::<T>::new` or `WriteRows::new`. The delivery guarantee (`Semantics`) and the boundedness of the input `PCollection` select the write execution mode:

- `Semantics::ExactlyOnce` (default) on bounded input writes each bundle to a pending stream without a shuffle, then commits all streams atomically.
- `Semantics::ExactlyOnce` on unbounded input shuffles Arrow batches across stream shards, appends with explicit offsets, and flushes committed rows on a timer.
- `Semantics::AtLeastOnce` appends directly to the table default stream with no shuffle; rows become visible immediately, and retried bundles can duplicate rows.

| `WriteOptions` builder | Default | Effect |
|---|---|---|
| `with_create_disposition(CreateDisposition)` | `CreateIfNeeded` | `CreateIfNeeded` creates a missing table from the input schema; `CreateNever` fails |
| `with_write_disposition(WriteDisposition)` | `WriteAppend` | `WriteAppend` appends rows; `WriteEmpty` fails when the table has rows |
| `with_semantics(Semantics)` | `ExactlyOnce` | `ExactlyOnce` or `AtLeastOnce` |
| `with_triggering_frequency(duration)` | `5s` | Flush interval for unbounded `ExactlyOnce` writes (invalid on bounded input) |
| `with_num_streams(count)` | `0` (auto) | Buffered stream shards for unbounded `ExactlyOnce` writes |
| `with_keep(fields)` / `with_drop(fields)` / `with_only(field)` | all fields | Selects a subset of fields or unwraps one nested row field (set at most one) |
| `with_ignore_unknown_values(bool)` | `false` | Drops selected fields that have no matching column in the destination table |
| `with_append_timeout(duration)` | `5m` | Per-append RPC timeout before reconnecting |
| `with_clustering_fields(fields)` | none | Clustering columns when the write creates the table |
| `with_kms_key(kms_key)` | none | Cloud KMS key when the write creates the table |

The Storage Write API cannot truncate an existing table. For `WRITE_TRUNCATE`, load jobs, CDC writes, or dynamic destinations, use `ManagedWrite::new(managed::BIGQUERY)`.

### Types, Dead-Letter Schemas, and Example

BigQuery `NUMERIC` and `BIGNUMERIC` map to `rust_decimal::Decimal`, `DATE` maps to `chrono::NaiveDate`, and `TIMESTAMP` maps to `chrono::DateTime<Utc>`. `DATETIME`, `TIME`, `GEOGRAPHY`, and `JSON` map to `String`. `STRUCT` and `ARRAY` map to nested `#[derive(BeamRow)]` structs (or `Row`) and `Vec<T>`.

- `bigquery::read_failure_schema()` has fields `(failed_row: STRING, error_message: STRING, stream: STRING, offset: INT64)`.
- `bigquery::write_failure_schema(&schema)` has fields `(failed_row: ROW<schema>, error_message: STRING)`.

```rust
use beam::io::gcp::bigquery::{
    ArrowCompression, CreateDisposition, Read, ReadOptions, Write, WriteDisposition, WriteOptions,
};
use beam::prelude::*;

#[derive(Clone, Debug, BeamRow)]
struct Observation {
    month: Option<i64>,
    tornado: Option<bool>,
}

#[derive(Clone, Debug, BeamRow)]
struct MonthlyCount {
    month: i64,
    tornado_count: i64,
}

fn build(p: &Pipeline) {
    let read_opts = ReadOptions::new("apache-beam-testing.samples.weather_stations")
        .with_billing_project("my-project")
        .with_row_restriction("month IS NOT NULL")
        .with_compression(ArrowCompression::Lz4Frame);

    let counts = p
        .apply("ReadWeather", Read::<Observation>::new(read_opts).with_failures())
        .failures_to("LogReadErrors", Inspect::new(|err: &Row| eprintln!("bad row: {err:?}")))
        .flat_map("FilterTornadoes", |obs: Observation| {
            obs.tornado.filter(|&is_tornado| is_tornado).and(obs.month)
        })
        .count_per_element("CountPerMonth")
        .map("ToRow", |(month, tornado_count)| MonthlyCount { month, tornado_count });

    let write_opts = WriteOptions::new("my-project:analytics.monthly_tornadoes")
        .with_create_disposition(CreateDisposition::CreateIfNeeded)
        .with_write_disposition(WriteDisposition::WriteAppend);

    let written = counts.apply("WriteCounts", Write::<MonthlyCount>::new(write_opts));
    written
        .failures
        .inspect("LogWriteErrors", |err: &Row| eprintln!("rejected row: {err:?}"));
}
```

## Cloud Bigtable (`beam::io::gcp::bigtable`)

Enable the `gcp-bigtable` feature. `Read` and `ReadRows` sample tablet boundaries with `SampleRowKeys` and run splittable `ReadRows` scans across workers. `Write` and `WriteRows` batch mutations into `MutateRows` streams with partial-failure retries and `RateLimitInfo` flow control.

### Authentication, Endpoints, and Targets

Workers authenticate with Google Application Default Credentials over pooled TLS gRPC channels. Call `.with_endpoint("http://localhost:8086")` on `ReadOptions` or `WriteOptions` to target a local Bigtable emulator, or `.with_client(client)` to share an `Arc<BigtableClient>`.

- Reads target a table (`ReadOptions::from_table(project, instance, table)`), a materialized view (`from_materialized_view(project, instance, view)`), or an authorized view (`from_authorized_view(project, instance, table, view)`).
- Writes target a table (`WriteOptions::from_table(project, instance, table)`) or an authorized view (`from_authorized_view(project, instance, table, view)`). Create the destination table and column families before running the job.
- Call `.with_app_profile_id("my-profile")` on `ReadOptions` or `WriteOptions` to select a routing profile or a Data Boost profile.

### Reading Columns and Rows

`ReadOptions` selects one of two output schemas with `.with_shape(shape)`:

- `BigtableReadShape::Flattened` (default, `flattened_row_schema()`): emits one element per column with `(key: BYTES, family_name: STRING, column_qualifier: BYTES, cells: ARRAY<ROW<value: BYTES, timestamp_micros: INT64>>)`. Convert a `Row` with `BigtableColumn::try_from(row)` and read `column.latest_value()`.
- `BigtableReadShape::Nested` (`nested_row_schema()`): emits one element per Bigtable row with `(key: BYTES, column_families: MAP<STRING, MAP<STRING, ARRAY<cell>>>)`.

Import `bigtable::RowFilterExt` to build server-side `bigtable::proto::RowFilter` predicates. Constructors include `chain`, `interleave`, `condition`, `cells_per_column_limit`, `family_name_regex`, `column_qualifier_regex`, `row_key_regex`, `timestamp_range`, `strip_values`, `value_bitmask`, and `column_value_bitmask`:

| `ReadOptions` builder | Default | Effect |
|---|---|---|
| `with_app_profile_id(id)` | default profile | Sets the Bigtable app profile |
| `with_shape(BigtableReadShape)` | `Flattened` | Emits one element per column (`Flattened`) or per row (`Nested`) |
| `with_key_range(start_key, end_key)` | full table | Restricts the scan to `[start_key, end_key)` (empty `end_key` scans to the end) |
| `with_row_filter(&filter)` | none | Pushes a `proto::RowFilter` to the Bigtable server |
| `with_row_filter_bytes(bytes)` | none | Pushes raw protobuf-encoded `google.bigtable.v2.RowFilter` bytes |
| `with_max_initial_bundles(n)` | `4096` | Maximum initial key-range bundles from `SampleRowKeys` |
| `with_sub_split_tablets(bool)` | `true` | Splits coalesced super-ranges at interior tablet boundaries |
| `with_max_silent_scan_duration(duration)` | `60s` | Checkpoints silent sparse scans after this duration |
| `with_attempt_timeout(duration)` | `60s` | Per-attempt timeout for `SampleRowKeys` and `ReadRows` |
| `with_endpoint(endpoint)` | production | Overrides the Bigtable gRPC endpoint |

### Writing Mutations

`Write<T>` and `WriteRows` accept either of two input schemas:

- **Flat mutations (`mutation_schema()`):** one mutation per element with `key: BYTES` and `type: STRING` (`"SetCell"`, `"DeleteFromColumn"`, `"DeleteFromFamily"`, `"DeleteFromRow"`). Nullable fields hold `value`, `column_qualifier`, `family_name`, `timestamp_micros`, `start_timestamp_micros`, and `end_timestamp_micros`. Build `BigtableMutation` values (`BigtableMutation::set_cell` or enum variants), convert them with `Row::from(mutation)`, and attach `.with_row_schema(&mutation_schema())`.
- **Batched mutations (`batched_mutation_schema()`):** `(key: BYTES, mutations: ARRAY<MAP<STRING, BYTES>>)` for grouping multiple mutations on the same row key into one atomic `MutateRows` entry.

A `SetCell` mutation with `timestamp_micros: None` (or `-1`) asks Bigtable to assign a server timestamp. If a stream retries after an ambiguous network failure, a server-assigned timestamp can write a second cell version. Set an explicit `timestamp_micros` when retries must overwrite the same cell version.

| `WriteOptions` builder | Default | Effect |
|---|---|---|
| `with_app_profile_id(id)` | default profile | Sets the Bigtable app profile |
| `with_max_elements_per_batch(n)` | `100` | Maximum row entries per `MutateRows` call (`1..=100_000`) |
| `with_max_bytes_per_batch(bytes)` | `2 MiB` | Maximum request payload bytes per `MutateRows` call |
| `with_max_in_flight_requests(n)` | `10` | Concurrent `MutateRows` calls per bundle |
| `with_flow_control(bool)` | `true` | Applies `RateLimitInfo` server backoff |
| `with_attempt_timeout(duration)` | `60s` | Per-attempt timeout for `MutateRows` |
| `with_endpoint(endpoint)` | production | Overrides the Bigtable gRPC endpoint |

### Dead-Letter Schemas and Example

- `bigtable::read_failure_schema()` has fields `(target: STRING, row_key: BYTES, error_message: STRING)`.
- `bigtable::write_failure_schema(&input_schema)` has fields `(failed_row: ROW<input_schema>, error_message: STRING)`.

```rust
use beam::io::gcp::bigtable::proto::RowFilter;
use beam::io::gcp::bigtable::{
    BigtableColumn, BigtableMutation, ReadOptions, ReadRows, RowFilterExt, WriteOptions, WriteRows,
    mutation_schema,
};
use beam::prelude::*;

fn build_write(counts: PCollection<(String, i64)>) {
    let mutations = counts
        .map("ToMutation", |(word, count)| {
            Row::from(BigtableMutation::SetCell {
                key: word.into_bytes(),
                family_name: "counts".to_string(),
                column_qualifier: b"total".to_vec(),
                value: count.to_string().into_bytes(),
                timestamp_micros: Some(1_700_000_000_000_000),
            })
        })
        .with_row_schema(&mutation_schema());

    let written = mutations.apply(
        "WriteBigtable",
        WriteRows::new(WriteOptions::from_table("my-project", "my-instance", "wordcount")),
    );
    written
        .failures
        .inspect("LogRejected", |err: &Row| eprintln!("rejected mutation: {err:?}"));
}

fn build_read(p: &Pipeline) -> PCollection<(String, String)> {
    let filter = RowFilter::chain([
        RowFilter::family_name_regex("^counts$"),
        RowFilter::cells_per_column_limit(1),
    ]);
    let options = ReadOptions::from_table("my-project", "my-instance", "wordcount")
        .with_app_profile_id("data-boost")
        .with_row_filter(&filter);

    p.apply("ReadBigtable", ReadRows::new(options))
        .flat_map("DecodeColumn", |row: Row| {
            let col = BigtableColumn::try_from(row).ok()?;
            let key = String::from_utf8(col.key).ok()?;
            let val = String::from_utf8(col.latest_value()?.to_vec()).ok()?;
            Some((key, val))
        })
}
```

## Apache Kafka (`beam::io::kafka`)

Enable the `kafka` feature (which also enables `avro`, `json`, and `proto`). The connector links a static `librdkafka` and vendored OpenSSL, reads topic partitions with a splittable `DoFn`, and writes batches with shared `librdkafka` producers.

### Client Properties and Authentication

Pass `librdkafka` client properties to `ReadOptions::with_consumer_properties` and `WriteOptions::with_producer_properties`. Set `security.protocol`, `sasl.mechanism`, `sasl.username`, and `sasl.password` for SASL authentication; `librdkafka` does not use `sasl.jaas.config`.

For Google Cloud Managed Service for Apache Kafka, call `.with_google_auth()` on `ReadOptions` or `WriteOptions`. That setting configures `security.protocol=SASL_SSL` and `sasl.mechanism=OAUTHBEARER` and refreshes Google ADC tokens automatically.

### Codecs, Typed Structs, and Record Metadata

`Read::new(options)` and `Write::new(options)` use `codec::Raw` (`Vec<u8>`) by default. Pass another codec from `beam::io::kafka::codec` to `.with_decoder(...)` or `.with_encoder(...)`:

| Codec (`beam::io::kafka::codec`) | Read constructor | Write constructor | Element type |
|---|---|---|---|
| `Raw` (default) | `Raw` | `Raw` | `Vec<u8>` |
| `Utf8` | `Utf8` | `Utf8` | `String` |
| `Json` | `Json::schema(json_schema)?` | `Json::new()` | `Row` |
| `Avro` | `Avro::schema(avro_json)?` or `Avro::latest(reg, subj)?` | `Avro::new()`, `Avro::schema(avro_json)?`, or `Avro::latest(reg, subj)?` | `Row` |
| `Proto` | `Proto::new(fds, msg)?` or `Proto::from_file(path, msg)?` | `Proto::new(fds, msg)?` or `Proto::from_file(path, msg)?` | `Row` |
| `BeamRow` | `BeamRow::schema(schema)` | `BeamRow::new()` | `Row` (`beam:coder:row:v1`) |
| `Payload` | `Payload::raw()` or `Payload::string()` | `Payload::raw()` or `Payload::string()` | `Row` (`payload` field) |
| `Typed<C, T>` | `Typed::new(row_codec)` | `Typed::new(row_codec)` | `T: BeamRow` |
| `WithMetadata<V, K>` | `WithMetadata::new(val_codec).with_key(key_codec)` | `WithMetadata::new(val_codec).with_key(key_codec)` | `KafkaRecord<KE, VE>` |

- **Typed structs (`Typed`):** configure a `Row` codec (`Json`, `Avro`, `Proto`, `BeamRow`, `Payload`) first. Wrap it in `Typed::new(codec)` to read or write a `#[derive(BeamRow)]` type `T`.
- **Confluent Schema Registry (`SchemaRegistry`):** build `SchemaRegistry::new("http://registry:8081")` with optional `.with_basic_auth(user, pass)` or `.with_google_auth()`. Read or write the latest subject schema with `Avro::latest(registry, "events-value")?`. You can also read with `Avro::schema(avro_json)?.with_registry(registry)` and write with `Avro::new().with_registry(registry).with_subject("events-value")`.
- **Keys, headers, and timestamps (`WithMetadata`):** wrap value and key codecs whose `Element` and `Output` types match (`Raw`, `Utf8`, or `Typed::new(row_codec)`). On write, build `KafkaRecord::of_value(val).with_key(key).with_header("source", Some(b"rust".to_vec())).with_timestamp(ts)`. Set `value: None` to send a tombstone.

### Reading Topics

`Read` is unbounded by default and sets each element timestamp to the Kafka record timestamp. Set `"group.id"` in `with_consumer_properties` to start from committed offsets and commit bundle offsets in a bundle finalizer.

| `ReadOptions` builder | Default | Effect |
|---|---|---|
| `with_start_offset(StartOffset)` | `Latest` | `Earliest`, `Latest`, or `Committed` when no group offset exists |
| `with_consumer_properties(props)` | consumer defaults | Passes `librdkafka` properties; `"group.id"` enables offset commits |
| `with_google_auth()` | off | Authenticates with Google ADC (`SASL_SSL` / `OAUTHBEARER`) |
| `with_max_read_time(duration)` | unbounded | Stops after `duration`, turning the read into a bounded source |
| `with_max_read_time_per_call(duration)` | `5s` | Maximum poll slice before checkpointing the bundle |
| `with_max_idle_per_call(duration)` | `1s` | Maximum idle wait before ending a poll slice and advancing the watermark |
| `with_consumer_pool(ConsumerPool)` | `OnePerPartition` | `OnePerPartition` (up to 16) or `ConsumerPool::Size(PoolSize::try_from(n)?)` |
| `with_commit_expiry(duration)` | runner default | Lifetime of pending bundle-finalizer offset commits |
| `with_reshuffle_partitions(bool)` | `false` | Adds a `Reshuffle` of partition descriptors on bounded reads |
| `with_redistribute(Redistribute)` | `Off` | `Off`, `Random`, or `Keys(NonZeroU32)` to spread records across workers |

### Writing Topics

`Write` sends records at least once with `enable.idempotence=true` and `acks=all` by default, waiting for broker acknowledgments at the end of each bundle.

| `WriteOptions` builder | Default | Effect |
|---|---|---|
| `with_producer_properties(props)` | `enable.idempotence=true`, `acks=all` | Passes `librdkafka` producer properties |
| `with_compression(Compression)` | `None` | `None`, `Gzip`, `Snappy`, `Lz4`, or `Zstd` record-batch compression |
| `with_google_auth()` | off | Authenticates with Google ADC (`SASL_SSL` / `OAUTHBEARER`) |

For transactional writes, multi-topic pattern subscriptions, or dynamic destination topics, use `ManagedRead::new(managed::KAFKA)` or `ManagedWrite::new(managed::KAFKA)`.

### Dead-Letter Schemas and Example

- `kafka::read_failure_schema()` has fields `(failed_row: BYTES, error_message: STRING)`, where `failed_row` holds the raw record value bytes.
- `kafka::write_failure_schema(failed_type)` has fields `(failed_row: <failed_type>, error_message: STRING)`, matching the input type of the encoder (`ROW<T>` for `Typed<C, T>`).

```rust
use beam::io::kafka::codec::{Avro, KafkaRecord, SchemaRegistry, Typed, Utf8, WithMetadata};
use beam::io::kafka::{Compression, Read, ReadOptions, StartOffset, Write, WriteOptions};
use beam::prelude::*;

#[derive(Clone, Debug, BeamRow)]
struct ClickEvent {
    user_id: String,
    url: String,
}

fn build(p: &Pipeline) -> beam::Result {
    let registry = SchemaRegistry::new("http://schema-registry:8081");
    let read_codec = WithMetadata::new(Typed::<Avro, ClickEvent>::new(Avro::latest(
        registry.clone(),
        "clicks-value",
    )?))
    .with_key(Utf8);

    let read_opts = ReadOptions::new("broker-1:9092,broker-2:9092", "clicks")
        .with_start_offset(StartOffset::Earliest)
        .with_consumer_properties([("group.id".to_string(), "click-router".to_string())]);

    let records = p
        .try_apply("ReadClicks", Read::new(read_opts).with_decoder(read_codec).with_failures())?
        .failures_to("LogBadRecords", Inspect::new(|err: &Row| eprintln!("decode error: {err:?}")));

    let outgoing = records.flat_map("KeepNonEmpty", |rec: KafkaRecord<String, ClickEvent>| {
        let event = rec.value?;
        let key = rec.key.unwrap_or_else(|| event.user_id.clone());
        Some(KafkaRecord::of_value(event).with_key(key))
    });

    let write_codec = WithMetadata::new(Typed::<Avro, ClickEvent>::new(
        Avro::new().with_registry(registry).with_subject("valid-clicks-value"),
    ))
    .with_key(Utf8);

    let write_opts = WriteOptions::new("broker-1:9092,broker-2:9092", "valid-clicks")
        .with_compression(Compression::Zstd);

    let written = outgoing.try_apply("WriteClicks", Write::new(write_opts).with_encoder(write_codec))?;
    written
        .failures
        .inspect("LogWriteErrors", |err: &Row| eprintln!("kafka reject: {err:?}"));
    Ok(())
}
```

## Apache Iceberg (`beam::io::iceberg`)

Enable the `iceberg` feature (and `gcp-gcs` when table files live on `gs://` without vended credentials). The connector plans batch snapshot scans and commits partitioned Parquet data files against an Iceberg REST catalog.

### Catalog Configuration and Authentication

Build a REST catalog handle with `Catalog::rest(uri)` and pass it to `ReadOptions::new(catalog, "namespace.table")` or `WriteOptions::new(catalog, "namespace.table")`:

| `Catalog` builder | Effect |
|---|---|
| `Catalog::rest(uri)` | Connects to the Iceberg REST catalog endpoint |
| `with_warehouse(warehouse)` | Sets the catalog warehouse (`"bl://projects/my-proj/catalogs/main"` or `"gs://bucket/warehouse"`) |
| `with_google_auth()` | Attaches a fresh Google ADC bearer token to every catalog request (required for BigLake) |
| `with_token(token)` | Authenticates with a static bearer token |
| `with_header(name, value)` | Sends an HTTP header on catalog calls (such as `"x-goog-user-project"`) |
| `with_property(key, value)` | Passes an additional catalog client property |
| `with_vended_credentials()` | Uses short-lived per-table storage credentials vended by the catalog |
| `with_metrics_reporting()` | Sends an Iceberg `commit-report` to the table metrics endpoint after each commit |

### Reading Snapshots

`Read<T>` and `ReadRows` plan the data files of one snapshot, shuffle the scan tasks across workers, and split Parquet files at row-group byte boundaries.

| `ReadOptions` builder | Default | Effect |
|---|---|---|
| `with_keep(columns)` | fields of `T` or all columns | Reads only the listed columns |
| `with_drop(columns)` | none | Reads all columns except the listed columns |
| `with_filter(predicate)` | none | Filters rows and prunes manifests and row groups with an `iceberg::expr::Predicate` |
| `with_snapshot_id(id)` | current snapshot | Reads the snapshot with the given ID |
| `with_as_of_timestamp_millis(ms)` | current snapshot | Reads the latest snapshot at or before `ms` (epoch milliseconds) |
| `with_batch_size(rows)` | `8192` | Number of rows per decoded Arrow batch |

`ReadRows` fetches the table schema from the catalog during expansion unless you pass `.with_schema(schema)`. Add `iceberg = "0.11"` to `Cargo.toml` to build a `Predicate` for `with_filter`. Build expressions with `iceberg::expr::Reference` and `iceberg::spec::Datum`.

### Writing Data Files and Snapshots

`Write<T>` and `WriteRows` create missing namespaces and tables, encode Arrow batches, and write Parquet data files. Each commit records a deterministic ID in the snapshot summary, so retried panes do not commit duplicate snapshots. Bounded inputs commit one snapshot at job completion. Unbounded inputs commit snapshots on a timer.

| `WriteOptions` builder | Default | Effect |
|---|---|---|
| `with_partition_fields(fields)` | unpartitioned | Sets `PartitionField::new(col, PartitionTransform)` when creating a new table |
| `with_table_property(key, value)` | none | Sets an Iceberg table property when creating a new table |
| `with_triggering_frequency(duration)` | `60s` | Snapshot commit period on unbounded input (invalid on bounded input) |
| `with_target_file_bytes(bytes)` | table property or `512 MiB` | Target data file size (each shard group caps at `128 MiB` Arrow IPC bytes) |
| `with_num_shards(shards)` | auto-sharded | Caps concurrent file writers per table |
| `with_keep(fields)` / `with_drop(fields)` / `with_only(field)` | all fields | Projects input fields before creating and writing the table |

`PartitionTransform` supports `Identity`, `Year`, `Month`, `Day`, `Hour`, `Bucket(u32)`, and `Truncate(u32)`. Iceberg writes accept `bool`, `i32`, `i64`, `f32`, `f64`, `String`, `bytes::Bytes`, `#[beam(bytes)] Vec<u8>`, `chrono::DateTime<Utc>`, `beam::Instant`, `chrono::NaiveDate`, nested structs, `Vec<T>`, `HashMap<K, V>`, and `Option<T>`. Native Iceberg writes reject `i8`, `i16`, and `Decimal` fields.

For Hadoop or Hive catalogs, incremental streaming reads, upserts, or dynamic table routing, use `ManagedRead::new(managed::ICEBERG)` and `ManagedWrite::new(managed::ICEBERG)` ([example](../examples/iceberg_wordcount/src/lib.rs)).

### Outputs, Dead-Letter Schemas, and Example

`Write<T>` and `WriteRows` return `WriteResult` with two `PCollection` fields:

- `written.failures`: `PCollection<Row>` with `iceberg::write_failure_schema(&schema)` (`(failed_row: ROW<schema>, error_message: STRING)`).
- `written.snapshots`: `PCollection<SnapshotInfo>` with `sequence_number`, `snapshot_id`, `parent_id`, `timestamp_millis`, `operation`, `summary`, `manifest_list_location`, `schema_id`, and `table_identifier_string`.
- `iceberg::read_failure_schema()` has fields `(failed_row: STRING, error_message: STRING, data_file: STRING)`.

```rust
use beam::io::iceberg::{
    Catalog, PartitionField, PartitionTransform, Read, ReadOptions, SnapshotInfo, Write,
    WriteOptions,
};
use beam::prelude::*;

#[derive(Clone, Debug, BeamRow)]
struct MetricEvent {
    id: i64,
    device: String,
    value: f64,
}

fn build(p: &Pipeline) {
    let catalog = Catalog::rest("https://biglake.googleapis.com/iceberg/v1/restcatalog")
        .with_warehouse("bl://projects/my-project/catalogs/telemetry")
        .with_google_auth()
        .with_header("x-goog-user-project", "my-project")
        .with_vended_credentials();

    let events = p.apply(
        "ReadSnapshot",
        Read::<MetricEvent>::new(ReadOptions::new(catalog.clone(), "raw.metrics")),
    );

    let write_opts = WriteOptions::new(catalog, "curated.metrics")
        .with_partition_fields([PartitionField::new("device", PartitionTransform::Identity)]);

    let written = events.apply("WriteCurated", Write::<MetricEvent>::new(write_opts));
    written
        .failures
        .inspect("LogWriteErrors", |err: &Row| eprintln!("rejected row: {err:?}"));
    written.snapshots.inspect("LogCommits", |snap: &SnapshotInfo| {
        println!("committed snapshot {} (seq {})", snap.snapshot_id, snap.sequence_number);
    });
}
```

## File Formats and Managed I/O

File-based formats and cross-language connectors complement the four service connectors above:

| Connector / Format | `apache-beam` feature | Path | Documentation |
|---|---|---|---|
| Text files (`TextIO`) | `io-file` (default) | `beam::prelude::textio` (`beam::io::file::textio`) | [Programming Guide: File I/O](programming-guide.md#file-io-beamio) |
| Whole-file matching & custom sinks | `io-file` (default) | `beam::io::file::{fileio, WriteFiles}` | [Programming Guide: Whole-file reads](programming-guide.md#whole-file-reads) |
| Cloud Storage (`gs://`) | `gcp-gcs` | `beam::io::gcp::gcs` (registers `"gs"` scheme) | [Programming Guide: File I/O](programming-guide.md#file-io-beamio) |
| Apache Parquet (`ParquetIO`) | `parquet` | `beam::io::parquet::parquetio` | [Programming Guide: Parquet and Avro](programming-guide.md#parquet-and-avro) |
| Apache Avro (`AvroIO` & `datum`) | `avro` | `beam::io::avro::{avroio, datum}` | [Programming Guide: Parquet and Avro](programming-guide.md#parquet-and-avro) |
| JSON Schema & JSON rows (`JsonIO`) | `json` | `beam::io::json::{JsonReader, write_json, schema_from_json_schema}` | [`apache-beam-io-json`](../beam/io/json/src/lib.rs) |
| TFRecord (`TfRecordIO`) | `tfrecord` | `beam::io::tfrecord::tfrecordio` | [Programming Guide: TFRecord](programming-guide.md#tfrecord) |
| Pub/Sub (cross-language) | `gcp-pubsub` | `beam::io::gcp::pubsub` | [Programming Guide: Cross-language](programming-guide.md#cross-language) |
| Managed I/O (`ICEBERG`, `KAFKA`, `BIGQUERY`, JDBC, `DELTA`) | `managed` | `beam::io::managed::{ManagedRead, ManagedWrite}` | [Programming Guide: Managed I/O](programming-guide.md#managed-io-beamiomanaged) and [rust-native-io-providers.md](rust-native-io-providers.md) |
