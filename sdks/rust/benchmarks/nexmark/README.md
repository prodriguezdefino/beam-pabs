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

# Apache Beam Rust Nexmark Suite

`apache-beam-nexmark` implements the Nexmark online-auction benchmark and correctness suite for
the Apache Beam Rust SDK.

## Crate Layout

- `src/config`: query identifiers, suite presets (`DEFAULT`, `SMOKE`, `STRESS`, `FULL_THROTTLE`),
  and rate curves (`SINE`, `SQUARE`).
- `src/model`: `Person`, `Auction`, `Bid`, `Event`, their schema row forms, query output types,
  byte-size estimates, and binary coders.
- `src/generator`: deterministic 48-bit LCG event synthesis matching `java.util.Random`.
- `src/source`: bounded and unbounded splittable DoFn sources over event ID ranges.
- `src/queries`: 17 Nexmark query `PTransform` implementations plus the stateful `WinningBids`
  join.
- `src/models`: pure in-memory reference models for deterministic correctness checks.
- `src/correctness.rs`: in-pipeline checks that compare query output with a model.
- `src/load.rs`: the `CpuDelay` and `DiskBusy` load steps that run before a query.
- `src/launcher` and `src/main.rs`: CLI flag parsing, pipeline assembly, perf monitors,
  and summary reporting.

## Queries

| ID | Name | Summary |
|---:|---|---|
| 0 | `PASSTHROUGH` | Encodes and decodes every `Event` with `EventCoder`. |
| 1 | `CURRENCY_CONVERSION` | Converts `Bid` prices from USD to EUR (`* 89 / 100`). |
| 2 | `SELECTION` | Selects bids where `auction % auction_skip == 0`. |
| 3 | `LOCAL_ITEM_SUGGESTION` | Joins sellers in `OR`, `ID`, or `CA` with category `10` auctions using state and timers. |
| 4 | `AVERAGE_PRICE_FOR_CATEGORY` | Computes sliding-window mean winning prices per category. |
| 5 | `HOT_ITEMS` | Finds auctions with the maximum bid count in each sliding window. |
| 6 | `AVERAGE_SELLING_PRICE_BY_SELLER` | Computes the moving average of the last 10 winning prices per seller. |
| 7 | `HIGHEST_BID` | Selects the highest-priced bids in each fixed window using a singleton side input. |
| 8 | `MONITOR_NEW_USERS` | Joins new users and auctions opened in the same fixed window. |
| 9 | `WINNING_BIDS` | Emits the highest valid bid at auction expiration. |
| 10 | `LOG_TO_SHARDED_FILES` | Archives fixed-window panes into sharded `LOG-*` files and `INDEX-*` manifests. |
| 11 | `USER_SESSIONS` | Counts bids per user in session windows with an element-count trigger. |
| 12 | `PROCESSING_TIME_WINDOWS` | Counts bids per user in global-window processing-time panes. |
| 13 | `BOUNDED_SIDE_INPUT_JOIN` | Enriches `Bid` events from a bounded `SideMap`. |
| 14 | `SESSION_SIDE_INPUT_JOIN` | Enriches `Bid` events in session windows from a bounded `SideMap`. |
| 15 | `PORTABILITY_BATCH` | Keys events into shard buckets, groups by key, and burns configurable CPU cycles (`Query13`). |
| 16 | `RESHUFFLE` | Passes events through one `Reshuffle` over `--num_key_buckets` shard keys (`Query14`). |

Note: Prism panics on `Sessions` combined with a non-default trigger ([apache/beam#31438](https://github.com/apache/beam/issues/31438)).
Suite runs on Prism skip Query 11 (`USER_SESSIONS`) and mark it `SKIPPED (apache/beam#31438)`.

## Running Tests and Pipelines

Run all commands from the repository root with Gradle:

```bash
# Run all unit, batch Prism, and streaming Prism tests for Nexmark.
./gradlew :sdks:rust:test -Ppkg=apache-beam-nexmark

# Run the SMOKE suite locally on Prism (batch or streaming).
./gradlew :sdks:rust:nexmarkPrism -Psuite=SMOKE -PextraArgs="--num_events=500"
./gradlew :sdks:rust:nexmarkPrism -Pquery=5 -Pstreaming -PextraArgs="--num_events=1000"

# Run a query or suite on Google Cloud Dataflow.
./gradlew :sdks:rust:nexmarkDataflow -Pquery=0 -PextraArgs="--num_events=1000000"
./gradlew :sdks:rust:nexmarkDataflow -Psuite=SMOKE -Pstreaming

# Run multi-round Dataflow benchmarks and compare against a baseline ref.
./gradlew :sdks:rust:nexmarkBenchmark -Pquery=0 -PnumEvents=1000000 -Pruns=3
./gradlew :sdks:rust:nexmarkBenchmark -Pquery=5 -Pstreaming -Pbaseline=origin/master
```

## Main CLI Flags

Pass Nexmark flags directly or through `-PextraArgs="..."`:

- `--suite=<DEFAULT|SMOKE|STRESS|FULL_THROTTLE>` and `--query=<0..16|NAME>`
- `--sink_type=<COUNT_ONLY|DEVNULL|TEXT|PUBSUB|KAFKA|AVRO>` and `--output_path=<dir-or-gs-uri>`
- `--num_events=<n>`, `--num_event_generators=<n>`, `--is_rate_limited`, `--first_event_rate=<n>`,
  `--next_event_rate=<n>`, `--rate_shape=<SINE|SQUARE>`, `--rate_unit=<PER_SECOND|PER_MINUTE>`
- `--window_size_sec=<n>`, `--window_period_sec=<n>`, `--session_gap_sec=<n>`, `--fanout=<n>`
- `--query10_max_workers=<n>`, `--max_log_events=<n>`, `--side_input_row_count=<n>`,
  `--num_key_buckets=<n>`, `--pardo_cpu_factor=<f>`
- `--assert_correctness`: in batch mode, compare the output of each query that has a reference
  model (0 to 9 and the two side-input joins) with the model. A mismatch fails the job and
  marks the row `FAILED`. Other queries and streaming runs log that the check is skipped.
- `--log_events`, `--log_results`: log each event or result at the `info` level.
- `--stream_timeout=<sec>` (default 240): longest streaming run after its first event.
- `--cancel_streaming_job_after_finish`: cancel a finished streaming job instead of draining it.
- `--source_type=<DIRECT|PUBSUB|KAFKA|AVRO>`, `--input_path=<prefix>` ([Avro files](#avro-files)),
  `--coder_strategy=<HAND|ROW>` ([coder strategies](#coder-strategies)), and the flags of
  [Pub/Sub and Kafka](#pubsub-and-kafka).
- `--cpu_delay_ms=<n>`, `--disk_busy_bytes=<n>`: [load steps](#load-steps) before each query.
- `--running_time_minutes=<n>`, `--preload_seconds=<n>`: [run-time limit](#run-time-limit).
- `--side_input_type=<DIRECT|CSV>`, `--side_input_url=<prefix>`, `--side_input_num_shards=<n>`:
  [side-input table](#side-input-table) of the two side-input joins.
- `--skip_queries=<list>`, `--nexmark_parallel=<n>`: [suite runs](#suite-runs).
- `--export_summary_to_bigquery`, `--bigquery_table=<project:dataset.table>`:
  [BigQuery summary](#bigquery-summary).

## Load Steps

As in the Java launcher, two optional steps run between `MonitorEvents` and the query:

- `--cpu_delay_ms=<n>` (`CpuDelay`): each event keeps one core busy with hash searches for `n`
  milliseconds of wall time.
- `--disk_busy_bytes=<n>` (`DiskBusy`): all events get one key, and each event writes `n`
  bytes to a value state, in writes of at most 16 MiB.

The default of both is 0, which leaves the step out of the graph.

## Run-Time Limit

`--running_time_minutes=<n>` cancels a job that still runs `n` minutes after its submission,
less `--preload_seconds`. This is how the Java launcher uses the two flags. The limit applies
to batch and streaming jobs and comes before the other streaming stop rules. The row shows
`OK (running time)` with the figures of the final metrics. `--preload_seconds` does not change
event time.

## Side-Input Table

The two side-input joins look up `bidder % side_input_row_count` in a table of `(id, label)`
rows, where each label is the decimal id.

- `--side_input_type=DIRECT` (default) builds the table in the query pipeline.
- `--side_input_type=CSV` reads it from the text files `<side_input_url>*`, one `id,label`
  line each. Before the first query, the launcher runs a batch job that writes the table
  into `--side_input_num_shards` files (default 3). The table has the rows of the largest join
  of the suite, so it covers each join. The launcher does not delete the files.

Java has these fields in its configuration, but not as launcher flags. The flags are a Rust
extension.

## Suite Runs

- `--skip_queries=3,HOT_ITEMS` leaves out the listed queries, by number or name. Their rows
  show `SKIPPED (--skip_queries)`.
- `--nexmark_parallel=<n>` (default 1) runs up to `n` queries at the same time. The table keeps
  the suite order. Each job still gets the query after its job name.

## BigQuery Summary

With `--export_summary_to_bigquery`, the launcher runs one more batch job after the suite. The
job appends one row for each query to `--bigquery_table` through the cross-language BigQuery
write (`CREATE_IF_NEEDED`, `WRITE_APPEND`) and honours `--expansion_service`.

| Column | Value |
|---|---|
| `timestamp` | Start of the suite run (`TIMESTAMP`) |
| `runId` | The job name, or `nexmark`, then the start time in epoch milliseconds |
| `queryNumber`, `queryName` | The query |
| `mode`, `runner` | `batch` or `streaming`, and `--runner` |
| `status`, `reason` | The status of the summary table, and the failure or skip reason |
| `config` | The resolved configuration of the query as JSON |
| `runtimeSec`, `numEvents`, `eventsPerSec`, `eventBytesPerSec`, `numResults`, `resultsPerSec`, `resultBytesPerSec`, `startupDelaySec`, `processingDelaySec`, `timeDilation` | The figures, empty for a query that did not run |

Skipped and failed queries get a row too, so failures stay in the table history. Java has
removed its BigQuery export. The columns `timestamp`, `runtimeSec`, `eventsPerSec` and
`numResults` match its last version, which wrote one table for each query. The other figures
use the JSON names of Java's `NexmarkPerf`.

## Pub/Sub and Kafka

With `--source_type=PUBSUB` or `KAFKA`, events travel through a topic. `--pubsub_mode` selects
the side of the exchange, for Kafka too:

| Mode | Jobs |
|---|---|
| `COMBINED` (default) | A publisher job (`<job_name>-publisher`) writes generated events to the topic. The query job reads them. The launcher cancels the publisher when the query run ends. |
| `PUBLISH_ONLY` | Only the publisher job. The row shows the event figures. |
| `SUBSCRIBE_ONLY` | Only the query job, on events that another process publishes. |

| Flag | Used by |
|---|---|
| `--pubsub_topic=projects/<p>/topics/<t>` | Pub/Sub publisher |
| `--pubsub_subscription=projects/<p>/subscriptions/<s>` | Pub/Sub query |
| `--pubsub_results_topic=projects/<p>/topics/<t>` | `--sink_type=PUBSUB` |
| `--use_pubsub_publish_time` | Event time from the publish time, not the `timestamp` attribute |
| `--kafka_topic=<t>`, `--bootstrap_servers=<host:port,...>` | Kafka publisher and query |
| `--kafka_results_topic=<t>` | `--sink_type=KAFKA` |
| `--expansion_service=<host:port>` | Instead of the default Java expansion service |

- A query job that reads a topic needs `--streaming`. `PUBLISH_ONLY` also runs in batch.
- Events are the bytes of the Java Nexmark `Event.CODER`. Results are their text form, UTF-8.
- The Pub/Sub publisher sets the `id` attribute and, unless `--use_pubsub_publish_time`, the
  `timestamp` attribute, so event time equals generator time. With the publish time, also set
  `--use_wallclock_event_time`.
- The transforms are cross-language. The graph build needs Java and the expansion service.
- The launcher does not create or delete topics or subscriptions. Create them before the run.
  A missing name stops the launcher before it submits a job.

Differences from the Java launcher:

- Results go to `--pubsub_results_topic`. Java writes them to the event topic, where a
  `COMBINED` query would read them back as events.
- A message that is not an event increments the `nexmark/decode_errors` counter and is
  dropped. Java fails the bundle. A `fatal` counter would make the streaming monitor cancel
  the job.
- The Kafka transforms carry no record timestamps, so each event read from Kafka gets its own
  `date_time` as timestamp. Run Kafka with `--use_wallclock_event_time`, so the read watermark
  and the event times agree. The reader starts at the earliest offset: use a new or empty
  topic for each run.

Dataflow recipe for query 5 through Pub/Sub:

```bash
gcloud pubsub topics create nexmark-events --project=my-project
gcloud pubsub subscriptions create nexmark-events-sub --topic=nexmark-events --project=my-project

# One launch runs both jobs.
./gradlew :sdks:rust:nexmarkDataflow -Pquery=5 -Pstreaming -PextraArgs="--num_events=1000000 \
  --source_type=PUBSUB --pubsub_mode=COMBINED \
  --pubsub_topic=projects/my-project/topics/nexmark-events \
  --pubsub_subscription=projects/my-project/subscriptions/nexmark-events-sub"

# Or two launches: the publisher, then the query.
./gradlew :sdks:rust:nexmarkDataflow -Pquery=5 -Pstreaming -PextraArgs="--num_events=1000000 \
  --source_type=PUBSUB --pubsub_mode=PUBLISH_ONLY \
  --pubsub_topic=projects/my-project/topics/nexmark-events"
./gradlew :sdks:rust:nexmarkDataflow -Pquery=5 -Pstreaming -PextraArgs="--num_events=1000000 \
  --source_type=PUBSUB --pubsub_mode=SUBSCRIBE_ONLY \
  --pubsub_subscription=projects/my-project/subscriptions/nexmark-events-sub"
```

## Avro Files

`--sink_type=AVRO` writes the input events to Avro container files under `--output_path` and
discards the query results, as the Java launcher does:

| Files | Records |
|---|---|
| `<output_path>/event-SSSSS-of-NNNNN.avro` | All events, as `{newPerson, newAuction, bid}` with one field set |
| `<output_path>/bid-*.avro`, `auction-*.avro`, `person-*.avro` | The bids, new auctions and new persons |

`--source_type=AVRO --input_path=<output_path>/event` reads the files `<input_path>*.avro` back
and gives each event its own `dateTime` as timestamp. It works in batch and streaming mode.

- Field names are the camelCase names of the Java model (`emailAddress`, `initialBid`, ...).
  Times (`dateTime`, `expires`) are `long` epoch milliseconds.
- The files do not promise to be readable by the Java launcher, or Java files by this one:
  Java derives its Avro schema by reflection.
- In streaming mode each file holds one fixed window of `--window_size_sec`.
- Java's `--generateEventFilePathPrefix` text dump of the events is not ported.

## Coder Strategies

`--coder_strategy=HAND` (default) moves every element with the hand-written binary coders of
the model. `--coder_strategy=ROW` converts the events after the source to schema rows
(`EventRow`), reshuffles them, so the runner moves them with `beam:coder:row:v1`, and converts
them back. The queries keep the hand coders. This is narrower than the Java `AVRO` and `JAVA`
strategies, which change the coder of every PCollection: in this SDK a Rust type has one
coder, so a pipeline cannot switch the coder of the model types.

## Metrics and Summary Table

The `MonitorEvents` step after the source and the `MonitorResults` step before the sink record,
in namespace `nexmark`, the counters `<prefix>.elements` and `<prefix>.bytes` and the
distributions `<prefix>.processing_time_ms` (wall clock) and `<prefix>.event_timestamp_ms`.
The prefix is `events` or `results`.

The launcher submits each job and computes these columns from the final metrics:

| Column | Value |
|---|---|
| `Runtime(sec)` | First event to the last event or result, whichever is later. |
| `Events(/sec)`, `Events(bytes/sec)` | Events and event bytes divided by the runtime. |
| `Results`, `Results(/sec)` | Results, and results divided by the runtime. |
| `Startup(sec)` | Job submission to the first event. |
| `Processing delay(sec)` | First event to the first result. |

A `-` marks a value that the metrics cannot give, for example a rate when the runtime is zero.

## Streaming Runs

A batch job runs to its end. A streaming job is polled every 15 seconds, and these rules stop it:

| Rule | Action | Row |
|---|---|---|
| A `fatal` counter is above zero. | Cancel. | `FAILED` |
| All `--num_events` passed and the counts did not change for 1 minute. | Drain, or cancel if the runner cannot drain or with `--cancel_streaming_job_after_finish`. | `OK` |
| The counts did not change for 1 hour (a warning after 10 minutes). | Cancel. | `FAILED` |
| `--stream_timeout` seconds passed since the first event. | Cancel. | `TIMEOUT` |
| The [run-time limit](#run-time-limit) passed. This rule comes first. | Cancel. | `OK (running time)` |

With at least 9 polls over at least 2 minutes, `Events(/sec)` is the steady-state rate: a
least-squares fit over the middle third of the polls. Prism ends a finite streaming job by
itself, so on Prism the job usually ends before the drain rule applies.

The unbounded source reports its backlog in estimated bytes: the unclaimed events at the
average event size plus the size of the held-back late events. Its restriction size and its
progress use this unit.

As in Java, the unbounded source holds events back (`--prob_delayed_event`,
`--occasional_delay_sec`) only with `--is_rate_limited`. One call of the source emits up to
10,000 events or runs up to 10 seconds before it checkpoints, the bounds that the Dataflow Java
worker gives a splittable DoFn.
