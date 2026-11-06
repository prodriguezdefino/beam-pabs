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
- `src/model`: `Person`, `Auction`, `Bid`, `Event`, query output types, byte-size estimates, and
  binary coders.
- `src/generator`: deterministic 48-bit LCG event synthesis matching `java.util.Random`.
- `src/source`: bounded and unbounded splittable DoFn sources over event ID ranges.
- `src/queries`: 17 Nexmark query `PTransform` implementations plus the stateful `WinningBids`
  join.
- `src/models`: pure in-memory reference models for deterministic correctness checks.
- `src/correctness.rs`: in-pipeline checks that compare query output with a model.
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
| 16 | `RESHUFFLE` | Passes events through two `Reshuffle` stages with optional CPU load (`Query14`). |

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
- `--sink_type=<COUNT_ONLY|DEVNULL|TEXT>` and `--output_path=<dir-or-gs-uri>`
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

With at least 9 polls over at least 2 minutes, `Events(/sec)` is the steady-state rate: a
least-squares fit over the middle third of the polls. Prism ends a finite streaming job by
itself, so on Prism the job usually ends before the drain rule applies.

The unbounded source reports its backlog in estimated bytes: the unclaimed events at the
average event size plus the size of the held-back late events. Its restriction size and its
progress use this unit.
