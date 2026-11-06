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
- `src/launcher` and `src/main.rs`: CLI flag parsing, pipeline assembly, metrics collection,
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
