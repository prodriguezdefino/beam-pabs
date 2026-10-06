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
matches the directory name. Run each example on the portable Prism runner.

The Gradle launch tasks run `cargo run --release -p <example> -- <runner flags> <extraArgs>`
(see [`prism.gradle`](../gradle/prism.gradle)):

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
| [`wordcount`](#wordcount) | Pipeline options, composite transforms |
| [`streaming`](#streaming-continuous-ingestion-with-periodicimpulse) | `PeriodicImpulse`, fixed windows |
| [`side_inputs`](#side-inputs--broadcast-joins) | Singleton and iterable side inputs, broadcast join |
| [`join`](#relational-joins) | `inner_join`, `left_join`, `CoGroupByKey` |
| [`gaming`](#mobile-gaming-stateful-processing--metrics) | Stateful DoFn, user metrics |
| [`partition`, `forest`](#partition--flatten) | `Partition`, `Flatten`, recursive construction |
| [`windowed_wordcount`](#windowed-wordcount-fixed-tumbling-windows) | Event timestamps, fixed windows |
| [`leaderboard`](#mobile-gaming-leaderboard-triggers-speculative-panes--allowed-lateness) | Triggers, accumulation, allowed lateness |
| [`sessions`](#user-session-analytics-dynamic-sessionization--window-merging) | Session windows |
| [`sliding_window`](#sliding-window-moving-average-overlapping-hopping-windows) | Sliding windows |
| [`row_schemas`](#type-driven-row-schemas--logical-types-row_schemas) | `#[derive(BeamRow)]`, logical types |
| [`state_conformance`](#cross-bundle-state-conformance-state_conformance) | `MapState`, `SetState`, timers |
| [`fold_state_backed`](#fold-combinators--state-backed-iterables-fold_state_backed) | Folds, state-backed iterables |

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
```

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
```

## Side Inputs & Broadcast Joins

This pipeline filters Shakespeare word counts with two side inputs: a singleton side input (`as_singleton`) for minimum word length and an iterable side input (`as_iter`) for a stopword list. It then enriches counts with character roles using a shuffle-free `broadcast_left_join`:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=side_inputs
```

The `apache-beam-runner-tests` suite tests multimap side inputs (`as_multimap`).

## Relational Joins

This pipeline joins user profiles with orders using keyed joins (`inner_join`, `left_join`) and
groups both collections with `CoGroupByKey` over a `KeyedPCollectionTuple`. `--output` is
optional:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=join
```

## Mobile Gaming (Stateful Processing & Metrics)

This pipeline parses gaming events from Google Cloud Storage, tracks user metrics (`Counter` and `Distribution`), and aggregates team scores. It uses a stateful DoFn with both [`ValueState`](../beam/core/src/transforms/dofn/state.rs) and [`BagState`](../beam/core/src/transforms/dofn/state.rs) to detect and award team milestones (`--threshold`, default `500`):

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=gaming -PextraArgs="--output=/tmp/gaming_output.txt"
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
```

## User Session Analytics (Dynamic Sessionization & Window Merging)

This pipeline groups user interactions into dynamic sessions (`Sessions::with_gap_duration`) separated by an inactivity gap of `--gap_duration` seconds (default `300`). It merges overlapping windows per user key:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=sessions
```

## Sliding Window Moving Average (Overlapping Hopping Windows)

This pipeline partitions time-series readings into overlapping sliding windows (`SlidingWindows::of(size).every(period)`, set by `--window_size` and `--window_period` in seconds, defaults `30` and `10`). It tracks rolling statistics: reading count, moving average, minimum, and maximum. The default input is the gaming CSV, which it reads as `user,team,score,timestamp_ms`:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=sliding_window
```

## Type-Driven Row Schemas & Logical Types (`row_schemas`)

This pipeline demonstrates type-driven Beam Row Schemas with `#[derive(BeamRow)]` and `#[derive(BeamEnum)]`. It supports nested structs, string-backed unit enums, logical decimals (`rust_decimal::Decimal`), dates (`chrono::NaiveDate`), UTC timestamps (`chrono::DateTime<Utc>`), and global aggregations (`combine_globally`):

```bash
# Portable Prism Runner (zero network, self-contained)
./gradlew :sdks:rust:prism -Pexample=row_schemas
```

## Cross-Bundle State Conformance (`state_conformance`)

This pipeline verifies that `MapState` and `SetState` mutations (`clear`, `remove` of a present or absent key) persist across bundles. Event-time timers stage seed, mutate, and verify operations in separate bundles. Each key reports `MATCH` or `DIVERGENCE` against the Beam model (`--strict` fails the pipeline instead):

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=state_conformance
```

## Fold Combinators & State-Backed Iterables (`fold_state_backed`)

This pipeline contrasts a combiner-backed fold (`fold_per_key` with combiner lifting) against a `GroupByKey`-backed fold (`fold_values` sequential reduction over grouped iterables). It exercises runner continuation tokens and lazy paging over the Fn API State channel (`beam:coder:state_backed_iterable:v1`) under skewed hot keys (`--num_elements`, default `50000`; `--payload_bytes`, default `128`), validating numerical parity against Gauss's summation formula:

```bash
# Portable Prism Runner
./gradlew :sdks:rust:prism -Pexample=fold_state_backed
```
