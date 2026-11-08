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

# ValidatesRunner Conformance Guide

This guide describes the `ValidatesRunner` conformance suite of the Rust SDK, and how it runs
on Prism and Google Cloud Dataflow.

## Architecture

The suite has four parts: in-graph assertions, runner-independent pipeline builders, a
central test registry, and runner drivers.

### In-graph assertions (`passert`)

[`beam/testing/src/passert.rs`](../beam/testing/src/passert.rs) checks expectations on the
workers instead of sending data back to the driver:

- `passert::that("name", &pcoll).contains_in_any_order(...)`
- `passert::that("name", &pcoll).has_count(...)`
- `passert::that("name", &pcoll).all("predicate", |x| ...)`
- `passert::that_singleton("name", &pcoll).is_equal_to(...)`

Each assertion increments `PAssertSuccess/<name>` or a failure counter in the `PAssert`
namespace. [`passert::assertion_status`](../beam/testing/src/passert/verify.rs) reads those
counters and returns one of three states:

| Status | Meaning |
|---|---|
| `AllPassed(n)` | Every expected assertion reported success. |
| `Pending { passed, missing }` | No assertion failed, and some assertions have not reported. |
| `Failed { failed, total }` | At least one assertion reported failure. |

A test passes only when every registered assertion reports success.
[`tests/transforms/passert_test.rs`](../beam/runner-tests/tests/transforms/passert_test.rs)
checks the failure path on Prism with `FailsWith` expectations.

### Registry and worker reconstruction

[`beam/runner-tests/src/pipelines/`](../beam/runner-tests/src/pipelines/) defines pipeline
builders (`fn(&TestPipeline)`). Each builder adds transforms and `passert` checks without
starting a runner.

[`beam/runner-tests/src/registry.rs`](../beam/runner-tests/src/registry.rs) lists every
registered test once in `VALIDATES_RUNNER_TESTS`. Each entry is a
`ValidatesRunnerTest { suite, id, build, expect }` where `expect` is `Succeeds`,
`FailsWith(&[fragments])`, or `Custom(fn)`.

Both test binaries ([`validates_runner_prism`](../beam/runner-tests/tests/validates_runner_prism/main.rs)
and [`validates_runner_dataflow`](../beam/runner-tests/tests/validates_runner_dataflow/main.rs))
call [`run_validates_runner_suite`](../beam/runner-tests/src/suite_harness.rs), which checks
`RUNNER_SKIPS` for `(runner, mode)` and prints `libtest` output.

Rust closures do not serialize into the pipeline proto. On a containerized runner, the driver
passes `--vr_test=<id>` in `ValidatesRunnerOptions`. At startup, `beam-runner-tests-worker`
looks `<id>` up in `VALIDATES_RUNNER_TESTS`, runs `build` to register the handlers, and
serves bundles over the Fn API (see [static-registration.md](static-registration.md)).

### `TestDataflowRunner`

[`beam/runners/dataflow/src/test_runner.rs`](../beam/runners/dataflow/src/test_runner.rs)
adapts `DataflowRunner` for conformance runs:

- **Batch**: Submits the job, waits for a terminal state, and checks `assertion_status`.
- **Streaming**: Polls job state and metrics every few seconds. It cancels the job and
  succeeds on `AllPassed`, cancels and fails on `Failed`, and cancels and fails if `Pending`
  exceeds `DEFAULT_TEST_TIMEOUT` (20 minutes).

---

## Gradle Tasks

[`gradle/validates-runner.gradle`](../gradle/validates-runner.gradle) registers the suite
tasks:

| Task | Runner | Mode |
|---|---|---|
| `validatesRunnerPrism` | Local Prism | Batch |
| `validatesRunnerPrismStreaming` | Local Prism | Streaming (`BEAM_STREAMING=true`, `BEAM_PRISM_STREAMING=true`) |
| `validatesRunnerDataflow` | Dataflow Runner v2 | Batch |
| `validatesRunnerDataflowStreaming` | Dataflow Runner v2 | Streaming |
| `validatesRunner` / `validatesRunnerStreaming` | Prism alias | Batch / Streaming |

```bash
./gradlew :sdks:rust:validatesRunnerPrism
./gradlew :sdks:rust:validatesRunnerPrismStreaming
./gradlew :sdks:rust:validatesRunnerDataflow -PtestThreads=8
./gradlew :sdks:rust:validatesRunnerDataflowStreaming -PtestThreads=8
```

Run the Prism tasks in separate Gradle commands, so that they run in parallel. All four tasks
accept:

- `-Pfilter=<substring>`: Run matching tests only.
- `-PtestThreads=<n>`: Set concurrent test jobs (Dataflow defaults to `4`; Prism uses the
  host CPU count).
- `-Pignored` / `-PincludeIgnored`: Run only the skipped tests, or run all tests.
- `-PextraArgs="<flags>"` (Dataflow): Pass extra pipeline flags to every job in the run.

`validates_runner_dataflow` sets `test = false` in [`Cargo.toml`](../beam/runner-tests/Cargo.toml),
so `:sdks:rust:check` lints it while `:sdks:rust:test` does not submit cloud jobs. The
Dataflow tasks depend on `buildRunnerTestsWorker` and read `gcpProject`, `gcpTempLocation`,
and `sdkContainerImage` from `sdks/rust/.local/sdk.properties` or `-P` flags. Every Dataflow
job adds `--experiments=enable_lineage` so `assertions::user_metrics` receives string-set
metrics.

---

## Cross-language Suites

[`gradle/xlang.gradle`](../gradle/xlang.gradle) and [`gradle/xlang/`](../gradle/xlang/) test
cross-language expansion in both directions.

### Foreign SDK pipelines calling the Rust expansion service

Python, Java, and Go cross-language suites run against `beam_rust_testing_expansion_service`
([`xlang_transforms.rs`](../beam/runner-tests/src/xlang_transforms.rs)), which serves the
`beam:transforms:xlang:test:*` URNs and runs as the Rust worker:

```bash
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonPrism -PpythonVersion=3.12
./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaPrism
./gradlew :sdks:rust:validatesCrossLanguageRunnerGoPrism
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonDataflow -PpythonVersion=3.12
./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaDataflow
./gradlew :sdks:rust:validatesCrossLanguageRunnerGoDataflow
```

Pass `-Ptests=<list>` to select individual tests. Dataflow runs require
`dataflowRepositoryRoot`, `gcpProject`, and `gcpTempLocation` in
`sdks/rust/.local/sdk.properties` (or `-P` flags) and a pushed linux/amd64 base image. Use
`pythonSdkDataflowImage`, `javaSdkDataflowImage`, or `goSdkDataflowImage` to push a prebuilt
container for the driver SDK.

### Rust pipelines calling Java and Python expansion services

[`beam/runner-tests/tests/xlang/`](../beam/runner-tests/tests/xlang/) expands the
`beam:transforms:xlang:test:*` URNs from Rust pipelines through the Java or Python test
expansion service on Prism. The tests cover `prefix`, `multi_input_output_with_side_input`,
`group_by_key`, `co_group_by_key`, `combine_globally`, `combine_per_key`, `flatten`,
`partition`, and `python_bs4` PyPI artifact staging:

```bash
./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaServicePrism
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonServicePrism -PpythonVersion=3.12
```

Pass `-PbuildJavaContainer` or `-PbuildPythonContainer` when the local SDK container image
does not exist. `test_co_group_by_key` against the Java service has `#[ignore]`, because
Prism's element splitter (`pullDecoderNoAlloc` in `runners/prism/internal/coders.go`) does
not decode chunked iterables (`count = -1`).

---

## Conformance Results

Run the suite tasks of [Gradle Tasks](#gradle-tasks) to get the results. Each task prints the
`libtest` summary (passed, failed, ignored) for its runner and mode. A test in `RUNNER_SKIPS`
counts as ignored.

### Skipped tests (`RUNNER_SKIPS`)

[`RUNNER_SKIPS`](../beam/runner-tests/src/registry.rs) records every runner limitation or bug
that causes a skip:

| Runner | Test(s) | Mode | Reason |
|---|---|---|---|
| Prism | `on_window_expiration_same_key_two_windows` | Batch, streaming | Go Prism registers a key for window expiration only in its first window of a batch. Passes on Dataflow. |
| Prism | `multimap_state_empty_after_remove` | Batch, streaming | Go Prism `MultimapKeysUserState` returns keys that an earlier bundle removed through `MultimapUserState`. Passes on Dataflow batch. |
| Dataflow | `requires_time_sorted_input` | Batch, streaming | Runner v2 rejects `beam:requirement:pardo:time_sorted_input:v1`. |
| Dataflow | `processing_time_timer`, `processing_time_timer_output_hold`, `processing_time_timer_family` | Batch | Runner v2 batch does not fire processing-time timers. |
| Dataflow | `rewindow_with_output_time` | Batch | Runner v2 batch ignores `OutputTime::LatestInPane` on `GlobalWindows` (`apache/beam#20931`). Passes on Dataflow streaming and Prism. |
| Dataflow | `group_by_key_large_keys_{10kb,100kb,1mb}` | Streaming | Streaming Engine rejects keys of 10 KiB or larger. |
| Dataflow | `group_by_key_large_keys_{10mb,100mb}` | Batch, streaming | Dataflow rejects keys of 10 MiB or larger. |
| Dataflow | `multimap_state`, `multimap_state_clear_remove`, `multimap_state_empty_after_remove` | Streaming | Streaming Engine lists multimap keys that earlier bundles only appended to, but returns no values unless cleared first. |
| Dataflow | `bundle_finalization_with_side_input` | Streaming | Streaming Engine sends `FinalizeBundle` only to stages that read a shuffled value stream, not to a stage after `GroupByKey`. |

---

## Capabilities Covered

| Area | Covered behavior |
|---|---|
| Element-wise & DAGs | `Impulse`, empty and nullable `Create`, `Map`, `FlatMap`, `Filter`, diamond DAGs, tagged multi-output `ParDo` (including empty outputs and custom timestamps), `Flatten`, `Partition`, `Reshuffle` (preserving timestamps, windows, and panes), `DoFn` lifecycle order, pipeline options in `DoFn` |
| Grouping & joins | `GroupByKey` (empty input, 64 MiB iterables, 10 KiB–100 MiB keys, nullable values), `GroupIntoBatches` (count, byte limits, sharded keys), `CoGroupByKey` (global and windowed), inner/left/right/full outer joins, `Distinct` |
| Combine | `CombinePerKey` (combiner lifting and capacity flushes), `CombineGlobally` (with and without defaults, triggered), `Count`, `Max`/`Min`, folds, composed tuple and `Decimal` accumulators |
| Schemas & coders | `#[derive(BeamRow)]` and dynamic `Row` elements across `ParDo`, tagged outputs, `Filter`, `GroupByKey`, and `Reshuffle`; `Option<T>` and nullable `Row` coders |
| Static registration | `#[derive(Register)]` on `DoFn`, `SplittableDoFn`, and `CombineFn` built directly from the pipeline graph on the worker |
| Side inputs | Singleton, iterable, and multimap views; empty, nested, windowed, and triggered views; views read from closures, `DoFn`s, `StatefulDoFn`s, timers, `on_window_expiration`, and `SplittableDoFn`s |
| State & timers | `ValueState`, `BagState`, `CombiningState`, `MapState`, `SetState`, `MultimapState`, `OrderedListState` (`read_range`, `clear_range`); event-time and processing-time timers and timer families, dynamic tags, looping timers, output-timestamp holds, `on_window_expiration` (with allowed lateness), `RequiresTimeSortedInput` |
| Splittable `DoFn` | `GenerateSequence`, `PeriodicImpulse`, self-checkpointing, manual watermark estimators, side inputs across checkpoints, tagged outputs, lifecycle order |
| Windowing & triggers | `GlobalWindows`, `FixedWindows`, `SlidingWindows`, `Sessions` merging, `OutputTime` policies, windowed `finish_bundle` output; `TestStream` trigger, pane, lateness, and timer tests in [`tests/transforms/test_stream_test.rs`](../beam/runner-tests/tests/transforms/test_stream_test.rs) (4 tests marked `#[ignore]` for Go Prism bugs `apache/beam#41073` and `#41075`) |
| Metrics & finalization | `passert` combinators, user counters, distributions, gauges, string sets, bounded tries (attempted vs committed), and bundle finalization callbacks |

[sdk-parity.md](sdk-parity.md) lists the features that the SDK does not support, so the suite
has no tests for them.
