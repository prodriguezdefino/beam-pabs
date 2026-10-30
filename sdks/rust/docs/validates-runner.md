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

This guide describes the `ValidatesRunner` conformance suite of the Rust SDK, how the test harness executes on Prism and Google Cloud Dataflow, and the current test results.

## Purpose

`ValidatesRunner` is the conformance test suite for Apache Beam SDKs and runners. It checks three areas:

- **Core semantics**: Element-wise transforms (`Map`, `FlatMap`, `Filter`), `GroupByKey`, `Combine`, `CoGroupByKey`, windowing, state and timers, and splittable DoFns. Triggers are tested separately (see the end of this page).
- **Worker execution**: User functions and standard coders run across the Fn API boundary without data loss or corruption.
- **Failure detection**: When an in-pipeline assertion (`passert`) fails on a worker, the failure stops the bundle and fails the job. The registered suite checks that assertions run and pass; [`tests/transforms/passert_test.rs`](../beam/runner-tests/tests/transforms/passert_test.rs) checks the failure path on Prism with `FailsWith` expectations.

---

## Architecture

The suite consists of in-graph assertions (`beam::testing::passert`), a runner-agnostic library of pipeline builders, a central test registry, and runner drivers.

### In-Graph Assertions (`passert`)

[`sdks/rust/beam/testing/src/passert.rs`](../beam/testing/src/passert.rs) evaluates expectations on workers instead of pulling data back to the driver:

- `passert::that("name", &pcoll).contains_in_any_order(...)`
- `passert::that("name", &pcoll).has_count(...)`
- `passert::that("name", &pcoll).all("predicate", |x| ...)`

Each assertion increments a success counter (`PAssertSuccess/<name>` in the `PAssert` namespace) or a failure counter. `passert::assertion_status(metrics, names)` ([`passert/verify.rs`](../beam/testing/src/passert/verify.rs)) inspects those counters and returns an `AssertionStatus`:

| Status | Meaning |
|---|---|
| `AllPassed(n)` | Every expected assertion reported success. |
| `Pending { passed, missing }` | No failure yet, and some assertions have not reported. |
| `Failed { failed, total }` | At least one assertion reported a failure. |

A test passes only when every expected assertion reports success. Both `TestPipeline` (after a batch run) and `TestDataflowRunner` (during a streaming run) enforce this check.

### Runner-Agnostic Pipeline Builders

The test pipelines in [`sdks/rust/beam/runner-tests/src/pipelines/`](../beam/runner-tests/src/pipelines/) are builder functions (`build_<x>(p: &TestPipeline)`). Each builder adds transforms and `passert` checks to `p` without running the pipeline:

- `build_map_and_filter`, `build_flat_map`, `build_dofn_lifecycle`, `build_diamond_dag`, `build_multi_output_pardo`
- `build_group_by_key`, `build_group_by_key_large_iterable`, `build_combine_per_key`, `build_combine_max_min`, `build_combine_flushes_accumulators_at_capacity`, `build_count_per_element`, `build_combine_globally`, `build_count_globally`, `build_folds`, `build_combine_composed_accumulators`
- `build_flatten_many`, `build_flatten_singleton_list`, `build_flatten_then_pardo`, `build_flatten_multiple_copies`, `build_create_empty`, `build_kv_swap`, `build_reshuffle`, `build_partition`, `build_partition_many`, `build_group_into_batches_sharded_key`
- `build_generate_sequence`, `build_periodic_impulse`, `build_sdf_self_checkpoint`, `build_sdf_watermark_estimator`
- `build_cogroup_by_key`, `build_joins`, `build_join_cross_product`, `build_row_schema_coder`
- `build_registered_dofn`, `build_registered_splittable_dofn`, `build_registered_combine_per_key`, `build_registered_combine_globally`
- `build_side_input_views`, `build_empty_iterable_side_input`, `build_windowed_side_input`, `build_side_input_multiple_and_coders`
- `build_stateful_pardo`, `build_combining_state`, `build_windowed_combining_state`, `build_windowed_stateful_pardo`, `build_map_and_set_state`, `build_map_and_set_state_clear`, `build_map_and_set_state_remove`, `build_ordered_list_state`, `build_ordered_list_state_clear`, `build_multimap_state`, `build_multimap_state_clear_remove`, `build_on_window_expiration`, `build_on_window_expiration_with_lateness`, `build_on_window_expiration_same_key_two_windows`, `build_bundle_lifecycle_batching`
- `build_timer_strict_ordering`, `build_timer_looping`, `build_timer_output_timestamps`, `build_timer_dynamic_tags`, `build_processing_time_timer`, `build_timer_with_side_input`, `build_requires_time_sorted_input`
- `build_windowed_group_by_key`, `build_sliding_windows_pardo`, `build_window_sums_gbk`, `build_window_sums_lifted`, `build_rewindow_preserves_multiplicity`, `build_timestamps_and_window_reify`, `build_distinct_per_window`
- `build_passert_success`, `build_user_metrics`, `build_bundle_finalization`
- `build_group_by_key_empty`, `build_group_by_key_large_keys_10kb`, `build_group_by_key_large_keys_100kb`, `build_group_by_key_large_keys_1mb`, `build_group_by_key_large_keys_10mb`, `build_group_by_key_large_keys_100mb`, `build_reshuffle_keeps_element_metadata`, `build_flatten_empty_inputs_then_pardo`, `build_group_into_batches_count_and_byte_limits`, `build_combine_globally_empty_without_defaults`, `build_schema_dofn_reads_and_writes_rows`, `build_schema_dofn_writes_rows_to_tagged_outputs`, `build_schema_kept_through_filter`
- `build_rewindow_with_output_time`, `build_cogroup_by_key_windowed`, `build_reshuffle_keeps_windows`, `build_reshuffle_after_group_by_key_keeps_windows`, `build_session_windows_merge_in_group_by_key`, `build_group_by_key_output_time`, `build_finish_bundle_windowed_output`, `build_windowed_combine_per_key`, `build_windowed_combine_globally`, `build_flatten_preserves_windowing`
- `build_multi_output_dofn_lifecycle`, `build_stateful_dofn_lifecycle`
- `build_global_side_inputs_in_fixed_windows`, `build_side_inputs_in_dofn_and_closure`, `build_broadcast_joins`, `build_windowed_multimap_side_input`, `build_empty_multimap_side_input`, `build_combine_globally_as_singleton`, `build_empty_flatten_side_input`, `build_nested_iterable_side_input`, `build_side_input_read_by_every_element`, `build_multi_output_side_inputs`, `build_bag_state_side_input`, `build_state_side_input`, `build_side_input_not_ready_timer`, `build_bundle_finalization_with_side_input`
- `build_windowed_singleton_is_equal_to`, `build_windowed_satisfies`, `build_multi_output_pardo_empty_outputs`, `build_impulse`, `build_identity_composite`
- `build_value_state_same_id_in_two_dofns`, `build_stateful_dofn_tagged_outputs`, `build_multimap_state_empty_after_remove`, `build_ordered_list_state_read_range`, `build_global_combine_with_defaults_and_trigger`
- `build_gbk_then_timers`, `build_event_time_timer_reset`, `build_timers_set_each_other`, `build_timer_output_hold`, `build_processing_time_timer_output_hold`, `build_processing_time_timer_family`, `build_on_window_expiration_global`, `build_pipeline_options_in_dofn`, `build_tagged_output_timestamps`
- `build_sdf_side_input`, `build_sdf_windowed_side_input`, `build_sdf_windowed_side_input_checkpoints`, `build_sdf_tagged_outputs`, `build_sdf_lifecycle`
- `build_impulse_timestamp_is_min`, `build_end_of_global_window_firing_times`
- `build_create_with_nulls_and_values`, `build_nullable_rows_through_reshuffle`, `build_nullable_values_grouped_by_key`

The same directory also has builders that the registry does not list, for example the trigger pipelines that [`tests/transforms/test_stream_test.rs`](../beam/runner-tests/tests/transforms/test_stream_test.rs) runs, `build_windowed_side_input_not_present` that `side_input_test.rs` runs, and the failing pipelines of `passert_test.rs`.

Each test pairs a builder with an `Expectation`:

| `Expectation` | Requirement |
|---|---|
| `Succeeds` | The job succeeds, and every registered assertion runs and passes. |
| `FailsWith(&[fragments])` | The job fails with an error message that contains every fragment. |
| `Custom(fn)` | The outcome satisfies the check function, which also gets the `RunnerKind` (Prism or Dataflow). |

`run_pipeline(id, build, expect, runner)` constructs a `TestPipeline`, runs it on `runner`, and checks the `Expectation`. The runner implements `SuiteRunner`, which gives its `RunnerKind`.

### Central Test Registry and Worker Reconstruction

[`sdks/rust/beam/runner-tests/src/registry.rs`](../beam/runner-tests/src/registry.rs) defines the 141 conformance tests once in `VALIDATES_RUNNER_TESTS` across 19 suites (`core_transforms`, `joins`, `schemas`, `registered_functions`, `side_inputs`, `state_and_timers`, `windowing`, `assertions`, `bundle_finalization`, `core_more`, `windowing_more`, `lifecycle_more`, `side_inputs_more`, `assertions_more`, `state_more`, `timers_more`, `sdf_more`, `time_bounds`, `nullable_elements`). Each entry is a `ValidatesRunnerTest { suite, id, build, expect }`:

- [`tests/validates_runner_prism/main.rs`](../beam/runner-tests/tests/validates_runner_prism/main.rs) and [`tests/validates_runner_dataflow/main.rs`](../beam/runner-tests/tests/validates_runner_dataflow/main.rs) (`harness = false`) call [`run_validates_runner_suite`](../beam/runner-tests/src/suite_harness.rs), which evaluates [`RUNNER_SKIPS`](../beam/runner-tests/src/registry.rs) for the active `(runner, mode)` and prints `libtest`-compatible results.
- `VALIDATES_RUNNER_TESTS` is also read by the worker binary (`beam-runner-tests-worker`).

Because Rust closures cannot be serialized into the pipeline proto, a containerized worker reconstructs the pipeline graph at startup (see [static-registration.md](static-registration.md)). The test driver passes `--vr_test=<id>` in `ValidatesRunnerOptions`. The worker looks `<id>` up in `VALIDATES_RUNNER_TESTS`, calls its `build` function on a `TestPipeline` with the worker options, and serves bundles over the Fn API.

### `TestDataflowRunner`

[`sdks/rust/beam/runners/dataflow/src/test_runner.rs`](../beam/runners/dataflow/src/test_runner.rs) implements `TestDataflowRunner`:

- **Batch**: Submits the job with `DataflowRunner` and waits for a terminal state.
- **Streaming**: Streaming jobs do not stop on their own. The runner polls the job state and metrics and evaluates `assertion_status`:
  - `AllPassed`: cancels the job and returns success.
  - `Failed`: cancels the job and returns an error.
  - `Pending` past the timeout (`DEFAULT_TEST_TIMEOUT`, 20 minutes): cancels the job and fails, listing the missing assertions.
  - Terminal job state reached before all assertions pass: returns an error.

### Gradle Tasks

[`sdks/rust/gradle/validates-runner.gradle`](../gradle/validates-runner.gradle) defines the verification tasks:

| Task | Runner | Mode |
|---|---|---|
| `validatesRunnerPrism` | Local Prism runner | Batch |
| `validatesRunnerPrismStreaming` | Local Prism runner | Streaming (`BEAM_STREAMING=true`, `BEAM_PRISM_STREAMING=true`) |
| `validatesRunnerDataflow` | Google Cloud Dataflow | Batch |
| `validatesRunnerDataflowStreaming` | Google Cloud Dataflow | Streaming |
| `validatesRunner` / `validatesRunnerStreaming` | Aggregate alias for the Prism task | Batch / Streaming |

All four tasks accept `-Pfilter=<substring>`, `-PtestThreads=<n>` (`--test-threads` or `RUST_TEST_THREADS`), `-Pignored` (run only the tests listed in `RUNNER_SKIPS` for that runner and mode), and `-PincludeIgnored` (run both normal and skipped tests). The Dataflow tasks default to `4` threads to stay within regional worker quota. The Prism tasks use the host CPU count. The Dataflow tasks also accept `-PextraArgs="<pipeline flags>"`, which every job of the suite gets, for example `-PextraArgs="--sdk_harness_log_level_overrides=harness::state=debug"` to log the State API paging of the workers.

Every Dataflow job gets `--experiments=enable_lineage`. Without it, Dataflow does not report user string set metrics (`assertions::user_metrics`). Dataflow does not report gauges, so on Dataflow that test accepts a missing gauge. On Prism it requires the gauge. The test checks the phase that each runner reports as final: on Prism the committed value (Prism reports only the last progress report of a bundle as attempted, which can be partial or absent), on Dataflow the attempted value, and the committed value when present (streaming jobs report none).

`validates_runner_dataflow` sets `test = false` in [`Cargo.toml`](../beam/runner-tests/Cargo.toml), so `:sdks:rust:check` (`--all-targets`) compiles and lints it while `:sdks:rust:test` does not submit cloud jobs. The Dataflow Gradle tasks depend on `buildRunnerTestsWorker`, which builds the linux worker binary `beam-runner-tests-worker` (in the builder container unless the host is Linux on the same architecture). They read `gcpProject`, `gcpTempLocation`, and `sdkContainerImage` (required), and `gcpRegion` (default `us-central1`), `network`, `subnetwork` and `workerBinary` (optional) from `sdks/rust/.local/sdk.properties` or `-P` flags, and pass them to the tests as `BEAM_DATAFLOW_*` variables:

```bash
./gradlew :sdks:rust:validatesRunnerPrism
./gradlew :sdks:rust:validatesRunnerPrismStreaming
./gradlew :sdks:rust:validatesRunnerDataflow -PtestThreads=4
./gradlew :sdks:rust:validatesRunnerDataflowStreaming -PtestThreads=4
```

### Cross-language Suites

[`sdks/rust/gradle/xlang.gradle`](../gradle/xlang.gradle) runs the Python cross-language suite (`validate_runner_xlang_test.py`) on Prism and on Dataflow against the Rust test expansion service in [`beam/runner-tests/src/xlang_transforms.rs`](../beam/runner-tests/src/xlang_transforms.rs). That service serves the `beam:transforms:xlang:test:*` URNs, as the Java and Python test expansion services do. Its image is `beam_rust_testing_expansion_service`.

```bash
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonPrism -PpythonVersion=3.12
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonPrism -Ptests=prefix,group_by_key
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonDataflow -PpythonVersion=3.12
```

- The tasks build the service image for the host architecture, start it on a free port and remove it at the end. Prism starts the Rust worker from the same image.
- `setupXlangPythonEnv` makes a virtualenv in `sdks/rust/build/xlang-venv` with the Python SDK of this tree and its `gcp` and `test` extras. The suite imports `apache_beam` from `sdks/python`, so the task builds the virtualenv once; `-PrefreshXlangPythonEnv` builds it again. `-PpythonVersion` selects the interpreter (3.11 or later).
- `-Ptests` selects tests in Python: test method names without the `test_` prefix (e.g. `-Ptests=prefix,group_by_key`).
- The tasks need Docker. If the Docker API is not on the default socket (for example with Colima), the tasks take the socket from the current Docker context.

`validatesCrossLanguageRunnerJavaServicePrism` runs the other direction: Rust pipelines on Prism expand the `beam:transforms:xlang:test:*` URNs through the Java test expansion service (`sdks/java/testing/expansion-service`). The tests are in [`beam/runner-tests/tests/xlang/`](../beam/runner-tests/tests/xlang/) and use the inputs and expected values of `ValidateRunnerXlangTest.java`:

| Rust test | Java test | Python test |
|---|---|---|
| `test_prefix` | `SingleInputOutputTest` | `test_prefix` |
| `test_multi_input_output_with_side_input` | `MultiInputOutputWithSideInputTest` | `test_multi_input_output_with_sideinput` |
| `test_group_by_key` | `GroupByKeyTest` | `test_group_by_key` |
| `test_co_group_by_key` | `CoGroupByKeyTest` | `test_cogroup_by_key` |
| `test_combine_globally` | `CombineGloballyTest` | `test_combine_globally` |
| `test_combine_per_key` | `CombinePerKeyTest` | `test_combine_per_key` |
| `test_flatten` | `FlattenTest` | `test_flatten` |
| `test_partition` | `PartitionTest` | `test_partition` |

```bash
./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaServicePrism
./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaServicePrism -Pfilter=test_partition
```

- The task builds the service JAR (`buildTestExpansionServiceJar`), starts it on a free port with the JVM of Gradle, and stops it at the end. The service log is `sdks/rust/build/xlang-java-test-expansion-service.log`.
- The task sets `BEAM_JAVA_TEST_EXPANSION_ADDR` to the service address. Without it, or without Docker, the tests skip with a message, so `:sdks:rust:test` skips them. The task fails when Docker is not reachable, so it never passes with no tests run.
- The service names the Java SDK image of its JVM version as the environment, for example `apache/beam_java21_sdk:<sdk_version>`. Prism starts that image, so it must exist locally. `-PbuildJavaContainer` builds it first.
- The client fetches the dependencies of the Java environment through the `ArtifactRetrievalService` of the service. It writes them to `~/.apache_beam/cache/jars/resolved/<sha256>` (or under `BEAM_CACHE_DIR`), so the runner stages local files. `url` and `embedded` artifacts stay as they are.
- `GroupByKeyWithGbekTest` and `PythonDependenciesTest` are not ported. The Rust SDK has no `gbek` pipeline option, and the Java test expansion service does not serve `python_bs4`.
- `test_co_group_by_key` carries `#[ignore]`. The Java `ParDo` after the `CoGroupByKey` writes its iterables in the chunked form (count `-1`, then blocks). The element splitter of Prism (`pullDecoderNoAlloc` in `runners/prism/internal/coders.go`) reads only the count form, so the Rust worker gets wrong elements. Run it with `-Pfilter=test_co_group_by_key -PincludeIgnored`.

The Dataflow tasks also build a linux/amd64 worker image and push it to the registry in `dataflowRepositoryRoot`, for example `us-central1-docker.pkg.dev/<project>/<repository>`. Each test is one Dataflow job, and `-PxlangParallelism` (default `8`) sets how many run at the same time. The tasks read these properties from `sdks/rust/.local/sdk.properties` (with the `example.xlang.` prefix or without it) or from `-P` flags:

| Property | Meaning |
|---|---|
| `dataflowRepositoryRoot` | Registry path of the worker images. Required. |
| `gcpProject`, `gcpTempLocation` | Project and temp location of the jobs. Required. |
| `gcpRegion` | Region of the jobs. Defaults to `us-central1`. |
| `network`, `subnetwork`, `workerMachineType` | Optional worker settings. |
| `pythonSdkImage` | Python SDK container that already holds the SDK of this tree. Optional for Python. |

The SDK base image must be in that registry for linux/amd64 first:

```bash
./gradlew :sdks:rust:container:docker -Pcontainer-architecture-list=amd64 \
  -Pdocker-repository-root=<dataflowRepositoryRoot> -Ppush-containers
```

Without `pythonSdkImage`, each worker builds the staged SDK into a wheel before the Python worker starts, which adds about five minutes to each job. `pythonSdkDataflowImage` builds the Python SDK container of this tree for linux/amd64, pushes it to `dataflowRepositoryRoot` and prints the image tag:

```bash
./gradlew :sdks:rust:pythonSdkDataflowImage -PpythonVersion=3.12
```

---

## Conformance Results

| Runner | Batch | Streaming | Notes |
|---|:---:|:---:|---|
| Prism | 139 passed, 2 ignored | 139 passed, 2 ignored | Run ignored bug demos with `-Pignored` or `-PincludeIgnored`. |
| Dataflow Runner V2 | 134 passed, 7 ignored | 131 passed, 10 ignored | Streaming jobs cancel automatically when all assertions pass. |

### Skipped Tests (`RUNNER_SKIPS`)

[`RUNNER_SKIPS`](../beam/runner-tests/src/registry.rs) lists every registered test that Prism or Dataflow reports as `ignored`, with the runner, modes, and reason. Pass `-Pignored` (or `-PincludeIgnored`) to any of the four Gradle tasks to execute them on demand.

| Runner | Test | Mode | Reason |
|---|---|---|---|
| Prism | `on_window_expiration_same_key_two_windows` | Batch, streaming | Go Prism registers a key for window expiration only in its first window of a pending batch, so the second window of key `a` never expires. Passes on Dataflow batch and streaming. |
| Prism | `multimap_state_empty_after_remove` | Batch, streaming | `MultimapKeysUserState` in Go Prism keeps keys that `MultimapUserState` cleared in an earlier bundle, so `keys()` is not empty in the verification timer. Passes on Dataflow batch. |
| Dataflow | `requires_time_sorted_input` | Batch, streaming | Runner V2 rejects `beam:requirement:pardo:time_sorted_input:v1`. The Java Runner V2 suites exclude `UsesRequiresTimeSortedInput` too. |
| Dataflow | `processing_time_timer`, `processing_time_timer_output_hold`, `processing_time_timer_family` | Batch | Runner V2 batch does not fire processing-time timers. The Java Runner V2 batch suite excludes `UsesTimersInParDo`. |
| Dataflow | `rewindow_with_output_time` | Batch | Runner V2 batch ignores `OutputTime::LatestInPane` on `GlobalWindows` and emits groups at `Instant::MIN` (`apache/beam#20931`). The Java Runner V2 batch suite excludes `testRewindowWithTimestampCombiner`. Passes on Dataflow streaming and Prism. |
| Dataflow | `group_by_key_large_keys_10kb`, `group_by_key_large_keys_100kb`, `group_by_key_large_keys_1mb` | Streaming | The Java Runner V2 streaming suite excludes `LargeKeys$Above10KB`. |
| Dataflow | `group_by_key_large_keys_10mb`, `group_by_key_large_keys_100mb` | Batch, streaming | The Java Runner V2 batch suite excludes `testLargeKeys10MB` and `testLargeKeys100MB`; streaming excludes all large keys. |
| Dataflow | `multimap_state`, `multimap_state_clear_remove`, `multimap_state_empty_after_remove` | Streaming | Streaming Engine returns no values for a multimap key that earlier bundles only appended to, and still lists the key. A key written with a clear first reads back. A Java pipeline on Runner V2 streaming shows the same loss (Beam 2.77.0, job `2026-11-05_00_29_04-448174293273453224`). The Java suites do not catch it, because their multimap tests read in the bundle that wrote. |
| Dataflow | `bundle_finalization_with_side_input` | Streaming | The harness reports `requires_finalization`, but Streaming Engine sends no `FinalizeBundle` to a stage that reads a `GroupByKey`. Only stages that read a shuffled value stream (a stateful `ParDo`, a splittable `DoFn`) get it. The Java Runner V2 suites exclude `UsesTestStream`, so they never run the Java bundle finalization tests, and the Go tests run only in loopback mode. Passes on Dataflow batch and Prism. |

### Coverage by Suite

| Suite | Capabilities Verified |
|---|---|
| `core_transforms` | `Map`, `FlatMap`, `Filter`, `DoFn` lifecycle order, diamond DAGs, multi-output `ParDo`, `GroupByKey` (also with a large iterable), `CombinePerKey` (with combiner lifting and accumulator flushes), `Max` / `Min`, `Count` per element and globally, `CombineGlobally`, folds, a `Compose` whose accumulators and elements use the tuple and `Decimal` coders, `Flatten` (many inputs, one input, the same input twice), empty `Create`, KV swap, `Reshuffle` (keyed, unkeyed, timestamps kept), `Partition`, `GroupIntoBatches` on a sharded key (the `sharded_key` coder), `GenerateSequence` and `PeriodicImpulse` (splittable DoFns), a splittable DoFn that checkpoints itself, a splittable DoFn with a manual watermark estimator |
| `joins` | `CoGroupByKey`, `inner_join`, `left_join`, `right_join`, `full_outer_join`, join cross product |
| `schemas` | `#[derive(BeamRow)]` elements (string, integer, list and boolean fields) through `GroupByKey` and `Reshuffle` with the row coder |
| `registered_functions` | `#[derive(Register)]` types that the worker builds from the graph: a `DoFn` with a field that `setup` builds, a splittable `DoFn`, and a `CombineFn` per key and globally (lifted on Prism, as `ParDo`s on Dataflow) |
| `side_inputs` | Singleton, iterable and multimap views, empty iterable side input, windowed side inputs, several side inputs with different coders in one `ParDo` |
| `state_and_timers` | `ValueState`, `BagState`, `MapState` and `SetState` (read back from a timer in a later bundle, clear, remove), `MultimapState` (duplicate values, remove, clear), `OrderedListState` (sorted reads, `clear_range` and `clear` from timers), `CombiningState` (tuple and `Decimal` accumulators, per window, read and clear in a later bundle), event-time timers (strict order, a timer that sets itself again, output timestamps, dynamic tags, a timer that reads a side input), processing-time timers, `on_window_expiration` under fixed windows (single and multiple windows per key, and with allowed lateness), `RequiresTimeSortedInput`, per-window state and timer isolation under `SlidingWindows`, bundle lifecycle batching |
| `windowing` | `FixedWindows`, `SlidingWindows`, `Sessions`, per-window `DoFn` explosion, lifted and unlifted window sums, re-windowing multiplicity, output timestamps and window reification, `Distinct` per window |
| `assertions` | `passert` assertions (`contains_in_any_order`, `contains`, `has_count`, `not_empty`, `all`, `empty`, `that_singleton`) that must all run and pass; user counters, distributions, gauges and string sets read from the job result, with the attempted and the committed phase checked apart |
| `bundle_finalization` | A bundle finalization callback runs after the runner commits the bundle |
| `core_more` | `GroupByKey` on an empty input and on keys of 10 KiB to 100 MiB, `Reshuffle` keeps timestamps, windows and panes, `Flatten` of empty inputs, `GroupIntoBatches` with count and byte limits, empty `combine_globally_without_defaults`, `DoFn`s that read and write `Row`s (one and two outputs), a `Row` schema kept through `Filter` |
| `windowing_more` | Re-window after a `LatestInPane` `GroupByKey`, windowed `CoGroupByKey`, `Reshuffle` of fixed, sliding and session windows (also after `GroupByKey`), session merging, `GroupByKey` output time for each `OutputTime`, windowed `finish_bundle` output, windowed `CombinePerKey`, `count_per_element` and global combines, `Flatten` keeps windowing |
| `lifecycle_more` | Call order of a multi-output `DoFn` and of a `StatefulDoFn` (`setup`, `start_bundle`, `process_element`, `finish_bundle`) |
| `side_inputs_more` | Global views read from fixed windows, a struct `DoFn` and a closure that share a view, broadcast joins, windowed and empty multimap views, `combine_globally(..).as_singleton()` of an empty input, a `Flatten` of empty inputs as a view, nested iterable views, a view read by every element, a multi-output `DoFn` with an unread view, side inputs in `StatefulDoFn`s with bag state, a slow side input read from `process_element`, a timer and `on_window_expiration`, bundle finalization in a `DoFn` that reads a view |
| `assertions_more` | `that_singleton(..).in_on_time_pane(..).is_equal_to(..)` per window, `in_window(..).satisfies(..)`, multi-output `ParDo` with empty outputs, `Impulse` emits one empty byte array, a composite that returns its input |
| `state_more` | Two `StatefulDoFn`s with the same state id, a `StatefulDoFn` with tagged outputs, a `MultimapState` whose `keys()` and `entries()` are empty after every key is removed, `OrderedListState::read_range` bounds, a global combine with a default value under a repeated count trigger |
| `timers_more` | A timer after `GroupByKey`, an event-time timer set again in one call and across elements, two timers that set each other, the watermark hold of an event-time and of a processing-time timer keeps a downstream window open, a processing-time timer family, `on_window_expiration` in the global window, pipeline options read in `process_element` and `on_timer`, `.at(..)` on a tagged output before the input timestamp |
| `sdf_more` | Splittable DoFns that read a singleton side input, a windowed side input (also after each checkpoint), with a tagged output, and in lifecycle order |
| `time_bounds` | `Create` outputs carry [`Instant::MIN`](../beam/core/src/time.rs), and timers and `on_window_expiration` fire at [`Instant::END_OF_GLOBAL_WINDOW`](../beam/core/src/time.rs) |
| `nullable_elements` | `Option<T>` elements and KV values through `beam:coder:nullable:v1`, also around a `Row` that the runner length-prefixes |

Triggers, `TestStream`, and file I/O pipelines have dedicated integration test binaries in `sdks/rust/beam/runner-tests/tests/` that run on Prism during `:sdks:rust:test`.

`test_stream_test.rs` also checks:

- late panes in accumulating mode, and a first element that is droppable;
- elements and watermark steps at the end of the global window;
- early panes of processing-time triggers, also accumulating and through a continuation trigger;
- a TestStream into chained stages, and outputs of `finish_bundle` that are never late;
- `RequiresTimeSortedInput` with TestStream input and with droppable data;
- the firing order of event-time timers, and a timer that a late element sets behind the watermark;
- a processing-time timer set again, and late data in a splittable DoFn.

Two tests carry `#[ignore]` to show Go Prism trigger bugs:

- `test_test_stream_pane_info_accumulating`: Prism adds fired elements again in accumulating mode and fires extra count panes.
- `test_trigger_processing_time_continuation`: Prism ignores `OnTimeBehavior::FireIfNonempty` in accumulating mode.
