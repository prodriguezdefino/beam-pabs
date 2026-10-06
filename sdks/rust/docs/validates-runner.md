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

## 1. Purpose

`ValidatesRunner` is the conformance test suite for Apache Beam SDKs and runners. It checks three areas:

- **Core semantics**: Element-wise transforms (`Map`, `FlatMap`, `Filter`), `GroupByKey`, `Combine`, `CoGroupByKey`, windowing, state and timers, and splittable DoFns. Triggers are tested separately (see the end of this page).
- **Worker execution**: User functions and standard coders run across the Fn API boundary without data loss or corruption.
- **Failure detection**: When an in-pipeline assertion (`passert`) fails on a worker, the failure stops the bundle and fails the job. The registered suite checks that assertions run and pass; [`tests/passert_test.rs`](../beam/runner-tests/tests/passert_test.rs) checks the failure path on Prism with `FailsWith` expectations.

---

## 2. Architecture

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
- `build_group_by_key`, `build_combine_per_key`, `build_combine_max_min`, `build_combine_flushes_accumulators_at_capacity`, `build_count_per_element`, `build_combine_globally`, `build_count_globally`, `build_folds`
- `build_flatten_many`, `build_flatten_singleton_list`, `build_flatten_then_pardo`, `build_flatten_multiple_copies`, `build_create_empty`, `build_kv_swap`, `build_reshuffle`, `build_partition`, `build_partition_many`
- `build_generate_sequence`, `build_periodic_impulse`
- `build_cogroup_by_key`, `build_joins`, `build_join_cross_product`, `build_row_schema_coder`
- `build_side_input_views`, `build_empty_iterable_side_input`, `build_windowed_side_input`
- `build_stateful_pardo`, `build_windowed_stateful_pardo`, `build_map_and_set_state`, `build_map_and_set_state_clear`, `build_bundle_lifecycle_batching`
- `build_windowed_group_by_key`, `build_sliding_windows_pardo`, `build_window_sums_gbk`, `build_window_sums_lifted`, `build_rewindow_preserves_multiplicity`
- `build_passert_success`

The same directory also has builders that the registry does not list, for example the trigger pipelines that [`tests/test_stream_test.rs`](../beam/runner-tests/tests/test_stream_test.rs) runs and the failing pipelines of `passert_test.rs`.

Each test pairs a builder with an `Expectation`:

| `Expectation` | Requirement |
|---|---|
| `Succeeds` | The job succeeds, and every registered assertion runs and passes. |
| `FailsWith(&[fragments])` | The job fails with an error message that contains every fragment. |
| `Custom(fn)` | The `PipelineResult` satisfies the custom check function. |

`run_pipeline(id, build, expect, runner)` constructs a `TestPipeline`, runs it on `runner`, and checks the `Expectation`.

### Central Test Registry and Worker Reconstruction

[`sdks/rust/beam/runner-tests/src/registry.rs`](../beam/runner-tests/src/registry.rs) defines the 42 conformance tests once in the `validates_runner_suite!` macro across 7 suites (`core_transforms`, `joins`, `schemas`, `side_inputs`, `state_and_timers`, `windowing`, `assertions`). Each entry is a `ValidatesRunnerTest { suite, id, build, expect }`:

- [`tests/validates_runner_prism.rs`](../beam/runner-tests/tests/validates_runner_prism.rs) and [`tests/validates_runner_dataflow.rs`](../beam/runner-tests/tests/validates_runner_dataflow.rs) expand the macro into `#[tokio::test]` functions named `<suite>::<id>`.
- `VALIDATES_RUNNER_TESTS` collects all entries into a static slice used by the worker binary (`beam-runner-tests-worker`).

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

All four tasks accept `-Pfilter=<substring>` (passed to `cargo test`; `-Ptest=<substring>` also works) and `-PbatchSize=<n>` (`--test-threads`; also `-PtestThreads`, `-Pconcurrency` or `RUST_TEST_THREADS`). The Dataflow tasks default to `4` threads to stay within regional worker quota. The Prism tasks use the cargo default.

Dataflow tests carry `#[ignore]`, so `cargo test` and `:sdks:rust:test` do not submit cloud jobs. The Dataflow Gradle tasks pass `--ignored` and depend on `buildRunnerTestsWorker`, which builds the linux worker binary `beam-runner-tests-worker` (in the builder container unless the host is Linux on the same architecture). They read `gcpProject`, `gcpTempLocation`, and `sdkContainerImage` (required), and `gcpRegion` (default `us-central1`), `network`, `subnetwork` and `workerBinary` (optional) from `sdks/rust/.local/sdk.properties` or `-P` flags, and pass them to the tests as `BEAM_DATAFLOW_*` variables:

```bash
./gradlew :sdks:rust:validatesRunnerPrism
./gradlew :sdks:rust:validatesRunnerPrismStreaming
./gradlew :sdks:rust:validatesRunnerDataflow -PbatchSize=4
./gradlew :sdks:rust:validatesRunnerDataflowStreaming -PbatchSize=4
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
- `-Ptests` takes test method names without the `test_` prefix. The default runs the whole `ValidateRunnerXlangTest` class.
- The tasks need Docker. If the Docker API is not on the default socket (for example with Colima), the tasks take the socket from the current Docker context.

The Dataflow task also builds a linux/amd64 worker image and pushes it to the registry in `dataflowRepositoryRoot`, for example `us-central1-docker.pkg.dev/<project>/<repository>`. Each test is one Dataflow job, and `-PxlangParallelism` (default `8`) sets how many run at the same time. The Python transforms run in the Python SDK container of this tree, with the SDK staged from `sdks/python/build/apache-beam.tar.gz`. The task reads these properties from `sdks/rust/.local/sdk.properties` (with the `example.xlang.` prefix or without it) or from `-P` flags:

| Property | Meaning |
|---|---|
| `dataflowRepositoryRoot` | Registry path of the worker images. Required. |
| `gcpProject`, `gcpTempLocation` | Project and temp location of the jobs. Required. |
| `gcpRegion` | Region of the jobs. Defaults to `us-central1`. |
| `network`, `subnetwork`, `workerMachineType` | Optional worker settings. |
| `pythonSdkImage` | Python SDK container that already holds the SDK of this tree. Optional. |

The SDK base image must be in that registry for linux/amd64 first:

```bash
./gradlew :sdks:rust:container:docker -Pcontainer-architecture-list=amd64 \
  -Pdocker-repository-root=<dataflowRepositoryRoot> -Ppush-containers
```

Without `pythonSdkImage`, each worker builds the staged SDK into a wheel before the Python worker starts, which adds about five minutes to each job. `pythonSdkDataflowImage` builds the Python SDK container of this tree for linux/amd64, pushes it to `dataflowRepositoryRoot` and prints the value for `pythonSdkImage`. The task then sets `--sdk_container_image` and `--sdk_location=container`. Build the image again after changes in `sdks/python`, and use the same `-PpythonVersion` for the image and the suite:

```bash
./gradlew :sdks:rust:pythonSdkDataflowImage -PpythonVersion=3.12
```

---

## 3. Conformance Results

| Runner | Batch | Streaming | Notes |
|---|:---:|:---:|---|
| Prism | 41 passed, 1 ignored | 41 passed, 1 ignored | `state_and_timers::map_and_set_state_clear` is skipped on Prism (see below). |
| Dataflow Runner V2 | 42 / 42 passed | 42 / 42 passed | Streaming jobs cancel automatically when all assertions pass. |

### Why `map_and_set_state_clear` is Skipped on Prism

`state_and_timers::map_and_set_state_clear` tests clearing `MapState` and `SetState` from an event-time `on_timer` callback across three separate bundles (seed in `process_element`, clear in a timer bundle at `t = 100`, verify in a timer bundle at `t = 200`).

On the Rust worker, `MapState::clear()` and `SetState::clear()` issue a `StateCleared` mutation against `StateKey::MultimapKeysUserState` over the Fn API State channel. In Prism's in-memory state engine (`TentativeData.clearState` in `sdks/go/pkg/beam/runners/prism/internal/engine/data.go`), a `MultimapKeysUserState` clear is recorded only if `b.state` for that key is already initialized in the current bundle's tentative write map. When a timer-only bundle calls `clear()` without first writing to that map or set in the same bundle, `b.state` is `nil`, Prism drops the clear, and the next bundle still reads the pre-clear entries.

On Google Cloud Dataflow Runner V2, the Windmill state backend commits `MultimapKeysUserState` clear requests regardless of whether the bundle wrote to the cell first, so `state_and_timers::map_and_set_state_clear` passes in both Batch and Streaming modes.

### Coverage by Suite

| Suite | Capabilities Verified |
|---|---|
| `core_transforms` | `Map`, `FlatMap`, `Filter`, `DoFn` lifecycle order, diamond DAGs, multi-output `ParDo`, `GroupByKey`, `CombinePerKey` (with combiner lifting and accumulator flushes), `Max` / `Min`, `Count` per element and globally, `CombineGlobally`, folds, `Flatten` (many inputs, one input, the same input twice), empty `Create`, KV swap, `Reshuffle` (keyed, unkeyed, timestamps kept), `Partition`, `GenerateSequence` and `PeriodicImpulse` (splittable DoFns) |
| `joins` | `CoGroupByKey`, `inner_join`, `left_join`, `right_join`, `full_outer_join`, join cross product |
| `schemas` | `#[derive(BeamRow)]` elements (string, integer, list and boolean fields) through `GroupByKey` and `Reshuffle` with the row coder |
| `side_inputs` | Singleton, iterable and multimap views, empty iterable side input, windowed side inputs |
| `state_and_timers` | `ValueState`, `BagState`, `MapState`, `SetState`, state clear from a timer, event-time timers, per-window state and timer isolation under `SlidingWindows`, bundle lifecycle batching |
| `windowing` | `FixedWindows`, `SlidingWindows`, `Sessions`, per-window `DoFn` explosion, lifted and unlifted window sums, re-windowing multiplicity |
| `assertions` | `passert` assertions (`contains_in_any_order`, `contains`, `has_count`, `not_empty`, `all`, `empty`, `that_singleton`) that must all run and pass |

Triggers, `TestStream`, and file I/O pipelines have dedicated integration test binaries in `sdks/rust/beam/runner-tests/tests/` that run on Prism during `:sdks:rust:test`.
