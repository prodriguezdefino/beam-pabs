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

# Building and testing the Rust SDK

This page gives the build, test, lint, coverage and code-quality tasks for developers
of the SDK. Pipeline authors do not need these tasks. See the [SDK README](../README.md).
[contributing.md](contributing.md) gives the rules for code and documentation, and
[benchmarks/README.md](../benchmarks/README.md) gives the benchmark and profiling tasks.

The workspace needs Rust 1.98 or later (`rust-version` in
[`Cargo.toml`](../Cargo.toml), edition 2024). The repository has no
`rust-toolchain` file. The `beam_rust_builder` image uses `rust:1.98`.

Run builds and tests through Gradle. The rest of the Beam repository uses it too:

```bash
# Core Lifecycle Tasks
./gradlew :sdks:rust:build             # Compile all crates in workspace (cargo build --workspace)
./gradlew :sdks:rust:build -Ppkg=apache-beam-io-gcp -PonlyFeatures=bigtable  # One crate with only these features (also for test)
./gradlew :sdks:rust:check             # Run formatting, clippy and script tests in parallel
./gradlew :sdks:rust:clippyFix         # Apply the machine-applicable lint fixes (cargo clippy --fix)
./gradlew :sdks:rust:clippyLintReport -PclippyLints=match_bool,expect_used  # Count warnings of candidate lints across all targets
./gradlew :sdks:rust:test              # Run all tests on a host Prism (-Ppkg=<crate>, -PtestConcurrency=4)
./gradlew :sdks:rust:test --continue   # Run every test binary and report all failures (cargo --no-fail-fast)
./gradlew :sdks:rust:fmt               # Apply code formatting (cargo fmt)
./gradlew :sdks:rust:clean             # Remove the cargo target directory (cleanSdk) and build/
./gradlew :sdks:rust:coverage          # Line coverage report (or one crate: -Ppkg=apache-beam-core)
./gradlew :sdks:rust:harnessBenchmark -Pbench=row_coder  # One criterion bench (-Ppkg=<crate> for another crate)

# Pipeline Launchers
./gradlew :sdks:rust:prism             # Launch on PrismRunner (-Pexample=wordcount|gaming [-Pdocker])
./gradlew :sdks:rust:dataflow          # Launch on Google Cloud Dataflow (-Pexample=gaming)

# Container & Validations
./gradlew :sdks:rust:docker                    # Build the SDK base container (apache/beam_rust_sdk)
./gradlew :sdks:rust:prebakedImage             # Example image with its binary pre-baked (-Pexample=wordcount [-Pdocker-repository-root=...])
./gradlew :sdks:rust:buildWorker               # Linux worker binary for an example (-Pexample=wordcount)
./gradlew :sdks:rust:buildNexmarkWorker        # Linux worker binary for Nexmark
./gradlew :sdks:rust:expansionServiceImage     # Expansion service image; the binary is also the worker
./gradlew :sdks:rust:validatesRunnerPrism      # ValidatesRunner conformance suite on Prism
./gradlew :sdks:rust:validatesRunnerPrismStreaming     # The same suite on Prism in streaming mode
./gradlew :sdks:rust:validatesRunnerDataflow   # ValidatesRunner conformance suite on Dataflow
./gradlew :sdks:rust:validatesRunnerDataflowStreaming  # The same suite on Dataflow in streaming mode
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonPrism     # Python xlang suite on Prism against the Rust test expansion service
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonDataflow  # Python xlang suite on Dataflow against the Rust test expansion service
./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaPrism       # Java xlang suite on Prism against the Rust test expansion service
./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaDataflow    # Java xlang suite on Dataflow against the Rust test expansion service
./gradlew :sdks:rust:validatesCrossLanguageRunnerGoPrism         # Go xlang suite on Prism against the Rust test expansion service
./gradlew :sdks:rust:validatesCrossLanguageRunnerGoDataflow      # Go xlang suite on Dataflow against the Rust test expansion service
./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaServicePrism  # Rust xlang suite on Prism against the Java test expansion service
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonServicePrism # Rust xlang suite on Prism against the Python test expansion service
./gradlew :sdks:rust:gcpExpansionServiceImage    # Expansion service image of the native Google Cloud connectors
./gradlew :sdks:rust:gcpExpansionServiceDataflowImage  # The same image for linux/amd64, pushed for Dataflow (used by bigQueryBenchmark)
./gradlew :sdks:rust:gcpExpansionServiceTestingImage   # GCP expansion service image for its Python tests on Prism
./gradlew :sdks:rust:gcpExpansionServiceTestingDataflowImage  # The same image for linux/amd64, pushed for Dataflow
./gradlew :sdks:rust:bigQueryIntegrationTest     # Native BigQuery reads against public tables on Prism
./gradlew :sdks:rust:gcpExpansionServicePythonTest  # Python pipelines on Prism against the GCP expansion service
./gradlew :sdks:rust:gcpExpansionServicePythonTestDataflow  # The same Python pipelines on Dataflow ([-Ptests=bigquery_storage] [-PtestFilter=unbounded])
./gradlew :sdks:rust:kafkaExpansionServiceImage  # Expansion service image of the Kafka connector of the Rust SDK
./gradlew :sdks:rust:kafkaExpansionServiceDataflowImage # The same image for linux/amd64, pushed for Dataflow
./gradlew :sdks:rust:kafkaExpansionServiceTestingImage  # Kafka expansion service image for its Python tests on Prism
./gradlew :sdks:rust:kafkaExpansionServicePythonTest  # Python pipelines on Prism against the Kafka expansion service and a local broker in Docker

# Tasks that other tasks use
./gradlew :sdks:rust:buildWorkspace            # cargo build of the workspace, for `build`
./gradlew :sdks:rust:fmtCheck                  # Format check, for `check`
./gradlew :sdks:rust:compileWorkspaceTests     # All test binaries in one cargo pass, for `test`
./gradlew :sdks:rust:test_<pkg>                # The tests of one crate, for `test`
./gradlew :sdks:rust:testSummary               # The summary table after `test`
./gradlew :sdks:rust:fetchPrism                # Download the host Prism binary (fetchPrismLinux: the Linux one)
./gradlew :sdks:rust:setupXlangPythonEnv       # Python virtualenv for the Python xlang suite
./gradlew :sdks:rust:pythonSdkDataflowImage    # Python SDK image for the Python xlang suite on Dataflow
./gradlew :sdks:rust:testingExpansionServiceImage          # Test expansion service image of the xlang suites
./gradlew :sdks:rust:testingExpansionServiceDataflowImage  # The same image for linux/amd64, pushed for Dataflow
./gradlew :sdks:rust:javaSdkDataflowImage        # Java SDK container of this tree for Dataflow
./gradlew :sdks:rust:goSdkDataflowImage          # Go SDK container of this tree for Dataflow
```

Start each ValidatesRunner suite in its own Gradle command, so that the suites run in
parallel. One command runs its tasks one after the other. `check` also runs
`pythonScriptsTest`, the unit tests of the Python scripts.

## Containerized builds

Several tasks run inside the `beam_rust_builder` image, so their results do
not depend on the host toolchain. Gradle builds the image from
[`container/Dockerfile.builder`](../container/Dockerfile.builder) the first
time that a task needs it. To build it on demand, run:

```bash
./gradlew :sdks:rust:buildBuilderImage  # Build beam_rust_builder:latest
```

The code-quality tasks below always use the image. The Linux binary tasks
use it when the host is not Linux on the target architecture. These tasks are
`buildWorker`, `buildRunnerTestsWorker`, the expansion service builds and
`:sdks:rust:container:docker`. To use a different builder image, set `-PrustBuilderImage=<image>`.

The `prism` task takes `-Pdocker`. With it, the workers run in the SDK base
image with `--environment_type=DOCKER`, and the task builds the Linux worker
binary first. Without it, the workers run in the launching process with
`--environment_type=LOOPBACK`.

## Incremental builds

These tasks write a stamp file: `buildWorkspace`, `fmtCheck`, `clippy`,
`pythonScriptsTest`, `compileWorkspaceTests` and each `test_<pkg>`. When their
inputs did not change, they report `UP-TO-DATE` (or `FROM-CACHE`). The inputs
are the sources of the crate and of the crates that it uses, `Cargo.toml`,
`Cargo.lock`, the model protos and the SDK version. `fmt` always runs cargo.

`test` without `-Ppkg` builds all test binaries in one
`cargo test --workspace --no-run`, so cargo unifies the features of all crates.
With `-Ppkg=<crate>`, cargo resolves the features of that crate only. The two
modes keep separate stamps, so one mode never makes the other `UP-TO-DATE`.

The Linux binary tasks track the workspace sources (`**/*.rs`, `Cargo.toml`,
`Cargo.lock`) and their properties (example, architecture, SDK version,
`-PworkerTargetCpu`). If these did not change, the task reports `UP-TO-DATE`
and does not call cargo.

## Code Coverage

The `coverage` task measures line coverage with
[`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) inside the
`beam_rust_builder` container. The tests that start a pipeline use the Linux
Prism:

```bash
./gradlew :sdks:rust:coverage                          # whole workspace
./gradlew :sdks:rust:coverage -Ppkg=apache-beam-core   # one crate
./gradlew :sdks:rust:coverage -Ppkg=apache-beam-ml -Pfeatures=remote
```

The reports are in `build/reports/coverage/<pkg|workspace>/`: `summary.txt`,
`lcov.info` and `html/index.html`. The task caches the instrumented build in
`build/cov-target`, and `coveragePerTest` also uses it. For the coverage of each
test of one crate, use `coveragePerTest` (below).

## Code quality

[gradle/quality.gradle](../gradle/quality.gradle) defines the code-quality
tasks. Their scripts are in [scripts/quality/](../scripts/quality/README.md).
All these tasks run in the `beam_rust_builder` container. They are in the
`Verification` task group.

### Test quality (per-test coverage, mutation testing)

These tasks measure the contribution of each test. Use them to find redundant
tests (merge or remove them). Also use them to find code that tests run but do
not check. [scripts/quality/README.md](../scripts/quality/README.md) gives the
method, the candidate rule and the caveats.

**Collection = container only; analysis = anywhere.** Per-test coverage and
cargo-mutants run only inside the `beam_rust_builder` container. They have no
host mode, and `-Pdocker=false` does not apply. The analysis scripts use only
the Python standard library. They run in the same container invocation. You
can also run them again on the host against the outputs.
The builder image sets `BEAM_RUST_BUILDER=1`. If your image does not set this
marker, rebuild it with `./gradlew :sdks:rust:buildBuilderImage`.

```bash
# Per-test coverage of one crate: every test runs alone with its own profile.
./gradlew :sdks:rust:coveragePerTest -Ppkg=apache-beam-core
./gradlew :sdks:rust:coveragePerTest -Ppkg=apache-beam-core -Pfocus=/coders/  # plus a focused report

# Mutation testing, with the kill matrix (which tests catch which mutant).
./gradlew :sdks:rust:mutants -Ppkg=apache-beam-core -Pfiles='beam/core/src/coders/**' -Pexclude='**/display.rs'
./gradlew :sdks:rust:mutants -Ppkg=apache-beam-core -Pfiles='beam/core/src/coders/pane.rs' -Ptag=pane
./gradlew :sdks:rust:mutants -Ppkg=apache-beam-core -Pfiles='beam/core/src/coders/**' -PlistOnly  # list, don't run

# Only the code a change touches (what the PreCommit does); -Ppkg is optional here.
git diff origin/master...HEAD -- sdks/rust > /tmp/rust.diff
./gradlew :sdks:rust:mutants -PinDiff=/tmp/rust.diff

# Remove/merge candidates, from the outputs above.
./gradlew :sdks:rust:testQualityReport -Ppkg=apache-beam-core \
    -PtestFileRegex='^(coder_|pane_info|element_metadata|param_windowed_value)'
```

| Property | Task | Meaning |
|---|---|---|
| `-Ppkg` | `coverage`, `coveragePerTest`, `mutants`, `testQualityReport` | Cargo package. Optional for `coverage` (default: the workspace) and for `mutants` with `-PinDiff` (then the diff filters the whole workspace). |
| `-Pfeatures` | `coverage`, `coveragePerTest`, `mutants` | Comma-separated cargo features of the crate. |
| `-Pcpus` | all | Limit the CPU share of the container (`docker run --cpus`). |
| `-Psrc` | `coveragePerTest` | Source dir kept in the lcov output; defaults to the crate's `src/`. |
| `-Pfocus` | `coveragePerTest` | Path substring for an extra focused report, e.g. `/coders/`. |
| `-Pfiles`, `-Pexclude` | `mutants` | Comma-separated globs, relative to `sdks/rust`. |
| `-PinDiff` | `mutants` | A unified diff. The task tests only the mutants in changed lines. |
| `-Pjobs` (4), `-Ptimeout` (60 s) | `mutants` | Parallel cargo-mutants jobs; per-test-run timeout. |
| `-Ptag` | `mutants` | Write to `mutants-<tag>/` so runs over different files coexist. |
| `-PlistOnly` | `mutants` | List the mutants without testing them. |
| `-PfailOnMissed` | `mutants` | Fail the task if any mutant is missed or times out (default: report only). |
| `-PexpansionServiceVersion` | `mutants` | Released Beam version of the expansion service JARs. The cargo-mutants copy of the tree has no locally built JARs. |
| `-PtestFileRegex` | `testQualityReport` | The tests (`<test file>::<test>`) that the report classifies and that can subsume others. |
| `-PmutantsDirs` | `testQualityReport` | Extra cargo-mutants output dirs (inside the repository). |

The outputs are under `build/reports/test-quality/<pkg>/`. A diff-only
`mutants` run uses `workspace/` in place of `<pkg>/`:

| Path | Content |
|---|---|
| `coverage/tests.tsv` | Every test with pass/fail and wall time. |
| `coverage/overlap_report.txt` | Union coverage, hits per line, identical groups, zero-unique and subsumed tests, greedy minimal cover, per test file and per source file. |
| `coverage/redundancy_all.tsv` | One row per test: covered and unique lines, subsumed by, in minimal cover. |
| `mutants/mutants.out/` | Raw cargo-mutants output (`missed.txt`, `caught.txt`, logs). |
| `mutants/kill_matrix.txt` | Mutation score, killers per mutant, sole killers, minimal killing set, missed mutants. |
| `mutants/kill_matrix.tsv` | One row per mutant: outcome and the tests that caught it. |
| `candidates.txt`, `candidates.tsv` | Category of each in-scope test, with what subsumes it. |

How to read the outputs:

- **Missed mutants** are behavior that no test asserts. Some missed mutants
  are *equivalent*: they cannot change behavior (e.g. a zero-sized `Default`).
  The other missed mutants are test gaps or code that nothing uses.
- A **REMOVE/MERGE candidate** has no unique covered line. It is not the only
  test that catches a mutant. It catches a subset of the mutants that another
  in-scope test catches. Review each candidate before you act. Usually, several
  candidates become one table-driven test.
- **keep (sole killer)** tests can look redundant by coverage. But they are the
  only check on some behavior. **review** tests catch no mutant in the mutated
  files. They probably target other code.
- Coverage is crate-local, and mutants cover only the files that you mutated.
  So scope `-PtestFileRegex` to the tests that target those files.

Runtime: most of the `coveragePerTest` time is the instrumented build (cached
in `build/cov-target`) and one `llvm-cov export` per test. `tests.tsv` records
the wall time of each test.
`mutants` takes about 25 s per mutant per job, with `mold` and debug info off.
The task sets both. 600 mutants at `-Pjobs=4` take about one hour.
Each job builds its own copy of the workspace. For `-Pjobs=4`, give Colima or
Docker Desktop at least 8 CPUs and about 16 GB, or decrease `-Pjobs`. The
`beam-cargo-registry` Docker volume keeps the cargo registry between runs.

### Size and complexity (cross-SDK scorecard)

This task makes a scorecard that compares the Rust, Go and Python SDKs area by
area. The scorecard contains:

- code lines and lines per feature point (features from [sdk-parity.md](sdk-parity.md))
- cyclomatic complexity per function
- test lines and test/prod ratios
- the same example in each SDK
- legacy markers and Rust over-abstraction signals

It measures production code only and runs in seconds.

```bash
./gradlew :sdks:rust:complexityScorecard               # -> build/reports/quality/scorecard.md and .json
./gradlew :sdks:rust:complexityScorecard -PwithJavaTs  # also Java and TypeScript in the whole-tree test totals
```

The task uses the `scc` and `lizard` versions that the builder image pins. An
image without these tools fails the marker check. Rebuild it with
`./gradlew :sdks:rust:buildBuilderImage`.

[scripts/quality/README.md](../scripts/quality/README.md#complexity-scorecard)
tells what the scorecard counts and how the areas map to the paths of each SDK.

### Unused dependencies

Check all workspace manifests for unused dependencies with `cargo-machete` in
the builder container:

```bash
./gradlew :sdks:rust:machete
```
