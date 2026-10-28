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

# Developing the Rust SDK

This page gives the build, test, lint and coverage tasks for developers of the
SDK itself. Pipeline authors do not need these tasks. See the [SDK README](../README.md).

The workspace needs Rust 1.98 or later (`rust-version` in
[`Cargo.toml`](../Cargo.toml), edition 2024). The repository has no
`rust-toolchain` file. The `beam_rust_builder` image uses `rust:1.98`.

Run builds and tests through Gradle. The rest of the Beam repository uses it too:

```bash
# Core Lifecycle Tasks
./gradlew :sdks:rust:build             # Compile all crates in workspace (cargo build --workspace)
./gradlew :sdks:rust:check             # Run formatting checks and clippy (fmtCheck + clippy)
./gradlew :sdks:rust:test              # Run all tests on a host Prism (or one crate: -Ppkg=apache-beam-core)
./gradlew :sdks:rust:test --continue   # Run every test binary and report all failures (cargo --no-fail-fast)
./gradlew :sdks:rust:fmt               # Apply code formatting (cargo fmt)
./gradlew :sdks:rust:clean             # Remove the cargo target directory (cleanSdk) and build/
./gradlew :sdks:rust:coverage          # Line coverage report (or one crate: -Ppkg=apache-beam-core)

# Pipeline Launchers
./gradlew :sdks:rust:prism             # Launch on PrismRunner (-Pexample=wordcount|gaming [-Pdocker])
./gradlew :sdks:rust:dataflow          # Launch on Google Cloud Dataflow (-Pexample=gaming)
./gradlew :sdks:rust:profilePrism      # Profile locally on Prism (-Pexample=wordcount [-Pmode=cpu|memory|both] ...)

# Container & Validations
./gradlew :sdks:rust:docker                    # Build the SDK base container (apache/beam_rust_sdk)
./gradlew :sdks:rust:prebakedImage             # Example image with its binary pre-baked (-Pexample=wordcount [-Pdocker-repository-root=...])
./gradlew :sdks:rust:buildWorker               # Linux worker binary for an example (-Pexample=wordcount)
./gradlew :sdks:rust:expansionServiceImage     # Expansion service image; the binary is also the worker
./gradlew :sdks:rust:validatesRunnerPrism      # ValidatesRunner conformance suite on Prism
./gradlew :sdks:rust:validatesRunnerDataflow   # ValidatesRunner conformance suite on Dataflow
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonPrism     # Python xlang suite on Prism against the Rust test expansion service
./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonDataflow  # Python xlang suite on Dataflow against the Rust test expansion service
```

## Profiling

The `profilePrism` task runs a pipeline on the local Prism runner and records a CPU
profile, a memory profile, or both. It runs on the host, so `-Pdocker` does not apply.
CPU profiles need `samply` (`cargo install --locked samply`). The task writes the profile,
the `time` output and a summary to `build/reports/profile/<binary>-<timestamp>/`.

```bash
./gradlew :sdks:rust:profilePrism -Pexample=wordcount -PextraArgs="--input=... --output=..." -Prows=5000000
```

- `-Pexample=<name>` runs an example. `-Ppkg=<crate> -Pbin=<binary>` runs another
  binary of the workspace.
- `-Ppkg=<crate> -PcargoExample=<name>` runs a Cargo example of a crate (a file in its
  `examples` directory), for example a program that profiles one connector.
- `-Ppkg=<crate> -PcargoBench=<name>` runs a Cargo bench of a crate (`harness = false`), for
  example `-Ppkg=apache-beam-harness -PcargoBench=chain_throughput`. A criterion bench gets
  `--bench --profile-time 10` and `-PextraArgs`, not the pipeline flags. It runs for a fixed
  time, so compare its hot frames, not its CPU seconds.
- `-Pmode=cpu` records with samply, `memory` runs `/usr/bin/time` (`-l` on macOS, `-v` on
  Linux), and `both` (the default) does both. With `both`, the pipeline runs two times.
- `-PextraArgs="<pipeline flags>"` passes flags to the pipeline.
- `-Prows=<n>` adds the CPU seconds for each 1M rows to the summary.
- `-PcargoProfile=worker` (the default) builds with the `worker` Cargo profile: fat LTO and
  one codegen unit, as the binaries that run on a runner. The inlining then matches the
  fleet. `-PcargoProfile=release` links faster, but its inlining differs, so use it only to
  find hot functions, not to compare two builds.

Compare two builds only with profiles from the same session: run them one after the other,
three times each. Results from different sessions differ by more than most code changes.

The summary splits the CPU time between the Rust process and Prism, and gives the CPU
share of each stage and the hottest functions of the Rust process. When samply records the
CPU time of each thread, each sample counts its CPU time. A thread that waits in a system
call, for example in `poll()`, then counts as idle. To open the profile in the Firefox
profiler, run `samply load <dir>/profile.json.gz`. Keep `profile.json.syms.json` in the same
directory: it holds the function names.

## Benchmarks

[gradle/benchmarks.gradle](../gradle/benchmarks.gradle) defines the benchmarks. Each runs
some rounds (`-Pruns`, default 3), one after the other, and writes `results.json` and
`report.md` to `build/reports/benchmarks/<task>-<timestamp>/`. The report gives each run and
the medians. The scripts are in [scripts/benchmarks/](../scripts/benchmarks/).

A benchmark runs the current checkout. With `-Pbaseline=<git ref>`, it also runs that ref
from a git worktree at `build/benchmarks/baseline` and gives the change against it. The
worktree gets a copy of `.local/`. It stays between runs (about 10 GB with its builds), so the
next baseline builds only what changed. To get the disk space back, delete it and run
`git worktree prune`. `-PcompareTo=<results.json>` also compares
with an earlier result. `-PdryRun` prints the commands and runs nothing.

```bash
# Wordcount on Dataflow, 100 or 300 Wikipedia shards.
./gradlew :sdks:rust:wordcountBenchmark -Pshards=100
./gradlew :sdks:rust:wordcountBenchmark -Pbaseline=origin/master -Pruns=3

# A Rust example or bench on Prism with profilePrism (worker profile, CPU mode).
gsutil cp gs://apache-beam-samples/wikipedia_edits/wiki_data-000000000000.json build/bench-input/
./gradlew :sdks:rust:profileCompare -Pinput='build/bench-input/*.json' -Pbaseline=HEAD~1
./gradlew :sdks:rust:profileCompare -Ppkg=apache-beam-harness -PcargoBench=chain_throughput
```

- A Dataflow benchmark first builds the worker and the launcher of each variant, so the jobs
  of a round start together. The report flags a round whose jobs started more than 60 s
  apart. It takes `-PnumWorkers` (1), `-PmaxNumWorkers` (10), `-PworkerMachineType`
  (`n2-standard-4`), `-PminCpuPlatform`, `-PextraArgs` and the GCP properties of the
  `dataflow` task. It finds each job by its name and gives it the labels `benchmark`,
  `variant`, `round` and `run_id`. `-PprepareOnly` runs only the builds, without jobs.
- The report gives the vCPU·s, the counted elements per vCPU·s, memory, PD, shuffle, the
  ProcessElement time of each step, the peak memory of each container, peak workers, CPU
  platform, warnings, and the start and end times. Cloud Monitoring gives the used vCPU·s,
  the container memory and the counter rate. It can be some minutes late, so the script
  waits up to 6 minutes for it, then marks the missing values. `-PmonitoringWait=<seconds>`
  changes the wait.
- The total vCPU·s includes worker startup and idle autoscaled workers. The used vCPU·s does
  not. To compare the code of two builds, use the step CPU seconds per 1M elements.
- The CPU platform of the workers changes the results by about 20% (for example, Cascade
  Lake or Ice Lake on `n2`). The report flags a round whose jobs ran on different platforms,
  a job that autoscaled beyond 1 worker, and stockout or quota warnings. Do not pin a
  platform with `-PminCpuPlatform` unless necessary: a pinned platform can stock out.
- To add a Dataflow benchmark of a Rust example, call `registerRustBenchmark` (see
  `wordcountBenchmark`). For other launchers, give launch commands to `runDataflowBenchmark`.
- `profileCompare` takes `-Pexample` (`wordcount`) or the target properties of `profilePrism`.
  It alternates the order of the variants in each round. `profilePrism` builds before it
  records, so the profile does not include the build. The report gives the Rust and Prism
  CPU seconds, the Rust CPU seconds per 1M rows and the hottest frames. `-Pinput` gives the
  pipeline `--input` and `--output`. `-Prows` sets the rows; the default is the lines of the
  input.
- `check` runs `pythonScriptsTest`, the unit tests of the Python scripts.

## Public API Changes

Every public API change follows these rules:

- **No aliases or compatibility shims.** A rename removes the old name. The same
  change updates every caller, example and doc.
- **One public path per item.** Pipeline items are in their module
  (`transforms`, `transforms::sdf`, `windowing`, `coders`, ...). Runner and
  harness plumbing (`BundleHandler`, `ElementSink`, `HandlerContext`, ...) is
  only in `beam::internals`. The preludes are the only additional path.
- **Fluent methods mirror core transforms.** Each method is the snake_case name
  of exactly one core transform (`.group_by_key` → `GroupByKey`). It adds no
  semantics. Documented exceptions:
  - `map_with_side_inputs`, a `ParDo` that maps each element with side input
    values.
  - One `*_globally` and one `*_per_key` method for each SDK combine function
    (`.sum_per_key` → `CombinePerKey` with `Sum`). A method returns `Result`
    when the constructor of its combine function does (`approximate_*`).
  - One method for each set operation (`.intersect_all` → `Sets::intersect_all`).
- **Fluent covers common cases.** For advanced configuration, use
  `.apply(name, Transform::new(..))`, which also chains. Do not add fluent variants
  for advanced configuration.
- **Construction.** Build transforms with `new(required..)` and `.with_*()`
  builders. The name of a transform is given at `apply(name, transform)`, not to
  the constructor. Value types (e.g. `FixedWindows::of`) are exempt.
- **Errors.** User hooks (`DoFn`, `CombineFn`, state, timers) return `beam::Result`.
- **Prelude.** `beam::prelude` contains only the items that a typical pipeline author uses.

## Programming style

Write functional, data-oriented, idiomatic Rust that follows current practice.

- **Functional.** Prefer pure functions and immutable bindings. Use iterator
  chains (`map`, `filter`, `fold`) and expressions (`match`, `if let`) instead of
  loops that change local state. Keep side effects at the edges of a module.
- **Data-oriented.** Model the domain with plain data types first, then write
  functions that transform them. Use `enum`s for closed sets of states, so that
  invalid states cannot be represented. Keep data in contiguous collections and
  avoid pointer-heavy object graphs and deep trait hierarchies.
- **Pattern matching.** Use `match` to branch on the shape of data, not chains
  of `if` with type or flag checks. Match exhaustively and do not add a `_` arm
  to an `enum` that the crate owns, so that a new variant is a compile error.
  Destructure structs, tuples and slices in `let`, function parameters and match
  arms instead of reading fields one at a time. Use guards (`if`), bindings
  (`name @ ..`) and or-patterns (`A | B`) to keep each case in one arm. Use
  `if let` or `let`-`else` when only one case is important.
- **No clones, no copies.** Borrow when you can and move when you must. Do not
  `clone` to satisfy the borrow checker; change the ownership design instead.
  Pass `&str`, `&[T]` and `impl Iterator` instead of `String`, `Vec<T>` or
  collected copies. Use `Cow` when a value is only sometimes changed, and
  `bytes::Bytes` or slices to share buffers without copying them. Reuse buffers
  in hot paths instead of allocating per element. A `clone` in a hot path needs
  a comment that says why it is necessary.
- **Avoid locks.** Design so that data has one owner: move it between tasks
  through channels, or split it so that each worker owns its part. Prefer
  immutable shared data (`Arc<T>` without a lock) and atomics over `Mutex` and
  `RwLock`. If a lock is necessary, keep the critical section short, never hold
  it across `.await` or I/O, and do not nest locks.
- **Errors as values.** Return `Result` or `Option` and propagate with `?`. Do
  not call `unwrap`, `expect` or `panic!` outside tests, unless an invariant makes
  the failure impossible; state the invariant in the `expect` message.
  `PTransform::expand` cannot return an error: record a configuration error with
  `Pipeline::record_construction_error` (then `validate` fails), and implement
  `TryPTransform` for an expansion that can fail outside the program, such as a
  cross-language expansion, so users get the error from `try_apply`.
- **Types carry meaning.** Use newtypes for identifiers and units. Use
  `From`/`TryFrom`, `Default`, `Display` and other standard traits instead of
  custom conversion methods.
- **Small generic surface.** Prefer `impl Trait` and generics over `dyn Trait`
  unless type erasure is necessary. Keep trait bounds at the point of use.
- **Current Rust.** Use the workspace edition and its idioms: `let`-`else`,
  `async fn` in traits where it fits, `std::sync::LazyLock` instead of
  `lazy_static`. Code must pass `cargo fmt` and `cargo clippy` with no warnings.
- **No `unsafe`** without a `// SAFETY:` comment that states why the code is
  sound.
- **Test placement.** Test a transform or combine function that is built from
  other transforms with unit tests in its crate and with Prism tests in
  `runner-tests/tests`. Add a ValidatesRunner test only for a feature that a
  runner must support: a primitive transform, combine lifting, state and timers,
  windowing, or a coder that crosses the Fn API.

## Comments and documentation

Comments and rustdoc follow the writing rules of
[ASD-STE100 Simplified Technical English](https://www.asd-ste100.org/about_STE.html).
Beam, Rust and protocol terms are permitted technical names.

- **Short sentences.** At most 20 words for an instruction and 25 words for a
  description. One topic per sentence.
- **Active voice.** Write instructions in the imperative: "Call `finish` before
  `drop`", "Do not share the sink".
- **One word, one meaning.** Use the same term for the same thing in all files.
- **Say what the code cannot.** Write a comment for a constraint, a contract or a
  reason that is not clear from the code. Do not repeat what the code does.
- **No numbered steps.** If the order is important, say so in words or split the
  code into functions.
- **No history.** Do not write "used to", "now", "previously", "no longer" or
  "Regression:". Write the rule that is true for the current code. A test
  comment states the behavior that the test checks.
- **This SDK only.** Do not compare with other SDKs ("like Java", "same as Go",
  "the equivalent of"). Name another SDK only when the behavior depends on it:
  a cross-language transform that the code calls, or a wire format, URN or
  protocol rule that other SDKs must decode. Comparisons between SDKs go in
  the [RFC](../README.md) and [`sdk-parity.md`](sdk-parity.md).
- **Plain words.** Use the simplest word that is correct. Do not use: thus, hence,
  therefore, obey, ensure, utilize, leverage, facilitate, via, prior to,
  subsequent, terminate, approximately, sufficient, robust, seamless,
  comprehensive, crucial, essential, simply, merely, basically, actually,
  effectively, notably, furthermore, moreover, additionally, "in order to",
  "note that", "stem from". Use so, follow, make sure, use, help, through,
  before, then, stop, about, enough, also, to.
- **Short.** A comment is shorter than the code it explains. Fill lines to about
  90 columns: one short sentence does not need its own line.
- **Rustdoc.** Start with a one-line summary. Do not repeat the item name
  ("Returns the key." on `fn key`). Add `# Errors`, `# Panics` or `# Safety` only
  when the condition is not clear from the signature and the summary.

## Containerized builds

Several tasks run inside the `beam_rust_builder` image, so their results do
not depend on the host toolchain. Gradle builds the image from
[`container/Dockerfile.builder`](../container/Dockerfile.builder) the first
time that a task needs it. To build it on demand, run:

```bash
./gradlew :sdks:rust:buildBuilderImage  # Build beam_rust_builder:latest
```

The code-quality tasks below always use the image. The Linux binary tasks
(`buildWorker`, `buildRunnerTestsWorker`, the expansion service builds and
`:sdks:rust:container:docker`) use it when the host is not Linux on the target
architecture. To use a different builder image, set `-PrustBuilderImage=<image>`.

The `prism` task takes `-Pdocker`. With it, the workers run in the SDK base
image with `--environment_type=DOCKER`, and the task builds the Linux worker
binary first. Without it, the workers run in the launching process with
`--environment_type=LOOPBACK`.

## Incremental builds

The lifecycle tasks (`build`, `check`, `test`, `fmt`) always run cargo, which
does its own incremental build. So their result is always the real result of
the cargo command.

The Linux binary tasks track the workspace sources (`**/*.rs`, `Cargo.toml`,
`Cargo.lock`) and their properties (example, architecture, SDK version,
`-PworkerTargetCpu`). If these did not change, the task reports `UP-TO-DATE`
and does not call cargo.

## Code Coverage

The `coverage` task measures line coverage with
[`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) inside the
`beam_rust_builder` container, with the Linux Prism for the tests that start a
pipeline:

```bash
./gradlew :sdks:rust:coverage                          # whole workspace
./gradlew :sdks:rust:coverage -Ppkg=apache-beam-core   # one crate
./gradlew :sdks:rust:coverage -Ppkg=apache-beam-ml -Pfeatures=remote
```

The reports are in `build/reports/coverage/<pkg|workspace>/`: `summary.txt`,
`lcov.info` and `html/index.html`. The instrumented build is cached in
`build/cov-target`, which `coveragePerTest` also uses. For the coverage of each
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
| `-Ppkg` | `coverage`, `coveragePerTest`, `mutants`, `testQualityReport` | Cargo package. Optional for `coverage` (default: the workspace) and for `mutants` with `-PinDiff` (then the whole workspace is filtered by the diff). |
| `-Pfeatures` | `coverage`, `coveragePerTest`, `mutants` | Comma-separated cargo features of the crate. |
| `-Pcpus` | all | Limit the CPU share of the container (`docker run --cpus`). |
| `-Psrc` | `coveragePerTest` | Source dir kept in the lcov output; defaults to the crate's `src/`. |
| `-Pfocus` | `coveragePerTest` | Path substring for an extra focused report, e.g. `/coders/`. |
| `-Pfiles`, `-Pexclude` | `mutants` | Comma-separated globs, relative to `sdks/rust`. |
| `-PinDiff` | `mutants` | A unified diff; only mutants in changed lines are tested. |
| `-Pjobs` (4), `-Ptimeout` (60 s) | `mutants` | Parallel cargo-mutants jobs; per-test-run timeout. |
| `-Ptag` | `mutants` | Write to `mutants-<tag>/` so runs over different files coexist. |
| `-PlistOnly` | `mutants` | List the mutants without testing them. |
| `-PfailOnMissed` | `mutants` | Fail the task if any mutant is missed or times out (default: report only). |
| `-PexpansionServiceVersion` | `mutants` | Released Beam version of the expansion service JARs. The cargo-mutants copy of the tree has no locally built JARs. |
| `-PtestFileRegex` | `testQualityReport` | Which tests (`<test file>::<test>`) are classified and may subsume others. |
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
