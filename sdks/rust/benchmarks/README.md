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

# Rust SDK benchmarks and profiling

This page gives the tasks that measure the performance of the SDK: local profiles on
Prism, criterion benchmarks of the harness, and benchmarks on Dataflow. Run them from
the repository root. [docs/build.md](../docs/build.md) gives the build and test tasks.

## Tasks

```bash
./gradlew :sdks:rust:nexmarkPrism      # Launch Nexmark on PrismRunner ([-Psuite=SMOKE] [-Pquery=0] [-Pstreaming])
./gradlew :sdks:rust:nexmarkDataflow   # Launch Nexmark on Dataflow ([-Psuite=DEFAULT] [-Pquery=0] [-Pstreaming])
./gradlew :sdks:rust:profilePrism      # Profile locally on Prism (-Pexample=wordcount [-Pmode=cpu|memory|both] ...)
./gradlew :sdks:rust:kafkaCopyBenchmark          # Local Kafka copy bench on Prism against a broker (`-Precords=100000 -Pruns=3`)
./gradlew :sdks:rust:bigQueryBenchmark           # BigQuery benchmark on Dataflow ([-Ppipeline=python|java|rust] [-Pshape=count|per_key|copy] -Preader=...|-Preaders=a,b [-Pwriter=...] [-PnumWorkers=4] [-PmaxNumWorkers=10] [-Pruns=1] [-PdryRun])
./gradlew :sdks:rust:bigtableBenchmark           # Bigtable benchmark on Dataflow ([-Ppipeline=python|java|rust] [-Pshape=bt_to_gcs|gcs_to_bt] -Preader=...|-Pwriter=... [-Pruns=1] [-PdryRun])
./gradlew :sdks:rust:kafkaBenchmark              # Kafka benchmark on Dataflow ([-Ppipeline=python|java|rust] [-Pshape=load|copy] [-Pformat=BYTES|RAW|JSON|AVRO] [-Pruns=1] [-PdryRun])
./gradlew :sdks:rust:icebergBenchmark            # Iceberg streaming ingest benchmark on Dataflow ([-Ppipeline=python|java|rust] [-Pwriter=...] [-Pruns=1] [-PdryRun])
./gradlew :sdks:rust:setupBenchmarkPythonEnv   # Python virtualenv with a released SDK for Dataflow I/O benchmarks
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
  For the Kafka copy bench, use `-Ppkg=apache-beam-io-kafka -PcargoBench=copy_throughput`,
  and `-PextraArgs="--runs=1 --formats=bytes"` for one copy.
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

The summary splits the CPU time between the Rust process and Prism. It also gives the CPU
share of each stage and the hottest functions of the Rust process. When samply records the
CPU time of each thread, each sample counts its CPU time. A thread that waits in a system
call, for example in `poll()`, then counts as idle. To open the profile in the Firefox
profiler, run `samply load <dir>/profile.json.gz`. Keep `profile.json.syms.json` in the same
directory: it holds the function names.

## Benchmarks

[gradle/benchmarks.gradle](../gradle/benchmarks.gradle) defines the benchmarks.
[gradle/benchmarks/nexmark.gradle](../gradle/benchmarks/nexmark.gradle) defines the Nexmark
tasks. [gradle/benchmarks/io.gradle](../gradle/benchmarks/io.gradle) (with
`gradle/benchmarks/io/*.gradle`) defines the I/O benchmarks. Each benchmark runs
some rounds (`-Pruns`, default 3), one after the other. It writes `results.json` and
`report.md` to `build/reports/benchmarks/<task>-<timestamp>/`. The report gives each run and
the medians. The scripts are in [benchmarks/tools/](tools/).

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

# Nexmark on Prism or Dataflow (`benchmarks/nexmark`).
./gradlew :sdks:rust:nexmarkPrism -Psuite=SMOKE -PextraArgs="--num_events=500"
./gradlew :sdks:rust:nexmarkDataflow -Pquery=0 -PextraArgs="--num_events=1000000"
./gradlew :sdks:rust:nexmarkBenchmark -Pquery=0 -PnumEvents=1000000 -Pbaseline=origin/master

# A Rust example or bench on Prism with profilePrism (worker profile, CPU mode).
gsutil cp gs://apache-beam-samples/wikipedia_edits/wiki_data-000000000000.json build/bench-input/
./gradlew :sdks:rust:profileCompare -Pinput='build/bench-input/*.json' -Pbaseline=HEAD~1
./gradlew :sdks:rust:profileCompare -Ppkg=apache-beam-harness -PcargoBench=chain_throughput

# Criterion benchmarks of the harness crate, on this machine.
./gradlew :sdks:rust:harnessBenchmark -Pbench=data_plane -PsaveBaseline=before
./gradlew :sdks:rust:harnessBenchmark -Pbench=data_plane -PbaselineName=before
```

- `nexmarkPrism` and `nexmarkDataflow` run the [Nexmark suite](nexmark/README.md)
  (`-Psuite=DEFAULT|SMOKE|STRESS|FULL_THROTTLE`, `-Pquery=0..16`, `-Pstreaming`, `-PextraArgs`).
  `nexmarkBenchmark` runs one query across rounds on Dataflow (`-Pquery=0`,
  `-PnumEvents=1000000`, `-Pstreaming`, `-Pbaseline=<git ref>`) and reports CPU and
  `events.elements` throughput.
- `harnessBenchmark` runs the criterion benches in
  [harness/benches/](../beam/harness/benches/). `-Ppkg=<crate> -Pbench=<name>` runs a bench
  of another crate: `row_bridge` of `apache-beam-io-arrow`, or `typed_encode` of
  `apache-beam-io-kafka`. `process_bundle` runs bundles through a worker and an in-process runner over gRPC. It uses the pipelines and the 16 concurrent
  callers of Java's `ProcessBundleBenchmark` (`sdks/java/harness/jmh`). Its times are per
  operation, so 1 / time compares with the ops/s of JMH. `data_plane` measures inbound
  routing, outbound buffering and the wait one slow bundle causes another on the same
  stream. `user_state` measures bag state against an in-process state server, and
  `side_input` the reads of a singleton side input. `row_coder` encodes and decodes a
  `#[derive(BeamRow)]` struct. `logging` measures the per-event cost of the Fn API logging
  layer. `chain_throughput` is not a criterion bench. It runs a fused WordCount chain with
  and without a user counter per word, and prints the ns/word of each. `-Pfilter` selects
  benchmark ids, and `-PsmokeTest` runs each once. `-PcargoProfile=worker` builds with the
  fat LTO of the binaries that run on a runner. Compare with other SDKs only in that
  profile. Reports and baselines are in `target/criterion/`.
- A Dataflow benchmark first builds the worker and the launcher of each variant, so the jobs
  of a round start together. The report flags a round whose jobs started more than 60 s
  apart. It takes `-PnumWorkers` (1), `-PmaxNumWorkers` (10), `-PworkerMachineType`
  (`n2-standard-4`), `-PminCpuPlatform`, `-PextraArgs` and the GCP properties of the
  `dataflow` task. It finds each job by its name and gives it the labels `benchmark`,
  `variant`, `round` and `run_id`. `-PprepareOnly` runs only the builds, without jobs.
- The report gives the vCPU·s, the counted elements per vCPU·s, memory, PD and shuffle. It
  also gives the ProcessElement time of each step and the peak memory of each container. The
  other values are peak workers, CPU platform, warnings, and the start and end times. Cloud Monitoring gives the used vCPU·s,
  the container memory and the counter rate. It can be some minutes late. The script
  waits up to 6 minutes for it, then marks the missing values. `-PmonitoringWait=<seconds>`
  changes the wait.
- The total vCPU·s includes worker startup and idle autoscaled workers. The used vCPU·s does
  not. To compare the code of two builds, use the step CPU seconds per 1M elements.
- The CPU platform of the workers changes the results by about 20% (for example, Cascade
  Lake or Ice Lake on `n2`). The report flags a round whose jobs ran on different platforms.
  It also flags a job that autoscaled beyond its initial workers, and stockout or quota warnings. Do not pin a
  platform with `-PminCpuPlatform` unless necessary: a pinned platform can stock out.
- To add a Dataflow benchmark of a Rust example, call `registerRustBenchmark` (see
  `wordcountBenchmark`). For other launchers, give launch commands to `runDataflowBenchmark`
  (see `nexmarkBenchmark`).
- The I/O benchmarks are `bigQueryBenchmark`, `bigtableBenchmark`, `kafkaBenchmark` and
  `icebergBenchmark` (see [gradle/benchmarks/io.gradle](../gradle/benchmarks/io.gradle)). They
  run one variant for each reader of `-Preaders=a,b` (or `-Preader`). They also run one
  variant for each writer of `-Pwriters` (or `-Pwriter`). All variants use the pipeline of
  `-Ppipeline`. `buildIoBenchmarkWorkers`
  builds all four Linux worker binaries over one shared `.rlib` cache. `-Ppipeline=rust` supports
  `-Pbaseline=<git ref>`. They run 1 round by default. Every Python, Java and Rust launcher
  submits its job, prints `JOB_ID=<id>` and exits. Then `job_wait.py` polls and drains the
  jobs of all SDKs in the same way. It uses `-PexpectedRows`, `-PdrainAfterSeconds`, or
  `-PidleAfterLoadSeconds` with a companion producer job. The report (`io_report.py`)
  compares the last variant with the others. It gives processing time, rows per vCPU·s,
  latency and streaming backlog. See [rust-native-io-providers.md](../docs/rust-native-io-providers.md#4-benchmarks).
- The companion producer of `kafkaBenchmark -Pshape=copy` and `icebergBenchmark` starts with
  `-PsamplePath`, `-Prate`, `-PloadSeconds` or `-PsharedProducer`. Each benchmark has its own
  sample key: `example.benchmark.kafkaSamplePath` or `example.benchmark.icebergSamplePath`.
  `icebergBenchmark` defaults to 100000 records/s for 1800 s and 270 s idle, and needs
  `example.benchmark.kafkaCluster`. The producer takes `-PproducerExtraArgs`, not `-PextraArgs`.
  `results.json` holds its job, state and rows. The medians do not include a round whose
  producer fails. Tasks that run at the same time with the same `-PsharedProducer=<id>` share
  one producer per round. An example is a Rust and a Java copy of one topic.
  `-PsharedProducerTasks` (default 2) sets the number of these tasks.
- `profileCompare` takes `-Pexample` (`wordcount`) or the target properties of `profilePrism`.
  It alternates the order of the variants in each round. `profilePrism` builds before it
  records, so the profile does not include the build. The report gives the Rust and Prism
  CPU seconds, the Rust CPU seconds per 1M rows and the hottest frames. `-Pinput` gives the
  pipeline `--input` and `--output`. `-Prows` sets the rows. The default is the number of lines
  of the input.

### BigQuery benchmark

`bigQueryBenchmark` has three shapes. `count` and `per_key` only read.
`copy` reads and passes each row through a step in the pipeline language.
Then it writes the rows to a new table in `-Pdataset` (or `example.benchmark.dataset`).
After each `copy` job, the task checks the rows and drops the table.

The readers depend on the pipeline:

- `-Ppipeline=python` (the default): `python`, `java` or `rust`.
- `-Ppipeline=java`: `java-native`, `java-managed` or `rust`.
- `-Ppipeline=rust`: `rust`.

The default reader is `rust`.
`-Preaders=java,rust` runs one job for each reader in each round. `copy` also needs
`-Pwriter` (or `-Pwriters`). The writers are `java` or `rust` for `-Ppipeline=python`,
`java-native`, `java-managed` or `rust` for `-Ppipeline=java`, and `rust` for `-Ppipeline=rust`.
The Rust pipeline ([`apache-beam-benchmark-bigquery`](io/bigquery/rust)) runs
`count` and `copy` in one Rust SDK harness. Its `count` adds the total to the `e2e/rows`
counter, and the script checks `-PexpectedRows` against it.
`-ProwRestriction` limits the
rows for a smoke run. Every shape runs on fixed workers (`-PnumWorkers=4`) or
with autoscaling (`-PnumWorkers=1 -PmaxNumWorkers=10`). See
[`bigquery.gradle`](../gradle/benchmarks/io/bigquery.gradle) for the
other properties.
