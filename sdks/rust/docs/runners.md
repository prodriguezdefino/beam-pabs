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

# Running Rust Pipelines

This page tells you how to submit Rust pipelines to each supported runner. For
the pipelines, see the [examples catalogue](../examples/README.md).

You select a runner at two levels. The two levels must agree:

- **Link time**: enable the runner as a feature of the `beam` dependency
  (`beam = { ..., features = ["prism", "dataflow"] }`). Runners register
  themselves, so only linked runners are available. A runner feature also links
  the worker harness.
- **Run time**: `--runner=prism` / `--runner=dataflow` selects one of the linked
  runners. The names are not case sensitive, and `PrismRunner` and
  `DataflowRunner` also work. The default is `prism`.

## Portable Prism Runner

Prism is the local portable runner and needs no cloud resources. It is the
default target for development and for the ValidatesRunner suite:

```bash
./gradlew :sdks:rust:prism -Pexample=wordcount
```

## Google Cloud Dataflow

Dataflow workers run in a container, so each launch must name the worker image.
By default, the build compiles and stages the Linux worker binary of the pipeline
automatically. To put the binary in the image instead, use `:sdks:rust:prebakedImage`.

```bash
./gradlew :sdks:rust:dataflow \
  -Pexample=gaming \
  -PgcpProject=<your-gcp-project> \
  -PgcpTempLocation=gs://<your-bucket>/temp \
  -PsdkContainerImage=<registry>/beam_rust_sdk:<sdk version> \
  -PextraArgs="--output=gs://<your-bucket>/gaming"
```

`gcpProject` falls back to `gcloud config get-value project`. `gcpRegion`
defaults to `us-central1`.

You can also keep settings in a local `sdks/rust/.local/sdk.properties` file. Do not commit it:

```properties
gcpProject=my-gcp-project
gcpRegion=us-central1
gcpTempLocation=gs://my-bucket/temp
sdkContainerImage=us-central1-docker.pkg.dev/my-gcp-project/beam/beam_rust_sdk:<sdk version>
workerMachineType=e2-standard-4
example.wordcount.workerMachineType=e2-standard-4
```

For each setting, the task uses the first value that it finds: `-P<name>`,
`example.<example>.<name>` in the file, `<name>` in the file, then an
environment variable (for example `GCP_PROJECT`, `GCP_TEMP_LOCATION`,
`SDK_CONTAINER_IMAGE`).

The task passes these settings to the pipeline as flags:

| Property | Pipeline flag |
|---|---|
| `gcpProject`, `gcpRegion`, `gcpTempLocation` | `--project`, `--region`, `--temp_location` |
| `sdkContainerImage` | `--sdk_container_image` |
| `workerBinary` | `--worker_binary` (see below) |
| `numWorkers`, `maxNumWorkers` | `--num_workers`, `--max_num_workers` |
| `workerMachineType`, `diskSizeGb` | `--worker_machine_type`, `--disk_size_gb` |
| `network`, `subnetwork`, `serviceAccountEmail` | `--network`, `--subnetwork`, `--service_account_email` |
| `experiments`, `dataflowServiceOptions` | `--experiments`, `--dataflow_service_options` |
| `exampleFeatures` | Cargo features of the example (`cargo run --features`) |
| `extraArgs` | Added to the pipeline arguments as they are |

The task also maps `workerDiskType`, `autoscalingAlgorithm`,
`numberOfWorkerHarnessThreads` and `usePublicIps`, but the Rust SDK declares
none of these flags, so it drops them with a warning. Use
`-PextraArgs="--disk_type=pd-ssd --no_use_public_ips"` for the disk type and
private IPs.

```bash
./gradlew :sdks:rust:dataflow \
  -Pexample=wordcount \
  -PnumWorkers=3 \
  -PmaxNumWorkers=10 \
  -PextraArgs="--input=gs://my-bucket/input.txt --output=gs://my-bucket/output.txt"
```

### Building Linux worker binaries

A Rust pipeline is a compiled binary, so the worker needs a Linux build of it. These
commands cross-compile binaries for container execution (Docker or Dataflow):

```bash
# Build worker binary for wordcount (linux/amd64 by default)
./gradlew :sdks:rust:buildWorker -Pexample=wordcount

# Build worker binary for any example and target architecture
./gradlew :sdks:rust:buildWorker -Pexample=join -PexampleArch=amd64
```

`-PworkerBinary=<path>` stages a binary that you built elsewhere.
`-PworkerBinary=none` stages no binary; use it for an image that already
contains the binary. You can scope this setting to one example in
`sdks/rust/.local/sdk.properties` (`example.<name>.workerBinary`), as with other settings.

## Other portable runners

Flink and Spark use the same portable Fn API as Prism, so a Rust pipeline should
run on them without changes. CI does not test this. Treat these runners as
untested, not supported.
