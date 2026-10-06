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

# Worker containers

On Dataflow, and on Prism with `--environment_type=DOCKER`, the workers of a
Rust pipeline run in a container. The container entrypoint is always
`/opt/apache/beam/boot`. `boot` connects to the runner and runs a Linux build of
the pipeline. The binary gets into the container in one of two ways:

| | Staged binary | Pre-baked image |
|---|---|---|
| Image | The SDK base image, which holds only `boot` | The base image plus the binary at `/opt/apache/beam/worker_binary` |
| Flags | `--sdk_container_image=<base image>` and `--worker_binary=<Linux binary>` | `--sdk_container_image=<your image>` |
| Uploaded per job | The binary and the pipeline graph | The pipeline graph |
| Suits | Development | Production, native libraries, locked-down environments, templates |

The [wordcount example](../examples/wordcount) shows both ways. Its
[`Dockerfile`](../examples/wordcount/Dockerfile) contains the full pre-baked layout.

## How the binary is chosen

At job submission:

- With `--worker_binary`, the runner stages that binary.
- With only `--sdk_container_image`, the image must contain a pre-baked binary.
  The driver logs a warning that says it assumes this.
- With neither flag, the run fails before the runner stages or submits anything.

On the worker, a pre-baked binary has priority, because the image is the
artifact that you built and tested. If the runner also staged a binary, `boot`
ignores it and logs a warning. In both cases, build the binary from the same
pipeline code as the driver.

Dataflow always needs `--sdk_container_image`. The default image,
`apache/beam_rust_sdk:<sdk version>`, is not published.

Prism runs workers in Docker in two cases:

- `--environment_type=DOCKER` is set.
- `--environment_type` is not set, and `--sdk_container_image` or
  `--environment_config` names an image.

In DOCKER mode without an image flag, Prism uses the default image with the
binary of `--worker_binary`. Build that image locally first. If the two flags name different images, the run fails. With
`LOOPBACK` or `EXTERNAL`, Prism ignores an image flag and logs a warning.

## Build the SDK base image

To build `apache/beam_rust_sdk:<sdk version>` for the architecture of this machine, run:

```bash
./gradlew :sdks:rust:docker
```

The image ([`container/Dockerfile`](../container/Dockerfile)) is
`gcr.io/distroless/cc-debian12` with `boot` and the license files in
`/opt/apache/beam/`. It has no shell.

Dataflow workers pull their image from a registry. They are `linux/amd64`
unless you select Arm machine types. This command builds for amd64 on any host
and pushes the result:

```bash
./gradlew :sdks:rust:container:docker \
  -Pcontainer-architecture-list=amd64 \
  -Pdocker-repository-root=us-central1-docker.pkg.dev/<project>/<repository> \
  -Ppush-containers
```

The command publishes
`us-central1-docker.pkg.dev/<project>/<repository>/beam_rust_sdk:<sdk version>`.
It also tags the same image with the Beam version (`version` in `gradle.properties`,
for example `<major>.<minor>.<patch>-SNAPSHOT`). If a tag with either name exists, it moves to the new image.
Run `gcloud auth configure-docker us-central1-docker.pkg.dev` first. The Dataflow
worker service account needs `roles/artifactregistry.reader` on the repository.

## Staged binary

The Gradle `dataflow` task builds the Linux binary and sets `--worker_binary`:

```bash
./gradlew :sdks:rust:dataflow -Pexample=wordcount \
  -PsdkContainerImage=us-central1-docker.pkg.dev/<project>/<repository>/beam_rust_sdk:<sdk version>
```

You can also set `sdkContainerImage` in `sdks/rust/.local/sdk.properties`. See
[Running Rust Pipelines](runners.md).

To use the same setup on Prism, pass `-Pdocker`. The `prism` task then builds
the Linux binary of the example (`buildWorker`) and sets
`--environment_type=DOCKER`, `--environment_config=apache/beam_rust_sdk:<sdk version>`
and `--worker_binary`. Build the binary for the architecture of the local image
(`arm64` on Apple silicon):

```bash
./gradlew :sdks:rust:prism -Pexample=wordcount -Pdocker -PexampleArch=arm64 \
  -PextraArgs="--output=gs://<bucket>/counts"
```

`-PenvironmentConfig=<image>` selects another image.

Workers resolve file paths inside their containers. Write to a location that
they can reach, such as `gs://`.

## Pre-baked image

The `worker` stage of
[`examples/wordcount/Dockerfile`](../examples/wordcount/Dockerfile) is the full layout:

```dockerfile
ARG BASE_IMAGE

FROM ${BASE_IMAGE} AS worker
COPY wordcount /opt/apache/beam/worker_binary
```

The `prebakedImage` task builds the Linux binary of the example, then the image.
With `-Ppush-containers`, it also pushes the result:

```bash
./gradlew :sdks:rust:prebakedImage -Pexample=wordcount \
  -PimageName=us-central1-docker.pkg.dev/<project>/<repository>/wordcount:1.0 \
  -Ppush-containers
```

| Property | Meaning |
|---|---|
| `imageName` | Tag for the new image. Required. |
| `baseImage` | Image to build on. Defaults to `apache/beam_rust_sdk:<sdk version>`, which must exist locally for the same architecture. |
| `exampleArch` | `amd64` (default) or `arm64`, for both the binary and the image. |
| `flex` | Build the Flex Template stage instead; see [Flex Templates](#flex-templates-experimental). |
| `workerBinary` | Bake this binary instead of building the example. |

Launch the job with that image and no binary to stage:

```bash
./gradlew :sdks:rust:dataflow -Pexample=wordcount \
  -PsdkContainerImage=us-central1-docker.pkg.dev/<project>/<repository>/wordcount:1.0 \
  -PworkerBinary=none
```

A pipeline of your own, outside this repository, sets the same pipeline flags:
`--sdk_container_image` and no `--worker_binary`. The driver builds the pipeline
graph locally, on any OS, and uploads only the graph.

To pre-bake another example, add an `examples/<name>/Dockerfile` with the same
layout. For native libraries such as GDAL or CUDA, start from an image that
contains them. Copy `boot` from the SDK image:

```dockerfile
FROM <your glibc-based image>
COPY --from=apache/beam_rust_sdk:<sdk version> /opt/apache/beam/boot /opt/apache/beam/boot
COPY <name> /opt/apache/beam/worker_binary
ENTRYPOINT ["/opt/apache/beam/boot"]
```

## Classic templates

`--template_location` stages the pipeline and writes the Dataflow job
description to a `gs://` path or a local path. It does not start a job:

```bash
cargo run -p wordcount -- --runner=dataflow \
  --project=<project> --region=us-central1 --temp_location=gs://<bucket>/temp \
  --output=gs://<bucket>/counts \
  --sdk_container_image=us-central1-docker.pkg.dev/<project>/<repository>/wordcount:1.0 \
  --template_location=gs://<bucket>/templates/wordcount

gcloud dataflow jobs run wordcount \
  --gcs-location=gs://<bucket>/templates/wordcount --region=us-central1
```

The pipeline options are fixed when the runner writes the template. These
templates take no runtime parameters. The template refers to files under the
staging location. Keep these files while the template is in use.

`--dataflow_job_file` writes the job description for inspection and stages
nothing. Do not set it together with `--template_location`.

## Flex Templates (experimental)

`gcloud` has no `RUST` SDK language. This procedure uses the Go Flex Template
launcher, because a Go pipeline is also a compiled binary. The wordcount example
runs end to end this way. The procedure depends on the behavior of the Go
launcher, so it is experimental.

The launcher runs your binary with its own flags added, in alphabetical order.
Parse the program arguments with `beam::options::parse::<Args>()`, as the
examples do. Do not use `Args::parse()`. Each option group, yours too, reads
only the flags that it declares, so launcher and runner flags do not cause a
parse failure. The SDK drops flags that no group declares and logs a warning.

The `flex` stage of the wordcount `Dockerfile` adds the Google Go launcher to
the pre-baked image. One image is then both the launcher and the worker:

```dockerfile
FROM worker AS flex
COPY --from=gcr.io/dataflow-templates-base/go-template-launcher-base:latest \
    /opt/google/dataflow/go_template_launcher \
    /opt/google/dataflow/go_template_launcher
ENV FLEX_TEMPLATE_GO_BINARY=/opt/apache/beam/worker_binary
```

Build and push the image, then build the template, then run it:

```bash
IMAGE=us-central1-docker.pkg.dev/<project>/<repository>/wordcount-flex:1.0

./gradlew :sdks:rust:prebakedImage -Pexample=wordcount -Pflex \
  -PimageName=$IMAGE -Ppush-containers

gcloud dataflow flex-template build gs://<bucket>/templates/wordcount.json \
  --image=$IMAGE --sdk-language=GO \
  --metadata-file=sdks/rust/examples/wordcount/metadata.json

gcloud dataflow flex-template run "wordcount-$(date +%s)" \
  --template-file-gcs-location=gs://<bucket>/templates/wordcount.json \
  --region=us-central1 \
  --parameters=output=gs://<bucket>/counts,sdk_container_image=$IMAGE
```

[`metadata.json`](../examples/wordcount/metadata.json) declares the template
parameters. The `sdk_container_image` parameter tells the workers to use the same image.

## Troubleshooting

| Message | Where | Fix |
|---|---|---|
| `Missing required option: sdk_container_image` | Dataflow submit | Build and push the base image, then pass it. |
| `nothing tells it which pipeline binary to run` | Driver | Pass `--worker_binary`, or `--sdk_container_image` with a pre-baked image. |
| `No pipeline binary to run: the image has none pre-baked` | Worker log | Bake the binary in at `/opt/apache/beam/worker_binary`, or submit with `--worker_binary`. |
| `Ignoring the staged pipeline binary` | Worker log | Expected when a pre-baked image is launched with `--worker_binary`. Drop the flag to stop staging it. |
| `experiment '…' disables Dataflow Runner v2` | Dataflow submit | Remove the experiment: Rust pipelines need Runner v2. |
| `exec format error` | Worker log | The binary and the image have different architectures. Rebuild with a matching `-PexampleArch`. |
