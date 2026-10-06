#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Worker image for the table row inference example.
#
# BASE_IMAGE is the Rust SDK image (container/Dockerfile), which holds only the
# `boot` launcher. The build context is the directory holding the Linux
# `columnar_feature_engineering` binary.
# `./gradlew :sdks:rust:prebakedImage -Pexample=columnar_feature_engineering`
# builds that binary and passes both in.
#
# The binary loads ONNX Runtime at run time (`ort/load-dynamic`), so the image
# adds the CPU build of libonnxruntime and points ORT_DYLIB_PATH at it.

ARG BASE_IMAGE
ARG ONNXRUNTIME_VERSION=1.28.2

FROM debian:bookworm-slim AS onnxruntime
ARG ONNXRUNTIME_VERSION
ARG TARGETARCH
RUN apt-get update -qq && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN case "${TARGETARCH}" in \
      amd64) ort_arch=x64 ;; \
      arm64) ort_arch=aarch64 ;; \
      *) echo "unsupported TARGETARCH ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl -fLsS "https://github.com/microsoft/onnxruntime/releases/download/v${ONNXRUNTIME_VERSION}/onnxruntime-linux-${ort_arch}-${ONNXRUNTIME_VERSION}.tgz" \
      | tar -xz -C /tmp \
    && mkdir -p /opt/onnxruntime \
    && cp -a /tmp/onnxruntime-linux-${ort_arch}-${ONNXRUNTIME_VERSION}/lib /opt/onnxruntime/lib

# worker: the SDK image with ONNX Runtime and the pipeline binary pre-baked.
FROM ${BASE_IMAGE} AS worker
COPY --from=onnxruntime /opt/onnxruntime /opt/onnxruntime
ENV ORT_DYLIB_PATH=/opt/onnxruntime/lib/libonnxruntime.so
COPY columnar_feature_engineering /opt/apache/beam/worker_binary

# flex: the worker image plus Google's Go Flex Template launcher, so the same
# image serves as the Flex Template launcher and as the SDK worker.
FROM worker AS flex
COPY --from=gcr.io/dataflow-templates-base/go-template-launcher-base:latest \
    /opt/google/dataflow/go_template_launcher \
    /opt/google/dataflow/go_template_launcher
ENV FLEX_TEMPLATE_GO_BINARY=/opt/apache/beam/worker_binary
