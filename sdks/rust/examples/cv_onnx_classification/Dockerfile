#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Stage 1: Build the cv_onnx_classification binary and boot launcher
FROM rust:1.98-slim-bookworm AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    pkg-config \
    libssl-dev \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV PROTOC_INCLUDE=/usr/include

WORKDIR /workspace
COPY . .

WORKDIR /workspace/sdks/rust
RUN cargo build --release -p cv_onnx_classification --features cuda --bin cv_onnx_classification && \
    cargo build --release -p apache-beam-harness --bin boot

# Stage 2: Runtime image with CUDA, cuDNN and ONNX Runtime GPU.
# ONNX Runtime 1.28's CUDA 12 package is built against CUDA 12.8 + cuDNN 9, and 1.28 is
# the ONNX Runtime line targeted by the `ort` 2.0.0-rc.13 crate (see Cargo.lock).
FROM nvidia/cuda:12.8.1-cudnn-runtime-ubuntu22.04 AS worker

ARG ORT_VERSION=1.28.2
ARG ORT_PACKAGE=onnxruntime-linux-x64-gpu_cuda12-${ORT_VERSION}

# Dataflow's `install-nvidia-driver` mounts the host driver (libcuda.so.1) under
# /usr/local/nvidia. CUDA 12.1 base images put that directory on LD_LIBRARY_PATH; the 12.8
# images only list /usr/local/cuda/lib64, so it has to be added back here or the CUDA
# execution provider fails to load.
ENV LD_LIBRARY_PATH=/usr/local/cuda/compat:/usr/local/nvidia/lib64:/usr/local/nvidia/lib:/usr/local/cuda/lib64

RUN apt-get update -qq && (apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tar \
    cuda-compat-12-8 || apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tar) \
    && rm -rf /var/lib/apt/lists/*

# Install ONNX Runtime GPU; `ort` (load-dynamic) dlopens libonnxruntime.so, which in turn
# loads libonnxruntime_providers_cuda.so from the same directory.
RUN curl -LsSf "https://github.com/microsoft/onnxruntime/releases/download/v${ORT_VERSION}/${ORT_PACKAGE}.tgz" | tar -xz -C /tmp && \
    cp -a /tmp/${ORT_PACKAGE}/lib/. /usr/local/lib/ && \
    ln -sf libonnxruntime.so.1 /usr/local/lib/libonnxruntime.so && \
    rm -rf /tmp/${ORT_PACKAGE} && \
    ldconfig

COPY --from=builder /workspace/sdks/rust/target/release/cv_onnx_classification /opt/apache/beam/worker_binary
COPY --from=builder /workspace/sdks/rust/target/release/boot /opt/apache/beam/boot

ENTRYPOINT ["/opt/apache/beam/boot"]
