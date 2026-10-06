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

# Stage 1: Build the text_embedding_candle worker binary and boot launcher with CUDA nvcc support
FROM nvidia/cuda:12.1.1-devel-ubuntu22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    curl build-essential pkg-config libssl-dev ca-certificates git unzip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://github.com/protocolbuffers/protobuf/releases/download/v25.2/protoc-25.2-linux-x86_64.zip -o /tmp/protoc.zip && \
    unzip -q -o /tmp/protoc.zip -d /usr/local && rm /tmp/protoc.zip

ENV PROTOC=/usr/local/bin/protoc \
    PROTOC_INCLUDE=/usr/local/include

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.98.0
ENV PATH=/root/.cargo/bin:/usr/local/cuda/bin:$PATH \
    CUDA_HOME=/usr/local/cuda \
    CUDACXX=/usr/local/cuda/bin/nvcc \
    CUDA_COMPUTE_CAP=75

WORKDIR /workspace
COPY . .

WORKDIR /workspace/sdks/rust
RUN cargo build --release -p text_embedding_candle --features cuda --bin text_embedding_candle -p apache-beam-harness --bin boot

# Stage 2: Runtime image based on CUDA runtime and Beam boot entrypoint
FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04 AS worker

ENV LD_LIBRARY_PATH=/usr/local/nvidia/lib64:/usr/local/nvidia/lib:/usr/local/cuda/lib64

RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    ca-certificates curl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/sdks/rust/target/release/text_embedding_candle /opt/apache/beam/worker_binary
COPY --from=builder /workspace/sdks/rust/target/release/boot /opt/apache/beam/boot

ENTRYPOINT ["/opt/apache/beam/boot"]
