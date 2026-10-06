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

FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

LABEL Author="Apache Beam <dev@beam.apache.org>"
LABEL description="Apache Beam Rust SDK worker container with CUDA acceleration and ONNX Runtime GPU"

# Install ca-certificates, curl, and libssl
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tar \
    && rm -rf /var/lib/apt/lists/*

# Install ONNX Runtime GPU (v1.19.0)
RUN curl -LsSf "https://github.com/microsoft/onnxruntime/releases/download/v1.19.0/onnxruntime-linux-x64-gpu-1.19.0.tgz" | tar -xz -C /tmp && \
    cp -a /tmp/onnxruntime-linux-x64-gpu-1.19.0/lib/. /usr/local/lib/ && \
    rm -rf /tmp/onnxruntime-linux-x64-gpu-1.19.0 && \
    ldconfig

# Copy Beam boot launcher binary
COPY build/target/linux_amd64/boot /opt/apache/beam/boot

ENTRYPOINT ["/opt/apache/beam/boot"]
