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

FROM rust:1.98-slim-bookworm

LABEL maintainer="Apache Beam <dev@beam.apache.org>"
LABEL description="Hermetic Rust SDK build and test container with cross-compilers and llvm-cov"

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    pkg-config \
    libssl-dev \
    gcc-x86-64-linux-gnu \
    g++-x86-64-linux-gnu \
    gcc-aarch64-linux-gnu \
    g++-aarch64-linux-gnu \
    curl \
    git \
    ca-certificates \
    python3 \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add llvm-tools-preview rustfmt clippy && \
    rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu

RUN ARCH="$(uname -m)" && \
    case "${ARCH}" in \
        x86_64) TGT="x86_64-unknown-linux-musl" ;; \
        aarch64|arm64) TGT="aarch64-unknown-linux-musl" ;; \
        *) echo "Unsupported arch ${ARCH}" && exit 1 ;; \
    esac && \
    curl -LsSf "https://github.com/taiki-e/cargo-llvm-cov/releases/latest/download/cargo-llvm-cov-${TGT}.tar.gz" | tar xzf - -C /usr/local/cargo/bin

# Test-quality tooling:
#   cargo-nextest: per-test process isolation, timings and flake detection; used by
#                  `cargo llvm-cov nextest` and by cargo-mutants when available.
#   cargo-mutants: mutation testing, to measure assertion strength and test redundancy.
RUN ARCH="$(uname -m)" && \
    case "${ARCH}" in \
        x86_64) NT="linux" ;; \
        aarch64|arm64) NT="linux-arm" ;; \
    esac && \
    curl -LsSf "https://get.nexte.st/latest/${NT}" | tar xzf - -C /usr/local/cargo/bin && \
    cargo install --locked cargo-mutants && \
    rm -rf /usr/local/cargo/registry /usr/local/cargo/git

# jq/bc: post-processing for test-quality scripts. mold: optional fast linker for
# link-heavy loops such as cargo-mutants, opt in with
# CARGO_TARGET_<TRIPLE>_RUSTFLAGS="-C link-arg=-fuse-ld=mold".
RUN apt-get update -qq && apt-get install -y --no-install-recommends jq bc mold \
    && rm -rf /var/lib/apt/lists/*

# Size and complexity tooling for scripts/quality/complexity_scorecard.py, pinned and
# verified: scc from the official release, checked against the sha256 published in that
# release's checksums.txt; lizard and its dependencies from PyPI with hash checking.
ARG SCC_VERSION=4.1.0
ARG SCC_SHA256_X86_64=c7328436d3027f4357d3d7853f7dc3ac2bbcb4ca08f1adad91a27c593884079b
ARG SCC_SHA256_ARM64=6e0d2a1f8d3540ba7df185477dec40bb7340f1b214bfd303147de5cad2bd7b8b
RUN set -eu; \
    case "$(uname -m)" in \
        x86_64) SCC_ARCH=x86_64; SCC_SHA256="${SCC_SHA256_X86_64}" ;; \
        aarch64|arm64) SCC_ARCH=arm64; SCC_SHA256="${SCC_SHA256_ARM64}" ;; \
        *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;; \
    esac; \
    curl -fsSLo /tmp/scc.tar.gz \
        "https://github.com/boyter/scc/releases/download/v${SCC_VERSION}/scc_Linux_${SCC_ARCH}.tar.gz"; \
    echo "${SCC_SHA256}  /tmp/scc.tar.gz" | sha256sum -c -; \
    tar xzf /tmp/scc.tar.gz -C /usr/local/bin scc; \
    rm /tmp/scc.tar.gz; \
    scc --version; \
    apt-get update -qq; \
    apt-get install -y --no-install-recommends python3-pip; \
    rm -rf /var/lib/apt/lists/*; \
    printf '%s\n' \
        'lizard==1.24.0 --hash=sha256:a688bc607a891ff4a7836826f25742dc9c1bf648da3075dbd495e199e8848602' \
        'pygments==2.21.0 --hash=sha256:2363c69b61c4a97c838da3b130dcd6468f4848992b21a82f2a63ec34377137d9' \
        'pathspec==1.1.1 --hash=sha256:a00ce642f577bf7f473932318056212bc4f8bfdf53128c78bbd5af0b9b20b189' \
        > /tmp/quality-requirements.txt; \
    pip install --no-cache-dir --break-system-packages --only-binary=:all: --require-hashes \
        -r /tmp/quality-requirements.txt; \
    rm /tmp/quality-requirements.txt; \
    lizard --version

# Marks this image; the collection scripts in scripts/quality only run where it is set.
ENV BEAM_RUST_BUILDER=1
ENV PROTOC_INCLUDE=/usr/include \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc \
    CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++ \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++

# gemm-f16 (from candle) needs FP16, which the generic aarch64 target does not enable.
# An explicit RUSTFLAGS overrides this.
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-feature=+fp16"

WORKDIR /workspace/sdks/rust
