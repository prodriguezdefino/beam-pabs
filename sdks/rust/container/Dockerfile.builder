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

# Each download is checked against the sha256 that its GitHub release publishes. Change the
# digests together with the version.
ARG CARGO_LLVM_COV_VERSION=0.9.2
ARG CARGO_LLVM_COV_SHA256_X86_64=8d863ab2df1fb53d4f41052b3fa4627ac003d00c46a6388ce037d17472c504ef
ARG CARGO_LLVM_COV_SHA256_ARM64=dab1bfd1f25f7fcd3d6fc6abf9d71b40217a7a103e1da88c1a23111960aa4a40
RUN ARCH="$(uname -m)" && \
    case "${ARCH}" in \
        x86_64) TGT="x86_64-unknown-linux-musl" SHA="${CARGO_LLVM_COV_SHA256_X86_64}" ;; \
        aarch64|arm64) TGT="aarch64-unknown-linux-musl" SHA="${CARGO_LLVM_COV_SHA256_ARM64}" ;; \
        *) echo "Unsupported arch ${ARCH}" && exit 1 ;; \
    esac && \
    curl -LsSf -o /tmp/cargo-llvm-cov.tar.gz "https://github.com/taiki-e/cargo-llvm-cov/releases/download/v${CARGO_LLVM_COV_VERSION}/cargo-llvm-cov-${TGT}.tar.gz" && \
    echo "${SHA}  /tmp/cargo-llvm-cov.tar.gz" | sha256sum -c - && \
    tar xzf /tmp/cargo-llvm-cov.tar.gz -C /usr/local/cargo/bin && \
    rm /tmp/cargo-llvm-cov.tar.gz

# Test-quality tooling:
#   cargo-nextest: per-test process isolation, timings and flake detection; used by
#                  `cargo llvm-cov nextest` and by cargo-mutants when available.
#   cargo-mutants: mutation testing, to measure assertion strength and test redundancy.
#   cargo-machete: fast check for unused Cargo.toml dependencies across the workspace.
ARG CARGO_NEXTEST_VERSION=0.9.152
ARG CARGO_MUTANTS_VERSION=27.1.0
ARG CARGO_MACHETE_VERSION=0.10.1
ARG CARGO_NEXTEST_SHA256_X86_64=d039b43c3e0958441b6ad0a39c8884d018b3d2e8fb9928f209074b1100b5e17a
ARG CARGO_NEXTEST_SHA256_ARM64=ca1f81a5b86533eb45ab97e0064aa88e925e53aab9a698ca6331a7cad1f85f07
RUN ARCH="$(uname -m)" && \
    case "${ARCH}" in \
        x86_64) TGT="x86_64-unknown-linux-gnu" SHA="${CARGO_NEXTEST_SHA256_X86_64}" ;; \
        aarch64|arm64) TGT="aarch64-unknown-linux-gnu" SHA="${CARGO_NEXTEST_SHA256_ARM64}" ;; \
        *) echo "Unsupported arch ${ARCH}" && exit 1 ;; \
    esac && \
    curl -LsSf -o /tmp/cargo-nextest.tar.gz "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${CARGO_NEXTEST_VERSION}/cargo-nextest-${CARGO_NEXTEST_VERSION}-${TGT}.tar.gz" && \
    echo "${SHA}  /tmp/cargo-nextest.tar.gz" | sha256sum -c - && \
    tar xzf /tmp/cargo-nextest.tar.gz -C /usr/local/cargo/bin && \
    rm /tmp/cargo-nextest.tar.gz && \
    cargo install --locked \
        "cargo-mutants@${CARGO_MUTANTS_VERSION}" \
        "cargo-machete@${CARGO_MACHETE_VERSION}" && \
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
