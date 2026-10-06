#!/usr/bin/env bash
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

# Per-test line coverage for one crate of the Rust SDK workspace.
#
# Builds the test binaries with LLVM source-based coverage and runs each test
# alone with its own LLVM_PROFILE_FILE, giving one lcov file per test for
# analyze_overlap.py. Runs in beam_rust_builder through `:sdks:rust:coveragePerTest`,
# from sdks/rust.
#
# Usage:
#   per_test_cov.sh <cargo-package> <src-dir> <out-dir>
#
#   cargo-package  e.g. apache-beam-core
#   src-dir        source directory to keep in the lcov output, relative to the
#                  repository root, e.g. sdks/rust/beam/core/src
#   out-dir        output directory (created; previous lcov/ and prof/ removed)
#
# Environment:
#   COV_TARGET_DIR    cargo target dir for the instrumented build
#                     (default /tmp/target-cov; mount it to reuse builds)
#   PER_TEST_TIMEOUT  seconds before a single test is killed (default 300)
#   COV_FEATURES      comma-separated cargo features to enable (default none)
#   BEAM_RUST_BUILDER set to 1 by the beam_rust_builder image; the script
#                     refuses to run without it
#   ALLOW_OUTSIDE_BUILDER=1  run anyway (Linux hosts with the same tools only)
#
# Outputs:
#   <out-dir>/tests.tsv        idx, binary, test, status (pass|fail), seconds
#   <out-dir>/lcov/<idx>.info  lcov (SF/DA records only) restricted to src-dir
#   <out-dir>/binaries.txt     test executables that were run
#   <out-dir>/build.err        stderr of the instrumented build

set -uo pipefail

# shellcheck source=builder_guard.sh
. "$(dirname "${BASH_SOURCE[0]}")/builder_guard.sh"
require_builder per_test_cov.sh "./gradlew :sdks:rust:coveragePerTest -Ppkg=<cargo-package>"

if [ "$#" -ne 3 ]; then
  sed -n '/^# Usage:/,/^# Environment:/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2
  exit 64
fi

PKG="$1"
SRC="${2#/}"
SRC="${SRC%/}"
OUT="$3"
PER_TEST_TIMEOUT="${PER_TEST_TIMEOUT:-300}"

for tool in jq bc timeout; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 69; }
done

rm -rf "$OUT/lcov" "$OUT/prof"
mkdir -p "$OUT/lcov" "$OUT/prof"

SYSROOT="$(rustc --print sysroot)"
TOOLS="$(ls -d "$SYSROOT"/lib/rustlib/*/bin | head -1)"
PROFDATA="$TOOLS/llvm-profdata"
LLVMCOV="$TOOLS/llvm-cov"
if [ ! -x "$PROFDATA" ] || [ ! -x "$LLVMCOV" ]; then
  echo "llvm-profdata/llvm-cov not found under $TOOLS (rustup component add llvm-tools-preview)" >&2
  exit 69
fi

# Instrumented build, same flags cargo-llvm-cov uses.
eval "$(cargo llvm-cov show-env --export-prefix)"
export CARGO_TARGET_DIR="${COV_TARGET_DIR:-/tmp/target-cov}"
cargo test -p "$PKG" ${COV_FEATURES:+--features "$COV_FEATURES"} --no-run --message-format=json 2>"$OUT/build.err" \
  | jq -r 'select(.reason=="compiler-artifact" and .profile.test==true and .executable!=null) | .executable' \
  > "$OUT/binaries.txt"
if [ ! -s "$OUT/binaries.txt" ]; then
  echo "no test binaries were built for $PKG; see $OUT/build.err" >&2
  tail -20 "$OUT/build.err" >&2
  exit 1
fi
echo "binaries: $(wc -l < "$OUT/binaries.txt")"

printf "idx\tbinary\ttest\tstatus\tseconds\n" > "$OUT/tests.tsv"
idx=0
while read -r BIN; do
  bname="$(basename "$BIN" | sed 's/-[0-9a-f]*$//')"
  mapfile -t TESTS < <("$BIN" --list --format=terse 2>/dev/null | sed -n 's/: test$//p')
  for T in "${TESTS[@]}"; do
    idx=$((idx+1))
    prof="$OUT/prof/$idx.profraw"
    start=$(date +%s.%N)
    if LLVM_PROFILE_FILE="$prof" timeout "$PER_TEST_TIMEOUT" "$BIN" --exact "$T" --test-threads=1 -q >/dev/null 2>&1; then
      st=pass
    else
      st=fail
    fi
    end=$(date +%s.%N)
    printf "%s\t%s\t%s\t%s\t%.3f\n" "$idx" "$bname" "$T" "$st" "$(echo "$end - $start" | bc)" >> "$OUT/tests.tsv"
    if [ -f "$prof" ]; then
      "$PROFDATA" merge -sparse "$prof" -o "$OUT/prof/$idx.profdata" 2>/dev/null \
        && "$LLVMCOV" export -format=lcov -skip-functions -instr-profile="$OUT/prof/$idx.profdata" "$BIN" \
             2>/dev/null | awk -v src="/$SRC/" '
               /^SF:/ { keep = index($0, src) > 0; if (keep) print; next }
               keep && /^DA:/ { print }' > "$OUT/lcov/$idx.info"
      rm -f "$prof" "$OUT/prof/$idx.profdata"
    fi
  done
done < "$OUT/binaries.txt"

rmdir "$OUT/prof" 2>/dev/null || true
fails=$(awk -F'\t' 'NR > 1 && $4 != "pass"' "$OUT/tests.tsv" | wc -l)
echo "done: $idx tests ($fails failed or timed out)"
