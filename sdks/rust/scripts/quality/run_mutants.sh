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

# cargo-mutants plus kill-matrix analysis; run by `./gradlew :sdks:rust:mutants`
# in beam_rust_builder, from sdks/rust.
#
# Usage:
#   run_mutants.sh <out-dir> <fail-on-missed: true|false> <cargo mutants args...>
#
#   out-dir         the directory passed to cargo mutants as --output; it gets
#                   mutants.out/, kill_matrix.txt and kill_matrix.tsv. If
#                   <out-dir>/../coverage/redundancy_all.tsv exists (from
#                   coveragePerTest), the report cross-references it.
#   fail-on-missed  exit non-zero when mutants were missed or timed out
#
# The cargo mutants args need `--test-tool nextest --cargo-test-arg=--no-fail-fast`
# to record every killing test. `--list` skips the analysis.
#
# Exit codes 2 (missed) and 3 (timeout) fail only with fail-on-missed=true;
# any other code is passed through.

set -o pipefail

if [ "$#" -lt 3 ]; then
  sed -n '/^# Usage:/,/^# The cargo/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2
  exit 64
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=builder_guard.sh
. "$HERE/builder_guard.sh"
require_builder run_mutants.sh "./gradlew :sdks:rust:mutants -Ppkg=<cargo-package>"

OUT="$1"; STRICT="$2"; shift 2

cargo mutants "$@"
rc=$?

for a in "$@"; do
  [ "$a" = --list ] && exit "$rc"
done

if [ -f "$OUT/mutants.out/outcomes.json" ]; then
  extra=()
  red="$OUT/../coverage/redundancy_all.tsv"
  [ -f "$red" ] && extra=(--redundancy "$red")
  python3 "$HERE/kill_matrix.py" "$OUT" --tsv "$OUT/kill_matrix.tsv" "${extra[@]}" \
    | tee "$OUT/kill_matrix.txt"
else
  echo "no mutants were tested (nothing matched the filters)"
fi

case "$rc" in
  0) exit 0 ;;
  2|3)
    echo "cargo mutants exited $rc (missed or timed-out mutants); see $OUT"
    [ "$STRICT" = true ] && exit "$rc"
    exit 0 ;;
  *) exit "$rc" ;;
esac
