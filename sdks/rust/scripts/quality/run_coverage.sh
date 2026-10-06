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

# Per-test coverage plus overlap analysis; run by `./gradlew :sdks:rust:coveragePerTest`
# in beam_rust_builder, from sdks/rust.
#
# Usage:
#   run_coverage.sh <cargo-package> <src-dir> <out-dir> [focus]
#
#   cargo-package, src-dir, out-dir  as for per_test_cov.sh
#   focus  optional path substring (e.g. /coders/) for a second, focused report
#
# Outputs, in <out-dir>: everything per_test_cov.sh writes, plus
#   overlap_report.txt, redundancy_all.tsv          whole crate
#   overlap_report_<focus>.txt, redundancy_<focus>.tsv  with a focus

set -eo pipefail

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  sed -n '/^# Usage:/,/^# Outputs/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2
  exit 64
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG="$1"; SRC="$2"; OUT="$3"; FOCUS="${4:-}"

# per_test_cov.sh enforces the container-only rule.
bash "$HERE/per_test_cov.sh" "$PKG" "$SRC" "$OUT"
python3 "$HERE/analyze_overlap.py" "$OUT" | tee "$OUT/overlap_report.txt"
if [ -n "$FOCUS" ]; then
  name="$(echo "$FOCUS" | sed 's#^/*##; s#/*$##; s#/#_#g')"
  python3 "$HERE/analyze_overlap.py" "$OUT" --focus "$FOCUS" | tee "$OUT/overlap_report_${name}.txt"
fi
