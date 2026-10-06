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

# Runs candidates.py and saves its summary; run by `./gradlew :sdks:rust:testQualityReport`.
# Analysis only: needs just python3.
#
# Usage:
#   run_report.sh <summary-file> <candidates.py args...>

set -eo pipefail

if [ "$#" -lt 2 ]; then
  sed -n '/^# Usage:/,$p' "$0" | sed -n '2,3p' | sed 's/^# \{0,1\}//' >&2
  exit 64
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUMMARY="$1"; shift
python3 "$HERE/candidates.py" "$@" | tee "$SUMMARY"
