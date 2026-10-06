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

# Sourced by per_test_cov.sh and run_mutants.sh. Collection needs the tools pinned
# in beam_rust_builder, which sets BEAM_RUST_BUILDER=1; ALLOW_OUTSIDE_BUILDER=1
# overrides on Linux hosts with the same tools.
# Usage: require_builder <script-name> <gradle command to suggest>

require_builder() {
  if [ "${BEAM_RUST_BUILDER:-}" = 1 ] || [ "${ALLOW_OUTSIDE_BUILDER:-}" = 1 ]; then
    return 0
  fi
  cat >&2 <<EOF
$1 must run inside the beam_rust_builder container.
Use the Gradle entry point, which starts the container for you:

  $2

If you are already in a builder container started from an image built before
BEAM_RUST_BUILDER was added, rebuild it: ./gradlew :sdks:rust:buildBuilderImage
On a Linux host with the same tools, set ALLOW_OUTSIDE_BUILDER=1 to override.
EOF
  exit 78
}
