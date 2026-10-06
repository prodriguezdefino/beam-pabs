/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! `ValidatesRunner` suite execution on Google Cloud Dataflow (`DataflowRunner`).
//!
//! Marked `test = false` in `Cargo.toml` so `:sdks:rust:test` does not submit cloud jobs;
//! run with `./gradlew :sdks:rust:validatesRunnerDataflow` (or `...DataflowStreaming`).
//! Entries in [`beam_runner_tests::RUNNER_SKIPS`] for Dataflow report as `ignored` per mode and can
//! run on demand with `-Pignored` or `-PincludeIgnored`.

use std::process::ExitCode;

fn main() -> ExitCode {
    let mode = if beam_runner_tests::streaming_from_env("BEAM_DATAFLOW_STREAMING") {
        beam_runner_tests::SuiteMode::Streaming
    } else {
        beam_runner_tests::SuiteMode::Batch
    };
    beam_runner_tests::run_validates_runner_suite(
        mode,
        Some(4),
        beam_runner_tests::dataflow_runner_from_env,
    )
}
