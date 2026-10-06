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

//! `ValidatesRunner` suite execution on `PrismRunner`.
//!
//! Runs [`tests::VALIDATES_RUNNER_TESTS`] through [`tests::run_validates_runner_suite`],
//! reporting entries of [`tests::RUNNER_SKIPS`] for Prism as `ignored` unless
//! `--ignored` or `--include-ignored` is passed.

use std::process::ExitCode;

fn main() -> ExitCode {
    let mode = if tests::streaming_from_env("BEAM_PRISM_STREAMING") {
        tests::SuiteMode::Streaming
    } else {
        tests::SuiteMode::Batch
    };
    tests::run_validates_runner_suite(mode, None, |_id| tests::prism_runner_from_env())
}
