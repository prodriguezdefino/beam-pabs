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

//! Tests for `TestPipeline` configuration from `BEAM_TEST_PIPELINE_OPTIONS`.
//!
//! This test is in its own test binary, as the only test, because it mutates the
//! process environment.

use beam::options::PipelineOptions;
use testing::{TEST_PIPELINE_OPTIONS_ENV, TestPipeline, test_pipeline_options};

#[test]
fn new_reads_the_environment_and_with_options_ignores_it() {
    // SAFETY: No other test runs in this binary, so no concurrent environment access occurs.
    unsafe { std::env::set_var(TEST_PIPELINE_OPTIONS_ENV, "--runner=from-env --job_name=j") };

    assert_eq!(test_pipeline_options().runner, "from-env");
    let from_env = TestPipeline::new().without_run_enforcement();
    assert_eq!(from_env.options().runner, "from-env");
    assert_eq!(from_env.options().job_name.as_deref(), Some("j"));

    let explicit = TestPipeline::with_options(PipelineOptions::with_runner("custom"))
        .without_run_enforcement();
    assert_eq!(explicit.options().runner, "custom");
    assert_eq!(explicit.options().job_name, None);

    // SAFETY: Same as above.
    unsafe { std::env::remove_var(TEST_PIPELINE_OPTIONS_ENV) };
    assert_eq!(
        TestPipeline::new().options().runner,
        PipelineOptions::default().runner
    );
}
