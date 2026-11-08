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

//! The `beam:transforms:xlang:test:*` transforms of the Python test expansion service
//! (`apache_beam.runners.portability.expansion_service_test`), including `python_bs4`.
//!
//! `./gradlew :sdks:rust:validatesCrossLanguageRunnerPythonServicePrism` starts the service
//! and sets its address in `BEAM_PYTHON_TEST_EXPANSION_ADDR`. Without it, the tests skip.

use beam::testing::TestPipeline;
use beam_runner_tests::*;

use crate::common::{TestService, run_against};

async fn run(test: &str, build: fn(&TestPipeline, &str)) {
    run_against(TestService::Python, test, build).await;
}

#[tokio::test]
async fn test_prefix() {
    run("python_service::test_prefix", build_xlang_prefix).await;
}

#[tokio::test]
async fn test_multi_input_output_with_side_input() {
    run(
        "python_service::test_multi_input_output_with_side_input",
        build_xlang_multi_input_output_with_side_input,
    )
    .await;
}

#[tokio::test]
async fn test_group_by_key() {
    run(
        "python_service::test_group_by_key",
        build_xlang_group_by_key,
    )
    .await;
}

#[tokio::test]
async fn test_co_group_by_key() {
    run(
        "python_service::test_co_group_by_key",
        build_xlang_co_group_by_key,
    )
    .await;
}

#[tokio::test]
async fn test_combine_globally() {
    run(
        "python_service::test_combine_globally",
        build_xlang_combine_globally,
    )
    .await;
}

#[tokio::test]
async fn test_combine_per_key() {
    run(
        "python_service::test_combine_per_key",
        build_xlang_combine_per_key,
    )
    .await;
}

#[tokio::test]
async fn test_flatten() {
    run("python_service::test_flatten", build_xlang_flatten).await;
}

#[tokio::test]
async fn test_partition() {
    run("python_service::test_partition", build_xlang_partition).await;
}

#[tokio::test]
async fn test_python_bs4() {
    run("python_service::test_python_bs4", build_xlang_python_bs4).await;
}
