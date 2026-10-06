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

//! The `beam:transforms:xlang:test:*` transforms of the Java test expansion service
//! (`sdks/java/testing/expansion-service`), with the inputs and expected values of
//! `ValidateRunnerXlangTest.java`.
//!
//! `./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaServicePrism` starts the service and
//! sets its address in `BEAM_JAVA_TEST_EXPANSION_ADDR`. Without it, the tests skip.

use beam::testing::TestPipeline;
use tests::*;

use crate::common::{runner, skip_without_docker};

/// Environment variable with the `host:port` of the Java test expansion service.
const EXPANSION_ADDR_VAR: &str = "BEAM_JAVA_TEST_EXPANSION_ADDR";

/// Builds the pipeline of `test` against the service of `EXPANSION_ADDR_VAR` and runs it on
/// Prism. Skips when the variable is not set or Docker is not reachable.
async fn run(test: &str, build: fn(&TestPipeline, &str)) {
    let Ok(endpoint) = std::env::var(EXPANSION_ADDR_VAR) else {
        eprintln!(
            "Skipping {test}: {EXPANSION_ADDR_VAR} is not set. Run \
             ./gradlew :sdks:rust:validatesCrossLanguageRunnerJavaServicePrism."
        );
        return;
    };
    if skip_without_docker(test) {
        return;
    }
    let p = test_pipeline();
    build(&p, &endpoint);
    Expectation::Succeeds.check(test, RunnerKind::Prism, p.run_with(&runner()).await);
}

#[tokio::test]
async fn test_prefix() {
    run("test_prefix", build_xlang_prefix).await;
}

#[tokio::test]
async fn test_multi_input_output_with_side_input() {
    run(
        "test_multi_input_output_with_side_input",
        build_xlang_multi_input_output_with_side_input,
    )
    .await;
}

#[tokio::test]
async fn test_group_by_key() {
    run("test_group_by_key", build_xlang_group_by_key).await;
}

#[tokio::test]
#[ignore = "Prism reads only the count form of beam:coder:iterable:v1, so it splits the \
            chunked iterables of the Java output into wrong elements"]
async fn test_co_group_by_key() {
    run("test_co_group_by_key", build_xlang_co_group_by_key).await;
}

#[tokio::test]
async fn test_combine_globally() {
    run("test_combine_globally", build_xlang_combine_globally).await;
}

#[tokio::test]
async fn test_combine_per_key() {
    run("test_combine_per_key", build_xlang_combine_per_key).await;
}

#[tokio::test]
async fn test_flatten() {
    run("test_flatten", build_xlang_flatten).await;
}

#[tokio::test]
async fn test_partition() {
    run("test_partition", build_xlang_partition).await;
}
