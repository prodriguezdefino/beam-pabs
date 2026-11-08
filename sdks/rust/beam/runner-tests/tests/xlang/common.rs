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

//! Helpers of the cross-language tests.

use beam::testing::TestPipeline;
use beam_runner_prism::PrismRunner;
use beam_runner_tests::{Expectation, RunnerKind, test_pipeline};

/// A Prism runner for one test.
pub fn runner() -> PrismRunner {
    PrismRunner::new()
}

/// A test expansion service that a Gradle task starts for the tests.
#[derive(Clone, Copy)]
pub enum TestService {
    /// `sdks/java/testing/expansion-service`.
    Java,
    /// `apache_beam.runners.portability.expansion_service_test`.
    Python,
}

impl TestService {
    /// Environment variable with the `host:port` of the service.
    const fn address_var(self) -> &'static str {
        match self {
            Self::Java => "BEAM_JAVA_TEST_EXPANSION_ADDR",
            Self::Python => "BEAM_PYTHON_TEST_EXPANSION_ADDR",
        }
    }

    /// Gradle task that starts the service and runs the tests.
    const fn gradle_task(self) -> &'static str {
        match self {
            Self::Java => "validatesCrossLanguageRunnerJavaServicePrism",
            Self::Python => "validatesCrossLanguageRunnerPythonServicePrism",
        }
    }
}

/// Builds the pipeline of `test` against `service` and runs it on Prism.
/// Skips when the service address is not set or Docker is not reachable.
pub async fn run_against(service: TestService, test: &str, build: fn(&TestPipeline, &str)) {
    let var = service.address_var();
    let Ok(endpoint) = std::env::var(var) else {
        eprintln!(
            "Skipping {test}: {var} is not set. Run ./gradlew :sdks:rust:{}.",
            service.gradle_task()
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

/// Whether a Docker daemon is reachable, which external SDK container environments need.
/// Asks the `docker` CLI, so contexts such as Colima's are honored.
fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Returns `true` with a skip message for `test` when no Docker daemon is reachable.
pub fn skip_without_docker(test: &str) -> bool {
    let skip = !docker_available();
    if skip {
        eprintln!(
            "Skipping {test}: no reachable Docker daemon (`docker info` failed); the external SDK \
             environment needs one."
        );
    }
    skip
}
