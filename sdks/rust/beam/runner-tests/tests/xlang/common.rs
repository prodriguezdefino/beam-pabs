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

use beam_runner_prism::PrismRunner;

/// A Prism runner for one test.
pub fn runner() -> PrismRunner {
    PrismRunner::new()
}

/// Whether a Docker daemon is reachable, which the Java SDK environment needs. Asks the
/// `docker` CLI, so contexts such as Colima's are honored.
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
            "Skipping {test}: no reachable Docker daemon (`docker info` failed); the Java SDK \
             environment needs one."
        );
    }
    skip
}
