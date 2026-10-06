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

//! Cross-language transform integration test suite.
//!
//! Verifies end-to-end execution of cross-language transforms by expanding against
//! a Java expansion service and executing with the containerized Java SDK harness.

use prism::PrismRunner;
use tests::*;

fn runner() -> PrismRunner {
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

#[tokio::test]
async fn test_xlang_generate_sequence() {
    if !docker_available() {
        eprintln!(
            "Skipping test_xlang_generate_sequence: no reachable Docker daemon (`docker info` \
             failed); the Java SDK environment needs one."
        );
        return;
    }
    let p = test_pipeline();
    build_xlang_generate_sequence(&p, external::expansionx::IO_EXPANSION_SERVICE_TARGET);
    Expectation::Succeeds.check("xlang_generate_sequence", p.run_with(&runner()).await);
}
