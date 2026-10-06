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

//! `--element_processing_timeout_minutes`: a worker stuck in one transform call past the
//! timeout exits, so the runner can retry the work elsewhere.
//!
//! Exiting ends the process and the timeout is set once per process, so each test re-runs
//! this binary as a child, marked by [`CHILD_ENV`], that sets the timeout and stays put.

use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};

use harness::bundle_processor::{ExecutionSampler, set_element_processing_timeout};

/// Set in the child's environment to the name of the test it should act out.
const CHILD_ENV: &str = "BEAM_ELEMENT_TIMEOUT_TEST_CHILD";

/// How long the parent lets a child run before killing it and failing.
const CHILD_DEADLINE: Duration = Duration::from_secs(20);

/// Whether this process is the child for `test`.
fn is_child_for(test: &str) -> bool {
    std::env::var(CHILD_ENV).is_ok_and(|name| name == test)
}

/// Child side: sets the timeout and stays in a transform for `stuck_for`.
fn act_stuck_element(timeout: Duration, stuck_for: Duration) {
    set_element_processing_timeout(timeout);
    let ids = vec!["stuck_transform".to_string()];
    let sampler = ExecutionSampler::new("inst_stuck", &ids);
    let previous = sampler.enter(0);
    std::thread::sleep(stuck_for);
    sampler.exit(previous);
}

/// Parent side: re-runs this binary restricted to `test` as a child and waits for it.
/// Panics if the child cannot start or outlives [`CHILD_DEADLINE`] (it is killed first).
fn run_child(test: &str) -> ExitStatus {
    let exe = std::env::current_exe().expect("path of the running test binary");
    let mut child = Command::new(exe)
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, test)
        .spawn()
        .expect("spawn the child test process");
    wait_with_deadline(&mut child, test)
}

fn wait_with_deadline(child: &mut Child, test: &str) -> ExitStatus {
    let deadline = Instant::now() + CHILD_DEADLINE;
    loop {
        if let Some(status) = child.try_wait().expect("poll the child test process") {
            return status;
        }
        if Instant::now() >= deadline {
            // Best effort: the child may exit between the poll and the kill.
            let _ = child.kill();
            let _ = child.wait();
            panic!("child for '{test}' still running after {CHILD_DEADLINE:?}; killed it");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_stuck_element_past_the_processing_timeout_terminates_the_worker() {
    const TEST: &str = "a_stuck_element_past_the_processing_timeout_terminates_the_worker";
    if is_child_for(TEST) {
        // Returning here would make the child exit 0.
        act_stuck_element(Duration::from_millis(200), Duration::from_secs(10));
        return;
    }

    let started = Instant::now();
    let status = run_child(TEST);
    assert_eq!(
        status.code(),
        Some(1),
        "the worker must exit 1 on timeout, got {status}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the worker must exit on timeout, not when the element finishes: took {:?}",
        started.elapsed()
    );
}

#[test]
fn an_element_within_the_processing_timeout_is_left_running() {
    const TEST: &str = "an_element_within_the_processing_timeout_is_left_running";
    if is_child_for(TEST) {
        // Outlasts the 2s a terminating worker waits before exiting.
        act_stuck_element(Duration::from_secs(60), Duration::from_secs(3));
        return;
    }

    let status = run_child(TEST);
    assert!(
        status.success(),
        "an element within the timeout must not terminate the worker, got {status}"
    );
}
