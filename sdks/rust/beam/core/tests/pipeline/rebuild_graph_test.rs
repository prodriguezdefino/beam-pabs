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

//! The graph check of the rebuild path: a worker compares its rebuilt graph with the
//! record of the launcher.

use beam_core::internals::PipelineInternals;
use beam_core::options::{HarnessOptions, OptionsSnapshot, PipelineOptions};
use beam_core::prelude::*;
use beam_core::runners::RunnerError;

/// The options that a worker restores from the snapshot of `launcher`.
fn worker_options(launcher: &PipelineOptions) -> PipelineOptions {
    let encoded = launcher.snapshot().unwrap().encode();
    let harness = HarnessOptions {
        worker: true,
        ..HarnessOptions::default()
    };
    PipelineOptions::from_snapshot(OptionsSnapshot::decode(&encoded).unwrap(), harness).unwrap()
}

/// Two sources, and a step `Step` that reads the source `read`.
fn build(p: &Pipeline, read: &str) {
    let a = p.apply("A", Create::new(vec![1_i64]));
    let b = p.apply("B", Create::new(vec![3_i64]));
    let input = if read == "A" { a } else { b };
    let _ = input.apply("Step", Map::new(|x: i64| x + 1));
}

/// Builds `launcher_read` on the launcher and `worker_read` on a worker that gets the
/// recorded options, then runs the check of the worker.
fn check(launcher_read: &str, worker_read: &str, record: bool) -> Result<(), RunnerError> {
    let options = PipelineOptions::default();
    let launcher = Pipeline::create(&options);
    build(&launcher, launcher_read);
    if record {
        launcher.record_rebuild_graph(&options).unwrap();
    }

    let options = worker_options(&options);
    let worker = Pipeline::create(&options);
    build(&worker, worker_read);
    worker.check_rebuild_graph(&options)
}

#[test]
fn a_worker_that_builds_the_launcher_graph_passes() {
    check("A", "A", true).unwrap();
}

#[test]
fn a_worker_that_wires_a_step_to_another_input_fails_and_names_the_step() {
    let error = check("A", "B", true).unwrap_err().to_string();
    assert!(error.contains("transform 'Step' has the inputs"), "{error}");
    assert!(
        error.contains("A.out") && error.contains("B.out"),
        "{error}"
    );
}

#[test]
fn a_worker_without_a_record_skips_the_check() {
    check("A", "B", false).unwrap();
}

#[test]
fn a_worker_without_a_transform_of_the_record_fails() {
    let options = PipelineOptions::default();
    let launcher = Pipeline::create(&options);
    build(&launcher, "A");
    let _ = launcher
        .apply("C", Create::new(vec![5_i64]))
        .apply("Extra", Map::new(|x: i64| x * 2));
    launcher.record_rebuild_graph(&options).unwrap();

    let options = worker_options(&options);
    let worker = Pipeline::create(&options);
    build(&worker, "A");
    let error = worker
        .check_rebuild_graph(&options)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("transform 'Extra' is missing on the worker"),
        "{error}"
    );
}
