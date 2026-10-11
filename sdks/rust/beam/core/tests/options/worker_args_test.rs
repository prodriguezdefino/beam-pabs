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

//! Tests that a pipeline binary parses the Fn API worker flags a runner's boot program
//! passes it.
//!
//! There is no separate worker entry point: [`HarnessOptions`] is flattened into
//! `PipelineOptions`, so every pipeline binary accepts the worker flags. The job's own
//! options then come from the snapshot named by `--options_file`, not from these flags.

use std::path::PathBuf;

use beam::options::{HarnessOptions, PipelineOptions};

fn some(value: &str) -> Option<String> {
    Some(value.to_string())
}

/// Writes the snapshot of a driver with `driver_args` and returns its path. A worker needs it.
fn driver_snapshot(name: &str, driver_args: &[&str]) -> PathBuf {
    let driver = PipelineOptions::try_parse_from(driver_args.iter().copied()).expect("driver");
    let path = std::env::temp_dir().join(format!(
        "beam_worker_args_test_{}_{name}.json",
        std::process::id()
    ));
    std::fs::write(&path, driver.snapshot().expect("snapshot").encode()).expect("writable");
    path
}

#[test]
fn worker_flags_parse_into_harness_options() {
    let prism = driver_snapshot("prism", &["wordcount"]);
    let dataflow = driver_snapshot("dataflow", &["wordcount", "--runner=dataflow"]);
    let prism_file = format!("--options_file={}", prism.display());
    let dataflow_file = format!("--options_file={}", dataflow.display());
    let cases: Vec<(&str, Vec<&str>, &str, HarnessOptions)> = vec![
        (
            "driver invocation",
            vec!["wordcount", "--input=in.txt", "--output=counts.txt"],
            "prism",
            HarnessOptions::default(),
        ),
        (
            "bare --worker",
            vec!["wordcount", "--worker", &prism_file],
            "prism",
            HarnessOptions {
                worker: true,
                options_file: Some(prism.clone()),
                ..Default::default()
            },
        ),
        (
            "--worker=false",
            vec!["wordcount", "--worker=false", "--runner=prism"],
            "prism",
            HarnessOptions::default(),
        ),
        (
            "snake_case endpoints",
            vec![
                "wordcount",
                "--worker",
                &prism_file,
                "--id=worker-42",
                "--control_endpoint=http://127.0.0.1:50001",
                "--logging_endpoint=http://127.0.0.1:50002",
                "--status_endpoint=http://127.0.0.1:50003",
                "--artifact_endpoint=http://127.0.0.1:50004",
            ],
            "prism",
            HarnessOptions {
                worker: true,
                id: some("worker-42"),
                control_endpoint: some("http://127.0.0.1:50001"),
                logging_endpoint: some("http://127.0.0.1:50002"),
                status_endpoint: some("http://127.0.0.1:50003"),
                artifact_endpoint: some("http://127.0.0.1:50004"),
                options_file: Some(prism.clone()),
                ..Default::default()
            },
        ),
        // Runners are inconsistent about the separator, so both spellings must work.
        (
            "kebab-case endpoints",
            vec![
                "wordcount",
                "--worker",
                &prism_file,
                "--id=worker-kebab",
                "--control-endpoint=http://127.0.0.1:50001",
                "--logging-endpoint=http://127.0.0.1:50002",
            ],
            "prism",
            HarnessOptions {
                worker: true,
                id: some("worker-kebab"),
                control_endpoint: some("http://127.0.0.1:50001"),
                logging_endpoint: some("http://127.0.0.1:50002"),
                options_file: Some(prism),
                ..Default::default()
            },
        ),
        // Only `--worker` selects worker mode. The container's boot program always passes
        // it, so endpoint flags alone leave a binary in driver mode.
        (
            "control endpoint alone",
            vec![
                "wordcount",
                "--id=container-worker-42",
                "--control_endpoint=10.0.0.1:12345",
                "--semi_persist_dir=/custom_tmp",
            ],
            "prism",
            HarnessOptions {
                id: some("container-worker-42"),
                control_endpoint: some("10.0.0.1:12345"),
                semi_persist_dir: "/custom_tmp".to_string(),
                ..Default::default()
            },
        ),
        (
            "provision endpoint alone",
            vec!["wordcount", "--provision_endpoint=127.0.0.1:54321"],
            "prism",
            HarnessOptions {
                provision_endpoint: some("127.0.0.1:54321"),
                ..Default::default()
            },
        ),
        (
            "worker flags next to core flags",
            vec![
                "wordcount",
                "--worker=true",
                &dataflow_file,
                "--id=worker-extra",
                "--control_endpoint=localhost:50000",
                "--input=gs://bucket/in.txt",
                "--output=gs://bucket/out.txt",
                "--runner=dataflow",
            ],
            "dataflow",
            HarnessOptions {
                worker: true,
                id: some("worker-extra"),
                control_endpoint: some("localhost:50000"),
                options_file: Some(dataflow),
                ..Default::default()
            },
        ),
    ];

    for (name, args, runner, expected) in cases {
        let options = PipelineOptions::try_parse_from(args).expect(name);
        assert_eq!(options.harness, expected, "{name}");
        assert_eq!(options.runner, runner, "{name}");
    }
}

/// `HarnessOptions::default()` is hand-written because clap `default_value` does not apply
/// to `Default`. The table above compares parsed values with it, so it must hold the clap
/// default of `semi_persist_dir`.
#[test]
fn default_semi_persist_dir_is_tmp() {
    assert_eq!(HarnessOptions::default().semi_persist_dir, "/tmp");
}
