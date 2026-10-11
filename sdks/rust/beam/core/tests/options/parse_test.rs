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

//! Tests that parse the option group of a pipeline together with the core options. On a
//! driver, the command line also has flags for runners, I/O connectors and launchers. On a
//! worker, the options come from the options snapshot of the driver.

use std::path::PathBuf;

use beam::options::{
    GroupSnapshot, OptionsError, OptionsSnapshot, ParseError, PipelineOptionGroup, PipelineOptions,
    PortableOptions, WorkerOptions, try_parse_from,
};
use clap::Args;
use clap::error::ErrorKind;
use serde::{Deserialize, Serialize};

/// Stands in for the argument struct of an example, such as `examples/wordcount`.
#[derive(Args, Serialize, Deserialize, Clone, Debug, PartialEq)]
#[command(version)]
struct AppArgs {
    #[arg(long, default_value = "in.txt")]
    input: String,

    #[arg(long, default_value = "/tmp/output.txt")]
    output: String,

    #[arg(long, default_value_t = 32)]
    batch_size: usize,
}

impl PipelineOptionGroup for AppArgs {}

fn parse(args: &[&str]) -> Result<(PipelineOptions, AppArgs), ParseError> {
    try_parse_from::<AppArgs, _, _>(args.iter().copied())
}

/// Returns a file path that is unique to this test process.
fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("beam_parse_test_{}_{name}", std::process::id()))
}

#[test]
fn parses_core_options_and_the_pipeline_group() {
    let (options, args) = parse(&[
        "app",
        "--runner=dataflow",
        "--output=gs://b/out",
        "--batch_size=8",
    ])
    .expect("valid command line");

    assert_eq!(options.runner, "dataflow");
    assert_eq!(args.output, "gs://b/out");
    assert_eq!(args.batch_size, 8);
    assert_eq!(options.view_as::<AppArgs>().expect("recorded"), args);
}

#[test]
fn flags_are_accepted_with_either_word_separator() {
    let (options, snake) = parse(&["app", "--batch_size=8", "--job_name=a"]).expect("snake_case");
    let (_, kebab) = parse(&["app", "--batch-size=8", "--job-name=a"]).expect("kebab-case");

    assert_eq!(snake, kebab);
    assert_eq!(options.job_name.as_deref(), Some("a"));
    let worker: WorkerOptions = parse(&["app", "--num-workers=2"])
        .expect("kebab-case")
        .0
        .view_as()
        .expect("parses");
    assert_eq!(worker.num_workers, Some(2));
}

#[test]
fn flags_after_an_undeclared_flag_are_still_parsed() {
    let (options, args) = parse(&[
        "app",
        "--machine_type=n2-standard-2",
        "--runner=dataflow",
        "--output=gs://bucket/counts",
    ])
    .expect("undeclared flags are left for other groups");

    assert_eq!(options.runner, "dataflow");
    assert_eq!(args.output, "gs://bucket/counts");
}

/// Uses the exact flag set that the Dataflow Flex Template Go launcher passes. The launcher
/// sorts the flags alphabetically.
#[test]
fn flex_template_launcher_command_line_selects_the_requested_runner() {
    let (options, args) = parse(&[
        "/opt/apache/beam/worker_binary",
        "--job_name=wordcount-flex",
        "--machine_type=n2-standard-2",
        "--network=default",
        "--output=gs://bucket/counts",
        "--project=my-project",
        "--region=us-central1",
        "--runner=DataflowRunner",
        "--sdk_container_image=example/wordcount-flex:1.0",
        "--service_account_email=sa@my-project.iam.gserviceaccount.com",
        "--staging_location=gs://bucket/staging",
        "--subnetwork=regions/us-central1/subnetworks/default",
        "--temp_location=gs://bucket/temp",
        "--template_location=gs://bucket/staging/template_launches/job_object",
    ])
    .expect("launcher command line parses");

    assert_eq!(options.runner, "DataflowRunner");
    assert_eq!(options.job_name.as_deref(), Some("wordcount-flex"));
    assert_eq!(args.output, "gs://bucket/counts");
    let worker: WorkerOptions = options.view_as().expect("worker options parse");
    assert_eq!(
        worker.sdk_container_image.as_deref(),
        Some("example/wordcount-flex:1.0")
    );
}

#[test]
fn space_separated_value_of_an_undeclared_flag_is_dropped_with_it() {
    let (options, args) = parse(&[
        "app",
        "--project",
        "my-project",
        "--input",
        "gs://bucket/in.txt",
    ])
    .expect("valid command line");

    assert_eq!(args.input, "gs://bucket/in.txt");
    assert_eq!(options.runner, "prism");
}

#[test]
fn bad_value_for_a_declared_flag_is_reported() {
    let err = parse(&["app", "--streaming=maybe", "--output=out"])
        .expect_err("an invalid boolean must not be silently ignored");

    assert!(matches!(err, ParseError::Cli(e) if e.kind() == ErrorKind::InvalidValue));
}

#[test]
fn help_and_version_survive_the_filter() {
    let help = parse(&["app", "--help"]).expect_err("--help stops parsing to print usage");
    assert!(matches!(help, ParseError::Cli(e) if e.kind() == ErrorKind::DisplayHelp));

    let version = parse(&["app", "--version"]).expect_err("--version prints the version");
    assert!(matches!(version, ParseError::Cli(e) if e.kind() == ErrorKind::DisplayVersion));
}

#[test]
fn other_groups_read_their_flags_from_the_same_command_line() {
    let (options, _) = parse(&[
        "app",
        "--environment_type=DOCKER",
        "--runner=prism",
        "--output=out",
    ])
    .expect("valid command line");

    let portable: PortableOptions = options.view_as().expect("portable options parse");
    assert!(portable.is_docker());
}

#[test]
fn invalid_values_of_other_groups_are_reported_when_read() {
    let (options, _) = parse(&["app", "--num_workers=10", "--max_num_workers=3"])
        .expect("the pipeline group itself is valid");

    assert!(matches!(
        options.view_as::<WorkerOptions>(),
        Err(OptionsError::Validation { .. })
    ));
}

#[test]
fn worker_restores_the_drivers_options_from_the_snapshot() {
    let (driver, driver_args) = parse(&[
        "app",
        "--runner=dataflow",
        "--job_name=restored",
        "--output=gs://b/out",
        "--batch_size=64",
        "--environment_type=DOCKER",
    ])
    .expect("valid command line");
    let _: PortableOptions = driver.view_as().expect("portable options parse");

    let path = temp_path("snapshot.json");
    std::fs::write(&path, driver.snapshot().expect("snapshot").encode()).expect("writable");

    let options_file = format!("--options_file={}", path.display());
    let (worker, worker_args) = parse(&["worker_binary", "--worker", "--id=1", &options_file])
        .expect("a worker restores its options");

    assert!(worker.harness.is_worker());
    assert_eq!(worker.harness.id.as_deref(), Some("1"));
    assert_eq!(worker.runner, "dataflow");
    assert_eq!(worker.job_name.as_deref(), Some("restored"));
    assert_eq!(worker_args, driver_args);
    let portable: PortableOptions = worker.view_as().expect("restored from the snapshot");
    assert!(portable.is_docker());
}

#[test]
fn worker_ignores_its_command_line_beyond_harness_flags() {
    let (driver, _) = parse(&["app", "--output=from-driver"]).expect("valid command line");
    let path = temp_path("ignored.json");
    std::fs::write(&path, driver.snapshot().expect("snapshot").encode()).expect("writable");

    let options_file = format!("--options_file={}", path.display());
    let (_, args) = parse(&[
        "worker_binary",
        "--worker",
        &options_file,
        "--output=from-worker",
    ])
    .expect("a worker restores its options");

    assert_eq!(args.output, "from-driver");
}

#[test]
fn worker_without_a_snapshot_is_an_error() {
    let err = parse(&["worker_binary", "--worker"]).expect_err("workers need the snapshot");

    assert!(matches!(
        err,
        ParseError::Options(OptionsError::Snapshot { .. })
    ));
}

#[test]
fn worker_does_not_validate_driver_side_checks() {
    // `worker_binary` names a file that exists on the submitting machine only.
    const DRIVER_ONLY: &str = "/only/on/the/driver";
    let (driver, _) = parse(&["app"]).expect("valid command line");
    let snapshot = driver.snapshot().expect("snapshot");
    let with_driver_path = OptionsSnapshot::from_groups(snapshot.groups().map(|(key, group)| {
        let group = if group.values().contains_key("worker_binary") {
            let mut values = group.values().clone();
            values.insert("worker_binary".to_string(), DRIVER_ONLY.into());
            GroupSnapshot::of(group.namespace(), &values).expect("a JSON object")
        } else {
            group.clone()
        };
        (key.to_string(), group)
    }));
    let path = temp_path("unvalidated.json");
    std::fs::write(&path, with_driver_path.encode()).expect("writable");

    let options_file = format!("--options_file={}", path.display());
    let (worker, _) = parse(&["worker_binary", "--worker", &options_file]).expect("restores");

    let restored: WorkerOptions = worker.view_as().expect("not validated on the worker");
    assert_eq!(restored.worker_binary.as_deref(), Some(DRIVER_ONLY));
}
