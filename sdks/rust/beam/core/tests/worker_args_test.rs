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

use beam::options::{HarnessOptions, PipelineOptions};

fn parse(args: &[&str]) -> PipelineOptions {
    PipelineOptions::try_parse_from(args.iter().copied()).expect("valid command line")
}

#[test]
fn driver_invocation_is_not_worker_mode() {
    let args = parse(&["wordcount", "--input=in.txt", "--output=counts.txt"]);

    assert!(!args.harness.is_worker());
    assert_eq!(args.harness, HarnessOptions::default());
}

#[test]
fn bare_worker_flag_enables_worker_mode() {
    let args = parse(&["wordcount", "--worker"]);

    assert!(args.harness.is_worker());
    assert_eq!(
        args.harness,
        HarnessOptions {
            worker: true,
            ..Default::default()
        }
    );
}

#[test]
fn worker_false_stays_in_driver_mode() {
    let args = parse(&["wordcount", "--worker=false", "--runner=prism"]);

    assert!(!args.harness.is_worker());
    assert_eq!(args.runner, "prism");
}

#[test]
fn endpoints_parse_in_snake_case() {
    let args = parse(&[
        "wordcount",
        "--worker",
        "--id=worker-42",
        "--control_endpoint=http://127.0.0.1:50001",
        "--logging_endpoint=http://127.0.0.1:50002",
        "--status_endpoint=http://127.0.0.1:50003",
        "--artifact_endpoint=http://127.0.0.1:50004",
    ]);

    assert_eq!(
        args.harness,
        HarnessOptions {
            worker: true,
            id: Some("worker-42".to_string()),
            control_endpoint: Some("http://127.0.0.1:50001".to_string()),
            logging_endpoint: Some("http://127.0.0.1:50002".to_string()),
            status_endpoint: Some("http://127.0.0.1:50003".to_string()),
            artifact_endpoint: Some("http://127.0.0.1:50004".to_string()),
            ..Default::default()
        }
    );
}

/// Runners are inconsistent about the separator, so both spellings must work.
#[test]
fn endpoints_parse_in_kebab_case() {
    let args = parse(&[
        "wordcount",
        "--worker",
        "--id=worker-kebab",
        "--control-endpoint=http://127.0.0.1:50001",
        "--logging-endpoint=http://127.0.0.1:50002",
    ]);

    assert_eq!(args.harness.id.as_deref(), Some("worker-kebab"));
    assert_eq!(
        args.harness.control_endpoint.as_deref(),
        Some("http://127.0.0.1:50001")
    );
    assert_eq!(
        args.harness.logging_endpoint.as_deref(),
        Some("http://127.0.0.1:50002")
    );
}

#[test]
fn worker_id_alias_is_accepted() {
    let args = parse(&["wordcount", "--worker", "--worker_id=aliased"]);

    assert_eq!(args.harness.id.as_deref(), Some("aliased"));
}

/// Only `--worker` selects worker mode. The container's boot program always passes it, so
/// endpoint flags alone leave a binary in driver mode.
#[test]
fn control_endpoint_alone_does_not_imply_worker_mode() {
    let args = parse(&[
        "wordcount",
        "--id=container-worker-42",
        "--control_endpoint=10.0.0.1:12345",
        "--semi_persist_dir=/custom_tmp",
    ]);

    assert!(!args.harness.is_worker());
    assert_eq!(
        args.harness.control_endpoint.as_deref(),
        Some("10.0.0.1:12345")
    );
    assert_eq!(args.harness.semi_persist_dir, "/custom_tmp");
}

#[test]
fn provision_endpoint_alone_does_not_imply_worker_mode() {
    let args = parse(&["wordcount", "--provision_endpoint=127.0.0.1:54321"]);

    assert!(!args.harness.is_worker());
    assert_eq!(
        args.harness.provision_endpoint.as_deref(),
        Some("127.0.0.1:54321")
    );
}

#[test]
fn semi_persist_dir_defaults_to_tmp() {
    let args = parse(&["wordcount", "--worker"]);

    assert_eq!(args.harness.semi_persist_dir, "/tmp");
}

/// Worker flags parse next to core flags on the same command line.
#[test]
fn worker_flags_coexist_with_driver_flags() {
    let args = parse(&[
        "wordcount",
        "--worker=true",
        "--id=worker-extra",
        "--control_endpoint=localhost:50000",
        "--input=gs://bucket/in.txt",
        "--output=gs://bucket/out.txt",
        "--runner=dataflow",
    ]);

    assert!(args.harness.is_worker());
    assert_eq!(args.harness.id.as_deref(), Some("worker-extra"));
    assert_eq!(args.runner, "dataflow");
}

/// `HarnessOptions::default()` is hand-written because clap `default_value` does not apply
/// to `Default`. If the two disagree on `semi_persist_dir`, a worker uses the wrong directory.
#[test]
fn default_impl_matches_parsed_defaults() {
    let parsed = parse(&["wordcount"]).harness;

    assert_eq!(parsed, HarnessOptions::default());
}
