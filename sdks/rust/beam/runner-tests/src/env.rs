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

//! Runners configured from the environment the Gradle tasks set up.

use std::time::{SystemTime, UNIX_EPOCH};

use beam::options::PipelineOptions;
use beam::runners::dataflow::TestDataflowRunner;
use prism::PrismRunner;

/// The value of the first of `names` that is set.
fn first_var(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| std::env::var(name).ok())
}

/// Whether the first of `names` that is set reads as true (`true` or `1`).
fn first_flag(names: &[&str]) -> bool {
    first_var(names).is_some_and(|v| v == "true" || v == "1")
}

/// Whether `runner_var` (such as `BEAM_PRISM_STREAMING`), or else `BEAM_STREAMING`, is set.
pub fn streaming_from_env(runner_var: &str) -> bool {
    first_flag(&[runner_var, "BEAM_STREAMING"])
}

/// A Prism runner, streaming when `BEAM_PRISM_STREAMING` or `BEAM_STREAMING` says so.
pub fn prism_runner_from_env() -> PrismRunner {
    if streaming_from_env("BEAM_PRISM_STREAMING") {
        // `PipelineOptions` has private fields, so it cannot be built with `..Default`.
        let mut options = PipelineOptions::default();
        options.streaming = true;
        PrismRunner::from(&options)
    } else {
        PrismRunner::new()
    }
}

/// A `TestDataflowRunner` for the ValidatesRunner test `test_id`, configured from the
/// `BEAM_DATAFLOW_*` variables that `./gradlew :sdks:rust:validatesRunnerDataflow` sets.
///
/// The job is named after the test and passes its id as `--vr_test`, so the worker can
/// rebuild the pipeline. Panics when a required variable is missing: the suite is
/// `#[ignore]`d, so a missing setting is an error, not a skip.
pub fn dataflow_runner_from_env(test_id: &str) -> TestDataflowRunner {
    let required = |names: &[&str]| {
        first_var(names).unwrap_or_else(|| {
            panic!(
                "{} is not set; run the Dataflow suite with \
                 `./gradlew :sdks:rust:validatesRunnerDataflow`, which sets it",
                names[0]
            )
        })
    };
    let project = required(&["BEAM_DATAFLOW_PROJECT", "GCP_PROJECT"]);
    let temp_location = required(&["BEAM_DATAFLOW_TEMP_LOCATION", "GCP_TEMP_LOCATION"]);
    let sdk_container_image =
        required(&["BEAM_DATAFLOW_SDK_CONTAINER_IMAGE", "SDK_CONTAINER_IMAGE"]);
    let region = first_var(&["BEAM_DATAFLOW_REGION", "GCP_REGION"])
        .unwrap_or_else(|| "us-central1".to_string());

    let streaming = streaming_from_env("BEAM_DATAFLOW_STREAMING");
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // The mode keeps names unique when the batch and streaming suites run in parallel.
    let mode = if streaming { "streaming" } else { "batch" };
    let job_name = format!("vr-{mode}-{}-{timestamp}", test_id.replace('_', "-"));

    let worker_binary = first_var(&["BEAM_DATAFLOW_WORKER_BINARY", "WORKER_BINARY"])
        .filter(|path| !path.is_empty() && path != "none");
    let optional = [
        worker_binary.map(|path| format!("--worker_binary={path}")),
        first_var(&["BEAM_DATAFLOW_NETWORK"]).map(|n| format!("--network={n}")),
        first_var(&["BEAM_DATAFLOW_SUBNETWORK"]).map(|s| format!("--subnetwork={s}")),
        streaming.then(|| "--streaming=true".to_string()),
    ];

    let args = [
        "validates-runner".to_string(),
        "--runner=TestDataflowRunner".to_string(),
        format!("--project={project}"),
        format!("--region={region}"),
        format!("--temp_location={temp_location}"),
        format!("--sdk_container_image={sdk_container_image}"),
        format!("--job_name={job_name}"),
        format!("--vr_test={test_id}"),
        "--num_workers=1".to_string(),
        "--worker_machine_type=e2-standard-2".to_string(),
    ]
    .into_iter()
    .chain(optional.into_iter().flatten());

    TestDataflowRunner::with_options(PipelineOptions::parse_from(args))
}
