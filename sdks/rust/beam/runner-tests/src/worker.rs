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

//! Worker binary for Apache Beam Rust ValidatesRunner integration tests.
//!
//! When running on containerized runners like Google Cloud Dataflow, the worker container
//! boots this binary with Fn API flags (`--worker=true`, `--id=...`, `--control_endpoint=...`).
//! Linking `apache-beam-harness`, which the facade does for its runner features, activates `WorkerRunner`, which connects to Fn API endpoints
//! and processes bundle execution for all test transforms and DoFns.
//!
//! Rust closures cannot be serialized, so the worker rebuilds the pipeline of the test named
//! by `--vr_test`: it calls the builder from [`VALIDATES_RUNNER_TESTS`] on a [`TestPipeline`]
//! with the job's options. The submitting test checks the outcome. See
//! `sdks/rust/docs/static-registration.md` for the planned alternative.

use beam::options::PipelineOptions;
use beam::pipeline::Pipeline;
use beam::testing::TestPipeline;
use tests::{VALIDATES_RUNNER_TESTS, ValidatesRunnerOptions, find_validates_runner_test};

/// The pipeline this worker executes: the one of the test named by `--vr_test`.
fn pipeline_for(options: &PipelineOptions) -> Pipeline {
    let test_id = options
        .view_as::<ValidatesRunnerOptions>()
        .ok()
        .and_then(|o| o.vr_test);
    let Some(test_id) = test_id else {
        tracing::warn!("No --vr_test option; running an empty pipeline");
        return Pipeline::create(options);
    };
    let Some(test) = find_validates_runner_test(&test_id) else {
        let known: Vec<&str> = VALIDATES_RUNNER_TESTS.iter().map(|t| t.id).collect();
        tracing::warn!("Unknown --vr_test '{test_id}'; known tests: {known:?}");
        return Pipeline::create(options);
    };
    // The worker executes the pipeline through `beam::runners::run`, not `run_with`, so
    // the test pipeline must not insist on being run.
    let test_pipeline = TestPipeline::with_options(options.clone()).without_run_enforcement();
    (test.build)(&test_pipeline);
    let pipeline = test_pipeline.pipeline().clone();
    tracing::info!(
        "Rebuilt the pipeline of test '{test_id}' with {} transform handlers",
        pipeline.transform_handlers().len()
    );
    pipeline
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Log level comes from RUST_LOG; `init_logging` defaults to INFO when it is unset.
    beam::harness::init_logging();
    let options = PipelineOptions::from_args();
    let pipeline = pipeline_for(&options);
    beam::runners::run(&pipeline, &options).await?;
    Ok(())
}
