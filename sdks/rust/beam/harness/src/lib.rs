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

//! Apache Beam Rust worker harness: the worker side of the Fn API (control, data, state and
//! logging streams, and bundle execution).
//!
//! Modules other than [`worker`] and [`logging`] are internal runner and test plumbing and
//! are not part of the stable public API.

#[doc(hidden)]
pub mod bundle_processor;
mod bundle_threads;
#[doc(hidden)]
pub mod control;
#[doc(hidden)]
pub mod data;
#[doc(hidden)]
pub mod grpc;
pub mod logging;
#[doc(hidden)]
pub mod memory;
#[doc(hidden)]
pub mod provisioning;
#[doc(hidden)]
pub mod replay;
#[doc(hidden)]
pub mod state;
#[doc(hidden)]
pub mod status;
#[doc(hidden)]
pub mod user_state;
pub mod worker;

pub use beam_model as model;
pub use bundle_processor::SamplingMode;

use beam::internals::PipelineInternals;
use beam::options::{HarnessOptions, PipelineOptions};
use beam::pipeline::Pipeline;
use beam::runners::{
    JobState, PipelineResult, PipelineRunner, RunnerError, RunnerRegistration, WORKER_RUNNER_NAME,
};

/// Serves a pipeline to a runner over the Fn API instead of submitting it.
///
/// [`beam::runners::run`] selects it when the process starts with worker flags. The pipeline
/// is then rebuilt from the driver's options, so [`Pipeline::transform_handlers`] holds
/// exactly the DoFns the runner asks for.
struct WorkerRunner {
    args: HarnessOptions,
    config: worker::WorkerConfig,
}

#[async_trait::async_trait]
impl PipelineRunner for WorkerRunner {
    async fn run(&self, pipeline: &Pipeline) -> Result<PipelineResult, RunnerError> {
        self.execute(pipeline).await.map_err(RunnerError::execution)
    }
}

impl WorkerRunner {
    /// Runs `pipeline`, reporting failures with this runner's own error type.
    async fn execute(&self, pipeline: &Pipeline) -> Result<PipelineResult, worker::WorkerError> {
        self.config.apply_to_process();
        worker::Worker::new(self.args.clone())
            .with_config(&self.config)
            .with_handlers(pipeline.transform_handlers())
            .run()
            .await?;

        Ok(PipelineResult::new(
            self.args.id.clone().unwrap_or_default(),
            JobState::Done,
        ))
    }
}

inventory::submit! {
    RunnerRegistration {
        name: WORKER_RUNNER_NAME,
        factory: |options: &PipelineOptions| Box::new(WorkerRunner {
            args: options.harness.clone(),
            config: worker::WorkerConfig::from(options),
        }),
    }
}
