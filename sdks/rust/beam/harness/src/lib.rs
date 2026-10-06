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

pub mod bundle_processor;
pub mod control;
pub mod data;
pub mod grpc;
pub mod logging;
#[doc(hidden)]
pub mod provisioning;
pub mod replay;
pub mod state;
pub mod status;
pub mod user_state;
pub mod worker;

pub use model;

pub use bundle_processor::BundleProcessor;
pub use control::ControlClient;
pub use data::DataManager;
pub use logging::{
    BeamFnLoggingHandle, BeamFnLoggingLayer, LoggingClient, create_layer, init_logging,
    set_global_client,
};
pub use state::{FnApiSideInputReader, StateChannel};
pub use status::{WorkerMetrics, WorkerStatusHandler, format_status_info};
pub use user_state::BundleUserState;
pub use worker::Worker;

use beam::options::{HarnessOptions, PipelineOptions, WorkerOptions};
use beam::pipeline::Pipeline;
use beam::runners::{
    PipelineResult, PipelineRunner, RunnerError, RunnerRegistration, WORKER_RUNNER_NAME,
};

/// Serves a pipeline to a runner over the Fn API instead of submitting it.
///
/// [`beam::runners::run`] selects it when the process starts with worker flags. The pipeline
/// is then rebuilt from the driver's options, so [`Pipeline::transform_handlers`] holds
/// exactly the DoFns the runner asks for.
struct WorkerRunner {
    args: HarnessOptions,
    /// `--element_processing_timeout_minutes`, from the job's options.
    element_processing_timeout: Option<std::time::Duration>,
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
        init_logging();
        if let Some(timeout) = self.element_processing_timeout {
            bundle_processor::set_element_processing_timeout(timeout);
        }
        let handlers = pipeline.transform_handlers();
        Worker::with_handlers(self.args.clone(), handlers)
            .run()
            .await?;

        Ok(PipelineResult::new(
            self.args.id.clone().unwrap_or_default(),
            "DONE",
        ))
    }
}

inventory::submit! {
    RunnerRegistration {
        name: WORKER_RUNNER_NAME,
        factory: |options: &PipelineOptions| Box::new(WorkerRunner {
            args: options.harness.clone(),
            element_processing_timeout: options
                .view_as::<WorkerOptions>()
                .ok()
                .and_then(|worker| worker.element_processing_timeout_minutes)
                .filter(|&minutes| minutes > 0)
                .map(|minutes| std::time::Duration::from_secs(minutes * 60)),
        }),
    }
}
