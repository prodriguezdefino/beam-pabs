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

//! The contract between a [`Pipeline`] and an execution engine. Runner implementations live
//! in their own crates under `sdks/rust/beam/runners`: `apache-beam-runner-prism` (local, with
//! the Prism job service) and `apache-beam-runner-dataflow` (Google Cloud Dataflow).
//!
//! # Registry
//!
//! Runners register at link time, so a dependency on a runner crate is enough to select it
//! by name:
//!
//! ```
//! # use beam::runners::{RunnerRegistration, PipelineRunner, PipelineResult, RunnerError};
//! # use beam::pipeline::Pipeline;
//! # struct CustomRunner;
//! # impl CustomRunner { fn new() -> Self { Self } }
//! # #[async_trait::async_trait]
//! # impl PipelineRunner for CustomRunner {
//! #     async fn run(&self, _p: &Pipeline) -> Result<PipelineResult, RunnerError> {
//! #         Ok(PipelineResult::new("test", "DONE"))
//! #     }
//! # }
//! inventory::submit! {
//!     RunnerRegistration {
//!         name: "custom",
//!         factory: |_opts| Box::new(CustomRunner::new()),
//!     }
//! }
//! ```
//!
//! Programs then execute a pipeline without naming a runner type at all:
//!
//! ```no_run
//! # use beam::pipeline::Pipeline;
//! # use beam::options::PipelineOptions;
//! # async fn doc_example(pipeline: Pipeline, options: PipelineOptions) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! beam::runners::run(&pipeline, &options).await?;
//! # Ok(())
//! # }
//! ```

use crate::options::PipelineOptions;
use crate::pipeline::{Pipeline, PipelineError};

/// Errors produced while selecting a runner or executing a pipeline on it.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RunnerError {
    /// `--runner` names a runner that is not linked into this binary.
    #[error("unknown runner '{name}'; {}", describe_available(available))]
    UnknownRunner {
        name: String,
        /// Runners that are linked, sorted.
        available: Vec<&'static str>,
    },

    /// The process was launched as an Fn API worker, but no worker harness is linked.
    #[error(
        "process started as an Fn API worker but no worker harness is linked; add \
         `apache-beam-harness` to the pipeline binary's dependencies (`use harness as _;`). \
         Linked runners: {}",
        linked.join(", ")
    )]
    WorkerHarnessMissing {
        /// Runners that are linked, sorted.
        linked: Vec<&'static str>,
    },

    /// The pipeline failed validation before it was handed to a runner.
    #[error("invalid pipeline: {0}")]
    InvalidPipeline(#[from] PipelineError),

    /// The runner accepted the pipeline but failed to execute it.
    ///
    /// The source is the runner's own error type, which callers can downcast to.
    #[error("pipeline execution failed: {0}")]
    Execution(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl RunnerError {
    /// Wraps a runner implementation's error as [`RunnerError::Execution`].
    pub fn execution(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Execution(error.into())
    }
}

fn describe_available(available: &[&str]) -> String {
    if available.is_empty() {
        "no runners are linked into this binary; enable a runner feature on the `beam` crate, \
         e.g. beam = { features = [\"prism\"] }"
            .to_string()
    } else {
        format!("available runners: {}", available.join(", "))
    }
}

/// Result metadata from a pipeline execution.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PipelineResult {
    pub job_id: String,
    pub state: String,
    pub metrics: Option<crate::metrics::MetricResults>,
}

impl PipelineResult {
    pub fn new(job_id: impl Into<String>, state: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
            state: state.into(),
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: crate::metrics::MetricResults) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn metrics(&self) -> Option<&crate::metrics::MetricResults> {
        self.metrics.as_ref()
    }
}

/// Executes a [`Pipeline`]. Implementations report their own failures through
/// [`RunnerError::execution`].
#[async_trait::async_trait]
pub trait PipelineRunner: Send + Sync {
    async fn run(&self, pipeline: &Pipeline) -> Result<PipelineResult, RunnerError>;
}

/// A runner selectable by name through [`PipelineOptions::runner`]. Runner crates submit one
/// with [`inventory::submit!`].
pub struct RunnerRegistration {
    /// Name matched against `--runner`, for example `direct`.
    pub name: &'static str,
    pub factory: fn(&PipelineOptions) -> Box<dyn PipelineRunner>,
}

inventory::collect!(RunnerRegistration);

/// Name of the Fn API worker harness registration that linking `apache-beam-harness` submits.
/// [`run`] selects it before `--runner` when the process starts with worker flags. In a worker
/// container, `--runner` still names the runner that launched the job.
pub const WORKER_RUNNER_NAME: &str = "worker";

/// Names of every runner linked into the current binary, sorted.
pub fn registered_runners() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = inventory::iter::<RunnerRegistration>
        .into_iter()
        .map(|r| r.name)
        .collect();
    names.sort_unstable();
    names
}

fn runner_named(
    name: &str,
    options: &PipelineOptions,
) -> Result<Box<dyn PipelineRunner>, RunnerError> {
    inventory::iter::<RunnerRegistration>
        .into_iter()
        .find(|registration| registration.name == name)
        .map(|registration| (registration.factory)(options))
        .ok_or_else(|| RunnerError::UnknownRunner {
            name: name.to_string(),
            available: registered_runners(),
        })
}

/// Builds the runner named by `options`, or returns an error listing what is available.
pub fn runner_for(options: &PipelineOptions) -> Result<Box<dyn PipelineRunner>, RunnerError> {
    runner_named(&options.runner.to_lowercase(), options)
}

/// Validates and runs `pipeline` on the runner named by `options`. Use this entry point so that
/// programs do not depend on a runner type.
///
/// When a runner's container boot program starts the process as a worker, this selects the
/// worker harness and ignores `--runner`. The harness serves the pipeline, rebuilt from the
/// forwarded options, so its transform handlers match the ones that the runner requests.
pub async fn run(
    pipeline: &Pipeline,
    options: &PipelineOptions,
) -> Result<PipelineResult, RunnerError> {
    let runner = if options.harness.is_worker() {
        runner_named(WORKER_RUNNER_NAME, options).map_err(|_| {
            RunnerError::WorkerHarnessMissing {
                linked: registered_runners(),
            }
        })?
    } else {
        runner_for(options)?
    };

    if !options.harness.is_worker() {
        pipeline.validate()?;
    }
    runner.run(pipeline).await
}
