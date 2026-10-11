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

//! A [`Pipeline`] for tests: configured from the environment, verified after it runs,
//! and checked for having been run at all.

use std::fmt;
use std::ops::Deref;
use std::sync::Mutex;

use beam::options::PipelineOptions;
use beam::pipeline::Pipeline;
use beam::runners::{PipelineResult, PipelineRunner, RunnerError};

use crate::passert;

/// Environment variable holding the options a [`TestPipeline`] runs with.
///
/// The value is a whitespace-separated list of flags, for example
/// `--runner=prism --job_name=it`. With it, one test suite can target a different
/// runner without code changes. Quoting is not supported.
pub const TEST_PIPELINE_OPTIONS_ENV: &str = "BEAM_TEST_PIPELINE_OPTIONS";

/// Why a [`TestPipeline`] run did not succeed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TestPipelineError {
    /// The runner could not be selected, rejected the pipeline, or the job failed.
    ///
    /// A failed assertion lands here: it fails the job it runs in.
    #[error(transparent)]
    Runner(#[from] RunnerError),

    /// The job succeeded, but not every assertion in it ran and passed.
    #[error("pipeline succeeded but its assertions could not be verified: {0}")]
    Verification(String),
}

/// Parses `flags`, a whitespace-separated list such as `--runner=prism`, into options.
pub fn parse_test_pipeline_options(flags: &str) -> PipelineOptions {
    PipelineOptions::parse_from(std::iter::once("test").chain(flags.split_whitespace()))
}

/// Options from [`TEST_PIPELINE_OPTIONS_ENV`], or the defaults when it is unset.
pub fn test_pipeline_options() -> PipelineOptions {
    std::env::var(TEST_PIPELINE_OPTIONS_ENV)
        .map(|flags| parse_test_pipeline_options(&flags))
        .unwrap_or_default()
}

/// A [`Pipeline`] for tests.
///
/// It dereferences to [`Pipeline`], so you build it the same way. It adds three
/// features:
///
/// - **Configuration from the environment.** [`TestPipeline::new`] reads
///   [`TEST_PIPELINE_OPTIONS_ENV`], so the person who runs the tests selects the runner.
/// - **Assertion verification.** [`run`](Self::run) counts the [`passert`] assertions
///   in the pipeline and fails unless all of them ran and passed. An assertion whose
///   window never closes does not fail by itself. It never runs.
/// - **Run enforcement.** A drop panics if the `TestPipeline` holds transforms but was
///   never run, or if it got transforms after it ran. Without this, a test that does
///   not call `run().await` passes without testing anything.
///
/// ```no_run
/// use beam::prelude::*;
/// use testing::{passert, TestPipeline};
///
/// # async fn example() -> Result<(), testing::TestPipelineError> {
/// let p = TestPipeline::new();
/// let doubled = p
///     .apply(Create::new("Create", vec![1i64, 2, 3]))
///     .apply(Map::new("Double", |x: i64| x * 2));
/// passert::that("AssertDoubled", &doubled).contains_in_any_order([2, 4, 6]);
///
/// p.run().await?;
/// # Ok(())
/// # }
/// ```
///
/// The runner is found through the link-time registry, so the test binary must link it,
/// for instance with the `prism` feature of the `apache-beam` crate.
pub struct TestPipeline {
    pipeline: Pipeline,
    options: PipelineOptions,
    enforce_run: bool,
    /// Number of transforms when `run` was first called, or `None` before then.
    transforms_at_run: Mutex<Option<usize>>,
}

impl Default for TestPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl TestPipeline {
    /// Creates an empty pipeline configured from [`TEST_PIPELINE_OPTIONS_ENV`].
    pub fn new() -> Self {
        Self::with_options(test_pipeline_options())
    }

    /// Creates an empty pipeline running with `options`, ignoring the environment. Transforms
    /// see the same options when they expand.
    pub fn with_options(options: PipelineOptions) -> Self {
        Self {
            pipeline: Pipeline::create(&options),
            options,
            enforce_run: true,
            transforms_at_run: Mutex::new(None),
        }
    }

    /// Disables the checks made when the pipeline is dropped.
    ///
    /// For tests that build a pipeline only to inspect its graph.
    #[must_use]
    pub fn without_run_enforcement(mut self) -> Self {
        self.enforce_run = false;
        self
    }

    /// The options [`run`](Self::run) executes with.
    pub fn options(&self) -> &PipelineOptions {
        &self.options
    }

    /// The underlying pipeline.
    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// How many [`passert`] assertions the pipeline holds.
    pub fn assertion_count(&self) -> usize {
        passert::count_assertions(&self.pipeline)
    }

    /// Runs the pipeline on the runner named by [`options`](Self::options), then checks
    /// that every assertion ran and passed.
    pub async fn run(&self) -> Result<PipelineResult, TestPipelineError> {
        self.mark_run();
        let result = beam::runners::run(&self.pipeline, &self.options).await?;
        self.verify(result)
    }

    /// Runs the pipeline on `runner`, ignoring the runner named by the options, then
    /// checks that every assertion ran and passed.
    pub async fn run_with<R: PipelineRunner + ?Sized>(
        &self,
        runner: &R,
    ) -> Result<PipelineResult, TestPipelineError> {
        self.mark_run();
        let result = self.pipeline.run_with_runner(runner).await?;
        self.verify(result)
    }

    fn transform_count(&self) -> usize {
        self.pipeline.lock().transform_order.len()
    }

    /// Records that a run was attempted. A failed run also counts: the test saw the
    /// failure, so the pipeline was not abandoned.
    fn mark_run(&self) {
        let count = self.transform_count();
        let mut at_run = self
            .transforms_at_run
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        at_run.get_or_insert(count);
    }

    /// Checks each assertion separately: it must pass at least once, and no assertion
    /// may fail. See [`passert::verify_assertions`].
    fn verify(&self, result: PipelineResult) -> Result<PipelineResult, TestPipelineError> {
        let assertions = passert::assertion_names(&self.pipeline);
        if !assertions.is_empty() {
            passert::verify_assertions(&result, &assertions)
                .map_err(TestPipelineError::Verification)?;
        }
        Ok(result)
    }
}

impl Deref for TestPipeline {
    type Target = Pipeline;

    fn deref(&self) -> &Pipeline {
        &self.pipeline
    }
}

impl fmt::Debug for TestPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestPipeline")
            .field("runner", &self.options.runner)
            .field("enforce_run", &self.enforce_run)
            .field("transforms", &self.transform_count())
            .field("assertions", &self.assertion_count())
            .finish_non_exhaustive()
    }
}

impl Drop for TestPipeline {
    fn drop(&mut self) {
        // Do not panic while unwinding. That aborts the test binary and hides the
        // original failure.
        if !self.enforce_run || std::thread::panicking() {
            return;
        }
        let transforms = self.transform_count();
        let at_run = *self
            .transforms_at_run
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match at_run {
            None if transforms > 0 => panic!(
                "TestPipeline was dropped without being run: call `run().await`, or \
                 `without_run_enforcement()` for a pipeline that is only inspected"
            ),
            Some(count) if transforms > count => panic!(
                "{} transform(s) were added to the TestPipeline after it was run, and \
                 were never executed",
                transforms - count
            ),
            _ => {}
        }
    }
}
