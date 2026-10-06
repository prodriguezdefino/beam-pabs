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

//! Test runner for Google Cloud Dataflow (`TestDataflowRunner`).
//!
//! A batch pipeline runs on [`DataflowRunner`]. A streaming pipeline does not stop on its
//! own, so the runner polls job metrics for `passert` assertions. It cancels the job when all
//! assertions pass (success, without waiting for VM deprovisioning), when one fails, or at
//! the test timeout ([`DEFAULT_TEST_TIMEOUT`] by default; failure with the pending
//! assertions). [`testing::passert::assertion_status`] decides the assertions, as in
//! [`TestPipeline`](testing::TestPipeline).

use std::time::{Duration, Instant};

use beam::metrics::MetricResults;
use beam::options::PipelineOptions;
use beam::pipeline::Pipeline;
use beam::runners::{PipelineResult, PipelineRunner, RunnerError, RunnerRegistration};
use testing::passert::{AssertionStatus, assertion_names, assertion_status};
use tracing::{info, warn};

use crate::client::{DataflowApiClient, JobState};
use crate::runner::{DataflowRunner, DataflowRunnerError, JobPoller, SubmittedJob, fetch_metrics};

/// How long a streaming test job may run before its assertions must be decided. Covers
/// worker start-up, which on Dataflow alone takes several minutes.
pub const DEFAULT_TEST_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// Test runner wrapping [`DataflowRunner`] with in-pipeline assertion monitoring.
#[derive(Clone, Debug)]
pub struct TestDataflowRunner {
    runner: DataflowRunner,
    test_timeout: Duration,
}

impl TestDataflowRunner {
    pub fn with_options(options: PipelineOptions) -> Self {
        Self::from_runner(DataflowRunner::with_options(options))
    }

    /// Wraps an already configured [`DataflowRunner`], for instance one with custom clients.
    pub fn from_runner(runner: DataflowRunner) -> Self {
        Self {
            runner,
            test_timeout: DEFAULT_TEST_TIMEOUT,
        }
    }

    /// Sets how long a streaming test job may run before its assertions must be decided.
    pub fn with_test_timeout(mut self, timeout: Duration) -> Self {
        self.test_timeout = timeout;
        self
    }

    pub fn test_timeout(&self) -> Duration {
        self.test_timeout
    }

    pub fn runner(&self) -> &DataflowRunner {
        &self.runner
    }
}

// Selectable as `--runner=testdataflowrunner` or `--runner=TestDataflowRunner`
inventory::submit! {
    RunnerRegistration {
        name: "testdataflowrunner",
        factory: |options: &PipelineOptions| Box::new(TestDataflowRunner::with_options(options.clone())),
    }
}

inventory::submit! {
    RunnerRegistration {
        name: "testdataflow",
        factory: |options: &PipelineOptions| Box::new(TestDataflowRunner::with_options(options.clone())),
    }
}

#[async_trait::async_trait]
impl PipelineRunner for TestDataflowRunner {
    async fn run(&self, pipeline: &Pipeline) -> Result<PipelineResult, RunnerError> {
        if !self.runner.options().streaming {
            return self.runner.run(pipeline).await;
        }

        let (job, client) = self
            .runner
            .prepare_and_submit(pipeline)
            .await
            .map_err(RunnerError::execution)?;

        let expected_assertions = assertion_names(pipeline);
        self.wait_for_streaming_test(&*client, &job, &expected_assertions)
            .await
            .map_err(RunnerError::execution)
    }
}

impl TestDataflowRunner {
    /// Polls the streaming test `job` until [`next_step`] decides it, then carries it out.
    async fn wait_for_streaming_test(
        &self,
        client: &dyn DataflowApiClient,
        job: &SubmittedJob,
        expected_assertions: &[String],
    ) -> Result<PipelineResult, DataflowRunnerError> {
        let started = Instant::now();
        let mut poller = JobPoller::new(client, job, self.runner.poll_interval());
        loop {
            let state = JobState::parse(&poller.next_status().await?.current_state);
            // Assertions report through metrics once workers run, and the final counts
            // decide a job that stopped.
            let metrics = if state == JobState::Running || state.is_terminal() {
                fetch_metrics(client, job).await
            } else {
                None
            };
            let assertions = observed_assertions(metrics.as_ref(), expected_assertions);

            match next_step(
                state,
                assertions.as_ref(),
                started.elapsed(),
                self.test_timeout,
            ) {
                Step::Continue => {}
                Step::Cancel(verdict) => {
                    info!(
                        "Streaming test job '{}' decided ({verdict:?}); cancelling it",
                        job.id()
                    );
                    cancel(client, job).await;
                    return self.conclude(job, JobState::Cancelled, verdict, metrics);
                }
                Step::Finish(verdict) => return self.conclude(job, state, verdict, metrics),
            }
        }
    }

    /// Turns the final `verdict` on `job`, last seen in `state`, into the run's outcome.
    fn conclude(
        &self,
        job: &SubmittedJob,
        state: JobState,
        verdict: Verdict,
        metrics: Option<MetricResults>,
    ) -> Result<PipelineResult, DataflowRunnerError> {
        let job_id = job.id().to_string();
        match verdict {
            Verdict::Passed => Ok(metrics.into_iter().fold(
                PipelineResult::new(job_id, state.as_str()),
                PipelineResult::with_metrics,
            )),
            Verdict::AssertionsFailed { failed, total } => {
                Err(DataflowRunnerError::AssertionsFailed {
                    job_id,
                    failed,
                    total,
                })
            }
            Verdict::TimedOut { missing } => Err(DataflowRunnerError::TestTimedOut {
                job_id,
                timeout: self.test_timeout,
                missing,
            }),
            Verdict::JobFailed => Err(DataflowRunnerError::JobFailed {
                job_id,
                state: state.as_str().to_string(),
            }),
        }
    }
}

/// Where the `expected` assertions stand, or `None` when the pipeline has none. Without
/// metrics every assertion is pending.
fn observed_assertions(
    metrics: Option<&MetricResults>,
    expected: &[String],
) -> Option<AssertionStatus> {
    if expected.is_empty() {
        return None;
    }
    Some(metrics.map_or_else(
        || AssertionStatus::Pending {
            passed: 0,
            missing: expected.to_vec(),
        },
        |m| assertion_status(m, expected),
    ))
}

/// What the streaming test runner does after one poll.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Step {
    /// Poll again.
    Continue,
    /// Cancel the still running job, then report the verdict.
    Cancel(Verdict),
    /// The job stopped on its own; report the verdict.
    Finish(Verdict),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Verdict {
    Passed,
    AssertionsFailed { failed: Vec<String>, total: i64 },
    TimedOut { missing: Vec<String> },
    JobFailed,
}

/// Decides what to do with a streaming test job in `state`, whose assertions stand at
/// `assertions` (`None` when the pipeline has none), `elapsed` into a `timeout`.
///
/// - A failed assertion fails the test, cancelling the job if it still runs.
/// - A running job whose assertions all passed is cancelled, and the test passes.
/// - A job that stopped passes if it succeeded, or if it was cancelled after all its
///   assertions passed; otherwise it fails.
/// - A job still undecided at the timeout is cancelled, and the test fails.
fn next_step(
    state: JobState,
    assertions: Option<&AssertionStatus>,
    elapsed: Duration,
    timeout: Duration,
) -> Step {
    let failure = match assertions {
        Some(AssertionStatus::Failed { failed, total }) => Some(Verdict::AssertionsFailed {
            failed: failed.clone(),
            total: *total,
        }),
        _ => None,
    };
    let all_passed = matches!(assertions, Some(AssertionStatus::AllPassed(_)));

    if state.is_terminal() {
        return Step::Finish(match failure {
            Some(failure) => failure,
            None if state.is_successful() || (state == JobState::Cancelled && all_passed) => {
                Verdict::Passed
            }
            None => Verdict::JobFailed,
        });
    }
    if let Some(failure) = failure {
        return Step::Cancel(failure);
    }
    if all_passed {
        return Step::Cancel(Verdict::Passed);
    }
    if elapsed >= timeout {
        let missing = match assertions {
            Some(AssertionStatus::Pending { missing, .. }) => missing.clone(),
            _ => Vec::new(),
        };
        return Step::Cancel(Verdict::TimedOut { missing });
    }
    Step::Continue
}

/// Requests cancellation of `job` and logs a failure: a job that is not cancelled keeps
/// running and billing.
async fn cancel(client: &dyn DataflowApiClient, job: &SubmittedJob) {
    if let Err(e) = client
        .update_job(job.project(), job.region(), job.id(), JobState::Cancelled)
        .await
    {
        warn!(
            "Failed to cancel Dataflow test job '{}'; it may still be running: {e}",
            job.id()
        );
    }
}
