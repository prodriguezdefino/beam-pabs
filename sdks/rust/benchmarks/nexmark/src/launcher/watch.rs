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

//! Async driver that polls a streaming job and applies the stop rules of [`decide`].

use std::time::Instant as WallInstant;

use beam::metrics::{MetricFilter, MetricResult, MetricResults};
use beam::runners::{JobHandle, JobState, PipelineResult, RunnerError};

use super::error::LauncherError;
use super::perf::{NexmarkPerf, ProgressSnapshot};
use super::stop::{Decision, MonitorState, MonitorTiming, Observation, StopRules, decide};

/// Why a run that did not fail stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// The job ended, or all expected events passed and the job went quiet.
    Finished,
    /// The run reached `stream_timeout`, so the launcher cancelled the job.
    TimedOut,
    /// The run reached `--running_time_minutes`, so the launcher cancelled the job.
    RunningTimeReached,
}

/// Final metrics and the poll history of a streaming run.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamingRun {
    pub metrics: MetricResults,
    pub stop: StopReason,
    pub snapshots: Vec<ProgressSnapshot>,
}

/// Runs `query` to its end, then cancels `publisher`, the publisher job of a `COMBINED` run.
///
/// The publisher writes until it stops, so it is cancelled whatever the outcome of `query`.
/// A failed cancel is logged and does not change that outcome.
pub async fn with_publisher<T>(publisher: &dyn JobHandle, query: impl Future<Output = T>) -> T {
    let outcome = query.await;
    if let Err(err) = publisher.cancel().await {
        tracing::warn!(
            job = publisher.job_id(),
            "cannot cancel the publisher job: {err}"
        );
    }
    outcome
}

/// Polls `job` every `timing.perf_delay` until it ends or a stop rule fires.
///
/// The wait between two polls stops as soon as the job ends.
///
/// When all expected events passed, the job is drained. It is cancelled when
/// `cancel_after_finish` is true or the runner cannot drain. `submit_ms` is the wall-clock
/// submission time in milliseconds since the Unix epoch.
///
/// # Errors
///
/// A failed or unexpectedly stopped job, fatal errors in the pipeline, and a job without
/// progress for `stuck_terminate` give an error. The launcher cancels the job in the last
/// two cases.
pub async fn monitor_streaming_job(
    job: &dyn JobHandle,
    timing: &MonitorTiming,
    rules: StopRules,
    cancel_after_finish: bool,
    submit_ms: i64,
) -> Result<StreamingRun, LauncherError> {
    let started = WallInstant::now();
    let mut state = MonitorState::new(rules);
    let mut snapshots = Vec::new();
    loop {
        let (job_state, metrics) = match (job.state().await, job.metrics().await) {
            (Ok(job_state), Ok(metrics)) => (job_state, metrics),
            (Err(err), _) | (_, Err(err)) => {
                tracing::warn!("cannot read the streaming job, polling again: {err}");
                if let Some(result) = wait_for_end(job, timing).await? {
                    return finished(result, MetricResults::default(), snapshots);
                }
                continue;
            }
        };
        let at = started.elapsed();
        let perf = NexmarkPerf::from_metrics(&metrics, submit_ms);
        snapshots.push(ProgressSnapshot {
            seconds_since_start: at.as_secs_f64(),
            runtime_sec: perf.runtime_sec,
            events: perf.num_events,
            results: perf.num_results,
        });

        if job_state.is_terminal() {
            return finished(job.wait_until_finish(None).await?, metrics, snapshots);
        }

        let observation = Observation {
            at,
            num_events: perf.num_events,
            num_results: perf.num_results,
            fatal: fatal_count(&metrics),
        };
        state = state.observe(&observation);
        let stop = match decide(&state, &observation) {
            Decision::Continue => None,
            Decision::RunningTimeReached => {
                tracing::info!("job reached --running_time_minutes; cancelling it");
                job.cancel().await?;
                Some(StopReason::RunningTimeReached)
            }
            Decision::Warn { quiet } => {
                tracing::warn!("streaming job made no progress for {} s", quiet.as_secs());
                None
            }
            Decision::Done => {
                tracing::info!("streaming job processed all events and went quiet; stopping it");
                stop_finished_job(job, cancel_after_finish).await?;
                Some(StopReason::Finished)
            }
            Decision::TimedOut => {
                tracing::info!("streaming job reached --stream_timeout; cancelling it");
                job.cancel().await?;
                Some(StopReason::TimedOut)
            }
            Decision::Fatal { count } => {
                job.cancel().await?;
                return Err(LauncherError::Stopped(format!(
                    "the pipeline reported {count} fatal errors"
                )));
            }
            Decision::Stuck { quiet } => {
                job.cancel().await?;
                return Err(LauncherError::Stopped(format!(
                    "no progress for {} s",
                    quiet.as_secs()
                )));
            }
        };
        if let Some(stop) = stop {
            return Ok(StreamingRun {
                metrics: final_metrics(job, timing, metrics).await,
                stop,
                snapshots,
            });
        }
        if let Some(result) = wait_for_end(job, timing).await? {
            return finished(result, metrics, snapshots);
        }
    }
}

/// Cancels a job that reached `--running_time_minutes` and reads its final metrics.
pub(super) async fn cancel_at_running_time(
    job: &dyn JobHandle,
    timing: &MonitorTiming,
) -> Result<StreamingRun, LauncherError> {
    tracing::info!("job reached --running_time_minutes; cancelling it");
    job.cancel().await?;
    let last = job.metrics().await.unwrap_or_default();
    Ok(StreamingRun {
        metrics: final_metrics(job, timing, last).await,
        stop: StopReason::RunningTimeReached,
        snapshots: Vec::new(),
    })
}

/// Waits up to `perf_delay` for the job to end. Gives its result when it ended in that time.
async fn wait_for_end(
    job: &dyn JobHandle,
    timing: &MonitorTiming,
) -> Result<Option<PipelineResult>, RunnerError> {
    let result = job.wait_until_finish(Some(timing.perf_delay)).await?;
    Ok(result.state.is_terminal().then_some(result))
}

/// The run of a job that ended by itself. `last` holds the metrics of the last poll.
fn finished(
    result: PipelineResult,
    last: MetricResults,
    snapshots: Vec<ProgressSnapshot>,
) -> Result<StreamingRun, LauncherError> {
    match result.state.is_successful() {
        true => Ok(StreamingRun {
            metrics: result.metrics.unwrap_or(last),
            stop: StopReason::Finished,
            snapshots,
        }),
        false => Err(LauncherError::Unsuccessful(result.state)),
    }
}

/// Drains the job, or cancels it when asked to or when the runner cannot drain.
async fn stop_finished_job(job: &dyn JobHandle, cancel: bool) -> Result<(), RunnerError> {
    match cancel {
        true => job.cancel().await,
        false => match job.drain().await {
            Err(RunnerError::Unsupported { runner, .. }) => {
                tracing::info!("the {runner} runner cannot drain; cancelling the job");
                job.cancel().await
            }
            other => other,
        },
    }
}

/// Waits up to `done_delay` for a stopped job to end and reads its final metrics.
///
/// The launcher stopped the job, so an error or a state that is not terminal only means that
/// the metrics of the last poll are the best figures.
async fn final_metrics(
    job: &dyn JobHandle,
    timing: &MonitorTiming,
    last: MetricResults,
) -> MetricResults {
    match job.wait_until_finish(Some(timing.done_delay)).await {
        Ok(result) => {
            if !matches!(
                result.state,
                JobState::Cancelled | JobState::Drained | JobState::Done
            ) {
                tracing::warn!("stopped job is still in state {}", result.state);
            }
            result.metrics.unwrap_or(last)
        }
        Err(err) => {
            tracing::warn!("cannot read the final state of the stopped job: {err}");
            last
        }
    }
}

/// Sum of the counters named `fatal` in all namespaces.
fn fatal_count(metrics: &MetricResults) -> i64 {
    metrics
        .query_counters(&MetricFilter::default().with_name("fatal"))
        .iter()
        .filter_map(MetricResult::result)
        .sum()
}
