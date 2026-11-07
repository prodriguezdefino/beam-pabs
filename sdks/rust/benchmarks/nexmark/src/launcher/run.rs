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

//! Run of one query: option pinning, submission, monitoring and performance figures.

use beam::options::PipelineOptions;
use beam::prelude::*;
use beam::runners::PipelineResult;
use beam::testing::passert;

use super::NexmarkArgs;
use super::error::LauncherError;
use super::perf::{NexmarkPerf, steady_state_events_per_sec};
use super::pipeline::{build_nexmark_pipeline, is_prism_runner};
use super::report::{NexmarkRunOutcome, QueryRunSummary};
use super::stop::{MonitorTiming, StopRules, running_time_limit};
use super::watch::{
    StopReason, StoppedJobs, StreamingRun, cancel_at_running_time, monitor_streaming_job,
    with_publisher,
};
use crate::config::{NexmarkConfig, NexmarkQueryName, PubsubMode, SourceType};

const PRISM_QUERY11_SKIP_REASON: &str = "apache/beam#31438";
const SKIP_QUERIES_REASON: &str = "--skip_queries";

/// Settings that each query run of one suite run shares.
#[derive(Clone, Copy)]
pub(super) struct SuiteRun<'a> {
    pub options: &'a PipelineOptions,
    pub args: &'a NexmarkArgs,
    pub timing: &'a MonitorTiming,
    /// More than one query runs, so each job name gets the query as a suffix.
    pub multi_query: bool,
    /// Stopped jobs that end in the background while the next queries run.
    pub stopped: &'a StoppedJobs,
}

impl SuiteRun<'_> {
    /// Runs the query of `config` and gives its row of the summary. A failure is a row too.
    pub async fn run_query(self, config: NexmarkConfig) -> QueryRunSummary {
        let query = config.query.unwrap_or(NexmarkQueryName::Passthrough);
        let skip_reason = match (
            self.args.skip_queries.contains(&query),
            query == NexmarkQueryName::UserSessions
                && self.args.query.is_none()
                && is_prism_runner(&self.options.runner),
        ) {
            (true, _) => Some(SKIP_QUERIES_REASON),
            (false, true) => Some(PRISM_QUERY11_SKIP_REASON),
            (false, false) => None,
        };
        let outcome = match skip_reason {
            Some(reason) => NexmarkRunOutcome::Skipped { reason },
            None => self
                .execute_query(query, &config)
                .await
                .unwrap_or_else(|err| NexmarkRunOutcome::Failed {
                    reason: err.to_string(),
                }),
        };
        QueryRunSummary {
            query,
            config,
            outcome,
        }
    }

    async fn execute_query(
        self,
        query: NexmarkQueryName,
        config: &NexmarkConfig,
    ) -> Result<NexmarkRunOutcome, LauncherError> {
        let options = self.options;
        let args = self.args;
        // Pin the active query in `PipelineOptions` before creating the pipeline so
        // remote workers booting from `OptionsSnapshot` build only this query's graph.
        let mut run_options = options.clone();
        if self.multi_query
            && !run_options.harness.is_worker()
            && let Some(base_name) = &options.job_name
        {
            run_options.job_name = Some(format!("{base_name}-{}", query.slug().replace('_', "-")));
        }
        // A COMBINED run is two jobs. Pin each to its side, so its workers rebuild its graph.
        let combined = matches!(
            (config.source_type, config.pubsub_mode),
            (SourceType::Pubsub | SourceType::Kafka, PubsubMode::Combined)
        );
        let query_args = match combined {
            true => args
                .pin_query(query)
                .pin_pubsub_mode(PubsubMode::SubscribeOnly),
            false => args.pin_query(query),
        };
        run_options.set(query_args)?;
        let publisher = match combined {
            true => Some(build_publisher(&run_options, args, query, config)?),
            false => None,
        };

        let pipeline = build_nexmark_pipeline(&run_options, args, config)?;
        // The graph holds a check only for a batch query with a model. Workers rebuild the
        // graph, so log here, on the driver, and not during the build.
        if args.assert_correctness
            && !options.harness.is_worker()
            && passert::count_assertions(&pipeline) == 0
        {
            let reason = match options.streaming {
                true => "streaming mode",
                false => "no reference model",
            };
            tracing::info!(%query, "skipping --assert_correctness: {reason}");
        }
        let run = RunSettings {
            streaming: options.streaming,
            config,
            args,
            timing: self.timing,
            stopped: self.stopped,
        };
        match &publisher {
            Some(publisher) => execute_with_publisher(publisher, &pipeline, &run).await,
            None => execute(&pipeline, &run).await,
        }
    }
}

/// Builds the publisher job of a `COMBINED` run: the query options pinned to `PUBLISH_ONLY`,
/// with `-publisher` after the job name.
fn build_publisher(
    query_options: &PipelineOptions,
    args: &NexmarkArgs,
    query: NexmarkQueryName,
    config: &NexmarkConfig,
) -> Result<Pipeline, LauncherError> {
    let mut options = query_options.clone();
    options.job_name = query_options
        .job_name
        .as_ref()
        .map(|name| format!("{name}-publisher"));
    let publisher_args = args
        .pin_query(query)
        .pin_pubsub_mode(PubsubMode::PublishOnly);
    options.set(publisher_args)?;
    let publisher_config = NexmarkConfig {
        pubsub_mode: PubsubMode::PublishOnly,
        ..*config
    };
    build_nexmark_pipeline(&options, args, &publisher_config)
}

/// Submits the `publisher` job, then runs `query` and cancels the publisher at its end. The
/// publisher then ends in the background.
async fn execute_with_publisher(
    publisher: &Pipeline,
    query: &Pipeline,
    run: &RunSettings<'_>,
) -> Result<NexmarkRunOutcome, LauncherError> {
    let publisher_job = publisher.submit().await?;
    let outcome = with_publisher(publisher_job.as_ref(), execute(query, run)).await;
    run.stopped.watch(publisher_job);
    outcome
}

/// The settings of one query run that [`execute`] needs.
struct RunSettings<'a> {
    streaming: bool,
    config: &'a NexmarkConfig,
    args: &'a NexmarkArgs,
    timing: &'a MonitorTiming,
    stopped: &'a StoppedJobs,
}

/// Submits `pipeline`, waits for or monitors the job, and computes its performance.
async fn execute(
    pipeline: &Pipeline,
    run: &RunSettings<'_>,
) -> Result<NexmarkRunOutcome, LauncherError> {
    let submit_ms = Instant::now().as_millis();
    let job = pipeline.submit().await?;
    let running_time =
        running_time_limit(run.args.running_time_minutes, run.config.preload_seconds);
    let StreamingRun {
        metrics,
        stop,
        snapshots,
        ends_in_background,
    } = match (run.streaming, running_time) {
        (true, _) => {
            let rules = StopRules::new(run.timing, run.config).with_running_time(running_time);
            let cancel = run.args.cancel_streaming_job_after_finish;
            monitor_streaming_job(job.as_ref(), run.timing, rules, cancel, submit_ms).await?
        }
        (false, Some(limit)) => {
            let result = job.wait_until_finish(Some(limit)).await?;
            match result.state.is_terminal() {
                true => batch_run(pipeline, result)?,
                false => cancel_at_running_time(job.as_ref(), run.timing).await?,
            }
        }
        (false, None) => batch_run(pipeline, job.wait_until_finish(None).await?)?,
    };
    if ends_in_background {
        run.stopped.watch(job);
    }
    let perf = NexmarkPerf::from_metrics(&metrics, submit_ms);
    let perf = match steady_state_events_per_sec(&snapshots, run.timing.min_steady_window) {
        Some(rate) => {
            tracing::info!(
                "revising events/sec from {:?} to the steady-state {rate:.1}",
                perf.events_per_sec
            );
            NexmarkPerf {
                events_per_sec: Some(rate),
                ..perf
            }
        }
        None => perf,
    };
    Ok(NexmarkRunOutcome::Executed {
        perf,
        stop,
        snapshots,
    })
}

/// The run of a batch job that ended by itself: its state and each correctness check pass.
fn batch_run(pipeline: &Pipeline, result: PipelineResult) -> Result<StreamingRun, LauncherError> {
    if !result.state.is_successful() {
        return Err(LauncherError::Unsuccessful(result.state));
    }
    // A check whose input never closes does not fail the job, so require each check to pass.
    let assertions = passert::assertion_names(pipeline);
    if !assertions.is_empty() {
        passert::verify_assertions(&result, &assertions).map_err(LauncherError::Correctness)?;
    }
    Ok(StreamingRun {
        metrics: result.metrics.unwrap_or_default(),
        stop: StopReason::Finished,
        snapshots: Vec::new(),
        ends_in_background: false,
    })
}
