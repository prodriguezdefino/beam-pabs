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

//! Suite execution driver and ASCII performance summary table.

use beam::options::PipelineOptions;
use beam::prelude::*;
use futures::StreamExt;

use super::NexmarkArgs;
use super::bigquery::export_summary;
use super::error::LauncherError;
use super::perf::{NexmarkPerf, ProgressSnapshot};
use super::run::{PendingRow, SuiteRun};
use super::side_input::write_side_input;
use super::stop::MonitorTiming;
use super::watch::{StopReason, StoppedJobs};
use crate::config::{NexmarkConfig, NexmarkQueryName};

/// Execution outcome recorded for one query in a suite run.
#[derive(Clone, Debug, PartialEq)]
pub enum NexmarkRunOutcome {
    /// The job ran and its checks passed.
    Executed {
        perf: NexmarkPerf,
        stop: StopReason,
        /// One entry per poll of a streaming job. Empty in batch mode.
        snapshots: Vec<ProgressSnapshot>,
    },
    /// The job failed, the monitor cancelled it, or a `--assert_correctness` check failed.
    Failed { reason: String },
    /// Pipeline was skipped before submission.
    Skipped { reason: &'static str },
}

impl NexmarkRunOutcome {
    /// Short status for the summary table and the BigQuery summary.
    pub fn status(&self) -> &'static str {
        match self {
            Self::Executed { stop, .. } => match stop {
                StopReason::Finished => "OK",
                StopReason::TimedOut => "TIMEOUT",
                StopReason::RunningTimeReached => "OK (running time)",
            },
            Self::Failed { .. } => "FAILED",
            Self::Skipped { .. } => "SKIPPED",
        }
    }

    /// Why the query failed or did not run.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Executed { .. } => None,
            Self::Failed { reason } => Some(reason),
            Self::Skipped { reason } => Some(reason),
        }
    }

    /// Figures of an executed run. Other outcomes give no figures.
    pub fn perf(&self) -> NexmarkPerf {
        match self {
            Self::Executed { perf, .. } => *perf,
            Self::Failed { .. } | Self::Skipped { .. } => NexmarkPerf::default(),
        }
    }
}

/// Outcome of a single query run.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRunSummary {
    pub query: NexmarkQueryName,
    /// The resolved configuration of the run.
    pub config: NexmarkConfig,
    pub outcome: NexmarkRunOutcome,
}

/// Executes every query resolved from `args` and prints a summary table on the driver.
///
/// A failed job gives a [`NexmarkRunOutcome::Failed`] row, and the suite continues. Up to
/// `--nexmark_parallel` queries run at the same time; the rows keep the suite order. `timing`
/// controls the polling and the stop rules of streaming jobs. A streaming job that the done
/// rule stops ends in the background, and its row takes the final metrics of the job. The
/// launcher waits for all of them before it prints the table.
/// On the driver, the launcher first writes the `CSV` side-input table and at the end exports
/// the summary to BigQuery, when the flags ask for it.
pub async fn run_nexmark(
    options: &PipelineOptions,
    args: &NexmarkArgs,
    timing: &MonitorTiming,
) -> Result<Vec<QueryRunSummary>, LauncherError> {
    let started_ms = Instant::now().as_millis();
    let configs = args.resolve_configs();
    let driver = !options.harness.is_worker();
    if driver {
        write_side_input(options, args, &configs).await?;
    }
    let stopped = StoppedJobs::new();
    let suite = SuiteRun {
        options,
        args,
        timing,
        multi_query: configs.len() > 1,
        stopped: &stopped,
    };
    // A slot frees when any query ends, so one slow query does not hold back the others.
    let suite = &suite;
    let mut runs: Vec<(usize, PendingRow)> = futures::stream::iter(configs.into_iter().enumerate())
        .map(|(position, config)| async move { (position, suite.run_query(config).await) })
        .buffer_unordered(args.nexmark_parallel.max(1))
        .collect()
        .await;
    runs.sort_unstable_by_key(|(position, _)| *position);
    let summaries: Vec<QueryRunSummary> =
        futures::future::join_all(runs.into_iter().map(|(_, row)| row.resolve())).await;

    if driver {
        println!("{}", format_summary_table(&summaries));
    }
    // The publishers end in the background. Wait for them before the export, which submits
    // one more job, and before the process can exit.
    stopped.wait_all().await;
    if driver && args.export_summary_to_bigquery {
        export_summary(options, args, &summaries, started_ms).await?;
    }
    Ok(summaries)
}

/// Formats query run summaries into a fixed-width ASCII table.
///
/// The reason of each failed row follows the table.
pub fn format_summary_table(summaries: &[QueryRunSummary]) -> String {
    let header = format!(
        "{:>5}  {:<31}  {:<27}  {:>12}  {:>12}  {:>17}  {:>9}  {:>12}  {:>12}  {:>21}",
        "Query",
        "Name",
        "Status",
        "Runtime(sec)",
        "Events(/sec)",
        "Events(bytes/sec)",
        "Results",
        "Results(/sec)",
        "Startup(sec)",
        "Processing delay(sec)"
    );
    let separator = "-".repeat(header.len());
    let rows = summaries.iter().map(|summary| {
        let outcome = &summary.outcome;
        let status = match outcome {
            NexmarkRunOutcome::Skipped { reason } => format!("{} ({reason})", outcome.status()),
            NexmarkRunOutcome::Executed { .. } | NexmarkRunOutcome::Failed { .. } => {
                outcome.status().to_owned()
            }
        };
        let perf = outcome.perf();
        format!(
            "{:>5}  {:<31}  {status:<27}  {:>12}  {:>12}  {:>17}  {:>9}  {:>12}  {:>12}  {:>21}",
            summary.query.number(),
            summary.query.as_str(),
            cell(perf.runtime_sec, 3),
            cell(perf.events_per_sec, 1),
            cell(perf.event_bytes_per_sec, 1),
            perf.num_results
                .map_or_else(|| "-".to_owned(), |n| n.to_string()),
            cell(perf.results_per_sec, 1),
            cell(perf.startup_delay_sec, 3),
            cell(perf.processing_delay_sec, 3),
        )
    });
    let failures = summaries
        .iter()
        .filter_map(|summary| match &summary.outcome {
            NexmarkRunOutcome::Failed { reason } => Some(format!(
                "\nQuery {} ({}) failed: {reason}",
                summary.query.number(),
                summary.query.as_str()
            )),
            NexmarkRunOutcome::Executed { .. } | NexmarkRunOutcome::Skipped { .. } => None,
        });
    let body: String = rows.map(|row| row + "\n").collect();
    let notes: String = failures.collect();
    format!("{separator}\n{header}\n{separator}\n{body}{separator}{notes}")
}

fn cell(value: Option<f64>, precision: usize) -> String {
    value.map_or_else(|| "-".to_owned(), |v| format!("{v:.precision$}"))
}
