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
use beam::testing::passert;

use super::NexmarkArgs;
use super::perf::NexmarkPerf;
use super::pipeline::{LauncherError, build_nexmark_pipeline, is_prism_runner};
use crate::config::NexmarkQueryName;

const PRISM_QUERY11_SKIP_REASON: &str = "apache/beam#31438";

/// Execution outcome recorded for one query in a suite run.
#[derive(Clone, Debug, PartialEq)]
pub enum NexmarkRunOutcome {
    /// The job finished and its checks passed.
    Executed(NexmarkPerf),
    /// The job failed, or a `--assert_correctness` check did not pass.
    Failed { reason: String },
    /// Pipeline was skipped before submission.
    Skipped { reason: &'static str },
}

/// Outcome of a single query run.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRunSummary {
    pub query: NexmarkQueryName,
    pub outcome: NexmarkRunOutcome,
}

/// Executes every query resolved from `args` and prints a summary table on the driver.
///
/// A failed job gives a [`NexmarkRunOutcome::Failed`] row, and the suite continues.
pub async fn run_nexmark(
    options: &PipelineOptions,
    args: &NexmarkArgs,
) -> Result<Vec<QueryRunSummary>, LauncherError> {
    let configs = args.resolve_configs();
    let multi_query = configs.len() > 1;
    let skip_prism_q11 = args.query.is_none() && is_prism_runner(&options.runner);
    let mut summaries = Vec::with_capacity(configs.len());

    for config in configs {
        let query = config.query.unwrap_or(NexmarkQueryName::Passthrough);
        if skip_prism_q11 && query == NexmarkQueryName::UserSessions {
            summaries.push(QueryRunSummary {
                query,
                outcome: NexmarkRunOutcome::Skipped {
                    reason: PRISM_QUERY11_SKIP_REASON,
                },
            });
            continue;
        }

        // Pin the active query in `PipelineOptions` before creating the pipeline so
        // remote workers booting from `OptionsSnapshot` build only this query's graph.
        let mut run_options = options.clone();
        if multi_query
            && !run_options.harness.is_worker()
            && let Some(base_name) = &options.job_name
        {
            run_options.job_name = Some(format!("{base_name}-{}", query.slug().replace('_', "-")));
        }
        run_options.set(args.pin_query(query))?;

        let pipeline = build_nexmark_pipeline(&run_options, args, &config)?;
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
        let outcome = match execute(&pipeline).await {
            Ok(perf) => NexmarkRunOutcome::Executed(perf),
            Err(err) => NexmarkRunOutcome::Failed {
                reason: err.to_string(),
            },
        };
        summaries.push(QueryRunSummary { query, outcome });
    }

    if !options.harness.is_worker() {
        println!("{}", format_summary_table(&summaries));
    }

    Ok(summaries)
}

/// Submits `pipeline`, waits for the job, checks its assertions and computes its performance.
async fn execute(pipeline: &Pipeline) -> Result<NexmarkPerf, LauncherError> {
    let submit_ms = Instant::now().as_millis();
    let job = pipeline.submit().await?;
    let result = job.wait_until_finish(None).await?;
    if !result.state.is_successful() {
        return Err(LauncherError::Unsuccessful(result.state));
    }
    // A check whose input never closes does not fail the job, so require each check to pass.
    let assertions = passert::assertion_names(pipeline);
    if !assertions.is_empty() {
        passert::verify_assertions(&result, &assertions).map_err(LauncherError::Correctness)?;
    }
    let metrics = result.metrics.unwrap_or_default();
    Ok(NexmarkPerf::from_metrics(&metrics, submit_ms))
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
        let (status, perf) = match &summary.outcome {
            NexmarkRunOutcome::Executed(perf) => ("OK".to_owned(), *perf),
            NexmarkRunOutcome::Failed { .. } => ("FAILED".to_owned(), NexmarkPerf::default()),
            NexmarkRunOutcome::Skipped { reason } => {
                (format!("SKIPPED ({reason})"), NexmarkPerf::default())
            }
        };
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
            NexmarkRunOutcome::Executed(_) | NexmarkRunOutcome::Skipped { .. } => None,
        });
    let body: String = rows.map(|row| row + "\n").collect();
    let notes: String = failures.collect();
    format!("{separator}\n{header}\n{separator}\n{body}{separator}{notes}")
}

fn cell(value: Option<f64>, precision: usize) -> String {
    value.map_or_else(|| "-".to_owned(), |v| format!("{v:.precision$}"))
}
