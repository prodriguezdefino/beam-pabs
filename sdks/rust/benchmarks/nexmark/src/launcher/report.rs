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

//! Suite execution driver and ASCII throughput summary table.

use std::time::{Duration, Instant as WallInstant};

use beam::options::PipelineOptions;

use super::NexmarkArgs;
use super::pipeline::{
    COUNTER_SINK_BYTES, COUNTER_SINK_RECORDS, COUNTER_SOURCE_BYTES, COUNTER_SOURCE_EVENTS,
    LauncherError, METRIC_NAMESPACE, build_nexmark_pipeline, is_prism_runner,
};
use crate::config::NexmarkQueryName;

const BYTES_PER_MB: f64 = 1_000_000.0;
const PRISM_QUERY11_SKIP_REASON: &str = "apache/beam#31438";

/// Execution outcome recorded for one query in a suite run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NexmarkRunOutcome {
    /// Pipeline completed and reported metrics.
    Executed {
        elapsed: Duration,
        source_events: i64,
        source_bytes: i64,
        sink_records: i64,
        sink_bytes: i64,
    },
    /// Pipeline was skipped before submission.
    Skipped { reason: &'static str },
}

/// Metrics and timing summary for a single query run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryRunSummary {
    pub query: NexmarkQueryName,
    pub outcome: NexmarkRunOutcome,
}

impl QueryRunSummary {
    /// Throughput in generated input events per wallclock second.
    pub fn events_per_sec(&self) -> Option<f64> {
        match &self.outcome {
            NexmarkRunOutcome::Executed {
                elapsed,
                source_events,
                ..
            } => {
                let secs = elapsed.as_secs_f64();
                Some(if secs > 0.0 {
                    (*source_events as f64) / secs
                } else {
                    0.0
                })
            }
            NexmarkRunOutcome::Skipped { .. } => None,
        }
    }

    /// Throughput in decimal megabytes (`1_000_000` bytes) per wallclock second.
    pub fn mb_per_sec(&self) -> Option<f64> {
        match &self.outcome {
            NexmarkRunOutcome::Executed {
                elapsed,
                source_bytes,
                ..
            } => {
                let secs = elapsed.as_secs_f64();
                Some(if secs > 0.0 {
                    (*source_bytes as f64) / (BYTES_PER_MB * secs)
                } else {
                    0.0
                })
            }
            NexmarkRunOutcome::Skipped { .. } => None,
        }
    }
}

/// Executes every query resolved from `args` and prints a summary table on the driver.
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
        let started = WallInstant::now();
        let result = pipeline.run().await?;
        let elapsed = started.elapsed();

        let counter = |name: &str| {
            result
                .metrics()
                .and_then(|m| m.counter(METRIC_NAMESPACE, name))
                .unwrap_or(0)
        };
        summaries.push(QueryRunSummary {
            query,
            outcome: NexmarkRunOutcome::Executed {
                elapsed,
                source_events: counter(COUNTER_SOURCE_EVENTS),
                source_bytes: counter(COUNTER_SOURCE_BYTES),
                sink_records: counter(COUNTER_SINK_RECORDS),
                sink_bytes: counter(COUNTER_SINK_BYTES),
            },
        });
    }

    if !options.harness.is_worker() {
        println!("{}", format_summary_table(&summaries));
    }

    Ok(summaries)
}

/// Formats query run summaries into a fixed-width ASCII table.
pub fn format_summary_table(summaries: &[QueryRunSummary]) -> String {
    let header = format!(
        "{:>5}  {:<31}  {:<26}  {:>9}  {:>12}  {:>12}  {:>9}  {:>12}  {:>12}",
        "Query", "Name", "Status", "Sec", "Events", "Events/s", "MB/s", "Results", "Sink Bytes"
    );
    let separator = "-".repeat(header.len());
    let body = summaries
        .iter()
        .map(|summary| {
            let num = summary.query.number();
            let name = summary.query.as_str();
            match &summary.outcome {
                NexmarkRunOutcome::Executed {
                    elapsed,
                    source_events,
                    sink_records,
                    sink_bytes,
                    ..
                } => {
                    let secs = elapsed.as_secs_f64();
                    let eps = summary.events_per_sec().unwrap_or(0.0);
                    let mbps = summary.mb_per_sec().unwrap_or(0.0);
                    format!(
                        "{num:>5}  {name:<31}  {:<26}  {secs:>9.3}  {source_events:>12}  {eps:>12.1}  {mbps:>9.2}  {sink_records:>12}  {sink_bytes:>12}",
                        "OK"
                    )
                }
                NexmarkRunOutcome::Skipped { reason } => {
                    let status = format!("SKIPPED ({reason})");
                    format!(
                        "{num:>5}  {name:<31}  {status:<26}  {:>9}  {:>12}  {:>12}  {:>9}  {:>12}  {:>12}",
                        "-", "-", "-", "-", "-", "-"
                    )
                }
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    if body.is_empty() {
        format!("{separator}\n{header}\n{separator}\n{separator}")
    } else {
        format!("{separator}\n{header}\n{separator}\n{body}\n{separator}")
    }
}
