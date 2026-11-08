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

//! Suite summary export to BigQuery: one row for each query, written by a small batch job.

use beam::io::gcp::bigquery::{BigQueryWrite, CreateDisposition, WriteDisposition};
use beam::options::PipelineOptions;
use beam::prelude::*;
use chrono::{DateTime, Utc};

use super::NexmarkArgs;
use super::error::LauncherError;
use super::report::QueryRunSummary;

/// One row of the BigQuery summary table.
///
/// The column names are camelCase. `timestamp`, `runtimeSec`, `eventsPerSec` and `numResults`
/// match the columns of Java's removed BigQuery export. The other figures use the JSON names
/// of Java's `NexmarkPerf`. A query that did not run has no figures.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct SummaryRow {
    /// Start of the suite run.
    pub timestamp: DateTime<Utc>,
    #[beam(rename = "runId")]
    pub run_id: String,
    #[beam(rename = "queryNumber")]
    pub query_number: i64,
    #[beam(rename = "queryName")]
    pub query_name: String,
    /// `batch` or `streaming`.
    pub mode: String,
    pub runner: String,
    /// The status of the summary table: `OK`, `TIMEOUT`, `FAILED`, `SKIPPED` and so on.
    pub status: String,
    /// Why the query failed or did not run.
    pub reason: Option<String>,
    /// The resolved configuration of the query as JSON.
    pub config: String,
    #[beam(rename = "runtimeSec")]
    pub runtime_sec: Option<f64>,
    #[beam(rename = "numEvents")]
    pub num_events: Option<i64>,
    #[beam(rename = "eventsPerSec")]
    pub events_per_sec: Option<f64>,
    #[beam(rename = "eventBytesPerSec")]
    pub event_bytes_per_sec: Option<f64>,
    #[beam(rename = "numResults")]
    pub num_results: Option<i64>,
    #[beam(rename = "resultsPerSec")]
    pub results_per_sec: Option<f64>,
    #[beam(rename = "resultBytesPerSec")]
    pub result_bytes_per_sec: Option<f64>,
    #[beam(rename = "startupDelaySec")]
    pub startup_delay_sec: Option<f64>,
    #[beam(rename = "processingDelaySec")]
    pub processing_delay_sec: Option<f64>,
    #[beam(rename = "timeDilation")]
    pub time_dilation: Option<f64>,
}

/// Values that each row of one suite run shares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunInfo {
    pub run_id: String,
    /// Start of the suite run, in milliseconds since the Unix epoch.
    pub started_ms: i64,
    pub streaming: bool,
    pub runner: String,
}

impl RunInfo {
    /// The run of `options` that started at `started_ms`. The run ID is the job name, or
    /// `nexmark`, then the start time.
    pub fn new(options: &PipelineOptions, started_ms: i64) -> Self {
        let base = options.job_name.as_deref().unwrap_or("nexmark");
        let runner = match options.runner.trim() {
            "" => "prism",
            runner => runner,
        };
        Self {
            run_id: format!("{base}-{started_ms}"),
            started_ms,
            streaming: options.streaming,
            runner: runner.to_owned(),
        }
    }
}

/// The summary rows of `summaries`, one for each query, in suite order.
pub fn summary_rows(
    summaries: &[QueryRunSummary],
    run: &RunInfo,
) -> Result<Vec<SummaryRow>, LauncherError> {
    let timestamp = DateTime::from_timestamp_millis(run.started_ms).ok_or_else(|| {
        LauncherError::Summary(format!("start time {} is out of range", run.started_ms))
    })?;
    let mode = if run.streaming { "streaming" } else { "batch" };
    summaries
        .iter()
        .map(|summary| {
            let config = serde_json::to_string(&summary.config)
                .map_err(|err| LauncherError::Summary(format!("config to JSON: {err}")))?;
            let perf = summary.outcome.perf();
            Ok(SummaryRow {
                timestamp,
                run_id: run.run_id.clone(),
                query_number: i64::from(summary.query.number()),
                query_name: summary.query.as_str().to_owned(),
                mode: mode.to_owned(),
                runner: run.runner.clone(),
                status: summary.outcome.status().to_owned(),
                reason: summary.outcome.reason().map(str::to_owned),
                config,
                runtime_sec: perf.runtime_sec,
                num_events: perf.num_events,
                events_per_sec: perf.events_per_sec,
                event_bytes_per_sec: perf.event_bytes_per_sec,
                num_results: perf.num_results,
                results_per_sec: perf.results_per_sec,
                result_bytes_per_sec: perf.result_bytes_per_sec,
                startup_delay_sec: perf.startup_delay_sec,
                processing_delay_sec: perf.processing_delay_sec,
                time_dilation: perf.time_dilation,
            })
        })
        .collect()
}

/// Builds the batch job that appends `rows` to `--bigquery_table`.
///
/// The rows travel in the graph, so the job runs no Rust code on the workers. The table is
/// created when it does not exist.
pub fn build_summary_pipeline(
    options: &PipelineOptions,
    args: &NexmarkArgs,
    rows: &[SummaryRow],
) -> Result<Pipeline, LauncherError> {
    let table = args
        .bigquery_table
        .as_deref()
        .map(str::trim)
        .filter(|table| !table.is_empty())
        .ok_or(LauncherError::MissingFlag {
            flag: "--bigquery_table",
            needed_by: "--export_summary_to_bigquery",
        })?;
    let rows = rows
        .iter()
        .map(SummaryRow::to_row)
        .collect::<Result<Vec<Row>, _>>()
        .map_err(|err| LauncherError::Summary(err.to_string()))?;
    let write = BigQueryWrite::new(table)
        .with_create_disposition(CreateDisposition::CreateIfNeeded)
        .with_write_disposition(WriteDisposition::WriteAppend);
    let write = match &args.expansion_service {
        Some(service) => write.with_expansion_service(service),
        None => write,
    };

    let mut summary_options = options.clone();
    summary_options.streaming = false;
    summary_options.job_name = options
        .job_name
        .as_ref()
        .map(|name| format!("{name}-summary"));
    let pipeline = Pipeline::create(&summary_options);
    pipeline
        .apply("SummaryRows", Create::new(rows))
        .with_row_schema(&SummaryRow::beam_schema())
        .try_apply("WriteSummary", write)?;
    Ok(pipeline)
}

/// Writes the summary rows of `summaries` to BigQuery and waits for the job to end.
pub(super) async fn export_summary(
    options: &PipelineOptions,
    args: &NexmarkArgs,
    summaries: &[QueryRunSummary],
    started_ms: i64,
) -> Result<(), LauncherError> {
    let rows = summary_rows(summaries, &RunInfo::new(options, started_ms))?;
    let pipeline = build_summary_pipeline(options, args, &rows)?;
    let result = pipeline.submit().await?.wait_until_finish(None).await?;
    if result.state.is_successful() {
        Ok(())
    } else {
        Err(LauncherError::Unsuccessful(result.state))
    }
}
