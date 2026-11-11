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

//! BigQuery summary export: row content, table schema, and the graph of the summary job.

use beam::options::PipelineOptions;
use beam::prelude::*;
use nexmark::config::{NexmarkConfig, NexmarkQueryName};
use nexmark::launcher::{
    LauncherError, NexmarkArgs, NexmarkPerf, NexmarkRunOutcome, QueryRunSummary, RunInfo,
    StopReason, SummaryRow, build_summary_pipeline, summary_rows,
};

const STARTED_MS: i64 = 1_700_000_000_123;

fn run_info() -> RunInfo {
    RunInfo {
        run_id: "nexmark-run".to_owned(),
        started_ms: STARTED_MS,
        streaming: true,
        runner: "DataflowRunner".to_owned(),
    }
}

fn summaries() -> Vec<QueryRunSummary> {
    let perf = NexmarkPerf {
        runtime_sec: Some(2.0),
        num_events: Some(1_000),
        events_per_sec: Some(500.0),
        num_results: Some(10),
        ..NexmarkPerf::default()
    };
    vec![
        QueryRunSummary {
            query: NexmarkQueryName::Passthrough,
            config: NexmarkConfig {
                query: Some(NexmarkQueryName::Passthrough),
                ..NexmarkConfig::default()
            },
            outcome: NexmarkRunOutcome::Executed {
                perf,
                stop: StopReason::Finished,
                snapshots: Vec::new(),
            },
        },
        QueryRunSummary {
            query: NexmarkQueryName::Selection,
            config: NexmarkConfig::default(),
            outcome: NexmarkRunOutcome::Failed {
                reason: "job ended in state FAILED".to_owned(),
            },
        },
        QueryRunSummary {
            query: NexmarkQueryName::UserSessions,
            config: NexmarkConfig::default(),
            outcome: NexmarkRunOutcome::Skipped {
                reason: "--skip_queries",
            },
        },
    ]
}

#[test]
fn rows_hold_every_query_with_its_status_and_figures() {
    let rows = summary_rows(&summaries(), &run_info()).expect("rows");
    assert_eq!(rows.len(), 3);

    let ok = &rows[0];
    assert_eq!(ok.timestamp.timestamp_millis(), STARTED_MS);
    assert_eq!(ok.run_id, "nexmark-run");
    assert_eq!(
        (ok.query_number, ok.query_name.as_str()),
        (0, "PASSTHROUGH")
    );
    assert_eq!(
        (ok.mode.as_str(), ok.runner.as_str()),
        ("streaming", "DataflowRunner")
    );
    assert_eq!((ok.status.as_str(), ok.reason.as_deref()), ("OK", None));
    assert_eq!(ok.runtime_sec, Some(2.0));
    assert_eq!(ok.events_per_sec, Some(500.0));
    assert_eq!(ok.num_results, Some(10));
    assert_eq!(ok.time_dilation, None);
    let config: serde_json::Value = serde_json::from_str(&ok.config).expect("config JSON");
    assert_eq!(config["query"], "PASSTHROUGH");
    assert_eq!(config["num_events"], 100_000);

    let failed = &rows[1];
    assert_eq!(
        (failed.status.as_str(), failed.reason.as_deref()),
        ("FAILED", Some("job ended in state FAILED"))
    );
    let skipped = &rows[2];
    assert_eq!(
        (skipped.query_number, skipped.status.as_str()),
        (11, "SKIPPED")
    );
    assert_eq!(skipped.reason.as_deref(), Some("--skip_queries"));
    for row in [failed, skipped] {
        assert_eq!(
            (
                row.runtime_sec,
                row.num_events,
                row.events_per_sec,
                row.num_results
            ),
            (None, None, None, None)
        );
    }
}

#[test]
fn run_info_names_the_run_after_the_job_and_start_time() {
    let mut options = PipelineOptions::default();
    let info = RunInfo::new(&options, 42);
    assert_eq!(info.run_id, "nexmark-42");
    assert_eq!(info.runner, "prism");
    assert!(!info.streaming);

    options.job_name = Some("bench".to_owned());
    options.runner = "DataflowRunner".to_owned();
    options.streaming = true;
    let info = RunInfo::new(&options, 42);
    assert_eq!(info.run_id, "bench-42");
    assert_eq!(info.runner, "DataflowRunner");
    assert!(info.streaming);
}

#[test]
fn schema_uses_java_column_names_and_nullable_figures() {
    let schema = SummaryRow::beam_schema();
    let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "timestamp",
            "runId",
            "queryNumber",
            "queryName",
            "mode",
            "runner",
            "status",
            "reason",
            "config",
            "runtimeSec",
            "numEvents",
            "eventsPerSec",
            "eventBytesPerSec",
            "numResults",
            "resultsPerSec",
            "resultBytesPerSec",
            "startupDelaySec",
            "processingDelaySec",
            "timeDilation",
        ]
    );
    let nullable: Vec<&str> = schema
        .fields
        .iter()
        .filter(|f| f.field_type.nullable)
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(nullable[0], "reason");
    assert_eq!(nullable.len(), 11);
}

#[test]
fn summary_job_writes_the_rows_with_the_storage_write_api() {
    let mut options = PipelineOptions::default();
    options.streaming = true;
    options.job_name = Some("bench".to_owned());
    let args = NexmarkArgs {
        export_summary_to_bigquery: true,
        bigquery_table: Some("p:d.t".to_owned()),
        ..NexmarkArgs::default()
    };
    let rows = summary_rows(&summaries(), &run_info()).expect("rows");
    let pipeline = build_summary_pipeline(&options, &args, &rows).expect("summary graph");
    pipeline.validate().expect("valid summary graph");

    let proto = pipeline.to_proto();
    let components = proto.components.expect("pipeline components");
    let names: Vec<&str> = components
        .transforms
        .values()
        .map(|transform| transform.unique_name.as_str())
        .collect();
    // A bounded input writes to pending streams and commits them once.
    for step in [
        "WriteSummary/PrepareTable",
        "WriteSummary/WritePendingStreams",
        "WriteSummary/CommitStreams",
    ] {
        assert!(names.contains(&step), "{step} in {names:?}");
    }
    assert!(
        !names
            .iter()
            .any(|name| name.contains("WriteBufferedStreams")),
        "{names:?}"
    );

    let write = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "WriteSummary")
        .expect("BigQuery write");
    let input = write.inputs.values().next().expect("write input");
    let coder_id = &components.pcollections[input].coder_id;
    let coder = components.coders[coder_id]
        .spec
        .as_ref()
        .expect("coder spec");
    let schema = Schema::from_proto_bytes(&coder.payload).expect("row schema");
    assert_eq!(schema.fields, SummaryRow::beam_schema().fields);
}

#[test]
fn summary_job_needs_a_table() {
    let rows = summary_rows(&summaries(), &run_info()).expect("rows");
    let args = NexmarkArgs {
        export_summary_to_bigquery: true,
        ..NexmarkArgs::default()
    };
    assert!(matches!(
        build_summary_pipeline(&PipelineOptions::default(), &args, &rows),
        Err(LauncherError::MissingFlag {
            flag: "--bigquery_table",
            ..
        })
    ));
}
