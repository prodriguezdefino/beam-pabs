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

use std::time::Duration;

use beam::options::{PipelineOptions, WorkerOptions, try_parse_from};
use nexmark::config::{
    NexmarkConfig, NexmarkQueryName, NexmarkSuite, RateShape, RateUnit, SinkType, SourceType,
};
use nexmark::launcher::{
    LauncherError, NexmarkArgs, NexmarkRunOutcome, QueryRunSummary, build_nexmark_pipeline,
    format_summary_table, is_prism_runner,
};

#[test]
fn parses_nexmark_and_dataflow_flags_without_collision() {
    let (options, args) = try_parse_from::<NexmarkArgs, _, _>([
        "nexmark",
        "--runner=DataflowRunner",
        "--streaming",
        "--job_name=nexmark-q10-bench",
        "--project=my-gcp-project",
        "--region=us-central1",
        "--temp_location=gs://my-bucket/temp",
        "--staging_location=gs://my-bucket/staging",
        "--num_workers=2",
        "--max_num_workers=10",
        "--worker_machine_type=n2-standard-4",
        "--suite=SMOKE",
        "--query=10",
        "--source_type=DIRECT",
        "--sink_type=TEXT",
        "--output_path=gs://my-bucket/nexmark-out",
        "--query10_max_workers=4",
        "--num_events=50000",
        "--num_event_generators=8",
        "--rate_shape=SQUARE",
        "--first_event_rate=5000",
        "--next_event_rate=15000",
        "--rate_unit=PER_SECOND",
        "--rate_period_sec=120",
        "--is_rate_limited",
        "--window_size_sec=30",
        "--window_period_sec=10",
        "--watermark_holdback_sec=2",
        "--num_in_flight_auctions=50",
        "--num_active_people=500",
        "--auction_skip=77",
        "--fanout=8",
        "--session_gap_sec=300",
        "--occasional_delay_sec=5",
        "--prob_delayed_event=0.25",
        "--max_log_events=1000",
        "--out_of_order_group_size=4",
        "--side_input_row_count=250",
        "--num_key_buckets=1024",
        "--pardo_cpu_factor=2.5",
    ])
    .expect("valid Nexmark and Dataflow CLI flags");

    assert_eq!(options.runner, "DataflowRunner");
    assert!(options.streaming);
    assert_eq!(options.job_name.as_deref(), Some("nexmark-q10-bench"));

    let worker_opts: WorkerOptions = options.view_as().expect("worker options");
    assert_eq!(worker_opts.num_workers, Some(2));
    assert_eq!(worker_opts.max_num_workers, Some(10));

    assert_eq!(args.suite, NexmarkSuite::Smoke);
    assert_eq!(args.query, Some(NexmarkQueryName::LogToShardedFiles));
    assert_eq!(args.source_type, Some(SourceType::Direct));
    assert_eq!(args.sink_type, Some(SinkType::Text));
    assert_eq!(
        args.output_path.as_deref(),
        Some("gs://my-bucket/nexmark-out")
    );
    assert_eq!(args.query10_max_workers, 4);
    assert_eq!(args.num_events, Some(50_000));
    assert_eq!(args.num_event_generators, Some(8));
    assert_eq!(args.rate_shape, Some(RateShape::Square));
    assert_eq!(args.first_event_rate, Some(5_000));
    assert_eq!(args.next_event_rate, Some(15_000));
    assert_eq!(args.rate_unit, Some(RateUnit::PerSecond));
    assert_eq!(args.rate_period_sec, Some(120));
    assert_eq!(args.is_rate_limited, Some(true));
    assert_eq!(args.window_size_sec, Some(30));
    assert_eq!(args.window_period_sec, Some(10));
    assert_eq!(args.watermark_holdback_sec, Some(2));
    assert_eq!(args.num_in_flight_auctions, Some(50));
    assert_eq!(args.num_active_people, Some(500));
    assert_eq!(args.auction_skip, Some(77));
    assert_eq!(args.fanout, Some(8));
    assert_eq!(args.session_gap_sec, Some(300));
    assert_eq!(args.occasional_delay_sec, Some(5));
    assert_eq!(args.prob_delayed_event, Some(0.25));
    assert_eq!(args.max_log_events, Some(1_000));
    assert_eq!(args.out_of_order_group_size, Some(4));
    assert_eq!(args.side_input_row_count, Some(250));
    assert_eq!(args.num_key_buckets, Some(1_024));
    assert_eq!(args.pardo_cpu_factor, Some(2.5));
}

#[test]
fn resolves_suites_and_query_overrides() {
    let default_configs = NexmarkArgs::default().resolve_configs();
    assert_eq!(default_configs.len(), 1);
    assert_eq!(
        default_configs[0].query,
        Some(NexmarkQueryName::Passthrough)
    );
    assert_eq!(default_configs[0].num_events, 100_000);

    let smoke_args = NexmarkArgs {
        suite: NexmarkSuite::Smoke,
        ..NexmarkArgs::default()
    };
    let smoke_configs = smoke_args.resolve_configs();
    assert_eq!(smoke_configs.len(), NexmarkQueryName::ALL.len());
    for cfg in &smoke_configs {
        let q = cfg.query.expect("smoke config query");
        assert_eq!(
            &smoke_args.pin_query(q).resolve_configs(),
            std::slice::from_ref(cfg)
        );
    }

    let smoke_q4 = NexmarkArgs {
        suite: NexmarkSuite::Smoke,
        query: Some(NexmarkQueryName::AveragePriceForCategory),
        ..NexmarkArgs::default()
    }
    .resolve_configs();
    assert_eq!(smoke_q4.len(), 1);
    assert_eq!(smoke_q4[0].num_events, 10_000);

    let overridden = NexmarkArgs {
        suite: NexmarkSuite::Stress,
        query: Some(NexmarkQueryName::HotItems),
        num_events: Some(750),
        sink_type: Some(SinkType::CountOnly),
        ..NexmarkArgs::default()
    }
    .resolve_configs();
    assert_eq!(overridden.len(), 1);
    assert_eq!(overridden[0].query, Some(NexmarkQueryName::HotItems));
    assert_eq!(overridden[0].num_events, 750);
    assert_eq!(overridden[0].sink_type, SinkType::CountOnly);
}

#[test]
fn pin_query_roundtrips_through_worker_options_snapshot() {
    let (run_opts, driver_args) =
        try_parse_from::<NexmarkArgs, _, _>(["nexmark", "--suite=SMOKE", "--num_events=300"])
            .expect("driver flags");
    let pinned = driver_args.pin_query(NexmarkQueryName::HotItems);
    run_opts.set(pinned).expect("set pinned args");

    let snapshot_file = tempfile::NamedTempFile::new().expect("temp snapshot file");
    std::fs::write(
        snapshot_file.path(),
        run_opts.snapshot().expect("snapshot").encode(),
    )
    .expect("write snapshot");

    let options_flag = format!("--options_file={}", snapshot_file.path().display());
    let (worker_opts, worker_args) = try_parse_from::<NexmarkArgs, _, _>([
        "nexmark",
        "--worker",
        "--id=worker-1",
        &options_flag,
    ])
    .expect("worker restores pinned options");

    assert!(worker_opts.harness.is_worker());
    let worker_configs = worker_args.resolve_configs();
    assert_eq!(worker_configs.len(), 1);
    assert_eq!(worker_configs[0].query, Some(NexmarkQueryName::HotItems));
    assert_eq!(worker_configs[0].num_events, 300);
}

#[test]
fn text_sink_without_output_path_returns_missing_output_path_error() {
    let options = PipelineOptions::default();
    let args = NexmarkArgs {
        sink_type: Some(SinkType::Text),
        output_path: None,
        ..NexmarkArgs::default()
    };
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::Passthrough),
        sink_type: SinkType::Text,
        num_events: 10,
        ..NexmarkConfig::default()
    };

    let Err(err) = build_nexmark_pipeline(&options, &args, &config) else {
        panic!("expected MissingOutputPath error");
    };
    assert!(matches!(err, LauncherError::MissingOutputPath));
}

#[test]
fn prism_runner_detection_and_summary_table_formatting() {
    assert!(is_prism_runner(""));
    assert!(is_prism_runner("prism"));
    assert!(is_prism_runner("PrismRunner"));
    assert!(!is_prism_runner("DataflowRunner"));
    assert!(!is_prism_runner("dataflow"));

    let summaries = vec![
        QueryRunSummary {
            query: NexmarkQueryName::Passthrough,
            outcome: NexmarkRunOutcome::Executed {
                elapsed: Duration::from_millis(500),
                source_events: 1_000,
                source_bytes: 200_000,
                sink_records: 1_000,
                sink_bytes: 200_000,
            },
        },
        QueryRunSummary {
            query: NexmarkQueryName::UserSessions,
            outcome: NexmarkRunOutcome::Skipped {
                reason: "apache/beam#31438",
            },
        },
    ];
    let table = format_summary_table(&summaries);
    assert!(table.contains("PASSTHROUGH"));
    assert!(table.contains("2000.0"));
    assert!(table.contains("0.40"));
    assert!(table.contains("USER_SESSIONS"));
    assert!(table.contains("SKIPPED (apache/beam#31438)"));
}
