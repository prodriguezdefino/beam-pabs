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

use beam::options::{PipelineOptions, WorkerOptions, try_parse_from};
use nexmark::config::{
    NexmarkConfig, NexmarkQueryName, NexmarkSuite, PubsubMode, RateShape, RateUnit, SinkType,
    SourceType,
};
use nexmark::launcher::{
    LauncherError, NexmarkArgs, NexmarkPerf, NexmarkRunOutcome, QueryRunSummary, StopReason,
    build_nexmark_pipeline, format_summary_table, is_prism_runner,
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
        "--assert_correctness",
        "--log_events",
        "--log_results",
        "--stream_timeout=90",
        "--cancel_streaming_job_after_finish",
        "--pubsub_mode=SUBSCRIBE_ONLY",
        "--pubsub_topic=projects/p/topics/t",
        "--pubsub_subscription=projects/p/subscriptions/s",
        "--pubsub_results_topic=projects/p/topics/r",
        "--use_pubsub_publish_time",
        "--kafka_topic=events",
        "--kafka_results_topic=results",
        "--bootstrap_servers=broker:9092",
        "--expansion_service=localhost:8097",
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
    assert!(args.assert_correctness && args.log_events && args.log_results);
    assert_eq!(args.stream_timeout, Some(90));
    assert!(args.cancel_streaming_job_after_finish);
    assert_eq!(args.resolve_configs()[0].stream_timeout, 90);
    assert_eq!(args.pubsub_mode, Some(PubsubMode::SubscribeOnly));
    assert_eq!(args.pubsub_topic.as_deref(), Some("projects/p/topics/t"));
    assert_eq!(
        args.pubsub_subscription.as_deref(),
        Some("projects/p/subscriptions/s")
    );
    assert_eq!(
        args.pubsub_results_topic.as_deref(),
        Some("projects/p/topics/r")
    );
    assert_eq!(args.kafka_topic.as_deref(), Some("events"));
    assert_eq!(args.kafka_results_topic.as_deref(), Some("results"));
    assert_eq!(args.bootstrap_servers.as_deref(), Some("broker:9092"));
    assert_eq!(args.expansion_service.as_deref(), Some("localhost:8097"));
    let config = args.resolve_configs().remove(0);
    assert_eq!(config.pubsub_mode, PubsubMode::SubscribeOnly);
    assert!(config.use_pubsub_publish_time);
}

#[test]
fn correctness_and_logging_flags_default_to_false() {
    let (_, args) =
        try_parse_from::<NexmarkArgs, _, _>(["nexmark", "--log_results"]).expect("flags");
    assert!(!args.assert_correctness);
    assert!(!args.log_events);
    assert!(args.log_results);
    assert_eq!(args.query10_max_workers, 5);

    let defaults = NexmarkArgs::default();
    assert!(!defaults.assert_correctness && !defaults.log_events && !defaults.log_results);
    assert!(!defaults.cancel_streaming_job_after_finish);
    assert_eq!(defaults.query10_max_workers, 5);
    assert_eq!(defaults.resolve_configs()[0].stream_timeout, 240);
    assert_eq!(
        defaults.resolve_configs()[0].pubsub_mode,
        PubsubMode::Combined
    );
    assert!(!defaults.resolve_configs()[0].use_pubsub_publish_time);
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
        panic!("expected a missing --output_path error");
    };
    assert!(matches!(
        err,
        LauncherError::MissingFlag {
            flag: "--output_path",
            needed_by: "--sink_type=TEXT"
        }
    ));
    let avro = NexmarkConfig {
        sink_type: SinkType::Avro,
        ..config
    };
    assert!(matches!(
        build_nexmark_pipeline(&options, &args, &avro),
        Err(LauncherError::MissingFlag {
            flag: "--output_path",
            needed_by: "--sink_type=AVRO"
        })
    ));
}

#[test]
fn prism_runner_detection_and_summary_table_formatting() {
    assert!(is_prism_runner(""));
    assert!(is_prism_runner("prism"));
    assert!(is_prism_runner("PrismRunner"));
    assert!(!is_prism_runner("DataflowRunner"));
    assert!(!is_prism_runner("dataflow"));

    let perf = NexmarkPerf {
        runtime_sec: Some(0.5),
        num_events: Some(1_000),
        events_per_sec: Some(2_000.0),
        event_bytes_per_sec: Some(400_000.0),
        num_results: Some(1_000),
        results_per_sec: Some(2_000.0),
        result_bytes_per_sec: Some(400_000.0),
        startup_delay_sec: Some(1.25),
        processing_delay_sec: None,
        time_dilation: Some(4.0),
    };
    let summaries = vec![
        QueryRunSummary {
            query: NexmarkQueryName::Passthrough,
            config: NexmarkConfig::default(),
            outcome: NexmarkRunOutcome::Executed {
                perf,
                stop: StopReason::Finished,
                snapshots: Vec::new(),
            },
        },
        QueryRunSummary {
            query: NexmarkQueryName::CurrencyConversion,
            config: NexmarkConfig::default(),
            outcome: NexmarkRunOutcome::Executed {
                perf,
                stop: StopReason::TimedOut,
                snapshots: Vec::new(),
            },
        },
        QueryRunSummary {
            query: NexmarkQueryName::Selection,
            config: NexmarkConfig::default(),
            outcome: NexmarkRunOutcome::Failed {
                reason: "PAssert 'AssertCorrectness' failed".to_owned(),
            },
        },
        QueryRunSummary {
            query: NexmarkQueryName::UserSessions,
            config: NexmarkConfig::default(),
            outcome: NexmarkRunOutcome::Skipped {
                reason: "apache/beam#31438",
            },
        },
    ];
    let table = format_summary_table(&summaries);
    for column in [
        "Runtime(sec)",
        "Events(/sec)",
        "Events(bytes/sec)",
        "Results",
        "Results(/sec)",
        "Startup(sec)",
        "Processing delay(sec)",
    ] {
        assert!(table.contains(column), "missing column {column}:\n{table}");
    }
    let passthrough = table
        .lines()
        .find(|line| line.contains("PASSTHROUGH"))
        .expect("PASSTHROUGH row");
    let cells: Vec<&str> = passthrough.split_whitespace().collect();
    assert_eq!(
        cells,
        [
            "0",
            "PASSTHROUGH",
            "OK",
            "0.500",
            "2000.0",
            "400000.0",
            "1000",
            "2000.0",
            "1.250",
            "-"
        ]
    );
    assert!(
        table
            .lines()
            .any(|line| line.contains("CURRENCY_CONVERSION") && line.contains("TIMEOUT"))
    );
    assert!(table.contains("FAILED"));
    assert!(table.contains("Query 2 (SELECTION) failed: PAssert 'AssertCorrectness' failed"));
    assert!(table.contains("SKIPPED (apache/beam#31438)"));
}
