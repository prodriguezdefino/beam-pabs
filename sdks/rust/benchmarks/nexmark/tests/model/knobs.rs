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

//! Load knobs, side-input flags, skip list and parallel runs: flags, graphs and pure helpers.

use std::time::{Duration, Instant};

use beam::options::{PipelineOptions, try_parse_from};
use beam::prelude::*;
use nexmark::config::{NexmarkConfig, NexmarkQueryName, NexmarkSuite, SideInputType};
use nexmark::launcher::{
    LauncherError, NexmarkArgs, NexmarkRunOutcome, QueryRunSummary, StopReason,
    build_nexmark_pipeline, build_side_input_pipeline, format_summary_table,
};
use nexmark::load::{burn_cpu, write_sizes};

fn transform_names(pipeline: &Pipeline) -> Vec<String> {
    let components = pipeline.to_proto().components.expect("pipeline components");
    components
        .transforms
        .values()
        .map(|transform| transform.unique_name.clone())
        .collect()
}

#[test]
fn parses_load_side_input_skip_and_parallel_flags() {
    let (_, args) = try_parse_from::<NexmarkArgs, _, _>([
        "nexmark",
        "--suite=SMOKE",
        "--cpu_delay_ms=3",
        "--disk_busy_bytes=1024",
        "--preload_seconds=30",
        "--running_time_minutes=2",
        "--side_input_type=CSV",
        "--side_input_url=/tmp/side",
        "--side_input_num_shards=4",
        "--skip_queries=11,hot_items,BOUNDED_SIDE_INPUT_JOIN",
        "--nexmark_parallel=4",
        "--export_summary_to_bigquery",
        "--bigquery_table=p:d.t",
    ])
    .expect("flags");
    assert_eq!(
        args.skip_queries,
        [
            NexmarkQueryName::UserSessions,
            NexmarkQueryName::HotItems,
            NexmarkQueryName::BoundedSideInputJoin
        ]
    );
    assert_eq!(args.nexmark_parallel, 4);
    assert_eq!(args.running_time_minutes, Some(2));
    assert_eq!(args.side_input_url.as_deref(), Some("/tmp/side"));
    assert!(args.export_summary_to_bigquery);
    assert_eq!(args.bigquery_table.as_deref(), Some("p:d.t"));

    let config = args.resolve_configs().remove(0);
    assert_eq!(config.cpu_delay_ms, 3);
    assert_eq!(config.disk_busy_bytes, 1024);
    assert_eq!(config.preload_seconds, 30);
    assert_eq!(config.side_input_type, SideInputType::Csv);
    assert_eq!(config.side_input_num_shards, 4);

    let defaults = NexmarkArgs::default().resolve_configs().remove(0);
    assert_eq!(
        (
            defaults.cpu_delay_ms,
            defaults.disk_busy_bytes,
            defaults.preload_seconds
        ),
        (0, 0, 0)
    );
    assert_eq!(defaults.side_input_type, SideInputType::Direct);
    assert_eq!(defaults.side_input_num_shards, 3);
    assert!(try_parse_from::<NexmarkArgs, _, _>(["nexmark", "--skip_queries=99"]).is_err());
}

#[test]
fn load_steps_run_before_the_query_only_when_set() {
    let options = PipelineOptions::default();
    let args = |cpu: i64, disk: i64| NexmarkArgs {
        query: Some(NexmarkQueryName::CurrencyConversion),
        num_events: Some(10),
        cpu_delay_ms: Some(cpu),
        disk_busy_bytes: Some(disk),
        ..NexmarkArgs::default()
    };
    let names = |args: &NexmarkArgs| {
        let config = args.resolve_configs().remove(0);
        transform_names(&build_nexmark_pipeline(&options, args, &config).expect("graph"))
    };
    let plain = names(&args(0, 0));
    assert!(!plain.iter().any(|name| name.starts_with("CpuDelay")));
    assert!(!plain.iter().any(|name| name.starts_with("DiskBusy")));
    let loaded = names(&args(5, 100));
    assert!(loaded.iter().any(|name| name == "CpuDelay"), "{loaded:?}");
    assert!(loaded.iter().any(|name| name == "DiskBusy"), "{loaded:?}");
}

#[test]
fn cpu_delay_works_for_the_wall_time() {
    let start = Instant::now();
    assert!(burn_cpu(Duration::from_millis(20)) > 0);
    assert!(start.elapsed() >= Duration::from_millis(20));
    assert_eq!(burn_cpu(Duration::ZERO), 0);
}

#[test]
fn disk_busy_writes_at_most_16_mib_at_a_time() {
    const MIB16: u64 = 1 << 24;
    assert_eq!(write_sizes(0).count(), 0);
    assert_eq!(write_sizes(10).collect::<Vec<_>>(), [10]);
    assert_eq!(write_sizes(MIB16).collect::<Vec<_>>(), [MIB16]);
    assert_eq!(
        write_sizes(2 * MIB16 + 5).collect::<Vec<_>>(),
        [MIB16, MIB16, 5]
    );
}

#[test]
fn csv_side_input_needs_a_url_and_reads_the_files() {
    let options = PipelineOptions::default();
    let join = |url: Option<&str>| NexmarkArgs {
        query: Some(NexmarkQueryName::BoundedSideInputJoin),
        num_events: Some(10),
        side_input_type: Some(SideInputType::Csv),
        side_input_url: url.map(str::to_owned),
        ..NexmarkArgs::default()
    };
    let missing = join(None);
    let config = missing.resolve_configs().remove(0);
    assert!(matches!(
        build_nexmark_pipeline(&options, &missing, &config),
        Err(LauncherError::MissingFlag {
            flag: "--side_input_url",
            ..
        })
    ));
    // A query without a side input ignores the side-input flags.
    let other = NexmarkArgs {
        query: Some(NexmarkQueryName::Passthrough),
        ..missing
    };
    let config = other.resolve_configs().remove(0);
    assert!(build_nexmark_pipeline(&options, &other, &config).is_ok());

    let with_url = join(Some("/tmp/side"));
    let config = with_url.resolve_configs().remove(0);
    let names =
        transform_names(&build_nexmark_pipeline(&options, &with_url, &config).expect("graph"));
    assert!(
        names.iter().any(|name| name.ends_with("ReadSideInput")),
        "{names:?}"
    );
    assert!(
        !names.iter().any(|name| name.ends_with("/SideInput")),
        "{names:?}"
    );
}

#[test]
fn side_input_job_covers_the_largest_csv_join() {
    let options = PipelineOptions::default();
    let args = NexmarkArgs {
        suite: NexmarkSuite::Smoke,
        side_input_url: Some("/tmp/side".to_owned()),
        ..NexmarkArgs::default()
    };
    let direct = args.resolve_configs();
    assert!(
        build_side_input_pipeline(&options, &args, &direct)
            .expect("direct")
            .is_none()
    );

    let csv: Vec<NexmarkConfig> = direct
        .iter()
        .map(|config| NexmarkConfig {
            side_input_type: SideInputType::Csv,
            ..*config
        })
        .collect();
    let pipeline = build_side_input_pipeline(&options, &args, &csv)
        .expect("csv")
        .expect("a job for the CSV joins");
    let names = transform_names(&pipeline);
    assert!(
        names.iter().any(|name| name == "SideInputLines"),
        "{names:?}"
    );
    assert!(
        names.iter().any(|name| name == "WriteSideInput"),
        "{names:?}"
    );

    let no_url = NexmarkArgs {
        side_input_url: None,
        ..args
    };
    assert!(matches!(
        build_side_input_pipeline(&options, &no_url, &csv),
        Err(LauncherError::MissingFlag { .. })
    ));
}

#[test]
fn summary_table_shows_running_time_and_skip_list_rows() {
    let summaries = [
        QueryRunSummary {
            query: NexmarkQueryName::Passthrough,
            config: NexmarkConfig::default(),
            outcome: NexmarkRunOutcome::Executed {
                perf: Default::default(),
                stop: StopReason::RunningTimeReached,
                snapshots: Vec::new(),
            },
        },
        QueryRunSummary {
            query: NexmarkQueryName::HotItems,
            config: NexmarkConfig::default(),
            outcome: NexmarkRunOutcome::Skipped {
                reason: "--skip_queries",
            },
        },
    ];
    let table = format_summary_table(&summaries);
    assert!(table.contains("OK (running time)"), "{table}");
    assert!(table.contains("SKIPPED (--skip_queries)"), "{table}");
    assert_eq!(summaries[0].outcome.status(), "OK (running time)");
    assert_eq!(summaries[1].outcome.reason(), Some("--skip_queries"));
}
