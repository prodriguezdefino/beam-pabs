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

use beam::metrics::MetricResults;
use beam::options::PipelineOptions;
use beam::prelude::*;
use beam::testing::TestPipeline;
use nexmark::config::{NexmarkConfig, NexmarkQueryName, NexmarkSuite, SinkType};
use nexmark::correctness::assert_timestamped;
use nexmark::launcher::{
    BYTES, ELEMENTS, EVENT_TIMESTAMP_MS, EVENTS, METRIC_NAMESPACE, NexmarkArgs, NexmarkRunOutcome,
    PROCESSING_TIME_MS, RESULTS, build_nexmark_pipeline, metric_name, run_nexmark,
};
use nexmark::models;
use nexmark::queries::Query2;
use nexmark::source::BoundedEventSource;

fn monitor_counter(metrics: &MetricResults, prefix: &str, metric: &str) -> i64 {
    metrics
        .counter(METRIC_NAMESPACE, &metric_name(prefix, metric))
        .unwrap_or(0)
}

#[tokio::test]
async fn count_only_and_devnull_sinks_record_monitor_metrics() -> Result {
    let options = PipelineOptions::default();
    let expected_q1_records = models::query1(&NexmarkConfig {
        num_events: 150,
        num_event_generators: 2,
        ..NexmarkConfig::default()
    })
    .len() as i64;

    for sink_type in [SinkType::CountOnly, SinkType::Devnull] {
        let args = NexmarkArgs {
            query: Some(NexmarkQueryName::CurrencyConversion),
            sink_type: Some(sink_type),
            num_events: Some(150),
            num_event_generators: Some(2),
            ..NexmarkArgs::default()
        };
        let config = args.resolve_configs().remove(0);
        let pipeline = build_nexmark_pipeline(&options, &args, &config)?;
        let result = pipeline.run().await?;
        let metrics = result.metrics().expect("Prism reports pipeline metrics");

        assert_eq!(monitor_counter(metrics, EVENTS, ELEMENTS), 150);
        assert!(monitor_counter(metrics, EVENTS, BYTES) > 0);
        assert_eq!(
            monitor_counter(metrics, RESULTS, ELEMENTS),
            expected_q1_records
        );
        assert!(monitor_counter(metrics, RESULTS, BYTES) > 0);
        for (prefix, metric) in [
            (EVENTS, PROCESSING_TIME_MS),
            (EVENTS, EVENT_TIMESTAMP_MS),
            (RESULTS, PROCESSING_TIME_MS),
            (RESULTS, EVENT_TIMESTAMP_MS),
        ] {
            let dist = metrics
                .distribution(METRIC_NAMESPACE, &metric_name(prefix, metric))
                .unwrap_or_else(|| panic!("missing distribution {prefix}.{metric}"));
            assert!(
                dist.count > 0 && dist.min <= dist.max,
                "{prefix}.{metric}: {dist:?}"
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn text_sink_writes_output_files_for_global_and_windowed_queries() -> Result {
    let dir = tempfile::tempdir()?;
    let output_path = dir.path().to_string_lossy().into_owned();
    let options = PipelineOptions::default();

    for (query, count_expected) in [
        (
            NexmarkQueryName::Selection,
            (|cfg: &NexmarkConfig| models::query2(cfg).len()) as fn(&NexmarkConfig) -> usize,
        ),
        (
            NexmarkQueryName::HotItems,
            (|cfg: &NexmarkConfig| models::query5(cfg).len()) as fn(&NexmarkConfig) -> usize,
        ),
    ] {
        let args = NexmarkArgs {
            query: Some(query),
            sink_type: Some(SinkType::Text),
            output_path: Some(output_path.clone()),
            num_events: Some(200),
            num_event_generators: Some(2),
            auction_skip: Some(2),
            window_size_sec: Some(4),
            window_period_sec: Some(2),
            ..NexmarkArgs::default()
        };
        let config = args.resolve_configs().remove(0);
        let expected_records = count_expected(&config);
        assert!(
            expected_records > 0,
            "expected non-empty output for {query}"
        );

        let pipeline = build_nexmark_pipeline(&options, &args, &config)?;
        let result = pipeline.run().await?;
        let metrics = result.metrics().expect("Prism reports pipeline metrics");
        assert_eq!(
            monitor_counter(metrics, RESULTS, ELEMENTS),
            expected_records as i64
        );

        let prefix = format!("{}-", query.slug());
        let written_lines: usize = std::fs::read_dir(dir.path())?
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".txt"))
            })
            .map(|entry| std::fs::read_to_string(entry.path()).expect("read text shard"))
            .map(|contents| contents.lines().count())
            .sum();
        assert_eq!(written_lines, expected_records);
    }

    Ok(())
}

#[tokio::test]
async fn prism_smoke_suite_passes_correctness_checks_and_skips_query11() -> Result {
    let options = PipelineOptions::default();
    let args = NexmarkArgs {
        suite: NexmarkSuite::Smoke,
        sink_type: Some(SinkType::CountOnly),
        num_events: Some(60),
        num_event_generators: Some(2),
        window_size_sec: Some(2),
        window_period_sec: Some(1),
        session_gap_sec: Some(2),
        max_log_events: Some(20),
        side_input_row_count: Some(16),
        num_key_buckets: Some(8),
        assert_correctness: true,
        ..NexmarkArgs::default()
    };

    let summaries = run_nexmark(&options, &args).await?;
    assert_eq!(summaries.len(), NexmarkQueryName::ALL.len());

    for summary in &summaries {
        match (&summary.outcome, summary.query) {
            (NexmarkRunOutcome::Skipped { reason }, NexmarkQueryName::UserSessions) => {
                assert_eq!(*reason, "apache/beam#31438");
            }
            (NexmarkRunOutcome::Executed(perf), _) => {
                assert_eq!(perf.num_events, Some(60), "events of {}", summary.query);
            }
            (outcome, query) => panic!("unexpected outcome on {query}: {outcome:?}"),
        }
    }

    Ok(())
}

#[tokio::test]
async fn run_reports_perf_from_monitor_metrics() -> Result {
    let options = PipelineOptions::default();
    let args = NexmarkArgs {
        query: Some(NexmarkQueryName::HotItems),
        sink_type: Some(SinkType::CountOnly),
        num_events: Some(1_000),
        num_event_generators: Some(2),
        window_size_sec: Some(4),
        window_period_sec: Some(2),
        assert_correctness: true,
        log_results: true,
        ..NexmarkArgs::default()
    };

    let summaries = run_nexmark(&options, &args).await?;
    let [summary] = summaries.as_slice() else {
        panic!("expected one summary, got {summaries:?}");
    };
    let NexmarkRunOutcome::Executed(perf) = summary.outcome else {
        panic!("expected an executed run, got {:?}", summary.outcome);
    };
    let config = args.resolve_configs().remove(0);
    assert_eq!(perf.num_events, Some(1_000));
    assert_eq!(perf.num_results, Some(models::query5(&config).len() as i64));
    assert!(perf.runtime_sec.is_some_and(|secs| secs > 0.0), "{perf:?}");
    assert!(
        perf.events_per_sec.is_some_and(|rate| rate > 0.0),
        "{perf:?}"
    );
    assert!(
        perf.event_bytes_per_sec.is_some_and(|rate| rate > 0.0),
        "{perf:?}"
    );
    assert!(
        perf.startup_delay_sec.is_some_and(|secs| secs >= 0.0),
        "{perf:?}"
    );
    assert!(
        perf.processing_delay_sec.is_some_and(|secs| secs >= 0.0),
        "{perf:?}"
    );
    assert!(
        perf.time_dilation.is_some_and(|ratio| ratio > 0.0),
        "{perf:?}"
    );
    Ok(())
}

#[tokio::test]
async fn correctness_check_fails_the_job_on_a_wrong_model() {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::Selection),
        num_events: 600,
        num_event_generators: 2,
        auction_skip: 5,
        ..NexmarkConfig::default()
    };
    let wrong_model = models::query2(&NexmarkConfig {
        auction_skip: 7,
        ..config
    });
    assert_ne!(wrong_model, models::query2(&config));

    let p = TestPipeline::new();
    let out = p
        .apply("ReadEvents", BoundedEventSource::new(&config))
        .apply("Query2", Query2::from_config(&config));
    assert_timestamped("Query2WrongModel", &out, wrong_model);
    let err = p.run().await.expect_err("a wrong model fails the job");
    assert!(err.to_string().contains("Query2WrongModel"), "{err}");
}
