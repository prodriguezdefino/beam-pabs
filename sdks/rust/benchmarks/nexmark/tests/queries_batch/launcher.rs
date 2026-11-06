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

use beam::options::PipelineOptions;
use nexmark::config::{NexmarkConfig, NexmarkQueryName, NexmarkSuite, SinkType};
use nexmark::launcher::{
    COUNTER_SINK_BYTES, COUNTER_SINK_RECORDS, COUNTER_SOURCE_BYTES, COUNTER_SOURCE_EVENTS,
    METRIC_NAMESPACE, NexmarkArgs, NexmarkRunOutcome, build_nexmark_pipeline, run_nexmark,
};
use nexmark::models;

#[tokio::test]
async fn count_only_and_devnull_sinks_record_metrics() -> beam::prelude::Result {
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

        assert_eq!(
            metrics.counter(METRIC_NAMESPACE, COUNTER_SOURCE_EVENTS),
            Some(150)
        );
        assert!(
            metrics
                .counter(METRIC_NAMESPACE, COUNTER_SOURCE_BYTES)
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            metrics.counter(METRIC_NAMESPACE, COUNTER_SINK_RECORDS),
            Some(expected_q1_records)
        );
        assert!(
            metrics
                .counter(METRIC_NAMESPACE, COUNTER_SINK_BYTES)
                .unwrap_or(0)
                > 0
        );
    }

    Ok(())
}

#[tokio::test]
async fn text_sink_writes_output_files_for_global_and_windowed_queries() -> beam::prelude::Result {
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
            metrics
                .counter(METRIC_NAMESPACE, COUNTER_SINK_RECORDS)
                .unwrap_or(0),
            expected_records as i64
        );

        let prefix = format!("{}-", query.slug());
        let written_lines: usize = std::fs::read_dir(dir.path())?
            .filter_map(Result::ok)
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
async fn prism_smoke_suite_skips_query11_and_executes_other_queries() -> beam::prelude::Result {
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
        ..NexmarkArgs::default()
    };

    let summaries = run_nexmark(&options, &args).await?;
    assert_eq!(summaries.len(), NexmarkQueryName::ALL.len());

    for summary in &summaries {
        if summary.query == NexmarkQueryName::UserSessions {
            assert_eq!(
                summary.outcome,
                NexmarkRunOutcome::Skipped {
                    reason: "apache/beam#31438"
                }
            );
        } else {
            match summary.outcome {
                NexmarkRunOutcome::Executed { source_events, .. } => {
                    assert_eq!(source_events, 60, "unexpected count on {}", summary.query);
                }
                NexmarkRunOutcome::Skipped { reason } => {
                    panic!("unexpected skip on {}: {reason}", summary.query);
                }
            }
        }
    }

    Ok(())
}
