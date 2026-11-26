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

//! Tests for the `WatermarkLatency` streaming pipeline and percentile calculation.

use std::collections::BTreeSet;
use std::str::FromStr;

use beam::prelude::*;
use beam::testing::TestPipeline;
use watermark_latency::{Args, LatencySummary, METRIC_NAMESPACE, build_pipeline, compute_summary};

#[test]
fn compute_summary_calculates_sorted_percentiles() {
    let samples: Vec<i64> = (1..=100).rev().collect();
    let summary = compute_summary("Smoke", "GBK0-GBK1", samples).expect("non-empty summary");
    assert_eq!(summary.config_name, "Smoke");
    assert_eq!(summary.stage, "GBK0-GBK1");
    assert_eq!(summary.count, 100);
    assert_eq!(summary.p50_ms, 51);
    assert_eq!(summary.p75_ms, 76);
    assert_eq!(summary.p95_ms, 96);
    assert_eq!(summary.p99_ms, 100);

    let formatted = summary.to_string();
    let parsed = LatencySummary::from_str(&formatted).expect("round-trip summary line");
    assert_eq!(parsed, summary);
    assert!(compute_summary("Smoke", "GBK0-GBK1", Vec::new()).is_none());
}

#[tokio::test]
async fn streaming_pipeline_emits_inter_shuffle_latencies() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out_prefix = dir.path().join("summary.csv");
    let out_str = out_prefix.to_str().expect("utf8 path").to_string();

    let args = Args {
        num_shuffles: 3,
        input_rate_per_sec: 50,
        num_keys: 8,
        window_size_secs: 1,
        summary_window_secs: 1,
        max_read_time_secs: None,
        num_events: Some(80),
        config_name: "PrismTest".to_string(),
        output: Some(out_str),
    };

    let p = TestPipeline::new();
    let summaries = build_pipeline(&p, &args);
    summaries.apply(
        "WriteSummaries",
        textio::Write::new(args.output.as_ref().expect("output")),
    );
    let result = p.run().await.expect("pipeline finishes");

    let mut stages = BTreeSet::new();
    for entry in std::fs::read_dir(dir.path()).expect("read output dir") {
        let path = entry.expect("dir entry").path();
        if !path.is_file() {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read shard");
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let parsed = LatencySummary::from_str(line).expect("valid summary CSV line");
            assert_eq!(parsed.config_name, "PrismTest");
            assert!(parsed.count > 0);
            assert!(parsed.p50_ms >= 0);
            assert!(parsed.p50_ms <= parsed.p75_ms);
            assert!(parsed.p75_ms <= parsed.p95_ms);
            assert!(parsed.p95_ms <= parsed.p99_ms);
            stages.insert(parsed.stage);
        }
    }
    assert_eq!(
        stages,
        BTreeSet::from(["GBK0-GBK1".to_string(), "GBK1-GBK2".to_string()])
    );

    let metrics = result.metrics().expect("metrics");
    assert_eq!(metrics.counter(METRIC_NAMESPACE, "events"), Some(80));
    assert!(
        metrics
            .distribution(METRIC_NAMESPACE, "GBK0-GBK1")
            .is_some()
    );
    assert!(
        metrics
            .distribution(METRIC_NAMESPACE, "GBK1-GBK2")
            .is_some()
    );
}
