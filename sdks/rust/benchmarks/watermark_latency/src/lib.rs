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

//! Multi-stage streaming watermark latency benchmark, ported from `WatermarkLatency.java`
//! (VLDB 2021).
//!
//! Generates a rate-limited stream, assigns keys and wall-clock timestamps, windows into
//! fixed 1 s windows, chains `GroupByKey` shuffles, and records inter-shuffle wall-clock
//! delay percentiles (`p50`, `p75`, `p95`, `p99`) in summary windows and Beam distributions.

use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Metric namespace for all counters and distributions of `WatermarkLatency`.
pub const METRIC_NAMESPACE: &str = "watermark_latency";

/// Command-line options for the `WatermarkLatency` pipeline.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "watermark_latency",
    about = "Multi-stage streaming watermark latency benchmark",
    version
)]
pub struct Args {
    /// Number of chained `GroupByKey` shuffle stages.
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(usize))]
    pub num_shuffles: usize,

    /// Target input rate, in elements per second.
    #[arg(long, default_value_t = 1_000, value_parser = clap::value_parser!(i64).range(1..))]
    pub input_rate_per_sec: i64,

    /// Distinct shuffle keys.
    #[arg(long, default_value_t = 1_000, value_parser = clap::value_parser!(i64).range(1..))]
    pub num_keys: i64,

    /// Fixed window length of the shuffle chain, in seconds.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
    pub window_size_secs: u64,

    /// Fixed window length of the percentile summary stage, in seconds.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    pub summary_window_secs: u64,

    /// Optional duration after which the unbounded source stops, in seconds.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub max_read_time_secs: Option<u64>,

    /// Optional element count upper bound (`[0, num_events)`).
    #[arg(long, value_parser = clap::value_parser!(i64).range(1..))]
    pub num_events: Option<i64>,

    /// Label written in the first column of each summary CSV line.
    #[arg(long, default_value = "Default")]
    pub config_name: String,

    /// Optional output path for summary CSV lines.
    #[arg(long)]
    pub output: Option<String>,
}

impl PipelineOptionGroup for Args {}

/// Percentile summary of one inter-shuffle transition (`GBK{i-1}-GBK{i}`) in one window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatencySummary {
    pub config_name: String,
    pub stage: String,
    pub p50_ms: i64,
    pub p75_ms: i64,
    pub p95_ms: i64,
    pub p99_ms: i64,
    pub count: usize,
}

impl fmt::Display for LatencySummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}, {}, {}, {}, {}, {}, {}",
            self.config_name,
            self.stage,
            self.p50_ms,
            self.p75_ms,
            self.p95_ms,
            self.p99_ms,
            self.count
        )
    }
}

impl FromStr for LatencySummary {
    type Err = String;

    fn from_str(line: &str) -> std::result::Result<Self, Self::Err> {
        let parts: Vec<&str> = line.split(',').map(str::trim).collect();
        let [config_name, stage, p50, p75, p95, p99, count] = parts.as_slice() else {
            return Err(format!("expected 7 comma-separated fields, got {line:?}"));
        };
        let parse_i64 = |s: &str| s.parse::<i64>().map_err(|e| format!("{e}: {line:?}"));
        let count = count
            .parse::<usize>()
            .map_err(|e| format!("{e}: {line:?}"))?;
        Ok(Self {
            config_name: (*config_name).to_string(),
            stage: (*stage).to_string(),
            p50_ms: parse_i64(p50)?,
            p75_ms: parse_i64(p75)?,
            p95_ms: parse_i64(p95)?,
            p99_ms: parse_i64(p99)?,
            count,
        })
    }
}

/// Outputs of one shuffle stage: forwarded records and optional inter-shuffle delay samples.
#[derive(Outputs)]
pub enum ShuffleOutput {
    Forwarded((i64, i64)),
    Latency((String, i64)),
}

/// Computes `p50`, `p75`, `p95`, and `p99` over `delays_ms` with `floor(len * q)` indexing.
#[must_use]
pub fn compute_summary(
    config_name: &str,
    stage: &str,
    mut delays_ms: Vec<i64>,
) -> Option<LatencySummary> {
    if delays_ms.is_empty() {
        return None;
    }
    delays_ms.sort_unstable();
    let count = delays_ms.len();
    let at = |q: u128| {
        let index = usize::try_from((count as u128 * q) / 100).unwrap_or(0);
        delays_ms.get(index).copied().unwrap_or(0)
    };
    Some(LatencySummary {
        config_name: config_name.to_string(),
        stage: stage.to_string(),
        p50_ms: at(50),
        p75_ms: at(75),
        p95_ms: at(95),
        p99_ms: at(99),
        count,
    })
}

fn current_wall_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[derive(Clone)]
struct StampWallClockFn {
    num_keys: i64,
    events: Counter,
}

impl DoFn for StampWallClockFn {
    type In = i64;
    type Out = (i64, i64);

    fn process_element(
        &mut self,
        seq: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        self.events.inc();
        let key = seq.rem_euclid(self.num_keys.max(1));
        ctx.emit((key, current_wall_millis()))
    }
}

#[derive(Clone)]
struct ShuffleStageFn {
    stage_label: Option<String>,
    delay_dist: Option<Distribution>,
    samples: Counter,
}

impl DoFn for ShuffleStageFn {
    type In = (i64, BeamIterable<i64>);
    type Out = ShuffleOutput;

    fn process_element(
        &mut self,
        (key, values): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let now_ms = current_wall_millis();
        let next_key = key.wrapping_add(1);
        let mut last_gbk_ms = None;
        for item in values {
            let prev_ms = item?;
            last_gbk_ms = Some(prev_ms);
            ctx.emit(ShuffleOutput::Forwarded((next_key, now_ms)))?;
        }
        if let (Some(stage), Some(prev_ms)) = (&self.stage_label, last_gbk_ms) {
            let delay_ms = now_ms.saturating_sub(prev_ms).max(0);
            if let Some(dist) = &self.delay_dist {
                dist.update(delay_ms);
            }
            self.samples.inc();
            ctx.emit(ShuffleOutput::Latency((stage.clone(), delay_ms)))?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct FormatSummaryFn {
    config_name: String,
}

impl DoFn for FormatSummaryFn {
    type In = (String, BeamIterable<i64>);
    type Out = String;

    fn process_element(
        &mut self,
        (stage, values): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let delays: Vec<i64> = values.into_vec()?;
        if let Some(summary) = compute_summary(&self.config_name, &stage, delays) {
            let line = summary.to_string();
            tracing::info!("{line}");
            ctx.emit(line)?;
        }
        Ok(())
    }
}

/// Builds the `WatermarkLatency` graph on `p` and returns formatted summary CSV lines.
pub fn build_pipeline(p: &Pipeline, args: &Args) -> PCollection<String> {
    let mut source =
        GenerateSequence::new(0).with_rate(args.input_rate_per_sec, Duration::from_secs(1));
    if let Some(secs) = args.max_read_time_secs {
        source = source.with_max_read_time(Duration::from_secs(secs));
    }
    if let Some(end) = args.num_events {
        source = source.with_end(end);
    }

    let mut current = p
        .apply("Generate", source)
        .apply(
            "StampWallClock",
            ParDo::new(StampWallClockFn {
                num_keys: args.num_keys,
                events: Metrics::counter(METRIC_NAMESPACE, "events"),
            }),
        )
        .apply(
            "Window1s",
            WindowInto::new(FixedWindows::of(Duration::from_secs(args.window_size_secs)))
                .with_accumulation_mode(AccumulationMode::Discarding)
                .with_allowed_lateness(Duration::ZERO),
        );

    let mut latencies = Vec::new();
    for idx in 0..args.num_shuffles {
        let stage_label = (idx > 0).then(|| format!("GBK{}-GBK{idx}", idx - 1));
        let delay_dist = stage_label
            .as_ref()
            .map(|label| Metrics::distribution(METRIC_NAMESPACE, label.clone()));
        let grouped = current.apply(format!("GBK{idx}"), GroupByKey::new());
        let ShuffleOutputPCollections { forwarded, latency } = grouped.apply(
            format!("RecordStage{idx}"),
            ParDo::new(ShuffleStageFn {
                stage_label,
                delay_dist,
                samples: Metrics::counter(METRIC_NAMESPACE, "latency_samples"),
            }),
        );
        if idx > 0 {
            latencies.push(latency);
        }
        current = forwarded;
    }

    let Some((first, rest)) = latencies.split_first() else {
        return p.apply("NoSummaryStages", Create::new(Vec::<String>::new()));
    };
    let merged = if rest.is_empty() {
        first.clone()
    } else {
        let refs: Vec<&PCollection<(String, i64)>> = rest.iter().collect();
        first.flatten("MergeStageLatencies", refs.as_slice())
    };

    merged
        .apply(
            "SummaryWindow",
            WindowInto::new(FixedWindows::of(Duration::from_secs(
                args.summary_window_secs,
            )))
            .with_accumulation_mode(AccumulationMode::Discarding)
            .with_allowed_lateness(Duration::ZERO),
        )
        .apply("GroupByStage", GroupByKey::new())
        .apply(
            "ComputePercentiles",
            ParDo::new(FormatSummaryFn {
                config_name: args.config_name.clone(),
            }),
        )
}
