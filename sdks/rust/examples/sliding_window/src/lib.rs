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

//! Sliding Window Moving Average example demonstrating multi-window element assignment.
//!
//! Demonstrates:
//! - Partitioning time-series events into overlapping sliding (hopping) windows.
//! - Multi-window element assignment where each event falls into `size / period` windows.
//! - Computing rolling aggregate statistics (count, average, minimum, maximum) per key and window.
//! - Extracting and formatting sliding window boundaries through
//!   [`ProcessContext::interval_window`].

use std::time::Duration;

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// Command-line arguments for the Sliding Window Moving Average pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "sliding_window",
    about = "Apache Beam Rust Sliding Window Moving Average Example",
    version
)]
pub struct SlidingWindowArgs {
    /// Input file path or glob pattern with CSV sensor readings (`sensor_id,reading,timestamp_ms` or `user,team,score,timestamp_ms`).
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/game/small/gaming_data.csv"
    )]
    pub input: String,

    /// Output file path for formatted moving average metrics.
    #[arg(long, default_value = "/tmp/sliding_window_output.txt")]
    pub output: String,

    /// Window duration size in seconds.
    #[arg(long, default_value_t = 30)]
    pub window_size: u64,

    /// Sliding period frequency in seconds (how often a new window begins).
    #[arg(long, default_value_t = 10)]
    pub window_period: u64,
}

impl PipelineOptionGroup for SlidingWindowArgs {}

/// Parsed time-series sensor reading record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SensorReading {
    pub sensor_id: String,
    pub reading: f64,
    pub timestamp_ms: i64,
}

/// Parses a CSV line (`sensor_id,reading,timestamp_ms` or `user,team,score,timestamp_ms`) into a [`SensorReading`].
pub fn parse_sensor_reading_line(line: &str) -> Option<SensorReading> {
    let fields: Vec<&str> = line.trim().split(',').map(str::trim).collect();
    let (reading_idx, ts_idx) = match fields.len() {
        4.. => (2, 3),
        3 => (1, 2),
        _ => return None,
    };
    Some(SensorReading {
        sensor_id: fields[0].to_string(),
        reading: fields[reading_idx].parse().ok()?,
        timestamp_ms: fields[ts_idx].parse().ok()?,
    })
}

/// Builds the Sliding Window Moving Average pipeline graph.
pub fn build_sliding_window_graph(
    lines: &PCollection<String>,
    window_size_secs: u64,
    window_period_secs: u64,
) -> PCollection<String> {
    lines
        .par_do_fn(
            "AssignSensorTimestamps",
            |line: String, ctx| match parse_sensor_reading_line(&line) {
                Some(r) => ctx
                    .output((r.sensor_id, r.reading))
                    .at(r.timestamp_ms)
                    .emit(),
                None => Ok(()),
            },
        )
        .window_into(
            "SlidingWindows",
            SlidingWindows::of(Duration::from_secs(window_size_secs))
                .every(Duration::from_secs(window_period_secs)),
        )
        .group_by_key("GroupSensorReadings")
        .par_do_fn(
            "FormatMovingAverage",
            |(sensor_id, values): (String, BeamIterable<f64>), ctx| {
                let values = values.into_vec()?;
                let count = values.len();
                let sum: f64 = values.iter().sum();
                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                let min = values.iter().copied().min_by(f64::total_cmp).unwrap_or(0.0);
                let max = values.iter().copied().max_by(f64::total_cmp).unwrap_or(0.0);
                let stats = format!("count={count}|avg={avg:.2}|min={min:.2}|max={max:.2}");
                let line = match ctx.interval_window() {
                    Some(w) => format!(
                        "AVG|{sensor_id}|[{}..{})|{stats}",
                        w.start_millis, w.end_millis
                    ),
                    None => format!("AVG|{sensor_id}|(global)|{stats}"),
                };
                ctx.emit(line)
            },
        )
}

/// Constructs the complete Sliding Window pipeline reading from input and writing to output.
pub fn build_pipeline(options: &PipelineOptions, args: &SlidingWindowArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let lines = p.apply(textio::Read::new("ReadLines", &args.input));
    let averages = build_sliding_window_graph(&lines, args.window_size, args.window_period);
    averages.apply(textio::Write::new("WriteLines", &args.output));
    p
}
