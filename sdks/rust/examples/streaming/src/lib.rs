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

//! Streaming pipeline example demonstrating continuous data generation through periodic impulse.
//!
//! Demonstrates:
//! - Generating periodic clock impulses through [`PeriodicImpulse`] for heartbeats and
//!   slowly-changing dimension updates.
//! - Processing continuous streaming updates using standard Beam transforms.
//! - Writing streaming results out to a file sink using [`textio::Write`].

use std::time::Duration;

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// Command-line arguments for the Streaming pipeline example.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "streaming",
    about = "Apache Beam Rust Streaming Example using PeriodicImpulse",
    version
)]
pub struct StreamingArgs {
    /// Periodic impulse interval in milliseconds.
    #[arg(
        long,
        default_value_t = 500,
        alias = "interval_ms",
        alias = "interval-ms"
    )]
    pub impulse_interval_ms: u64,

    /// Fixed event-time window size in seconds.
    #[arg(long, default_value_t = 5, alias = "window_size")]
    pub window_size_secs: u64,

    /// Optional total number of impulses to generate before completing (unbounded if omitted).
    #[arg(long, alias = "end")]
    pub limit: Option<i64>,

    /// Maximum duration to run streaming generation in seconds (default: 600s = 10 minutes).
    #[arg(long, default_value_t = 600)]
    pub max_read_time_secs: u64,

    /// Output file path for stream records.
    #[arg(long, default_value = "/tmp/streaming_output.txt")]
    pub output: String,
}

impl PipelineOptionGroup for StreamingArgs {}

/// Builds the streaming execution graph from the provided pipeline and configuration arguments.
pub fn build_streaming_graph(pipeline: &Pipeline, args: &StreamingArgs) -> PCollection<String> {
    let mut impulse = PeriodicImpulse::new(
        "PeriodicImpulse",
        Duration::from_millis(args.impulse_interval_ms.max(1)),
    );
    if let Some(limit) = args.limit {
        impulse = impulse.with_limit(limit);
    }
    if args.max_read_time_secs > 0 {
        impulse = impulse.with_max_read_time(Duration::from_secs(args.max_read_time_secs));
    }

    pipeline
        .apply(impulse)
        .window_into(
            "FixedWindows",
            FixedWindows::of(Duration::from_secs(args.window_size_secs.max(1))),
        )
        .map("KeyByMetric", |tick: i64| ("heartbeat".to_string(), tick))
        .group_by_key("GroupPerWindow")
        .par_do_fn(
            "FormatStats",
            |(metric, ticks): (String, BeamIterable<i64>), ctx| {
                let count = ticks
                    .try_into_iter()
                    .try_fold(0usize, |seen, tick| tick.map(|_| seen + 1))
                    .map_err(|e| {
                        Error::from(e)
                            .context(format!("failed to read ticks for metric '{metric}'"))
                    })?;
                let line = match ctx.interval_window() {
                    Some(w) => format!(
                        "[window: {}..{}) metric: '{metric}', count: {count}",
                        w.start_millis, w.end_millis
                    ),
                    None => format!(
                        "[ts: {}] metric: '{metric}', count: {count}",
                        ctx.timestamp()
                    ),
                };
                ctx.emit(line)
            },
        )
}

/// Constructs the complete streaming pipeline reading from the periodic impulse generator and writing to the sink file.
pub fn build_pipeline(options: &PipelineOptions, args: &StreamingArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let stream = build_streaming_graph(&p, args);
    stream.apply(textio::Write::new("WriteLines", &args.output));
    p
}
