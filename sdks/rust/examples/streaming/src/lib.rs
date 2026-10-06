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

//! Streaming: counts the ticks of a [`PeriodicImpulse`] in fixed windows.
//!
//! The impulse is an unbounded source whose event times follow the wall clock. Each tick
//! is even or odd, and [`CountTicks`] counts both kinds per window with `.sum_per_key`.

use std::time::Duration;

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(name = "streaming", about = "Apache Beam streaming example", version)]
pub struct Args {
    /// Interval between two ticks, in milliseconds.
    #[arg(long, default_value_t = 500, value_parser = clap::value_parser!(u64).range(1..))]
    pub impulse_interval_ms: u64,

    /// Length of a fixed window, in seconds.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    pub window_size_secs: u64,

    /// Number of ticks before the source stops. Without it, the source stops after
    /// `--max_read_time_secs`.
    #[arg(long, value_parser = clap::value_parser!(i64).range(1..))]
    pub limit: Option<i64>,

    /// Time after which the source stops, in seconds.
    #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..))]
    pub max_read_time_secs: u64,

    /// Path of the file to write to.
    #[arg(long, default_value = "/tmp/streaming_output.txt")]
    pub output: String,
}

impl PipelineOptionGroup for Args {}

impl From<&Args> for PeriodicImpulse {
    fn from(args: &Args) -> Self {
        let impulse = PeriodicImpulse::new(Duration::from_millis(args.impulse_interval_ms))
            .with_max_read_time(Duration::from_secs(args.max_read_time_secs));
        args.limit
            .into_iter()
            .fold(impulse, PeriodicImpulse::with_limit)
    }
}

/// Counts the even and odd ticks of each fixed window, one `[window) parity: count` line
/// per count.
pub struct CountTicks {
    pub window_size: Duration,
}

impl PTransform<PCollection<i64>> for CountTicks {
    type Output = PCollection<String>;

    fn expand(&self, ticks: &PCollection<i64>) -> Self::Output {
        ticks
            .window_into("FixedWindows", FixedWindows::of(self.window_size))
            .map("KeyByParity", |tick: i64| {
                let parity = if tick % 2 == 0 { "even" } else { "odd" };
                (parity.to_string(), 1_i64)
            })
            .sum_per_key("CountPerWindow")
            .par_do_fn(
                "FormatCounts",
                |Windowed {
                     value: (parity, count),
                     window,
                 }: Windowed<(String, i64)>,
                 ctx| ctx.emit(format!("{window} {parity}: {count}")),
            )
    }
}
