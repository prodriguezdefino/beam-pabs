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

//! Windowed WordCount: counts words in fixed event-time windows.
//!
//! Shows event timestamps set with `ctx.output(..).at(..)`, [`FixedWindows`], the reuse
//! of a composite transform ([`CountWords`]) on windowed data, and [`Windowed`] input to
//! read the window of each count.

use std::time::Duration;

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};
use wordcount::CountWords;

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "windowed_wordcount",
    about = "Apache Beam Windowed WordCount example",
    version
)]
pub struct Args {
    /// Path of the file to read from.
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/shakespeare/kinglear.txt"
    )]
    pub input: String,

    /// Path of the file to write to.
    #[arg(long, default_value = "/tmp/windowed_wordcount.txt")]
    pub output: String,

    /// Length of a fixed window, in seconds.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    pub window_size: u64,

    /// Start of the event times, in milliseconds since the Unix epoch.
    #[arg(long, default_value_t = 1_000_000)]
    pub base_timestamp: i64,
}

impl PipelineOptionGroup for Args {}

/// Counts the words of each window and formats one `[window) word: count` line per count.
///
/// Text files have no event times, so each line gets a time from its hash, in the first
/// three windows after `base_timestamp`.
pub struct WindowedWordCount {
    pub window_size: Duration,
    pub base_timestamp: Instant,
}

impl From<&Args> for WindowedWordCount {
    fn from(args: &Args) -> Self {
        Self {
            window_size: Duration::from_secs(args.window_size),
            base_timestamp: Instant::from_millis(args.base_timestamp),
        }
    }
}

impl PTransform<PCollection<String>> for WindowedWordCount {
    type Output = PCollection<String>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        let &Self {
            window_size,
            base_timestamp,
        } = self;
        lines
            .par_do_fn("AssignTimestamps", move |line: String, ctx| {
                let timestamp = event_time(&line, window_size, base_timestamp);
                ctx.output(line).at(timestamp).emit()
            })
            .window_into("FixedWindows", FixedWindows::of(window_size))
            .apply("CountWords", CountWords)
            .par_do_fn(
                "FormatCounts",
                |Windowed {
                     value: (word, count),
                     window,
                 }: Windowed<(String, i64)>,
                 ctx| ctx.emit(format!("{window} {word}: {count}")),
            )
    }
}

/// A time for `line` in the three windows after `base`. Equal lines get the same time.
fn event_time(line: &str, window_size: Duration, base: Instant) -> Instant {
    let hash = line.bytes().fold(0u64, |acc, b| {
        acc.wrapping_mul(31).wrapping_add(u64::from(b))
    });
    let span = window_size.as_millis() as u64 * 3;
    base + Duration::from_millis(hash.checked_rem(span).unwrap_or(0))
}
