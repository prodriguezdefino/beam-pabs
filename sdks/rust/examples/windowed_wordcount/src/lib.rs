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

//! Windowed WordCount example demonstrating fixed tumbling event-time windows in Apache Beam.
//!
//! Demonstrates:
//! - Assigning event timestamps to input records through [`ProcessContext::output`] with
//!   `.at(timestamp)`.
//! - Tumbling time windowing through [`FixedWindows`].
//! - Re-using standard PTransforms ([`PCollectionExt::count_per_element`]) over windowed streams.
//! - Inspecting active window boundaries ([`ProcessContext::interval_window`]) to format
//!   per-window counts.

use std::time::Duration;

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// Command-line arguments for the Windowed WordCount pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "windowed_wordcount",
    about = "Apache Beam Rust Windowed WordCount Example",
    version
)]
pub struct WindowedWordCountArgs {
    /// Input file path or glob pattern.
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/shakespeare/kinglear.txt"
    )]
    pub input: String,

    /// Output file path for window-formatted word counts.
    #[arg(long, default_value = "/tmp/windowed_wordcount_output.txt")]
    pub output: String,

    /// Tumbling fixed window duration in seconds.
    #[arg(long, default_value_t = 60)]
    pub window_size: u64,

    /// Base timestamp in milliseconds since Unix epoch for synthetic event-time assignment.
    #[arg(long, default_value_t = 1_000_000)]
    pub base_timestamp: i64,
}

impl PipelineOptionGroup for WindowedWordCountArgs {}

/// Deterministic event timestamp for `line`, spread over three windows after `base_timestamp`.
pub fn synthetic_timestamp(line: &str, window_size_secs: u64, base_timestamp: i64) -> i64 {
    let max_offset = (window_size_secs as i64 * 1000).saturating_mul(3);
    let hash_val = line
        .bytes()
        .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
    let offset = if max_offset > 0 {
        (hash_val % (max_offset as u64)) as i64
    } else {
        0
    };
    base_timestamp + offset
}

/// Builds the Windowed WordCount execution graph from an input collection of lines.
pub fn build_windowed_wordcount_graph(
    lines: &PCollection<String>,
    window_size_secs: u64,
    base_timestamp: i64,
) -> PCollection<String> {
    lines
        .par_do_fn("AssignTimestamps", move |line: String, ctx| {
            let timestamp = synthetic_timestamp(&line, window_size_secs, base_timestamp);
            ctx.output(line).at(timestamp).emit()
        })
        .flat_map("ExtractWords", |line: String| {
            line.split(|c: char| !c.is_alphanumeric())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_lowercase())
                .collect::<Vec<_>>()
        })
        .window_into(
            "FixedWindows",
            FixedWindows::of(Duration::from_secs(window_size_secs)),
        )
        .count_per_element("CountWordsPerWindow")
        .par_do_fn(
            "FormatWindowedCounts",
            |(word, count): (String, i64), ctx| {
                let line = match ctx.interval_window() {
                    Some(w) => format!("[{}..{}) {word}: {count}", w.start_millis, w.end_millis),
                    None => format!("(ts={}) {word}: {count}", ctx.timestamp()),
                };
                ctx.emit(line)
            },
        )
}

/// Constructs the complete Windowed WordCount pipeline reading from and writing to files.
pub fn build_pipeline(options: &PipelineOptions, args: &WindowedWordCountArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let lines = p.apply(textio::Read::new("ReadLines", &args.input));
    let formatted = build_windowed_wordcount_graph(&lines, args.window_size, args.base_timestamp);
    formatted.apply(textio::Write::new("WriteLines", &args.output));
    p
}
