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

//! User Session Analytics example demonstrating dynamic activity sessionization and window merging.
//!
//! Demonstrates:
//! - Grouping event-time user activities into dynamic [`Sessions`] separated by an inactivity gap.
//! - Automatic merging of overlapping and contiguous activity windows per user key.
//! - Extracting session metrics (duration, action count, window boundaries) through
//!   [`ProcessContext::interval_window`].

use std::time::Duration;

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// Command-line arguments for the User Session Analytics pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "sessions",
    about = "Apache Beam Rust User Session Analytics Example",
    version
)]
pub struct SessionsArgs {
    /// Input file path or glob pattern with CSV records (`user,action_or_team,value,timestamp_ms`).
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/game/small/gaming_data.csv"
    )]
    pub input: String,

    /// Output file path for formatted session summaries.
    #[arg(long, default_value = "/tmp/sessions_output.txt")]
    pub output: String,

    /// Maximum inactivity gap duration in seconds before a session closes.
    #[arg(long, default_value_t = 300)]
    pub gap_duration: u64,
}

impl PipelineOptionGroup for SessionsArgs {}

/// Parsed user activity record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserActivityRecord {
    pub user: String,
    pub action: String,
    pub timestamp_ms: i64,
}

/// Parses a CSV line (`user,action,timestamp_ms` or `user,team,score,timestamp_ms`) into a [`UserActivityRecord`].
pub fn parse_activity_line(line: &str) -> Option<UserActivityRecord> {
    let fields: Vec<&str> = line.trim().split(',').map(str::trim).collect();
    let ts_idx = match fields.len() {
        4.. => 3,
        3 => 2,
        _ => return None,
    };
    Some(UserActivityRecord {
        user: fields[0].to_string(),
        action: fields[1].to_string(),
        timestamp_ms: fields[ts_idx].parse().ok()?,
    })
}

/// Builds the User Session Analytics pipeline graph.
pub fn build_sessions_graph(
    lines: &PCollection<String>,
    gap_duration_secs: u64,
) -> PCollection<String> {
    lines
        .par_do_fn(
            "AssignActivityTimestamps",
            |line: String, ctx| match parse_activity_line(&line) {
                Some(r) => ctx.output((r.user, r.action)).at(r.timestamp_ms).emit(),
                None => Ok(()),
            },
        )
        .window_into(
            "SessionWindows",
            Sessions::with_gap_duration(Duration::from_secs(gap_duration_secs)),
        )
        .group_by_key("GroupUserSessions")
        .par_do_fn(
            "FormatSessionSummaries",
            |(user, actions): (String, BeamIterable<String>), ctx| {
                let action_count = actions.into_vec()?.len();
                let line = match ctx.interval_window() {
                    Some(w) => format!(
                        "SESSION|{user}|[{}..{})|duration_ms={}|actions={action_count}",
                        w.start_millis,
                        w.end_millis,
                        w.span_millis()
                    ),
                    None => format!("SESSION|{user}|(global)|actions={action_count}"),
                };
                ctx.emit(line)
            },
        )
}

/// Constructs the complete User Session Analytics pipeline reading from input and writing to output.
pub fn build_pipeline(options: &PipelineOptions, args: &SessionsArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let lines = p.apply(textio::Read::new("ReadLines", &args.input));
    let sessions = build_sessions_graph(&lines, args.gap_duration);
    sessions.apply(textio::Write::new("WriteLines", &args.output));
    p
}
