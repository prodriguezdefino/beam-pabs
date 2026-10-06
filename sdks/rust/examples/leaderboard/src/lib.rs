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

//! Mobile Gaming LeaderBoard example demonstrating advanced windowing, triggers, and late data handling.
//!
//! Demonstrates:
//! - Speculative early firings and late-data firings using composite [`Trigger`] configurations.
//! - Accumulating pane results through [`AccumulationMode::Accumulating`].
//! - Bounding late data through `WindowInto::with_allowed_lateness`.
//! - Global windowing with repeated processing-based triggers for real-time user standings.

use std::time::Duration;

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// Command-line arguments for the Mobile Gaming LeaderBoard pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "leaderboard",
    about = "Apache Beam Rust Mobile Gaming LeaderBoard Example",
    version
)]
pub struct LeaderBoardArgs {
    /// Input CSV file or glob pattern (`user,team,score,timestamp_ms,readable_time`).
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/game/small/gaming_data.csv"
    )]
    pub input: String,

    /// Output file path for formatted team and user leaderboards.
    #[arg(long, default_value = "/tmp/leaderboard_output.txt")]
    pub output: String,

    /// Fixed window duration in seconds for team scores.
    #[arg(long, default_value_t = 60)]
    pub team_window_duration: u64,

    /// Allowed lateness in seconds for late-arriving gaming events.
    #[arg(long, default_value_t = 120)]
    pub allowed_lateness: u64,

    /// Element count threshold for speculative early team pane firings.
    #[arg(long, default_value_t = 5)]
    pub early_count: i32,

    /// Element count threshold for periodic user global window updates.
    #[arg(long, default_value_t = 3)]
    pub user_trigger_count: i32,
}

impl PipelineOptionGroup for LeaderBoardArgs {}

/// Parsed mobile game action event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameActionInfo {
    pub user: String,
    pub team: String,
    pub score: i64,
    pub timestamp_ms: i64,
}

/// Parses a CSV line (`user,team,score,timestamp_ms[,readable_time]`) into a [`GameActionInfo`].
pub fn parse_game_action_line(line: &str) -> Option<GameActionInfo> {
    let mut fields = line.trim().split(',').map(str::trim);
    match (fields.next(), fields.next(), fields.next(), fields.next()) {
        (Some(user), Some(team), Some(score_str), Some(ts_str))
            if !user.is_empty() && !team.is_empty() =>
        {
            let score = score_str.parse::<i64>().ok()?;
            let timestamp_ms = ts_str.parse::<i64>().ok()?;
            Some(GameActionInfo {
                user: user.to_string(),
                team: team.to_string(),
                score,
                timestamp_ms,
            })
        }
        _ => None,
    }
}

/// Builds the LeaderBoard pipeline graph producing both team and user score streams.
pub fn build_leaderboard_graph(
    lines: &PCollection<String>,
    team_window_duration_secs: u64,
    allowed_lateness_secs: u64,
    early_count: i32,
    user_trigger_count: i32,
) -> (PCollection<String>, PCollection<String>) {
    // (team, (user, score)) stamped with the event time.
    let parsed_events =
        lines.par_do_fn(
            "ParseAndTimestamp",
            |line: String, ctx| match parse_game_action_line(&line) {
                Some(info) => ctx
                    .output((info.team, (info.user, info.score)))
                    .at(info.timestamp_ms)
                    .emit(),
                None => Ok(()),
            },
        );

    // Team scores with fixed windows, early and late firings.
    let team_trigger = Trigger::after_end_of_window()
        .with_early_firings(Trigger::after_count(early_count))
        .with_late_firings(Trigger::repeatedly(Trigger::after_count(1)));

    let team_strategy = WindowInto::new(
        "TeamFixedWindows",
        FixedWindows::of(Duration::from_secs(team_window_duration_secs)),
    )
    .triggering(team_trigger)
    .accumulating_fired_panes()
    .with_allowed_lateness(Duration::from_secs(allowed_lateness_secs));

    let team_scores = parsed_events
        .map(
            "ExtractTeamScore",
            |(team, (_user, score)): (String, (String, i64))| (team, score),
        )
        .apply(team_strategy)
        .group_by_key("GroupTeamScores")
        .map(
            "SumTeamScores",
            |(team, scores): (String, BeamIterable<i64>)| (team, scores.into_iter().sum::<i64>()),
        )
        .par_do_fn("FormatTeamScores", |(team, score): (String, i64), ctx| {
            let line = match ctx.interval_window() {
                Some(w) => format!("TEAM|{team}|{score}|[{}..{})", w.start_millis, w.end_millis),
                None => format!("TEAM|{team}|{score}|(global)"),
            };
            ctx.emit(line)
        });

    // Real-time user standings with global windows and repeated firings.
    let user_trigger = Trigger::repeatedly(Trigger::after_count(user_trigger_count));

    let user_strategy = WindowInto::new("UserGlobalWindows", GlobalWindows)
        .triggering(user_trigger)
        .accumulating_fired_panes();

    let user_scores = parsed_events
        .map(
            "ExtractUserScore",
            |(_team, (user, score)): (String, (String, i64))| (user, score),
        )
        .apply(user_strategy)
        .group_by_key("GroupUserScores")
        .map(
            "SumUserScores",
            |(user, scores): (String, BeamIterable<i64>)| (user, scores.into_iter().sum::<i64>()),
        )
        .map("FormatUserScores", |(user, score): (String, i64)| {
            format!("USER|{user}|{score}")
        });

    (team_scores, user_scores)
}

/// Constructs the complete LeaderBoard pipeline reading from input and writing to output.
pub fn build_pipeline(options: &PipelineOptions, args: &LeaderBoardArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let lines = p.apply(textio::Read::new("ReadLines", &args.input));
    let (team_scores, user_scores) = build_leaderboard_graph(
        &lines,
        args.team_window_duration,
        args.allowed_lateness,
        args.early_count,
        args.user_trigger_count,
    );
    let output_prefix = args.output.strip_suffix(".txt").unwrap_or(&args.output);
    team_scores.apply(textio::Write::new(
        "WriteLines",
        format!("{output_prefix}_team.txt"),
    ));
    user_scores.apply(textio::Write::new(
        "WriteLines",
        format!("{output_prefix}_user.txt"),
    ));
    p
}
