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

//! Mobile Gaming example (`StatefulTeamScore` + `UserScore`).
//!
//! Reads CSV gaming events (`user,team,score,timestamp_ms,readable_time`) from
//! `gs://apache-beam-samples/game/small/gaming_data.csv` and demonstrates:
//! - User metrics (`Counter` and `Distribution`) recorded from a `flat_map` closure.
//! - Stateful [`StatefulDoFn`] using [`ValueState`] (`total_score`) and [`BagState`]
//!   (`recent_scorers`) to emit team milestone achievements when a team's running score crosses
//!   a multiple of `--threshold`.
//! - Per-team total aggregation through `CombinePerKey` (`Sum`) merged with `Flatten`.

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// Command-line arguments for the Mobile Gaming pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "gaming",
    about = "Apache Beam Rust Mobile Gaming (StatefulTeamScore & UserScore) Example",
    version
)]
pub struct GamingArgs {
    /// Input CSV file or glob pattern (`user,team,score,timestamp_ms,readable_time`).
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/game/small/gaming_data.csv"
    )]
    pub input: String,

    /// Output file path for formatted team milestones and final scores.
    #[arg(long, default_value = "/tmp/gaming_output.txt")]
    pub output: String,

    /// Score threshold multiple at which a team earns a milestone achievement.
    #[arg(long, default_value_t = 500)]
    pub threshold: i64,
}

impl PipelineOptionGroup for GamingArgs {}

/// Parsed mobile game event record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameActionInfo {
    pub user: String,
    pub team: String,
    pub score: i64,
    pub timestamp_ms: i64,
}

/// Parses a single CSV line (`user,team,score,timestamp_ms[,readable_time]`) into a [`GameActionInfo`].
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

/// Structured audit record for team milestones, serializable as a Beam Row (`beam:coder:row:v1`).
///
/// `#[derive(BeamRow)]` derives the schema from the field *types*, so every
/// instance encodes against the same schema regardless of its contents, and
/// supplies `to_row_bytes`/`from_row_bytes` for the portable wire format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BeamRow)]
#[beam(id = "beam.examples.gaming.TeamMilestoneReport")]
pub struct TeamMilestoneReport {
    pub team: String,
    pub score: i64,
    pub contributors: Vec<String>,
}

/// Stateful [`DoFn`] updating cumulative team scores and tracking milestones.
#[derive(State)]
pub struct TeamScoreState {
    pub total_score: ValueState<i64>,
    pub recent_scorers: BagState<String>,
    pub user_scores: MapState<String, i64>,
    pub team_badges: SetState<String>,
}

#[derive(Clone)]
pub struct UpdateTeamScoreDoFn {
    pub threshold: i64,
}

impl UpdateTeamScoreDoFn {
    pub fn new(threshold: i64) -> Self {
        Self {
            threshold: threshold.max(1),
        }
    }
}

impl StatefulDoFn for UpdateTeamScoreDoFn {
    type Key = String;
    type Value = (String, i64);
    type Out = String;
    type State = TeamScoreState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        team: &Self::Key,
        (user, score): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let old_score = state.total_score.read()?.unwrap_or(0);
        let new_score = old_score + score;

        state.total_score.write(new_score)?;
        state.recent_scorers.append(user.clone())?;

        // Track per-player cumulative scores in MapState.
        let prev_user_score = state.user_scores.get(&user)?.unwrap_or(0);
        state.user_scores.put(user, prev_user_score + score)?;

        // Track distinct badges in SetState. The insert is idempotent.
        state.team_badges.insert("first_points".to_string())?;

        if new_score / self.threshold > old_score / self.threshold {
            let mut contributors = state.recent_scorers.read()?;
            contributors.sort();
            contributors.dedup();
            state.recent_scorers.clear()?;

            state.team_badges.insert("milestone_achiever".to_string())?;

            let report = TeamMilestoneReport {
                team: team.clone(),
                score: new_score,
                contributors,
            };

            ctx.emit(format!(
                "MILESTONE|{}|{}|{}",
                report.team,
                report.score,
                report.contributors.join(",")
            ))?;
        }

        Ok(())
    }
}

/// Builds the complete Mobile Gaming pipeline from a `PCollection<String>` of CSV lines.
///
/// Returns the unified `PCollection<String>` containing both `MILESTONE|...` achievements
/// (from the stateful `DoFn`) and `FINAL_SCORE|...` summaries (from `CombinePerKey`).
pub fn build_gaming_graph(lines: &PCollection<String>, threshold: i64) -> PCollection<String> {
    let parsed_events = Metrics::counter("gaming", "parsed_events");
    let malformed_events = Metrics::counter("gaming", "malformed_events");
    let score_distribution = Metrics::distribution("gaming", "user_score_dist");
    let keyed_events = lines.flat_map("ParseGameActionEvents", move |line: String| {
        let event = parse_game_action_line(&line);
        match &event {
            Some(e) => {
                parsed_events.inc();
                score_distribution.update(e.score);
            }
            None => malformed_events.inc(),
        }
        event.map(|e| (e.team, (e.user, e.score)))
    });

    // Stateful team milestone detection (ValueState, BagState, MapState and SetState).
    let stateful_fn = UpdateTeamScoreDoFn::new(threshold);

    let milestones = keyed_events.apply(ParDo::new("UpdateTeamScoreStateful", stateful_fn));

    // Total team score aggregation through CombinePerKey(Sum).
    let final_scores = keyed_events
        .map(
            "ExtractTeamScore",
            |(team, (_user, score)): (String, (String, i64))| (team, score),
        )
        .combine_per_key("SumTeamScores", Sum)
        .map("FormatFinalScore", |(team, total): (String, i64)| {
            format!("FINAL_SCORE|{team}|{total}")
        });

    milestones.flatten("MergeMilestonesAndFinalScores", &final_scores)
}

/// Builds the Mobile Gaming pipeline reading from `args.input` and writing to `args.output`.
pub fn build_pipeline(options: &PipelineOptions, args: &GamingArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let lines = p.apply(textio::Read::new("ReadLines", &args.input));
    let results = build_gaming_graph(&lines, args.threshold);
    results.apply(textio::Write::new("WriteLines", &args.output));
    p
}
