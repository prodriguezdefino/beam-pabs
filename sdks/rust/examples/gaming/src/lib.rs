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

//! Mobile Gaming example: team totals and stateful team milestones.
//!
//! Reads CSV game events (`user,team,score,timestamp_ms[,readable_time]`) and writes two
//! typed results:
//! - `<output>-totals.txt`: the total score of each team, from `.sum_per_key`.
//! - `<output>-milestones.txt`: a [`TeamMilestoneReport`] each time the running total of a
//!   team passes a multiple of `--threshold`. [`TeamMilestoneFn`] keeps the total in a
//!   `CombiningState<Sum<i64>>` and the contributors in a `SetState<String>`.

use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// Command-line arguments for the Mobile Gaming pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "gaming",
    about = "Apache Beam Rust Mobile Gaming example",
    version
)]
pub struct GamingArgs {
    /// Input CSV file or glob pattern (`user,team,score,timestamp_ms,readable_time`).
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/game/small/gaming_data.csv"
    )]
    pub input: String,

    /// Prefix of the output files.
    #[arg(long, default_value = "/tmp/gaming")]
    pub output: String,

    /// A team earns a milestone each time its total passes a multiple of this score.
    #[arg(long, default_value = "500")]
    pub threshold: NonZeroU32,
}

impl PipelineOptionGroup for GamingArgs {}

/// One game event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GameActionInfo {
    pub user: String,
    pub team: String,
    pub score: i64,
    pub timestamp: Instant,
}

/// A line that is not `user,team,score,timestamp_ms[,readable_time]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedEvent;

impl fmt::Display for MalformedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("expected user,team,score,timestamp_ms")
    }
}

impl std::error::Error for MalformedEvent {}

impl FromStr for GameActionInfo {
    type Err = MalformedEvent;

    fn from_str(line: &str) -> Result<Self, MalformedEvent> {
        let mut fields = line.split(',').map(str::trim);
        match (fields.next(), fields.next(), fields.next(), fields.next()) {
            (Some(user), Some(team), Some(score), Some(millis))
                if !user.is_empty() && !team.is_empty() =>
            {
                Ok(Self {
                    user: user.to_string(),
                    team: team.to_string(),
                    score: score.parse().map_err(|_| MalformedEvent)?,
                    timestamp: Instant::from_millis(millis.parse().map_err(|_| MalformedEvent)?),
                })
            }
            _ => Err(MalformedEvent),
        }
    }
}

/// Parses CSV lines into `(team, (user, score))` at the event time of each line.
///
/// Drops malformed lines and counts them in the `gaming/malformed_events` counter.
pub struct ParseGameEvents;

impl PTransform<PCollection<String>> for ParseGameEvents {
    type Output = PCollection<(String, (String, i64))>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        let malformed = Metrics::counter("gaming", "malformed_events");
        let scores = Metrics::distribution("gaming", "event_score");
        lines.par_do_fn("Parse", move |line: String, ctx| {
            match line.parse::<GameActionInfo>() {
                Ok(GameActionInfo {
                    user,
                    team,
                    score,
                    timestamp,
                }) => {
                    scores.update(score);
                    ctx.output((team, (user, score))).at(timestamp).emit()
                }
                Err(MalformedEvent) => {
                    malformed.inc();
                    Ok(())
                }
            }
        })
    }
}

/// A milestone of one team: its total score, and the users who scored since the previous
/// milestone, sorted.
#[derive(Debug, Clone, PartialEq, Eq, BeamRow)]
#[beam(id = "beam.examples.gaming.TeamMilestoneReport")]
pub struct TeamMilestoneReport {
    pub team: String,
    pub score: i64,
    pub contributors: Vec<String>,
}

impl fmt::Display for TeamMilestoneReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            team,
            score,
            contributors,
        } = self;
        write!(f, "{team},{score},{}", contributors.join(";"))
    }
}

/// The state of one team.
#[derive(State)]
pub struct TeamState {
    total: CombiningState<Sum<i64>>,
    contributors: SetState<String>,
}

/// Emits a [`TeamMilestoneReport`] each time the total of a team passes a multiple of
/// `threshold`.
///
/// `#[derive(Register)]` puts the function and its `threshold` into the pipeline graph, so a
/// worker builds it without running `main` again.
#[derive(Clone, Serialize, Deserialize, Register)]
pub struct TeamMilestoneFn {
    pub threshold: NonZeroU32,
}

impl StatefulDoFn for TeamMilestoneFn {
    type Key = String;
    type Value = (String, i64);
    type Out = TeamMilestoneReport;
    type State = TeamState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        team: &String,
        (user, score): (String, i64),
        state: &mut TeamState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let threshold = i64::from(self.threshold.get());
        let previous = state.total.read()?;
        let total = previous + score;
        state.total.add(score)?;
        state.contributors.insert(user)?;
        if total / threshold == previous / threshold {
            return Ok(());
        }
        let mut contributors = state.contributors.read()?;
        contributors.sort_unstable();
        state.contributors.clear()?;
        ctx.emit(TeamMilestoneReport {
            team: team.clone(),
            score: total,
            contributors,
        })
    }
}

/// Builds the Mobile Gaming pipeline from [`GamingArgs`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &GamingArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let events = p
        .apply("ReadLines", textio::Read::new(&args.input))
        .apply("ParseGameEvents", ParseGameEvents);

    events
        .map(
            "TeamScore",
            |(team, (_user, score)): (String, (String, i64))| (team, score),
        )
        .sum_per_key("TotalPerTeam")
        .map("FormatTotals", |(team, total): (String, i64)| {
            format!("{team},{total}")
        })
        .apply(
            "WriteTotals",
            textio::Write::new(format!("{}-totals", args.output)).with_suffix(".txt"),
        );

    events
        .apply(
            "TeamMilestones",
            ParDo::new(TeamMilestoneFn {
                threshold: args.threshold,
            }),
        )
        .map("FormatMilestones", |report: TeamMilestoneReport| {
            report.to_string()
        })
        .apply(
            "WriteMilestones",
            textio::Write::new(format!("{}-milestones", args.output)).with_suffix(".txt"),
        );
    p
}
