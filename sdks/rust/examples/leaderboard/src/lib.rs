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

//! Mobile Gaming LeaderBoard example for windowing, triggers and late data.
//!
//! Demonstrates:
//! - Speculative early firings and late-data firings using composite [`Trigger`]
//!   configurations.
//! - Accumulating pane results through [`AccumulationMode::Accumulating`].
//! - Bounding late data through `WindowInto::with_allowed_lateness`.
//! - Global windowing with a repeated element-count trigger for user standings.
//!
//! The events are parsed by [`gaming::ParseGameEvents`], at their event time.

use std::time::Duration;

use beam::prelude::*;
use clap::Args;
use gaming::ParseGameEvents;
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

    /// Prefix of the output files.
    #[arg(long, default_value = "/tmp/leaderboard")]
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

/// Sums the scores of each team in fixed windows, from `(team, (user, score))` events.
///
/// A window fires early after `early_count` events, on time at its end, and late once for
/// each event within `allowed_lateness`. Panes accumulate, so each pane has the total so far.
pub struct CalculateTeamScores {
    pub window: Duration,
    pub allowed_lateness: Duration,
    pub early_count: i32,
}

impl PTransform<PCollection<(String, (String, i64))>> for CalculateTeamScores {
    type Output = PCollection<(String, i64)>;

    fn expand(&self, events: &PCollection<(String, (String, i64))>) -> Self::Output {
        let trigger = Trigger::after_end_of_window()
            .with_early_firings(Trigger::after_count(self.early_count))
            .with_late_firings(Trigger::repeatedly(Trigger::after_count(1)));
        events
            .map(
                "TeamScore",
                |(team, (_user, score)): (String, (String, i64))| (team, score),
            )
            .apply(
                "TeamFixedWindows",
                WindowInto::new(FixedWindows::of(self.window))
                    .with_trigger(trigger)
                    .with_accumulation_mode(AccumulationMode::Accumulating)
                    .with_allowed_lateness(self.allowed_lateness),
            )
            .sum_per_key("SumTeamScores")
    }
}

/// Sums the scores of each user in the global window, from `(team, (user, score))` events.
///
/// It fires after every `trigger_count` events of a user. Panes accumulate, so each pane has
/// the running total of the user.
pub struct CalculateUserScores {
    pub trigger_count: i32,
}

impl PTransform<PCollection<(String, (String, i64))>> for CalculateUserScores {
    type Output = PCollection<(String, i64)>;

    fn expand(&self, events: &PCollection<(String, (String, i64))>) -> Self::Output {
        events
            .map(
                "UserScore",
                |(_team, (user, score)): (String, (String, i64))| (user, score),
            )
            .apply(
                "UserGlobalWindows",
                WindowInto::new(GlobalWindows)
                    .with_trigger(Trigger::repeatedly(Trigger::after_count(
                        self.trigger_count,
                    )))
                    .with_accumulation_mode(AccumulationMode::Accumulating),
            )
            .sum_per_key("SumUserScores")
    }
}

/// Builds the LeaderBoard pipeline from [`LeaderBoardArgs`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &LeaderBoardArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let events = p
        .apply("ReadLines", textio::Read::new(&args.input))
        .apply("ParseGameEvents", ParseGameEvents);

    events
        .apply(
            "CalculateTeamScores",
            CalculateTeamScores {
                window: Duration::from_secs(args.team_window_duration),
                allowed_lateness: Duration::from_secs(args.allowed_lateness),
                early_count: args.early_count,
            },
        )
        .par_do_fn(
            "FormatTeamScores",
            |Windowed {
                 value: (team, score),
                 window,
             }: Windowed<(String, i64)>,
             ctx| ctx.emit(format!("{team},{score},{window}")),
        )
        .apply(
            "WriteTeamScores",
            textio::Write::new(format!("{}-team", args.output)).with_suffix(".txt"),
        );

    events
        .apply(
            "CalculateUserScores",
            CalculateUserScores {
                trigger_count: args.user_trigger_count,
            },
        )
        .map("FormatUserScores", |(user, score): (String, i64)| {
            format!("{user},{score}")
        })
        .apply(
            "WriteUserScores",
            textio::Write::new(format!("{}-user", args.output)).with_suffix(".txt"),
        );
    p
}
