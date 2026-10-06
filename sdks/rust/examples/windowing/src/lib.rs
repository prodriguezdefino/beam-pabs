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

//! Windowing: the same game events in sliding windows and in sessions.
//!
//! - [`SlidingWindows`] overlap, so each event is in several windows. Each window gets the
//!   count, mean, min and max score of each team, from one [`Compose`] combine function.
//! - [`Sessions`] merge the events of a user that are less than a gap apart. Each session
//!   gets the number of events of the user.
//!
//! Both take [`Windowed`] input to print the window of each result.

use std::str::FromStr;
use std::time::Duration;

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(name = "windowing", about = "Apache Beam windowing example", version)]
pub struct Args {
    /// Path of the CSV file to read from: `user,team,score,timestamp_ms[,...]`.
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/game/small/gaming_data.csv"
    )]
    pub input: String,

    /// Prefix of the output files: `<output>-teams.txt` and `<output>-sessions.txt`.
    #[arg(long, default_value = "/tmp/windowing")]
    pub output: String,

    /// Length of a sliding window, in minutes.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    pub window_minutes: u64,

    /// Time between the starts of two sliding windows, in minutes.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    pub period_minutes: u64,

    /// Largest gap between two events of one session, in minutes.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    pub gap_minutes: u64,
}

impl PipelineOptionGroup for Args {}

/// One line of the game log.
#[derive(Debug, PartialEq, BeamRow)]
pub struct GameEvent {
    pub user: String,
    pub team: String,
    pub score: i64,
    pub timestamp_ms: i64,
}

impl FromStr for GameEvent {
    type Err = String;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let fields: Vec<&str> = line.split(',').map(str::trim).collect();
        let [user, team, score, timestamp_ms, ..] = fields[..] else {
            return Err(format!("expected user,team,score,timestamp_ms: {line:?}"));
        };
        let number = |field: &str| field.parse::<i64>().map_err(|e| format!("{e}: {line:?}"));
        Ok(Self {
            user: user.to_string(),
            team: team.to_string(),
            score: number(score)?,
            timestamp_ms: number(timestamp_ms)?,
        })
    }
}

/// Parses game log lines into timestamped [`GameEvent`]s.
///
/// Counts the lines that do not parse in the metric `windowing/bad_lines` and skips them.
pub struct ParseEvents;

impl PTransform<PCollection<String>> for ParseEvents {
    type Output = PCollection<GameEvent>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        let bad_lines = Metrics::counter("windowing", "bad_lines");
        lines.par_do_fn("Parse", move |line: String, ctx| {
            match line.parse::<GameEvent>() {
                Ok(event) => {
                    let timestamp = Instant::from_millis(event.timestamp_ms);
                    ctx.output(event).at(timestamp).emit()
                }
                Err(_) => {
                    bad_lines.inc();
                    Ok(())
                }
            }
        })
    }
}

/// Count, mean, min and max score of each team, per sliding window, as text.
pub struct TeamStats {
    pub size: Duration,
    pub period: Duration,
}

impl PTransform<PCollection<GameEvent>> for TeamStats {
    type Output = PCollection<String>;

    fn expand(&self, events: &PCollection<GameEvent>) -> Self::Output {
        events
            .map("TeamScore", |e: GameEvent| (e.team, e.score))
            .window_into(
                "SlidingWindows",
                SlidingWindows::of(self.size).every(self.period),
            )
            .combine_per_key("ScoreStats", score_stats())
            .par_do_fn(
                "Format",
                |Windowed {
                     value: (team, (count, mean, min, max)),
                     window,
                 }: Windowed<(String, (i64, f64, i64, i64))>,
                 ctx| {
                    ctx.emit(format!(
                        "{window} team {team}: count={count} mean={mean:.1} min={min} max={max}"
                    ))
                },
            )
    }
}

/// Number of events of each user, per session, as text.
pub struct UserSessions {
    pub gap: Duration,
}

impl PTransform<PCollection<GameEvent>> for UserSessions {
    type Output = PCollection<String>;

    fn expand(&self, events: &PCollection<GameEvent>) -> Self::Output {
        events
            .map("UserEvent", |e: GameEvent| (e.user, e.score))
            .window_into("Sessions", Sessions::with_gap_duration(self.gap))
            .combine_per_key("CountEvents", Sum::new().on(|_: &i64| 1_i64))
            .par_do_fn(
                "Format",
                |Windowed {
                     value: (user, count),
                     window,
                 }: Windowed<(String, i64)>,
                 ctx| ctx.emit(format!("{window} user {user}: {count} events")),
            )
    }
}

/// Reads the game log, and writes the team statistics to `<output>-teams.txt` and the
/// sessions to `<output>-sessions.txt`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let minutes = |m: u64| Duration::from_secs(m * 60);
    let p = Pipeline::create(options);
    let events = p
        .apply("ReadLines", textio::Read::new(&args.input))
        .apply("ParseEvents", ParseEvents);
    events
        .apply(
            "TeamStats",
            TeamStats {
                size: minutes(args.window_minutes),
                period: minutes(args.period_minutes),
            },
        )
        .apply(
            "WriteTeams",
            textio::Write::new(format!("{}-teams.txt", args.output)),
        );
    events
        .apply(
            "UserSessions",
            UserSessions {
                gap: minutes(args.gap_minutes),
            },
        )
        .apply(
            "WriteSessions",
            textio::Write::new(format!("{}-sessions.txt", args.output)),
        );
    p
}

/// Count, mean, min and max of the scores, in one pass.
fn score_stats() -> impl CombineFn<Input = i64, Output = (i64, f64, i64, i64)> {
    Compose::new((
        Sum::new().on(|_: &i64| 1_i64),
        Mean::new().on(|score: &i64| *score),
        Min::new().on(|score: &i64| *score),
        Max::new().on(|score: &i64| *score),
    ))
}
