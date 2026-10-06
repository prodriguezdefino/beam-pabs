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

//! Stateful DoFns for the state and timer pipelines.

use beam::prelude::*;

/// The state of one team. The field names are the state ids.
#[derive(State)]
pub struct TeamState {
    total_score: ValueState<i64>,
    scorers: BagState<String>,
}

#[derive(Timers)]
pub enum TeamTimer {
    #[timer(event_time, id = "flush_timer")]
    Flush,
}

#[derive(Clone)]
pub struct StatefulTeamMilestoneDoFn;

impl StatefulDoFn for StatefulTeamMilestoneDoFn {
    type Key = String;
    type Value = (String, i64);
    type Out = String;
    type State = TeamState;
    type Timer = TeamTimer;

    fn process_element(
        &mut self,
        team: &String,
        (user, score): (String, i64),
        state: &mut TeamState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let prev = state.total_score.read()?.unwrap_or(0);
        let next = prev + score;
        state.total_score.write(next)?;
        state.scorers.append(user)?;
        ctx.timer(TeamTimer::Flush)
            .at(Instant::from_millis(1_000))?;

        if next >= 100 && prev < 100 {
            let mut who = state.scorers.read()?;
            who.sort();
            ctx.emit(format!("milestone:{team}:{next}:{}", who.join("+")))?;
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer: TeamTimer,
        team: &String,
        state: &mut TeamState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            TeamTimer::Flush => {
                if let Some(final_total) = state.total_score.read()? {
                    ctx.emit(format!("timer_flush:{team}:{final_total}"))?;
                    state.total_score.clear()?;
                }
                Ok(())
            }
        }
    }
}

#[derive(State)]
pub struct PlayerInventoryState {
    pub player_points: MapState<String, i64>,
    pub unlocked_achievements: SetState<String>,
}

#[derive(Timers)]
pub enum PlayerInventoryTimer {
    #[timer(event_time, id = "flush_inventory")]
    Flush,
}

#[derive(Clone)]
pub struct PlayerInventoryDoFn;

impl StatefulDoFn for PlayerInventoryDoFn {
    type Key = String;
    type Value = (String, (i64, String));
    type Out = String;
    type State = PlayerInventoryState;
    type Timer = PlayerInventoryTimer;

    fn process_element(
        &mut self,
        team: &Self::Key,
        (player, (points, badge)): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(PlayerInventoryTimer::Flush)
            .at(Instant::from_millis(1_000))?;

        if badge == "CLEAR" {
            state.player_points.clear()?;
            state.unlocked_achievements.clear()?;
            return ctx.emit(format!("cleared:{team}"));
        }

        let prev_score = state.player_points.get(&player)?.unwrap_or(0);
        let next_score = prev_score + points;
        state.player_points.put(player.clone(), next_score)?;

        let already_had = state.unlocked_achievements.contains(&badge)?;
        if !already_had {
            state.unlocked_achievements.insert(badge)?;
        }

        let mut keys = state.player_points.keys()?;
        keys.sort();
        let mut badges = state.unlocked_achievements.read()?;
        badges.sort();

        ctx.emit(format!(
            "entry:{team}:{player}:{next_score}:{already_had}|keys:{}:badges:{}",
            keys.join("+"),
            badges.join("+")
        ))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        team: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            PlayerInventoryTimer::Flush => {
                let mut keys = state.player_points.keys()?;
                keys.sort();
                let mut badges = state.unlocked_achievements.read()?;
                badges.sort();

                ctx.emit(format!(
                    "final:{team}|keys:{}:badges:{}",
                    keys.join("+"),
                    badges.join("+")
                ))
            }
        }
    }
}

#[derive(State)]
pub struct MutateAcrossBundlesState {
    pub scores: MapState<String, i64>,
    pub players: SetState<String>,
}

#[derive(Debug, Timers)]
pub enum MutateAcrossBundlesTimer {
    #[timer(event_time, id = "mutate")]
    Mutate,
    #[timer(event_time, id = "verify")]
    Verify,
}

/// What the `mutate` timer of [`MutateAcrossBundlesDoFn`] does to the committed state.
#[derive(Clone, Copy, Debug)]
pub enum StateMutation {
    /// Clears the map and the set.
    Clear,
    /// Removes these keys from the map and the set. A key can be absent.
    Remove(&'static [&'static str]),
}

/// Every player that the `verify` timer reads after a [`StateMutation::Remove`].
pub const PLAYERS: [&str; 4] = ["alice", "bob", "carol", "ghost"];

/// Writes the players in `process_element`, changes them in the `mutate` timer and reads
/// them back in the `verify` timer.
///
/// Each step runs in its own bundle. So the mutation must reach the runner, and the last
/// read must fetch the state from the runner and not from the cache of the writing bundle.
#[derive(Clone)]
pub struct MutateAcrossBundlesDoFn {
    pub mutation: StateMutation,
}

impl StatefulDoFn for MutateAcrossBundlesDoFn {
    type Key = String;
    type Value = (String, i64);
    type Out = String;
    type State = MutateAcrossBundlesState;
    type Timer = MutateAcrossBundlesTimer;

    fn process_element(
        &mut self,
        _team: &Self::Key,
        (player, points): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.scores.put(player.clone(), points)?;
        state.players.insert(player)?;
        ctx.timer(MutateAcrossBundlesTimer::Mutate)
            .at(Instant::from_millis(1_000))?;
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        team: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            MutateAcrossBundlesTimer::Mutate => {
                let mut keys = state.scores.keys()?;
                keys.sort();
                ctx.emit(format!("before:{team}|keys:{}", keys.join("+")))?;
                match self.mutation {
                    StateMutation::Clear => {
                        state.scores.clear()?;
                        state.players.clear()?;
                    }
                    StateMutation::Remove(players) => {
                        for player in players.iter().map(|player| player.to_string()) {
                            state.scores.remove(&player)?;
                            state.players.remove(&player)?;
                        }
                    }
                }
                ctx.timer(MutateAcrossBundlesTimer::Verify)
                    .at(Instant::from_millis(2_000))?;
                Ok(())
            }
            MutateAcrossBundlesTimer::Verify => match self.mutation {
                StateMutation::Clear => {
                    let mut keys = state.scores.keys()?;
                    keys.sort();
                    let mut remaining = state.players.read()?;
                    remaining.sort();
                    ctx.emit(format!(
                        "after:{team}|keys:{}:players:{}",
                        keys.join("+"),
                        remaining.join("+")
                    ))
                }
                // Prism also lists a removed key in `keys`, so read each player instead.
                StateMutation::Remove(_) => {
                    let probes = PLAYERS.map(String::from);
                    let scores = probes
                        .iter()
                        .map(|player| {
                            Ok(state
                                .scores
                                .get(player)?
                                .map(|points| format!("{player}={points}")))
                        })
                        .filter_map(Result::transpose)
                        .collect::<Result<Vec<_>>>()?;
                    let players = probes
                        .iter()
                        .map(|player| {
                            Ok(state.players.contains(player)?.then_some(player.as_str()))
                        })
                        .filter_map(Result::transpose)
                        .collect::<Result<Vec<_>>>()?;
                    ctx.emit(format!(
                        "after:{team}|scores:{}:players:{}",
                        scores.join("+"),
                        players.join("+")
                    ))
                }
            },
        }
    }
}

#[derive(State)]
pub struct WindowedStatefulTimerState {
    #[state(id = "win_count")]
    pub count: ValueState<i64>,
}

#[derive(Timers)]
pub enum WindowedStatefulTimerTimer {
    #[timer(event_time, id = "win_flush")]
    Flush,
}

#[derive(Clone)]
pub struct WindowedStatefulTimerDoFn;

impl StatefulDoFn for WindowedStatefulTimerDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = WindowedStatefulTimerState;
    type Timer = WindowedStatefulTimerTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        val: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let win = ctx.window()?.interval().ok_or("expected IntervalWindow")?;
        let next = state.count.read()?.unwrap_or(0) + val;
        state.count.write(next)?;
        ctx.timer(WindowedStatefulTimerTimer::Flush)
            .at(win.max_timestamp())?;
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            WindowedStatefulTimerTimer::Flush => {
                let win = ctx.window()?.interval().ok_or("expected IntervalWindow")?;
                if let Some(total) = state.count.read()? {
                    ctx.emit((format!("{key}:{}", win.start.as_millis()), total))?;
                    state.count.clear()?;
                }
                Ok(())
            }
        }
    }
}

#[derive(State)]
pub struct OrderedListLogState {
    #[state(id = "device_events")]
    pub events: OrderedListState<String>,
}

#[derive(Clone)]
pub struct OrderedListLogDoFn;

impl StatefulDoFn for OrderedListLogDoFn {
    type Key = String;
    type Value = (i64, String);
    type Out = String;
    type State = OrderedListLogState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        key: &Self::Key,
        (timestamp, value): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        if value == "CLEAR" {
            state.events.clear()?;
            return ctx.emit(format!("cleared:{key}"));
        }

        if let Some(range_spec) = value.strip_prefix("CLEAR_RANGE:") {
            let (from_str, to_str) = range_spec
                .split_once(':')
                .ok_or_else(|| format!("invalid CLEAR_RANGE payload: {value}"))?;
            let from: i64 = from_str
                .parse()
                .map_err(|err| format!("invalid from timestamp '{from_str}': {err}"))?;
            let to: i64 = to_str
                .parse()
                .map_err(|err| format!("invalid to timestamp '{to_str}': {err}"))?;
            state
                .events
                .clear_range(Instant::from_millis(from)..Instant::from_millis(to))?;
            let remaining = state.events.read()?;
            let formatted: Vec<String> = remaining
                .into_iter()
                .map(|(ts, v)| format!("{}={v}", ts.as_millis()))
                .collect();
            return ctx.emit(format!("after_clear_range:{key}:{}", formatted.join(",")));
        }

        state.events.add(Instant::from_millis(timestamp), value)?;
        let entries = state.events.read()?;
        let formatted: Vec<String> = entries
            .into_iter()
            .map(|(ts, v)| format!("{}={v}", ts.as_millis()))
            .collect();
        ctx.emit(format!("entry:{key}:{}", formatted.join(",")))
    }
}

/// The combining cells of [`CombiningStateDoFn`]: an `i64` sum, a mean with a tuple
/// accumulator, a maximum and a `Decimal` sum.
#[derive(State)]
pub struct ReadingTotals {
    count: CombiningState<Sum<i64>>,
    average: CombiningState<Mean<i64>>,
    peak: CombiningState<Max<i64>>,
    amount: CombiningState<Sum<rust_decimal::Decimal>>,
}

#[derive(Timers)]
pub enum ReadingTimer {
    #[timer(event_time, id = "report")]
    Report,
}

/// What [`CombiningStateDoFn`] emits for a key: the count, the mean, the maximum and the
/// amount, then whether every cell is empty after `clear`.
pub type ReadingReport = (String, (i64, f64, i64, rust_decimal::Decimal, bool));

/// Adds each reading to the combining cells, and reports them from an event-time timer.
///
/// The timer fires in a later bundle than the adds, so the harness reads the accumulators
/// that the runner stored, not the cells that the adding bundle kept in memory.
#[derive(Clone)]
pub struct CombiningStateDoFn;

impl StatefulDoFn for CombiningStateDoFn {
    type Key = String;
    type Value = (i64, rust_decimal::Decimal);
    type Out = ReadingReport;
    type State = ReadingTotals;
    type Timer = ReadingTimer;

    fn process_element(
        &mut self,
        _key: &String,
        (value, amount): Self::Value,
        state: &mut ReadingTotals,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.count.add(1)?;
        state.average.add(value)?;
        state.peak.add(value)?;
        state.amount.add(amount)?;
        // In an interval window the report fires at its end, so each window reports its own
        // cells. In the global window it fires at a fixed time.
        let report_at = ctx
            .window()?
            .interval()
            .map_or(Instant::from_millis(1_000), |window| window.max_timestamp());
        ctx.timer(ReadingTimer::Report).at(report_at)
    }

    fn on_timer(
        &mut self,
        timer: ReadingTimer,
        key: &String,
        state: &mut ReadingTotals,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            ReadingTimer::Report => {
                let report = (
                    state.count.read()?,
                    state.average.read()?,
                    state.peak.read()?,
                    state.amount.read()?,
                );
                state.count.clear()?;
                state.average.clear()?;
                state.peak.clear()?;
                state.amount.clear()?;
                let cleared = state.count.is_empty()?
                    && state.average.is_empty()?
                    && state.peak.is_empty()?
                    && state.amount.is_empty()?;
                let (count, average, peak, amount) = report;
                ctx.emit((key.clone(), (count, average, peak, amount, cleared)))
            }
        }
    }
}
