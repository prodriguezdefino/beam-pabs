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
        state.total_score.write(&next)?;
        state.scorers.append(&user)?;
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

/// Adds the points of each player to a map and each badge to a set. A timer reads both back
/// after all elements, so the output does not depend on the order of the elements.
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
        _team: &Self::Key,
        (player, (points, badge)): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let score = state.player_points.get(&player)?.unwrap_or(0) + points;
        state.player_points.put(&player, &score)?;
        state.unlocked_achievements.insert(&badge)?;
        ctx.timer(PlayerInventoryTimer::Flush)
            .at(Instant::from_millis(1_000))
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
                let mut players = state.player_points.keys()?;
                players.sort();
                let points = players
                    .iter()
                    .map(|player| {
                        let score = state.player_points.get(player)?.unwrap_or(0);
                        Ok(format!("{player}={score}"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut badges = state.unlocked_achievements.read()?;
                badges.sort();
                ctx.emit(format!(
                    "{team}|points:{}|badges:{}",
                    points.join(","),
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
        state.scores.put(&player, &points)?;
        state.players.insert(&player)?;
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
        state.count.write(&next)?;
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
        state.events.add(Instant::from_millis(timestamp), &value)?;
        ctx.emit(format!("entry:{key}:{}", ordered_entries(&state.events)?))
    }
}

/// Formats the content of an ordered list as `<ts>=<value>,...`, in timestamp order.
fn ordered_entries(events: &OrderedListState<String>) -> Result<String> {
    let entries: Vec<String> = events
        .read()?
        .into_iter()
        .map(|(ts, v)| format!("{}={v}", ts.as_millis()))
        .collect();
    Ok(entries.join(","))
}

/// Timers of [`OrderedListClearDoFn`]: `Clear` after all elements, then `Check`.
#[derive(Timers)]
pub enum OrderedListClearTimer {
    #[timer(event_time, id = "clear")]
    Clear,
    #[timer(event_time, id = "check")]
    Check,
}

/// Adds each event to an ordered list. The `Clear` timer removes a range and then clears the
/// list; the `Check` timer reads it back. Timers run after all elements, so the output does
/// not depend on the order of the elements.
#[derive(Clone)]
pub struct OrderedListClearDoFn;

impl StatefulDoFn for OrderedListClearDoFn {
    type Key = String;
    type Value = (i64, String);
    type Out = String;
    type State = OrderedListLogState;
    type Timer = OrderedListClearTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        (timestamp, value): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.events.add(Instant::from_millis(timestamp), &value)?;
        ctx.timer(OrderedListClearTimer::Clear)
            .at(Instant::from_millis(1_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            OrderedListClearTimer::Clear => {
                state
                    .events
                    .clear_range(Instant::from_millis(10)..Instant::from_millis(20))?;
                let remaining = ordered_entries(&state.events)?;
                state.events.clear()?;
                ctx.timer(OrderedListClearTimer::Check)
                    .at(Instant::from_millis(2_000))?;
                ctx.emit(format!("after_clear_range:{key}:{remaining}"))
            }
            OrderedListClearTimer::Check => ctx.emit(format!(
                "after_clear:{key}:{}",
                ordered_entries(&state.events)?
            )),
        }
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

/// The multimap cell of [`MultimapScoresDoFn`] and [`MultimapMutateDoFn`]: the scores of
/// each player.
#[derive(State)]
pub struct MultimapScoresState {
    pub scores: MultimapState<String, i64>,
}

/// Formats the entries of `scores` as `alice=10+20,bob=5`, with keys and values sorted.
fn sorted_entries(scores: &MultimapState<String, i64>) -> Result<String> {
    let mut entries = scores.entries()?;
    entries.sort();
    Ok(entries
        .into_iter()
        .map(|(player, mut points)| {
            points.sort_unstable();
            let points: Vec<String> = points.iter().map(i64::to_string).collect();
            format!("{player}={}", points.join("+"))
        })
        .collect::<Vec<_>>()
        .join(","))
}

#[derive(Timers)]
pub enum MultimapScoresTimer {
    #[timer(event_time, id = "report")]
    Report,
}

/// Adds every score of a player to a `MultimapState`. A timer in a later bundle reads the
/// committed entries back.
#[derive(Clone)]
pub struct MultimapScoresDoFn;

impl StatefulDoFn for MultimapScoresDoFn {
    type Key = String;
    type Value = (String, i64);
    type Out = String;
    type State = MultimapScoresState;
    type Timer = MultimapScoresTimer;

    fn process_element(
        &mut self,
        _team: &Self::Key,
        (player, points): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.scores.put(&player, &points)?;
        ctx.timer(MultimapScoresTimer::Report)
            .at(Instant::from_millis(1_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        team: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            MultimapScoresTimer::Report => {
                let ghost = "ghost".to_string();
                ctx.emit(format!(
                    "{team}|{}|ghost:{}:{}",
                    sorted_entries(&state.scores)?,
                    state.scores.contains_key(&ghost)?,
                    state.scores.get(&ghost)?.len()
                ))
            }
        }
    }
}

#[derive(Debug, Timers)]
pub enum MultimapMutateTimer {
    #[timer(event_time, id = "mutate")]
    Mutate,
    #[timer(event_time, id = "verify")]
    Verify,
}

/// Writes scores in `process_element`, changes them in the `mutate` timer and reads them
/// back in the `verify` timer, each in its own bundle.
///
/// The `mutate` timer removes `alice` and `ghost` from `teamA` and then adds a new score for
/// `alice`. It clears every other team and then adds a score for `dave`.
#[derive(Clone)]
pub struct MultimapMutateDoFn;

impl StatefulDoFn for MultimapMutateDoFn {
    type Key = String;
    type Value = (String, i64);
    type Out = String;
    type State = MultimapScoresState;
    type Timer = MultimapMutateTimer;

    fn process_element(
        &mut self,
        _team: &Self::Key,
        (player, points): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.scores.put(&player, &points)?;
        ctx.timer(MultimapMutateTimer::Mutate)
            .at(Instant::from_millis(1_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        team: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            MultimapMutateTimer::Mutate => {
                ctx.emit(format!("before:{team}|{}", sorted_entries(&state.scores)?))?;
                match team.as_str() {
                    "teamA" => {
                        state.scores.remove(&"alice".to_string())?;
                        state.scores.remove(&"ghost".to_string())?;
                        state.scores.put(&"alice".to_string(), &99)?;
                    }
                    _ => {
                        state.scores.clear()?;
                        state.scores.put(&"dave".to_string(), &1)?;
                    }
                }
                ctx.timer(MultimapMutateTimer::Verify)
                    .at(Instant::from_millis(2_000))
            }
            MultimapMutateTimer::Verify => ctx.emit(format!(
                "after:{team}|{}|alice:{}",
                sorted_entries(&state.scores)?,
                state.scores.contains_key(&"alice".to_string())?
            )),
        }
    }
}

/// The buffer of [`SumOnWindowExpirationDoFn`].
#[derive(State)]
pub struct BufferedValues {
    pub buffer: BagState<i64>,
}

/// Buffers the values of each key and window, and emits their sum when the window expires.
#[derive(Clone)]
pub struct SumOnWindowExpirationDoFn;

impl StatefulDoFn for SumOnWindowExpirationDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = BufferedValues;
    type Timer = NoTimers;
    const ON_WINDOW_EXPIRATION: bool = true;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.buffer.append(&value)
    }

    fn on_window_expiration(
        &mut self,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let values = state.buffer.read()?;
        if values.is_empty() {
            Ok(())
        } else {
            ctx.emit((key.clone(), values.into_iter().sum()))
        }
    }
}

/// The cell of [`SameIdCounterDoFn`] and [`SameIdStepDoFn`]. Both DoFns use the id `foo`.
#[derive(State)]
pub struct SameIdState {
    #[state(id = "foo")]
    pub value: ValueState<i64>,
}

/// Emits the number of elements that it saw before for the key, under the key `sizzle`.
#[derive(Clone)]
pub struct SameIdCounterDoFn;

impl StatefulDoFn for SameIdCounterDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = SameIdState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let current = state.value.read()?.unwrap_or(0);
        state.value.write(&(current + 1))?;
        ctx.emit(("sizzle".to_string(), current))
    }
}

/// Emits 13 for the first element of a key, and 13 more for each next element.
#[derive(Clone)]
pub struct SameIdStepDoFn;

impl StatefulDoFn for SameIdStepDoFn {
    type Key = String;
    type Value = i64;
    type Out = i64;
    type State = SameIdState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let current = state.value.read()?.unwrap_or(13);
        state.value.write(&(current + 13))?;
        ctx.emit(current)
    }
}

/// The outputs of [`StatefulParityDoFn`]: one PCollection per variant.
#[derive(Outputs)]
pub enum StateParity {
    Evens(i64),
    Odds(i64),
}

/// Counts the elements of each key, and sends each count to the output of its parity.
#[derive(Clone)]
pub struct StatefulParityDoFn;

impl StatefulDoFn for StatefulParityDoFn {
    type Key = String;
    type Value = i64;
    type Out = StateParity;
    type State = SameIdState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let current = state.value.read()?.unwrap_or(0);
        state.value.write(&(current + 1))?;
        match current % 2 {
            0 => ctx.emit(StateParity::Evens(current)),
            _ => ctx.emit(StateParity::Odds(current)),
        }
    }
}

/// Removes every key of a committed `MultimapState` in the `mutate` timer, and checks both
/// `keys` and `entries` in the same bundle and in the `verify` timer of a later bundle.
#[derive(Clone)]
pub struct MultimapRemoveAllDoFn;

impl StatefulDoFn for MultimapRemoveAllDoFn {
    type Key = String;
    type Value = (String, i64);
    type Out = String;
    type State = MultimapScoresState;
    type Timer = MultimapMutateTimer;

    fn process_element(
        &mut self,
        _team: &Self::Key,
        (player, points): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.scores.put(&player, &points)?;
        ctx.timer(MultimapMutateTimer::Mutate)
            .at(Instant::from_millis(1_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        team: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            MultimapMutateTimer::Mutate => {
                let mut players = state.scores.keys()?;
                players.sort();
                let empty_after = players
                    .iter()
                    .map(|player| {
                        state.scores.remove(player)?;
                        let empty =
                            state.scores.keys()?.is_empty() && state.scores.entries()?.is_empty();
                        Ok(format!("{player}={empty}"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                ctx.timer(MultimapMutateTimer::Verify)
                    .at(Instant::from_millis(2_000))?;
                ctx.emit(format!(
                    "mutate:{team}|empty_after:{}",
                    empty_after.join(",")
                ))
            }
            MultimapMutateTimer::Verify => {
                let present = PLAYERS
                    .map(String::from)
                    .iter()
                    .map(|player| state.scores.contains_key(player))
                    .collect::<Result<Vec<_>>>()?;
                let empty = state.scores.keys()?.is_empty() && state.scores.entries()?.is_empty();
                ctx.emit(format!(
                    "verify:{team}|empty:{empty}|any_key:{}",
                    present.contains(&true)
                ))
            }
        }
    }
}

/// Adds each event to an ordered list, and reads four ranges when the window expires.
#[derive(Clone)]
pub struct OrderedListRangeDoFn;

impl StatefulDoFn for OrderedListRangeDoFn {
    type Key = String;
    type Value = (i64, String);
    type Out = Vec<(i64, String)>;
    type State = OrderedListLogState;
    type Timer = NoTimers;
    const ON_WINDOW_EXPIRATION: bool = true;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        (timestamp, value): Self::Value,
        state: &mut Self::State,
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.events.add(Instant::from_millis(timestamp), &value)
    }

    fn on_window_expiration(
        &mut self,
        _key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        [(0, 12), (0, 13), (13, 53), (52, 98)]
            .into_iter()
            .try_for_each(|(start, end)| {
                let range = Instant::from_millis(start)..Instant::from_millis(end);
                let events = state
                    .events
                    .read_range(range)?
                    .into_iter()
                    .map(|(ts, value)| (ts.as_millis(), value))
                    .collect();
                ctx.emit(events)
            })
    }
}

/// The cell of [`TimeSortedDeltaDoFn`].
#[derive(State)]
pub struct LastValueState {
    pub last: ValueState<i64>,
}

/// Requires time-sorted input and emits the difference from the value before it.
///
/// The first value of a key gives 1. When each value is its timestamp in milliseconds and
/// no value is missing, every output is 1.
#[derive(Clone)]
pub struct TimeSortedDeltaDoFn;

impl StatefulDoFn for TimeSortedDeltaDoFn {
    type Key = String;
    type Value = i64;
    type Out = i64;
    type State = LastValueState;
    type Timer = NoTimers;
    const REQUIRES_TIME_SORTED_INPUT: bool = true;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let last = state.last.read()?.unwrap_or(value - 1);
        state.last.write(&value)?;
        ctx.emit(value - last)
    }
}

/// Event-time timer of [`OrderedListRangeDeleteDoFn`].
#[derive(Timers)]
pub enum OrderedListDeleteTimer {
    #[timer(event_time)]
    Delete,
}

/// Adds timestamped events to an ordered list, clears `[0, 43 ms)` when its 42 ms timer fires,
/// and emits the remaining entries when the window expires.
#[derive(Clone)]
pub struct OrderedListRangeDeleteDoFn;

impl StatefulDoFn for OrderedListRangeDeleteDoFn {
    type Key = String;
    type Value = (i64, String);
    type Out = (i64, String);
    type State = OrderedListLogState;
    type Timer = OrderedListDeleteTimer;
    const ON_WINDOW_EXPIRATION: bool = true;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        (timestamp, value): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.events.add(Instant::from_millis(timestamp), &value)?;
        ctx.timer(OrderedListDeleteTimer::Delete)
            .at(Instant::from_millis(42))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        _key: &Self::Key,
        state: &mut Self::State,
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            OrderedListDeleteTimer::Delete => state
                .events
                .clear_range(Instant::EPOCH..Instant::from_millis(43)),
        }
    }

    fn on_window_expiration(
        &mut self,
        _key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state
            .events
            .read()?
            .into_iter()
            .try_for_each(|(ts, value)| ctx.emit((ts.as_millis(), value)))
    }
}
