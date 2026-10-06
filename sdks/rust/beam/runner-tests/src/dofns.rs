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

use beam::prelude::*;
use beam::schema::BeamRow;
use beam::transforms::{FinishBundleContext, ProcessContext, Windowed};
use beam::windowing::GlobalWindow;

/// A custom DoFn testing bundle lifecycle execution order.
///
/// Fails the bundle if a lifecycle method is called out of order: `start_bundle`
/// before `setup` or inside a bundle, `process_element` or `finish_bundle` outside
/// one. Emits `process:<element>` per element and `finish:<n>` per bundle, `n` being
/// the number of elements that bundle processed.
#[derive(Clone, Default)]
pub struct LifecycleDoFn {
    set_up: bool,
    in_bundle: bool,
    processed: usize,
}

impl DoFn for LifecycleDoFn {
    type In = String;
    type Out = String;

    fn setup(&mut self) -> Result {
        self.set_up = true;
        Ok(())
    }

    fn start_bundle(&mut self) -> Result {
        if !self.set_up {
            return Err("start_bundle called before setup".into());
        }
        if self.in_bundle {
            return Err("start_bundle called inside a bundle".into());
        }
        self.in_bundle = true;
        self.processed = 0;
        Ok(())
    }

    fn process_element(&mut self, element: String, out: &mut ProcessContext<String>) -> Result {
        if !self.in_bundle {
            return Err(format!("process_element({element}) called outside a bundle").into());
        }
        self.processed += 1;
        out.emit(format!("process:{element}"))
    }

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, String>) -> Result {
        if !self.in_bundle {
            return Err("finish_bundle called outside a bundle".into());
        }
        self.in_bundle = false;
        // The pipeline is globally windowed.
        out.emit(
            format!("finish:{}", self.processed),
            BoundedWindow::Global(GlobalWindow),
            Instant::END_OF_GLOBAL_WINDOW,
        )
    }
}
/// The outputs of [`SplitTagsDoFn`]: one PCollection per variant.
#[derive(Outputs)]
pub enum Parity {
    Evens(String),
    Odds(String),
}

/// Splits elements across multiple tagged outputs.
#[derive(Clone)]
pub struct SplitTagsDoFn;

impl DoFn for SplitTagsDoFn {
    type In = i64;
    type Out = Parity;

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        if element % 2 == 0 {
            out.emit(Parity::Evens(format!("even:{element}")))?;
        } else {
            out.emit(Parity::Odds(format!("odd:{element}")))?;
        }
        Ok(())
    }
}
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
#[derive(Clone, Default)]
pub struct ValidatesAssignTimestampDoFn;
impl DoFn for ValidatesAssignTimestampDoFn {
    type In = (String, (i64, i64));
    type Out = (String, i64);

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let (k, (v, ts)) = element;
        ctx.output((k, v)).at(Instant::from_millis(ts)).emit()
    }
}
/// Timer family that clears the inventory state.
pub const CLEAR_TIMER: &str = "clear_inventory";

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
pub struct ClearAcrossBundlesState {
    pub scores: MapState<String, i64>,
    pub players: SetState<String>,
}

#[derive(Debug, Timers)]
pub enum ClearAcrossBundlesTimer {
    #[timer(event_time, id = "clear_inventory")]
    Clear,
    #[timer(event_time, id = "verify_inventory")]
    Verify,
}

#[derive(Clone)]
pub struct ClearAcrossBundlesDoFn;

impl StatefulDoFn for ClearAcrossBundlesDoFn {
    type Key = String;
    type Value = (String, i64);
    type Out = String;
    type State = ClearAcrossBundlesState;
    type Timer = ClearAcrossBundlesTimer;

    fn process_element(
        &mut self,
        _team: &Self::Key,
        (player, points): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.scores.put(player.clone(), points)?;
        state.players.insert(player)?;
        ctx.timer(ClearAcrossBundlesTimer::Clear)
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
            ClearAcrossBundlesTimer::Clear => {
                let mut keys = state.scores.keys()?;
                keys.sort();
                ctx.emit(format!("before:{team}|keys:{}", keys.join("+")))?;
                state.scores.clear()?;
                state.players.clear()?;
                ctx.timer(ClearAcrossBundlesTimer::Verify)
                    .at(Instant::from_millis(2_000))?;
                Ok(())
            }
            ClearAcrossBundlesTimer::Verify => {
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
        }
    }
}
pub struct TransactionalBatchSink {
    pub batch_size: usize,
    pub buffer: Vec<String>,
    pub active_tx: Option<u64>,
}

/// A copy starts with an empty buffer and no transaction: both belong to one bundle.
impl Clone for TransactionalBatchSink {
    fn clone(&self) -> Self {
        Self {
            batch_size: self.batch_size,
            buffer: Vec::new(),
            active_tx: None,
        }
    }
}
impl DoFn for TransactionalBatchSink {
    type In = String;
    type Out = String;

    fn start_bundle(&mut self) -> Result {
        self.active_tx = Some(101);
        Ok(())
    }

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> Result {
        self.buffer.push(element);
        if self.buffer.len() >= self.batch_size {
            let chunk = std::mem::take(&mut self.buffer);
            out.emit(format!("batch_flush:{}", chunk.join("+")))?;
        }
        Ok(())
    }

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, Self::Out>) -> Result {
        // The pipeline is globally windowed.
        let window = BoundedWindow::Global(GlobalWindow);
        let ts = Instant::END_OF_GLOBAL_WINDOW;
        let tx = self.active_tx.take().unwrap_or(0);
        if !self.buffer.is_empty() {
            let chunk = std::mem::take(&mut self.buffer);
            out.emit(format!("batch_flush:{}", chunk.join("+")), window, ts)?;
        }
        out.emit(format!("tx_commit:{tx}"), window, ts)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, BeamRow)]
#[beam(crate = "::beam", id = "beam.runner_tests.AuditEvent")]
pub struct AuditEvent {
    pub account_id: String,
    pub amount: i64,
    pub tags: Vec<String>,
    pub is_verified: bool,
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

        if value.starts_with("CLEAR_RANGE:") {
            let parts: Vec<&str> = value.split(':').collect();
            let from: i64 = parts[1].parse().unwrap();
            let to: i64 = parts[2].parse().unwrap();
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

#[derive(Clone, Default, serde::Serialize, serde::Deserialize, Register)]
pub struct ValidatesObserveWindowDoFn;

impl DoFn for ValidatesObserveWindowDoFn {
    type In = Windowed<(String, i64)>;
    type Out = (String, (i64, i64));

    fn process_element(
        &mut self,
        Windowed {
            value: (k, v),
            window,
        }: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let win = window.interval().ok_or("expected IntervalWindow")?;
        ctx.emit((k, (win.start.as_millis(), v)))
    }
}
