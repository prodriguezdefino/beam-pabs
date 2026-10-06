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
use beam::transforms::ProcessContext;

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

    fn finish_bundle(&mut self, out: &mut ProcessContext<String>) -> Result {
        if !self.in_bundle {
            return Err("finish_bundle called outside a bundle".into());
        }
        self.in_bundle = false;
        out.emit(format!("finish:{}", self.processed))
    }
}
/// Splits elements across multiple tagged outputs.
#[derive(Clone)]
pub struct SplitTagsDoFn;

impl DoFn for SplitTagsDoFn {
    type In = i64;
    type Out = String;

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        if element % 2 == 0 {
            out.output(format!("even:{element}")).to("evens").emit()?;
        } else {
            out.output(format!("odd:{element}")).to("odds").emit()?;
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct StatefulTeamMilestoneDoFn {
    pub total_score: ValueStateSpec<i64>,
    pub scorers: BagStateSpec<String>,
    pub flush_timer: TimerFamilySpec,
}
impl DoFn for StatefulTeamMilestoneDoFn {
    type In = (String, (String, i64));
    type Out = String;

    fn process_element(
        &mut self,
        (team, (user, score)): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let mut total_state = ctx.value_state(&self.total_score, &team)?;
        let mut scorers_bag = ctx.bag_state(&self.scorers, &team)?;
        let timer = ctx.timer(&self.flush_timer)?.key(&team)?;

        let prev = total_state.read()?.unwrap_or(0);
        let next = prev + score;
        total_state.write(next)?;
        scorers_bag.append(user)?;
        timer.set(1_000);

        if next >= 100 && prev < 100 {
            let mut who = scorers_bag.read()?;
            who.sort();
            ctx.emit(format!("milestone:{team}:{next}:{}", who.join("+")))?;
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        _timer_family_id: &str,
        _tag: &str,
        _timestamp: i64,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let team: String = ctx.current_key()?;
        let mut total_state = ctx.value_state(&self.total_score, &team)?;
        if let Some(final_total) = total_state.read()? {
            ctx.emit(format!("timer_flush:{team}:{final_total}"))?;
            total_state.clear()?;
        }
        Ok(())
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
        ctx.output((k, v)).at(ts).emit()
    }
}
/// Timer family that clears the inventory state.
pub const CLEAR_TIMER: &str = "clear_inventory";

#[derive(Clone)]
pub struct PlayerInventoryDoFn {
    pub player_points: MapStateSpec<String, i64>,
    pub unlocked_achievements: SetStateSpec<String>,
    pub flush_timer: TimerFamilySpec,
}
impl DoFn for PlayerInventoryDoFn {
    type In = (String, (String, (i64, String)));
    type Out = String;

    fn process_element(
        &mut self,
        (team, (player, (points, badge))): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let mut scores_map = ctx.map_state(&self.player_points, &team)?;
        let mut badges_set = ctx.set_state(&self.unlocked_achievements, &team)?;
        ctx.timer(&self.flush_timer)?.key(&team)?.set(1_000);

        if badge == "CLEAR" {
            scores_map.clear()?;
            badges_set.clear()?;
            return ctx.emit(format!("cleared:{team}"));
        }

        let prev_score = scores_map.get(&player)?.unwrap_or(0);
        let next_score = prev_score + points;
        scores_map.put(player.clone(), next_score)?;

        let already_had = badges_set.contains(&badge)?;
        if !already_had {
            badges_set.insert(badge)?;
        }

        let mut keys = scores_map.keys()?;
        keys.sort();
        let mut badges = badges_set.read()?;
        badges.sort();

        ctx.emit(format!(
            "entry:{team}:{player}:{next_score}:{already_had}|keys:{}:badges:{}",
            keys.join("+"),
            badges.join("+")
        ))
    }

    fn on_timer(
        &mut self,
        _timer_family_id: &str,
        _tag: &str,
        _timestamp: i64,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let team: String = ctx.current_key()?;
        let scores_map = ctx.map_state(&self.player_points, &team)?;
        let badges_set = ctx.set_state(&self.unlocked_achievements, &team)?;

        let mut keys = scores_map.keys()?;
        keys.sort();
        let mut badges = badges_set.read()?;
        badges.sort();

        ctx.emit(format!(
            "final:{team}|keys:{}:badges:{}",
            keys.join("+"),
            badges.join("+")
        ))
    }
}
#[derive(Clone)]
pub struct ClearAcrossBundlesDoFn {
    pub scores: MapStateSpec<String, i64>,
    pub players: SetStateSpec<String>,
    pub clear_timer: TimerFamilySpec,
    pub verify_timer: TimerFamilySpec,
}
impl DoFn for ClearAcrossBundlesDoFn {
    type In = (String, (String, i64));
    type Out = String;

    fn process_element(
        &mut self,
        (team, (player, points)): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        ctx.map_state(&self.scores, &team)?
            .put(player.clone(), points)?;
        ctx.set_state(&self.players, &team)?.insert(player)?;
        ctx.timer(&self.clear_timer)?.key(&team)?.set(1_000);
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer_family_id: &str,
        _tag: &str,
        _timestamp: i64,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let team: String = ctx.current_key()?;
        let mut scores = ctx.map_state(&self.scores, &team)?;
        let mut players = ctx.set_state(&self.players, &team)?;

        let mut keys = scores.keys()?;
        keys.sort();

        if timer_family_id == CLEAR_TIMER {
            ctx.emit(format!("before:{team}|keys:{}", keys.join("+")))?;
            scores.clear()?;
            players.clear()?;
            ctx.timer(&self.verify_timer)?.key(&team)?.set(2_000);
            return Ok(());
        }

        let mut remaining = players.read()?;
        remaining.sort();
        ctx.emit(format!(
            "after:{team}|keys:{}:players:{}",
            keys.join("+"),
            remaining.join("+")
        ))
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

    fn finish_bundle(&mut self, out: &mut ProcessContext<Self::Out>) -> Result {
        let tx = self.active_tx.take().unwrap_or(0);
        if !self.buffer.is_empty() {
            let chunk = std::mem::take(&mut self.buffer);
            out.emit(format!("batch_flush:{}", chunk.join("+")))?;
        }
        out.emit(format!("tx_commit:{tx}"))
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

#[derive(Clone)]
pub struct WindowedStatefulTimerDoFn {
    pub count: ValueStateSpec<i64>,
    pub flush_timer: TimerFamilySpec,
}

impl DoFn for WindowedStatefulTimerDoFn {
    type In = (String, i64);
    type Out = (String, i64);

    fn process_element(
        &mut self,
        (key, val): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let win = ctx.interval_window().ok_or("expected IntervalWindow")?;
        let mut count_state = ctx.value_state(&self.count, &key)?;
        let next = count_state.read()?.unwrap_or(0) + val;
        count_state.write(next)?;
        ctx.timer(&self.flush_timer)?
            .key(&key)?
            .set(win.max_timestamp());
        Ok(())
    }

    fn on_timer(
        &mut self,
        _timer_family_id: &str,
        _tag: &str,
        _timestamp: i64,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let key: String = ctx.current_key()?;
        let win = ctx.interval_window().ok_or("expected IntervalWindow")?;
        let mut count_state = ctx.value_state(&self.count, &key)?;
        if let Some(total) = count_state.read()? {
            ctx.emit((format!("{key}:{}", win.start_millis), total))?;
            count_state.clear()?;
        }
        Ok(())
    }
}
