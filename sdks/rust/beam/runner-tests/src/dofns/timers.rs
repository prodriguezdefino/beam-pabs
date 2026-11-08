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

//! Stateful DoFns for the timer and time-sorted input pipelines, and a DoFn that sets the
//! timestamps of tagged outputs.

use std::time::Duration;

use beam::prelude::*;

/// Records fired timer tags in firing order for [`TimerStrictOrderingDoFn`].
#[derive(State)]
pub struct TimerOrderState {
    pub fired: ValueState<Vec<String>>,
}

/// Dynamic event-time timer family for [`TimerStrictOrderingDoFn`].
#[derive(Timers)]
pub enum TimerOrderTimer {
    #[timer(event_time)]
    Step(String),
}

/// Sets three event-time timers out of timestamp order and records the firing sequence.
#[derive(Clone)]
pub struct TimerStrictOrderingDoFn;

impl StatefulDoFn for TimerStrictOrderingDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, String);
    type State = TimerOrderState;
    type Timer = TimerOrderTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _val: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(TimerOrderTimer::Step("t3000".to_string()))
            .at(Instant::from_millis(3_000))?;
        ctx.timer(TimerOrderTimer::Step("t1000".to_string()))
            .at(Instant::from_millis(1_000))?;
        ctx.timer(TimerOrderTimer::Step("t2000".to_string()))
            .at(Instant::from_millis(2_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            TimerOrderTimer::Step(tag) => {
                let fire_ts = ctx
                    .fire_timestamp()
                    .ok_or("missing fire timestamp in on_timer")?;
                let mut steps = state.fired.read()?.unwrap_or_default();
                steps.push(format!("{tag}@{}", fire_ts.as_millis()));
                if tag == "t3000" {
                    state.fired.clear()?;
                    ctx.emit((key.clone(), steps.join("->")))
                } else {
                    state.fired.write(&steps)
                }
            }
        }
    }
}

/// Holds the remaining loop iterations for [`TimerLoopingDoFn`].
#[derive(State)]
pub struct TimerLoopState {
    pub remaining: ValueState<i64>,
}

/// Event-time timer family that rearms itself in [`TimerLoopingDoFn`].
#[derive(Timers)]
pub enum TimerLoopTimer {
    #[timer(event_time)]
    Tick,
}

/// Rearms an event-time timer from `on_timer` until the remaining count reaches zero.
#[derive(Clone)]
pub struct TimerLoopingDoFn;

impl StatefulDoFn for TimerLoopingDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = TimerLoopState;
    type Timer = TimerLoopTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        count: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.remaining.write(&count)?;
        ctx.timer(TimerLoopTimer::Tick)
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
            TimerLoopTimer::Tick => {
                let Some(remaining) = state.remaining.read()? else {
                    return Ok(());
                };
                let fire_ts = ctx
                    .fire_timestamp()
                    .ok_or("missing fire timestamp in on_timer")?;
                ctx.emit(format!("{key}:step={remaining}:ts={}", fire_ts.as_millis()))?;
                if remaining > 1 {
                    state.remaining.write(&(remaining - 1))?;
                    ctx.timer(TimerLoopTimer::Tick)
                        .at(fire_ts + Duration::from_secs(1))?;
                } else {
                    state.remaining.clear()?;
                }
                Ok(())
            }
        }
    }
}

/// Two event-time timer families: one with the default hold and one with an explicit hold.
#[derive(Timers)]
pub enum OutputTimestampTimer {
    #[timer(event_time)]
    DefaultHold,
    #[timer(event_time)]
    ExplicitHold,
}

/// Sets one timer with the default watermark hold and one timer with an earlier hold.
#[derive(Clone)]
pub struct TimerOutputTimestampDoFn;

impl StatefulDoFn for TimerOutputTimestampDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, String);
    type State = ();
    type Timer = OutputTimestampTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _val: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(OutputTimestampTimer::DefaultHold)
            .at(Instant::from_millis(2_000))?;
        ctx.timer(OutputTimestampTimer::ExplicitHold)
            .hold(Instant::from_millis(500))
            .at(Instant::from_millis(2_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let label = match timer {
            OutputTimestampTimer::DefaultHold => "default",
            OutputTimestampTimer::ExplicitHold => "held",
        };
        ctx.emit((key.clone(), label.to_string()))
    }
}

/// Collects the tags of fired dynamic timers for [`DynamicTimerDoFn`].
#[derive(State)]
pub struct DynamicTimerState {
    pub tags: BagState<String>,
}

/// A dynamic-tag timer family and a summary timer family for [`DynamicTimerDoFn`].
#[derive(Timers)]
pub enum DynamicTimer {
    #[timer(event_time)]
    Action(String),
    #[timer(event_time)]
    Summary,
}

/// Sets three tagged timers, clears one before it fires, and emits the surviving tags.
#[derive(Clone)]
pub struct DynamicTimerDoFn;

impl StatefulDoFn for DynamicTimerDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = DynamicTimerState;
    type Timer = DynamicTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _val: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(DynamicTimer::Action("keep_a".to_string()))
            .at(Instant::from_millis(1_000))?;
        ctx.timer(DynamicTimer::Action("cancel_b".to_string()))
            .at(Instant::from_millis(1_500))?;
        ctx.timer(DynamicTimer::Action("keep_c".to_string()))
            .at(Instant::from_millis(2_000))?;
        ctx.timer(DynamicTimer::Action("cancel_b".to_string()))
            .clear()?;
        ctx.timer(DynamicTimer::Summary)
            .at(Instant::from_millis(3_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            DynamicTimer::Action(tag) => state.tags.append(&tag),
            DynamicTimer::Summary => {
                let mut tags = state.tags.read()?;
                tags.sort();
                state.tags.clear()?;
                ctx.emit(format!("{key}:{}", tags.join("+")))
            }
        }
    }
}

/// Buffers per-key totals until a processing-time timer flushes them.
#[derive(State)]
pub struct ProcessingTimeState {
    pub pending: ValueState<i64>,
}

/// Processing-time flush timer for [`ProcessingTimeTimerDoFn`].
#[derive(Timers)]
pub enum ProcessingTimeTimer {
    #[timer(processing_time)]
    Flush,
}

/// Accumulates values per key and flushes them from a processing-time timer one second later.
#[derive(Clone)]
pub struct ProcessingTimeTimerDoFn;

impl StatefulDoFn for ProcessingTimeTimerDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = ProcessingTimeState;
    type Timer = ProcessingTimeTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        val: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let next = state.pending.read()?.unwrap_or(0) + val;
        state.pending.write(&next)?;
        ctx.timer(ProcessingTimeTimer::Flush)
            .after(Duration::from_secs(1))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            ProcessingTimeTimer::Flush => {
                if let Some(total) = state.pending.read()? {
                    state.pending.clear()?;
                    ctx.emit((key.clone(), total))?;
                }
                Ok(())
            }
        }
    }
}

/// Stores the value for [`TimerWithSideInputDoFn`] until its timer fires.
#[derive(State)]
pub struct TimerSideInputState {
    pub value: ValueState<i64>,
}

/// Event-time timer family for [`TimerWithSideInputDoFn`].
#[derive(Timers)]
pub enum TimerSideInputTimer {
    #[timer(event_time)]
    Apply,
}

/// Reads a singleton side input inside `on_timer` to scale the stored value of each key.
#[derive(Clone)]
pub struct TimerWithSideInputDoFn {
    pub multiplier: SingletonView<i64>,
}

impl StatefulDoFn for TimerWithSideInputDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = TimerSideInputState;
    type Timer = TimerSideInputTimer;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.multiplier]
    }

    fn process_element(
        &mut self,
        _key: &Self::Key,
        val: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.value.write(&val)?;
        ctx.timer(TimerSideInputTimer::Apply)
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
            TimerSideInputTimer::Apply => {
                if let Some(val) = state.value.read()? {
                    let factor = ctx.side_input(&self.multiplier)?;
                    state.value.clear()?;
                    ctx.emit((key.clone(), val * factor))?;
                }
                Ok(())
            }
        }
    }
}

/// Tracks the last timestamp and received values for [`TimeSortedInputDoFn`].
#[derive(State)]
pub struct TimeSortedState {
    pub last_timestamp: ValueState<i64>,
    pub values: ValueState<Vec<i64>>,
}

/// Event-time flush timer for [`TimeSortedInputDoFn`].
#[derive(Timers)]
pub enum TimeSortedTimer {
    #[timer(event_time)]
    Flush,
}

/// Declares `REQUIRES_TIME_SORTED_INPUT` and checks that timestamps arrive in order per key.
#[derive(Clone)]
pub struct TimeSortedInputDoFn;

impl StatefulDoFn for TimeSortedInputDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, Vec<i64>);
    type State = TimeSortedState;
    type Timer = TimeSortedTimer;

    const REQUIRES_TIME_SORTED_INPUT: bool = true;

    fn process_element(
        &mut self,
        key: &Self::Key,
        val: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let ts = ctx.timestamp().as_millis();
        if let Some(prev_ts) = state.last_timestamp.read()?
            && ts < prev_ts
        {
            return Err(format!("key {key} received timestamp {ts} after {prev_ts}").into());
        }
        state.last_timestamp.write(&ts)?;
        let mut values = state.values.read()?.unwrap_or_default();
        values.push(val);
        state.values.write(&values)?;
        ctx.timer(TimeSortedTimer::Flush)
            .at(Instant::from_millis(10_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            TimeSortedTimer::Flush => {
                if let Some(values) = state.values.read()? {
                    state.last_timestamp.clear()?;
                    state.values.clear()?;
                    ctx.emit((key.clone(), values))?;
                }
                Ok(())
            }
        }
    }
}

/// The value that [`EmitAtWindowEndDoFn`] holds until its timer fires.
#[derive(State)]
pub struct HeldValueState {
    pub value: ValueState<i64>,
}

/// Event-time timer at the end of the window, for [`EmitAtWindowEndDoFn`].
#[derive(Timers)]
pub enum WindowEndTimer {
    #[timer(event_time)]
    End,
}

/// Holds the last value of each key and window, and emits it from a timer at the end of
/// the window.
#[derive(Clone)]
pub struct EmitAtWindowEndDoFn;

impl StatefulDoFn for EmitAtWindowEndDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = HeldValueState;
    type Timer = WindowEndTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.value.write(&value)?;
        ctx.timer(WindowEndTimer::End)
            .at(ctx.window()?.max_timestamp())
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            WindowEndTimer::End => match state.value.read()? {
                Some(value) => ctx.emit((key.clone(), value)),
                None => Err(format!("the timer of key {key} fired without a value").into()),
            },
        }
    }
}

/// The number of elements of each key, for the DoFns that reset a timer per element.
#[derive(State)]
pub struct ElementCountState {
    pub count: ValueState<i64>,
}

/// Event-time timer of [`EventTimeTimerResetDoFn`].
#[derive(Timers)]
pub enum EventTimeResetTimer {
    #[timer(event_time)]
    Fire,
}

/// Sets its timer twice for each element. The last time, `count` seconds, replaces the
/// earlier ones, so the timer fires once for each key.
#[derive(Clone)]
pub struct EventTimeTimerResetDoFn;

impl StatefulDoFn for EventTimeTimerResetDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ElementCountState;
    type Timer = EventTimeResetTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let count = state.count.read()?.unwrap_or(0) + 1;
        state.count.write(&count)?;
        ctx.timer(EventTimeResetTimer::Fire)
            .at(Instant::from_millis(9_000))?;
        ctx.timer(EventTimeResetTimer::Fire)
            .at(Instant::from_millis(count * 1_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            EventTimeResetTimer::Fire => {
                let fire_ts = ctx
                    .fire_timestamp()
                    .ok_or("missing fire timestamp in on_timer")?;
                let count = state.count.read()?.unwrap_or(0);
                ctx.emit(format!("{key}:count={count}@{}", fire_ts.as_millis()))
            }
        }
    }
}

/// Processing-time timer of [`ProcessingTimeTimerResetDoFn`].
#[derive(Timers)]
pub enum ProcessingTimeResetTimer {
    #[timer(processing_time)]
    Fire,
}

/// Sets its processing-time timer again for each element, so the timer fires once for each
/// key and sees the count of all elements.
#[derive(Clone)]
pub struct ProcessingTimeTimerResetDoFn;

impl StatefulDoFn for ProcessingTimeTimerResetDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ElementCountState;
    type Timer = ProcessingTimeResetTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let count = state.count.read()?.unwrap_or(0) + 1;
        state.count.write(&count)?;
        ctx.timer(ProcessingTimeResetTimer::Fire)
            .after(Duration::from_secs(10))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            ProcessingTimeResetTimer::Fire => {
                let count = state.count.read()?.unwrap_or(0);
                ctx.emit(format!("{key}:count={count}"))
            }
        }
    }
}

/// The number of firings of [`TimerPingPongDoFn`] for each key.
#[derive(State)]
pub struct PingPongState {
    pub fired: ValueState<i64>,
}

/// Two event-time timers that set each other, for [`TimerPingPongDoFn`].
#[derive(Timers)]
pub enum PingPongTimer {
    #[timer(event_time)]
    Ping,
    #[timer(event_time)]
    Pong,
}

/// Sets `Ping` at 1 s. Each firing sets the other timer one second later, until `firings`
/// timers fired for the key.
#[derive(Clone)]
pub struct TimerPingPongDoFn {
    pub firings: i64,
}

impl StatefulDoFn for TimerPingPongDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = PingPongState;
    type Timer = PingPongTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(PingPongTimer::Ping)
            .at(Instant::from_millis(1_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let fire_ts = ctx
            .fire_timestamp()
            .ok_or("missing fire timestamp in on_timer")?;
        let fired = state.fired.read()?.unwrap_or(0) + 1;
        state.fired.write(&fired)?;
        let (label, next) = match timer {
            PingPongTimer::Ping => ("ping", PingPongTimer::Pong),
            PingPongTimer::Pong => ("pong", PingPongTimer::Ping),
        };
        ctx.emit(format!("{key}:{label}@{}", fire_ts.as_millis()))?;
        if fired < self.firings {
            ctx.timer(next).at(fire_ts + Duration::from_secs(1))
        } else {
            Ok(())
        }
    }
}

/// Event-time timers of [`EventTimeOutputHoldDoFn`].
#[derive(Timers)]
pub enum EventTimeHoldTimer {
    #[timer(event_time)]
    Held,
    #[timer(event_time)]
    Default,
}

/// Sets `Held` at 10 s with a hold at 500 ms, and `Default` at 2.5 s with the default hold.
/// Emits `key:label` when a timer fires.
#[derive(Clone)]
pub struct EventTimeOutputHoldDoFn;

impl StatefulDoFn for EventTimeOutputHoldDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = EventTimeHoldTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(EventTimeHoldTimer::Held)
            .hold(Instant::from_millis(500))
            .at(Instant::from_millis(10_000))?;
        ctx.timer(EventTimeHoldTimer::Default)
            .at(Instant::from_millis(2_500))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let label = match timer {
            EventTimeHoldTimer::Held => "held",
            EventTimeHoldTimer::Default => "default",
        };
        ctx.emit(format!("{key}:{label}"))
    }
}

/// Processing-time timer of [`ProcessingTimeOutputHoldDoFn`].
#[derive(Timers)]
pub enum ProcessingTimeHoldTimer {
    #[timer(processing_time)]
    Flush,
}

/// Sets a processing-time timer that holds the watermark at 500 ms, and emits the key when
/// it fires.
#[derive(Clone)]
pub struct ProcessingTimeOutputHoldDoFn;

impl StatefulDoFn for ProcessingTimeOutputHoldDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = ProcessingTimeHoldTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(ProcessingTimeHoldTimer::Flush)
            .hold(Instant::from_millis(500))
            .after(Duration::from_secs(1))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            ProcessingTimeHoldTimer::Flush => ctx.emit(key.clone()),
        }
    }
}

/// A processing-time timer family with dynamic tags, for [`ProcessingTimerFamilyDoFn`].
#[derive(Timers)]
pub enum ProcessingTimerFamily {
    #[timer(processing_time)]
    Tick(String),
}

/// Sets the tags `a` and `b` of a processing-time family, and emits `key:tag` for each tag
/// that fires.
#[derive(Clone)]
pub struct ProcessingTimerFamilyDoFn;

impl StatefulDoFn for ProcessingTimerFamilyDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = ProcessingTimerFamily;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(ProcessingTimerFamily::Tick("a".to_string()))
            .after(Duration::from_secs(1))?;
        ctx.timer(ProcessingTimerFamily::Tick("b".to_string()))
            .after(Duration::from_secs(2))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            ProcessingTimerFamily::Tick(tag) => ctx.emit(format!("{key}:{tag}")),
        }
    }
}

/// Event-time timer of [`PipelineOptionsDoFn`].
#[derive(Timers)]
pub enum OptionsTimer {
    #[timer(event_time)]
    Fire,
}

/// Emits `option`, which the builder reads from the pipeline options, from
/// `process_element` and from `on_timer`.
#[derive(Clone)]
pub struct PipelineOptionsDoFn {
    pub option: String,
}

impl StatefulDoFn for PipelineOptionsDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = OptionsTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.emit(format!("process:{}", self.option))?;
        ctx.timer(OptionsTimer::Fire)
            .at(Instant::from_millis(1_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        _key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            OptionsTimer::Fire => ctx.emit(format!("timer:{}", self.option)),
        }
    }
}

/// The firing times that [`TimerFiringOrderDoFn`] saw for each key.
#[derive(State)]
pub struct FiringTimesState {
    pub fired: ValueState<Vec<i64>>,
}

/// One event-time timer for each element, tagged with the element timestamp.
#[derive(Timers)]
pub enum ElementTimer {
    #[timer(event_time)]
    At(String),
}

/// Sets a timer at the timestamp of each element and fails if the timers of a key fire out
/// of timestamp order. Emits the firing times after `expected` firings.
#[derive(Clone)]
pub struct TimerFiringOrderDoFn {
    pub expected: usize,
}

impl StatefulDoFn for TimerFiringOrderDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, Vec<i64>);
    type State = FiringTimesState;
    type Timer = ElementTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let ts = ctx.timestamp();
        ctx.timer(ElementTimer::At(ts.as_millis().to_string()))
            .at(ts)
    }

    fn on_timer(
        &mut self,
        _timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let fire_ts = ctx
            .fire_timestamp()
            .ok_or("missing fire timestamp in on_timer")?
            .as_millis();
        let mut fired = state.fired.read()?.unwrap_or_default();
        if let Some(last) = fired.last()
            && fire_ts < *last
        {
            return Err(format!("key {key}: timer at {fire_ts} fired after {fired:?}").into());
        }
        fired.push(fire_ts);
        if fired.len() == self.expected {
            state.fired.clear()?;
            ctx.emit((key.clone(), fired))
        } else {
            state.fired.write(&fired)
        }
    }
}

/// The outputs of [`TaggedOutputTimestampDoFn`].
#[derive(Outputs)]
pub enum ShiftedOutputs {
    /// The value, at its own second.
    Main(i64),
    /// `tagged:<value>`, 500 ms later.
    Tagged(String),
}

/// Emits each value `v` to the main output at `v` seconds, and to the tagged output 500 ms
/// later. Both times are before the input timestamp.
#[derive(Clone)]
pub struct TaggedOutputTimestampDoFn;

impl DoFn for TaggedOutputTimestampDoFn {
    type In = i64;
    type Out = ShiftedOutputs;

    fn process_element(&mut self, v: i64, ctx: &mut ProcessContext<'_, ShiftedOutputs>) -> Result {
        let at = Instant::from_millis(v * 1_000);
        ctx.output(ShiftedOutputs::Main(v)).at(at).emit()?;
        ctx.output(ShiftedOutputs::Tagged(format!("tagged:{v}")))
            .at(at + Duration::from_millis(500))
            .emit()
    }
}

/// Tracks whether [`EndOfGlobalWindowTimer::EndOfWindow`] ran before window expiration.
#[derive(State)]
pub struct EndOfGlobalWindowState {
    timer_ran: ValueState<bool>,
}

/// The event-time timer of [`EndOfGlobalWindowDoFn`].
#[derive(Timers)]
pub enum EndOfGlobalWindowTimer {
    #[timer(event_time)]
    EndOfWindow,
}

/// Emits the firing checks at the end of the global window: a timer at `max_timestamp` and
/// `on_window_expiration` after it.
#[derive(Clone)]
pub struct EndOfGlobalWindowDoFn;

impl StatefulDoFn for EndOfGlobalWindowDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, bool);
    type State = EndOfGlobalWindowState;
    type Timer = EndOfGlobalWindowTimer;
    const ON_WINDOW_EXPIRATION: bool = true;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(EndOfGlobalWindowTimer::EndOfWindow)
            .at(ctx.window()?.max_timestamp())
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        _key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            EndOfGlobalWindowTimer::EndOfWindow => {
                state.timer_ran.write(&true)?;
                let at_end = ctx.fire_timestamp() == Some(Instant::END_OF_GLOBAL_WINDOW)
                    && ctx.timestamp() == Instant::END_OF_GLOBAL_WINDOW;
                ctx.emit(("timer".to_string(), at_end))
            }
        }
    }

    fn on_window_expiration(
        &mut self,
        _key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let timer_ran = state.timer_ran.read()?.unwrap_or(false);
        let fire_ok = ctx
            .fire_timestamp()
            .is_some_and(|fire| fire >= Instant::END_OF_GLOBAL_WINDOW && fire < Instant::MAX);
        let hold_ok = ctx.timestamp() == Instant::END_OF_GLOBAL_WINDOW;
        ctx.emit(("expiration".to_string(), timer_ran && fire_ok && hold_ok))
    }
}

/// Event-time timer of [`SetAndClearEventTimeTimerDoFn`].
#[derive(Timers)]
pub enum SetAndClearEventTimer {
    #[timer(event_time)]
    Cleared,
}

/// Sets and clears an event-time timer in `process_element`. Emits `fired:<key>` if it fires.
#[derive(Clone)]
pub struct SetAndClearEventTimeTimerDoFn;

impl StatefulDoFn for SetAndClearEventTimeTimerDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = SetAndClearEventTimer;

    fn process_element(
        &mut self,
        key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(SetAndClearEventTimer::Cleared)
            .at(Instant::from_millis(1_000))?;
        ctx.timer(SetAndClearEventTimer::Cleared).clear()?;
        ctx.emit(format!("processed:{key}"))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            SetAndClearEventTimer::Cleared => ctx.emit(format!("fired:{key}")),
        }
    }
}

/// Event-time timers of [`ClearEventTimeTimerInTimerDoFn`].
#[derive(Timers)]
pub enum TwoEventTimers {
    #[timer(event_time)]
    First,
    #[timer(event_time)]
    Second,
}

/// Sets two event-time timers, and clears `Second` when `First` fires.
#[derive(Clone)]
pub struct ClearEventTimeTimerInTimerDoFn;

impl StatefulDoFn for ClearEventTimeTimerInTimerDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = TwoEventTimers;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(TwoEventTimers::First)
            .at(Instant::from_millis(1_000))?;
        ctx.timer(TwoEventTimers::Second)
            .at(Instant::from_millis(2_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            TwoEventTimers::First => {
                ctx.timer(TwoEventTimers::Second).clear()?;
                ctx.emit(format!("{key}:first"))
            }
            TwoEventTimers::Second => ctx.emit(format!("{key}:second")),
        }
    }
}

/// Processing-time timer of [`SetAndClearProcessingTimeTimerDoFn`].
#[derive(Timers)]
pub enum SetAndClearProcessingTimer {
    #[timer(processing_time)]
    Cleared,
}

/// Sets and clears a processing-time timer in `process_element`.
#[derive(Clone)]
pub struct SetAndClearProcessingTimeTimerDoFn;

impl StatefulDoFn for SetAndClearProcessingTimeTimerDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = SetAndClearProcessingTimer;

    fn process_element(
        &mut self,
        key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(SetAndClearProcessingTimer::Cleared)
            .after(Duration::from_secs(5))?;
        ctx.timer(SetAndClearProcessingTimer::Cleared).clear()?;
        ctx.emit(format!("processed:{key}"))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            SetAndClearProcessingTimer::Cleared => ctx.emit(format!("fired:{key}")),
        }
    }
}

/// Processing-time timers of [`ClearProcessingTimeTimerInTimerDoFn`].
#[derive(Timers)]
pub enum TwoProcessingTimers {
    #[timer(processing_time)]
    First,
    #[timer(processing_time)]
    Second,
}

/// Sets two processing-time timers, and clears `Second` when `First` fires.
#[derive(Clone)]
pub struct ClearProcessingTimeTimerInTimerDoFn;

impl StatefulDoFn for ClearProcessingTimeTimerInTimerDoFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = TwoProcessingTimers;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(TwoProcessingTimers::First)
            .after(Duration::from_secs(5))?;
        ctx.timer(TwoProcessingTimers::Second)
            .after(Duration::from_secs(10))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            TwoProcessingTimers::First => {
                ctx.timer(TwoProcessingTimers::Second).clear()?;
                ctx.emit(format!("{key}:first"))
            }
            TwoProcessingTimers::Second => ctx.emit(format!("{key}:second")),
        }
    }
}

/// Event-time timer of [`UpstreamHoldDoFn`].
#[derive(Timers)]
pub enum UpstreamHoldTimer {
    #[timer(event_time)]
    Release,
}

/// Holds the output watermark at 5 s on value `1` until its 20 s timer emits `3`.
#[derive(Clone)]
pub struct UpstreamHoldDoFn;

impl StatefulDoFn for UpstreamHoldDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = ();
    type Timer = UpstreamHoldTimer;

    fn process_element(
        &mut self,
        key: &Self::Key,
        value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        if value == 1 {
            ctx.timer(UpstreamHoldTimer::Release)
                .hold(Instant::from_millis(5_000))
                .at(Instant::from_millis(20_000))?;
        }
        ctx.emit((key.clone(), value))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            UpstreamHoldTimer::Release => ctx.emit((key.clone(), 3)),
        }
    }
}

/// Buffered values of [`DownstreamCollectTimerDoFn`].
#[derive(State)]
pub struct DownstreamHoldState {
    pub values: BagState<i64>,
}

/// Event-time timer of [`DownstreamCollectTimerDoFn`].
#[derive(Timers)]
pub enum DownstreamFlushTimer {
    #[timer(event_time)]
    Flush,
}

/// Buffers values per key and flushes them sorted when its 10 s event-time timer fires.
#[derive(Clone)]
pub struct DownstreamCollectTimerDoFn;

impl StatefulDoFn for DownstreamCollectTimerDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, Vec<i64>);
    type State = DownstreamHoldState;
    type Timer = DownstreamFlushTimer;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.values.append(&value)?;
        ctx.timer(DownstreamFlushTimer::Flush)
            .at(Instant::from_millis(10_000))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            DownstreamFlushTimer::Flush => {
                let mut values = state.values.read()?;
                state.values.clear()?;
                values.sort_unstable();
                ctx.emit((key.clone(), values))
            }
        }
    }
}
