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

//! DoFns and stateful DoFns that read side inputs.

use std::time::Duration;

use beam::prelude::*;

/// Multiplies the points of the users in `vip_users_view` by `multiplier_view`.
#[derive(Clone, serde::Serialize, serde::Deserialize, Register)]
pub struct EnrichOrderDoFn {
    pub multiplier_view: SingletonView<i64>,
    pub vip_users_view: IterView<String>,
}

impl DoFn for EnrichOrderDoFn {
    type In = (String, i64);
    type Out = String;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.multiplier_view, &self.vip_users_view]
    }

    fn process_element(
        &mut self,
        (user, points): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let mult = ctx.side_input(&self.multiplier_view)?;
        let vips = ctx.side_input(&self.vip_users_view)?.to_vec()?;
        let total = if vips.contains(&user) {
            points * mult
        } else {
            points
        };
        ctx.emit(format!("{user}={total}"))
    }
}

/// The outputs of [`TaggedSideInputsDoFn`]: one PCollection per variant.
#[derive(Outputs)]
pub enum SideTagged {
    Main(String),
    Extra(i64),
}

/// Declares three singleton views, reads `first` and `second` for each element, and
/// writes only to the main output.
#[derive(Clone)]
pub struct TaggedSideInputsDoFn {
    pub first: SingletonView<i64>,
    pub unread: SingletonView<i64>,
    pub second: SingletonView<i64>,
}

impl DoFn for TaggedSideInputsDoFn {
    type In = i64;
    type Out = SideTagged;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.first, &self.unread, &self.second]
    }

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let first = ctx.side_input(&self.first)?;
        let second = ctx.side_input(&self.second)?;
        ctx.emit(SideTagged::Main(format!("{element}:{first}+{second}")))
    }
}

/// The buffer of [`BufferWithSideListDoFn`] and [`BufferSideSingletonDoFn`].
#[derive(State)]
pub struct SideInputBuffer {
    pub buffer: BagState<i64>,
}

/// Buffers the values of each key. At four values, emits them sorted, then the side list
/// sorted.
#[derive(Clone)]
pub struct BufferWithSideListDoFn {
    pub side: IterView<i64>,
}

impl StatefulDoFn for BufferWithSideListDoFn {
    type Key = String;
    type Value = i64;
    type Out = Vec<i64>;
    type State = SideInputBuffer;
    type Timer = NoTimers;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.side]
    }

    fn process_element(
        &mut self,
        _key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.buffer.append(&value)?;
        let mut buffered = state.buffer.read()?;
        if buffered.len() < 4 {
            return Ok(());
        }
        buffered.sort_unstable();
        ctx.emit(buffered)?;
        let mut side = ctx.side_input(&self.side)?.to_vec()?;
        side.sort_unstable();
        ctx.emit(side)
    }
}

/// Appends `value + side` to the bag state of the key and emits the sum after two elements.
#[derive(Clone)]
pub struct BufferSideSingletonDoFn {
    pub side: SingletonView<i64>,
}

impl StatefulDoFn for BufferSideSingletonDoFn {
    type Key = i64;
    type Value = i64;
    type Out = i64;
    type State = SideInputBuffer;
    type Timer = NoTimers;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.side]
    }

    fn process_element(
        &mut self,
        _key: &Self::Key,
        value: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let side = ctx.side_input(&self.side)?;
        state.buffer.append(&(value + side))?;
        let buffered = state.buffer.read()?;
        if buffered.len() < 2 {
            return Ok(());
        }
        ctx.emit(buffered.into_iter().sum())
    }
}

/// Event-time timer of [`SideInputInTimerDoFn`].
#[derive(Timers)]
pub enum SideInputTimer {
    #[timer(event_time)]
    Fire,
}

/// Emits the side singleton from `process_element`, from a timer 1 ms after the element,
/// and from `on_window_expiration`.
#[derive(Clone)]
pub struct SideInputInTimerDoFn {
    pub side: SingletonView<String>,
}

impl StatefulDoFn for SideInputInTimerDoFn {
    type Key = String;
    type Value = String;
    type Out = String;
    type State = ();
    type Timer = SideInputTimer;
    const ON_WINDOW_EXPIRATION: bool = true;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.side]
    }

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(SideInputTimer::Fire)
            .at(ctx.timestamp() + Duration::from_millis(1))?;
        let side = ctx.side_input(&self.side)?;
        ctx.emit(side)
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        _key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            SideInputTimer::Fire => {
                let side = ctx.side_input(&self.side)?;
                ctx.emit(side)
            }
        }
    }

    fn on_window_expiration(
        &mut self,
        _key: &Self::Key,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let side = ctx.side_input(&self.side)?;
        ctx.emit(side)
    }
}
