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

//! The context of a [`StatefulDoFn`]: a [`ProcessContext`] bound to the current key, plus
//! timers.

use std::ops::{Deref, DerefMut};
use std::time::Duration;

use super::ProcessContext;
use crate::Instant;
use crate::transforms::dofn::input::window_of;
use crate::transforms::dofn::stateful::{StatefulDoFn, TimerFamilies};
use crate::transforms::dofn::timer::TimeDomain;
use crate::windowing::BoundedWindow;

/// The context of a [`StatefulDoFn`] for one element or one fired timer.
///
/// It dereferences to [`ProcessContext`] for output, side inputs and timestamps. State and
/// timers are scoped to the key and the one [`window`](Self::window) of the current element.
/// The time domain of each timer comes from its variant in [`StatefulDoFn::Timer`].
pub struct StatefulContext<'a, F: StatefulDoFn> {
    inner: ProcessContext<'a, F::Out>,
    key: Vec<u8>,
}

impl<'a, F: StatefulDoFn> StatefulContext<'a, F> {
    /// Creates a stateful context bound to `inner` and `key`.
    pub(crate) fn new(inner: ProcessContext<'a, F::Out>, key: Vec<u8>) -> Self {
        Self { inner, key }
    }

    /// The window of the current element or fired timer.
    pub fn window(&self) -> crate::Result<BoundedWindow> {
        window_of(self.inner.header())
    }

    /// Starts setting or clearing `timer` for the current key and window.
    ///
    /// ```ignore
    /// ctx.timer(Flush::EndOfWindow).at(ctx.window()?.max_timestamp())?;
    /// ctx.timer(Poll::Next).after(Duration::from_secs(10))?;
    /// ```
    pub fn timer(&self, timer: F::Timer) -> Timer<'_, 'a, F> {
        Timer {
            ctx: self,
            timer,
            hold: None,
        }
    }

    fn collector(&self) -> crate::Result<&crate::transforms::dofn::timer::TimerCollector> {
        self.inner
            .timer_collector
            .map(AsRef::as_ref)
            .ok_or_else(no_collector)
    }
}

/// A timer of a [`StatefulContext`] to set or clear.
///
/// The firing time is in the domain of the timer: event time, or processing time. By default
/// the output watermark is held at the firing time of an event-time timer, and at the
/// timestamp of the current element for a processing-time timer.
#[must_use = "a timer does nothing until `at`, `after` or `clear` is called"]
pub struct Timer<'c, 'a, F: StatefulDoFn> {
    ctx: &'c StatefulContext<'a, F>,
    timer: F::Timer,
    hold: Option<Instant>,
}

impl<F: StatefulDoFn> Timer<'_, '_, F> {
    /// Holds the output watermark at `hold` until the timer fires, so outputs of `on_timer`
    /// at `hold` are not late.
    pub fn hold(mut self, hold: Instant) -> Self {
        self.hold = Some(hold);
        self
    }

    /// Fires the timer at `time`.
    pub fn at(self, time: Instant) -> crate::Result {
        let element = self.ctx.inner.timestamp();
        let hold = self.hold.unwrap_or(match self.timer.time_domain() {
            TimeDomain::EventTime => time,
            TimeDomain::ProcessingTime => element,
        });
        // The TimerCollector buffers an owned TimerRecord for each mutation.
        // The harness drains these records after the callback returns.
        self.ctx.collector()?.set_with_context(
            self.timer.family(),
            self.timer.tag(),
            Some(self.ctx.key.clone()),
            Some(self.ctx.inner.window_bytes().to_vec()),
            time,
            hold,
        );
        Ok(())
    }

    /// Fires the timer `offset` after the current time of its domain: the timestamp of the
    /// current element for event time, the system clock for processing time.
    pub fn after(self, offset: Duration) -> crate::Result {
        let now = match self.timer.time_domain() {
            TimeDomain::EventTime => self.ctx.inner.timestamp(),
            TimeDomain::ProcessingTime => Instant::now(),
        };
        self.at(now + offset)
    }

    /// Clears the timer.
    pub fn clear(self) -> crate::Result {
        // The TimerCollector buffers an owned TimerRecord for each mutation.
        // The harness drains these records after the callback returns.
        self.ctx.collector()?.clear_with_context(
            self.timer.family(),
            self.timer.tag(),
            Some(self.ctx.key.clone()),
            Some(self.ctx.inner.window_bytes().to_vec()),
        );
        Ok(())
    }
}

fn no_collector() -> crate::Error {
    "No active TimerCollector for the stateful DoFn"
        .to_string()
        .into()
}

impl<'a, F: StatefulDoFn> Deref for StatefulContext<'a, F> {
    type Target = ProcessContext<'a, F::Out>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<F: StatefulDoFn> DerefMut for StatefulContext<'_, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
