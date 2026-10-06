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
use crate::coders::{TimerAction, TimerRecord, WindowedHeader};
use crate::transforms::dofn::input::window_of;
use crate::transforms::dofn::stateful::{StatefulDoFn, TimerFamilies};
use crate::transforms::dofn::timer::TimeDomain;
use crate::windowing::BoundedWindow;

/// Fn API sentinel (`MAX_TIMESTAMP_MILLIS + 1`) that tells the runner not to hold the watermark.
const NO_HOLD_TIMESTAMP: Instant = Instant::from_millis(Instant::MAX.as_millis() + 1);

/// Watermark hold setting for a [`Timer`].
#[derive(Clone, Copy)]
enum Hold {
    /// Firing time for event time, or current timestamp for processing time.
    Default,
    /// Explicit watermark hold timestamp.
    At(Instant),
    /// No watermark hold.
    None,
}

/// The context of a [`StatefulDoFn`] for one element or one fired timer.
///
/// It dereferences to [`ProcessContext`] for output, side inputs and timestamps. State and
/// timers are scoped to the key and the one [`window`](Self::window) of the current element.
/// The time domain of each timer comes from its variant in [`StatefulDoFn::Timer`].
pub struct StatefulContext<'a, F: StatefulDoFn> {
    inner: ProcessContext<'a, F::Out>,
    key: Vec<u8>,
    fired: Option<(TimeDomain, Instant)>,
}

impl<'a, F: StatefulDoFn> StatefulContext<'a, F> {
    /// Creates a stateful context bound to `inner` and `key`.
    pub(crate) fn new(inner: ProcessContext<'a, F::Out>, key: Vec<u8>) -> Self {
        Self {
            inner,
            key,
            fired: None,
        }
    }

    /// Binds the context to a timer of `domain` that fired at `fire`.
    pub(crate) fn fired(self, domain: TimeDomain, fire: Instant) -> Self {
        Self {
            fired: Some((domain, fire)),
            ..self
        }
    }

    /// The firing time of the timer in `on_timer`, in the domain of the timer. `None` in
    /// `process_element`.
    pub fn fire_timestamp(&self) -> Option<Instant> {
        self.fired.map(|(_, fire)| fire)
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
            hold: Hold::Default,
            period: Duration::ZERO,
        }
    }

    /// The current event time: the firing time of an event-time timer, else the timestamp.
    fn event_time(&self) -> Instant {
        match self.fired {
            Some((TimeDomain::EventTime, fire)) => fire,
            Some((TimeDomain::ProcessingTime, _)) | None => self.inner.timestamp(),
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
/// timestamp of the current element for a processing-time timer. Outputs of `on_timer` carry
/// the hold as their timestamp.
#[must_use = "a timer does nothing until `at`, `after` or `clear` is called"]
pub struct Timer<'c, 'a, F: StatefulDoFn> {
    ctx: &'c StatefulContext<'a, F>,
    timer: F::Timer,
    hold: Hold,
    period: Duration,
}

impl<F: StatefulDoFn> Timer<'_, '_, F> {
    /// Holds the output watermark at `hold` until the timer fires.
    ///
    /// Outputs of `on_timer` carry `hold` as their timestamp, so they are not late. Setting
    /// the timer fails if `hold` is before the current timestamp minus
    /// [`StatefulDoFn::allowed_timestamp_skew`], after an event-time firing time, or after
    /// window expiration.
    pub fn hold(mut self, hold: Instant) -> Self {
        self.hold = Hold::At(hold);
        self
    }

    /// Sets no output watermark hold on the timer.
    ///
    /// Emitting from `on_timer` with the default timestamp fails. Pass `.at(ts)` with enough
    /// [`allowed_timestamp_skew`](StatefulDoFn::allowed_timestamp_skew) when an output is
    /// needed.
    pub fn without_hold(mut self) -> Self {
        self.hold = Hold::None;
        self
    }

    /// Snaps the target time of [`after`](Self::after) to the next boundary of `period`.
    pub fn with_align(mut self, period: Duration) -> Self {
        self.period = period;
        self
    }

    /// Fires the timer at `time`.
    ///
    /// Fails for an event-time timer when `time` is after the window expiration.
    pub fn at(self, time: Instant) -> crate::Result {
        let window = self.ctx.window()?;
        let expiry = window_expiration(window, self.ctx.inner.allowed_lateness);
        match self.timer.time_domain() {
            TimeDomain::EventTime if time > expiry => {
                return Err(crate::Error::msg(format!(
                    "Cannot set an event-time timer for {time}: it is after the expiration \
                     of window {window} ({expiry})"
                )));
            }
            TimeDomain::EventTime | TimeDomain::ProcessingTime
                if !(Instant::MIN..=Instant::MAX).contains(&time) =>
            {
                return Err(crate::Error::msg(format!(
                    "Cannot set a timer for {time}: firing timestamp must be in {}..={}",
                    Instant::MIN,
                    Instant::MAX
                )));
            }
            TimeDomain::EventTime | TimeDomain::ProcessingTime => {}
        }
        self.schedule(time, window, expiry)
    }

    /// Fires the timer `offset` after the current time of its domain.
    ///
    /// For event time, that is the firing time of an event-time timer in `on_timer`, else the
    /// current timestamp. For processing time, it is the system clock. With
    /// [`with_align`](Self::with_align), snaps the target to the `period` grid. Event-time
    /// targets past window expiration clamp to window expiration.
    pub fn after(self, offset: Duration) -> crate::Result {
        let window = self.ctx.window()?;
        let expiry = window_expiration(window, self.ctx.inner.allowed_lateness);
        let now = match self.timer.time_domain() {
            TimeDomain::EventTime => self.ctx.event_time(),
            TimeDomain::ProcessingTime => Instant::now(),
        };
        let target = aligned_target(now, offset, self.period);
        let fire = match self.timer.time_domain() {
            TimeDomain::EventTime => target.min(expiry),
            TimeDomain::ProcessingTime => target,
        };
        self.schedule(fire, window, expiry)
    }

    /// Clears the timer.
    pub fn clear(self) -> crate::Result {
        self.push(TimerAction::Clear)
    }

    fn schedule(
        self,
        fire_timestamp: Instant,
        window: BoundedWindow,
        expiry: Instant,
    ) -> crate::Result {
        let domain = self.timer.time_domain();
        let hold_timestamp = match self.hold {
            Hold::None => NO_HOLD_TIMESTAMP,
            Hold::At(hold) => {
                check_hold_skew(
                    hold,
                    self.ctx.inner.header(),
                    self.ctx.inner.allowed_timestamp_skew,
                )?;
                check_hold_upper(domain, hold, fire_timestamp, window, expiry)?;
                hold
            }
            Hold::Default => {
                let hold = match domain {
                    TimeDomain::EventTime => fire_timestamp,
                    TimeDomain::ProcessingTime => self.ctx.inner.timestamp(),
                };
                check_hold_upper(domain, hold, fire_timestamp, window, expiry)?;
                hold
            }
        };
        let pane = self.ctx.inner.pane();
        self.push(TimerAction::Set {
            fire_timestamp,
            hold_timestamp,
            pane,
        })
    }

    /// Records `action` for this timer, its key and its window. The harness sends it after the
    /// callback returns.
    fn push(self, action: TimerAction) -> crate::Result {
        let record = TimerRecord {
            user_key: self.ctx.key.clone(),
            dynamic_tag: self.timer.tag().to_string(),
            windows: vec![self.ctx.inner.window_bytes().to_vec()],
            action,
        };
        self.ctx
            .collector()?
            .push(self.ctx.inner.transform_id, self.timer.family(), record);
        Ok(())
    }
}

/// Garbage-collection timestamp of `window`, capped at [`Instant::END_OF_GLOBAL_WINDOW`].
fn window_expiration(window: BoundedWindow, allowed_lateness: Duration) -> Instant {
    (window.max_timestamp() + allowed_lateness).min(Instant::END_OF_GLOBAL_WINDOW)
}

/// Computes the relative firing target from `now`, `offset` and alignment `period`.
fn aligned_target(now: Instant, offset: Duration, period: Duration) -> Instant {
    let period_ms = i128::try_from(period.as_millis()).unwrap_or(i128::MAX);
    if period_ms == 0 {
        return now + offset;
    }
    let now_ms = i128::from(now.as_millis());
    let offset_ms = i128::try_from(offset.as_millis()).unwrap_or(i128::MAX);
    let rem = now_ms.saturating_add(offset_ms) % period_ms;
    let target_ms = if rem == 0 {
        now_ms
    } else {
        now_ms.saturating_add(period_ms).saturating_sub(rem)
    };
    let clamped = target_ms.clamp(
        i128::from(Instant::MIN.as_millis()),
        i128::from(Instant::MAX.as_millis()),
    ) as i64;
    Instant::from_millis(clamped)
}

/// Fails if `hold` is before the current timestamp minus `skew`, or outside [`Instant`] bounds.
fn check_hold_skew(hold: Instant, current: &WindowedHeader, skew: Duration) -> crate::Result {
    let input = (!current.is_empty()).then(|| Instant::from_millis(current.timestamp_millis()));
    let min = input.map_or(Instant::MIN, |input| input - skew);
    if (min..=Instant::MAX).contains(&hold) {
        return Ok(());
    }
    let input = input.map_or_else(|| "none".to_string(), |input| input.to_string());
    Err(crate::Error::msg(format!(
        "Cannot set a timer with output timestamp {hold}. A timer hold must not be earlier \
         than the timestamp of the current input or timer ({input}) minus the allowed \
         timestamp skew ({skew:?}), not earlier than {}, and not later than {}. To raise the \
         skew, implement `allowed_timestamp_skew` on the function.",
        Instant::MIN,
        Instant::MAX
    )))
}

/// Fails if `hold` exceeds the firing time (event time) or window expiration.
fn check_hold_upper(
    domain: TimeDomain,
    hold: Instant,
    fire: Instant,
    window: BoundedWindow,
    expiry: Instant,
) -> crate::Result {
    match domain {
        TimeDomain::EventTime if hold > fire => Err(crate::Error::msg(format!(
            "Cannot set an event-time timer with output timestamp {hold} after its firing \
             timestamp {fire}"
        ))),
        TimeDomain::EventTime | TimeDomain::ProcessingTime if hold > expiry => {
            let kind = match domain {
                TimeDomain::EventTime => "an event-time",
                TimeDomain::ProcessingTime => "a processing-time",
            };
            Err(crate::Error::msg(format!(
                "Cannot set {kind} timer with output timestamp {hold} after the expiration \
                 of window {window} ({expiry})"
            )))
        }
        TimeDomain::EventTime | TimeDomain::ProcessingTime => Ok(()),
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
