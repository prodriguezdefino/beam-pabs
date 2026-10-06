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

//! The check of `ctx.output(v).at(ts)` against the allowed timestamp skew of the function.

use std::time::Duration;

use beam::coders::{DefaultCoder, PaneInfo, TimerRecord, WindowedHeader};
use beam::internals::{ElementSink, HandlerContext, IntoParDoFn};
use beam::prelude::*;
use beam::transforms::{BatchedDoFn, BatchedDoFnAdapter, ProcessContext};

const ELEMENT_MILLIS: i64 = 10_000;
const ELEMENT: Instant = Instant::from_millis(ELEMENT_MILLIS);
const SECOND: Duration = Duration::from_secs(1);

/// Records the timestamp of each output.
#[derive(Default)]
struct Timestamps(Vec<i64>);

impl ElementSink for Timestamps {
    fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
        Err("an output has no header".to_string())
    }

    fn push_windowed(
        &mut self,
        header: &WindowedHeader,
        _element: Vec<u8>,
    ) -> std::result::Result<(), String> {
        self.0.push(header.timestamp_millis());
        Ok(())
    }
}

/// Emits each target timestamp at that timestamp. Emits `None` with no `.at`.
#[derive(Clone)]
struct EmitAt {
    skew: Duration,
}

impl DoFn for EmitAt {
    type In = Option<i64>;
    type Out = i64;

    fn process_element(&mut self, at: Option<i64>, ctx: &mut ProcessContext<'_, i64>) -> Result {
        match at {
            Some(millis) => ctx.output(millis).at(Instant::from_millis(millis)).emit(),
            None => ctx
                .output(0)
                .with_pane(PaneInfo::ON_TIME_AND_ONLY_FIRING)
                .emit(),
        }
    }

    fn allowed_timestamp_skew(&self) -> Duration {
        self.skew
    }
}

/// Processes `at` with `skew` for an element at `ELEMENT`. Returns the output timestamps.
fn emit_at(at: Option<Instant>, skew: Duration) -> std::result::Result<Vec<i64>, String> {
    let header = WindowedHeader::global(ELEMENT_MILLIS, PaneInfo::NO_FIRING);
    let element = at.map(Instant::as_millis).encode().expect("encode");
    let mut handler = EmitAt { skew }.into_handler().instantiate();
    let mut sink = Timestamps::default();
    let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
    handler.process(&element, &mut ctx)?;
    Ok(sink.0)
}

#[test]
fn an_output_at_or_after_the_input_timestamp_passes_with_zero_skew() {
    for at in [ELEMENT, ELEMENT + SECOND, Instant::MAX] {
        assert_eq!(emit_at(Some(at), Duration::ZERO), Ok(vec![at.as_millis()]));
    }
}

#[test]
fn an_output_before_the_input_timestamp_fails_with_zero_skew() {
    let at = ELEMENT - Duration::from_millis(1);
    let error = emit_at(Some(at), Duration::ZERO).expect_err("the output is too early");
    for part in [
        format!("Cannot output with timestamp {at}."),
        format!("current input or timer ({ELEMENT})"),
        "allowed timestamp skew (0ns)".to_string(),
        "`allowed_timestamp_skew`".to_string(),
    ] {
        assert!(error.contains(&part), "'{part}' is not in: {error}");
    }
}

#[test]
fn an_output_before_the_input_timestamp_passes_within_the_skew() {
    let at = ELEMENT - SECOND;
    assert_eq!(emit_at(Some(at), SECOND), Ok(vec![at.as_millis()]));
}

#[test]
fn an_output_before_the_input_timestamp_minus_the_skew_fails() {
    let at = ELEMENT - SECOND - Duration::from_millis(1);
    let error = emit_at(Some(at), SECOND).expect_err("the output is too early");
    assert!(error.contains("allowed timestamp skew (1s)"), "{error}");
}

#[test]
fn the_maximum_skew_permits_the_minimum_timestamp() {
    assert_eq!(
        emit_at(Some(Instant::MIN), Duration::MAX),
        Ok(vec![Instant::MIN.as_millis()])
    );
}

#[test]
fn an_output_after_the_maximum_timestamp_fails_with_each_skew() {
    let at = Instant::from_millis(Instant::MAX.as_millis() + 1);
    for skew in [Duration::ZERO, Duration::MAX] {
        let error = emit_at(Some(at), skew).expect_err("the output is too late");
        assert!(
            error.contains(&format!("not later than {}", Instant::MAX)),
            "{error}"
        );
    }
}

#[test]
fn an_output_with_no_timestamp_keeps_the_input_timestamp() {
    assert_eq!(emit_at(None, Duration::ZERO), Ok(vec![ELEMENT_MILLIS]));
}

#[test]
fn an_output_with_no_current_element_checks_the_instant_bounds() {
    let run = |millis| {
        let element = Some(millis).encode().expect("encode");
        let mut handler = EmitAt {
            skew: Duration::ZERO,
        }
        .into_handler()
        .instantiate();
        let mut sink = Timestamps::default();
        let mut ctx = HandlerContext::new(&mut sink);
        handler.process(&element, &mut ctx).map(|()| sink.0)
    };
    assert_eq!(run(-1), Ok(vec![-1]));
    let too_early = run(Instant::MIN.as_millis() - 1).expect_err("before Instant::MIN");
    assert!(
        too_early.contains(&format!("not earlier than {}", Instant::MIN)),
        "{too_early}"
    );
}

#[derive(Timers)]
enum Flush {
    #[timer(event_time, id = "flush")]
    Now,
}

/// Emits `back` before the current timestamp, for an element and for a timer.
#[derive(Clone)]
struct EmitBefore {
    back: Duration,
    skew: Duration,
}

impl StatefulDoFn for EmitBefore {
    type Key = String;
    type Value = i64;
    type Out = i64;
    type State = ();
    type Timer = Flush;

    fn process_element(
        &mut self,
        _key: &String,
        value: i64,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let at = ctx.timestamp() - self.back;
        ctx.output(value).at(at).emit()
    }

    fn on_timer(
        &mut self,
        _timer: Flush,
        _key: &String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let at = ctx.timestamp() - self.back;
        ctx.output(0).at(at).emit()
    }

    fn allowed_timestamp_skew(&self) -> Duration {
        self.skew
    }
}

const FIRE_MILLIS: i64 = 20_000;
const HOLD_MILLIS: i64 = 12_000;

/// Fires a timer with a hold at `HOLD_MILLIS`. Returns the output timestamps.
fn fire(func: EmitBefore) -> std::result::Result<Vec<i64>, String> {
    let fired = TimerRecord::new_set(
        "k".to_string().encode().expect("encode"),
        "",
        Instant::from_millis(FIRE_MILLIS),
        Instant::from_millis(HOLD_MILLIS),
    );
    let mut handler = func.into_handler().instantiate();
    let mut sink = Timestamps::default();
    let mut ctx = HandlerContext::new(&mut sink);
    handler.on_timer("flush", &fired, &mut ctx)?;
    Ok(sink.0)
}

#[test]
fn a_timer_output_before_the_hold_fails_with_zero_skew() {
    let func = EmitBefore {
        back: SECOND,
        skew: Duration::ZERO,
    };
    let error = fire(func).expect_err("the output is before the hold");
    let hold = Instant::from_millis(HOLD_MILLIS);
    assert!(
        error.contains(&format!("current input or timer ({hold})")),
        "{error}"
    );
}

#[test]
fn a_timer_output_before_the_hold_passes_within_the_skew() {
    let func = EmitBefore {
        back: SECOND,
        skew: SECOND,
    };
    assert_eq!(fire(func), Ok(vec![HOLD_MILLIS - 1_000]));
}

#[test]
fn a_timer_output_at_the_hold_passes_with_zero_skew() {
    let func = EmitBefore {
        back: Duration::ZERO,
        skew: Duration::ZERO,
    };
    assert_eq!(fire(func), Ok(vec![HOLD_MILLIS]));
}

/// Processes one element at `ELEMENT` with a stateful function.
fn process_stateful(func: EmitBefore) -> std::result::Result<Vec<i64>, String> {
    let header = WindowedHeader::global(ELEMENT_MILLIS, PaneInfo::NO_FIRING);
    let element = ("k".to_string(), 1_i64).encode().expect("encode");
    let mut handler = func.into_handler().instantiate();
    let mut sink = Timestamps::default();
    let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
    handler.process(&element, &mut ctx)?;
    Ok(sink.0)
}

#[test]
fn a_stateful_function_has_the_same_check_for_an_element() {
    let early = |skew| EmitBefore { back: SECOND, skew };
    let error = process_stateful(early(Duration::ZERO)).expect_err("the output is too early");
    assert!(error.contains("allowed timestamp skew (0ns)"), "{error}");
    assert_eq!(
        process_stateful(early(SECOND)),
        Ok(vec![ELEMENT_MILLIS - 1_000])
    );
}

/// A batched function with a skew of one second.
#[derive(Clone)]
struct SkewedBatches;

impl BatchedDoFn for SkewedBatches {
    type InBatch = Vec<i64>;
    type OutBatch = Vec<i64>;

    fn process_batch(&mut self, batch: Vec<i64>, ctx: &mut ProcessContext<'_, Vec<i64>>) -> Result {
        ctx.emit(batch)
    }

    fn allowed_timestamp_skew(&self) -> Duration {
        SECOND
    }
}

#[test]
fn a_batched_function_gives_its_skew_to_its_adapter() {
    assert_eq!(
        BatchedDoFnAdapter::new(SkewedBatches).allowed_timestamp_skew(),
        SECOND
    );
}

/// Sets a timer whose explicit hold is `hold`, firing at `Instant::END_OF_GLOBAL_WINDOW`.
#[derive(Clone)]
struct HoldAt {
    hold: Instant,
    skew: Duration,
}

impl StatefulDoFn for HoldAt {
    type Key = String;
    type Value = i64;
    type Out = i64;
    type State = ();
    type Timer = Flush;

    fn process_element(
        &mut self,
        _key: &String,
        _value: i64,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(Flush::Now)
            .hold(self.hold)
            .at(Instant::END_OF_GLOBAL_WINDOW)
    }

    fn on_timer(
        &mut self,
        _timer: Flush,
        _key: &String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(Flush::Now)
            .hold(self.hold)
            .at(Instant::END_OF_GLOBAL_WINDOW)
    }

    fn allowed_timestamp_skew(&self) -> Duration {
        self.skew
    }
}

fn process_hold(func: HoldAt) -> std::result::Result<(), String> {
    let collector = std::sync::Arc::new(beam::internals::TimerCollector::new());
    let header = WindowedHeader::global(ELEMENT_MILLIS, PaneInfo::NO_FIRING);
    let element = ("k".to_string(), 1_i64).encode().expect("encode");
    let mut handler = func.into_handler().instantiate();
    let mut sink = Timestamps::default();
    let mut ctx = HandlerContext::new(&mut sink)
        .with_header(&header)
        .with_timer_collector(Some(&collector));
    handler.process(&element, &mut ctx)
}

fn on_timer_hold(func: HoldAt) -> std::result::Result<(), String> {
    let collector = std::sync::Arc::new(beam::internals::TimerCollector::new());
    let fired = TimerRecord::new_set(
        "k".to_string().encode().expect("encode"),
        "",
        Instant::from_millis(FIRE_MILLIS),
        Instant::from_millis(HOLD_MILLIS),
    );
    let mut handler = func.into_handler().instantiate();
    let mut sink = Timestamps::default();
    let mut ctx = HandlerContext::new(&mut sink).with_timer_collector(Some(&collector));
    handler.on_timer("flush", &fired, &mut ctx)
}

#[test]
fn timer_hold_is_checked_against_the_allowed_skew_in_process_element_and_on_timer() {
    let early_elem = ELEMENT - Duration::from_millis(1);
    let elem_err = process_hold(HoldAt {
        hold: early_elem,
        skew: Duration::ZERO,
    })
    .expect_err("hold before element timestamp");
    for part in [
        format!("Cannot set a timer with output timestamp {early_elem}."),
        format!("current input or timer ({ELEMENT})"),
        "allowed timestamp skew (0ns)".to_string(),
        "`allowed_timestamp_skew`".to_string(),
    ] {
        assert!(elem_err.contains(&part), "'{part}' is not in: {elem_err}");
    }
    process_hold(HoldAt {
        hold: ELEMENT - SECOND,
        skew: SECOND,
    })
    .expect("hold within skew passes");
    process_hold(HoldAt {
        hold: Instant::MIN,
        skew: Duration::MAX,
    })
    .expect("Duration::MAX permits Instant::MIN hold");

    let early_timer = Instant::from_millis(HOLD_MILLIS) - Duration::from_millis(1);
    let timer_err = on_timer_hold(HoldAt {
        hold: early_timer,
        skew: Duration::ZERO,
    })
    .expect_err("hold before timer hold timestamp");
    assert!(
        timer_err.contains(&format!(
            "current input or timer ({})",
            Instant::from_millis(HOLD_MILLIS)
        )),
        "{timer_err}"
    );
    on_timer_hold(HoldAt {
        hold: Instant::from_millis(HOLD_MILLIS) - SECOND,
        skew: SECOND,
    })
    .expect("on_timer hold within skew passes");
}
