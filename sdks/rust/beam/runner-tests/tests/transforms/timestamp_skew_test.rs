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

//! `WithTimestamps` and the allowed timestamp skew of output timestamps, on Prism.
//!
//! Java: `WithTimestampsTest` and `ParDoTest.TimestampTests`.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam::transforms::{ProcessContext, WithTimestamps};
use beam_runner_tests::prism_runner_from_env;

const YEAR_2000_MILLIS: i64 = 946_684_800_000;

async fn run(p: TestPipeline) {
    p.run_with(&prism_runner_from_env())
        .await
        .expect("pipeline and assertions pass");
}

/// Runs `p`, which must fail, and returns the error text.
async fn failure(p: Pipeline) -> String {
    let error = p
        .run_with_runner(&prism_runner_from_env())
        .await
        .expect_err("an output before the allowed timestamp must fail the pipeline");
    format!("{error:?}")
}

fn millis_values() -> Vec<i64> {
    vec![1_234, 0, i64::from(i32::MAX), YEAR_2000_MILLIS]
}

fn at_own_millis(ms: &i64) -> Instant {
    Instant::from_millis(*ms)
}

fn one_second_before(ms: &i64) -> Instant {
    Instant::from_millis(*ms) - Duration::from_secs(1)
}

/// Pairs each element with its timestamp in milliseconds.
fn with_millis(values: &PCollection<i64>, name: &str) -> PCollection<(i64, i64)> {
    values.par_do_fn(name, |value: i64, ctx| {
        ctx.emit((value, ctx.timestamp().as_millis()))
    })
}

/// Java: `WithTimestampsTest.withTimestampsShouldApplyTimestamps`.
#[tokio::test]
async fn with_timestamps_applies_timestamps() {
    let p = TestPipeline::new();
    let timestamped = p
        .apply("Create", Create::new(millis_values()))
        .with_timestamps("WithTimestamps", at_own_millis);

    passert::that("AssertValues", &timestamped).contains_in_any_order(millis_values());
    passert::that(
        "AssertTimestamps",
        &with_millis(&timestamped, "ReifyTimestamps"),
    )
    .contains_in_any_order(millis_values().into_iter().map(|ms| (ms, ms)));
    run(p).await;
}

/// Java: `WithTimestampsTest.withTimestampsBackwardsInTimeShouldThrow`.
#[tokio::test]
async fn with_timestamps_backwards_in_time_fails() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(millis_values()))
        .with_timestamps("WithTimestamps", at_own_millis)
        .with_timestamps("AddSkew", one_second_before);

    let message = failure(p).await;
    assert!(
        message.contains("must not be earlier than the timestamp of the current input"),
        "{message}"
    );
}

/// Java:
/// `WithTimestampsTest.withTimestampsBackwardsInTimeAndWithAllowedTimestampSkewShouldSucceed`.
#[tokio::test]
async fn with_timestamps_backwards_in_time_with_allowed_skew_succeeds() {
    let p = TestPipeline::new();
    let timestamped = p
        .apply("Create", Create::new(millis_values()))
        .with_timestamps("FirstTimestamp", at_own_millis)
        .apply(
            "WithSkew",
            WithTimestamps::new(one_second_before)
                .with_allowed_timestamp_skew(Duration::from_millis(1_100)),
        );

    passert::that("AssertValues", &timestamped).contains_in_any_order(millis_values());
    passert::that(
        "AssertTimestamps",
        &with_millis(&timestamped, "ReifyTimestamps"),
    )
    .contains_in_any_order(millis_values().into_iter().map(|ms| (ms, ms - 1_000)));
    run(p).await;
}

/// Emits each element `back` before its timestamp.
#[derive(Clone)]
struct ShiftBack {
    skew: Duration,
    back: Duration,
}

impl DoFn for ShiftBack {
    type In = i64;
    type Out = i64;

    fn process_element(&mut self, value: i64, ctx: &mut ProcessContext<'_, i64>) -> Result {
        let at = ctx.timestamp() - self.back;
        ctx.output(value).at(at).emit()
    }

    fn allowed_timestamp_skew(&self) -> Duration {
        self.skew
    }
}

/// `3`, `42` and `6` at their own milliseconds, then moved `back` by a function with `skew`.
fn shifted_back(p: &Pipeline, skew: Duration, back: Duration) -> PCollection<i64> {
    p.apply("Create", Create::new(vec![3_i64, 42, 6]))
        .with_timestamps("OutputTimestamp", at_own_millis)
        .par_do("ShiftTimestamp", ShiftBack { skew, back })
}

/// Java: `ParDoTest.TimestampTests.testParDoShiftTimestamp`.
#[tokio::test]
async fn par_do_shifts_a_timestamp_back_within_the_allowed_skew() {
    let p = TestPipeline::new();
    let shifted = shifted_back(&p, Duration::from_secs(1), Duration::from_secs(1));

    passert::that("AssertShifted", &with_millis(&shifted, "ReifyTimestamps"))
        .contains_in_any_order([(3, -997), (42, -958), (6, -994)]);
    run(p).await;
}

/// Java: `ParDoTest.TimestampTests.testParDoShiftTimestampInvalid`.
#[tokio::test]
async fn par_do_fails_a_timestamp_before_the_allowed_skew() {
    let p = Pipeline::new();
    shifted_back(&p, Duration::from_secs(1), Duration::from_millis(1_001));

    let message = failure(p).await;
    for part in [
        "Cannot output with timestamp",
        "must not be earlier than the timestamp of the current input",
        "minus the allowed timestamp skew (1s)",
    ] {
        assert!(message.contains(part), "'{part}' is not in: {message}");
    }
}

/// Java: `ParDoTest.TimestampTests.testParDoShiftTimestampInvalidZeroAllowed`.
#[tokio::test]
async fn par_do_fails_a_timestamp_before_the_input_with_zero_skew() {
    let p = Pipeline::new();
    shifted_back(&p, Duration::ZERO, Duration::from_millis(1_001));

    let message = failure(p).await;
    for part in [
        "Cannot output with timestamp",
        "must not be earlier than the timestamp of the current input",
        "minus the allowed timestamp skew (0ns)",
    ] {
        assert!(message.contains(part), "'{part}' is not in: {message}");
    }
}

/// A closure function has zero skew.
#[tokio::test]
async fn par_do_fn_fails_a_timestamp_before_the_input() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(vec![3_i64]))
        .with_timestamps("OutputTimestamp", at_own_millis)
        .par_do_fn("ShiftTimestamp", |value: i64, ctx| {
            let at = ctx.timestamp() - Duration::from_millis(1);
            ctx.output(value).at(at).emit()
        });

    let message = failure(p).await;
    assert!(
        message.contains("minus the allowed timestamp skew (0ns)"),
        "{message}"
    );
}

/// Java: `ParDoTest.TimestampTests.testParDoShiftTimestampUnlimited`.
#[tokio::test]
async fn par_do_shifts_a_timestamp_to_the_minimum_with_the_maximum_skew() {
    let values = [
        0,
        Instant::MIN.as_millis(),
        Instant::END_OF_GLOBAL_WINDOW.as_millis(),
    ];
    let p = TestPipeline::new();
    let shifted = p
        .apply("Create", Create::new(values.to_vec()))
        .with_timestamps("AssignTimestampToValue", at_own_millis)
        .apply(
            "ReassignToMinimumTimestamp",
            WithTimestamps::new(|_: &i64| Instant::MIN).with_allowed_timestamp_skew(Duration::MAX),
        );

    passert::that("AssertShifted", &with_millis(&shifted, "ReifyTimestamps"))
        .contains_in_any_order(values.map(|value| (value, Instant::MIN.as_millis())));
    run(p).await;
}

/// Emits `output` at `back` before the current timestamp. If the SDK rejects that
/// timestamp, emits `output:<millis>` at the current timestamp.
fn emit_before(ctx: &mut ProcessContext<'_, String>, back: Duration) -> Result {
    let at = ctx.timestamp() - back;
    match ctx.output("output".to_string()).at(at).emit() {
        Ok(()) => Ok(()),
        Err(error) if rejects(&error, at) => ctx.emit(format!("output:{}", at.as_millis())),
        Err(error) => Err(error),
    }
}

fn rejects(error: &Error, at: Instant) -> bool {
    let message = error.to_string();
    message.contains(&format!("Cannot output with timestamp {at}."))
        && message.contains("`allowed_timestamp_skew`")
}

fn rejects_timer(error: &Error, at: Instant) -> bool {
    let message = error.to_string();
    message.contains(&format!("Cannot set a timer with output timestamp {at}."))
        && message.contains("`allowed_timestamp_skew`")
}

#[derive(Timers)]
enum SkewTimer {
    #[timer(event_time)]
    Fire,
    #[timer(event_time)]
    Second,
    #[timer(processing_time)]
    Proc,
}

/// Sets a timer at the element timestamp. The timer emits and holds `back` before its hold.
#[derive(Clone)]
struct OnTimerSkew {
    skew: Duration,
    back: Duration,
}

impl StatefulDoFn for OnTimerSkew {
    type Key = String;
    type Value = String;
    type Out = String;
    type State = ();
    type Timer = SkewTimer;

    fn process_element(
        &mut self,
        _key: &String,
        _value: String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(SkewTimer::Fire).at(ctx.timestamp())
    }

    fn on_timer(
        &mut self,
        timer: SkewTimer,
        _key: &String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            SkewTimer::Fire => {
                emit_before(ctx, self.back)?;
                let at = ctx.timestamp() - self.back;
                match ctx.timer(SkewTimer::Second).hold(at).at(ctx.timestamp()) {
                    Ok(()) => ctx.emit("timer".to_string()),
                    Err(error) if rejects_timer(&error, at) => {
                        ctx.emit(format!("timer:{}", at.as_millis()))
                    }
                    Err(error) => Err(error),
                }
            }
            SkewTimer::Second | SkewTimer::Proc => Ok(()),
        }
    }

    fn allowed_timestamp_skew(&self) -> Duration {
        self.skew
    }
}

/// Sets a processing-time timer with a hold `back` before the input timestamp.
#[derive(Clone)]
struct ProcessElementTimerHoldSkew {
    skew: Duration,
    back: Duration,
}

impl StatefulDoFn for ProcessElementTimerHoldSkew {
    type Key = String;
    type Value = String;
    type Out = String;
    type State = ();
    type Timer = SkewTimer;

    fn process_element(
        &mut self,
        _key: &String,
        _value: String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        emit_before(ctx, self.back)?;
        let at = ctx.timestamp() - self.back;
        match ctx.timer(SkewTimer::Proc).hold(at).at(Instant::EPOCH) {
            Ok(()) => ctx.emit("timer".to_string()),
            Err(error) if rejects_timer(&error, at) => {
                ctx.emit(format!("timer:{}", at.as_millis()))
            }
            Err(error) => Err(error),
        }
    }

    fn allowed_timestamp_skew(&self) -> Duration {
        self.skew
    }
}

/// One keyed element at the epoch.
fn keyed_at_epoch(p: &Pipeline, name: &str) -> PCollection<(String, String)> {
    p.apply(
        format!("{name}/Create"),
        Create::new(vec![("0".to_string(), "0".to_string())]),
    )
    .with_timestamps(format!("{name}/AtEpoch"), |_: &(String, String)| {
        Instant::EPOCH
    })
}

/// Java: `ParDoTest.TimestampTests.testProcessElementSkew`.
#[tokio::test]
async fn process_element_checks_output_and_timer_hold_against_the_input_timestamp() {
    let millis = Duration::from_millis;
    let p = TestPipeline::new();
    let no_skew = keyed_at_epoch(&p, "NoSkew").par_do(
        "NoSkew",
        ProcessElementTimerHoldSkew {
            skew: Duration::ZERO,
            back: millis(1),
        },
    );
    let skew = keyed_at_epoch(&p, "Skew").par_do(
        "Skew",
        ProcessElementTimerHoldSkew {
            skew: millis(2),
            back: millis(1),
        },
    );

    passert::that("AssertNoSkew", &no_skew)
        .contains_in_any_order(["output:-1".to_string(), "timer:-1".to_string()]);
    passert::that("AssertSkew", &skew)
        .contains_in_any_order(["output".to_string(), "timer".to_string()]);
    run(p).await;
}

/// Java: `ParDoTest.TimestampTests.testOnTimerTimestampSkew`.
#[tokio::test]
async fn on_timer_checks_an_output_against_the_hold_of_the_timer() {
    let millis = Duration::from_millis;
    let p = TestPipeline::new();
    let no_skew = keyed_at_epoch(&p, "NoSkew").par_do(
        "NoSkew",
        OnTimerSkew {
            skew: Duration::ZERO,
            back: millis(3),
        },
    );
    let skew = keyed_at_epoch(&p, "Skew").par_do(
        "Skew",
        OnTimerSkew {
            skew: millis(3),
            back: millis(2),
        },
    );

    passert::that("AssertNoSkew", &no_skew)
        .contains_in_any_order(["output:-3".to_string(), "timer:-3".to_string()]);
    passert::that("AssertSkew", &skew)
        .contains_in_any_order(["output".to_string(), "timer".to_string()]);
    passert::that("AssertSkewTimestamp", &with_text_millis(&skew))
        .contains_in_any_order([("output".to_string(), -2), ("timer".to_string(), 0)]);
    run(p).await;
}

fn with_text_millis(values: &PCollection<String>) -> PCollection<(String, i64)> {
    values.par_do_fn("ReifyTimestamps", |value: String, ctx| {
        ctx.emit((value, ctx.timestamp().as_millis()))
    })
}

/// Emits `back` before the output timestamp of the window expiration.
#[derive(Clone)]
struct OnWindowExpirationSkew {
    skew: Duration,
    back: Duration,
}

impl StatefulDoFn for OnWindowExpirationSkew {
    type Key = String;
    type Value = String;
    type Out = String;
    type State = ();
    type Timer = NoTimers;
    const ON_WINDOW_EXPIRATION: bool = true;

    fn process_element(
        &mut self,
        _key: &String,
        _value: String,
        _state: &mut (),
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        Ok(())
    }

    fn on_window_expiration(
        &mut self,
        _key: &String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        emit_before(ctx, self.back)
    }

    fn allowed_timestamp_skew(&self) -> Duration {
        self.skew
    }
}

/// Java: `ParDoTest.TimestampTests.testOnWindowTimestampSkew`.
#[tokio::test]
async fn on_window_expiration_checks_an_output_against_its_timestamp() {
    let millis = Duration::from_millis;
    let windows = || WindowInto::new(FixedWindows::of(millis(10)));
    let p = TestPipeline::new();
    let no_skew = keyed_at_epoch(&p, "NoSkew")
        .apply("NoSkewWindow", windows())
        .par_do(
            "NoSkew",
            OnWindowExpirationSkew {
                skew: Duration::ZERO,
                back: millis(3),
            },
        );
    let skew = keyed_at_epoch(&p, "Skew")
        .apply("SkewWindow", windows())
        .par_do(
            "Skew",
            OnWindowExpirationSkew {
                skew: millis(4),
                back: millis(3),
            },
        );

    // The window is [0, 10), so the output timestamp of the expiration is 9.
    passert::that("AssertNoSkew", &no_skew).contains_in_any_order(["output:6".to_string()]);
    passert::that("AssertSkew", &skew).contains_in_any_order(["output".to_string()]);
    run(p).await;
}
