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

//! Window-expiration bound checks on event-time timers and timer holds, on Prism.
//!
//! Java: `ParDoTest.TimerTests.testOutOfBoundsEventTimeTimer`,
//! `testOutOfBoundsEventTimeTimerHold`, `testOutOfBoundsProcessingTimeTimerHold`.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use beam_runner_tests::prism_runner_from_env;

async fn failure(p: Pipeline) -> String {
    let error = p
        .run_with_runner(&prism_runner_from_env())
        .await
        .expect_err("out-of-bounds timer or hold must fail the pipeline");
    format!("{error:?}")
}

#[derive(Timers)]
enum BoundTimers {
    #[timer(event_time)]
    Event,
    #[timer(processing_time)]
    Processing,
}

#[derive(Clone, Copy)]
enum Attempt {
    EventPastWindowEnd,
    EventAtMillis(i64),
    EventHoldPastFire,
    ProcessingHoldPastWindowEnd,
}

#[derive(Clone)]
struct TrySetTimer(Attempt);

impl StatefulDoFn for TrySetTimer {
    type Key = String;
    type Value = i64;
    type Out = i64;
    type State = ();
    type Timer = BoundTimers;

    fn process_element(
        &mut self,
        _key: &String,
        value: i64,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let max = ctx.window()?.max_timestamp();
        let past = Instant::from_millis(max.as_millis() + 1);
        match self.0 {
            Attempt::EventPastWindowEnd => ctx.timer(BoundTimers::Event).at(past),
            Attempt::EventAtMillis(ms) => {
                ctx.timer(BoundTimers::Event).at(Instant::from_millis(ms))?;
                ctx.emit(value)
            }
            Attempt::EventHoldPastFire => ctx.timer(BoundTimers::Event).hold(past).at(max),
            Attempt::ProcessingHoldPastWindowEnd => ctx
                .timer(BoundTimers::Processing)
                .hold(past)
                .after(Duration::from_secs(1)),
        }
    }
}

/// Java: `ParDoTest.TimerTests.testOutOfBoundsEventTimeTimer`.
#[tokio::test]
async fn out_of_bounds_event_time_timer_fails() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(vec![("hello".to_string(), 37i64)]))
        .par_do("OutOfBounds", TrySetTimer(Attempt::EventPastWindowEnd));

    let message = failure(p).await;
    for part in ["timer", "expiration"] {
        assert!(message.contains(part), "'{part}' is not in: {message}");
    }
}

/// Validates that `WindowInto::with_allowed_lateness` extends the timer bound on the worker.
#[tokio::test]
async fn event_time_timer_respects_window_allowed_lateness_bound() {
    let window_with_lateness = || {
        WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
            .with_allowed_lateness(Duration::from_secs(5))
    };

    let at_five_seconds = |(key, ms): (String, i64),
                           ctx: &mut ProcessContext<'_, (String, i64)>| {
        ctx.output((key, ms)).at(Instant::from_millis(ms)).emit()
    };

    // Window [0, 10 s) has max_timestamp 9_999; 5 s allowed lateness extends expiry to 14_999.
    let ok = TestPipeline::new();
    let out = ok
        .apply("Create", Create::new(vec![("hello".to_string(), 5_000i64)]))
        .par_do_fn("AtFiveSeconds", at_five_seconds)
        .apply("Window", window_with_lateness())
        .par_do("AtExpiry", TrySetTimer(Attempt::EventAtMillis(14_999)));
    passert::that("AssertEmitted", &out).contains_in_any_order([5_000i64]);
    ok.run_with(&prism_runner_from_env())
        .await
        .expect("timer at window expiration passes");

    let bad = Pipeline::new();
    bad.apply("Create", Create::new(vec![("hello".to_string(), 5_000i64)]))
        .par_do_fn("AtFiveSeconds", at_five_seconds)
        .apply("Window", window_with_lateness())
        .par_do("PastExpiry", TrySetTimer(Attempt::EventAtMillis(15_000)));
    let message = failure(bad).await;
    for part in ["timer", "expiration"] {
        assert!(message.contains(part), "'{part}' is not in: {message}");
    }
}

/// Java: `ParDoTest.TimerTests.testOutOfBoundsEventTimeTimerHold`.
#[tokio::test]
async fn out_of_bounds_event_time_timer_hold_fails() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(vec![("hello".to_string(), 37i64)]))
        .par_do("OutOfBoundsHold", TrySetTimer(Attempt::EventHoldPastFire));

    let message = failure(p).await;
    for part in ["timer", "output timestamp"] {
        assert!(message.contains(part), "'{part}' is not in: {message}");
    }
}

/// Java: `ParDoTest.TimerTests.testOutOfBoundsProcessingTimeTimerHold`.
#[tokio::test]
async fn out_of_bounds_processing_time_timer_hold_fails() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(vec![("hello".to_string(), 37i64)]))
        .par_do(
            "OutOfBoundsHold",
            TrySetTimer(Attempt::ProcessingHoldPastWindowEnd),
        );

    let message = failure(p).await;
    for part in ["timer", "output timestamp"] {
        assert!(message.contains(part), "'{part}' is not in: {message}");
    }
}
