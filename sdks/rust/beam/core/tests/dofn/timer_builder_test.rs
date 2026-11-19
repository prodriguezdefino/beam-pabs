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

//! The `ctx.timer(t)` builder: firing times and default watermark holds per time domain, and
//! the timestamps that `on_timer` sees.

use std::sync::Arc;
use std::time::Duration;

use beam_core::coders::{DefaultCoder, PaneInfo, TimerAction, TimerRecord, Timing, WindowedHeader};
use beam_core::internals::{ElementSink, HandlerContext, IntoParDoFn, TimerCollector};
use beam_core::prelude::*;

const ELEMENT_MILLIS: i64 = 10_000;
const OFFSET: Duration = Duration::from_secs(5);

#[derive(Timers)]
#[beam(crate = "beam_core")]
enum Delayed {
    #[timer(event_time, id = "event")]
    Event,
    #[timer(processing_time, id = "processing")]
    Processing,
}

/// Sets one timer `OFFSET` after "now" in the domain chosen by the element value.
#[derive(Clone)]
struct SetsDelayedTimer;

impl StatefulDoFn for SetsDelayedTimer {
    type Key = String;
    type Value = bool;
    type Out = String;
    type State = ();
    type Timer = Delayed;

    fn process_element(
        &mut self,
        _key: &String,
        event_time: bool,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let timer = if event_time {
            Delayed::Event
        } else {
            Delayed::Processing
        };
        ctx.timer(timer).after(OFFSET)
    }
}

/// Processes one `(key, event_time)` element at `ELEMENT_MILLIS` and returns its timers.
fn timers_after_processing(event_time: bool) -> Vec<(String, TimerRecord)> {
    timers_after_processing_in_pane(event_time, PaneInfo::NO_FIRING)
}

/// Processes one `(key, event_time)` element of `pane` and returns its timers.
fn timers_after_processing_in_pane(event_time: bool, pane: PaneInfo) -> Vec<(String, TimerRecord)> {
    let collector = Arc::new(TimerCollector::new());
    let header = WindowedHeader::global(ELEMENT_MILLIS, pane);
    let element = ("k".to_string(), event_time).encode().expect("encode");
    let mut handler = SetsDelayedTimer.into_handler().instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    {
        let mut ctx = HandlerContext::new(&mut sink)
            .with_header(&header)
            .with_timer_collector(Some(&collector));
        handler.process(&element, &mut ctx).expect("process");
    }
    collector
        .drain()
        .into_iter()
        .map(|change| (change.family.into_owned(), change.record))
        .collect()
}

#[test]
fn an_event_time_timer_after_fires_after_the_element_timestamp_and_holds_at_the_fire_time() {
    let timers = timers_after_processing(true);
    let [(family, record)] = timers.as_slice() else {
        panic!("expected one timer, got {}", timers.len());
    };
    let fire = Instant::from_millis(ELEMENT_MILLIS) + OFFSET;
    assert_eq!(family, "event");
    let TimerAction::Set {
        fire_timestamp,
        hold_timestamp,
        ..
    } = record.action
    else {
        panic!("expected a set timer, got {:?}", record.action);
    };
    assert_eq!(fire_timestamp, fire);
    assert_eq!(hold_timestamp, fire);
    assert_eq!(record.user_key, "k".to_string().encode().unwrap());
}

#[test]
fn a_processing_time_timer_after_fires_after_the_clock_and_holds_at_the_element_timestamp() {
    let before = Instant::now();
    let timers = timers_after_processing(false);
    let after = Instant::now();
    let [(family, record)] = timers.as_slice() else {
        panic!("expected one timer, got {}", timers.len());
    };
    assert_eq!(family, "processing");
    let TimerAction::Set {
        fire_timestamp,
        hold_timestamp,
        ..
    } = record.action
    else {
        panic!("expected a set timer, got {:?}", record.action);
    };
    let fire = fire_timestamp;
    assert!(
        (before + OFFSET) <= fire && fire <= (after + OFFSET),
        "fire {fire:?} must be the wall clock plus the offset"
    );
    assert_eq!(hold_timestamp, Instant::from_millis(ELEMENT_MILLIS));
}

#[test]
fn a_timer_keeps_the_pane_of_its_element() {
    let pane = PaneInfo::new(false, true, Timing::Late, Some(3), Some(2));
    for event_time in [true, false] {
        let timers = timers_after_processing_in_pane(event_time, pane);
        let [(_, record)] = timers.as_slice() else {
            panic!("expected one timer, got {}", timers.len());
        };
        let TimerAction::Set {
            pane: timer_pane, ..
        } = record.action
        else {
            panic!("expected a set timer, got {:?}", record.action);
        };
        assert_eq!(timer_pane, pane);
    }
}

const FIRE_MILLIS: i64 = 20_000;
const HOLD_MILLIS: i64 = 12_000;

/// Records each element with the header it was pushed under.
#[derive(Default)]
struct HeaderRecorder(Vec<(Option<WindowedHeader>, Vec<u8>)>);

impl ElementSink for HeaderRecorder {
    fn push(&mut self, element: Vec<u8>) -> std::result::Result<(), String> {
        self.0.push((None, element));
        Ok(())
    }

    fn push_windowed(
        &mut self,
        header: &WindowedHeader,
        element: Vec<u8>,
    ) -> std::result::Result<(), String> {
        self.0.push((Some(header.clone()), element));
        Ok(())
    }
}

/// Emits the firing time of each timer and sets the same timer again `OFFSET` later.
#[derive(Clone)]
struct Rearms;

impl StatefulDoFn for Rearms {
    type Key = String;
    type Value = bool;
    type Out = i64;
    type State = ();
    type Timer = Delayed;

    fn process_element(
        &mut self,
        _key: &String,
        _value: bool,
        _state: &mut (),
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer: Delayed,
        _key: &String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let fire = ctx.fire_timestamp().expect("on_timer has a firing time");
        ctx.timer(timer).after(OFFSET)?;
        ctx.emit(fire.as_millis())
    }
}

/// Fires `family` at `FIRE_MILLIS` with a hold at `HOLD_MILLIS`. Returns the output timestamp,
/// the emitted firing time and the timer that `on_timer` set.
fn fire_and_rearm(family: &str) -> (i64, i64, TimerRecord) {
    let collector = Arc::new(TimerCollector::new());
    let fired = TimerRecord::new_set(
        "k".to_string().encode().expect("encode"),
        "",
        Instant::from_millis(FIRE_MILLIS),
        Instant::from_millis(HOLD_MILLIS),
    );
    let mut handler = Rearms.into_handler().instantiate();
    let mut sink = HeaderRecorder::default();
    {
        let mut ctx = HandlerContext::new(&mut sink).with_timer_collector(Some(&collector));
        handler
            .on_timer(family, &fired, &mut ctx)
            .expect("on_timer");
    }
    let [(Some(header), bytes)] = sink.0.as_slice() else {
        panic!("expected one windowed output, got {}", sink.0.len());
    };
    let emitted = <i64 as DefaultCoder>::decode(bytes).expect("decode");
    let [change] = <[_; 1]>::try_from(collector.drain()).expect("one timer");
    let record = change.record;
    (header.timestamp_millis(), emitted, record)
}

#[test]
fn an_event_time_timer_outputs_at_its_hold_and_counts_after_from_its_firing_time() {
    let (output, emitted, record) = fire_and_rearm("event");
    assert_eq!(output, HOLD_MILLIS);
    assert_eq!(emitted, FIRE_MILLIS);
    let next = Instant::from_millis(FIRE_MILLIS) + OFFSET;
    let TimerAction::Set {
        fire_timestamp,
        hold_timestamp,
        ..
    } = record.action
    else {
        panic!("expected a set timer, got {:?}", record.action);
    };
    assert_eq!(fire_timestamp, next);
    assert_eq!(hold_timestamp, next);
}

#[test]
fn a_processing_time_timer_outputs_at_its_hold_and_its_next_timer_keeps_the_hold() {
    let (output, emitted, record) = fire_and_rearm("processing");
    assert_eq!(output, HOLD_MILLIS);
    assert_eq!(emitted, FIRE_MILLIS);
    let TimerAction::Set { hold_timestamp, .. } = record.action else {
        panic!("expected a set timer, got {:?}", record.action);
    };
    assert_eq!(hold_timestamp, Instant::from_millis(HOLD_MILLIS));
}

#[test]
fn process_element_has_no_firing_time() {
    #[derive(Clone)]
    struct ReadsFire;

    impl StatefulDoFn for ReadsFire {
        type Key = String;
        type Value = bool;
        type Out = bool;
        type State = ();
        type Timer = NoTimers;

        fn process_element(
            &mut self,
            _key: &String,
            _value: bool,
            _state: &mut (),
            ctx: &mut StatefulContext<'_, Self>,
        ) -> Result {
            let fired = ctx.fire_timestamp().is_some();
            ctx.emit(fired)
        }
    }

    let header = WindowedHeader::global(ELEMENT_MILLIS, PaneInfo::NO_FIRING);
    let element = ("k".to_string(), true).encode().expect("encode");
    let mut handler = ReadsFire.into_handler().instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    {
        let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
        handler.process(&element, &mut ctx).expect("process");
    }
    let emitted = sink
        .iter()
        .map(|bytes| <bool as DefaultCoder>::decode(bytes).expect("decode"))
        .collect::<Vec<_>>();
    assert_eq!(emitted, [false]);
}

const NO_HOLD_MILLIS: i64 = Instant::MAX.as_millis() + 1;

#[derive(Clone)]
enum Configure {
    WithoutHold(Instant),
    HoldThenWithoutHold(Instant, Instant),
    WithoutHoldThenHold(Instant, Instant),
    Aligned { period: Duration, offset: Duration },
    At(Instant),
    HoldAt { hold: Instant, fire: Instant },
    HoldAfter { hold: Instant, offset: Duration },
}

#[derive(Clone)]
struct ConfiguredTimer {
    event_time: bool,
    config: Configure,
}

impl StatefulDoFn for ConfiguredTimer {
    type Key = String;
    type Value = ();
    type Out = i64;
    type State = ();
    type Timer = Delayed;

    fn process_element(
        &mut self,
        _key: &String,
        (): (),
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let timer = if self.event_time {
            Delayed::Event
        } else {
            Delayed::Processing
        };
        let b = ctx.timer(timer);
        match self.config {
            Configure::WithoutHold(fire) => b.without_hold().at(fire),
            Configure::HoldThenWithoutHold(hold, fire) => b.hold(hold).without_hold().at(fire),
            Configure::WithoutHoldThenHold(hold, fire) => b.without_hold().hold(hold).at(fire),
            Configure::Aligned { period, offset } => b.with_align(period).after(offset),
            Configure::At(fire) => b.at(fire),
            Configure::HoldAt { hold, fire } => b.hold(hold).at(fire),
            Configure::HoldAfter { hold, offset } => b.hold(hold).after(offset),
        }
    }
}

fn run_configured(
    func: ConfiguredTimer,
    header: &WindowedHeader,
    allowed_lateness: Duration,
) -> std::result::Result<TimerRecord, String> {
    let collector = Arc::new(TimerCollector::new());
    let element = ("k".to_string(), ()).encode().expect("encode");
    let mut handler = func.into_handler().instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink)
        .with_header(header)
        .with_timer_collector(Some(&collector))
        .with_allowed_lateness(allowed_lateness);
    handler.process(&element, &mut ctx)?;
    let [change] = <[_; 1]>::try_from(collector.drain()).expect("one timer");
    Ok(change.record)
}

#[test]
fn without_hold_writes_the_max_plus_one_sentinel_and_overrides_an_earlier_hold() {
    let header = WindowedHeader::global(ELEMENT_MILLIS, PaneInfo::NO_FIRING);
    let fire = Instant::from_millis(25_000);
    let hold = Instant::from_millis(15_000);
    for event_time in [true, false] {
        for config in [
            Configure::WithoutHold(fire),
            Configure::HoldThenWithoutHold(hold, fire),
        ] {
            let record = run_configured(
                ConfiguredTimer { event_time, config },
                &header,
                Duration::ZERO,
            )
            .expect("without_hold succeeds");
            let TimerAction::Set {
                fire_timestamp,
                hold_timestamp,
                ..
            } = record.action
            else {
                panic!("expected a set timer");
            };
            assert_eq!(fire_timestamp, fire);
            assert_eq!(hold_timestamp.as_millis(), NO_HOLD_MILLIS);
        }

        let record = run_configured(
            ConfiguredTimer {
                event_time,
                config: Configure::WithoutHoldThenHold(hold, fire),
            },
            &header,
            Duration::ZERO,
        )
        .expect("hold overrides without_hold");
        let TimerAction::Set { hold_timestamp, .. } = record.action else {
            panic!("expected a set timer");
        };
        assert_eq!(hold_timestamp, hold);
    }
}

#[test]
fn emitting_with_the_default_timestamp_from_a_without_hold_timer_fails() {
    #[derive(Clone, Copy)]
    enum EmitKind {
        Direct,
        BuilderDefault,
        BuilderWithPane,
        ExplicitAtFire,
    }

    #[derive(Clone)]
    struct OnNoHold {
        kind: EmitKind,
        skew: Duration,
    }

    impl StatefulDoFn for OnNoHold {
        type Key = String;
        type Value = ();
        type Out = i64;
        type State = ();
        type Timer = Delayed;

        fn process_element(
            &mut self,
            _key: &String,
            (): (),
            _state: &mut (),
            _ctx: &mut StatefulContext<'_, Self>,
        ) -> Result {
            Ok(())
        }

        fn on_timer(
            &mut self,
            _timer: Delayed,
            _key: &String,
            _state: &mut (),
            ctx: &mut StatefulContext<'_, Self>,
        ) -> Result {
            match self.kind {
                EmitKind::Direct => ctx.emit(1),
                EmitKind::BuilderDefault => ctx.output(1).emit(),
                EmitKind::BuilderWithPane => ctx
                    .output(1)
                    .with_pane(PaneInfo::ON_TIME_AND_ONLY_FIRING)
                    .emit(),
                EmitKind::ExplicitAtFire => {
                    let fire = ctx.fire_timestamp().expect("firing time");
                    ctx.output(1).at(fire).emit()
                }
            }
        }

        fn allowed_timestamp_skew(&self) -> Duration {
            self.skew
        }
    }

    let fired = TimerRecord::new_set(
        "k".to_string().encode().expect("encode"),
        "",
        Instant::from_millis(FIRE_MILLIS),
        Instant::from_millis(NO_HOLD_MILLIS),
    );
    let run = |kind, skew| {
        let mut handler = OnNoHold { kind, skew }.into_handler().instantiate();
        let mut sink = HeaderRecorder::default();
        let mut ctx = HandlerContext::new(&mut sink);
        handler.on_timer("event", &fired, &mut ctx).map(|()| sink.0)
    };

    for kind in [
        EmitKind::Direct,
        EmitKind::BuilderDefault,
        EmitKind::BuilderWithPane,
    ] {
        for skew in [Duration::ZERO, Duration::MAX] {
            let error = run(kind, skew).expect_err("default timestamp is above Instant::MAX");
            assert!(error.contains("Cannot output with timestamp"), "{error}");
        }
    }
    let zero_skew = run(EmitKind::ExplicitAtFire, Duration::ZERO)
        .expect_err("zero skew rejects an output before the sentinel hold");
    assert!(
        zero_skew.contains("Cannot output with timestamp"),
        "{zero_skew}"
    );

    let with_max_skew =
        run(EmitKind::ExplicitAtFire, Duration::MAX).expect("Duration::MAX permits explicit .at");
    assert_eq!(
        with_max_skew[0].0.as_ref().unwrap().timestamp_millis(),
        FIRE_MILLIS
    );
}

#[test]
fn with_align_snaps_event_time_targets_and_clamps_to_window_expiration() {
    // Instant::MIN aligned to 1 s with 1 ms offset -> Instant::MIN + 1774 ms.
    let min_header = WindowedHeader::global(Instant::MIN.as_millis(), PaneInfo::NO_FIRING);
    let min_record = run_configured(
        ConfiguredTimer {
            event_time: true,
            config: Configure::Aligned {
                period: Duration::from_secs(1),
                offset: Duration::from_millis(1),
            },
        },
        &min_header,
        Duration::ZERO,
    )
    .expect("aligned from Instant::MIN");
    let expected_min = Instant::MIN + Duration::from_millis(1_774);
    assert!(matches!(
        min_record.action,
        TimerAction::Set {
            fire_timestamp,
            hold_timestamp,
            ..
        } if fire_timestamp == expected_min && hold_timestamp == expected_min
    ));

    // 5 s aligned to 1 min with 1 s offset -> 59 s.
    let five_s = WindowedHeader::global(5_000, PaneInfo::NO_FIRING);
    let five_record = run_configured(
        ConfiguredTimer {
            event_time: true,
            config: Configure::Aligned {
                period: Duration::from_secs(60),
                offset: Duration::from_secs(1),
            },
        },
        &five_s,
        Duration::ZERO,
    )
    .expect("aligned from 5 s");
    assert!(matches!(
        five_record.action,
        TimerAction::Set { fire_timestamp, .. } if fire_timestamp == Instant::from_millis(59_000)
    ));

    // Already on boundary: (59 s + 1 s) % 60 s == 0 stays at 59 s.
    let aligned_already = WindowedHeader::global(59_000, PaneInfo::NO_FIRING);
    let exact_record = run_configured(
        ConfiguredTimer {
            event_time: true,
            config: Configure::Aligned {
                period: Duration::from_secs(60),
                offset: Duration::from_secs(1),
            },
        },
        &aligned_already,
        Duration::ZERO,
    )
    .expect("already aligned");
    assert!(matches!(
        exact_record.action,
        TimerAction::Set { fire_timestamp, .. } if fire_timestamp == Instant::from_millis(59_000)
    ));

    // END_OF_GLOBAL_WINDOW aligned by 1 day clamps to END_OF_GLOBAL_WINDOW.
    let end_header = WindowedHeader::global(
        Instant::END_OF_GLOBAL_WINDOW.as_millis(),
        PaneInfo::NO_FIRING,
    );
    let clamped = run_configured(
        ConfiguredTimer {
            event_time: true,
            config: Configure::Aligned {
                period: Duration::from_secs(86_400),
                offset: Duration::ZERO,
            },
        },
        &end_header,
        Duration::ZERO,
    )
    .expect("clamped to end of global window");
    assert!(matches!(
        clamped.action,
        TimerAction::Set {
            fire_timestamp,
            hold_timestamp,
            ..
        } if fire_timestamp == Instant::END_OF_GLOBAL_WINDOW
            && hold_timestamp == Instant::END_OF_GLOBAL_WINDOW
    ));
}

fn interval_header(timestamp_ms: i64, start_ms: i64, end_ms: i64) -> WindowedHeader {
    let window = BoundedWindow::Interval(IntervalWindow::new(
        Instant::from_millis(start_ms),
        Instant::from_millis(end_ms),
    ));
    let mut window_bytes = Vec::new();
    window.encode(&mut window_bytes).expect("encode window");
    WindowedHeader::new(timestamp_ms, &[window_bytes], PaneInfo::NO_FIRING)
}

#[test]
fn window_expiration_bounds_check_timers_and_holds_with_allowed_lateness() {
    let header = interval_header(5_000, 0, 10_000);
    let lateness = Duration::from_secs(5);
    // Window max_timestamp is 9_999; with 5 s allowed lateness, expiry is 14_999.
    let expiry = Instant::from_millis(14_999);
    let past_expiry = Instant::from_millis(15_000);

    run_configured(
        ConfiguredTimer {
            event_time: true,
            config: Configure::At(expiry),
        },
        &header,
        lateness,
    )
    .expect("event-time timer at window expiry passes");

    let timer_err = run_configured(
        ConfiguredTimer {
            event_time: true,
            config: Configure::At(past_expiry),
        },
        &header,
        lateness,
    )
    .expect_err("event-time timer past window expiry fails");
    assert!(
        timer_err.contains("timer") && timer_err.contains("expiration"),
        "{timer_err}"
    );

    let event_hold_err = run_configured(
        ConfiguredTimer {
            event_time: true,
            config: Configure::HoldAt {
                hold: Instant::from_millis(9_001),
                fire: Instant::from_millis(9_000),
            },
        },
        &header,
        lateness,
    )
    .expect_err("event-time hold after firing time fails");
    assert!(
        event_hold_err.contains("timer") && event_hold_err.contains("output timestamp"),
        "{event_hold_err}"
    );

    run_configured(
        ConfiguredTimer {
            event_time: false,
            config: Configure::HoldAfter {
                hold: expiry,
                offset: Duration::from_secs(1),
            },
        },
        &header,
        lateness,
    )
    .expect("processing-time hold at window expiry passes");

    let proc_hold_err = run_configured(
        ConfiguredTimer {
            event_time: false,
            config: Configure::HoldAfter {
                hold: past_expiry,
                offset: Duration::from_secs(1),
            },
        },
        &header,
        lateness,
    )
    .expect_err("processing-time hold past window expiry fails");
    assert!(
        proc_hold_err.contains("timer") && proc_hold_err.contains("output timestamp"),
        "{proc_hold_err}"
    );
}
