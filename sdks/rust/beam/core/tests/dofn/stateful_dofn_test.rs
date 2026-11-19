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

//! The handler of a `StatefulDoFn`: each lifecycle call reaches the function, and the
//! `ParDo` payload carries what the function declares about itself.

use std::sync::{Arc, Mutex};

use beam_core::coders::{DefaultCoder, TimerRecord};
use beam_core::internals::{
    HandlerContext, IntoParDoFn, StateBag, TypedElement, UserStateBackend, UserStateError,
};
use beam_core::pipeline::Pipeline;
use beam_core::prelude::*;
use beam_core::transforms::WithFinalizer;
use beam_core::transforms::display_data::{DisplayDataBuilder, DisplayDataItem};
use beam_core::windowing::GlobalWindow;
use beam_model::pipeline as proto;
use prost::Message;

#[derive(Timers)]
#[beam(crate = "beam_core")]
enum Flush {
    #[timer(event_time, id = "flush")]
    Now,
}

/// A state reader that fails every call. The cells bind to it; the test does not read them.
struct NoReads;

type Unused<T> = std::result::Result<T, UserStateError>;

fn unused<T>() -> Unused<T> {
    Err(UserStateError::Other(
        "the test does not read state".to_string(),
    ))
}

impl UserStateBackend for NoReads {
    fn get_state(&self, _: &str, _: &[u8], _: &[u8]) -> Unused<StateBag> {
        unused()
    }
    fn append_state(&self, _: &str, _: &[u8], _: &[u8], _: Vec<u8>) -> Unused<()> {
        unused()
    }
    fn clear_state(&self, _: &str, _: &[u8], _: &[u8]) -> Unused<()> {
        unused()
    }
    fn get_map_state(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> Unused<Vec<Vec<u8>>> {
        unused()
    }
    fn get_map_keys(&self, _: &str, _: &[u8], _: &[u8]) -> Unused<Vec<Vec<u8>>> {
        unused()
    }
    fn put_map_state(&self, _: &str, _: &[u8], _: &[u8], _: Vec<u8>, _: Vec<u8>) -> Unused<()> {
        unused()
    }
    fn append_map_state(&self, _: &str, _: &[u8], _: &[u8], _: Vec<u8>, _: Vec<u8>) -> Unused<()> {
        unused()
    }
    fn remove_map_key(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> Unused<()> {
        unused()
    }
    fn clear_map_state(&self, _: &str, _: &[u8], _: &[u8]) -> Unused<()> {
        unused()
    }
    fn get_ordered_list_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: i64,
    ) -> Unused<Vec<(i64, Vec<u8>)>> {
        unused()
    }
    fn append_ordered_list_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: Vec<u8>,
    ) -> Unused<()> {
        unused()
    }
    fn clear_ordered_list_range(&self, _: &str, _: &[u8], _: &[u8], _: i64, _: i64) -> Unused<()> {
        unused()
    }
    fn clear_ordered_list_state(&self, _: &str, _: &[u8], _: &[u8]) -> Unused<()> {
        unused()
    }
}

/// One cell of each kind. The handler binds all of them for each element and timer.
#[derive(State)]
#[beam(crate = "beam_core")]
#[expect(
    dead_code,
    reason = "the test checks that the cells bind, not their contents"
)]
struct AllCells {
    value: ValueState<i64>,
    bag: BagState<String>,
    map: MapState<String, i64>,
    set: SetState<String>,
    list: OrderedListState<String>,
}

/// Records each call, emits from the timer and from `finish_bundle`, and declares
/// finalization. Copies share the record.
#[derive(Clone)]
struct Recording {
    calls: Arc<Mutex<Vec<String>>>,
}

impl Recording {
    fn record(&self, call: impl Into<String>) {
        if let Ok(mut calls) = self.calls.lock() {
            calls.push(call.into());
        }
    }
}

impl StatefulDoFn for Recording {
    type Key = String;
    type Value = i64;
    type Out = WithFinalizer<String>;
    type State = AllCells;
    type Timer = Flush;

    fn setup(&mut self) -> Result {
        self.record("setup");
        Ok(())
    }

    fn start_bundle(&mut self) -> Result {
        self.record("start_bundle");
        Ok(())
    }

    fn process_element(
        &mut self,
        key: &String,
        value: i64,
        _state: &mut AllCells,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        self.record(format!("process {key}={value}"));
        ctx.emit(format!("{key}={value}").into())
    }

    fn on_timer(
        &mut self,
        timer: Flush,
        key: &String,
        _state: &mut AllCells,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            Flush::Now => {
                self.record(format!("timer {key}"));
                ctx.emit(format!("flushed {key}").into())
            }
        }
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> Result {
        self.record("finish_bundle");
        ctx.emit(
            "finished".to_string().into(),
            BoundedWindow::Global(GlobalWindow),
            Instant::END_OF_GLOBAL_WINDOW,
        )
    }

    fn teardown(&mut self) -> Result {
        self.record("teardown");
        Ok(())
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("recording", "yes");
    }
}

/// Overrides only `process_element`.
#[derive(Clone)]
struct Plain;

impl StatefulDoFn for Plain {
    type Key = String;
    type Value = i64;
    type Out = i64;
    type State = ();
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        _key: &String,
        value: i64,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.emit(value)
    }
}

#[test]
fn every_lifecycle_call_reaches_the_stateful_dofn() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut handler = Recording {
        calls: Arc::clone(&calls),
    }
    .into_handler()
    .instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    let reader: Arc<dyn UserStateBackend> = Arc::new(NoReads);

    handler.setup().expect("setup");
    handler.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink).with_state_backend(Some(&reader));
        let encoded = ("a".to_string(), 1i64).encode().expect("encode");
        handler.process(&encoded, &mut ctx).expect("process");
        let mut fused = Some(("b".to_string(), 2i64));
        handler
            .process_value(TypedElement::new(&mut fused), &mut ctx)
            .expect("process_value");
        let fired = TimerRecord::new_set(
            DefaultCoder::encode(&"c".to_string()).expect("encode"),
            "",
            Instant::from_millis(10),
            Instant::from_millis(10),
        );
        handler
            .on_timer("flush", &fired, &mut ctx)
            .expect("on_timer");
        handler.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    handler.teardown().expect("teardown");

    assert_eq!(
        *calls.lock().expect("calls"),
        [
            "setup",
            "start_bundle",
            "process a=1",
            "process b=2",
            "timer c",
            "finish_bundle",
            "teardown",
        ]
    );
    let emitted = sink
        .iter()
        .map(|bytes| <String as DefaultCoder>::decode(bytes).expect("decode"))
        .collect::<Vec<_>>();
    assert_eq!(emitted, ["a=1", "b=2", "flushed c", "finished"]);
}

/// The `ParDoPayload` and display data of the transform named `name`.
fn par_do(p: &Pipeline, name: &str) -> (proto::ParDoPayload, Vec<DisplayDataItem>) {
    let transform = p
        .to_proto()
        .components
        .expect("components")
        .transforms
        .into_values()
        .find(|t| t.unique_name == name)
        .expect("transform exists");
    let payload = proto::ParDoPayload::decode(transform.spec.expect("spec").payload.as_slice())
        .expect("ParDoPayload");
    let display_data = transform
        .display_data
        .iter()
        .map(|d| DisplayDataItem::from_proto(d).expect("display data decodes"))
        .collect();
    (payload, display_data)
}

#[test]
fn the_par_do_carries_the_finalization_request_and_display_data_of_the_function() {
    let p = Pipeline::new();
    let input = p.apply("Create", Create::new(vec![("k".to_string(), 1i64)]));
    let _ = input.apply(
        "Recording",
        ParDo::new(Recording {
            calls: Arc::new(Mutex::new(Vec::new())),
        }),
    );
    let _ = input.apply("Plain", ParDo::new(Plain));

    let (recording, recording_display) = par_do(&p, "Recording");
    assert!(recording.requests_finalization);
    assert!(
        recording_display
            .iter()
            .any(|item| item.key == "recording" && item.value == "yes"),
        "{recording_display:?}"
    );

    let (plain, _) = par_do(&p, "Plain");
    assert!(!plain.requests_finalization);
}

/// Sets a timer for each element.
#[derive(Clone)]
struct SetsTimer;

impl StatefulDoFn for SetsTimer {
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
        ctx.timer(Flush::Now).at(Instant::from_millis(100))
    }
}

#[test]
fn the_default_hooks_succeed_and_an_undeclared_timer_family_fails() {
    let mut handler = Plain.into_handler().instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    handler.setup().expect("setup");
    handler.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink);
        let key = DefaultCoder::encode(&"k".to_string()).expect("encode");
        let cleared = TimerRecord::new_clear(key.clone(), "");
        handler
            .on_timer("unknown", &cleared, &mut ctx)
            .expect("a cleared timer is ignored");
        let fired =
            TimerRecord::new_set(key, "", Instant::from_millis(10), Instant::from_millis(10));
        let error = handler
            .on_timer("unknown", &fired, &mut ctx)
            .expect_err("Plain declares no timers");
        assert!(error.contains("undeclared family 'unknown'"), "{error}");
        handler.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    handler.teardown().expect("teardown");
    assert!(sink.is_empty());
}

#[test]
fn setting_a_timer_without_a_timer_collector_fails() {
    let mut handler = SetsTimer.into_handler().instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink);
    let element = ("k".to_string(), 1i64).encode().expect("encode");
    let error = handler
        .process(&element, &mut ctx)
        .expect_err("no timer collector");
    assert!(error.contains("No active TimerCollector"), "{error}");
}

/// Emits the key, the firing time and the hold from `on_window_expiration`.
#[derive(Clone)]
struct Expiring;

impl StatefulDoFn for Expiring {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = ();
    type Timer = Flush;
    const ON_WINDOW_EXPIRATION: bool = true;

    fn process_element(
        &mut self,
        _key: &String,
        _value: i64,
        _state: &mut (),
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer: Flush,
        key: &String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            Flush::Now => ctx.emit(format!("timer {key}")),
        }
    }

    fn on_window_expiration(
        &mut self,
        key: &String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let fire = ctx.fire_timestamp().map(|fire| fire.as_millis());
        let hold = ctx.timestamp().as_millis();
        ctx.emit(format!("expired {key} fire={fire:?} hold={hold}"))
    }
}

#[test]
fn window_expiration_declares_an_event_time_family_and_the_requirement() {
    use beam_core::internals::TimeDomain;
    use beam_core::pipeline::URN_REQUIREMENT_ON_WINDOW_EXPIRATION;

    let p = Pipeline::new();
    let input = p.apply("Create", Create::new(vec![("k".to_string(), 1i64)]));
    let _ = input.apply("Expiring", ParDo::new(Expiring));
    let (payload, _) = par_do(&p, "Expiring");
    let family = &payload.on_window_expiration_timer_family_spec;
    assert!(!family.is_empty());
    assert_ne!(family, "flush");
    let spec = payload
        .timer_family_specs
        .get(family)
        .expect("the expiration family is declared");
    assert_eq!(
        spec.time_domain,
        proto::time_domain::Enum::from(TimeDomain::EventTime) as i32
    );
    assert!(payload.timer_family_specs.contains_key("flush"));
    assert!(
        p.to_proto()
            .requirements
            .iter()
            .any(|r| r == URN_REQUIREMENT_ON_WINDOW_EXPIRATION)
    );

    let plain = Pipeline::new();
    let _ = plain
        .apply("Create", Create::new(vec![("k".to_string(), 1i64)]))
        .apply("Plain", ParDo::new(Plain));
    assert!(
        par_do(&plain, "Plain")
            .0
            .on_window_expiration_timer_family_spec
            .is_empty()
    );
    assert!(
        !plain
            .to_proto()
            .requirements
            .iter()
            .any(|r| r == URN_REQUIREMENT_ON_WINDOW_EXPIRATION)
    );
}

#[test]
fn the_expiration_family_reaches_on_window_expiration_and_other_families_on_timer() {
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![("k".to_string(), 1i64)]))
        .apply("Expiring", ParDo::new(Expiring));
    let family = par_do(&p, "Expiring")
        .0
        .on_window_expiration_timer_family_spec;

    let mut handler = Expiring.into_handler().instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    {
        let mut ctx = HandlerContext::new(&mut sink);
        let key = DefaultCoder::encode(&"k".to_string()).expect("encode");
        let fired = TimerRecord::new_set(
            key.clone(),
            "",
            Instant::from_millis(9_999),
            Instant::from_millis(9_999),
        );
        handler
            .on_timer(&family, &fired, &mut ctx)
            .expect("on_window_expiration");
        let flush = TimerRecord::new_set(key, "", Instant::from_millis(5), Instant::from_millis(5));
        handler
            .on_timer("flush", &flush, &mut ctx)
            .expect("on_timer");
    }
    let emitted = sink
        .iter()
        .map(|bytes| <String as DefaultCoder>::decode(bytes).expect("decode"))
        .collect::<Vec<_>>();
    assert_eq!(emitted, ["expired k fire=Some(9999) hold=9999", "timer k"]);
}

#[derive(Timers)]
#[beam(crate = "beam_core")]
enum Reserved {
    #[timer(event_time, id = "__on_window_expiration")]
    Clash,
}

/// Declares a user timer family with the reserved id.
#[derive(Clone)]
struct ReservedFamily;

impl StatefulDoFn for ReservedFamily {
    type Key = String;
    type Value = i64;
    type Out = i64;
    type State = ();
    type Timer = Reserved;

    fn process_element(
        &mut self,
        _key: &String,
        value: i64,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.emit(value)
    }
}

#[test]
fn a_user_timer_family_with_the_reserved_id_fails_validation() {
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![("k".to_string(), 1i64)]))
        .apply("Reserved", ParDo::new(ReservedFamily));
    let error = p.validate().expect_err("the id is reserved");
    assert!(error.to_string().contains("is reserved"), "{error}");
    let _ = Reserved::Clash;
}

#[derive(State)]
#[beam(crate = "beam_core")]
struct WindowScoreState {
    score: ValueState<i64>,
}

/// Writes per-window state during `process_element` and emits `(window, state)` on timer.
#[derive(Clone)]
struct PerWindowTimerFn;

impl StatefulDoFn for PerWindowTimerFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = WindowScoreState;
    type Timer = Flush;

    fn process_element(
        &mut self,
        _key: &String,
        value: i64,
        state: &mut WindowScoreState,
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.score.write(&value)
    }

    fn on_timer(
        &mut self,
        timer: Flush,
        key: &String,
        state: &mut WindowScoreState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            Flush::Now => {
                let win = ctx.window()?;
                let score = state.score.read()?.unwrap_or(-1);
                ctx.emit(format!("{key}:{win}:{score}"))
            }
        }
    }
}

#[test]
fn a_multi_window_timer_fires_once_per_window_with_per_window_state() {
    use beam_core::coders::{PaneInfo, TimerAction, WindowedHeader};
    use beam_core::internals::ElementSink;

    #[derive(Default)]
    struct WindowedRecorder(Vec<(BoundedWindow, String)>);

    impl ElementSink for WindowedRecorder {
        fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
            Err("expected push_windowed".to_string())
        }

        fn push_windowed(
            &mut self,
            header: &WindowedHeader,
            element: Vec<u8>,
        ) -> std::result::Result<(), String> {
            let window = header.single_window().map_err(|e| e.to_string())?;
            let value = <String as DefaultCoder>::decode(&element).map_err(|e| e.to_string())?;
            self.0.push((window, value));
            Ok(())
        }
    }

    let w1 = BoundedWindow::Interval(IntervalWindow::new(
        Instant::from_millis(0),
        Instant::from_millis(10_000),
    ));
    let w2 = BoundedWindow::Interval(IntervalWindow::new(
        Instant::from_millis(5_000),
        Instant::from_millis(15_000),
    ));
    let mut w1_bytes = Vec::new();
    let mut w2_bytes = Vec::new();
    w1.encode(&mut w1_bytes).expect("encode w1");
    w2.encode(&mut w2_bytes).expect("encode w2");

    let backend: Arc<dyn UserStateBackend> = Arc::new(crate::common::TestStateBackend::default());
    let mut handler = PerWindowTimerFn.into_handler().instantiate();
    handler.setup().expect("setup");
    handler.start_bundle().expect("start_bundle");

    let mut sink = WindowedRecorder::default();
    let h1 = WindowedHeader::global(2_000, PaneInfo::NO_FIRING)
        .with_windows(&[w1])
        .expect("header w1");
    let h2 = WindowedHeader::global(7_000, PaneInfo::NO_FIRING)
        .with_windows(&[w2])
        .expect("header w2");
    {
        let mut ctx1 = HandlerContext::new(&mut sink)
            .with_header(&h1)
            .with_state_backend(Some(&backend));
        handler
            .process(
                &("k".to_string(), 10i64).encode().expect("encode"),
                &mut ctx1,
            )
            .expect("process w1");

        let mut ctx2 = HandlerContext::new(&mut sink)
            .with_header(&h2)
            .with_state_backend(Some(&backend));
        handler
            .process(
                &("k".to_string(), 20i64).encode().expect("encode"),
                &mut ctx2,
            )
            .expect("process w2");

        let multi_timer = TimerRecord {
            user_key: DefaultCoder::encode(&"k".to_string()).expect("encode key"),
            dynamic_tag: String::new(),
            windows: vec![w1_bytes, w2_bytes],
            action: TimerAction::Set {
                fire_timestamp: Instant::from_millis(15_000),
                hold_timestamp: Instant::from_millis(9_000),
                pane: PaneInfo::NO_FIRING,
            },
        };
        let mut ctx_timer = HandlerContext::new(&mut sink).with_state_backend(Some(&backend));
        handler
            .on_timer("flush", &multi_timer, &mut ctx_timer)
            .expect("multi-window on_timer");
    }

    assert_eq!(
        sink.0,
        vec![(w1, format!("k:{w1}:10")), (w2, format!("k:{w2}:20")),]
    );
}
