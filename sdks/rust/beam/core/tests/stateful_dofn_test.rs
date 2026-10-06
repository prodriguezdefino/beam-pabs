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

use beam::coders::{DefaultCoder, TimerRecord};
use beam::internals::{
    HandlerContext, IntoParDoFn, TypedElement, UserStateBackend, UserStateError,
};
use beam::pipeline::Pipeline;
use beam::prelude::*;
use beam::transforms::WithFinalizer;
use beam::transforms::display_data::{DisplayDataBuilder, DisplayDataItem};
use beam::windowing::GlobalWindow;
use model::pipeline as proto;
use prost::Message;

#[derive(Timers)]
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
    fn get_state(&self, _: &str, _: &[u8], _: &[u8]) -> Unused<Vec<Vec<u8>>> {
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
