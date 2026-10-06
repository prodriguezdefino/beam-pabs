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

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::{
    DefaultCoder, TimerAction, TimerCoder, TimerRecord, URN_GLOBAL_WINDOW, URN_STRING_UTF8,
    URN_TIMER,
};
use beam::internals::{TimerCollector, TimerFamilySpec};
use beam::pipeline::Pipeline;
use beam::pipeline::URN_REQUIREMENT_STATEFUL;
use beam::prelude::*;
use beam::transforms::{Create, ParDo};
use model::pipeline::time_domain::Enum as TimeDomainProto;
use model::pipeline::{Coder as ProtoCoder, FunctionSpec};

#[test]
fn test_timer_coder_roundtrip_set_and_clear() {
    let mut coders = HashMap::new();
    coders.insert(
        "key_coder".to_string(),
        ProtoCoder {
            spec: Some(FunctionSpec {
                urn: URN_STRING_UTF8.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );
    coders.insert(
        "window_coder".to_string(),
        ProtoCoder {
            spec: Some(FunctionSpec {
                urn: URN_GLOBAL_WINDOW.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );

    // Timer set record roundtrip.
    let user_key = "user_42".to_string().encode().unwrap();
    let set_record = TimerRecord::new_set(
        user_key.clone(),
        "tag_alpha",
        Instant::from_millis(1_700_000_000),
        Instant::from_millis(1_699_999_990),
    );
    let mut buf = Vec::new();
    TimerCoder::encode(&set_record, &mut buf).unwrap();

    let mut cursor = Cursor::new(buf.as_slice());
    let decoded_set =
        TimerCoder::decode(&mut cursor, "key_coder", "window_coder", &coders).unwrap();
    assert_eq!(decoded_set, set_record);
    assert_eq!(cursor.position() as usize, buf.len());

    // Timer clear record roundtrip: clear bit 1, fields 5-7 omitted.
    let clear_record = TimerRecord::new_clear(user_key, "tag_beta");
    let mut clear_buf = Vec::new();
    TimerCoder::encode(&clear_record, &mut clear_buf).unwrap();

    let mut clear_cursor = Cursor::new(clear_buf.as_slice());
    let decoded_clear =
        TimerCoder::decode(&mut clear_cursor, "key_coder", "window_coder", &coders).unwrap();
    assert_eq!(decoded_clear, clear_record);
    assert_eq!(clear_cursor.position() as usize, clear_buf.len());

    // The clear byte is the last byte of a clear record. Only 0 and 1 are valid (C3-F18).
    let mut bad_buf = clear_buf.clone();
    *bad_buf.last_mut().unwrap() = 0x02;
    let err = TimerCoder::decode(
        &mut Cursor::new(bad_buf.as_slice()),
        "key_coder",
        "window_coder",
        &coders,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("Invalid timer clear byte: 2"),
        "{err}"
    );
}

#[test]
fn a_timer_window_count_of_zero_is_valid_and_a_negative_one_is_not() {
    let coder = |urn: &str| ProtoCoder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    };
    let coders = HashMap::from([
        ("key_coder".to_string(), coder(URN_STRING_UTF8)),
        ("window_coder".to_string(), coder(URN_GLOBAL_WINDOW)),
    ]);
    let decode = |bytes: &[u8]| {
        TimerCoder::decode(
            &mut Cursor::new(bytes),
            "key_coder",
            "window_coder",
            &coders,
        )
    };

    let mut windowless = TimerRecord::new_clear("k".to_string().encode().unwrap(), "");
    windowless.windows.clear();
    let mut buf = Vec::new();
    TimerCoder::encode(&windowless, &mut buf).unwrap();
    assert_eq!(
        decode(&buf).expect("a timer may carry no windows"),
        windowless
    );

    // The same record claiming -1 windows: the count follows the key and the empty tag.
    let count_at = buf.len() - 5;
    buf[count_at..count_at + 4].copy_from_slice(&(-1i32).to_be_bytes());
    let err = decode(&buf).expect_err("a negative window count is malformed");
    assert_eq!(err.to_string(), "Negative timer window count: -1");
}

#[test]
fn a_timer_pane_with_the_metadata_flag_is_read_past_its_metadata() {
    let coder = |urn: &str| ProtoCoder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    };
    let coders = HashMap::from([
        ("key_coder".to_string(), coder(URN_STRING_UTF8)),
        ("window_coder".to_string(), coder(URN_GLOBAL_WINDOW)),
    ]);
    let record = TimerRecord::new_set(
        "k".to_string().encode().unwrap(),
        "t",
        Instant::from_millis(10),
        Instant::from_millis(5),
    );
    let mut buf = Vec::new();
    TimerCoder::encode(&record, &mut buf).unwrap();
    // The pane is the last byte and has no indices. Flag metadata and append a blob.
    let pane = buf.last_mut().unwrap();
    assert_eq!(*pane & 0xF0, 0, "the test expects a one-byte pane");
    *pane |= 0x80;
    buf.extend_from_slice(&[0x02, 0xAA, 0xBB]);
    let body_len = buf.len();
    buf.push(0xEE); // sentinel

    let mut cursor = Cursor::new(buf.as_slice());
    let decoded = TimerCoder::decode(&mut cursor, "key_coder", "window_coder", &coders).unwrap();
    assert_eq!(decoded, record);
    assert_eq!(cursor.position() as usize, body_len);
}

#[derive(Timers)]
enum InactivityTimers {
    #[timer(processing_time, id = "inactivity_timer")]
    Inactive,
    #[timer(processing_time, id = "tagged_timer")]
    InactiveTagged(String),
}

/// Sets a relative timer and clears a tagged one for each element. It has no state.
#[derive(Clone)]
struct InactivityFn;

impl StatefulDoFn for InactivityFn {
    type Key = String;
    type Value = ();
    type Out = String;
    type State = ();
    type Timer = InactivityTimers;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(InactivityTimers::Inactive)
            .after(Duration::from_millis(5000))?;
        ctx.timer(InactivityTimers::InactiveTagged("session_tag".to_string()))
            .clear()
    }
}

#[test]
fn timers_are_set_for_the_key_of_the_element_without_a_state_backend() {
    use beam::internals::{HandlerContext, IntoParDoFn};

    let collector = Arc::new(TimerCollector::new());
    let mut sink = Vec::<Vec<u8>>::new();
    let element = ("key1".to_string(), ()).encode().expect("encode");
    let mut handler = InactivityFn.into_handler().instantiate();
    {
        let mut ctx = HandlerContext::new(&mut sink).with_timer_collector(Some(&collector));
        handler.process(&element, &mut ctx).expect("process");
    }

    let key = "key1".to_string().encode().expect("encode");
    let drained = collector.drain_records(&key, b"");
    let (set, clear) = match drained.as_slice() {
        [a, b] if a.action != TimerAction::Clear => (a, b),
        [a, b] => (b, a),
        other => panic!("expected one set and one clear, got {}", other.len()),
    };
    assert!(matches!(set.action, TimerAction::Set { .. }));
    assert_eq!(set.dynamic_tag, "");
    assert_eq!(
        (clear.action, clear.dynamic_tag.as_str()),
        (TimerAction::Clear, "session_tag")
    );
    assert!(drained.iter().all(|record| record.user_key == key));
}

/// Projects the fields `drain_records` fills in, plus the set/clear payload: `(clear, fire,
/// hold)`, with zero timestamps for a clear.
fn summary(record: &TimerRecord) -> (&str, &[u8], &[Vec<u8>], bool, i64, i64) {
    let (clear, fire, hold) = match record.action {
        TimerAction::Clear => (true, 0, 0),
        TimerAction::Set {
            fire_timestamp,
            hold_timestamp,
            ..
        } => (
            false,
            fire_timestamp.as_millis(),
            hold_timestamp.as_millis(),
        ),
    };
    (
        record.dynamic_tag.as_str(),
        &record.user_key,
        &record.windows,
        clear,
        fire,
        hold,
    )
}

#[test]
fn drained_timers_take_the_element_key_and_window_only_when_unbound() {
    let collector = Arc::new(TimerCollector::new());
    collector.set(
        "f",
        "unbound",
        Instant::from_millis(10),
        Instant::from_millis(5),
    );
    collector.clear("f", "cleared");
    collector.set_with_context(
        "f",
        "bound",
        Some(b"own-key".to_vec()),
        Some(b"own-window".to_vec()),
        Instant::from_millis(30),
        Instant::from_millis(20),
    );

    let drained = collector.drain_records(b"key", b"window");
    let (window, own_window) = ([b"window".to_vec()], [b"own-window".to_vec()]);
    assert_eq!(
        drained.iter().map(summary).collect::<Vec<_>>(),
        vec![
            ("unbound", &b"key"[..], &window[..], false, 10, 5),
            ("cleared", &b"key"[..], &window[..], true, 0, 0),
            ("bound", &b"own-key"[..], &own_window[..], false, 30, 20),
        ]
    );
}

#[test]
fn timer_family_specs_carry_their_time_domain_into_the_proto() {
    for (spec, domain) in [
        (TimerFamilySpec::event_time("e"), TimeDomainProto::EventTime),
        (
            TimerFamilySpec::processing_time("p"),
            TimeDomainProto::ProcessingTime,
        ),
    ] {
        let proto = spec.to_proto("coder");
        assert_eq!(proto.time_domain, domain as i32, "{spec:?}");
        assert_eq!(proto.timer_family_coder_id, "coder");
    }
}

#[derive(Timers)]
enum EventTimers {
    #[timer(event_time, id = "event_timer")]
    Event,
}

#[derive(Clone)]
struct StatefulTimerDoFn;

impl StatefulDoFn for StatefulTimerDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = ();
    type Timer = EventTimers;

    fn process_element(
        &mut self,
        key: &Self::Key,
        val: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.timer(EventTimers::Event)
            .at(Instant::from_millis(val + 1000))?;
        ctx.emit((key.clone(), val))
    }
}

#[test]
fn test_timer_transform_populates_proto_and_pipeline_requirements() {
    let pipeline = Pipeline::new();
    let words = pipeline.apply(Create::new("Create", vec![("key".to_string(), 100i64)]));

    let _ = words.apply(ParDo::new("timer_pardo", StatefulTimerDoFn));

    let proto = pipeline.to_proto();
    assert!(
        proto
            .requirements
            .contains(&URN_REQUIREMENT_STATEFUL.to_string()),
        "Pipeline requirements must contain URN_REQUIREMENT_STATEFUL when timers are declared"
    );

    let components = proto.components.unwrap();
    let has_timer_coder = components
        .coders
        .values()
        .any(|c| c.spec.as_ref().map(|s| s.urn.as_str()) == Some(URN_TIMER));
    assert!(has_timer_coder, "Pipeline coders must contain URN_TIMER");
}
