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
    DefaultCoder, TimerCoder, TimerRecord, URN_GLOBAL_WINDOW, URN_STRING_UTF8, URN_TIMER,
};
use beam::internals::ElementSink;
use beam::internals::TimerCollector;
use beam::pipeline::Pipeline;
use beam::pipeline::constants::URN_REQUIREMENT_STATEFUL;
use beam::transforms::{Create, DoFn, ParDo, ProcessContext};
use beam::transforms::{TimeDomain, Timer, TimerFamilySpec};
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
    let set_record =
        TimerRecord::new_set(user_key.clone(), "tag_alpha", 1_700_000_000, 1_699_999_990);
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
fn test_timer_collector_and_process_context_bindings() {
    let collector = Arc::new(TimerCollector::new());

    let timer_spec = TimerFamilySpec::processing_time("inactivity_timer");
    let mut captured = Vec::new();
    struct MockSink<'a>(&'a mut Vec<Vec<u8>>);
    impl ElementSink for MockSink<'_> {
        fn push(&mut self, elem: Vec<u8>) -> Result<(), String> {
            self.0.push(elem);
            Ok(())
        }
        fn push_tagged(&mut self, _tag: &str, elem: Vec<u8>) -> Result<(), String> {
            self.0.push(elem);
            Ok(())
        }
    }

    let mut sink = MockSink(&mut captured);
    let ctx = ProcessContext::<String>::new(&mut sink)
        .with_timer_collector(&collector)
        .with_key_bytes(b"key1".to_vec());

    let timer = ctx.timer(&timer_spec).unwrap();
    assert_eq!(timer.family(), "inactivity_timer");
    assert_eq!(timer.time_domain(), TimeDomain::ProcessingTime);

    // Setting relative timer
    timer.set_relative(Duration::from_millis(5000));

    // Dynamic tagged timer
    let tagged = ctx.timer(&timer_spec).unwrap().tag("session_tag");
    assert_eq!(tagged.dynamic_tag(), "session_tag");
    tagged.clear();

    let drained = collector.drain_records(b"key1", b"");
    assert_eq!(drained.len(), 2);
    assert!(!drained[0].clear);
    assert_eq!(drained[0].dynamic_tag, "");
    assert!(drained[1].clear);
    assert_eq!(drained[1].dynamic_tag, "session_tag");
}

/// Projects the fields `drain_records` fills in, plus the set/clear payload.
fn summary(record: &TimerRecord) -> (&str, &[u8], &[Vec<u8>], bool, i64, i64) {
    (
        record.dynamic_tag.as_str(),
        &record.user_key,
        &record.windows,
        record.clear,
        record.fire_timestamp,
        record.hold_timestamp,
    )
}

#[test]
fn drained_timers_take_the_element_key_and_window_only_when_unbound() {
    let collector = Arc::new(TimerCollector::new());
    collector.set("f", "unbound", 10, 5);
    collector.clear("f", "cleared");
    Timer::with_context(
        "f",
        "bound",
        b"own-key".to_vec(),
        b"own-window".to_vec(),
        TimeDomain::EventTime,
        Arc::clone(&collector),
    )
    .set_with_hold(30, 20);

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

#[derive(Clone)]
struct StatefulTimerDoFn;

impl DoFn for StatefulTimerDoFn {
    type In = (String, i64);
    type Out = (String, i64);

    fn process_element(
        &mut self,
        elem: Self::In,
        ctx: &mut ProcessContext<Self::Out>,
    ) -> beam::Result {
        let timer_spec = TimerFamilySpec::event_time("event_timer");
        let timer = ctx.timer(&timer_spec)?;
        timer.set(elem.1 + 1000);
        ctx.emit(elem)
    }
}

#[test]
fn test_timer_transform_populates_proto_and_pipeline_requirements() {
    let pipeline = Pipeline::new();
    let words = pipeline.apply(Create::new("Create", vec![("key".to_string(), 100i64)]));

    let timer_spec = TimerFamilySpec::event_time("event_timer");
    let _ =
        words.apply(ParDo::new("timer_pardo", StatefulTimerDoFn).with_timer_family(&timer_spec));

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
