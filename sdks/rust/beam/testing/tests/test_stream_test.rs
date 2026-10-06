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

#![expect(
    clippy::unwrap_used,
    reason = "test helpers; a failure is a test failure"
)]

//! Runner API translation of `TestStream`.

use std::time::Duration;

use beam::coders::{URN_BYTES, URN_LENGTH_PREFIX};
use beam::pipeline::URN_TEST_STREAM;
use beam::prelude::*;
use beam::windowing::BEAM_MIN_TIMESTAMP;
use model::pipeline as proto;
use model::pipeline::test_stream_payload::event::Event as EventKind;
use prost::Message;
use testing::{TestStream, WATERMARK_INFINITY_MILLIS};

/// Applies `stream` to a fresh pipeline and returns the pipeline and its components.
fn expand(stream: TestStream<String>) -> (Pipeline, PCollection<String>, proto::Components) {
    let p = Pipeline::new();
    let out = p.apply(stream);
    let components = p.to_proto().components.unwrap();
    (p, out, components)
}

fn primitive(components: &proto::Components) -> &proto::PTransform {
    let mut found = components
        .transforms
        .values()
        .filter(|t| t.spec.as_ref().is_some_and(|s| s.urn == URN_TEST_STREAM));
    let primitive = found.next().expect("no TestStream primitive");
    assert!(found.next().is_none(), "more than one TestStream primitive");
    primitive
}

fn payload(components: &proto::Components) -> proto::TestStreamPayload {
    let spec = primitive(components).spec.as_ref().unwrap();
    proto::TestStreamPayload::decode(spec.payload.as_slice()).unwrap()
}

fn describe(event: &proto::test_stream_payload::Event) -> String {
    match event.event.as_ref().unwrap() {
        EventKind::ElementEvent(add) => format!("elements:{}", add.elements.len()),
        EventKind::WatermarkEvent(w) => format!("watermark:{}", w.new_watermark),
        EventKind::ProcessingTimeEvent(p) => format!("processing:{}", p.advance_duration),
    }
}

fn element_timestamps(payload: &proto::TestStreamPayload) -> Vec<i64> {
    payload
        .events
        .iter()
        .filter_map(|e| match e.event.as_ref()? {
            EventKind::ElementEvent(add) => Some(add.elements.iter().map(|el| el.timestamp)),
            _ => None,
        })
        .flatten()
        .collect()
}

#[test]
fn events_follow_the_script_in_order() {
    let (_, _, components) = expand(
        TestStream::new("TestStream")
            .add_timestamped_elements([("a".to_string(), 5)])
            .advance_watermark_to(10)
            .advance_processing_time(Duration::from_secs(2))
            .advance_watermark_to_infinity(),
    );
    let events: Vec<String> = payload(&components).events.iter().map(describe).collect();
    assert_eq!(
        events,
        vec![
            "elements:1".to_string(),
            "watermark:10".to_string(),
            "processing:2000".to_string(),
            format!("watermark:{WATERMARK_INFINITY_MILLIS}"),
        ]
    );
}

#[test]
fn elements_are_length_prefixed_encodings() {
    let long = "x".repeat(200);
    let (_, _, components) = expand(
        TestStream::new("TestStream")
            .add_timestamped_elements([("hi".to_string(), 7), (long.clone(), 8)]),
    );
    let payload = payload(&components);
    let Some(EventKind::ElementEvent(add)) = payload.events[0].event.as_ref() else {
        panic!("expected an element event");
    };

    // Golden bytes: the varint length of the element encoding, then the element
    // encoding. For a `String`, that encoding is its own varint length and its UTF-8.
    assert_eq!(add.elements[0].encoded_element, [3, 2, b'h', b'i']);
    assert_eq!(add.elements[0].timestamp, 7);

    // Two-byte varint lengths: 202 is 0xCA 0x01, 200 is 0xC8 0x01.
    let mut expected = vec![0xCA, 0x01, 0xC8, 0x01];
    expected.extend(long.as_bytes());
    assert_eq!(add.elements[1].encoded_element, expected);
    assert_eq!(add.elements[1].timestamp, 8);
}

#[test]
fn payload_coder_is_length_prefixed_bytes() {
    let (_, _, components) = expand(TestStream::new("TestStream").add_elements(["x".to_string()]));
    let coder = &components.coders[&payload(&components).coder_id];
    assert_eq!(coder.spec.as_ref().unwrap().urn, URN_LENGTH_PREFIX);
    let inner = &components.coders[&coder.component_coder_ids[0]];
    assert_eq!(inner.spec.as_ref().unwrap().urn, URN_BYTES);

    let raw_output = primitive(&components).outputs.values().next().unwrap();
    assert_eq!(
        components.pcollections[raw_output].coder_id,
        payload(&components).coder_id
    );
}

#[test]
fn add_elements_uses_the_current_watermark() {
    let stream = TestStream::new("TestStream")
        .add_elements(["early".to_string()])
        .advance_watermark_to(42)
        .add_elements(["later".to_string()]);
    assert_eq!(stream.current_watermark(), 42);

    let (_, _, components) = expand(stream);
    assert_eq!(
        element_timestamps(&payload(&components)),
        vec![BEAM_MIN_TIMESTAMP, 42]
    );
}

#[test]
fn empty_element_batches_are_skipped() {
    let (_, _, components) = expand(
        TestStream::new("TestStream")
            .add_elements(Vec::<String>::new())
            .advance_watermark_to(1),
    );
    let events: Vec<String> = payload(&components).events.iter().map(describe).collect();
    assert_eq!(events, vec!["watermark:1".to_string()]);
}

#[test]
fn primitive_is_runner_executed_and_output_is_unbounded() {
    let (p, out, components) = expand(TestStream::new("Events").add_elements(["x".to_string()]));

    assert!(primitive(&components).environment_id.is_empty());
    assert!(primitive(&components).inputs.is_empty());

    let composite = &components.transforms["Events"];
    assert_eq!(
        composite.outputs.values().collect::<Vec<_>>(),
        vec![out.id()]
    );
    assert_eq!(composite.subtransforms.len(), 2);
    assert_eq!(
        components.pcollections[out.id()].is_bounded,
        proto::is_bounded::Enum::Unbounded as i32
    );
    p.validate().unwrap();
}

#[test]
#[should_panic(expected = "cannot move backwards")]
fn watermark_cannot_recede() {
    let _ = TestStream::<String>::new("TestStream")
        .advance_watermark_to(10)
        .advance_watermark_to(5);
}

#[test]
#[should_panic(expected = "at least one millisecond")]
fn processing_time_must_advance() {
    let _ =
        TestStream::<String>::new("TestStream").advance_processing_time(Duration::from_micros(10));
}
