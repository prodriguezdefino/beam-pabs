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

//! Timers that a bundle sets or clears: each goes to the transform that set it, a delivered
//! timer that the bundle changed is skipped, and a timer set earlier fires first.
#![expect(clippy::unwrap_used, reason = "integration test helpers")]

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use tokio::sync::mpsc;

use beam::Instant;
use beam::coders::{
    Coder, Context, StringUtf8Coder, TimerAction, TimerCoder, TimerRecord, URN_TIMER,
    WindowedValue, WindowedValueCoder,
};
use beam::internals::{BundleHandler, HandlerContext, HandlerInstance, TransformFn};
use harness::bundle_processor::BundleProcessor;
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::elements::{Data, Timers};
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleDescriptor, ProcessBundleRequest, RegisterRequest,
    instruction_request::Request, instruction_response::Response,
};
use model::pipeline as proto_pipeline;

mod common;
use common::{DescriptorBuilder, SINK_ID, SOURCE_ID, STAGE_ID};

const FAMILY: &str = "fam";
const TIMER_CODER: &str = "coder_timer";
const KEY_CODER: &str = "coder_string";
const WINDOW_CODER: &str = "coder_global_window";
const DESCRIPTOR: &str = "desc_tracking";
const INSTRUCTION: &str = "inst_tracking";
const SET_TAG_MILLIS: i64 = 5_500;

fn encode_string(value: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    StringUtf8Coder
        .encode(&value.to_string(), &mut bytes, Context::Nested)
        .unwrap();
    bytes
}

/// Sets timer `tag` of `FAMILY` at `SET_TAG_MILLIS` for the key and window of `record`.
fn set_at(ctx: &HandlerContext<'_>, record: &TimerRecord, tag: &str) -> Result<(), String> {
    let at = Instant::from_millis(SET_TAG_MILLIS);
    let set = TimerRecord {
        dynamic_tag: tag.to_string(),
        action: TimerAction::Set {
            fire_timestamp: at,
            hold_timestamp: at,
            pane: beam::coders::PaneInfo::NO_FIRING,
        },
        ..record.clone()
    };
    let collector = ctx.timer_collector().ok_or("no timer collector")?;
    collector.push(ctx.transform_id(), FAMILY, set);
    Ok(())
}

/// Emits `<tag>` for each fired timer, and changes timers of the same key and window:
/// - `a` clears `b` and sets `c` at `SET_TAG_MILLIS`;
/// - `loop_start` sets `loop` at `SET_TAG_MILLIS`, and `loop` sets itself again at that time.
#[derive(Clone)]
struct ChangesTimers;

impl BundleHandler for ChangesTimers {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, _element: &[u8], _ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        Ok(())
    }

    fn on_timer(
        &mut self,
        _timer_family: &str,
        record: &TimerRecord,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        match record.dynamic_tag.as_str() {
            "a" => {
                let collector = ctx.timer_collector().ok_or("no timer collector")?;
                let clear = TimerRecord {
                    dynamic_tag: "b".to_string(),
                    action: TimerAction::Clear,
                    ..record.clone()
                };
                collector.push(ctx.transform_id(), FAMILY, clear);
                set_at(ctx, record, "c")?;
            }
            "loop_start" | "loop" => set_at(ctx, record, "loop")?,
            _ => {}
        }
        let header = ctx.header().clone();
        ctx.sink
            .push_windowed(&header, encode_string(&record.dynamic_tag))
    }
}

/// Forwards each element and sets a timer of `FAMILY` whose tag is its transform id.
#[derive(Clone)]
struct TagsWithTransform;

impl BundleHandler for TagsWithTransform {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let at = Instant::from_millis(SET_TAG_MILLIS);
        let set = TimerRecord::new_set(encode_string("k"), ctx.transform_id(), at, at);
        let collector = ctx.timer_collector().ok_or("no timer collector")?;
        collector.push(ctx.transform_id(), FAMILY, set);
        let header = ctx.header().clone();
        ctx.sink.push_windowed(&header, element.to_vec())
    }
}

/// Declares the event-time timer family `FAMILY` on transform `id` of `descriptor`.
fn declare_family(descriptor: &mut ProcessBundleDescriptor, id: &str) {
    let stage = descriptor.transforms.get_mut(id).expect("stage");
    let spec = stage.spec.as_mut().expect("stage spec");
    let pardo = proto_pipeline::ParDoPayload {
        do_fn: proto_pipeline::ParDoPayload::decode(spec.payload.as_slice())
            .expect("stage payload is a ParDoPayload")
            .do_fn,
        timer_family_specs: HashMap::from([(
            FAMILY.to_string(),
            proto_pipeline::TimerFamilySpec {
                time_domain: proto_pipeline::time_domain::Enum::EventTime as i32,
                timer_family_coder_id: TIMER_CODER.to_string(),
            },
        )]),
        ..Default::default()
    };
    spec.payload = pardo.encode_to_vec();
}

fn fired(tag: &str, millis: i64) -> TimerRecord {
    let at = Instant::from_millis(millis);
    TimerRecord::new_set(encode_string("k"), tag, at, at)
}

/// What a bundle wrote: the elements with their timestamps, and the timers per transform as
/// `(tag, fire millis)`, `None` for a clear.
struct Written {
    elements: Vec<(String, i64)>,
    timers: Vec<(String, String, Option<i64>)>,
}

/// Runs one bundle of `descriptor`. It reads `element` (if any) and the timers `delivered` for
/// `STAGE_ID`.
async fn run_bundle(
    descriptor: ProcessBundleDescriptor,
    handlers: HashMap<String, TransformFn>,
    element: Option<&str>,
    delivered: &[TimerRecord],
) -> Written {
    let (data_out_tx, mut data_out) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let control = ControlClient::new(Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    )));
    control
        .handle_instruction(InstructionRequest {
            instruction_id: "reg".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor.clone()],
            })),
        })
        .await;
    let bundle = tokio::spawn({
        let control = control.clone();
        async move {
            control
                .handle_instruction(InstructionRequest {
                    instruction_id: INSTRUCTION.to_string(),
                    request: Some(Request::ProcessBundle(ProcessBundleRequest {
                        process_bundle_descriptor_id: DESCRIPTOR.to_string(),
                        ..Default::default()
                    })),
                })
                .await
        }
    });

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut data = Vec::new();
    if let Some(value) = element {
        coder
            .encode(
                &WindowedValue::global(value.to_string(), 1_000),
                &mut data,
                Context::Nested,
            )
            .unwrap();
    }
    let mut timers = Vec::new();
    delivered
        .iter()
        .for_each(|record| TimerCoder::encode(record, &mut timers).unwrap());
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![Data {
                instruction_id: INSTRUCTION.to_string(),
                transform_id: SOURCE_ID.to_string(),
                data,
                is_last: true,
            }],
            timers: vec![Timers {
                instruction_id: INSTRUCTION.to_string(),
                transform_id: STAGE_ID.to_string(),
                timer_family_id: FAMILY.to_string(),
                timers,
                is_last: true,
            }],
        })
        .await;

    let resp = bundle.await.unwrap();
    assert!(resp.error.is_empty(), "bundle failed: {}", resp.error);
    assert!(matches!(resp.response, Some(Response::ProcessBundle(_))));

    let mut sink_bytes = Vec::new();
    let mut written_timers = Vec::new();
    while let Ok(elements) = data_out.try_recv() {
        elements
            .data
            .iter()
            .for_each(|data| sink_bytes.extend_from_slice(&data.data));
        written_timers.extend(elements.timers.into_iter().flat_map(|chunk| {
            let mut cursor = chunk.timers.as_slice();
            let mut records = Vec::new();
            while !cursor.is_empty() {
                let record =
                    TimerCoder::decode(&mut cursor, KEY_CODER, WINDOW_CODER, &descriptor.coders)
                        .unwrap();
                let fire = match record.action {
                    TimerAction::Set { fire_timestamp, .. } => Some(fire_timestamp.as_millis()),
                    TimerAction::Clear => None,
                };
                records.push((chunk.transform_id.clone(), record.dynamic_tag, fire));
            }
            records
        }));
    }
    let mut cursor = sink_bytes.as_slice();
    let mut elements = Vec::new();
    while !cursor.is_empty() {
        let wv: WindowedValue<String> = coder.decode(&mut cursor, Context::Nested).unwrap();
        elements.push((wv.value, wv.timestamp_millis));
    }
    Written {
        elements,
        timers: written_timers,
    }
}

fn one_stage(handler: TransformFn) -> (ProcessBundleDescriptor, HashMap<String, TransformFn>) {
    let mut descriptor = DescriptorBuilder::new(DESCRIPTOR)
        .with_coder(TIMER_CODER, URN_TIMER, &[KEY_CODER, WINDOW_CODER])
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    declare_family(&mut descriptor, STAGE_ID);
    (descriptor, HashMap::from([(STAGE_ID.to_string(), handler)]))
}

fn timer(transform: &str, tag: &str, fire: Option<i64>) -> (String, String, Option<i64>) {
    (transform.to_string(), tag.to_string(), fire)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_timer_is_skipped_and_a_timer_set_earlier_fires_first() {
    let (descriptor, handlers) = one_stage(Arc::new(ChangesTimers));
    // `a` clears `b` and sets `c` at 5.5 s. `c` fires before `b` is reached, `b` is skipped,
    // and `d` fires last.
    let delivered = [fired("a", 5_000), fired("b", 6_000), fired("d", 7_000)];
    let written = run_bundle(descriptor, handlers, None, &delivered).await;

    assert_eq!(
        written.elements,
        [
            ("a".to_string(), 5_000),
            ("c".to_string(), SET_TAG_MILLIS),
            ("d".to_string(), 7_000),
        ]
    );
    // The clear of `c` keeps the runner from firing it again.
    assert_eq!(
        written.timers,
        [
            timer(STAGE_ID, "b", None),
            timer(STAGE_ID, "c", Some(SET_TAG_MILLIS)),
            timer(STAGE_ID, "c", None),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timer_that_sets_itself_at_the_same_time_fires_inline_once() {
    let (descriptor, handlers) = one_stage(Arc::new(ChangesTimers));
    let delivered = [fired("loop_start", 5_000), fired("z", 6_000)];
    let written = run_bundle(descriptor, handlers, None, &delivered).await;

    assert_eq!(
        written.elements,
        [
            ("loop_start".to_string(), 5_000),
            ("loop".to_string(), SET_TAG_MILLIS),
            ("z".to_string(), 6_000),
        ]
    );
    // The last set of `loop` goes to the runner.
    assert_eq!(
        written.timers,
        [
            timer(STAGE_ID, "loop", Some(SET_TAG_MILLIS)),
            timer(STAGE_ID, "loop", None),
            timer(STAGE_ID, "loop", Some(SET_TAG_MILLIS)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fused_transforms_with_the_same_family_id_get_their_own_timers() {
    const SECOND: &str = "second_transform";
    let mut descriptor = DescriptorBuilder::new(DESCRIPTOR)
        .with_coder(TIMER_CODER, URN_TIMER, &[KEY_CODER, WINDOW_CODER])
        .stage(STAGE_ID, "pcoll_input", "pcoll_mid")
        .stage(SECOND, "pcoll_mid", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    declare_family(&mut descriptor, STAGE_ID);
    declare_family(&mut descriptor, SECOND);
    let handler: TransformFn = Arc::new(TagsWithTransform);
    let handlers = HashMap::from([
        (STAGE_ID.to_string(), Arc::clone(&handler)),
        (SECOND.to_string(), handler),
    ]);
    let written = run_bundle(descriptor, handlers, Some("x"), &[]).await;

    assert_eq!(written.elements, [("x".to_string(), 1_000)]);
    let mut timers = written.timers;
    timers.sort();
    let mut expected = [
        timer(STAGE_ID, STAGE_ID, Some(SET_TAG_MILLIS)),
        timer(SECOND, SECOND, Some(SET_TAG_MILLIS)),
    ];
    expected.sort();
    assert_eq!(timers, expected);
}
