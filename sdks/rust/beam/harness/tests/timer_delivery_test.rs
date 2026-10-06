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

//! Inbound timers delivered to a bundle that is already running.
//!
//! Other tests send timers before the bundle registers its channels (the early-buffer path).
//! Here the timer arrives after an element is processed, through the registered timer channel.
#![expect(clippy::unwrap_used, reason = "integration test helpers")]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Condvar, Mutex};

use prost::Message;
use tokio::sync::mpsc;

use beam::coders::{
    Coder, Context, StringUtf8Coder, TimerCoder, TimerRecord, URN_TIMER, WindowedValue,
    WindowedValueCoder,
};
use beam::internals::HandlerContext;
use beam::internals::{BundleHandler, HandlerInstance, TransformFn};
use harness::bundle_processor::BundleProcessor;
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::elements::{Data, Timers};
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleRequest, RegisterRequest,
    instruction_request::Request, instruction_response::Response,
};
use model::pipeline as proto_pipeline;

mod common;
use common::{DescriptorBuilder, SINK_ID, SOURCE_ID, STAGE_ID};

const FAMILY: &str = "fam";
const TIMER_CODER: &str = "coder_timer";
const INSTRUCTION: &str = "inst_timer";

type Gate = Arc<(Mutex<bool>, Condvar)>;

fn encode_string(value: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    StringUtf8Coder
        .encode(&value.to_string(), &mut bytes, Context::Nested)
        .unwrap();
    bytes
}

/// Emits each element as-is and each timer firing as `timer:<family>:<key>:<tag>:<ts>`.
/// Opens `processed` once the first element has been handled.
#[derive(Clone)]
struct TimerProbe {
    processed: Gate,
}

impl BundleHandler for TimerProbe {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let header = ctx.header().clone();
        ctx.sink.push_windowed(&header, element.to_vec())?;
        let (lock, cvar) = &*self.processed;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer_family: &str,
        record: &TimerRecord,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let key = StringUtf8Coder
            .decode(
                &mut Cursor::new(record.user_key.as_slice()),
                Context::Nested,
            )
            .map_err(|e| e.to_string())?;
        let text = format!(
            "timer:{timer_family}:{key}:{}:{}",
            record.dynamic_tag, record.fire_timestamp
        );
        let header = ctx.header().clone();
        ctx.sink.push_windowed(&header, encode_string(&text))
    }
}

fn descriptor() -> model::fn_execution::ProcessBundleDescriptor {
    let mut descriptor = DescriptorBuilder::new("desc_timer")
        .with_coder(
            TIMER_CODER,
            URN_TIMER,
            &["coder_string", "coder_global_window"],
        )
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    let stage = descriptor.transforms.get_mut(STAGE_ID).expect("stage");
    let spec = stage.spec.as_mut().expect("stage spec");
    // Keep the builder's `do_fn` (the handler key); only add the timer family.
    let pardo = proto_pipeline::ParDoPayload {
        do_fn: proto_pipeline::ParDoPayload::decode(spec.payload.as_slice())
            .expect("stage payload is a ParDoPayload")
            .do_fn,
        timer_family_specs: HashMap::from([(
            FAMILY.to_string(),
            proto_pipeline::TimerFamilySpec {
                timer_family_coder_id: TIMER_CODER.to_string(),
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    spec.payload = pardo.encode_to_vec();
    descriptor
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timer_arriving_mid_bundle_is_delivered_to_its_transform() {
    let (data_out_tx, mut data_out) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let processed: Gate = Arc::new((Mutex::new(false), Condvar::new()));
    let handler: TransformFn = Arc::new(TimerProbe {
        processed: processed.clone(),
    });
    let control = ControlClient::new(Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        HashMap::from([(STAGE_ID.to_string(), handler)]),
    )));
    control
        .handle_instruction(InstructionRequest {
            instruction_id: "reg".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor()],
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
                        process_bundle_descriptor_id: "desc_timer".to_string(),
                        ..Default::default()
                    })),
                })
                .await
        }
    });

    // One element, without closing the data stream, so the bundle stays running.
    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut element = Vec::new();
    coder
        .encode(
            &WindowedValue::global("a".to_string(), 1_000),
            &mut element,
            Context::Nested,
        )
        .unwrap();
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![Data {
                instruction_id: INSTRUCTION.to_string(),
                transform_id: SOURCE_ID.to_string(),
                data: element,
                is_last: false,
            }],
            timers: vec![],
        })
        .await;
    // Wait for the element; fail, not hang, if the bundle ends first (e.g. unresolved handler).
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while !*processed.0.lock().unwrap() {
        if bundle.is_finished() {
            let response = bundle.await.expect("bundle task");
            panic!(
                "bundle ended before processing its element: {}",
                response.error
            );
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "element not processed within 30s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // The bundle is now running with its timer channel registered.
    let mut timers = Vec::new();
    TimerCoder::encode(
        &TimerRecord::new_set(encode_string("k1"), "tag1", 5_000, 4_000),
        &mut timers,
    )
    .unwrap();
    // A cleared timer is skipped, not delivered.
    TimerCoder::encode(
        &TimerRecord::new_clear(encode_string("k2"), ""),
        &mut timers,
    )
    .unwrap();
    TimerCoder::encode(
        &TimerRecord::new_set(encode_string("k3"), "", 7_000, 7_000),
        &mut timers,
    )
    .unwrap();
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![Data {
                instruction_id: INSTRUCTION.to_string(),
                transform_id: SOURCE_ID.to_string(),
                data: vec![],
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
    let mut outbound_timers = Vec::new();
    while let Ok(elements) = data_out.try_recv() {
        for data in elements.data {
            assert_eq!(data.transform_id, SINK_ID);
            sink_bytes.extend_from_slice(&data.data);
        }
        outbound_timers.extend(elements.timers);
    }
    let mut cursor = Cursor::new(sink_bytes.as_slice());
    let mut emitted = Vec::new();
    while (cursor.position() as usize) < sink_bytes.len() {
        let wv = coder.decode(&mut cursor, Context::Nested).unwrap();
        emitted.push((wv.value, wv.timestamp_millis));
    }
    assert_eq!(
        emitted,
        [
            ("a".to_string(), 1_000),
            // Timer output is stamped with the firing timestamp.
            ("timer:fam:k1:tag1:5000".to_string(), 5_000),
            ("timer:fam:k3::7000".to_string(), 7_000),
        ]
    );

    // The handler set no timers, so the family's outbound stream is only closed.
    assert_eq!(
        outbound_timers,
        [Timers {
            instruction_id: INSTRUCTION.to_string(),
            transform_id: STAGE_ID.to_string(),
            timer_family_id: FAMILY.to_string(),
            timers: vec![],
            is_last: true,
        }]
    );
}

#[tokio::test]
async fn timers_route_to_a_registered_channel_in_order_then_end_of_stream() {
    let (tx, _rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(tx);
    let mut rx = data_manager.register_inbound_timers("inst").await;

    let chunk = |bytes: &[u8], is_last| Timers {
        instruction_id: "inst".to_string(),
        transform_id: "t".to_string(),
        timer_family_id: "f".to_string(),
        timers: bytes.to_vec(),
        is_last,
    };
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![],
            timers: vec![chunk(b"one", false), chunk(b"two", true)],
        })
        .await;

    assert_eq!(rx.recv().await, Some(Some(chunk(b"one", false))));
    assert_eq!(rx.recv().await, Some(Some(chunk(b"two", true))));
    assert_eq!(rx.recv().await, Some(None), "is_last ends the timer stream");

    // An empty final chunk carries only the end-of-stream marker.
    let mut rx = data_manager.register_inbound_timers("inst2").await;
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![],
            timers: vec![Timers {
                instruction_id: "inst2".to_string(),
                is_last: true,
                ..Default::default()
            }],
        })
        .await;
    assert_eq!(rx.recv().await, Some(None));
    assert!(
        rx.try_recv().is_err(),
        "nothing but the end-of-stream marker"
    );
}
