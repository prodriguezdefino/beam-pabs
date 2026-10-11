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

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use tokio::sync::mpsc;

use beam::Instant;
use beam::coders::{
    Coder, Context, StringUtf8Coder, TimerAction, TimerCoder, TimerRecord, URN_TIMER,
    WindowedValue, WindowedValueCoder,
};
use beam::internals::HandlerContext;
use beam::internals::{BundleHandler, HandlerInstance, TransformFn};
use beam_harness::bundle_processor::BundleProcessor;
use beam_harness::control::ControlClient;
use beam_harness::data::DataManager;
use beam_model::fn_execution::elements::{Data, Timers};
use beam_model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleRequest, RegisterRequest,
    instruction_request::Request, instruction_response::Response,
};
use beam_model::pipeline as proto_pipeline;

use crate::common;
use common::{DescriptorBuilder, Gate, SINK_ID, SOURCE_ID, STAGE_ID};

const FAMILY: &str = "fam";
const TIMER_CODER: &str = "coder_timer";
const INSTRUCTION: &str = "inst_timer";

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
    processed: Arc<Gate>,
}

impl BundleHandler for TimerProbe {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let header = ctx.header().clone();
        ctx.sink.push_windowed(&header, element.to_vec())?;
        self.processed.open();
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer_family: &str,
        record: &TimerRecord,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let key = StringUtf8Coder
            .decode(&mut &record.user_key.as_slice()[..], Context::Nested)
            .map_err(|e| e.to_string())?;
        let TimerAction::Set { fire_timestamp, .. } = record.action else {
            return Err("a cleared timer is not delivered".to_string());
        };
        let text = format!(
            "timer:{timer_family}:{key}:{}:{}",
            record.dynamic_tag,
            fire_timestamp.as_millis()
        );
        let header = ctx.header().clone();
        ctx.sink.push_windowed(&header, encode_string(&text))
    }
}

fn descriptor() -> beam_model::fn_execution::ProcessBundleDescriptor {
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
    let processed = Gate::new();
    let handler: TransformFn = Arc::new(TimerProbe {
        processed: Arc::clone(&processed),
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
    while !processed.is_open() {
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

    // The bundle runs and its timer channel is registered.
    let mut timers = Vec::new();
    TimerCoder::encode(
        &TimerRecord::new_set(
            encode_string("k1"),
            "tag1",
            Instant::from_millis(5_000),
            Instant::from_millis(4_000),
        ),
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
        &TimerRecord::new_set(
            encode_string("k3"),
            "",
            Instant::from_millis(7_000),
            Instant::from_millis(7_000),
        ),
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
    let mut cursor = sink_bytes.as_slice();
    let mut emitted = Vec::new();
    while !cursor.is_empty() {
        let wv = coder.decode(&mut cursor, Context::Nested).unwrap();
        emitted.push((wv.value, wv.timestamp_millis));
    }
    assert_eq!(
        emitted,
        [
            ("a".to_string(), 1_000),
            // Timer output carries the hold timestamp, not the firing timestamp.
            ("timer:fam:k1:tag1:5000".to_string(), 4_000),
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
    let mut rx = data_manager.register_inbound_timers("inst");

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
    let mut rx = data_manager.register_inbound_timers("inst2");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_bytes_pass_through_the_inbound_channel_unchanged() {
    let (tx, _rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(tx);
    let mut rx = data_manager.register_inbound_timers("inst");

    // Simulate runner sending overwrite sequence
    let mut timers = Vec::new();
    TimerCoder::encode(
        &TimerRecord::new_set(
            encode_string("k1"),
            "tag1",
            Instant::from_millis(5_000),
            Instant::from_millis(4_000),
        ),
        &mut timers,
    )
    .unwrap();
    TimerCoder::encode(
        &TimerRecord::new_set(
            encode_string("k1"),
            "tag1",
            Instant::from_millis(9_000),
            Instant::from_millis(8_000),
        ),
        &mut timers,
    )
    .unwrap();

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
            timers: vec![chunk(&timers, true)],
        })
        .await;

    // The inbound timer channel delivers the timer bytes unchanged.
    let received = rx.recv().await.unwrap().unwrap();
    assert_eq!(received.timers, timers);
}

#[tokio::test]
async fn bundle_processor_threads_input_allowed_lateness_to_timer_bounds() {
    use beam::coders::{PaneInfo, URN_INTERVAL_WINDOW, URN_KV, URN_VARINT};
    use beam::internals::IntoParDoFn;
    use beam::prelude::{StatefulContext, StatefulDoFn, Timers};
    use beam::windowing::{BoundedWindow, IntervalWindow};

    #[derive(Timers)]
    enum LateTimer {
        #[timer(event_time, id = "fam")]
        At,
    }

    #[derive(Clone)]
    struct SetAtValue;

    impl StatefulDoFn for SetAtValue {
        type Key = String;
        type Value = i64;
        type Out = String;
        type State = ();
        type Timer = LateTimer;

        fn process_element(
            &mut self,
            _key: &String,
            target_ms: i64,
            _state: &mut (),
            ctx: &mut StatefulContext<'_, Self>,
        ) -> beam::Result {
            ctx.timer(LateTimer::At).at(Instant::from_millis(target_ms))
        }
    }

    let mut desc = descriptor();
    desc.coders.insert(
        "coder_varint".to_string(),
        proto_pipeline::Coder {
            spec: Some(proto_pipeline::FunctionSpec {
                urn: URN_VARINT.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );
    desc.coders.insert(
        "coder_kv".to_string(),
        proto_pipeline::Coder {
            spec: Some(proto_pipeline::FunctionSpec {
                urn: URN_KV.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: vec!["coder_string".to_string(), "coder_varint".to_string()],
        },
    );
    desc.coders.insert(
        "coder_interval_window".to_string(),
        proto_pipeline::Coder {
            spec: Some(proto_pipeline::FunctionSpec {
                urn: URN_INTERVAL_WINDOW.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );
    desc.coders.insert(
        "coder_wv_kv_interval".to_string(),
        proto_pipeline::Coder {
            spec: Some(proto_pipeline::FunctionSpec {
                urn: beam::coders::URN_WINDOWED_VALUE.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: vec!["coder_kv".to_string(), "coder_interval_window".to_string()],
        },
    );
    desc.coders
        .get_mut(TIMER_CODER)
        .expect("timer coder")
        .component_coder_ids[1] = "coder_interval_window".to_string();
    // Source port uses coder_wv_kv_interval so inbound interval windows decode cleanly.
    let source = desc.transforms.get_mut(SOURCE_ID).expect("source");
    let mut source_port = beam_model::fn_execution::RemoteGrpcPort::decode(
        source.spec.as_ref().unwrap().payload.as_slice(),
    )
    .expect("source port");
    source_port.coder_id = "coder_wv_kv_interval".to_string();
    source.spec.as_mut().unwrap().payload = source_port.encode_to_vec();

    desc.windowing_strategies.insert(
        "ws_late".to_string(),
        proto_pipeline::WindowingStrategy {
            window_coder_id: "coder_interval_window".to_string(),
            allowed_lateness: 5_000,
            ..Default::default()
        },
    );
    let input = desc
        .pcollections
        .entry("pcoll_input".to_string())
        .or_default();
    input.coder_id = "coder_kv".to_string();
    input.windowing_strategy_id = "ws_late".to_string();

    let (data_out_tx, _data_out) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let handler: TransformFn = SetAtValue.into_handler();
    let control = ControlClient::new(Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        HashMap::from([(STAGE_ID.to_string(), handler)]),
    )));
    control
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_late".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![desc],
            })),
        })
        .await;

    let encode_elem = |target_ms: i64| {
        let win = BoundedWindow::Interval(IntervalWindow::new(
            Instant::EPOCH,
            Instant::from_millis(10_000),
        ));
        let mut win_bytes = Vec::new();
        win.encode(&mut win_bytes).expect("encode window");
        let header = beam::coders::WindowedHeader::new(5_000, &[win_bytes], PaneInfo::NO_FIRING);
        let mut bytes = header.as_bytes().to_vec();
        beam::coders::DefaultCoder::encode_element(&("k".to_string(), target_ms), &mut bytes)
            .expect("encode kv");
        bytes
    };

    // Window [0, 10_000) has max_timestamp 9_999; allowed_lateness 5_000 extends expiry to 14_999.
    let run_one = |inst: &str, target_ms: i64| {
        let dm = data_manager.clone();
        let ctrl = control.clone();
        let inst = inst.to_string();
        let payload = encode_elem(target_ms);
        async move {
            dm.handle_inbound_elements(Elements {
                data: vec![Data {
                    instruction_id: inst.clone(),
                    transform_id: SOURCE_ID.to_string(),
                    data: payload,
                    is_last: true,
                }],
                timers: vec![Timers {
                    instruction_id: inst.clone(),
                    transform_id: STAGE_ID.to_string(),
                    timer_family_id: FAMILY.to_string(),
                    timers: Vec::new(),
                    is_last: true,
                }],
            })
            .await;
            ctrl.handle_instruction(InstructionRequest {
                instruction_id: inst,
                request: Some(Request::ProcessBundle(ProcessBundleRequest {
                    process_bundle_descriptor_id: "desc_timer".to_string(),
                    ..Default::default()
                })),
            })
            .await
        }
    };

    let ok_resp = run_one("inst_ok", 14_999).await;
    assert!(
        ok_resp.error.is_empty(),
        "14_999 is within allowed lateness: {}",
        ok_resp.error
    );
    assert!(matches!(ok_resp.response, Some(Response::ProcessBundle(_))));

    let err_resp = run_one("inst_err", 15_000).await;
    assert!(
        err_resp.error.contains("expiration"),
        "expected window expiration error, got: {}",
        err_resp.error
    );
}
