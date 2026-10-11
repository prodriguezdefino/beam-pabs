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

//! Multi-window inbound timer firing in `BundleProcessor` with per-window user state.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use tokio::sync::mpsc;

use beam::Instant;
use beam::coders::{
    Coder, Context, DefaultCoder, IntervalWindowCoder, PaneInfo, StringUtf8Coder, TimerAction,
    TimerCoder, TimerRecord, URN_INTERVAL_WINDOW, URN_KV, URN_TIMER, URN_VARINT,
    URN_WINDOWED_VALUE, WindowedHeader, WindowedValueCoder,
};
use beam::internals::{IntoParDoFn, TransformFn};
use beam::prelude::{State, StatefulContext, StatefulDoFn, Timers, ValueState};
use beam::windowing::{BoundedWindow, IntervalWindow};
use beam_harness::bundle_processor::BundleProcessor;
use beam_harness::control::ControlClient;
use beam_harness::data::DataManager;
use beam_model::fn_execution::elements::{Data, Timers as ElementTimers};
use beam_model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleDescriptor, ProcessBundleRequest, RegisterRequest,
    instruction_request::Request, instruction_response::Response,
};
use beam_model::pipeline::{self as proto_pipeline, ApiServiceDescriptor, FunctionSpec};

use crate::common::{DescriptorBuilder, SINK_ID, SOURCE_ID, STAGE_ID};
use crate::state_mock::StrictStateServer;

const FAMILY: &str = "fam";
const TIMER_CODER: &str = "coder_timer";

#[derive(State)]
struct WindowScoreState {
    score: ValueState<i64>,
}

#[derive(Timers)]
enum WindowTimer {
    #[timer(event_time, id = "fam")]
    Flush,
}

/// Writes per-window state in `process_element` and emits `<key>:<window>:<score>` on timer.
/// When `clear_score` matches an element value, clears `WindowTimer::Flush` for that window.
#[derive(Clone)]
struct PerWindowTimerFn {
    clear_score: Option<i64>,
}

impl StatefulDoFn for PerWindowTimerFn {
    type Key = String;
    type Value = i64;
    type Out = String;
    type State = WindowScoreState;
    type Timer = WindowTimer;

    fn process_element(
        &mut self,
        _key: &String,
        value: i64,
        state: &mut WindowScoreState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        state.score.write(&value)?;
        if self.clear_score == Some(value) {
            ctx.timer(WindowTimer::Flush).clear()?;
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer: WindowTimer,
        key: &String,
        state: &mut WindowScoreState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        match timer {
            WindowTimer::Flush => {
                let win = ctx.window()?;
                let score = state.score.read()?.unwrap_or(-1);
                ctx.emit(format!("{key}:{win}:{score}"))
            }
        }
    }
}

fn multi_window_descriptor(id: &str, state_endpoint: &str) -> ProcessBundleDescriptor {
    let mut descriptor = DescriptorBuilder::new(id)
        .with_coder("coder_varint", URN_VARINT, &[])
        .with_coder("coder_kv", URN_KV, &["coder_string", "coder_varint"])
        .with_coder("coder_interval_window", URN_INTERVAL_WINDOW, &[])
        .with_coder(
            "coder_wv_kv_interval",
            URN_WINDOWED_VALUE,
            &["coder_kv", "coder_interval_window"],
        )
        .with_coder(
            "coder_wv_string_interval",
            URN_WINDOWED_VALUE,
            &["coder_string", "coder_interval_window"],
        )
        .with_coder(
            TIMER_CODER,
            URN_TIMER,
            &["coder_string", "coder_interval_window"],
        )
        .with_source_coder("coder_wv_kv_interval")
        .windowing_strategy("ws_interval", "coder_interval_window")
        .pcollection("pcoll_input", "coder_kv", "ws_interval")
        .pcollection("pcoll_output", "coder_string", "ws_interval")
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink_with_coder(SINK_ID, "pcoll_output", "coder_wv_string_interval")
        .build();

    descriptor.state_api_service_descriptor = Some(ApiServiceDescriptor {
        url: state_endpoint.to_string(),
        ..Default::default()
    });

    let stage = descriptor.transforms.get_mut(STAGE_ID).expect("stage");
    let spec = stage.spec.as_mut().expect("stage spec");
    let do_fn = proto_pipeline::ParDoPayload::decode(spec.payload.as_slice())
        .expect("stage payload is a ParDoPayload")
        .do_fn;
    let score_spec = proto_pipeline::StateSpec {
        protocol: Some(FunctionSpec {
            urn: "beam:user_state:bag:v1".to_string(),
            payload: Vec::new(),
        }),
        spec: Some(proto_pipeline::state_spec::Spec::ReadModifyWriteSpec(
            proto_pipeline::ReadModifyWriteStateSpec {
                coder_id: "coder_varint".to_string(),
            },
        )),
    };
    let pardo = proto_pipeline::ParDoPayload {
        do_fn,
        state_specs: HashMap::from([("score".to_string(), score_spec)]),
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
    descriptor
}

fn encode_window(window: IntervalWindow) -> Vec<u8> {
    let mut bytes = Vec::new();
    BoundedWindow::Interval(window)
        .encode(&mut bytes)
        .expect("encode interval window");
    bytes
}

fn encode_kv_element(window: IntervalWindow, timestamp_ms: i64, key: &str, value: i64) -> Vec<u8> {
    let header = WindowedHeader::new(timestamp_ms, &[encode_window(window)], PaneInfo::NO_FIRING);
    let mut bytes = header.as_bytes().to_vec();
    DefaultCoder::encode_element(&(key.to_string(), value), &mut bytes).expect("encode kv");
    bytes
}

async fn run_multi_window_bundle(
    desc_id: &str,
    inst_id: &str,
    clear_score: Option<i64>,
    w1: IntervalWindow,
    w2: IntervalWindow,
) -> Vec<(String, Vec<IntervalWindow>, i64)> {
    let server = StrictStateServer::start();
    let (data_out_tx, mut data_out) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let handler: TransformFn = PerWindowTimerFn { clear_score }.into_handler();
    let control = ControlClient::new(Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        HashMap::from([(STAGE_ID.to_string(), handler)]),
    )));

    control
        .handle_instruction(InstructionRequest {
            instruction_id: format!("reg_{inst_id}"),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![multi_window_descriptor(
                    desc_id,
                    server.endpoint(),
                )],
            })),
        })
        .await;

    let mut data_bytes = encode_kv_element(w1, 2_000, "k", 10);
    data_bytes.extend_from_slice(&encode_kv_element(w2, 7_000, "k", 20));

    let mut timer_bytes = Vec::new();
    TimerCoder::encode(
        &TimerRecord {
            user_key: DefaultCoder::encode(&"k".to_string()).expect("encode key"),
            dynamic_tag: String::new(),
            windows: vec![encode_window(w1), encode_window(w2)],
            action: TimerAction::Set {
                fire_timestamp: Instant::from_millis(15_000),
                hold_timestamp: Instant::from_millis(9_000),
                pane: PaneInfo::NO_FIRING,
            },
        },
        &mut timer_bytes,
    )
    .expect("encode multi-window timer");

    data_manager
        .handle_inbound_elements(Elements {
            data: vec![Data {
                instruction_id: inst_id.to_string(),
                transform_id: SOURCE_ID.to_string(),
                data: data_bytes,
                is_last: true,
            }],
            timers: vec![ElementTimers {
                instruction_id: inst_id.to_string(),
                transform_id: STAGE_ID.to_string(),
                timer_family_id: FAMILY.to_string(),
                timers: timer_bytes,
                is_last: true,
            }],
        })
        .await;

    let resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: inst_id.to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: desc_id.to_string(),
                ..Default::default()
            })),
        })
        .await;
    assert!(resp.error.is_empty(), "bundle failed: {}", resp.error);
    assert!(matches!(resp.response, Some(Response::ProcessBundle(_))));

    let mut sink_bytes = Vec::new();
    while let Ok(elements) = data_out.try_recv() {
        for data in elements.data {
            assert_eq!(data.transform_id, SINK_ID);
            sink_bytes.extend_from_slice(&data.data);
        }
    }

    let coder = WindowedValueCoder::with_window_coder(StringUtf8Coder, IntervalWindowCoder);
    let mut cursor = sink_bytes.as_slice();
    let mut emitted = Vec::new();
    while !cursor.is_empty() {
        let wv = coder
            .decode(&mut cursor, Context::Nested)
            .expect("decode output");
        emitted.push((wv.value, wv.windows, wv.timestamp_millis));
    }
    emitted
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_window_timer_fires_once_per_window_with_per_window_state() {
    let w1 = IntervalWindow::new(Instant::from_millis(0), Instant::from_millis(10_000));
    let w2 = IntervalWindow::new(Instant::from_millis(5_000), Instant::from_millis(15_000));

    let emitted = run_multi_window_bundle("desc_mw_all", "inst_mw_all", None, w1, w2).await;

    assert_eq!(
        emitted,
        vec![
            (
                format!("k:{}:10", BoundedWindow::Interval(w1)),
                vec![w1],
                9_000
            ),
            (
                format!("k:{}:20", BoundedWindow::Interval(w2)),
                vec![w2],
                9_000
            ),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cleared_window_in_a_multi_window_timer_is_skipped_while_other_windows_fire() {
    let w1 = IntervalWindow::new(Instant::from_millis(0), Instant::from_millis(10_000));
    let w2 = IntervalWindow::new(Instant::from_millis(5_000), Instant::from_millis(15_000));

    let emitted = run_multi_window_bundle("desc_mw_stale", "inst_mw_stale", Some(10), w1, w2).await;

    assert_eq!(
        emitted,
        vec![(
            format!("k:{}:20", BoundedWindow::Interval(w2)),
            vec![w2],
            9_000
        )]
    );
}
