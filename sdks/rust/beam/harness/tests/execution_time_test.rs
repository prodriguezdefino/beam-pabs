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

//! Tests for transform execution time reporting across start, process, and finish phases.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use beam::coders::{
    Coder, Context, StringUtf8Coder, VarIntCoder, WindowedValue, WindowedValueCoder,
};
use beam::internals::{BundleHandler, HandlerContext, HandlerInstance};
use beam::metrics::{
    LABEL_PTRANSFORM, URN_FINISH_BUNDLE_MSECS, URN_PROCESS_BUNDLE_MSECS, URN_START_BUNDLE_MSECS,
    URN_TOTAL_MSECS,
};
use harness::bundle_processor::{BundleProcessor, TransformFn};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleDescriptor, ProcessBundleProgressRequest,
    ProcessBundleRequest, RegisterRequest, instruction_request::Request,
    instruction_response::Response,
};

mod common;
use common::{DescriptorBuilder, Gate, SINK_ID, SOURCE_ID, STAGE_ID};

/// A handler that sleeps in start_bundle, process, and finish_bundle.
#[derive(Clone)]
struct SleepingHandler {
    phase_sleep: Duration,
}

impl BundleHandler for SleepingHandler {
    fn start_bundle(&mut self) -> Result<(), String> {
        std::thread::sleep(self.phase_sleep);
        Ok(())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        std::thread::sleep(self.phase_sleep);
        ctx.sink.push(element.to_vec())
    }

    fn finish_bundle(&mut self, _ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        std::thread::sleep(self.phase_sleep);
        Ok(())
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

fn create_descriptor(descriptor_id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(descriptor_id)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build()
}

/// Checks that a bundle reports start, process, finish, and total execution time for a transform.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_execution_time_for_start_process_and_finish_phases() {
    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);

    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(
        STAGE_ID.to_string(),
        Arc::new(SleepingHandler {
            phase_sleep: Duration::from_millis(50),
        }),
    );

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor);

    let descriptor_id = "desc_execution_time_test";
    let pbd = create_descriptor(descriptor_id);

    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_reg".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![pbd],
            })),
        })
        .await;

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    let wv = WindowedValue::global("elem".to_string(), 1700000000);
    coder.encode(&wv, &mut payload, Context::Nested).unwrap();

    let instruction_id = "inst_bundle_exec_time";

    data_manager
        .handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: instruction_id.to_string(),
                transform_id: SOURCE_ID.to_string(),
                data: payload,
                is_last: true,
            }],
            timers: Vec::new(),
        })
        .await;

    let resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: instruction_id.to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: descriptor_id.to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                ..Default::default()
            })),
        })
        .await;

    let Some(Response::ProcessBundle(pb_resp)) = resp.response else {
        panic!("Expected ProcessBundleResponse");
    };

    let find_metric = |urn: &str| -> Option<i64> {
        pb_resp
            .monitoring_infos
            .iter()
            .find(|info| {
                info.urn == urn
                    && info.labels.get(LABEL_PTRANSFORM).map(String::as_str) == Some(STAGE_ID)
            })
            .and_then(|info| VarIntCoder::decode_varint(&mut Cursor::new(&info.payload)).ok())
    };

    let start_msecs = find_metric(URN_START_BUNDLE_MSECS)
        .expect("start_bundle_msecs must be present for the transform");
    let process_msecs = find_metric(URN_PROCESS_BUNDLE_MSECS)
        .expect("process_bundle_msecs must be present for the transform");
    let finish_msecs = find_metric(URN_FINISH_BUNDLE_MSECS)
        .expect("finish_bundle_msecs must be present for the transform");
    let total_msecs =
        find_metric(URN_TOTAL_MSECS).expect("total_msecs must be present for the transform");

    assert!(
        start_msecs > 0,
        "start_bundle_msecs must be non-zero, got {start_msecs}"
    );
    assert!(
        process_msecs > 0,
        "process_bundle_msecs must be non-zero, got {process_msecs}"
    );
    assert!(
        finish_msecs > 0,
        "finish_bundle_msecs must be non-zero, got {finish_msecs}"
    );
    assert!(
        total_msecs > 0,
        "total_msecs must be non-zero, got {total_msecs}"
    );

    let sum = start_msecs + process_msecs + finish_msecs;
    assert!(
        total_msecs >= sum,
        "total_msecs ({total_msecs}) must be >= sum of phase msecs ({sum})"
    );
}

/// A handler that blocks on a gate during processing to test progress metrics.
#[derive(Clone)]
struct GatedHandler {
    phase_sleep: Duration,
    gate: Arc<Gate>,
}

impl BundleHandler for GatedHandler {
    fn start_bundle(&mut self) -> Result<(), String> {
        std::thread::sleep(self.phase_sleep);
        Ok(())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        std::thread::sleep(self.phase_sleep);
        self.gate.wait("the test to release the gated handler");
        ctx.sink.push(element.to_vec())
    }

    fn finish_bundle(&mut self, _ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        std::thread::sleep(self.phase_sleep);
        Ok(())
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

/// Checks that progress reports contain execution times before the bundle completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_execution_time_in_progress_response() {
    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);

    let gate = Gate::new();
    let _release_on_drop = gate.open_on_drop();

    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(
        STAGE_ID.to_string(),
        Arc::new(GatedHandler {
            phase_sleep: Duration::from_millis(40),
            gate: Arc::clone(&gate),
        }),
    );

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor);

    let descriptor_id = "desc_progress_time_test";
    let pbd = create_descriptor(descriptor_id);

    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_reg".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![pbd],
            })),
        })
        .await;

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut payload = Vec::new();
    let wv = WindowedValue::global("elem".to_string(), 1700000000);
    coder.encode(&wv, &mut payload, Context::Nested).unwrap();

    let instruction_id = "inst_bundle_progress_time";

    data_manager
        .handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: instruction_id.to_string(),
                transform_id: SOURCE_ID.to_string(),
                data: payload,
                is_last: true,
            }],
            timers: Vec::new(),
        })
        .await;

    let cc = control_client.clone();
    let bundle_handle = tokio::spawn(async move {
        cc.handle_instruction(InstructionRequest {
            instruction_id: instruction_id.to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: descriptor_id.to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                ..Default::default()
            })),
        })
        .await
    });

    // Poll until progress sync copies execution times (sync interval is 100ms).
    let mut start_metric = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let progress_resp = control_client
            .handle_instruction(InstructionRequest {
                instruction_id: "inst_prog".to_string(),
                request: Some(Request::ProcessBundleProgress(
                    ProcessBundleProgressRequest {
                        instruction_id: instruction_id.to_string(),
                    },
                )),
            })
            .await;

        if let Some(Response::ProcessBundleProgress(prog)) = progress_resp.response {
            start_metric = prog
                .monitoring_infos
                .iter()
                .find(|info| {
                    info.urn == URN_START_BUNDLE_MSECS
                        && info.labels.get(LABEL_PTRANSFORM).map(String::as_str) == Some(STAGE_ID)
                })
                .and_then(|info| VarIntCoder::decode_varint(&mut Cursor::new(&info.payload)).ok());
            if start_metric.is_some_and(|m| m > 0) {
                break;
            }
        }
    }

    gate.open();
    let _ = bundle_handle.await;

    assert!(
        start_metric.is_some_and(|m| m > 0),
        "Progress response must report start_bundle_msecs, got {start_metric:?}"
    );
}
