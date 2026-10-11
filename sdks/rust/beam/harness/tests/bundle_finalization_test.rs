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

//! Bundle finalization runs through the harness.
//! The finalizer reaches stages of an expanded splittable ParDo when their payload
//! requests it. `FinalizeBundle` runs the callbacks, and expired callbacks do not run.

#![expect(clippy::unwrap_used, reason = "integration test helpers")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use beam::coders::{Coder, Context, StringUtf8Coder, WindowedValue, WindowedValueCoder};
use beam::internals::{
    BundleHandler, HandlerContext, HandlerInstance, PendingFinalizer, TransformFn,
};
use beam::pipeline::{URN_PAR_DO, URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS};
use beam::transforms::{ProcessContext, WithFinalizer};
use harness::bundle_processor::BundleProcessor;
use harness::bundle_processor::spawn_finalizer_sweeper;
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, FinalizeBundleRequest, InstructionRequest, InstructionResponse, ProcessBundleRequest,
    RegisterRequest, instruction_request::Request, instruction_response::Response,
};
use model::pipeline::{FunctionSpec, ParDoPayload};
use prost::Message;
use tokio::time::sleep;

mod common;
use common::{DescriptorBuilder, SINK_ID, SOURCE_ID, STAGE_ID, within};

/// Registers one callback per element, as the process stage of a splittable ParDo does. It
/// is its own handler for every stage URN.
#[derive(Clone)]
struct Registering {
    callbacks: Arc<AtomicUsize>,
}

impl BundleHandler for Registering {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, _element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let callbacks = Arc::clone(&self.callbacks);
        let ctx: ProcessContext<'_, WithFinalizer<String>> = ctx.as_process_context();
        ctx.finalizer(move || {
            callbacks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .register()
        .map_err(|e| e.to_string())
    }

    fn stage_handler(&self, _stage_urn: &str) -> Option<Arc<dyn BundleHandler>> {
        Some(Arc::new(self.clone()))
    }
}

/// A one-stage descriptor whose stage has `urn` and a payload that sets
/// `requests_finalization` to `requests`.
fn descriptor(id: &str, urn: &str, requests: bool) -> model::fn_execution::ProcessBundleDescriptor {
    let mut descriptor = DescriptorBuilder::new(id)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    let payload = ParDoPayload {
        do_fn: Some(FunctionSpec {
            urn: "beam:dofn:rust:v1".to_string(),
            payload: STAGE_ID.as_bytes().to_vec(),
        }),
        requests_finalization: requests,
        ..Default::default()
    };
    descriptor.transforms.get_mut(STAGE_ID).unwrap().spec = Some(FunctionSpec {
        urn: urn.to_string(),
        payload: payload.encode_to_vec(),
    });
    descriptor
}

/// Runs one bundle of one element through a stage with `urn`.
/// Returns the bundle response, the control client, and the callback counter.
async fn run_bundle(
    urn: &str,
    requests: bool,
) -> (InstructionResponse, ControlClient, Arc<AtomicUsize>) {
    let (data_out_tx, _data_out_rx) = tokio::sync::mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let callbacks = Arc::new(AtomicUsize::new(0));
    let handler: TransformFn = Arc::new(Registering {
        callbacks: Arc::clone(&callbacks),
    });
    let processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        HashMap::from([(STAGE_ID.to_string(), handler)]),
    ));
    let control = ControlClient::new(processor);
    within(
        "registration",
        control.handle_instruction(InstructionRequest {
            instruction_id: "register".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor("descriptor", urn, requests)],
            })),
        }),
    )
    .await;

    let mut element = Vec::new();
    WindowedValueCoder::new(StringUtf8Coder)
        .encode(
            &WindowedValue::global("a".to_string(), 0),
            &mut element,
            Context::Nested,
        )
        .unwrap();
    within(
        "the inbound data",
        data_manager.handle_inbound_elements(Elements {
            data: vec![model::fn_execution::elements::Data {
                instruction_id: "bundle".to_string(),
                transform_id: SOURCE_ID.to_string(),
                data: element,
                is_last: true,
            }],
            timers: Vec::new(),
        }),
    )
    .await;
    let response = within(
        "the bundle",
        control.handle_instruction(InstructionRequest {
            instruction_id: "bundle".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "descriptor".to_string(),
                ..Default::default()
            })),
        }),
    )
    .await;
    (response, control, callbacks)
}

async fn finalize(control: &ControlClient) -> InstructionResponse {
    within(
        "the finalization",
        control.handle_instruction(InstructionRequest {
            instruction_id: "finalize".to_string(),
            request: Some(Request::FinalizeBundle(FinalizeBundleRequest {
                instruction_id: "bundle".to_string(),
            })),
        }),
    )
    .await
}

fn requires_finalization(response: &InstructionResponse) -> bool {
    match &response.response {
        Some(Response::ProcessBundle(bundle)) => bundle.requires_finalization,
        other => panic!(
            "not a bundle response: {other:?}, error: {}",
            response.error
        ),
    }
}

#[tokio::test]
async fn the_process_stage_of_a_splittable_pardo_gets_the_finalizer_when_its_payload_asks() {
    let (response, control, callbacks) =
        run_bundle(URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS, true).await;
    assert!(requires_finalization(&response));
    assert_eq!(
        callbacks.load(Ordering::SeqCst),
        0,
        "nothing runs before FinalizeBundle"
    );

    let finalized = finalize(&control).await;
    assert!(finalized.error.is_empty(), "{}", finalized.error);
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_pardo_gets_the_finalizer_when_its_payload_asks() {
    let (response, control, callbacks) = run_bundle(URN_PAR_DO, true).await;
    assert!(requires_finalization(&response));
    finalize(&control).await;
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_stage_whose_payload_does_not_ask_gets_no_finalizer() {
    let (response, _, _) = run_bundle(URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS, false).await;
    assert!(
        response.error.contains("no finalizer"),
        "{}",
        response.error
    );
}

fn counting(deadline: Instant, ran: &Arc<AtomicUsize>) -> PendingFinalizer {
    let ran = Arc::clone(ran);
    PendingFinalizer {
        deadline,
        callback: Box::new(move || {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    }
}

fn processor() -> BundleProcessor {
    let (data_out_tx, _data_out_rx) = tokio::sync::mpsc::channel::<Elements>(64);
    BundleProcessor::new(DataManager::new(data_out_tx))
}

#[test]
fn finalize_bundle_runs_only_the_callbacks_that_have_not_expired() {
    let processor = processor();
    let ran = Arc::new(AtomicUsize::new(0));
    let now = Instant::now();
    processor.store_finalization(
        "bundle",
        vec![
            counting(now + Duration::from_secs(60), &ran),
            counting(now - Duration::from_secs(1), &ran),
        ],
    );
    processor.finalize_bundle("bundle").unwrap();
    assert_eq!(ran.load(Ordering::SeqCst), 1);
    assert_eq!(
        processor.expired_finalization_count(),
        1,
        "a callback dropped at finalization counts as expired"
    );
}

#[test]
fn a_sweep_drops_the_expired_callbacks_of_every_bundle() {
    let processor = processor();
    let ran = Arc::new(AtomicUsize::new(0));
    let now = Instant::now();
    processor.store_finalization(
        "expired",
        vec![
            counting(now - Duration::from_secs(1), &ran),
            counting(now - Duration::from_secs(2), &ran),
        ],
    );
    processor.store_finalization("live", vec![counting(now + Duration::from_secs(60), &ran)]);
    assert_eq!(
        processor.expired_finalization_count(),
        0,
        "storing a bundle drops no callback"
    );

    assert_eq!(processor.sweep_expired_finalizations(), 2);
    assert_eq!(
        processor.sweep_expired_finalizations(),
        0,
        "a sweep drops each callback once"
    );
    assert_eq!(processor.expired_finalization_count(), 2);

    processor.finalize_bundle("expired").unwrap();
    assert_eq!(ran.load(Ordering::SeqCst), 0, "dropped callbacks never run");
    processor.finalize_bundle("live").unwrap();
    assert_eq!(ran.load(Ordering::SeqCst), 1, "a live callback stays");
}

/// Stores one callback that has expired under `bundle`.
fn store_expired(processor: &BundleProcessor, bundle: &str, ran: &Arc<AtomicUsize>) {
    processor.store_finalization(
        bundle,
        vec![counting(Instant::now() - Duration::from_secs(1), ran)],
    );
}

#[tokio::test(start_paused = true)]
async fn the_sweeper_drops_expired_callbacks_at_each_interval_and_stops_with_the_processor() {
    let processor = Arc::new(processor());
    let ran = Arc::new(AtomicUsize::new(0));
    let every = Duration::from_secs(60);
    let sweeper = spawn_finalizer_sweeper(Arc::downgrade(&processor), every);
    store_expired(&processor, "first", &ran);

    sleep(every - Duration::from_secs(1)).await;
    assert_eq!(
        processor.expired_finalization_count(),
        0,
        "the first sweep is one interval after the start"
    );
    sleep(Duration::from_secs(2)).await;
    assert_eq!(processor.expired_finalization_count(), 1);

    store_expired(&processor, "second", &ran);
    sleep(every).await;
    assert_eq!(
        processor.expired_finalization_count(),
        2,
        "each interval has a sweep"
    );
    assert!(!sweeper.is_finished());

    drop(processor);
    sleep(every).await;
    assert!(sweeper.is_finished(), "the sweeper ends with the processor");
    assert_eq!(ran.load(Ordering::SeqCst), 0);
}
