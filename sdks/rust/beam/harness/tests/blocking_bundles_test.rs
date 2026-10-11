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

//! Bundles and finalization callbacks whose user code blocks do not stop the runtime worker
//! threads.
//!
//! The multi-threaded runtimes here have one worker thread. User code that blocks on that
//! thread would stop every other bundle and task, so these tests time out unless the user
//! code runs elsewhere.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use beam::internals::{ElementSink, TransformFn};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::elements::Data;
use model::fn_execution::{
    Elements, FinalizeBundleRequest, InstructionRequest, InstructionResponse, ProcessBundleRequest,
    RegisterRequest, instruction_request::Request,
};

mod common;
use common::{Gate, SOURCE_ID, STAGE_ID, TestBundle, linear_descriptor, within};

const DESCRIPTOR: &str = "blocking";

/// Registers the linear descriptor whose stage runs `stage`.
async fn client_with_stage(stage: TransformFn) -> TestBundle {
    let bundle = TestBundle::builder().handler(STAGE_ID, stage).build();
    let registered = bundle
        .control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "register".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![linear_descriptor(DESCRIPTOR)],
            })),
        })
        .await;
    assert!(registered.error.is_empty(), "{}", registered.error);
    bundle
}

/// Starts bundle `id` on a task of the runtime, as the control stream does.
fn start_bundle(client: &ControlClient, id: &str) -> tokio::task::JoinHandle<InstructionResponse> {
    let client = client.clone();
    let request = InstructionRequest {
        instruction_id: id.to_string(),
        request: Some(Request::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: DESCRIPTOR.to_string(),
            ..Default::default()
        })),
    };
    tokio::spawn(async move { client.handle_instruction_isolated(request).await })
}

/// Sends one last element to each bundle of `ids`.
async fn feed(data_manager: &DataManager, ids: &[&str]) {
    data_manager
        .handle_inbound_elements(Elements {
            data: ids
                .iter()
                .map(|id| Data {
                    instruction_id: id.to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: vec![1],
                    is_last: true,
                })
                .collect(),
            timers: Vec::new(),
        })
        .await;
}

async fn assert_succeeds(bundle: tokio::task::JoinHandle<InstructionResponse>) {
    let response = within("the bundle response", bundle)
        .await
        .expect("the bundle task does not panic");
    assert!(response.error.is_empty(), "{}", response.error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn two_bundles_that_wait_for_each_other_both_finish_on_one_worker_thread() {
    let arrived = Arc::new(AtomicUsize::new(0));
    let both = Gate::new();
    let stage: TransformFn = {
        let (arrived, both) = (Arc::clone(&arrived), Arc::clone(&both));
        Arc::new(move |element: &[u8], sink: &mut dyn ElementSink| {
            if arrived.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                both.open();
            }
            both.wait("the other bundle to start its element");
            sink.push_bytes(element)
        })
    };
    let TestBundle {
        control_client,
        data_manager,
        data_out_rx: _data_out_rx,
        ..
    } = client_with_stage(stage).await;
    let _release = both.open_on_drop();

    let first = start_bundle(&control_client, "first");
    let second = start_bundle(&control_client, "second");
    feed(&data_manager, &["first", "second"]).await;

    assert_succeeds(first).await;
    assert_succeeds(second).await;
    assert_eq!(arrived.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_blocked_bundle_does_not_stop_the_other_tasks_of_the_runtime() {
    let entered = Gate::new();
    let released = Gate::new();
    let stage: TransformFn = {
        let (entered, released) = (Arc::clone(&entered), Arc::clone(&released));
        Arc::new(move |element: &[u8], sink: &mut dyn ElementSink| {
            entered.open();
            released.wait("a task of the runtime to release the bundle");
            sink.push_bytes(element)
        })
    };
    let TestBundle {
        control_client,
        data_manager,
        data_out_rx: _data_out_rx,
        ..
    } = client_with_stage(stage).await;
    let _release = released.open_on_drop();

    let bundle = start_bundle(&control_client, "blocked");
    feed(&data_manager, &["blocked"]).await;
    // Only the worker thread runs this task. It needs the timer driver too.
    let releaser = tokio::spawn({
        let (entered, released) = (Arc::clone(&entered), Arc::clone(&released));
        async move {
            while !entered.is_open() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            released.open();
        }
    });

    within("the task that releases the bundle", releaser)
        .await
        .expect("the releasing task does not panic");
    assert_succeeds(bundle).await;
}

#[tokio::test]
async fn a_bundle_that_blocks_finishes_on_a_current_thread_runtime() {
    let released = Gate::new();
    let stage: TransformFn = {
        let released = Arc::clone(&released);
        Arc::new(move |element: &[u8], sink: &mut dyn ElementSink| {
            released.wait("the test to release the bundle");
            sink.push_bytes(element)
        })
    };
    let TestBundle {
        control_client,
        data_manager,
        data_out_rx: _data_out_rx,
        ..
    } = client_with_stage(stage).await;
    let _release = released.open_on_drop();

    let bundle = start_bundle(&control_client, "current");
    feed(&data_manager, &["current"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    released.open();

    assert_succeeds(bundle).await;
}

/// A finalization callback that expires in one minute.
fn pending(
    callback: impl FnOnce() -> beam::Result + Send + 'static,
) -> beam::internals::PendingFinalizer {
    beam::internals::PendingFinalizer {
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
        callback: Box::new(callback),
    }
}

fn finalize(id: &str, target: &str) -> InstructionRequest {
    InstructionRequest {
        instruction_id: id.to_string(),
        request: Some(Request::FinalizeBundle(FinalizeBundleRequest {
            instruction_id: target.to_string(),
        })),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_finalization_callback_that_blocks_does_not_stop_other_instructions() {
    let TestBundle {
        processor,
        control_client,
        ..
    } = TestBundle::empty();
    let released = Gate::new();
    let _release = released.open_on_drop();
    processor.store_finalization(
        "bundle",
        vec![pending({
            let released = Arc::clone(&released);
            move || {
                released.wait("another instruction to release the callback");
                Ok(())
            }
        })],
    );

    let blocked = tokio::spawn({
        let client = control_client.clone();
        async move {
            client
                .handle_instruction_isolated(finalize("finalize", "bundle"))
                .await
        }
    });
    // Another instruction on the only worker thread answers while the callback blocks.
    let other = within(
        "an instruction while a callback blocks",
        tokio::spawn({
            let client = control_client.clone();
            async move {
                client
                    .handle_instruction(finalize("other", "unknown"))
                    .await
            }
        }),
    )
    .await
    .expect("the other instruction does not panic");
    assert!(other.error.is_empty(), "{}", other.error);
    released.open();

    let response = within("the finalization response", blocked)
        .await
        .expect("the finalization task does not panic");
    assert!(response.error.is_empty(), "{}", response.error);
}

#[tokio::test]
async fn a_finalization_callback_that_panics_fails_its_instruction() {
    let TestBundle {
        processor,
        control_client,
        ..
    } = TestBundle::empty();
    processor.store_finalization("bundle", vec![pending(|| panic!("commit exploded"))]);

    let response = within(
        "the response of a callback that panics",
        control_client.handle_instruction_isolated(finalize("finalize", "bundle")),
    )
    .await;

    assert!(
        response.error.contains("panicked: commit exploded"),
        "{}",
        response.error
    );
}
