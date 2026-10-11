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

//! The warning that a bundle still waits for its first inbound element.
//!
//! Bundles run on pool threads outside the runtime. Each test lets the bundle thread react
//! in real time with [`settle`], and paused time only moves between these waits.

use std::fmt::{self, Write};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context as LayerContext, Layer, SubscriberExt};

use beam::coders::{Coder, Context, StringUtf8Coder, WindowedValue, WindowedValueCoder};
use beam_harness::data::DataManager;
use beam_model::fn_execution::{
    Elements, InstructionRequest, InstructionResponse, ProcessBundleRequest, RegisterRequest,
    elements, instruction_request::Request,
};

use crate::common::{
    SOURCE_ID, STAGE_ID, TestBundle, identity_handlers, windowed_linear_descriptor, within,
};

const WAIT_WARNING: &str = "Still waiting for inbound data on instruction";
/// Must equal `INBOUND_WAIT_WARNING` in `src/bundle_processor/processor.rs`.
const WAIT_INTERVAL: Duration = Duration::from_secs(15);
/// Real time for the bundle thread to react to a chunk or a timer.
const SETTLE: Duration = Duration::from_millis(50);

/// The message of each event on any thread. The tests share it, so each test counts only
/// the warnings for its own instruction.
static MESSAGES: LazyLock<Messages> = LazyLock::new(|| {
    let messages = Messages::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(messages.clone()))
        .expect("no other global subscriber in this test binary");
    messages
});

#[derive(Clone, Default)]
struct Messages(std::sync::Arc<Mutex<Vec<String>>>);

impl Messages {
    /// The number of wait warnings for `instruction` so far.
    fn wait_warnings(&self, instruction: &str) -> usize {
        let prefix = format!("{WAIT_WARNING} '{instruction}'");
        let messages = self.0.lock().unwrap();
        messages.iter().filter(|m| m.starts_with(&prefix)).count()
    }
}

struct MessageField<'a>(&'a mut String);

impl Visit for MessageField<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            write!(self.0, "{value:?}").unwrap();
        }
    }
}

impl<S: Subscriber> Layer<S> for Messages {
    fn on_event(&self, event: &Event<'_>, _: LayerContext<'_, S>) {
        let mut message = String::new();
        event.record(&mut MessageField(&mut message));
        self.0.lock().unwrap().push(message);
    }
}

/// Waits in real time. A blocking task stops paused time from moving meanwhile.
async fn settle() {
    tokio::task::spawn_blocking(|| std::thread::sleep(SETTLE))
        .await
        .unwrap();
}

/// A running bundle over windowed strings that waits for data.
struct Bundle {
    instruction: &'static str,
    data_manager: DataManager,
    response: JoinHandle<InstructionResponse>,
    _data_out: tokio::sync::mpsc::Receiver<Elements>,
}

impl Bundle {
    async fn start(instruction: &'static str) -> Self {
        let TestBundle {
            data_manager,
            control_client: control,
            data_out_rx,
            ..
        } = TestBundle::new(identity_handlers(&[STAGE_ID]));
        let descriptor = windowed_linear_descriptor("desc_inbound_wait");
        let descriptor_id = descriptor.id.clone();
        let registered = within(
            "registration",
            control.handle_instruction(InstructionRequest {
                instruction_id: format!("reg_{instruction}"),
                request: Some(Request::Register(RegisterRequest {
                    process_bundle_descriptor: vec![descriptor],
                })),
            }),
        )
        .await;
        assert!(registered.error.is_empty(), "{}", registered.error);
        let response = tokio::spawn(async move {
            control
                .handle_instruction(InstructionRequest {
                    instruction_id: instruction.to_string(),
                    request: Some(Request::ProcessBundle(ProcessBundleRequest {
                        process_bundle_descriptor_id: descriptor_id,
                        ..Default::default()
                    })),
                })
                .await
        });
        settle().await;
        Self {
            instruction,
            data_manager,
            response,
            _data_out: data_out_rx,
        }
    }

    async fn send(&self, data: Vec<u8>, is_last: bool) {
        self.data_manager
            .handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: self.instruction.to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data,
                    is_last,
                }],
                timers: Vec::new(),
            })
            .await;
        settle().await;
    }

    /// Waits `duration` of paused time, then lets the bundle thread react.
    async fn wait(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
        settle().await;
    }

    fn wait_warnings(&self) -> usize {
        MESSAGES.wait_warnings(self.instruction)
    }

    async fn finish(self) {
        self.send(Vec::new(), true).await;
        let response = within("the bundle response", self.response).await.unwrap();
        assert!(response.error.is_empty(), "{}", response.error);
    }
}

/// One encoded windowed string element.
fn element() -> Vec<u8> {
    let mut bytes = Vec::new();
    WindowedValueCoder::new(StringUtf8Coder)
        .encode(
            &WindowedValue::global("a".to_string(), 1_700_000_000),
            &mut bytes,
            Context::Nested,
        )
        .unwrap();
    bytes
}

#[tokio::test(start_paused = true)]
async fn a_bundle_without_data_warns_at_each_wait_interval() {
    LazyLock::force(&MESSAGES);
    let bundle = Bundle::start("inst_without_data").await;

    bundle.wait(WAIT_INTERVAL - Duration::from_secs(1)).await;
    assert_eq!(bundle.wait_warnings(), 0, "no warning before the interval");
    bundle.wait(Duration::from_secs(2)).await;
    assert_eq!(bundle.wait_warnings(), 1, "one warning after the interval");
    bundle.wait(WAIT_INTERVAL).await;
    assert_eq!(bundle.wait_warnings(), 2, "one more warning each interval");

    bundle.send(element(), false).await;
    bundle.finish().await;
}

/// Each chunk is one byte of one element, so the bundle reads no element until the last byte.
#[tokio::test(start_paused = true)]
async fn a_bundle_does_not_warn_while_data_arrives() {
    LazyLock::force(&MESSAGES);
    let bundle = Bundle::start("inst_data_arrives").await;

    let element = element();
    let gap = WAIT_INTERVAL - Duration::from_secs(5);
    assert!(gap * element.len() as u32 > 2 * WAIT_INTERVAL);
    let (last, bytes) = element.split_last().unwrap();
    for &byte in bytes {
        bundle.wait(gap).await;
        bundle.send(vec![byte], false).await;
    }
    assert_eq!(bundle.wait_warnings(), 0, "each chunk restarts the wait");

    bundle.wait(WAIT_INTERVAL + Duration::from_secs(1)).await;
    assert_eq!(
        bundle.wait_warnings(),
        1,
        "the wait warns once the chunks stop"
    );

    bundle.send(vec![*last], false).await;
    bundle.finish().await;
}
