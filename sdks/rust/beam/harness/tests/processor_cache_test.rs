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

//! Bundle processors own their handler instances, reuse them across bundles, and discard
//! them after a failure.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use beam::coders::{Coder, Context, StringUtf8Coder, WindowedValue, WindowedValueCoder};
use beam::internals::{BundleHandler, HandlerContext, TransformFn};
use harness::bundle_processor::BundleProcessor;
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleRequest, RegisterRequest, elements,
    instruction_request::Request,
};

mod common;
use common::{SOURCE_ID, STAGE_ID, windowed_linear_descriptor};

const DESCRIPTOR_ID: &str = "reuse";

/// Records every lifecycle call, tagged with the receiving instance's id. The prototype is
/// instance 0; each copy the worker makes gets the next id.
#[derive(Clone)]
struct Recorder {
    id: usize,
    instances: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
}

impl Recorder {
    fn record(&self, event: &str) {
        self.log
            .lock()
            .expect("log lock")
            .push(format!("{event}:{}", self.id));
    }
}

impl BundleHandler for Recorder {
    fn instantiate(&self) -> beam::internals::HandlerInstance {
        Box::new(Self {
            id: self.instances.fetch_add(1, Ordering::SeqCst) + 1,
            ..self.clone()
        })
    }

    fn setup(&mut self) -> Result<(), String> {
        self.record("setup");
        Ok(())
    }

    fn start_bundle(&mut self) -> Result<(), String> {
        self.record("start");
        Ok(())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        if element.windows(4).any(|w| w == b"fail") {
            return Err("asked to fail".to_string());
        }
        ctx.sink.push(element.to_vec())
    }

    fn teardown(&mut self) -> Result<(), String> {
        self.record("teardown");
        Ok(())
    }
}

struct Worker {
    processor: Arc<BundleProcessor>,
    control: ControlClient,
    data: DataManager,
    log: Arc<Mutex<Vec<String>>>,
    // Keeps the outbound channel open for the bundles' sinks.
    _outbound: mpsc::Receiver<Elements>,
}

impl Worker {
    async fn start() -> Self {
        Self::start_with(windowed_linear_descriptor(DESCRIPTOR_ID), None).await
    }

    /// Starts a worker for `descriptor`, with `idle_timeout` when it is set.
    async fn start_with(
        descriptor: model::fn_execution::ProcessBundleDescriptor,
        idle_timeout: Option<Duration>,
    ) -> Self {
        let log = Arc::new(Mutex::new(Vec::new()));
        let prototype: TransformFn = Arc::new(Recorder {
            id: 0,
            instances: Arc::new(AtomicUsize::new(0)),
            log: Arc::clone(&log),
        });
        let (tx, outbound) = mpsc::channel(64);
        let data = DataManager::new(tx);
        let processor = BundleProcessor::with_handlers(
            data.clone(),
            HashMap::from([(STAGE_ID.to_string(), prototype)]),
        );
        let processor = Arc::new(match idle_timeout {
            Some(timeout) => processor.with_processor_idle_timeout(timeout),
            None => processor,
        });
        let control = ControlClient::new(Arc::clone(&processor));

        let registered = control
            .handle_instruction(InstructionRequest {
                instruction_id: "register".to_string(),
                request: Some(Request::Register(RegisterRequest {
                    process_bundle_descriptor: vec![descriptor],
                })),
            })
            .await;
        assert!(registered.error.is_empty(), "{}", registered.error);

        Self {
            processor,
            control,
            data,
            log,
            _outbound: outbound,
        }
    }

    /// Sends `lines` as the whole input of `instruction`, after `delay`.
    fn feed(&self, instruction: &str, lines: &[&str], delay: Duration) {
        let coder = WindowedValueCoder::new(StringUtf8Coder);
        let mut bytes = Vec::new();
        lines.iter().for_each(|line| {
            coder
                .encode(
                    &WindowedValue::global((*line).to_string(), 0),
                    &mut bytes,
                    Context::Nested,
                )
                .expect("encode element");
        });
        let (data, instruction) = (self.data.clone(), instruction.to_string());
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            data.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: instruction,
                    transform_id: SOURCE_ID.to_string(),
                    data: bytes,
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        });
    }

    /// Runs one bundle to completion, returning the error it reported, if any.
    async fn bundle(&self, instruction: &str, lines: &[&str]) -> Result<(), String> {
        self.feed(instruction, lines, Duration::from_millis(10));
        self.process(instruction).await
    }

    async fn process(&self, instruction: &str) -> Result<(), String> {
        let response = self
            .control
            .handle_instruction(InstructionRequest {
                instruction_id: instruction.to_string(),
                request: Some(Request::ProcessBundle(ProcessBundleRequest {
                    process_bundle_descriptor_id: DESCRIPTOR_ID.to_string(),
                    ..Default::default()
                })),
            })
            .await;
        if response.error.is_empty() {
            Ok(())
        } else {
            Err(response.error)
        }
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().expect("log lock").clone()
    }
}

#[tokio::test]
async fn consecutive_bundles_reuse_one_set_up_instance() {
    let worker = Worker::start().await;

    worker.bundle("b1", &["a"]).await.unwrap();
    worker.bundle("b2", &["b"]).await.unwrap();

    // One copy, set up once, runs both bundles; the prototype itself never runs.
    assert_eq!(worker.log(), ["setup:1", "start:1", "start:1"]);
}

#[tokio::test]
async fn a_failed_bundle_discards_its_instance() {
    let worker = Worker::start().await;

    worker.bundle("b1", &["a"]).await.unwrap();
    let failed = worker.bundle("b2", &["fail"]).await;
    assert!(failed.is_err(), "the bundle should report the failure");
    worker.bundle("b3", &["c"]).await.unwrap();

    // A failed instance may hold half-finished state, so the next bundle gets a fresh one.
    assert_eq!(
        worker.log(),
        [
            "setup:1",
            "start:1",
            "start:1",
            "teardown:1",
            "setup:2",
            "start:2"
        ]
    );
}

#[tokio::test]
async fn concurrent_bundles_run_on_separate_instances() {
    let worker = Arc::new(Worker::start().await);

    // b1 waits for its input while b2 starts, so both are in flight at once.
    worker.feed("b1", &["a"], Duration::from_millis(200));
    worker.feed("b2", &["b"], Duration::from_millis(50));
    let (first, second) = tokio::join!(worker.process("b1"), async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        worker.process("b2").await
    });
    first.unwrap();
    second.unwrap();

    let log = worker.log();
    assert!(
        log.contains(&"start:1".to_string()) && log.contains(&"start:2".to_string()),
        "each bundle should run on its own instance: {log:?}"
    );
    assert_eq!(
        log.iter().filter(|e| e.starts_with("setup")).count(),
        2,
        "{log:?}"
    );

    // Both are idle; a third bundle reuses one of them instead of making another.
    worker.bundle("b3", &["c"]).await.unwrap();
    assert!(!worker.log().contains(&"setup:3".to_string()));
}

#[tokio::test]
async fn shutdown_tears_down_idle_instances() {
    let worker = Worker::start().await;
    worker.bundle("b1", &["a"]).await.unwrap();

    worker.processor.shutdown();

    assert_eq!(worker.log(), ["setup:1", "start:1", "teardown:1"]);
}

#[tokio::test]
async fn a_sweep_tears_down_instances_idle_past_the_timeout() {
    let descriptor = windowed_linear_descriptor(DESCRIPTOR_ID);
    let worker = Worker::start_with(descriptor, Some(Duration::from_millis(20))).await;
    worker.bundle("b1", &["a"]).await.unwrap();
    assert_eq!(worker.processor.sweep_idle_processors(), 0, "still fresh");

    tokio::time::sleep(Duration::from_millis(40)).await;

    assert_eq!(worker.processor.sweep_idle_processors(), 1);
    assert_eq!(worker.log(), ["setup:1", "start:1", "teardown:1"]);
    assert!(worker.processor.idle_processors().is_empty());
}

/// The worker reads one data source for each descriptor, so a second one is an error, not
/// lost data.
#[tokio::test]
async fn a_descriptor_with_two_data_sources_fails_its_bundles() {
    let mut descriptor = windowed_linear_descriptor(DESCRIPTOR_ID);
    let source = descriptor.transforms[SOURCE_ID].clone();
    descriptor
        .transforms
        .insert("second_source".to_string(), source);
    let worker = Worker::start_with(descriptor, None).await;

    let error = worker.process("b1").await.unwrap_err();

    assert!(
        error.contains("more than one DATA_SOURCE") && error.contains("second_source"),
        "{error}"
    );
    assert!(
        worker.log().is_empty(),
        "no handler is set up: {:?}",
        worker.log()
    );
}
