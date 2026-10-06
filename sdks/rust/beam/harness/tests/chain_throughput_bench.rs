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

//! Throughput of the fused operator chain, for measuring per-element harness cost.
//!
//! Ignored by default. Run with:
//!
//! ```text
//! cargo test --release -p apache-beam-harness --test chain_throughput_bench -- --ignored --nocapture
//! ```
//!
//! The chain models WordCount's hot stage: split lines, pair each word with one, count at
//! bundle end. The user code is trivial, so harness work between operators dominates.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beam::coders::{Coder, Context, StringUtf8Coder, WindowedValue, WindowedValueCoder};
use beam::internals::DoFnHandler;
use beam::transforms::{DoFn, ProcessContext};
use harness::bundle_processor::{BundleProcessor, TransformFn};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleRequest, RegisterRequest, elements,
    instruction_request::Request,
};
use tokio::sync::mpsc;

mod common;
use common::{DescriptorBuilder, SINK_ID, SOURCE_ID};

const LINES: usize = 200_000;
const WORDS_PER_LINE: usize = 10;
const RUNS: usize = 5;

#[derive(Clone)]
struct SplitWords;

impl DoFn for SplitWords {
    type In = String;
    type Out = String;

    fn process_element(&mut self, line: String, out: &mut ProcessContext<String>) -> beam::Result {
        line.split(' ')
            .try_for_each(|word| out.emit(word.to_string()))
    }
}

#[derive(Clone)]
struct PairWithOne;

impl DoFn for PairWithOne {
    type In = String;
    type Out = (String, i64);

    fn process_element(
        &mut self,
        word: String,
        out: &mut ProcessContext<(String, i64)>,
    ) -> beam::Result {
        out.emit((word, 1))
    }
}

/// Sums the counts it sees and emits the total when the bundle finishes.
#[derive(Clone, Default)]
struct CountAtFinish {
    total: i64,
}

impl DoFn for CountAtFinish {
    type In = (String, i64);
    type Out = String;

    fn start_bundle(&mut self) -> beam::Result {
        self.total = 0;
        Ok(())
    }

    fn process_element(
        &mut self,
        (_, count): (String, i64),
        _out: &mut ProcessContext<String>,
    ) -> beam::Result {
        self.total += count;
        Ok(())
    }

    fn finish_bundle(&mut self, out: &mut ProcessContext<String>) -> beam::Result {
        out.emit(self.total.to_string())
    }
}

fn handler<F: DoFn>(f: F) -> TransformFn {
    Arc::new(DoFnHandler::new(f))
}

fn encoded_input() -> Vec<u8> {
    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let line = (0..WORDS_PER_LINE)
        .map(|i| format!("word{i}"))
        .collect::<Vec<_>>()
        .join(" ");
    let mut bytes = Vec::new();
    (0..LINES).for_each(|_| {
        coder
            .encode(
                &WindowedValue::global(line.clone(), 0),
                &mut bytes,
                Context::Nested,
            )
            .expect("encode input line");
    });
    bytes
}

async fn run_bundle(
    control: &ControlClient,
    dm: &DataManager,
    descriptor_id: &str,
    bundle: usize,
    input: Vec<u8>,
) -> Duration {
    let instruction_id = format!("bundle-{bundle}");
    let feed = {
        let dm = dm.clone();
        let instruction_id = instruction_id.clone();
        async move {
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id,
                    transform_id: SOURCE_ID.to_string(),
                    data: input,
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    };
    let started = Instant::now();
    let (resp, ()) = tokio::join!(
        control.handle_instruction(InstructionRequest {
            instruction_id,
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: descriptor_id.to_string(),
                ..Default::default()
            })),
        }),
        feed
    );
    let elapsed = started.elapsed();
    assert!(resp.error.is_empty(), "bundle failed: {}", resp.error);
    elapsed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "benchmark; run explicitly with --release --ignored --nocapture"]
async fn fused_chain_throughput() {
    let (data_out_tx, mut data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let handlers = HashMap::from([
        ("split".to_string(), handler(SplitWords)),
        ("pair".to_string(), handler(PairWithOne)),
        ("count".to_string(), handler(CountAtFinish::default())),
    ]);
    let control = ControlClient::new(Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    )));
    let descriptor = DescriptorBuilder::new("wordcount_chain")
        .stage("split", "pcoll_input", "words")
        .stage("pair", "words", "pairs")
        .stage("count", "pairs", "total")
        .sink(SINK_ID, "total")
        .build();
    let descriptor_id = descriptor.id.clone();
    let reg = control
        .handle_instruction(InstructionRequest {
            instruction_id: "register".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;
    assert!(reg.error.is_empty(), "register failed: {}", reg.error);

    let input = encoded_input();
    let words = (LINES * WORDS_PER_LINE) as f64;
    let timings: Vec<Duration> = {
        let mut timings = Vec::with_capacity(RUNS);
        for bundle in 0..RUNS {
            timings.push(
                run_bundle(
                    &control,
                    &data_manager,
                    &descriptor_id,
                    bundle,
                    input.clone(),
                )
                .await,
            );
            let mut output = Vec::new();
            while let Ok(elements) = data_out_rx.try_recv() {
                elements
                    .data
                    .into_iter()
                    .for_each(|d| output.extend(d.data));
            }
            let expected = (LINES * WORDS_PER_LINE).to_string();
            assert!(
                output
                    .windows(expected.len())
                    .any(|w| w == expected.as_bytes()),
                "bundle {bundle} did not count {expected} words"
            );
        }
        timings
    };
    let best = timings.iter().min().copied().unwrap_or_default();
    let per_word = |d: &Duration| d.as_nanos() as f64 / words;
    println!(
        "fused chain: {LINES} lines, {words} words; best {:.1} ns/word ({best:?}); all runs: {:?}",
        per_word(&best),
        timings
            .iter()
            .map(|d| format!("{:.1}", per_word(d)))
            .collect::<Vec<_>>()
    );
}
