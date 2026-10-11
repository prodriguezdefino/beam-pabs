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

//! Elements passed by value between fused operators.
//!
//! In a fused chain, an operator's output reaches its only consumer as the value itself, and
//! other consumers as bytes. These tests check that the value is moved, not decoded again,
//! and that every case that needs the encoding still gets it.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

use beam::coders::{
    Coder, Context, GlobalWindow, StringUtf8Coder, URN_KV, URN_VARINT, WindowedValue,
    WindowedValueCoder,
};
use beam::internals::DoFnHandler;
use beam::transforms::{DoFn, ProcessContext};
use harness::bundle_processor::{BundleProcessor, TransformFn};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleDescriptor, ProcessBundleRequest, RegisterRequest,
    elements, instruction_request::Request,
};

mod common;
use common::{
    DescriptorBuilder, Observer, SINK_ID, SOURCE_ID, element_counts, run_bundle as run_raw_bundle,
    sampled_byte_sizes,
};

/// Capacity [`SplitWords`] gives every word it emits. A decoded copy is allocated to
/// fit, so a consumer seeing this capacity received the emitted value itself.
const MARKER_CAPACITY: usize = 64;

#[derive(Clone)]
struct SplitWords;

impl DoFn for SplitWords {
    type In = String;
    type Out = String;

    fn process_element(&mut self, line: String, out: &mut ProcessContext<String>) -> beam::Result {
        line.split_whitespace().try_for_each(|word| {
            let mut marked = String::with_capacity(MARKER_CAPACITY);
            marked.push_str(word);
            out.emit(marked)
        })
    }
}

/// Pairs each word with 1, counting how many words arrived as the upstream value.
#[derive(Clone)]
struct PairWithOne {
    moved: Arc<AtomicUsize>,
}

impl DoFn for PairWithOne {
    type In = String;
    type Out = (String, i64);

    fn process_element(
        &mut self,
        word: String,
        out: &mut ProcessContext<(String, i64)>,
    ) -> beam::Result {
        if word.capacity() == MARKER_CAPACITY {
            self.moved.fetch_add(1, Ordering::SeqCst);
        }
        out.emit((word, 1))
    }
}

#[derive(Clone)]
struct Format;

impl DoFn for Format {
    type In = (String, i64);
    type Out = String;

    fn process_element(
        &mut self,
        (word, count): (String, i64),
        out: &mut ProcessContext<String>,
    ) -> beam::Result {
        out.emit(format!("{word}={count}"))
    }
}

fn handler<F: DoFn>(f: F) -> TransformFn {
    Arc::new(DoFnHandler::new(f))
}

fn encode_lines(lines: &[&str]) -> Vec<u8> {
    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut bytes = Vec::new();
    lines.iter().for_each(|line| {
        coder
            .encode(
                &WindowedValue::global((*line).to_string(), 0),
                &mut bytes,
                Context::Nested,
            )
            .expect("encode input line");
    });
    bytes
}

/// Runs one bundle over `lines` and returns what each sink received, decoded.
async fn run_bundle(
    handlers: HashMap<String, TransformFn>,
    descriptor: ProcessBundleDescriptor,
    lines: &[&str],
) -> HashMap<String, Vec<String>> {
    let (data_out_tx, mut data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let control = ControlClient::new(Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    )));

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

    let input = encode_lines(lines);
    tokio::spawn({
        let dm = data_manager.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: "bundle".to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: input,
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: "bundle".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: descriptor_id,
                ..Default::default()
            })),
        })
        .await;
    assert!(resp.error.is_empty(), "bundle failed: {}", resp.error);

    let mut raw: HashMap<String, Vec<u8>> = HashMap::new();
    while let Ok(elements) = data_out_rx.try_recv() {
        elements.data.into_iter().for_each(|d| {
            raw.entry(d.transform_id).or_default().extend(d.data);
        });
    }

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    raw.into_iter()
        .map(|(sink, bytes)| {
            let mut cursor = Cursor::new(bytes.as_slice());
            let mut values = Vec::new();
            while (cursor.position() as usize) < bytes.len() {
                let wv: WindowedValue<String, GlobalWindow> = coder
                    .decode(&mut cursor, Context::Nested)
                    .expect("decode sink output");
                values.push(wv.value);
            }
            (sink, values)
        })
        .collect()
}

#[tokio::test]
async fn single_consumer_receives_the_value_itself() {
    let moved = Arc::new(AtomicUsize::new(0));
    let handlers = HashMap::from([
        ("split".to_string(), handler(SplitWords)),
        (
            "pair".to_string(),
            handler(PairWithOne {
                moved: Arc::clone(&moved),
            }),
        ),
        ("format".to_string(), handler(Format)),
    ]);
    let descriptor = DescriptorBuilder::new("typed_chain")
        .stage("split", "pcoll_input", "words")
        .stage("pair", "words", "pairs")
        .stage("format", "pairs", "formatted")
        .sink(SINK_ID, "formatted")
        .build();

    let out = run_bundle(handlers, descriptor, &["a bb", "ccc"]).await;

    assert_eq!(
        out.get(SINK_ID).cloned().unwrap_or_default(),
        vec!["a=1", "bb=1", "ccc=1"]
    );
    assert_eq!(
        moved.load(Ordering::SeqCst),
        3,
        "every word should reach its only consumer as the emitted value, not a decoded copy"
    );
}

#[tokio::test]
async fn shared_collection_falls_back_to_bytes_for_every_consumer() {
    let moved = Arc::new(AtomicUsize::new(0));
    let handlers = HashMap::from([
        ("split".to_string(), handler(SplitWords)),
        (
            "pair".to_string(),
            handler(PairWithOne {
                moved: Arc::clone(&moved),
            }),
        ),
        ("format".to_string(), handler(Format)),
    ]);
    // `words` feeds an operator and a data sink, so its elements are encoded once and shared.
    let descriptor = DescriptorBuilder::new("typed_fanout")
        .stage("split", "pcoll_input", "words")
        .stage("pair", "words", "pairs")
        .stage("format", "pairs", "formatted")
        .sink("words_sink", "words")
        .sink(SINK_ID, "formatted")
        .build();

    let out = run_bundle(handlers, descriptor, &["x yy"]).await;

    assert_eq!(
        out.get("words_sink").cloned().unwrap_or_default(),
        vec!["x", "yy"]
    );
    assert_eq!(
        out.get(SINK_ID).cloned().unwrap_or_default(),
        vec!["x=1", "yy=1"]
    );
    assert_eq!(
        moved.load(Ordering::SeqCst),
        0,
        "a collection with a sink must reach the operator as decoded bytes"
    );
}

/// By-value sizes are sampled: the first 16 of a bundle, then one in 32 (17 of 64 here).
#[tokio::test]
async fn typed_values_have_their_sizes_sampled() {
    const WORDS: usize = 64;
    const SAMPLED_OF_64: i64 = 17;

    let handlers = HashMap::from([
        ("split".to_string(), handler(SplitWords)),
        (
            "pair".to_string(),
            handler(PairWithOne {
                moved: Arc::new(AtomicUsize::new(0)),
            }),
        ),
        ("format".to_string(), handler(Format)),
    ]);
    let descriptor = DescriptorBuilder::new("typed_sampling")
        .stage("split", "pcoll_input", "words")
        .stage("pair", "words", "pairs")
        .stage("format", "pairs", "formatted")
        .sink(SINK_ID, "formatted")
        .build();
    // Equal-length words, so every sampled size is the same.
    let line = (0..WORDS)
        .map(|i| format!("w{i:02}"))
        .collect::<Vec<_>>()
        .join(" ");

    let run = run_raw_bundle(handlers, descriptor, vec![encode_lines(&[&line])]).await;
    let infos = &run.bundle_response().monitoring_infos;

    assert_eq!(element_counts(infos).get("words"), Some(&(WORDS as i64)));
    let sizes = sampled_byte_sizes(infos)
        .get("words")
        .copied()
        .expect("the by-value collection reports sampled sizes");
    assert_eq!(sizes.count, SAMPLED_OF_64, "{sizes:?}");
    assert!(sizes.min > 0, "{sizes:?}");
    assert_eq!(sizes.min, sizes.max, "{sizes:?}");
    assert_eq!(sizes.sum, SAMPLED_OF_64 * sizes.min, "{sizes:?}");
}

/// User state needs the encoded key, so a stateful consumer is always fed bytes.
#[tokio::test]
async fn a_stateful_consumer_of_a_typed_producer_still_receives_its_key() {
    let observer = Observer::default();
    let handlers = HashMap::from([
        (
            "pair".to_string(),
            handler(PairWithOne {
                moved: Arc::new(AtomicUsize::new(0)),
            }),
        ),
        ("keyed".to_string(), observer.handler()),
    ]);
    let descriptor = DescriptorBuilder::new("typed_to_stateful")
        .with_coder("coder_varint", URN_VARINT, &[])
        .with_coder("coder_kv", URN_KV, &["coder_string", "coder_varint"])
        .stage("pair", "pcoll_input", "pairs")
        .pcollection("pairs", "coder_kv", "")
        .stateful_stage("keyed", "pairs", "keyed_out", &["count"])
        .sink(SINK_ID, "keyed_out")
        .build();

    let run = run_raw_bundle(handlers, descriptor, vec![encode_lines(&["k1", "k22"])]).await;
    run.bundle_response();

    let nested = |key: &str| {
        let mut bytes = Vec::new();
        StringUtf8Coder
            .encode(&key.to_string(), &mut bytes, Context::Nested)
            .expect("encode key");
        bytes
    };
    let keys: Vec<Option<Vec<u8>>> = observer.seen().into_iter().map(|o| o.key_bytes).collect();
    assert_eq!(keys, vec![Some(nested("k1")), Some(nested("k22"))]);
}
