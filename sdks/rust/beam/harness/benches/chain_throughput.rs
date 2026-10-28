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
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=chain_throughput
//! ```
//!
//! One iteration is one bundle. The time of a case is the time from the send of one bundle
//! to its response, and `thrpt` is the words of the bundle for each second. A bundle has
//! 2,000,000 words, but `read_loop` has about 580,000. Subtract the time of `plain` from the
//! time of a case that changes one step: the result is the cost of the change for one bundle.
//!
//! `plain` models WordCount's hot stage: split lines, pair each word with one, count at
//! bundle end. The user code is trivial, so harness work between operators dominates. Each of
//! these cases changes one step of `plain`:
//!
//! - `counter_per_word`: the pair step makes a user counter for each word and increments it.
//! - `counter_field`: the pair step increments a user counter in a field of the `DoFn`.
//! - `distribution_field`: the pair step updates a user distribution in a field of the
//!   `DoFn`.
//! - `result_output`: the pair step has a `Result` output, so each pair goes to a tagged
//!   output. No word fails.
//! - `window_into`: a `WindowInto` step is between the split step and the pair step.
//!
//! Two more cases have a different shape:
//!
//! - `data_sink`: the pair step writes to the data sink, so the harness encodes each pair.
//! - `read_loop`: chunks of about 1 MB of 16-byte words go to a step that counts the words
//!   and does not decode them. The read loop of the bundle dominates.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant as WallClock;

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use beam::coders::{
    Coder, Context, DefaultCoder, URN_GLOBAL_WINDOW, URN_KV, URN_STRING_UTF8, URN_VARINT,
    URN_WINDOWED_VALUE, WindowedValue, WindowedValueCoder,
};
use beam::internals::{
    BundleHandler, DoFnHandler, HandlerContext, HandlerInstance, TransformFn, WindowIntoHandler,
};
use beam::metrics::{Counter, Distribution, Metrics};
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};
use beam::windowing::{BoundedWindow, FixedWindows, GlobalWindow};
use beam_harness::control::ControlClient;
use beam_harness::data::DataManager;
use beam_model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleDescriptor, ProcessBundleRequest, RegisterRequest,
    elements, instruction_request::Request,
};

#[path = "../tests/common/mod.rs"]
mod common;
use common::{DescriptorBuilder, SINK_ID, SOURCE_ID, TestBundle};

const LINES: usize = 200_000;
const WORDS_PER_LINE: usize = 10;
/// The size of the windows of the `WindowInto` step.
const WINDOW: Duration = Duration::from_secs(10);
/// The word of the read loop chain. With its length byte, it encodes to 16 bytes.
const SMALL_WORD: &str = "0123456789abcde";
/// The chunks of one bundle of the read loop chain, and the size of one chunk.
const SMALL_CHUNKS: usize = 16;
const SMALL_CHUNK_BYTES: usize = 1 << 20;
/// The messages that the output queue holds. One bundle of the sink chain writes about 40
/// messages, and the bench reads the queue only after the bundle.
const OUTPUT_QUEUE: usize = 256;

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

/// [`PairWithOne`] that also increments an unbound user counter for each word.
#[derive(Clone)]
struct PairAndCount;

impl DoFn for PairAndCount {
    type In = String;
    type Out = (String, i64);

    fn process_element(
        &mut self,
        word: String,
        out: &mut ProcessContext<(String, i64)>,
    ) -> beam::Result {
        Metrics::counter("bench", "words").inc();
        out.emit((word, 1))
    }
}

/// [`PairWithOne`] that also increments a user counter field for each word.
#[derive(Clone)]
struct PairAndCountField {
    words: Counter,
}

impl DoFn for PairAndCountField {
    type In = String;
    type Out = (String, i64);

    fn process_element(
        &mut self,
        word: String,
        out: &mut ProcessContext<(String, i64)>,
    ) -> beam::Result {
        self.words.inc();
        out.emit((word, 1))
    }
}

/// [`PairWithOne`] that also updates a user distribution field for each word.
#[derive(Clone)]
struct PairAndMeasureField {
    lengths: Distribution,
}

impl DoFn for PairAndMeasureField {
    type In = String;
    type Out = (String, i64);

    fn process_element(
        &mut self,
        word: String,
        out: &mut ProcessContext<(String, i64)>,
    ) -> beam::Result {
        self.lengths.update(word.len() as i64);
        out.emit((word, 1))
    }
}

/// [`PairWithOne`] with a failure output. No word fails.
#[derive(Clone)]
struct PairOrFail;

impl DoFn for PairOrFail {
    type In = String;
    type Out = Result<(String, i64), String>;

    fn process_element(
        &mut self,
        word: String,
        out: &mut ProcessContext<Result<(String, i64), String>>,
    ) -> beam::Result {
        out.emit(Ok((word, 1)))
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

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, String>) -> beam::Result {
        out.emit(
            self.total.to_string(),
            BoundedWindow::Global(GlobalWindow),
            beam::Instant::END_OF_GLOBAL_WINDOW,
        )
    }
}

/// Counts the elements and does not decode them. Emits the count when the bundle finishes.
#[derive(Clone, Default)]
struct CountElements {
    seen: usize,
}

impl BundleHandler for CountElements {
    fn start_bundle(&mut self) -> Result<(), String> {
        self.seen = 0;
        Ok(())
    }

    fn process(&mut self, _element: &[u8], _ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        self.seen += 1;
        Ok(())
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let count = self.seen.to_string().encode().map_err(|e| e.to_string())?;
        ctx.sink.push(count)
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

fn handler<F: DoFn>(f: F) -> TransformFn {
    Arc::new(DoFnHandler::new(f))
}

/// One `value` in the global window, as a data port has it.
fn windowed<T: DefaultCoder>(value: T) -> Vec<u8> {
    let mut bytes = Vec::new();
    WindowedValueCoder::new(T::coder())
        .encode(
            &WindowedValue::global(value, 0),
            &mut bytes,
            Context::Nested,
        )
        .expect("encode windowed value");
    bytes
}

/// What the sink of a chain gets in one bundle.
enum Expected {
    /// The count of the words, as text.
    Count,
    /// This number of bytes.
    Bytes(usize),
}

impl Expected {
    /// Panics if `output`, the sink data of bundle `bundle`, is not the output of `words`.
    fn check(&self, output: &[Vec<u8>], words: usize, bundle: usize) {
        match self {
            Self::Count => {
                let count = words.to_string();
                assert!(
                    output
                        .iter()
                        .any(|data| data.windows(count.len()).any(|w| w == count.as_bytes())),
                    "bundle {bundle} did not count {count} words"
                );
            }
            Self::Bytes(bytes) => {
                let written: usize = output.iter().map(Vec::len).sum();
                assert_eq!(written, *bytes, "the sink bytes of bundle {bundle}");
            }
        }
    }
}

/// A chain to measure: its steps, its descriptor and the input of one bundle.
struct Chain {
    handlers: HashMap<String, TransformFn>,
    descriptor: ProcessBundleDescriptor,
    /// The data chunks of one bundle.
    chunks: Vec<Vec<u8>>,
    /// The words that the steps after the source get in one bundle.
    words: usize,
    expected: Expected,
}

/// Starts the descriptor of a chain on lines: the split step.
fn split() -> DescriptorBuilder {
    DescriptorBuilder::new("chain").stage("split", "pcoll_input", "words")
}

/// Ends the descriptor of a chain: the count step reads `pairs`, and the sink gets the count.
fn counted(chain: DescriptorBuilder) -> ProcessBundleDescriptor {
    chain
        .stage("count", "pairs", "total")
        .sink(SINK_ID, "total")
        .build()
}

/// A chain on the lines of one bundle in one chunk, with `pair` as its pair step.
fn on_lines(pair: TransformFn, descriptor: ProcessBundleDescriptor, expected: Expected) -> Chain {
    let window: TransformFn = Arc::new(WindowIntoHandler::new(Arc::new(FixedWindows::of(WINDOW))));
    let line = (0..WORDS_PER_LINE)
        .map(|i| format!("word{i}"))
        .collect::<Vec<_>>()
        .join(" ");
    Chain {
        handlers: HashMap::from([
            ("split".to_string(), handler(SplitWords)),
            ("window".to_string(), window),
            ("pair".to_string(), pair),
            ("count".to_string(), handler(CountAtFinish::default())),
        ]),
        descriptor,
        chunks: vec![windowed(line).repeat(LINES)],
        words: LINES * WORDS_PER_LINE,
        expected,
    }
}

/// The WordCount chain with `pair` as its pair step.
fn wordcount(pair: TransformFn) -> Chain {
    let descriptor = counted(split().stage("pair", "words", "pairs"));
    on_lines(pair, descriptor, Expected::Count)
}

/// The chain whose pair step writes to the data sink. The sink has the coder of the pairs.
fn to_sink() -> Chain {
    let descriptor = split()
        .with_coder("string", URN_STRING_UTF8, &[])
        .with_coder("varint", URN_VARINT, &[])
        .with_coder("pair", URN_KV, &["string", "varint"])
        .with_coder("window", URN_GLOBAL_WINDOW, &[])
        .with_coder("windowed_pair", URN_WINDOWED_VALUE, &["pair", "window"])
        .stage("pair", "words", "pairs")
        .sink_with_coder(SINK_ID, "pairs", "windowed_pair")
        .build();
    // Each word has five characters, so all pairs have the same size.
    let pair_bytes = windowed(("word0".to_string(), 1i64)).len();
    let expected = Expected::Bytes(LINES * WORDS_PER_LINE * pair_bytes);
    on_lines(handler(PairWithOne), descriptor, expected)
}

/// The chain that reads [`SMALL_CHUNKS`] chunks of [`SMALL_WORD`] and counts the words.
fn read_loop() -> Chain {
    let word = windowed(SMALL_WORD.to_string());
    let words_per_chunk = SMALL_CHUNK_BYTES / word.len();
    let count: TransformFn = Arc::new(CountElements::default());
    Chain {
        handlers: HashMap::from([("count".to_string(), count)]),
        descriptor: DescriptorBuilder::new("chain")
            .stage("count", "pcoll_input", "total")
            .sink(SINK_ID, "total")
            .build(),
        chunks: vec![word.repeat(words_per_chunk); SMALL_CHUNKS],
        words: words_per_chunk * SMALL_CHUNKS,
        expected: Expected::Count,
    }
}

async fn run_bundle(
    control: &ControlClient,
    dm: &DataManager,
    descriptor_id: &str,
    bundle: usize,
    chunks: Vec<Vec<u8>>,
) -> Duration {
    let instruction_id = format!("bundle-{bundle}");
    let last = chunks.len().saturating_sub(1);
    let data = chunks
        .into_iter()
        .enumerate()
        .map(|(index, data)| elements::Data {
            instruction_id: instruction_id.clone(),
            transform_id: SOURCE_ID.to_string(),
            data,
            is_last: index == last,
        })
        .collect();
    let feed = {
        let dm = dm.clone();
        async move {
            dm.handle_inbound_elements(Elements {
                data,
                timers: Vec::new(),
            })
            .await;
        }
    };
    let started = WallClock::now();
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

/// Adds the bench `name` of `chain` to `group`. One iteration is one bundle, and its time is
/// the time of [`run_bundle`]. The copy of the input and the check of the sink output are not
/// in that time.
fn bench_chain(
    group: &mut BenchmarkGroup<'_, WallTime>,
    runtime: &Runtime,
    name: &str,
    chain: Chain,
) {
    // The worker is made and dropped in the context of the runtime.
    let _context = runtime.enter();
    let Chain {
        handlers,
        descriptor,
        chunks,
        words,
        expected,
    } = chain;
    let TestBundle {
        mut data_out_rx,
        data_manager,
        control_client: control,
        ..
    } = TestBundle::builder()
        .capacity(OUTPUT_QUEUE)
        .handlers(handlers)
        .build();
    let descriptor_id = descriptor.id.clone();
    let reg = runtime.block_on(control.handle_instruction(InstructionRequest {
        instruction_id: "register".to_string(),
        request: Some(Request::Register(RegisterRequest {
            process_bundle_descriptor: vec![descriptor],
        })),
    }));
    assert!(reg.error.is_empty(), "register failed: {}", reg.error);

    let mut bundle = 0;
    group.throughput(Throughput::Elements(words as u64));
    group.bench_function(name, |b| {
        b.iter_custom(|iterations| {
            runtime.block_on(async {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    bundle += 1;
                    elapsed += run_bundle(
                        &control,
                        &data_manager,
                        &descriptor_id,
                        bundle,
                        chunks.clone(),
                    )
                    .await;
                    let output: Vec<Vec<u8>> = std::iter::from_fn(|| data_out_rx.try_recv().ok())
                        .flat_map(|elements| elements.data)
                        .map(|data| data.data)
                        .collect();
                    expected.check(&output, words, bundle);
                }
                elapsed
            })
        });
    });
}

fn chain_throughput(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("chain_throughput");
    // A bundle of 2,000,000 words is long, so each case has few samples.
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    let plain = wordcount(handler(PairWithOne));
    bench_chain(&mut group, &runtime, "plain", plain);
    let counter = wordcount(handler(PairAndCount));
    bench_chain(&mut group, &runtime, "counter_per_word", counter);
    let field = wordcount(handler(PairAndCountField {
        words: Metrics::counter("bench", "words"),
    }));
    bench_chain(&mut group, &runtime, "counter_field", field);
    let distribution = wordcount(handler(PairAndMeasureField {
        lengths: Metrics::distribution("bench", "word_lengths"),
    }));
    bench_chain(&mut group, &runtime, "distribution_field", distribution);
    let tagged = on_lines(
        handler(PairOrFail),
        counted(split().stage_with_outputs(
            "pair",
            "words",
            &[("output", "pairs"), ("errors", "failures")],
        )),
        Expected::Count,
    );
    bench_chain(&mut group, &runtime, "result_output", tagged);
    let with_window = on_lines(
        handler(PairWithOne),
        counted(
            split()
                .stage("window", "words", "windowed")
                .stage("pair", "windowed", "pairs"),
        ),
        Expected::Count,
    );
    bench_chain(&mut group, &runtime, "window_into", with_window);
    bench_chain(&mut group, &runtime, "data_sink", to_sink());
    bench_chain(&mut group, &runtime, "read_loop", read_loop());

    group.finish();
}

criterion_group!(benches, chain_throughput);
criterion_main!(benches);
