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

//! Bundles end to end through a worker and an in-process runner over gRPC, as Java's
//! `ProcessBundleBenchmark` (sdks/java/harness/jmh). Each benchmark id names the Java method it
//! matches. Like JMH with `@Threads(16)`, 16 tasks send operations at the same time, and the
//! reported time is per operation, so 1 / time is JMH's ops/s.
//!
//! - `tiny_bundle`, `large_bundle`: one bundle of 1 or 1,000 empty byte arrays through
//!   [three outputs, length, key "foo"], as `testTinyBundle` and `testLargeBundle`.
//! - `state_without_caching`, `state_with_caching`: four bundles of one element each through a
//!   bag state DoFn, as `testStateWithoutCaching` and `testStateWithCaching`.
//! - `tiny_bundle_latency`: one task, so the time is one bundle's round trip.
//! - `small_bundle`, `small_bundle_oversubscribed`: bundles of 10 elements from 16 or 256
//!   tasks, as the small bundles of a streaming runner. With 256 tasks, more bundles run at
//!   the same time than the machine has CPUs.
//! - `streaming_bundle_cost/{1,4,16,256}`: per-element cost of interval-windowed streaming
//!   bundles across bundle sizes 1, 4, 16, and 256.
//! - `combined_bundle/{uncached,cached}`: one keyed streaming bundle that reads a singleton
//!   side input, appends and clears bag state, sets an event-time timer, and fires it.
//! - `outbound_flush_latency`: time to first byte before a 5 ms wait, with and without
//!   `flush_output`.
//!
//! The `process_bundle_cpu` group runs the same benchmarks and reports the CPU time of the
//! process per operation.
//!
//! The callers run on their own small runtime, and the worker and the runner services run on
//! another one. The bench encodes the inputs and decodes the outputs once, outside the
//! measurement.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=process_bundle
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use criterion::measurement::{Measurement, WallTime};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use prost::Message;

use beam::coders::{
    Coder, Context, DefaultCoder, IntervalWindowCoder, PaneInfo, TimerAction, TimerCoder,
    TimerRecord, URN_INTERVAL_WINDOW, URN_WINDOWED_VALUE, WindowedValue, WindowedValueCoder,
};
use beam::internals::{DoFnHandler, IntoParDoFn, PipelineInternals, TransformFn};
use beam::pipeline::Pipeline;
use beam::transforms::{
    BagState, Create, DoFn, NoTimers, ParDo, ProcessContext, State, StatefulContext, StatefulDoFn,
    Timers,
};
use beam::values::{AnySideInput, SingletonView};
use beam::windowing::{FixedWindows, IntervalWindow, WindowInto};
use beam_model::fn_execution::process_bundle_request::{CacheToken, cache_token};
use beam_model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleDescriptor, ProcessBundleRequest, RegisterRequest,
    RemoteGrpcPort, elements, instruction_request::Request,
};
use beam_model::pipeline::{self as proto, FunctionSpec};

#[path = "../tests/common/mod.rs"]
mod common;
use common::{DescriptorBuilder, SINK_ID, SOURCE_ID, STAGE_ID, TestBundle};

#[path = "support/cpu_time.rs"]
mod cpu_time;
use cpu_time::CpuTime;

#[path = "support/runner.rs"]
mod runner;
use runner::{BenchRunner, InboundTimers};

const THREADS: usize = 16;
/// Threads of the runtime that runs the callers. The callers mostly wait, so two are enough.
const CALLER_THREADS: usize = 2;

#[derive(Clone)]
struct OutputZeroOneTwo;

impl DoFn for OutputZeroOneTwo {
    type In = Vec<u8>;
    type Out = String;

    fn process_element(&mut self, _: Vec<u8>, out: &mut ProcessContext<String>) -> beam::Result {
        ["zero", "one", "two"]
            .into_iter()
            .try_for_each(|word| out.emit(word.to_string()))
    }
}

#[derive(Clone)]
struct ConvertLength;

impl DoFn for ConvertLength {
    type In = String;
    type Out = i64;

    fn process_element(&mut self, word: String, out: &mut ProcessContext<i64>) -> beam::Result {
        out.emit(word.len() as i64)
    }
}

#[derive(Clone)]
struct AddKeys;

impl DoFn for AddKeys {
    type In = i64;
    type Out = (String, i64);

    fn process_element(&mut self, n: i64, out: &mut ProcessContext<(String, i64)>) -> beam::Result {
        out.emit(("foo".to_string(), n))
    }
}

#[derive(State)]
struct Bag {
    values: BagState<String>,
}

/// Buffers three values of a key, then emits them on the fourth and clears the bag.
#[derive(Clone)]
struct StatefulOutputZeroOneTwo;

impl StatefulDoFn for StatefulOutputZeroOneTwo {
    type Key = String;
    type Value = String;
    type Out = (String, String);
    type State = Bag;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        key: &String,
        value: String,
        state: &mut Bag,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        let values = state.values.read()?;
        if values.len() == 3 {
            values
                .into_iter()
                .try_for_each(|v| ctx.emit((key.clone(), v)))?;
            state.values.clear()
        } else {
            state.values.append(&value)
        }
    }
}

#[derive(Clone)]
struct StreamIncrement;

impl DoFn for StreamIncrement {
    type In = (String, i64);
    type Out = (String, i64);

    fn process_element(
        &mut self,
        (key, n): (String, i64),
        out: &mut ProcessContext<(String, i64)>,
    ) -> beam::Result {
        out.emit((key, n + 1))
    }
}

#[derive(State)]
struct CombinedState {
    acc: BagState<i64>,
}

#[derive(Timers)]
enum CombinedTimer {
    #[timer(event_time, id = "flush")]
    Flush,
}

/// Reads a singleton side input, appends to bag state, sets an event-time timer, and emits
/// the accumulated sum when the timer fires.
#[derive(Clone)]
struct CombinedDoFn {
    threshold: SingletonView<i64>,
}

impl StatefulDoFn for CombinedDoFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = CombinedState;
    type Timer = CombinedTimer;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.threshold]
    }

    fn process_element(
        &mut self,
        _key: &String,
        value: i64,
        state: &mut CombinedState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        let limit = ctx.side_input(&self.threshold)?;
        state.acc.append(&value)?;
        if value < limit {
            ctx.timer(CombinedTimer::Flush)
                .at(beam::Instant::from_millis(19_999))?;
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        _timer: CombinedTimer,
        key: &String,
        state: &mut CombinedState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        let limit = ctx.side_input(&self.threshold)?;
        let sum: i64 = state.acc.read()?.into_iter().sum();
        state.acc.clear()?;
        ctx.emit((key.clone(), sum + limit))
    }
}

fn handler<F: DoFn>(f: F) -> TransformFn {
    Arc::new(DoFnHandler::new(f))
}

/// Counts the windowed values of type `T` in `bytes`.
fn count<T: DefaultCoder>(bytes: &[u8]) -> usize {
    let coder = WindowedValueCoder::new(T::coder());
    let mut reader = bytes;
    let mut n = 0;
    while !reader.is_empty() {
        coder
            .decode(&mut reader, Context::Nested)
            .expect("decoding an output element");
        n += 1;
    }
    n
}

fn encode<T: DefaultCoder>(values: impl IntoIterator<Item = T>) -> Vec<u8> {
    let coder = WindowedValueCoder::new(T::coder());
    let mut bytes = Vec::new();
    for value in values {
        coder
            .encode(
                &WindowedValue::global(value, 0),
                &mut bytes,
                Context::Nested,
            )
            .expect("encoding an input element");
    }
    bytes
}

fn encode_interval<T: DefaultCoder + Send + Sync + 'static>(
    values: impl IntoIterator<Item = T>,
    window: IntervalWindow,
    timestamp_ms: i64,
) -> Vec<u8> {
    let coder = WindowedValueCoder::with_window_coder(T::coder(), IntervalWindowCoder);
    let mut bytes = Vec::new();
    for value in values {
        let wv = WindowedValue::new(value, timestamp_ms, vec![window], PaneInfo::NO_FIRING);
        coder
            .encode(&wv, &mut bytes, Context::Nested)
            .expect("encoding an interval-windowed element");
    }
    bytes
}

fn count_interval<T: DefaultCoder + Send + Sync + 'static>(bytes: &[u8]) -> usize {
    let coder = WindowedValueCoder::with_window_coder(T::coder(), IntervalWindowCoder);
    let mut reader = bytes;
    let mut n = 0;
    while !reader.is_empty() {
        coder
            .decode(&mut reader, Context::Nested)
            .expect("decoding an interval-windowed output element");
        n += 1;
    }
    n
}

struct CombinedFixture {
    descriptor: ProcessBundleDescriptor,
    transform_id: String,
    side_input_id: String,
    handler_key: String,
    handler: TransformFn,
}

fn build_combined_fixture() -> CombinedFixture {
    let p = Pipeline::new();
    let threshold = p
        .apply("Threshold", Create::new(vec![100i64]))
        .apply(
            "ThresholdWin",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .as_singleton();
    let main_in = p
        .apply("MainIn", Create::new(vec![("k".to_string(), 1i64)]))
        .apply(
            "MainWin",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        );
    let main_out = main_in.apply("CombinedStage", ParDo::new(CombinedDoFn { threshold }));
    let in_pcoll = main_in.id().to_string();
    let out_pcoll = main_out.id().to_string();

    let proto = p.to_proto();
    let mut components = proto.components.expect("pipeline components");
    let (transform_id, stage_transform) = components
        .transforms
        .into_iter()
        .find(|(_, t)| {
            t.unique_name.contains("CombinedStage")
                && t.spec
                    .as_ref()
                    .is_some_and(|s| s.urn == "beam:transform:pardo:v1")
        })
        .expect("CombinedStage ParDo transform");
    let pardo = proto::ParDoPayload::decode(
        stage_transform
            .spec
            .as_ref()
            .expect("spec")
            .payload
            .as_slice(),
    )
    .expect("ParDoPayload");
    let side_input_id = pardo
        .side_inputs
        .keys()
        .next()
        .expect("side input tag")
        .clone();
    let handler_key = String::from_utf8(pardo.do_fn.expect("do_fn").payload).expect("handler key");
    let handler = p
        .transform_handlers()
        .remove(&handler_key)
        .expect("CombinedDoFn handler");

    let in_elem_coder = components.pcollections[&in_pcoll].coder_id.clone();
    let out_elem_coder = components.pcollections[&out_pcoll].coder_id.clone();
    let ws_id = components.pcollections[&in_pcoll]
        .windowing_strategy_id
        .clone();
    let win_coder = components.windowing_strategies[&ws_id]
        .window_coder_id
        .clone();

    components.coders.insert(
        "bench_w_in".to_string(),
        proto::Coder {
            spec: Some(FunctionSpec {
                urn: URN_WINDOWED_VALUE.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: vec![in_elem_coder, win_coder.clone()],
        },
    );
    components.coders.insert(
        "bench_w_out".to_string(),
        proto::Coder {
            spec: Some(FunctionSpec {
                urn: URN_WINDOWED_VALUE.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: vec![out_elem_coder, win_coder],
        },
    );

    let port = |coder_id: &str| {
        RemoteGrpcPort {
            api_service_descriptor: None,
            coder_id: coder_id.to_string(),
        }
        .encode_to_vec()
    };
    let transforms = HashMap::from([
        (
            SOURCE_ID.to_string(),
            proto::PTransform {
                unique_name: "Source".to_string(),
                spec: Some(FunctionSpec {
                    urn: beam_harness::bundle_processor::URN_DATA_SOURCE.to_string(),
                    payload: port("bench_w_in"),
                }),
                outputs: HashMap::from([("out".to_string(), in_pcoll)]),
                ..Default::default()
            },
        ),
        (transform_id.clone(), stage_transform),
        (
            SINK_ID.to_string(),
            proto::PTransform {
                unique_name: "Sink".to_string(),
                spec: Some(FunctionSpec {
                    urn: beam_harness::bundle_processor::URN_DATA_SINK.to_string(),
                    payload: port("bench_w_out"),
                }),
                inputs: HashMap::from([("in".to_string(), out_pcoll)]),
                ..Default::default()
            },
        ),
    ]);

    let descriptor = ProcessBundleDescriptor {
        id: "combined".to_string(),
        transforms,
        pcollections: components.pcollections,
        windowing_strategies: components.windowing_strategies,
        coders: components.coders,
        environments: components.environments,
        state_api_service_descriptor: None,
        timer_api_service_descriptor: None,
    };
    CombinedFixture {
        descriptor,
        transform_id,
        side_input_id,
        handler_key,
        handler,
    }
}

/// The coders of the ports: windowed bytes in, windowed KV<string, varint> or
/// KV<string, string> out.
fn with_port_coders(builder: DescriptorBuilder) -> DescriptorBuilder {
    builder
        .with_coder("bytes", "beam:coder:bytes:v1", &[])
        .with_coder("string", "beam:coder:string_utf8:v1", &[])
        .with_coder("varint", "beam:coder:varint:v1", &[])
        .with_coder("kv_long", "beam:coder:kv:v1", &["string", "varint"])
        .with_coder("kv_string", "beam:coder:kv:v1", &["string", "string"])
        .with_coder("window", "beam:coder:global_window:v1", &[])
        .with_coder(
            "w_bytes",
            "beam:coder:windowed_value:v1",
            &["bytes", "window"],
        )
        .with_coder(
            "w_kv_long",
            "beam:coder:windowed_value:v1",
            &["kv_long", "window"],
        )
        .with_coder(
            "w_kv_string",
            "beam:coder:windowed_value:v1",
            &["kv_string", "window"],
        )
}

fn descriptors(
    runner: &BenchRunner,
    mut combined: ProcessBundleDescriptor,
) -> Vec<ProcessBundleDescriptor> {
    let trivial = with_port_coders(DescriptorBuilder::new("trivial"))
        .with_source_coder("w_bytes")
        .stage("create", "pcoll_input", "words")
        .stage("len", "words", "lengths")
        .stage("addKeys", "lengths", "keyed")
        .sink_with_coder(SINK_ID, "keyed", "w_kv_long")
        .build();
    let mut stateful = with_port_coders(DescriptorBuilder::new("stateful"))
        .with_source_coder("w_kv_string")
        .stateful_stage("stateful", "pcoll_input", "outputs", &["values"])
        .sink_with_coder(SINK_ID, "outputs", "w_kv_string")
        .build();
    let bag = proto::StateSpec {
        protocol: Some(FunctionSpec {
            urn: "beam:user_state:bag:v1".to_string(),
            payload: Vec::new(),
        }),
        spec: Some(proto::state_spec::Spec::BagSpec(proto::BagStateSpec {
            element_coder_id: "string".to_string(),
        })),
    };
    let transform = stateful
        .transforms
        .get_mut("stateful")
        .expect("the stateful transform");
    let spec = transform.spec.as_mut().expect("a ParDo spec");
    let mut payload = proto::ParDoPayload::decode(spec.payload.as_slice()).expect("ParDoPayload");
    payload.state_specs = HashMap::from([("values".to_string(), bag)]);
    spec.payload = payload.encode_to_vec();
    let endpoint = runner.api_service_descriptor();
    stateful.state_api_service_descriptor = Some(endpoint.clone());
    combined.state_api_service_descriptor = Some(endpoint.clone());
    combined.timer_api_service_descriptor = Some(endpoint.clone());
    let streaming = with_port_coders(DescriptorBuilder::new("streaming"))
        .with_coder("iwindow", URN_INTERVAL_WINDOW, &[])
        .with_coder("w_stream_kv", URN_WINDOWED_VALUE, &["kv_long", "iwindow"])
        .with_source_coder("w_stream_kv")
        .pcollection("pcoll_input", "kv_long", "ws_interval")
        .pcollection("stream_out", "kv_long", "ws_interval")
        .windowing_strategy("ws_interval", "iwindow")
        .stage("stream_inc", "pcoll_input", "stream_out")
        .sink_with_coder(SINK_ID, "stream_out", "w_stream_kv")
        .build();
    let mut descriptors = vec![trivial, stateful, streaming, combined];
    for descriptor in &mut descriptors {
        for transform in descriptor.transforms.values_mut() {
            let Some(spec) = transform.spec.as_mut() else {
                continue;
            };
            if let Ok(mut port) = RemoteGrpcPort::decode(spec.payload.as_slice())
                && (spec.urn == beam_harness::bundle_processor::URN_DATA_SOURCE
                    || spec.urn == beam_harness::bundle_processor::URN_DATA_SINK)
            {
                port.api_service_descriptor = Some(endpoint.clone());
                spec.payload = port.encode_to_vec();
            }
        }
    }
    descriptors
}

/// Runs `op` `iterations` times split over `tasks` concurrent tasks, and returns what
/// `measurement` reads over the run. Each task is spawned on the runtime of the callers.
async fn concurrently<M, F, Fut>(measurement: &M, tasks: usize, iterations: u64, op: F) -> Duration
where
    M: Measurement<Value = Duration>,
    F: Fn(usize) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let per_task = iterations.div_ceil(tasks as u64);
    let started = measurement.start();
    let callers: Vec<_> = (0..tasks)
        .map(|task| {
            let op = op.clone();
            tokio::spawn(async move {
                for _ in 0..per_task {
                    op(task).await;
                }
            })
        })
        .collect();
    for caller in join_all(callers).await {
        caller.expect("a bench caller panicked");
    }
    // Scale to the requested count: the last round may run a few extra operations.
    measurement
        .end(started)
        .mul_f64(iterations as f64 / (per_task * tasks as u64) as f64)
}

fn process_bundle<M: Measurement<Value = Duration>>(
    c: &mut Criterion<M>,
    group_name: &str,
    measurement: &'static M,
) {
    // The worker and the runner services have their own runtime, so the callers do not take
    // the threads of the worker.
    let worker_rt = tokio::runtime::Runtime::new().expect("worker runtime");
    let callers_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(CALLER_THREADS)
        .enable_all()
        .build()
        .expect("callers runtime");
    let combined_fixture = build_combined_fixture();
    let mut threshold_bytes = Vec::new();
    100i64
        .encode_element(&mut threshold_bytes)
        .expect("encode side input i64");
    let side_inputs = HashMap::from([(
        (
            combined_fixture.transform_id.clone(),
            combined_fixture.side_input_id.clone(),
        ),
        threshold_bytes,
    )]);

    let handlers = HashMap::from([
        ("create".to_string(), handler(OutputZeroOneTwo)),
        ("len".to_string(), handler(ConvertLength)),
        ("addKeys".to_string(), handler(AddKeys)),
        ("stream_inc".to_string(), handler(StreamIncrement)),
        (
            "stateful".to_string(),
            StatefulOutputZeroOneTwo.into_handler(),
        ),
        (combined_fixture.handler_key, combined_fixture.handler),
    ]);
    let combined_transform_id = combined_fixture.transform_id;
    let combined_side_id = combined_fixture.side_input_id;
    let combined_descriptor = combined_fixture.descriptor;
    let runner = worker_rt.block_on(async {
        let runner = BenchRunner::start_with_side_inputs(handlers, side_inputs).await;
        runner
            .register(descriptors(&runner, combined_descriptor))
            .await;
        runner
    });
    let tiny = Arc::new(encode([Vec::<u8>::new()]));
    let large = Arc::new(encode(vec![Vec::<u8>::new(); 1_000]));
    let small = Arc::new(encode(vec![Vec::<u8>::new(); 10]));

    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(1));
    for (name, input, outputs, tasks) in [
        ("tiny_bundle", &tiny, 3, THREADS),
        ("large_bundle", &large, 3_000, THREADS),
        ("tiny_bundle_latency", &tiny, 3, 1),
        ("small_bundle", &small, 30, THREADS),
        ("small_bundle_oversubscribed", &small, 30, 256),
    ] {
        // Decode the output once here. The measured loop compares only its length.
        let output_len = callers_rt.block_on(async {
            let output = runner
                .process_bundle("trivial", SOURCE_ID, input.to_vec(), false)
                .await;
            assert_eq!(count::<(String, i64)>(&output), outputs);
            output.len()
        });
        group.bench_function(name, |b| {
            b.to_async(&callers_rt).iter_custom(|iterations| {
                let (runner, input) = (Arc::clone(&runner), Arc::clone(input));
                concurrently(measurement, tasks, iterations, move |_| {
                    let (runner, input) = (Arc::clone(&runner), Arc::clone(&input));
                    async move {
                        let output = runner
                            .process_bundle("trivial", SOURCE_ID, input.to_vec(), false)
                            .await;
                        assert_eq!(output.len(), output_len);
                    }
                })
            });
        });
    }
    // The four inputs of each task, encoded before the measurement.
    let state_inputs: Arc<Vec<[Vec<u8>; 4]>> = Arc::new(
        (0..THREADS)
            .map(|task| {
                let key = format!("{task:016x}");
                ["zero", "one", "two", "flush"]
                    .map(|value| encode([(key.clone(), value.to_string())]))
            })
            .collect(),
    );
    for (name, cache_state) in [
        ("state_without_caching", false),
        ("state_with_caching", true),
    ] {
        let run_task = {
            let (runner, state_inputs) = (Arc::clone(&runner), Arc::clone(&state_inputs));
            move |task: usize| {
                let (runner, state_inputs) = (Arc::clone(&runner), Arc::clone(&state_inputs));
                async move {
                    let mut outputs = Vec::new();
                    for input in &state_inputs[task] {
                        let output = runner
                            .process_bundle("stateful", SOURCE_ID, input.clone(), cache_state)
                            .await;
                        outputs.extend(output);
                    }
                    outputs
                }
            }
        };
        // Every task emits three values of a 16-character key, so all outputs have one length.
        let output_len = callers_rt.block_on(async {
            let output = run_task(0).await;
            assert_eq!(count::<(String, String)>(&output), 3);
            output.len()
        });
        group.bench_function(name, |b| {
            b.to_async(&callers_rt).iter_custom(|iterations| {
                let run_task = run_task.clone();
                concurrently(measurement, THREADS, iterations, move |task| {
                    let run_task = run_task.clone();
                    async move {
                        assert_eq!(run_task(task).await.len(), output_len);
                    }
                })
            });
        });
    }

    let interval = IntervalWindow::new(
        beam::Instant::from_millis(10_000),
        beam::Instant::from_millis(20_000),
    );
    for bundle_size in [1usize, 4, 16, 256] {
        let stream_input = Arc::new(encode_interval(
            (0..bundle_size).map(|i| (format!("k-{:04}", i & 63), i as i64)),
            interval,
            12_000,
        ));
        let output_len = callers_rt.block_on(async {
            let output = runner
                .process_bundle("streaming", SOURCE_ID, stream_input.to_vec(), false)
                .await;
            assert_eq!(count_interval::<(String, i64)>(&output), bundle_size);
            output.len()
        });
        group.throughput(Throughput::Elements(bundle_size as u64));
        group.bench_function(
            BenchmarkId::new("streaming_bundle_cost", bundle_size),
            |b| {
                b.to_async(&callers_rt).iter_custom(|iterations| {
                    let (runner, stream_input) = (Arc::clone(&runner), Arc::clone(&stream_input));
                    concurrently(measurement, THREADS, iterations, move |_| {
                        let (runner, stream_input) =
                            (Arc::clone(&runner), Arc::clone(&stream_input));
                        async move {
                            let output = runner
                                .process_bundle(
                                    "streaming",
                                    SOURCE_ID,
                                    stream_input.to_vec(),
                                    false,
                                )
                                .await;
                            assert_eq!(output.len(), output_len);
                        }
                    })
                });
            },
        );
    }

    let mut win_bytes = Vec::new();
    IntervalWindowCoder
        .encode(&interval, &mut win_bytes, Context::Nested)
        .expect("encode interval window");
    let combined_tasks: Arc<Vec<(Vec<u8>, InboundTimers)>> = Arc::new(
        (0..THREADS)
            .map(|task| {
                let key = format!("comb-{task:08x}");
                let elem = encode_interval([(key.clone(), 25i64)], interval, 12_000);
                let mut key_bytes = Vec::new();
                key.encode_element(&mut key_bytes).expect("encode key");
                let mut timer_bytes = Vec::new();
                TimerCoder::encode(
                    &TimerRecord {
                        user_key: key_bytes,
                        dynamic_tag: String::new(),
                        windows: vec![win_bytes.clone()],
                        action: TimerAction::Set {
                            fire_timestamp: beam::Instant::from_millis(20_000),
                            hold_timestamp: beam::Instant::from_millis(20_000),
                            pane: PaneInfo::NO_FIRING,
                        },
                    },
                    &mut timer_bytes,
                )
                .expect("encode timer record");
                (
                    elem,
                    InboundTimers {
                        transform_id: combined_transform_id.clone(),
                        timer_family_id: "flush".to_string(),
                        timers: timer_bytes,
                    },
                )
            })
            .collect(),
    );
    group.throughput(Throughput::Elements(1));
    for (label, cached) in [("uncached", false), ("cached", true)] {
        let tokens: Arc<Vec<CacheToken>> = Arc::new(if cached {
            vec![
                CacheToken {
                    token: b"combUserState".to_vec(),
                    r#type: Some(cache_token::Type::UserState(cache_token::UserState {})),
                },
                CacheToken {
                    token: b"combSideInput".to_vec(),
                    r#type: Some(cache_token::Type::SideInput(cache_token::SideInput {
                        transform_id: combined_transform_id.clone(),
                        side_input_id: combined_side_id.clone(),
                    })),
                },
            ]
        } else {
            Vec::new()
        });
        let output_min_len = callers_rt.block_on(async {
            let (ref elem, ref timers) = combined_tasks[0];
            let output = runner
                .process_bundle_ext(
                    "combined",
                    SOURCE_ID,
                    elem.clone(),
                    Some(timers.clone()),
                    tokens.to_vec(),
                )
                .await;
            assert_eq!(count_interval::<(String, i64)>(&output), 1);
            output.len().saturating_sub(4)
        });
        group.bench_function(BenchmarkId::new("combined_bundle", label), |b| {
            b.to_async(&callers_rt).iter_custom(|iterations| {
                let runner = Arc::clone(&runner);
                let combined_tasks = Arc::clone(&combined_tasks);
                let tokens = Arc::clone(&tokens);
                concurrently(measurement, THREADS, iterations, move |task| {
                    let runner = Arc::clone(&runner);
                    let combined_tasks = Arc::clone(&combined_tasks);
                    let tokens = Arc::clone(&tokens);
                    async move {
                        let (ref elem, ref timers) = combined_tasks[task];
                        let output = runner
                            .process_bundle_ext(
                                "combined",
                                SOURCE_ID,
                                elem.clone(),
                                Some(timers.clone()),
                                tokens.to_vec(),
                            )
                            .await;
                        assert!(output.len() >= output_min_len);
                    }
                })
            });
        });
    }
    group.finish();
}

/// Blocks a handler for up to its timeout unless the receiver wakes it after reading the
/// flushed chunk.
#[derive(Default)]
struct WaitLatch {
    done: Mutex<bool>,
    changed: Condvar,
}

impl WaitLatch {
    fn reset(&self) {
        *self.done.lock().expect("latch lock") = false;
    }

    fn wake(&self) {
        *self.done.lock().expect("latch lock") = true;
        self.changed.notify_all();
    }

    fn wait_timeout(&self, timeout: Duration) {
        let guard = self.done.lock().expect("latch lock");
        let _ = self
            .changed
            .wait_timeout_while(guard, timeout, |done| !*done)
            .expect("latch wait");
    }
}

/// Emits one 64-byte element, optionally flushes outbound buffers, and waits up to `wait`.
#[derive(Clone)]
struct EmitBeforeWait {
    flush: bool,
    wait: Duration,
    latch: Arc<WaitLatch>,
}

impl DoFn for EmitBeforeWait {
    type In = Vec<u8>;
    type Out = Vec<u8>;

    fn process_element(&mut self, _: Vec<u8>, out: &mut ProcessContext<Vec<u8>>) -> beam::Result {
        out.emit(vec![7u8; 64])?;
        if self.flush {
            out.flush_output()?;
        }
        self.latch.wait_timeout(self.wait);
        Ok(())
    }
}

/// Compares time to first byte on the outbound receiver when emitting a 64-byte element
/// before a 5 ms blocking wait, with and without [`ProcessContext::flush_output`].
fn outbound_flush_latency(c: &mut Criterion) {
    let worker_rt = tokio::runtime::Runtime::new().expect("worker runtime");
    let callers_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(CALLER_THREADS)
        .enable_all()
        .build()
        .expect("callers runtime");

    let wait = Duration::from_millis(5);
    let input = Arc::new(encode([Vec::<u8>::new()]));
    let next_id = Arc::new(AtomicU64::new(0));

    let mut group = c.benchmark_group("outbound_flush_latency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));

    for (name, flush) in [("with_flush", true), ("without_flush", false)] {
        let latch = Arc::new(WaitLatch::default());
        let TestBundle {
            data_out_rx,
            data_manager,
            control_client,
            ..
        } = TestBundle::builder()
            .handler(
                STAGE_ID,
                handler(EmitBeforeWait {
                    flush,
                    wait,
                    latch: Arc::clone(&latch),
                }),
            )
            .build();
        let descriptor = with_port_coders(DescriptorBuilder::new(name))
            .with_source_coder("w_bytes")
            .stage(STAGE_ID, "pcoll_input", "pcoll_output")
            .sink_with_coder(SINK_ID, "pcoll_output", "w_bytes")
            .build();
        worker_rt.block_on(async {
            let reg = control_client
                .handle_instruction(InstructionRequest {
                    instruction_id: format!("reg-{name}"),
                    request: Some(Request::Register(RegisterRequest {
                        process_bundle_descriptor: vec![descriptor],
                    })),
                })
                .await;
            assert!(reg.error.is_empty(), "register: {}", reg.error);
        });

        let data_out_rx = Arc::new(tokio::sync::Mutex::new(data_out_rx));
        let worker_handle = worker_rt.handle().clone();
        group.bench_function(name, |b| {
            b.to_async(&callers_rt).iter_custom(|iterations| {
                let data_out_rx = Arc::clone(&data_out_rx);
                let data_manager = data_manager.clone();
                let control_client = control_client.clone();
                let input = Arc::clone(&input);
                let latch = Arc::clone(&latch);
                let next_id = Arc::clone(&next_id);
                let worker_handle = worker_handle.clone();
                async move {
                    let mut rx = data_out_rx.lock().await;
                    let mut total = Duration::ZERO;
                    for _ in 0..iterations {
                        let instruction_id =
                            format!("flush-{name}-{}", next_id.fetch_add(1, Ordering::Relaxed));
                        latch.reset();
                        data_manager
                            .handle_inbound_elements(Elements {
                                data: vec![elements::Data {
                                    instruction_id: instruction_id.clone(),
                                    transform_id: SOURCE_ID.to_string(),
                                    data: input.to_vec(),
                                    is_last: true,
                                }],
                                timers: Vec::new(),
                            })
                            .await;

                        let started = Instant::now();
                        let bundle = worker_handle.spawn({
                            let control_client = control_client.clone();
                            async move {
                                control_client
                                    .handle_instruction(InstructionRequest {
                                        instruction_id,
                                        request: Some(Request::ProcessBundle(
                                            ProcessBundleRequest {
                                                process_bundle_descriptor_id: name.to_string(),
                                                ..Default::default()
                                            },
                                        )),
                                    })
                                    .await
                            }
                        });

                        let first = rx.recv().await.expect("first outbound chunk");
                        total += started.elapsed();
                        latch.wake();

                        let mut saw_last = first.data.iter().any(|d| d.is_last);
                        let mut saw_bytes: usize = first.data.iter().map(|d| d.data.len()).sum();
                        while !saw_last {
                            let next = rx.recv().await.expect("outbound EOF chunk");
                            saw_bytes += next.data.iter().map(|d| d.data.len()).sum::<usize>();
                            saw_last = next.data.iter().any(|d| d.is_last);
                        }
                        assert!(saw_bytes >= 64);
                        let resp = bundle.await.expect("bundle task");
                        assert!(resp.error.is_empty(), "bundle failed: {}", resp.error);
                    }
                    total
                }
            });
        });
    }
    group.finish();
}

fn wall_time(c: &mut Criterion) {
    process_bundle(c, "process_bundle", &WallTime);
    outbound_flush_latency(c);
}

fn cpu_time(c: &mut Criterion<CpuTime>) {
    process_bundle(c, "process_bundle_cpu", &CpuTime);
}

criterion_group!(benches, wall_time);
criterion_group! {
    name = cpu_benches;
    config = Criterion::default().with_measurement(CpuTime);
    targets = cpu_time
}
criterion_main!(benches, cpu_benches);
