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
//!
//! The `process_bundle_cpu` group runs the same benchmarks and reports the CPU time of the
//! process per operation.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=process_bundle
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use criterion::measurement::{Measurement, WallTime};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use prost::Message;

use beam::coders::{Coder, Context, DefaultCoder, WindowedValue, WindowedValueCoder};
use beam::internals::{DoFnHandler, IntoParDoFn, TransformFn};
use beam::transforms::{
    BagState, DoFn, NoTimers, ProcessContext, State, StatefulContext, StatefulDoFn,
};
use model::pipeline::{self as proto, FunctionSpec};

#[path = "../tests/common/mod.rs"]
mod common;
use common::{DescriptorBuilder, SINK_ID, SOURCE_ID};

#[path = "support/cpu_time.rs"]
mod cpu_time;
use cpu_time::CpuTime;

#[path = "support/runner.rs"]
mod runner;
use runner::BenchRunner;

const THREADS: usize = 16;

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
            state.values.append(value)
        }
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

fn descriptors(runner: &BenchRunner) -> Vec<model::fn_execution::ProcessBundleDescriptor> {
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
    stateful.state_api_service_descriptor = Some(runner.api_service_descriptor());
    let mut descriptors = vec![trivial, stateful];
    for descriptor in &mut descriptors {
        for transform in descriptor.transforms.values_mut() {
            let Some(spec) = transform.spec.as_mut() else {
                continue;
            };
            if let Ok(mut port) =
                model::fn_execution::RemoteGrpcPort::decode(spec.payload.as_slice())
                && (spec.urn == harness::bundle_processor::URN_DATA_SOURCE
                    || spec.urn == harness::bundle_processor::URN_DATA_SINK)
            {
                port.api_service_descriptor = Some(runner.api_service_descriptor());
                spec.payload = port.encode_to_vec();
            }
        }
    }
    descriptors
}

/// Runs `op` `iterations` times split over `tasks` concurrent tasks, and returns what
/// `measurement` reads over the run. Each task is spawned, so the runner side of the callers
/// runs on all worker threads.
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
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let handlers = HashMap::from([
        ("create".to_string(), handler(OutputZeroOneTwo)),
        ("len".to_string(), handler(ConvertLength)),
        ("addKeys".to_string(), handler(AddKeys)),
        (
            "stateful".to_string(),
            StatefulOutputZeroOneTwo.into_handler(),
        ),
    ]);
    let runner = rt.block_on(async {
        let runner = BenchRunner::start(handlers).await;
        runner.register(descriptors(&runner)).await;
        runner
    });
    let tiny = encode([Vec::<u8>::new()]);
    let large = encode(vec![Vec::<u8>::new(); 1_000]);

    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(1));
    for (name, input, outputs, tasks) in [
        ("tiny_bundle", &tiny, 3, THREADS),
        ("large_bundle", &large, 3_000, THREADS),
        ("tiny_bundle_latency", &tiny, 3, 1),
    ] {
        group.bench_function(name, |b| {
            b.to_async(&rt).iter_custom(|iterations| {
                let (runner, input) = (Arc::clone(&runner), input.clone());
                concurrently(measurement, tasks, iterations, move |_| {
                    let (runner, input) = (Arc::clone(&runner), input.clone());
                    async move {
                        let output = runner
                            .process_bundle("trivial", SOURCE_ID, input, false)
                            .await;
                        assert_eq!(count::<(String, i64)>(&output), outputs);
                    }
                })
            });
        });
    }
    for (name, cache_state) in [
        ("state_without_caching", false),
        ("state_with_caching", true),
    ] {
        group.bench_function(name, |b| {
            b.to_async(&rt).iter_custom(|iterations| {
                let runner = Arc::clone(&runner);
                concurrently(measurement, THREADS, iterations, move |task| {
                    let runner = Arc::clone(&runner);
                    async move {
                        let key = format!("{task:016x}");
                        let mut outputs = 0;
                        for value in ["zero", "one", "two", "flush"] {
                            let input = encode([(key.clone(), value.to_string())]);
                            let output = runner
                                .process_bundle("stateful", SOURCE_ID, input, cache_state)
                                .await;
                            outputs += count::<(String, String)>(&output);
                        }
                        assert_eq!(outputs, 3);
                    }
                })
            });
        });
    }
    group.finish();
}

fn wall_time(c: &mut Criterion) {
    process_bundle(c, "process_bundle", &WallTime);
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
