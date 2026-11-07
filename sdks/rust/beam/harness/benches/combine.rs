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

//! The stages of a registered `CombinePerKey(Sum<i64>)` on hot keys, with a sink that drops
//! the output. Each iteration runs one bundle of 100,000 elements on 10 keys.
//!
//! - `precombine/<key>/process`: the elements arrive encoded.
//! - `precombine/<key>/process_value`: a fused step before the combine gives each element by
//!   value.
//! - `precombine_fanout/<key>/...`: as `precombine`, for the first phase of a combine with a
//!   hot key fanout. The keys are `(i32, K)`.
//! - `postcombine_fanout/<key>/process`: the precombine stage of the second phase of a hot
//!   key fanout. Each element is an accumulator of the first phase.
//! - `merge/<key>/process`: the merge stage after the shuffle. Each key has 10,000
//!   accumulators.
//!
//! The worker builds the handlers from the spec in the pipeline graph, so the bench builds
//! them in the same way. `<key>` is `string` or `i64`.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=combine
//! ```

use std::hint::black_box;
use std::time::Duration;

use beam::coders::{BeamIterable, DefaultCoder, PaneInfo, WindowedHeader};
use beam::internals::{
    BundleHandler, HandlerContext, TransformFn, TypedElement, registered_combine_handler,
};
use beam::pipeline::CombineStage;
use beam::prelude::*;
use beam_model::pipeline as proto;
use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main,
};
use prost::Message;

const ELEMENTS: usize = 100_000;
const KEYS: usize = 10;
const FANOUT: usize = 4;

#[path = "support/drop_sink.rs"]
mod drop_sink;
use drop_sink::DropSink;

/// The step of a `CombinePerKey` that a handler runs.
#[derive(Clone, Copy)]
enum Step {
    /// The combine without a hot key fanout.
    Combine,
    /// The first phase of a hot key fanout.
    PreCombine,
    /// The second phase of a hot key fanout.
    PostCombine,
}

/// The `combine_fn` spec payload of the `CombinePerKey` step `step` in `p`.
fn combine_fn_payload(p: &Pipeline, step: &str) -> Vec<u8> {
    let spec = p
        .to_proto()
        .components
        .expect("components")
        .transforms
        .into_values()
        .find(|t| t.unique_name == step)
        .and_then(|t| t.spec)
        .expect("combine step");
    proto::CombinePayload::decode(spec.payload.as_slice())
        .expect("CombinePayload")
        .combine_fn
        .expect("combine_fn")
        .payload
}

/// Builds the handler of `stage` of `step` for a `Sum<i64>` per key of type `K`. A hot key
/// fanout has [`FANOUT`] shards.
fn handler<K>(key: K, step: Step, stage: CombineStage) -> TransformFn
where
    K: DefaultCoder + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    let p = Pipeline::new();
    let input = p.apply("Create", Create::new(vec![(key, 1i64)]));
    let sum = CombinePerKey::new(Sum::<i64>::new());
    let (combine, name) = match step {
        Step::Combine => (sum, "Sum"),
        Step::PreCombine => (sum.with_hot_key_fanout(FANOUT), "Sum/PreCombine"),
        Step::PostCombine => (sum.with_hot_key_fanout(FANOUT), "Sum/PostCombine"),
    };
    let _ = input.apply("Sum", combine);
    registered_combine_handler(&combine_fn_payload(&p, name), stage.as_str())
        .expect("registered combine handler")
}

/// Runs one bundle of `elements` through `handler`, with `process` for each element.
fn run(
    handler: &TransformFn,
    process: impl FnOnce(&mut dyn BundleHandler, &mut HandlerContext<'_>),
) {
    let header = WindowedHeader::global(1_700_000_000_123, PaneInfo::NO_FIRING);
    let mut sink = DropSink;
    let mut instance = handler.instantiate();
    instance.setup().expect("setup");
    instance.start_bundle().expect("start_bundle");
    let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
    process(instance.as_mut(), &mut ctx);
    instance.finish_bundle(&mut ctx).expect("finish_bundle");
    instance.teardown().expect("teardown");
}

fn encoded<T: DefaultCoder>(elements: &[T]) -> Vec<Vec<u8>> {
    elements
        .iter()
        .map(|element| element.encode().expect("encode element"))
        .collect()
}

/// Adds the `process` bench of `handler`: one bundle of the encoded `elements`.
fn bench_process(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    handler: &TransformFn,
    elements: &[Vec<u8>],
) {
    group.bench_function(format!("{name}/process"), |b| {
        b.iter(|| {
            run(handler, |instance, ctx| {
                elements.iter().for_each(|element| {
                    instance.process(black_box(element), ctx).expect("process");
                });
            });
        });
    });
}

/// Adds the `process` and `process_value` benches of the precombine `handler` on `elements`.
fn bench_precombine<T>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    handler: &TransformFn,
    elements: &[T],
) where
    T: DefaultCoder + Clone + Send + 'static,
{
    bench_process(group, name, handler, &encoded(elements));
    group.bench_function(format!("{name}/process_value"), |b| {
        b.iter_batched(
            || elements.to_vec(),
            |values| {
                run(handler, |instance, ctx| {
                    values.into_iter().for_each(|value| {
                        let mut slot = Some(value);
                        instance
                            .process_value(TypedElement::new(&mut slot), ctx)
                            .expect("process_value");
                    });
                });
            },
            BatchSize::LargeInput,
        );
    });
}

/// Adds the `process` bench of the merge stage, with one grouped element for each key.
fn bench_merge<K>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str, keys: &[K])
where
    K: DefaultCoder + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    let merge = handler(keys[0].clone(), Step::Combine, CombineStage::Merge);
    let accumulators = (0..(ELEMENTS / KEYS) as i64).collect::<Vec<_>>();
    let grouped = keys
        .iter()
        .map(|key| {
            (key.clone(), BeamIterable::from_vec(accumulators.clone()))
                .encode()
                .expect("encode grouped element")
        })
        .collect::<Vec<_>>();
    bench_process(group, name, &merge, &grouped);
}

/// Adds the benches of each stage for keys `keys`.
fn bench_key<K>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str, keys: &[K])
where
    K: DefaultCoder + Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    let elements = (0..ELEMENTS)
        .map(|i| (keys[i % KEYS].clone(), (i % 100) as i64))
        .collect::<Vec<_>>();
    let precombine = handler(keys[0].clone(), Step::Combine, CombineStage::Precombine);
    bench_precombine(group, &format!("precombine/{name}"), &precombine, &elements);

    let sharded = (0..ELEMENTS)
        .map(|i| {
            (
                ((i % FANOUT) as i32, keys[i % KEYS].clone()),
                (i % 100) as i64,
            )
        })
        .collect::<Vec<_>>();
    let pre = handler(keys[0].clone(), Step::PreCombine, CombineStage::Precombine);
    bench_precombine(group, &format!("precombine_fanout/{name}"), &pre, &sharded);

    // The accumulator of `Sum<i64>` is an `i64`, so the elements of the second phase have
    // the type of the input elements.
    let post = handler(keys[0].clone(), Step::PostCombine, CombineStage::Precombine);
    bench_process(
        group,
        &format!("postcombine_fanout/{name}"),
        &post,
        &encoded(&elements),
    );

    bench_merge(group, &format!("merge/{name}"), keys);
}

fn combine(c: &mut Criterion) {
    let mut group = c.benchmark_group("combine");
    group.throughput(Throughput::Elements(ELEMENTS as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(4));

    let strings = (0..KEYS)
        .map(|i| format!("user-{i:04}-of-{KEYS:04}"))
        .collect::<Vec<_>>();
    bench_key(&mut group, "string", &strings);
    let ints = (0..KEYS as i64).collect::<Vec<_>>();
    bench_key(&mut group, "i64", &ints);

    group.finish();
}

criterion_group!(benches, combine);
criterion_main!(benches);
