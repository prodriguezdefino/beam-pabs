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

//! Throughput benchmarks for direct joins and state-backed hot-key streaming joins.
//!
//! - `join_hot_key`: `1:100_000` and `10:100_000` state-backed `InnerJoin` and `LeftJoin`,
//!   comparing the streaming join handler with a collect-all baseline.
//! - `join_small_keys`: `10_000` in-memory keys with 2 elements each (1 left, 1 right),
//!   comparing the direct `InnerJoin` handler with a two-step `CoGbkResult` baseline.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Pbench=join
//! ```

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::{
    BeamIterable, CoderError, DefaultCoder, PageStream, PaneInfo, StateStreamReader, VarIntCoder,
    WindowedHeader,
};
use beam::internals::{
    BundleHandler, ElementSink, HandlerContext, PipelineInternals, RawUnionValue, TransformFn,
    TypedElement,
};
use beam::prelude::*;
use beam::transforms::{
    CoGbkResult, CoGroupByKey, InnerJoin, KeyedPCollectionTuple, LeftJoin, TupleTag,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

#[path = "support/drop_sink.rs"]
mod drop_sink;
use drop_sink::DropSink;

const STATE_TOKEN: &[u8] = b"bench-join-state";
const PAGE_ELEMENTS: usize = 2_000;
const INLINE_ELEMENTS: usize = 16;
const SMALL_KEY_COUNT: usize = 10_000;

/// In-memory page reader that replays pre-encoded state pages on every open.
struct BenchStateReader {
    pages: Vec<Vec<u8>>,
}

impl StateStreamReader for BenchStateReader {
    fn stream_runner_pages(&self, _token: &[u8]) -> std::result::Result<PageStream, CoderError> {
        let pages = self.pages.clone();
        Ok(Box::new(pages.into_iter().map(Ok)))
    }
}

fn raw_union<V: DefaultCoder>(tag: i32, value: &V) -> RawUnionValue {
    RawUnionValue {
        tag,
        value: value.encode().expect("encode value"),
    }
}

fn encode_state_backed_group(key: i64, inline: &[RawUnionValue], token: &[u8]) -> Vec<u8> {
    let mut buf = key.encode().expect("encode key");
    buf.extend((-1i32).to_be_bytes());
    if !inline.is_empty() {
        VarIntCoder::encode_varint(inline.len() as i64, &mut buf).expect("chunk len");
        for rv in inline {
            rv.encode_element(&mut buf).expect("encode inline rv");
        }
    }
    VarIntCoder::encode_varint(-1, &mut buf).expect("continuation marker");
    VarIntCoder::encode_varint(token.len() as i64, &mut buf).expect("token len");
    buf.extend_from_slice(token);
    buf
}

fn encode_pages(values: &[RawUnionValue], per_page: usize) -> Vec<Vec<u8>> {
    values
        .chunks(per_page.max(1))
        .map(|chunk| {
            let mut page = Vec::new();
            for rv in chunk {
                rv.encode_element(&mut page).expect("encode page rv");
            }
            page
        })
        .collect()
}

fn build_hot_key_fixture(
    left_count: usize,
    right_count: usize,
) -> (Vec<u8>, Arc<dyn StateStreamReader>) {
    let mut all = Vec::with_capacity(left_count + right_count);
    for i in 0..left_count {
        all.push(raw_union(0, &(i as i64)));
    }
    for i in 0..right_count {
        all.push(raw_union(1, &(i as i64)));
    }
    let split = INLINE_ELEMENTS.min(all.len());
    let encoded = encode_state_backed_group(1, &all[..split], STATE_TOKEN);
    let reader: Arc<dyn StateStreamReader> = Arc::new(BenchStateReader {
        pages: encode_pages(&all[split..], PAGE_ELEMENTS),
    });
    (encoded, reader)
}

fn run_bundle(
    handler: &TransformFn,
    reader: Option<&Arc<dyn StateStreamReader>>,
    process: impl FnOnce(&mut dyn BundleHandler, &mut HandlerContext<'_>),
) {
    let header = WindowedHeader::global(1_700_000_000_123, PaneInfo::NO_FIRING);
    let mut sink = DropSink;
    let mut instance = handler.instantiate();
    instance.setup().expect("setup");
    instance.start_bundle().expect("start_bundle");
    let mut ctx = HandlerContext::new(&mut sink)
        .with_header(&header)
        .with_state_stream_reader(reader);
    process(instance.as_mut(), &mut ctx);
    instance.finish_bundle(&mut ctx).expect("finish_bundle");
    instance.teardown().expect("teardown");
}

fn inner_join_handler() -> TransformFn {
    let p = Pipeline::new();
    let left = p.apply("Left", Create::new(Vec::<(i64, i64)>::new()));
    let right = p.apply("Right", Create::new(Vec::<(i64, i64)>::new()));
    let _ = left.apply("Inner", InnerJoin::new(&right));
    p.transform_handlers()
        .remove("Inner/Join")
        .expect("InnerJoin handler")
}

fn left_join_handler() -> TransformFn {
    let p = Pipeline::new();
    let left = p.apply("Left", Create::new(Vec::<(i64, i64)>::new()));
    let right = p.apply("Right", Create::new(Vec::<(i64, i64)>::new()));
    let _ = left.apply("LeftJoin", LeftJoin::new(&right));
    p.transform_handlers()
        .remove("LeftJoin/Join")
        .expect("LeftJoin handler")
}

/// Collect-all baseline for comparing against the streaming join handler.
fn collect_all_join_baseline(
    element: &[u8],
    reader: &Arc<dyn StateStreamReader>,
    left_outer: bool,
) {
    let header = WindowedHeader::global(1_700_000_000_123, PaneInfo::NO_FIRING);
    let mut sink = DropSink;
    let (key, raw_values) = <(i64, BeamIterable<RawUnionValue>)>::decode_with_context(
        element,
        None,
        None,
        Some(reader),
    )
    .expect("decode grouped");

    let mut by_tag: [Vec<Vec<u8>>; 2] = [Vec::new(), Vec::new()];
    for rv in raw_values.try_into_iter() {
        let RawUnionValue { tag, value } = rv.expect("read state item");
        by_tag[tag as usize].push(value);
    }
    let left: Vec<i64> = by_tag[0]
        .iter()
        .map(|b| i64::decode(b).expect("decode left"))
        .collect();
    let right: Vec<i64> = by_tag[1]
        .iter()
        .map(|b| i64::decode(b).expect("decode right"))
        .collect();

    if left_outer {
        if right.is_empty() {
            for l in left {
                let mut slot = Some((key, (l, Option::<i64>::None)));
                sink.push_value(None, Some(&header), TypedElement::new(&mut slot))
                    .expect("emit");
            }
        } else {
            for &l in &left {
                for &r in &right {
                    let mut slot = Some((key, (l, Some(r))));
                    sink.push_value(None, Some(&header), TypedElement::new(&mut slot))
                        .expect("emit");
                }
            }
        }
    } else {
        for &l in &left {
            for &r in &right {
                let mut slot = Some((key, (l, r)));
                sink.push_value(None, Some(&header), TypedElement::new(&mut slot))
                    .expect("emit");
            }
        }
    }
}

/// Fused sink forwarding `(i64, CoGbkResult)` into a downstream join step for the small-keys
/// baseline.
struct FusedCartesianSink {
    sink: DropSink,
}

impl ElementSink for FusedCartesianSink {
    fn push(&mut self, element: Vec<u8>) -> std::result::Result<(), String> {
        self.sink.push(element)
    }

    fn push_value(
        &mut self,
        _tag: Option<&str>,
        header: Option<&WindowedHeader>,
        mut element: TypedElement<'_>,
    ) -> std::result::Result<(), String> {
        let (key, res) = element
            .take::<(i64, CoGbkResult)>()
            .expect("fused CoGbkResult");
        let left = res
            .get_by_index::<i64>(0)
            .expect("left")
            .into_vec()
            .expect("left vec");
        let right = res
            .get_by_index::<i64>(1)
            .expect("right")
            .into_vec()
            .expect("right vec");
        for &l in &left {
            for &r in &right {
                let mut slot = Some((key, (l, r)));
                self.sink
                    .push_value(None, header, TypedElement::new(&mut slot))?;
            }
        }
        Ok(())
    }
}

fn cogbk_handler() -> TransformFn {
    let p = Pipeline::new();
    let left = p.apply("Left", Create::new(Vec::<(i64, i64)>::new()));
    let right = p.apply("Right", Create::new(Vec::<(i64, i64)>::new()));
    let _ = KeyedPCollectionTuple::of(&TupleTag::new("0"), &left)
        .and(&TupleTag::new("1"), &right)
        .apply("CoGbk", CoGroupByKey::new());
    p.transform_handlers()
        .remove("CoGbk/ConstructCoGbkResult")
        .expect("ConstructCoGbkResult handler")
}

fn join_hot_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_hot_key");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));

    let inner = inner_join_handler();
    let left = left_join_handler();

    for (left_count, right_count) in [(1usize, 100_000usize), (10, 100_000)] {
        let (encoded, reader) = build_hot_key_fixture(left_count, right_count);
        let label = format!("{left_count}x{right_count}");
        group.throughput(Throughput::Elements((left_count * right_count) as u64));

        group.bench_function(format!("streaming/inner_join/{label}"), |b| {
            b.iter(|| {
                run_bundle(&inner, Some(&reader), |instance, ctx| {
                    instance.process(black_box(&encoded), ctx).expect("process");
                });
            });
        });
        group.bench_function(format!("collect_all_baseline/inner_join/{label}"), |b| {
            b.iter(|| collect_all_join_baseline(black_box(&encoded), &reader, false));
        });

        group.bench_function(format!("streaming/left_join/{label}"), |b| {
            b.iter(|| {
                run_bundle(&left, Some(&reader), |instance, ctx| {
                    instance.process(black_box(&encoded), ctx).expect("process");
                });
            });
        });
        group.bench_function(format!("collect_all_baseline/left_join/{label}"), |b| {
            b.iter(|| collect_all_join_baseline(black_box(&encoded), &reader, true));
        });
    }

    group.finish();
}

fn join_small_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_small_keys");
    group.throughput(Throughput::Elements(SMALL_KEY_COUNT as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));

    let inner = inner_join_handler();
    let cogbk = cogbk_handler();
    let groups: Vec<Vec<u8>> = (0..SMALL_KEY_COUNT as i64)
        .map(|k| {
            let iter = BeamIterable::from_vec(vec![raw_union(0, &k), raw_union(1, &(k * 10))]);
            (k, iter).encode().expect("encode small group")
        })
        .collect();

    group.bench_function("direct/inner_join/10k_x_2", |b| {
        b.iter(|| {
            run_bundle(&inner, None, |instance, ctx| {
                for element in &groups {
                    instance.process(black_box(element), ctx).expect("process");
                }
            });
        });
    });

    group.bench_function("cogbk_baseline/inner_join/10k_x_2", |b| {
        b.iter(|| {
            let header = WindowedHeader::global(1_700_000_000_123, PaneInfo::NO_FIRING);
            let mut sink = FusedCartesianSink { sink: DropSink };
            let mut instance = cogbk.instantiate();
            instance.setup().expect("setup");
            instance.start_bundle().expect("start_bundle");
            let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
            for element in &groups {
                instance
                    .process(black_box(element), &mut ctx)
                    .expect("process");
            }
            instance.finish_bundle(&mut ctx).expect("finish_bundle");
            instance.teardown().expect("teardown");
        });
    });

    group.finish();
}

criterion_group!(benches, join_hot_key, join_small_keys);
criterion_main!(benches);
