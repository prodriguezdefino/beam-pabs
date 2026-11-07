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

//! Tests for chunked state-backed accumulator merging and `CombineFn::merge_two`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use beam_core::coders::{
    BeamIterable, CoderError, DefaultCoder, PageStream, StateStreamReader, VarIntCoder,
};
use beam_core::internals::{HandlerContext, PipelineInternals, TransformFn};
use beam_core::pipeline::{CombineStage, Pipeline, stage_key};
use beam_core::prelude::*;
use beam_core::transforms::{
    AnySample, ApproximateQuantiles, ApproximateUnique, CombineFn, Compose, Max, Mean, Min, Sample,
    Sum, Top,
};
use rust_decimal::Decimal;

use super::common::OpaqueKey;

const MERGE_CHUNK_SIZE: usize = 1024;

#[derive(Default)]
struct MergeStats {
    merge_calls: AtomicUsize,
    merge_two_calls: AtomicUsize,
    max_batch: AtomicUsize,
}

/// Unregistered sum that records `merge_accumulators` batch sizes and `merge_two` calls.
struct CountingMergeSum {
    stats: Arc<MergeStats>,
}

impl CountingMergeSum {
    fn new() -> (Self, Arc<MergeStats>) {
        let stats = Arc::new(MergeStats::default());
        (
            Self {
                stats: Arc::clone(&stats),
            },
            stats,
        )
    }
}

impl CombineFn for CountingMergeSum {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> Result<i64> {
        Ok(0)
    }

    fn add_input(&self, acc: i64, input: i64) -> Result<i64> {
        Ok(acc + input)
    }

    fn merge_accumulators(&self, accumulators: Vec<i64>) -> Result<i64> {
        self.stats.merge_calls.fetch_add(1, Ordering::Relaxed);
        self.stats
            .max_batch
            .fetch_max(accumulators.len(), Ordering::Relaxed);
        Ok(accumulators.into_iter().sum())
    }

    fn merge_two(&self, left: i64, right: i64) -> Result<i64> {
        self.stats.merge_two_calls.fetch_add(1, Ordering::Relaxed);
        Ok(left + right)
    }

    fn extract_output(&self, acc: i64) -> Result<i64> {
        Ok(acc)
    }
}

struct PageReader {
    expected_token: Vec<u8>,
    pages: Vec<Vec<u8>>,
}

impl StateStreamReader for PageReader {
    fn stream_runner_pages(&self, token: &[u8]) -> std::result::Result<PageStream, CoderError> {
        assert_eq!(token, self.expected_token.as_slice());
        let pages = self.pages.clone();
        Ok(Box::new(pages.into_iter().map(Ok)))
    }
}

/// Encodes `(key, state_backed_iterable)` with `inline` prefix elements and continuation `token`.
fn encode_state_backed_kv<K: DefaultCoder>(key: &K, inline: &[i64], token: &[u8]) -> Vec<u8> {
    let mut buf = key.encode().expect("encode key");
    buf.extend((-1i32).to_be_bytes());
    if !inline.is_empty() {
        VarIntCoder::encode_varint(inline.len() as i64, &mut buf).expect("encode chunk len");
        for &value in inline {
            VarIntCoder::encode_varint(value, &mut buf).expect("encode inline value");
        }
    }
    VarIntCoder::encode_varint(-1, &mut buf).expect("encode continuation marker");
    VarIntCoder::encode_varint(token.len() as i64, &mut buf).expect("encode token len");
    buf.extend_from_slice(token);
    buf
}

fn encode_varint_pages(values: &[i64], per_page: usize) -> Vec<Vec<u8>> {
    values
        .chunks(per_page.max(1))
        .map(|chunk| {
            let mut page = Vec::new();
            for &value in chunk {
                VarIntCoder::encode_varint(value, &mut page).expect("encode page value");
            }
            page
        })
        .collect()
}

fn run_with_state_reader(
    handler: &TransformFn,
    element: &[u8],
    reader: Option<&Arc<dyn StateStreamReader>>,
) -> Vec<Vec<u8>> {
    let mut instance = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    instance.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink).with_state_stream_reader(reader);
        instance.process(element, &mut ctx).expect("process");
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    sink
}

#[test]
fn merge_only_streams_state_backed_iterable_in_chunks() {
    let (combine_fn, stats) = CountingMergeSum::new();
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![("hot".to_string(), 0i64)]))
        .apply("Sum", CombinePerKey::new(combine_fn));
    let merge = p
        .transform_handlers()
        .remove(&stage_key("Sum", CombineStage::Merge))
        .expect("merge handler");

    let inline: Vec<i64> = (1..=50).collect();
    let suffix: Vec<i64> = (51..=2_550).collect();
    let token = b"merge-token";
    let encoded = encode_state_backed_kv(&"hot".to_string(), &inline, token);
    let reader: Arc<dyn StateStreamReader> = Arc::new(PageReader {
        expected_token: token.to_vec(),
        pages: encode_varint_pages(&suffix, 250),
    });

    let emitted = run_with_state_reader(&merge, &encoded, Some(&reader));
    let decoded: Vec<(String, i64)> = emitted
        .iter()
        .map(|b| <(String, i64)>::decode(b).expect("decode"))
        .collect();
    let expected_sum: i64 = (1..=2_550).sum();
    assert_eq!(decoded, vec![("hot".to_string(), expected_sum)]);

    let max_batch = stats.max_batch.load(Ordering::Relaxed);
    assert!(
        max_batch > 0 && max_batch <= MERGE_CHUNK_SIZE + 1,
        "max_batch={max_batch}"
    );
    assert!(stats.merge_calls.load(Ordering::Relaxed) >= 3);
}

#[test]
fn post_combine_grouped_values_streams_state_backed_iterable_in_chunks() {
    let (combine_fn, stats) = CountingMergeSum::new();
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![(OpaqueKey(1), 0i64)]))
        .apply("Sum", CombinePerKey::new(combine_fn).with_hot_key_fanout(3));
    let post_grouped = p
        .transform_handlers()
        .remove(&stage_key(
            "Sum/PostCombine",
            CombineStage::CombineGroupedValues,
        ))
        .expect("post combine grouped values handler");

    let inline: Vec<i64> = (1..=100).collect();
    let suffix: Vec<i64> = (101..=2_600).collect();
    let token = b"post-token";
    let encoded = encode_state_backed_kv(&OpaqueKey(1), &inline, token);
    let reader: Arc<dyn StateStreamReader> = Arc::new(PageReader {
        expected_token: token.to_vec(),
        pages: encode_varint_pages(&suffix, 300),
    });

    let emitted = run_with_state_reader(&post_grouped, &encoded, Some(&reader));
    let decoded: Vec<(OpaqueKey, i64)> = emitted
        .iter()
        .map(|b| <(OpaqueKey, i64)>::decode(b).expect("decode"))
        .collect();
    let expected_sum: i64 = (1..=2_600).sum();
    assert_eq!(decoded, vec![(OpaqueKey(1), expected_sum)]);

    let max_batch = stats.max_batch.load(Ordering::Relaxed);
    assert!(
        max_batch > 0 && max_batch <= MERGE_CHUNK_SIZE + 1,
        "max_batch={max_batch}"
    );
    assert!(stats.merge_calls.load(Ordering::Relaxed) >= 3);
}

#[test]
fn merge_only_fast_paths_small_in_memory_and_boundary_state_backed_iterables() {
    let (combine_fn, stats) = CountingMergeSum::new();
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![("k".to_string(), 0i64)]))
        .apply("Sum", CombinePerKey::new(combine_fn));
    let merge = p
        .transform_handlers()
        .remove(&stage_key("Sum", CombineStage::Merge))
        .expect("merge handler");

    let reset = || {
        stats.merge_calls.store(0, Ordering::Relaxed);
        stats.merge_two_calls.store(0, Ordering::Relaxed);
        stats.max_batch.store(0, Ordering::Relaxed);
    };

    // 0 in-memory elements: returns identity without calling merge.
    reset();
    let raw = ("k".to_string(), BeamIterable::<i64>::from_vec(vec![]))
        .encode()
        .expect("encode");
    let out = run_with_state_reader(&merge, &raw, None);
    assert_eq!(<(String, i64)>::decode(&out[0]).expect("decode").1, 0);
    assert_eq!(stats.merge_calls.load(Ordering::Relaxed), 0);
    assert_eq!(stats.merge_two_calls.load(Ordering::Relaxed), 0);

    // 1 in-memory element: still invokes `merge_accumulators` so the CombineFn lifecycle holds.
    reset();
    let raw = ("k".to_string(), BeamIterable::from_vec(vec![42i64]))
        .encode()
        .expect("encode");
    let out = run_with_state_reader(&merge, &raw, None);
    assert_eq!(<(String, i64)>::decode(&out[0]).expect("decode").1, 42);
    assert_eq!(stats.merge_calls.load(Ordering::Relaxed), 1);
    assert_eq!(stats.merge_two_calls.load(Ordering::Relaxed), 0);

    // 2 in-memory elements: calls `merge_two`, not `merge_accumulators`.
    reset();
    let raw = ("k".to_string(), BeamIterable::from_vec(vec![10i64, 32]))
        .encode()
        .expect("encode");
    let out = run_with_state_reader(&merge, &raw, None);
    assert_eq!(<(String, i64)>::decode(&out[0]).expect("decode").1, 42);
    assert_eq!(stats.merge_calls.load(Ordering::Relaxed), 0);
    assert_eq!(stats.merge_two_calls.load(Ordering::Relaxed), 1);

    // Exact chunk boundary (1024) and boundary + 1 (1025) over a state-backed iterable. Each
    // case gives (count, merge_accumulators calls, merge_two calls, largest batch).
    for (count, merges, merge_twos, batch) in [
        (0usize, 0, 0, 0),
        (1, 1, 0, 1),
        (MERGE_CHUNK_SIZE, 1, 0, MERGE_CHUNK_SIZE),
        (MERGE_CHUNK_SIZE + 1, 1, 1, MERGE_CHUNK_SIZE),
    ] {
        reset();
        let values: Vec<i64> = (1..=count as i64).collect();
        let token = b"edge";
        let raw = encode_state_backed_kv(&"k".to_string(), &[], token);
        let reader: Arc<dyn StateStreamReader> = Arc::new(PageReader {
            expected_token: token.to_vec(),
            pages: encode_varint_pages(&values, 128),
        });
        let out = run_with_state_reader(&merge, &raw, Some(&reader));
        let sum = <(String, i64)>::decode(&out[0]).expect("decode").1;
        assert_eq!(sum, values.iter().sum::<i64>(), "count={count}");
        assert_eq!(
            (
                stats.merge_calls.load(Ordering::Relaxed),
                stats.merge_two_calls.load(Ordering::Relaxed),
                stats.max_batch.load(Ordering::Relaxed),
            ),
            (merges, merge_twos, batch),
            "count={count}"
        );
    }
}

/// Checks that `merge_two` and `merge_accumulators` produce the same extracted output.
fn assert_merge_two_matches<CF>(combine_fn: &CF, left: CF::Accum, right: CF::Accum)
where
    CF: CombineFn,
    CF::Accum: Clone,
    CF::Output: PartialEq + std::fmt::Debug,
{
    let via_two = combine_fn
        .extract_output(
            combine_fn
                .merge_two(left.clone(), right.clone())
                .expect("merge_two"),
        )
        .expect("extract merge_two");
    let via_many = combine_fn
        .extract_output(
            combine_fn
                .merge_accumulators(vec![left, right])
                .expect("merge_accumulators"),
        )
        .expect("extract merge_accumulators");
    assert_eq!(via_two, via_many);
}

#[test]
fn merge_two_matches_merge_accumulators_across_builtins() -> Result {
    assert_merge_two_matches(&Sum::<i32>::new(), 12, -5);
    assert_merge_two_matches(&Sum::<i64>::new(), 100, -42);
    assert_merge_two_matches(&Sum::<f64>::new(), 1.5, 2.25);
    assert_merge_two_matches(
        &Sum::<Decimal>::new(),
        Decimal::new(1050, 2),
        Decimal::new(-325, 2),
    );
    assert!(Sum::<i64>::new().merge_two(i64::MAX, 1).is_err());

    assert_merge_two_matches(&Max::<i32>::new(), -10, 4);
    assert_merge_two_matches(&Max::<i64>::new(), 9, 3);
    assert_merge_two_matches(&Max::<f64>::new(), -2.5, 7.5);
    assert_merge_two_matches(
        &Max::<Decimal>::new(),
        Decimal::new(12, 1),
        Decimal::new(34, 1),
    );

    assert_merge_two_matches(&Min::<i32>::new(), -10, 4);
    assert_merge_two_matches(&Min::<i64>::new(), 9, 3);
    assert_merge_two_matches(&Min::<f64>::new(), -2.5, 7.5);
    assert_merge_two_matches(
        &Min::<Decimal>::new(),
        Decimal::new(12, 1),
        Decimal::new(34, 1),
    );

    assert_merge_two_matches(&Mean::<i64>::new(), (10.0, 2), (20.0, 3));
    assert_merge_two_matches(
        &Mean::<Decimal>::new(),
        (Decimal::new(10, 0), 2),
        (Decimal::new(20, 0), 3),
    );

    assert_merge_two_matches(&Top::largest(3), vec![1i64, 9, 4], vec![7, 2, 11, 5]);
    assert_merge_two_matches(&Top::smallest(3), vec![1i64, 9, 4], vec![7, 2, 11, 5]);
    assert_merge_two_matches(&Top::largest(0), vec![1i64, 2], vec![3, 4]);
    assert_merge_two_matches(&Top::largest(3), vec![1i64], vec![7, 2, 11, 5]);

    assert_merge_two_matches(
        &Sample::<i64>::new(3),
        vec![(10, 1), (30, 2)],
        vec![(20, 3), (40, 4), (5, 5)],
    );

    assert_merge_two_matches(&AnySample::<i64>::new(3), vec![1, 2], vec![3, 4, 5]);
    assert_merge_two_matches(&AnySample::<i64>::new(3), vec![1, 2, 3], vec![4, 5]);
    assert_merge_two_matches(&AnySample::<i64>::new(2), vec![1, 2, 3, 4], vec![5]);

    let on_fn = Sum::<i64>::new().on(|x: &(i64, i64)| x.0);
    assert_merge_two_matches(&on_fn, 15, 27);
    let mapped_fn = Sum::<i64>::new().map_output(|x| x * 2);
    assert_merge_two_matches(&mapped_fn, 15, 27);

    let c2 = Compose::new((Sum::new().on(|x: &i64| *x), Max::new().on(|x: &i64| *x)));
    assert_merge_two_matches(&c2, (10, 4), (20, 9));

    let c3 = Compose::new((
        Sum::new().on(|x: &i64| *x),
        Max::new().on(|x: &i64| *x),
        Min::new().on(|x: &i64| *x),
    ));
    assert_merge_two_matches(&c3, (10, 4, -2), (20, 9, 1));

    let c6 = Compose::new((
        Sum::new().on(|x: &i64| *x),
        Max::new().on(|x: &i64| *x),
        Min::new().on(|x: &i64| *x),
        Mean::new().on(|x: &i64| *x),
        Top::largest(2).on(|x: &i64| *x),
        AnySample::new(2).on(|x: &i64| *x),
    ));
    assert_merge_two_matches(
        &c6,
        (10, 4, -2, (10.0, 2), vec![4, 1], vec![1]),
        (20, 9, 1, (20.0, 3), vec![9, 5], vec![2, 3]),
    );

    let aq = ApproximateQuantiles::<i64, _>::new(5)?;
    let left_aq = (0..50i64).try_fold(aq.create_accumulator()?, |acc, v| aq.add_input(acc, v))?;
    let right_aq =
        (50..100i64).try_fold(aq.create_accumulator()?, |acc, v| aq.add_input(acc, v))?;
    let empty_aq = aq.create_accumulator()?;
    assert_merge_two_matches(&aq, empty_aq.clone(), right_aq.clone());
    assert_merge_two_matches(&aq, left_aq.clone(), empty_aq);
    assert_merge_two_matches(&aq, left_aq, right_aq);

    let au = ApproximateUnique::<i64>::with_sample_size(16)?;
    let left_au = (0..40i64).try_fold(au.create_accumulator()?, |acc, v| au.add_input(acc, v))?;
    let right_au = (20..60i64).try_fold(au.create_accumulator()?, |acc, v| au.add_input(acc, v))?;
    assert_merge_two_matches(&au, left_au, right_au);

    // `Arc<CF>` forwards `merge_two` without falling back to `merge_accumulators`.
    let (counting, stats) = CountingMergeSum::new();
    let shared = Arc::new(counting);
    assert_eq!(shared.merge_two(13, 29)?, 42);
    assert_eq!(stats.merge_two_calls.load(Ordering::Relaxed), 1);
    assert_eq!(stats.merge_calls.load(Ordering::Relaxed), 0);
    Ok(())
}

#[test]
fn fanout_post_combine_precombine_calls_merge_two_not_merge_accumulators() {
    let (combine_fn, stats) = CountingMergeSum::new();
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![("hot".to_string(), 0i64)]))
        .apply("Sum", CombinePerKey::new(combine_fn).with_hot_key_fanout(4));
    let post_precombine = p
        .transform_handlers()
        .remove(&stage_key("Sum/PostCombine", CombineStage::Precombine))
        .expect("post combine precombine handler");

    let mut instance = post_precombine.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    instance.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink);
        for v in 1..=10i64 {
            let raw = ("hot".to_string(), v).encode().expect("encode");
            instance.process(&raw, &mut ctx).expect("process");
        }
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }

    let out: Vec<(String, i64)> = sink
        .iter()
        .map(|b| <(String, i64)>::decode(b).expect("decode"))
        .collect();
    assert_eq!(out, vec![("hot".to_string(), 55)]);
    assert_eq!(stats.merge_calls.load(Ordering::Relaxed), 0);
    assert!(stats.merge_two_calls.load(Ordering::Relaxed) >= 10);
}
