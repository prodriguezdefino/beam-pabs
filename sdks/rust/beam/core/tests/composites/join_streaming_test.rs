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

//! Tests for streaming state-backed joins, direct join expansion, and lazy `CoGbkResult`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::support;

use beam_core::coders::{
    BeamIterable, CoderError, DefaultCoder, PageStream, StateStreamReader, VarIntCoder,
    WindowedHeader,
};
use beam_core::internals::{
    ElementSink, HandlerContext, PipelineInternals, RawUnionValue, TransformFn, TypedElement,
};
use beam_core::pipeline::{URN_FLATTEN, URN_GROUP_BY_KEY, URN_PAR_DO};
use beam_core::prelude::*;
use beam_core::schema::{FieldType, FieldValue};
use beam_core::transforms::{
    CoGroupByKey, FullOuterJoin, InnerJoin, KeyedPCollectionTuple, LeftJoin, RightJoin, TupleTag,
};
use beam_testing::{TestPipeline, passert};

const STATE_TOKEN: &[u8] = b"join-state-token";

/// Mock runner state reader that serves scripted pages and counts stream opens.
struct CountingStateReader {
    expected_token: Vec<u8>,
    pages: Vec<std::result::Result<Vec<u8>, String>>,
    fail_on_open: Option<usize>,
    opens: Arc<AtomicUsize>,
}

impl CountingStateReader {
    fn from_pages(pages: Vec<Vec<u8>>) -> (Arc<dyn StateStreamReader>, Arc<AtomicUsize>) {
        let opens = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(Self {
            expected_token: STATE_TOKEN.to_vec(),
            pages: pages.into_iter().map(Ok).collect(),
            fail_on_open: None,
            opens: Arc::clone(&opens),
        });
        (reader, opens)
    }

    fn with_scripted(
        pages: Vec<std::result::Result<Vec<u8>, String>>,
        fail_on_open: Option<usize>,
    ) -> (Arc<dyn StateStreamReader>, Arc<AtomicUsize>) {
        let opens = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(Self {
            expected_token: STATE_TOKEN.to_vec(),
            pages,
            fail_on_open,
            opens: Arc::clone(&opens),
        });
        (reader, opens)
    }
}

impl StateStreamReader for CountingStateReader {
    fn stream_runner_pages(&self, token: &[u8]) -> std::result::Result<PageStream, CoderError> {
        assert_eq!(token, self.expected_token.as_slice());
        let call = self.opens.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_on_open == Some(call) {
            return Err(CoderError::StateStream(format!(
                "open failed on pass {call}"
            )));
        }
        let pages = self.pages.clone();
        Ok(Box::new(
            pages
                .into_iter()
                .map(|p| p.map_err(CoderError::StateStream)),
        ))
    }
}

/// Sink that captures fused outputs by value without encoding them.
struct ValueSink<T>(Vec<T>);

impl<T: 'static> ElementSink for ValueSink<T> {
    fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
        panic!("expected push_value from DoFnHandler");
    }

    fn push_value(
        &mut self,
        _tag: Option<&str>,
        _header: Option<&WindowedHeader>,
        mut element: TypedElement<'_>,
    ) -> std::result::Result<(), String> {
        let value = element.take::<T>().expect("exact fused output type");
        self.0.push(value);
        Ok(())
    }
}

fn raw_left<V: DefaultCoder>(value: &V) -> RawUnionValue {
    RawUnionValue {
        tag: 0,
        value: value.encode().expect("encode left"),
    }
}

fn raw_right<V: DefaultCoder>(value: &V) -> RawUnionValue {
    RawUnionValue {
        tag: 1,
        value: value.encode().expect("encode right"),
    }
}

fn raw_tagged<V: DefaultCoder>(tag: i32, value: &V) -> RawUnionValue {
    RawUnionValue {
        tag,
        value: value.encode().expect("encode tagged"),
    }
}

/// Encodes `(key, state_backed_iterable)` with multiple inline chunks and a continuation token.
fn encode_state_backed_group<K: DefaultCoder>(
    key: &K,
    inline_chunks: &[&[RawUnionValue]],
    token: &[u8],
) -> Vec<u8> {
    let mut buf = key.encode().expect("encode key");
    buf.extend((-1i32).to_be_bytes());
    for chunk in inline_chunks {
        if !chunk.is_empty() {
            VarIntCoder::encode_varint(chunk.len() as i64, &mut buf).expect("chunk len");
            for rv in *chunk {
                rv.encode_element(&mut buf).expect("encode inline rv");
            }
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

fn run_handler(
    handler: &TransformFn,
    element: &[u8],
    reader: Option<&Arc<dyn StateStreamReader>>,
) -> std::result::Result<Vec<Vec<u8>>, String> {
    let mut instance = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    instance.setup()?;
    instance.start_bundle()?;
    {
        let mut ctx = HandlerContext::new(&mut sink).with_state_stream_reader(reader);
        instance.process(element, &mut ctx)?;
        instance.finish_bundle(&mut ctx)?;
    }
    instance.teardown()?;
    Ok(sink)
}

fn decode_outputs<T: DefaultCoder>(raw: Vec<Vec<u8>>) -> Vec<T> {
    raw.into_iter()
        .map(|b| T::decode(&b).expect("decode output"))
        .collect()
}

fn inner_handler<K, V1, V2>(max_items: usize, max_bytes: usize) -> TransformFn
where
    K: DefaultCoder + Eq + std::hash::Hash + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
{
    let p = Pipeline::new();
    let left = p.apply("Left", Create::new(Vec::<(K, V1)>::new()));
    let right = p.apply("Right", Create::new(Vec::<(K, V2)>::new()));
    let _ = left.apply(
        "Join",
        InnerJoin::new(&right).with_buffer_limits(max_items, max_bytes),
    );
    p.transform_handlers()
        .remove("Join/Join")
        .expect("InnerJoin handler")
}

fn left_handler<K, V1, V2>(max_items: usize, max_bytes: usize) -> TransformFn
where
    K: DefaultCoder + Eq + std::hash::Hash + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
{
    let p = Pipeline::new();
    let left = p.apply("Left", Create::new(Vec::<(K, V1)>::new()));
    let right = p.apply("Right", Create::new(Vec::<(K, V2)>::new()));
    let _ = left.apply(
        "Join",
        LeftJoin::new(&right).with_buffer_limits(max_items, max_bytes),
    );
    p.transform_handlers()
        .remove("Join/Join")
        .expect("LeftJoin handler")
}

fn right_handler<K, V1, V2>(max_items: usize, max_bytes: usize) -> TransformFn
where
    K: DefaultCoder + Eq + std::hash::Hash + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
{
    let p = Pipeline::new();
    let left = p.apply("Left", Create::new(Vec::<(K, V1)>::new()));
    let right = p.apply("Right", Create::new(Vec::<(K, V2)>::new()));
    let _ = left.apply(
        "Join",
        RightJoin::new(&right).with_buffer_limits(max_items, max_bytes),
    );
    p.transform_handlers()
        .remove("Join/Join")
        .expect("RightJoin handler")
}

fn full_handler<K, V1, V2>(max_items: usize, max_bytes: usize) -> TransformFn
where
    K: DefaultCoder + Eq + std::hash::Hash + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
{
    let p = Pipeline::new();
    let left = p.apply("Left", Create::new(Vec::<(K, V1)>::new()));
    let right = p.apply("Right", Create::new(Vec::<(K, V2)>::new()));
    let _ = left.apply(
        "Join",
        FullOuterJoin::new(&right).with_buffer_limits(max_items, max_bytes),
    );
    p.transform_handlers()
        .remove("Join/Join")
        .expect("FullOuterJoin handler")
}

type FullOuterRow = (i64, (Option<String>, Option<i64>));

fn assert_all_four_modes_match(
    key: i64,
    inline_chunks: &[&[RawUnionValue]],
    pages: Vec<Vec<u8>>,
    (max_items, max_bytes): (usize, usize),
    expected_left: &[String],
    expected_right: &[i64],
    expected_opens: [usize; 4],
) {
    let encoded = encode_state_backed_group(&key, inline_chunks, STATE_TOKEN);

    let expected_pairs = || {
        let mut pairs = Vec::new();
        for l in expected_left {
            for &r in expected_right {
                pairs.push((key, (l.clone(), r)));
            }
        }
        pairs.sort();
        pairs
    };

    // InnerJoin
    {
        let (reader, opens) = CountingStateReader::from_pages(pages.clone());
        let h = inner_handler::<i64, String, i64>(max_items, max_bytes);
        let mut out: Vec<(i64, (String, i64))> =
            decode_outputs(run_handler(&h, &encoded, Some(&reader)).expect("inner"));
        out.sort();
        assert_eq!(out, expected_pairs(), "InnerJoin output mismatch");
        assert_eq!(
            opens.load(Ordering::SeqCst),
            expected_opens[0],
            "InnerJoin opens"
        );
    }

    // LeftJoin
    {
        let (reader, opens) = CountingStateReader::from_pages(pages.clone());
        let h = left_handler::<i64, String, i64>(max_items, max_bytes);
        let mut out: Vec<(i64, (String, Option<i64>))> =
            decode_outputs(run_handler(&h, &encoded, Some(&reader)).expect("left"));
        out.sort();
        let mut expected: Vec<(i64, (String, Option<i64>))> = if expected_right.is_empty() {
            expected_left
                .iter()
                .map(|l| (key, (l.clone(), None)))
                .collect()
        } else {
            expected_pairs()
                .into_iter()
                .map(|(k, (l, r))| (k, (l, Some(r))))
                .collect()
        };
        expected.sort();
        assert_eq!(out, expected, "LeftJoin output mismatch");
        assert_eq!(
            opens.load(Ordering::SeqCst),
            expected_opens[1],
            "LeftJoin opens"
        );
    }

    // RightJoin
    {
        let (reader, opens) = CountingStateReader::from_pages(pages.clone());
        let h = right_handler::<i64, String, i64>(max_items, max_bytes);
        let mut out: Vec<(i64, (Option<String>, i64))> =
            decode_outputs(run_handler(&h, &encoded, Some(&reader)).expect("right"));
        out.sort();
        let mut expected: Vec<(i64, (Option<String>, i64))> = if expected_left.is_empty() {
            expected_right.iter().map(|&r| (key, (None, r))).collect()
        } else {
            expected_pairs()
                .into_iter()
                .map(|(k, (l, r))| (k, (Some(l), r)))
                .collect()
        };
        expected.sort();
        assert_eq!(out, expected, "RightJoin output mismatch");
        assert_eq!(
            opens.load(Ordering::SeqCst),
            expected_opens[2],
            "RightJoin opens"
        );
    }

    // FullOuterJoin
    {
        let (reader, opens) = CountingStateReader::from_pages(pages);
        let h = full_handler::<i64, String, i64>(max_items, max_bytes);
        let mut out: Vec<FullOuterRow> =
            decode_outputs(run_handler(&h, &encoded, Some(&reader)).expect("full"));
        out.sort();
        let mut expected: Vec<FullOuterRow> =
            match (expected_left.is_empty(), expected_right.is_empty()) {
                (true, true) => Vec::new(),
                (false, true) => expected_left
                    .iter()
                    .map(|l| (key, (Some(l.clone()), None)))
                    .collect(),
                (true, false) => expected_right
                    .iter()
                    .map(|&r| (key, (None, Some(r))))
                    .collect(),
                (false, false) => expected_pairs()
                    .into_iter()
                    .map(|(k, (l, r))| (k, (Some(l), Some(r))))
                    .collect(),
            };
        expected.sort();
        assert_eq!(out, expected, "FullOuterJoin output mismatch");
        assert_eq!(
            opens.load(Ordering::SeqCst),
            expected_opens[3],
            "FullOuterJoin opens"
        );
    }
}

#[test]
fn join_expands_directly_without_cogbk_result_stage() {
    let p = Pipeline::new();
    let left = p.apply("Users", Create::new(vec![(1i64, "Alice".to_string())]));
    let right = p.apply("Orders", Create::new(vec![(1i64, 100i64)]));
    let joined = left.apply("Join", InnerJoin::new(&right));

    let proto = p.to_proto();
    assert_eq!(
        support::graph::urn(support::graph::transform(&proto, "Join/TagLeft")),
        URN_PAR_DO
    );
    assert_eq!(
        support::graph::urn(support::graph::transform(&proto, "Join/TagRight")),
        URN_PAR_DO
    );
    assert_eq!(
        support::graph::urn(support::graph::transform(&proto, "Join/Flatten")),
        URN_FLATTEN
    );
    assert_eq!(
        support::graph::urn(support::graph::transform(&proto, "Join/GroupByKey")),
        URN_GROUP_BY_KEY
    );
    let join_node = support::graph::transform(&proto, "Join/Join");
    assert_eq!(support::graph::urn(join_node), URN_PAR_DO);
    assert_eq!(
        support::graph::input_producer_names(&proto, "Join/Join"),
        ["Join/GroupByKey"]
    );
    assert_eq!(
        support::graph::single_output(&proto, "Join/Join"),
        joined.id()
    );
}

#[test]
fn streaming_join_left_small_right_overflow_all_four_modes() {
    let left_vals: Vec<String> = vec!["L1".to_string(), "L2".to_string()];
    let right_vals: Vec<i64> = (1..=12).collect();

    // Split inline values across two chunks so multi-chunk prefix replay is covered.
    let chunk1 = vec![raw_right(&1i64), raw_left(&left_vals[0])];
    let chunk2 = vec![raw_right(&2i64), raw_right(&3i64)];
    let mut suffix: Vec<RawUnionValue> = (4..=12).map(|r| raw_right(&r)).collect();
    suffix.insert(5, raw_left(&left_vals[1]));

    assert_all_four_modes_match(
        7,
        &[&chunk1, &chunk2],
        encode_pages(&suffix, 3),
        (3, 64 * 1024),
        &left_vals,
        &right_vals,
        [2, 2, 2, 2],
    );
}

#[test]
fn streaming_join_left_overflow_right_small_all_four_modes() {
    let left_vals: Vec<String> = (1..=11).map(|i| format!("L{i:02}")).collect();
    let right_vals: Vec<i64> = vec![100, 200];

    let chunk1 = vec![raw_left(&left_vals[0]), raw_right(&right_vals[0])];
    let chunk2 = vec![raw_left(&left_vals[1])];
    let mut suffix: Vec<RawUnionValue> = left_vals[2..].iter().map(raw_left).collect();
    suffix.push(raw_right(&right_vals[1]));

    assert_all_four_modes_match(
        9,
        &[&chunk1, &chunk2],
        encode_pages(&suffix, 4),
        (3, 64 * 1024),
        &left_vals,
        &right_vals,
        [2, 2, 2, 2],
    );
}

#[test]
fn streaming_join_both_overflow_block_nested_loop_all_four_modes() {
    // 7 left items with max_items=3 -> 3 chunks (3 + 3 + 1):
    // 1 open for pass 1 + 1 open for left stream + 3 opens for right passes = 5 opens.
    let left_vals: Vec<String> = (1..=7).map(|i| format!("L{i}")).collect();
    let right_vals: Vec<i64> = (1..=5).collect();

    let chunk1 = vec![raw_left(&left_vals[0]), raw_right(&1i64)];
    let mut suffix = Vec::new();
    for i in 1..left_vals.len() {
        suffix.push(raw_left(&left_vals[i]));
        if i < right_vals.len() {
            suffix.push(raw_right(&right_vals[i]));
        }
    }

    assert_all_four_modes_match(
        42,
        &[&chunk1],
        encode_pages(&suffix, 3),
        (3, 64 * 1024),
        &left_vals,
        &right_vals,
        [5, 5, 5, 5],
    );
}

#[test]
fn streaming_join_block_nested_loop_exact_multiple_and_byte_cap() {
    // Exact multiple: 6 left items with max_items=3 -> 2 chunks (1 + 1 + 2 = 4 opens).
    let left_vals: Vec<String> = (1..=6).map(|i| format!("L{i}")).collect();
    let right_vals: Vec<i64> = (1..=4).collect();
    let mut suffix: Vec<RawUnionValue> = left_vals.iter().map(raw_left).collect();
    suffix.extend(right_vals.iter().map(raw_right));

    assert_all_four_modes_match(
        1,
        &[],
        encode_pages(&suffix, 3),
        (3, 64 * 1024),
        &left_vals,
        &right_vals,
        [4, 4, 4, 4],
    );

    // Byte cap smaller than a single encoded value forces 1-item chunks without stalling.
    let wide_left = vec!["AAAAAAAAAA".to_string(), "BBBBBBBBBB".to_string()];
    let wide_right = vec![10i64, 20i64];
    let mut byte_suffix: Vec<RawUnionValue> = wide_left.iter().map(raw_left).collect();
    byte_suffix.extend(wide_right.iter().map(raw_right));
    // max_bytes=1 gives one left element per chunk, so 4 opens.
    assert_all_four_modes_match(
        2,
        &[],
        encode_pages(&byte_suffix, 2),
        (100, 1),
        &wide_left,
        &wide_right,
        [4, 4, 4, 4],
    );
}

#[test]
fn streaming_join_empty_side_outer_joins_all_four_modes() {
    // Left empty, right overflows: Inner and Left skip pass 2 (1 open);
    // Right and Full stream (2 opens).
    let right_vals: Vec<i64> = (1..=8).collect();
    let right_inline = vec![raw_right(&1i64), raw_right(&2i64)];
    let right_suffix: Vec<RawUnionValue> = right_vals[2..].iter().map(raw_right).collect();
    assert_all_four_modes_match(
        10,
        &[&right_inline],
        encode_pages(&right_suffix, 2),
        (3, 64 * 1024),
        &[],
        &right_vals,
        [1, 1, 2, 2],
    );

    // Right empty, left overflows: Inner and Right skip pass 2 (1 open);
    // Left and Full stream (2 opens).
    let left_vals: Vec<String> = (1..=8).map(|i| format!("L{i}")).collect();
    let left_inline = vec![raw_left(&left_vals[0])];
    let left_suffix: Vec<RawUnionValue> = left_vals[1..].iter().map(raw_left).collect();
    assert_all_four_modes_match(
        11,
        &[&left_inline],
        encode_pages(&left_suffix, 3),
        (3, 64 * 1024),
        &left_vals,
        &[],
        [1, 2, 1, 2],
    );
}

#[test]
fn streaming_join_under_cap_finishes_in_one_pass() {
    let left_vals = vec!["A".to_string(), "B".to_string()];
    let right_vals = vec![1i64, 2, 3];
    let inline = vec![raw_left(&left_vals[0]), raw_right(&1i64)];
    let suffix = vec![raw_left(&left_vals[1]), raw_right(&2i64), raw_right(&3i64)];

    assert_all_four_modes_match(
        5,
        &[&inline],
        encode_pages(&suffix, 2),
        (10, 64 * 1024),
        &left_vals,
        &right_vals,
        [1, 1, 1, 1],
    );
}

#[test]
fn streaming_join_propagates_decode_and_state_errors() {
    let handler = inner_handler::<i64, i64, i64>(2, 64 * 1024);

    // Corrupt right value before overflow fails in pass 1.
    let bad_pass1 = vec![
        raw_left(&1i64),
        RawUnionValue {
            tag: 1,
            value: vec![0xFF],
        },
    ];
    let encoded = encode_state_backed_group(&1i64, &[], STATE_TOKEN);
    let (reader, _) = CountingStateReader::from_pages(encode_pages(&bad_pass1, 2));
    let err = run_handler(&handler, &encoded, Some(&reader)).unwrap_err();
    assert!(
        err.contains("Failed to decode Join value for tag index 1"),
        "{err}"
    );

    // Corrupt right value after overflow fails during pass 2 streaming probe.
    let bad_pass2 = vec![
        raw_left(&1i64),
        raw_right(&10i64),
        raw_right(&20i64),
        raw_right(&30i64),
        RawUnionValue {
            tag: 1,
            value: vec![0xFF],
        },
    ];
    let (reader, opens) = CountingStateReader::from_pages(encode_pages(&bad_pass2, 2));
    let err = run_handler(&handler, &encoded, Some(&reader)).unwrap_err();
    assert!(
        err.contains("Failed to decode Join value for tag index 1"),
        "{err}"
    );
    assert_eq!(opens.load(Ordering::SeqCst), 2);

    // Out-of-range union tag fails cleanly.
    let bad_tag = vec![raw_tagged(5, &1i64)];
    let (reader, _) = CountingStateReader::from_pages(encode_pages(&bad_tag, 1));
    let err = run_handler(&handler, &encoded, Some(&reader)).unwrap_err();
    assert!(
        err.contains("Join read a value with tag 5 but has 2 inputs"),
        "{err}"
    );

    // State stream reopen failure on pass 2 surfaces as an error.
    let overflow_pages = encode_pages(
        &[
            raw_left(&1i64),
            raw_right(&10i64),
            raw_right(&20i64),
            raw_right(&30i64),
        ],
        2,
    );
    let (reader, _) =
        CountingStateReader::with_scripted(overflow_pages.into_iter().map(Ok).collect(), Some(2));
    let err = run_handler(&handler, &encoded, Some(&reader)).unwrap_err();
    assert!(err.contains("open failed on pass 2"), "{err}");

    // Mid-stream page error fails cleanly.
    let (reader, _) = CountingStateReader::with_scripted(
        vec![
            Ok(encode_pages(&[raw_left(&1i64)], 1).pop().unwrap()),
            Err("page dropped".to_string()),
        ],
        None,
    );
    let err = run_handler(&handler, &encoded, Some(&reader)).unwrap_err();
    assert!(err.contains("page dropped"), "{err}");
}

#[tokio::test]
async fn join_preserves_dynamic_row_schemas_in_memory_and_streaming() {
    let schema = Arc::new(Schema::builder().field("name", FieldType::string()).build());
    let row = |name: &str| {
        Row::new(
            Arc::clone(&schema),
            vec![Some(FieldValue::String(name.to_string()))],
        )
        .expect("row matches schema")
    };

    // End-to-end in-memory Prism test with dynamic Row values on the left side.
    let p = TestPipeline::new();
    let users = p.apply(
        "Users",
        Create::new(vec![(1i64, row("Alice")), (2i64, row("Bob"))]),
    );
    let orders = p.apply("Orders", Create::new(vec![(1i64, 100i64), (1i64, 200i64)]));
    let joined = users.apply("Join", InnerJoin::new(&orders)).apply(
        "Names",
        Map::new(|(k, (u, amt)): (i64, (Row, i64))| {
            (k, u.get_string("name").unwrap().unwrap().to_string(), amt)
        }),
    );
    passert::that("AssertJoined", &joined).contains_in_any_order([
        (1i64, "Alice".to_string(), 100i64),
        (1i64, "Alice".to_string(), 200i64),
    ]);
    p.run().await.expect("pipeline and assertions");

    // State-backed streaming join with dynamic Row on left and overflowing right side.
    let p2 = Pipeline::new();
    let u2 = p2.apply("Users", Create::new(vec![(1i64, row("Alice"))]));
    let o2 = p2.apply("Orders", Create::new(vec![(1i64, 10i64)]));
    let _ = u2.apply("Join", InnerJoin::new(&o2).with_buffer_limits(2, 64 * 1024));
    let handler = p2
        .transform_handlers()
        .remove("Join/Join")
        .expect("row join handler");

    let encoded = encode_state_backed_group(&1i64, &[&[raw_left(&row("Alice"))]], STATE_TOKEN);
    let suffix: Vec<RawUnionValue> = (1..=5i64).map(|r| raw_right(&r)).collect();
    let (reader, opens) = CountingStateReader::from_pages(encode_pages(&suffix, 2));

    let mut sink = ValueSink::<(i64, (Row, i64))>(Vec::new());
    let mut instance = handler.instantiate();
    instance.setup().unwrap();
    instance.start_bundle().unwrap();
    {
        let mut ctx = HandlerContext::new(&mut sink).with_state_stream_reader(Some(&reader));
        instance.process(&encoded, &mut ctx).unwrap();
        instance.finish_bundle(&mut ctx).unwrap();
    }
    instance.teardown().unwrap();

    assert_eq!(opens.load(Ordering::SeqCst), 2);
    let mut got: Vec<(i64, String, i64)> = sink
        .0
        .into_iter()
        .map(|(k, (r, amt))| (k, r.get_string("name").unwrap().unwrap().to_string(), amt))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            (1, "Alice".to_string(), 1),
            (1, "Alice".to_string(), 2),
            (1, "Alice".to_string(), 3),
            (1, "Alice".to_string(), 4),
            (1, "Alice".to_string(), 5),
        ]
    );
}

#[test]
fn state_backed_cogbk_result_streams_lazily_above_cap_and_buffers_below() {
    let p = Pipeline::new();
    let users = p.apply("Users", Create::new(Vec::<(i64, String)>::new()));
    let orders = p.apply("Orders", Create::new(Vec::<(i64, i64)>::new()));
    let notes = p.apply("Notes", Create::new(Vec::<(i64, String)>::new()));
    let users_tag = TupleTag::<String>::new("users");
    let orders_tag = TupleTag::<i64>::new("orders");
    let notes_tag = TupleTag::<String>::new("notes");
    let _ = KeyedPCollectionTuple::of(&users_tag, &users)
        .and(&orders_tag, &orders)
        .and(&notes_tag, &notes)
        .apply("CoGbk", CoGroupByKey::new());
    let handler = p
        .transform_handlers()
        .remove("CoGbk/ConstructCoGbkResult")
        .expect("ConstructCoGbkResult handler");

    let run_cogbk = |inline: &[&[RawUnionValue]],
                     pages: Vec<Vec<u8>>|
     -> (CoGbkResult, Arc<AtomicUsize>) {
        let encoded = encode_state_backed_group(&7i64, inline, STATE_TOKEN);
        let (reader, opens) = CountingStateReader::from_pages(pages);
        let mut sink = ValueSink::<(i64, CoGbkResult)>(Vec::new());
        let mut instance = handler.instantiate();
        instance.setup().unwrap();
        instance.start_bundle().unwrap();
        {
            let mut ctx = HandlerContext::new(&mut sink).with_state_stream_reader(Some(&reader));
            instance.process(&encoded, &mut ctx).unwrap();
            instance.finish_bundle(&mut ctx).unwrap();
        }
        instance.teardown().unwrap();
        assert_eq!(sink.0.len(), 1);
        assert_eq!(sink.0[0].0, 7);
        (sink.0.pop().unwrap().1, opens)
    };

    // Below cap: stays in memory; `get` returns an in-memory iterable with no extra open.
    let small_inline = vec![raw_tagged(0, &"Alice".to_string())];
    let small_suffix = vec![raw_tagged(1, &10i64), raw_tagged(1, &20i64)];
    let (small_res, small_opens) = run_cogbk(&[&small_inline], encode_pages(&small_suffix, 2));
    assert_eq!(small_opens.load(Ordering::SeqCst), 1);
    let small_orders = small_res.get(&orders_tag).unwrap();
    assert!(small_orders.is_in_memory());
    assert_eq!(small_orders.into_vec().unwrap(), vec![10, 20]);
    assert_eq!(small_opens.load(Ordering::SeqCst), 1);

    // Above cap (> 10_000 items): switches to lazy streaming without buffering the hot key.
    let hot_count = 10_050i64;
    let hot_inline = vec![raw_tagged(0, &"Alice".to_string()), raw_tagged(1, &1i64)];
    let mut hot_suffix: Vec<RawUnionValue> = (2..=hot_count).map(|i| raw_tagged(1, &i)).collect();
    hot_suffix.push(raw_tagged(0, &"Bob".to_string()));
    let (hot_res, hot_opens) = run_cogbk(&[&hot_inline], encode_pages(&hot_suffix, 1_000));

    assert!(!hot_res.is_empty());
    assert_eq!(hot_res.tag_names(), &["users", "orders", "notes"]);
    assert_eq!(hot_res, hot_res.clone());
    assert_ne!(hot_res, small_res);
    assert!(format!("{hot_res:?}").contains("streaming: true"));
    // Pass 1 stopped on overflow; no extra opens until `get` is called.
    assert_eq!(hot_opens.load(Ordering::SeqCst), 1);

    // `get` returns a streaming BeamIterable for each tag and replays inline + state pages.
    let users_iter = hot_res.get(&users_tag).unwrap();
    assert!(!users_iter.is_in_memory());
    assert_eq!(hot_opens.load(Ordering::SeqCst), 2);
    assert_eq!(
        users_iter.into_vec().unwrap(),
        vec!["Alice".to_string(), "Bob".to_string()]
    );

    let orders_iter = hot_res.get(&orders_tag).unwrap();
    assert!(!orders_iter.is_in_memory());
    assert_eq!(hot_opens.load(Ordering::SeqCst), 3);
    let mut count = 0i64;
    let mut sum = 0i64;
    for item in orders_iter {
        sum += item.unwrap();
        count += 1;
    }
    assert_eq!(count, hot_count);
    assert_eq!(sum, hot_count * (hot_count + 1) / 2);

    // Empty tag and unknown index behave identically to in-memory CoGbkResult.
    assert!(hot_res.get_vec(&notes_tag).unwrap().is_empty());
    assert!(
        hot_res
            .get_by_index::<i64>(99)
            .unwrap()
            .into_vec()
            .unwrap()
            .is_empty()
    );
    assert!(hot_res.get(&TupleTag::<i64>::new("missing")).is_err());

    // Encoding a streaming CoGbkResult materializes it into the exact declared wire format.
    let wire = hot_res.encode().expect("encode streaming CoGbkResult");
    let decoded = CoGbkResult::decode(&wire).expect("decode CoGbkResult");
    assert_eq!(
        decoded.get_vec(&users_tag).unwrap(),
        vec!["Alice".to_string(), "Bob".to_string()]
    );
    assert_eq!(
        decoded.get_vec(&orders_tag).unwrap().len(),
        hot_count as usize
    );

    // Corrupt element in a streaming CoGbkResult reports the tag index during iteration.
    let mut broken_suffix: Vec<RawUnionValue> =
        (1..=10_010i64).map(|i| raw_tagged(1, &i)).collect();
    broken_suffix.push(RawUnionValue {
        tag: 1,
        value: vec![0xFF],
    });
    let (broken_res, _) = run_cogbk(&[], encode_pages(&broken_suffix, 1_000));
    let err = broken_res.get_vec(&orders_tag).unwrap_err();
    assert!(
        err.to_string()
            .contains("Failed to decode CoGbkResult value for tag index 1"),
        "{err}"
    );
}

#[test]
fn in_memory_join_fast_path_handles_one_to_one_and_empty_sides() {
    let inner = inner_handler::<i64, String, i64>(10, 64 * 1024);
    let left = left_handler::<i64, String, i64>(10, 64 * 1024);
    let right = right_handler::<i64, String, i64>(10, 64 * 1024);
    let full = full_handler::<i64, String, i64>(10, 64 * 1024);

    // 1:1 in-memory group exercises the owned-move fast path.
    let one_to_one = (
        1i64,
        BeamIterable::from_vec(vec![raw_left(&"Alice".to_string()), raw_right(&42i64)]),
    )
        .encode()
        .expect("encode 1:1");
    assert_eq!(
        decode_outputs::<(i64, (String, i64))>(run_handler(&inner, &one_to_one, None).unwrap()),
        vec![(1, ("Alice".to_string(), 42))]
    );
    assert_eq!(
        decode_outputs::<(i64, (String, Option<i64>))>(
            run_handler(&left, &one_to_one, None).unwrap()
        ),
        vec![(1, ("Alice".to_string(), Some(42)))]
    );
    assert_eq!(
        decode_outputs::<(i64, (Option<String>, i64))>(
            run_handler(&right, &one_to_one, None).unwrap()
        ),
        vec![(1, (Some("Alice".to_string()), 42))]
    );
    assert_eq!(
        decode_outputs::<(i64, (Option<String>, Option<i64>))>(
            run_handler(&full, &one_to_one, None).unwrap()
        ),
        vec![(1, (Some("Alice".to_string()), Some(42)))]
    );

    // Empty in-memory group emits nothing for any mode.
    let empty = (2i64, BeamIterable::<RawUnionValue>::from_vec(Vec::new()))
        .encode()
        .expect("encode empty");
    assert!(run_handler(&inner, &empty, None).unwrap().is_empty());
    assert!(run_handler(&left, &empty, None).unwrap().is_empty());
    assert!(run_handler(&right, &empty, None).unwrap().is_empty());
    assert!(run_handler(&full, &empty, None).unwrap().is_empty());
}

/// Sink that records the header of each output.
#[derive(Default)]
struct HeaderSink(Vec<(WindowedHeader, Vec<u8>)>);

impl ElementSink for HeaderSink {
    fn push(&mut self, element: Vec<u8>) -> std::result::Result<(), String> {
        self.0.push((WindowedHeader::default(), element));
        Ok(())
    }

    fn push_windowed(
        &mut self,
        header: &WindowedHeader,
        element: Vec<u8>,
    ) -> std::result::Result<(), String> {
        self.0.push((header.clone(), element));
        Ok(())
    }
}

/// Each output of a group in two interval windows keeps the windows and the timestamp of the
/// group, on the in-memory path and on the state-backed path.
#[test]
fn join_outputs_keep_the_two_windows_and_the_timestamp_of_the_group() {
    use beam_core::coders::PaneInfo;
    use beam_core::windowing::{BoundedWindow, IntervalWindow};

    let windows = [(0, 1_000), (500, 1_500)].map(|(start, end)| {
        BoundedWindow::Interval(IntervalWindow::new(
            Instant::from_millis(start),
            Instant::from_millis(end),
        ))
    });
    let header = WindowedHeader::global(999, PaneInfo::NO_FIRING)
        .with_windows(&windows)
        .expect("encode windows");
    let values = [
        raw_left(&"L1".to_string()),
        raw_right(&1i64),
        raw_left(&"L2".to_string()),
        raw_right(&2i64),
    ];
    let in_memory = (3i64, BeamIterable::from_vec(values.to_vec()))
        .encode()
        .expect("encode in-memory group");
    let state_backed = encode_state_backed_group(&3i64, &[&values[..1]], STATE_TOKEN);
    let (reader, _) = CountingStateReader::from_pages(encode_pages(&values[1..], 1));
    let handler = inner_handler::<i64, String, i64>(1, 64 * 1024);

    for (element, reader) in [(&in_memory, None), (&state_backed, Some(&reader))] {
        let mut instance = handler.instantiate();
        let mut sink = HeaderSink::default();
        instance.start_bundle().expect("start bundle");
        {
            let mut ctx = HandlerContext::new(&mut sink)
                .with_header(&header)
                .with_state_stream_reader(reader);
            instance.process(element, &mut ctx).expect("process");
        }
        let mut rows: Vec<(i64, (String, i64))> = sink
            .0
            .iter()
            .map(|(out_header, bytes)| {
                assert_eq!(out_header, &header);
                <(i64, (String, i64))>::decode(bytes).expect("decode row")
            })
            .collect();
        rows.sort();
        let expected: Vec<_> = [("L1", 1), ("L1", 2), ("L2", 1), ("L2", 2)]
            .map(|(l, r)| (3i64, (l.to_string(), r)))
            .into();
        assert_eq!(rows, expected, "state-backed: {}", reader.is_some());
    }
}

/// With the default caps, a side whose values pass 8 MiB streams from the state stream.
#[test]
fn a_side_over_the_default_byte_cap_streams_against_the_other_side() {
    let p = Pipeline::new();
    let left = p.apply("Left", Create::new(Vec::<(i64, String)>::new()));
    let right = p.apply("Right", Create::new(Vec::<(i64, i64)>::new()));
    let _ = left.apply("Join", InnerJoin::new(&right));
    let handler = p
        .transform_handlers()
        .remove("Join/Join")
        .expect("InnerJoin handler");

    let large: Vec<String> = ['a', 'b', 'c']
        .map(|c| c.to_string().repeat(3 * 1024 * 1024))
        .into();
    let mut values: Vec<RawUnionValue> = large.iter().map(raw_left).collect();
    values.extend([raw_right(&1i64), raw_right(&2i64)]);
    let element = encode_state_backed_group(&5i64, &[&values[..1]], STATE_TOKEN);
    let (reader, opens) = CountingStateReader::from_pages(encode_pages(&values[1..], 1));

    let rows = decode_outputs::<(i64, (String, i64))>(
        run_handler(&handler, &element, Some(&reader)).expect("join runs"),
    );
    assert_eq!(rows.len(), 6);
    for value in &large {
        for right in [1, 2] {
            assert!(rows.contains(&(5, (value.clone(), right))));
        }
    }
    assert_eq!(
        opens.load(Ordering::SeqCst),
        2,
        "one read and one re-read of the stream"
    );
}
