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

//! Tests for `DefaultCoder::skip_nested` and the partial-combine `BundleHandler`.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::Arc;

use beam_core::Instant;
use beam_core::coders::{
    Coder, CoderError, CoderRegistry, Context, DefaultCoder, FieldMap, IntervalWindowCoder,
    PaneInfo, ShardedKey, StateStreamReader, StringUtf8Coder, VarIntCoder, WindowedHeader,
};
use beam_core::internals::{
    ElementSink, HandlerContext, PipelineInternals, TransformFn, TypedElement,
};
use beam_core::pipeline::{CombineStage, Pipeline, stage_key};
use beam_core::prelude::*;
use beam_core::schema::{Row, Schema};
use beam_core::windowing::{BoundedWindow, IntervalWindow};
use rust_decimal::Decimal;

/// Plain unregistered sum so `CombinePerKey` exposes its typed stage handlers directly.
struct PlainSum;

impl CombineFn for PlainSum {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> Result<i64> {
        Ok(0)
    }

    fn add_input(&self, acc: i64, input: i64) -> Result<i64> {
        Ok(acc + input)
    }

    fn merge_accumulators(&self, accs: Vec<i64>) -> Result<i64> {
        Ok(accs.into_iter().sum())
    }

    fn extract_output(&self, acc: i64) -> Result<i64> {
        Ok(acc)
    }
}

/// Builds the precombine handler of `CombinePerKey<K, PlainSum>`.
fn precombine_handler<K>(sample: K) -> TransformFn
where
    K: DefaultCoder + Eq + std::hash::Hash,
{
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![(sample, 0i64)]))
        .apply("Sum", CombinePerKey::new(PlainSum));
    p.transform_handlers()
        .remove(&stage_key("Sum", CombineStage::Precombine))
        .expect("precombine stage handler")
}

fn is_canonical<T: DefaultCoder>() -> bool {
    T::CANONICAL_BYTES
}

/// Checks that `skip_nested` stops at the same byte as `decode_element`, right before a sentinel.
fn assert_skip_round_trip<T>(value: &T)
where
    T: DefaultCoder + PartialEq + std::fmt::Debug,
{
    const SENTINEL: u8 = 0xAB;
    let mut encoded = value.encode().expect("encode");
    encoded.push(SENTINEL);

    let mut decoded_rest = encoded.as_slice();
    let decoded = T::decode_element(&mut decoded_rest).expect("decode_element");
    assert_eq!(&decoded, value);
    assert_eq!(decoded_rest, &[SENTINEL]);

    let mut skipped_rest = encoded.as_slice();
    T::skip_nested(&mut skipped_rest).expect("skip_nested");
    assert_eq!(skipped_rest, &[SENTINEL]);
}

#[test]
fn canonical_bytes_flag_matches_wire_representation() {
    assert!(is_canonical::<String>());
    assert!(is_canonical::<Vec<u8>>());
    assert!(is_canonical::<bool>());
    assert!(is_canonical::<i32>());
    assert!(is_canonical::<i64>());
    assert!(is_canonical::<()>());
    assert!(is_canonical::<(i32, String)>());
    assert!(is_canonical::<(String, bool, i64)>());
    assert!(is_canonical::<Option<String>>());
    assert!(is_canonical::<ShardedKey<String>>());
    assert!(is_canonical::<(i32, (String, Vec<u8>))>());

    assert!(!is_canonical::<f64>());
    assert!(!is_canonical::<f32>());
    assert!(!is_canonical::<Decimal>());
    assert!(!is_canonical::<Vec<i64>>());
    assert!(!is_canonical::<Row>());
    assert!(!is_canonical::<(String, Decimal)>());
    assert!(!is_canonical::<(i32, Vec<i64>)>());
    assert!(!is_canonical::<Option<Decimal>>());
    assert!(!is_canonical::<ShardedKey<Decimal>>());
}

#[test]
fn skip_nested_round_trips_primitives_strings_and_tuples() {
    for s in ["", "ascii", "héllo-🦀", &"a".repeat(200)] {
        assert_skip_round_trip(&s.to_string());
    }
    assert_skip_round_trip(&Vec::<u8>::new());
    assert_skip_round_trip(&vec![0u8, 1, 255, 42]);
    assert_skip_round_trip(&false);
    assert_skip_round_trip(&true);
    for n in [0i32, 1, -1, i32::MIN, i32::MAX] {
        assert_skip_round_trip(&n);
    }
    for n in [0i64, 1, -1, i64::MIN, i64::MAX] {
        assert_skip_round_trip(&n);
    }
    assert_skip_round_trip(&());
    assert_skip_round_trip(&(7i32, "sharded-key".to_string()));
    assert_skip_round_trip(&("a".to_string(), true, -99i64));
    assert_skip_round_trip(&(1i32, "b".to_string(), false, 42i64));
    assert_skip_round_trip(&Option::<String>::None);
    assert_skip_round_trip(&Some("present".to_string()));
    assert_skip_round_trip(&ShardedKey::new("k".to_string(), &b"shard"[..]));

    // `String::skip_nested` skips framing without checking UTF-8; `decode_element` rejects it.
    let invalid_utf8 = [0x02u8, 0xFF, 0xFE, 0xAB];
    let mut skipped = invalid_utf8.as_slice();
    String::skip_nested(&mut skipped).expect("skip_nested skips length-prefixed bytes");
    assert_eq!(skipped, &[0xAB]);
    assert!(String::decode_element(&mut invalid_utf8.as_slice()).is_err());

    // Invalid boolean and out-of-range i32 varints fail `skip_nested`.
    assert!(bool::skip_nested(&mut [2u8].as_slice()).is_err());
    let mut big = Vec::new();
    VarIntCoder::encode_varint(i64::MAX, &mut big).expect("encode varint");
    assert!(i32::skip_nested(&mut big.as_slice()).is_err());
    assert!(Option::<i32>::skip_nested(&mut [2u8].as_slice()).is_err());
}

thread_local! {
    static KEY_COUNTS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

fn reset_key_counts() {
    KEY_COUNTS.with(|c| c.set((0, 0)));
}

fn key_counts() -> (usize, usize) {
    KEY_COUNTS.with(Cell::get)
}

/// Canonical string key that counts `decode_element` and `encode_element` calls on this thread.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CountingKey(String);

struct CountingKeyCoder;

impl Coder<CountingKey> for CountingKeyCoder {
    fn urn(&self) -> &'static str {
        "beam:coder:test:counting_key:v1"
    }

    fn encode(
        &self,
        value: &CountingKey,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> std::result::Result<(), CoderError> {
        StringUtf8Coder.encode(&value.0, writer, context)
    }

    fn decode(
        &self,
        reader: &mut &[u8],
        context: Context,
    ) -> std::result::Result<CountingKey, CoderError> {
        StringUtf8Coder.decode(reader, context).map(CountingKey)
    }
}

impl DefaultCoder for CountingKey {
    type Coder = CountingKeyCoder;
    const CANONICAL_BYTES: bool = true;

    fn coder() -> Self::Coder {
        CountingKeyCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> std::result::Result<(), CoderError> {
        KEY_COUNTS.with(|c| {
            let (decodes, encodes) = c.get();
            c.set((decodes, encodes + 1));
        });
        self.0.encode_element(writer)
    }

    fn decode_element(reader: &mut &[u8]) -> std::result::Result<Self, CoderError> {
        KEY_COUNTS.with(|c| {
            let (decodes, encodes) = c.get();
            c.set((decodes + 1, encodes));
        });
        String::decode_element(reader).map(CountingKey)
    }

    fn decode_whole_stream_with_context(
        reader: &mut &[u8],
        _schema: Option<&Arc<Schema>>,
        _field_map: Option<&FieldMap>,
        _state_reader: Option<&Arc<dyn StateStreamReader>>,
    ) -> std::result::Result<Self, CoderError> {
        Self::coder().decode(reader, Context::WholeStream)
    }

    fn skip_nested(reader: &mut &[u8]) -> std::result::Result<(), CoderError> {
        String::skip_nested(reader)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(CountingKeyCoder.urn(), Vec::new())
    }
}

/// Sink that accepts typed values by value without encoding them.
struct TypedSink<T>(Vec<T>);

impl<T> Default for TypedSink<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T: 'static> ElementSink for TypedSink<T> {
    fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
        Err("expected push_value".to_string())
    }

    fn push_value(
        &mut self,
        _tag: Option<&str>,
        _header: Option<&WindowedHeader>,
        mut element: TypedElement<'_>,
    ) -> std::result::Result<(), String> {
        let value = element
            .take::<T>()
            .ok_or_else(|| "typed element did not match expected type".to_string())?;
        self.0.push(value);
        Ok(())
    }
}

#[test]
fn precombine_process_hits_without_decoding_or_reencoding_key() {
    let handler = precombine_handler(CountingKey("seed".to_string()));
    let keys = ["k0", "k1", "k2"];
    let encoded: Vec<Vec<u8>> = (0..300i64)
        .map(|i| {
            (CountingKey(keys[(i as usize) % keys.len()].to_string()), i)
                .encode()
                .expect("encode")
        })
        .collect();

    reset_key_counts();
    let mut instance = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    instance.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink);
        for element in &encoded {
            instance.process(element, &mut ctx).expect("process");
        }
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    let (decodes, encodes) = key_counts();
    // Each of the 3 unique keys decodes once on miss and never re-encodes on drain.
    assert_eq!(decodes, 3, "only misses decode the key");
    assert_eq!(encodes, 0, "drain writes cached key bytes directly");

    let mut partials: Vec<(CountingKey, i64)> = sink
        .iter()
        .map(|bytes| <(CountingKey, i64)>::decode(bytes).expect("decode"))
        .collect();
    partials.sort();
    let expected: Vec<(CountingKey, i64)> = keys
        .iter()
        .enumerate()
        .map(|(rem, &k)| {
            let sum: i64 = (0..300i64).filter(|i| (*i as usize) % 3 == rem).sum();
            (CountingKey(k.to_string()), sum)
        })
        .collect();
    assert_eq!(partials, expected);

    // On the fused `process_value` path, a typed sink sees zero key decodes and encodes the
    // key only when the entry weigher samples memory (16 warm-up samples + 1 in 64 updates).
    reset_key_counts();
    let mut typed_sink = TypedSink::<(CountingKey, i64)>::default();
    instance.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut typed_sink);
        for i in 0..300i64 {
            let mut slot = Some((CountingKey(keys[(i as usize) % keys.len()].to_string()), i));
            instance
                .process_value(TypedElement::new(&mut slot), &mut ctx)
                .expect("process_value");
            assert!(slot.is_none());
        }
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    let (decodes, encodes) = key_counts();
    assert_eq!(decodes, 0, "typed path never decodes keys");
    assert_eq!(
        encodes, 20,
        "only weigher sampling encodes keys on the typed path"
    );
    typed_sink.0.sort();
    assert_eq!(typed_sink.0, expected);
}

#[test]
fn precombine_rejects_invalid_utf8_key_on_miss() {
    let handler = precombine_handler("seed".to_string());
    let mut instance = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    instance.start_bundle().expect("start_bundle");

    let mut ctx = HandlerContext::new(&mut sink);
    let valid = ("ok".to_string(), 1i64).encode().expect("encode");
    instance.process(&valid, &mut ctx).expect("valid key");

    // Length 2 with invalid UTF-8 bytes followed by varint 5.
    let invalid = [0x02u8, 0xFF, 0xFE, 0x05];
    let err = instance
        .process(&invalid, &mut ctx)
        .expect_err("invalid UTF-8 key must fail on miss");
    assert!(err.contains("UTF-8"), "unexpected error: {err}");
}

/// Records each emitted element together with its windowed header.
#[derive(Default)]
struct WindowedSink(Vec<(WindowedHeader, Vec<u8>)>);

impl ElementSink for WindowedSink {
    fn push(&mut self, element: Vec<u8>) -> std::result::Result<(), String> {
        self.0.push((WindowedHeader::EMPTY.clone(), element));
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

fn encode_interval_window(start_ms: i64, end_ms: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    IntervalWindowCoder
        .encode(
            &IntervalWindow::new(Instant::from_millis(start_ms), Instant::from_millis(end_ms)),
            &mut buf,
            Context::Nested,
        )
        .expect("encode interval window");
    buf
}

#[test]
fn precombine_splits_multi_window_elements_and_tracks_min_timestamp() {
    let handler = precombine_handler("seed".to_string());
    let w1 = encode_interval_window(0, 10);
    let w2 = encode_interval_window(5, 15);
    let multi = WindowedHeader::new(7, &[w1.clone(), w2], PaneInfo::NO_FIRING);
    let single = WindowedHeader::new(3, &[w1], PaneInfo::NO_FIRING);

    for use_value_path in [false, true] {
        let mut instance = handler.instantiate();
        let mut sink = WindowedSink::default();
        instance.start_bundle().expect("start_bundle");
        if use_value_path {
            let mut ctx = HandlerContext::new(&mut sink).with_header(&multi);
            let mut slot = Some(("a".to_string(), 10i64));
            instance
                .process_value(TypedElement::new(&mut slot), &mut ctx)
                .expect("multi-window process_value");
            let mut ctx = HandlerContext::new(&mut sink).with_header(&single);
            let mut slot = Some(("a".to_string(), 5i64));
            instance
                .process_value(TypedElement::new(&mut slot), &mut ctx)
                .expect("single-window process_value");
            instance.finish_bundle(&mut ctx).expect("finish_bundle");
        } else {
            let e1 = ("a".to_string(), 10i64).encode().expect("encode");
            let e2 = ("a".to_string(), 5i64).encode().expect("encode");
            let mut ctx = HandlerContext::new(&mut sink).with_header(&multi);
            instance
                .process(&e1, &mut ctx)
                .expect("multi-window process");
            let mut ctx = HandlerContext::new(&mut sink).with_header(&single);
            instance
                .process(&e2, &mut ctx)
                .expect("single-window process");
            instance.finish_bundle(&mut ctx).expect("finish_bundle");
        }

        let mut results: Vec<(BoundedWindow, i64, String, i64)> = sink
            .0
            .into_iter()
            .map(|(header, bytes)| {
                let (key, sum) = <(String, i64)>::decode(&bytes).expect("decode");
                (
                    header.single_window().expect("single window"),
                    header.timestamp_millis(),
                    key,
                    sum,
                )
            })
            .collect();
        results.sort_by_key(|(_, ts, _, _)| *ts);

        let expected = vec![
            (
                BoundedWindow::Interval(IntervalWindow::new(
                    Instant::from_millis(0),
                    Instant::from_millis(10),
                )),
                3,
                "a".to_string(),
                15,
            ),
            (
                BoundedWindow::Interval(IntervalWindow::new(
                    Instant::from_millis(5),
                    Instant::from_millis(15),
                )),
                7,
                "a".to_string(),
                10,
            ),
        ];
        assert_eq!(results, expected, "use_value_path={use_value_path}");
    }
}

#[test]
fn precombine_combines_mixed_process_and_process_value_in_one_bundle() {
    let handler = precombine_handler("seed".to_string());

    // First `process`, then `process_value`.
    let mut instance = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    instance.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink);
        let raw = ("a".to_string(), 10i64).encode().expect("encode");
        instance.process(&raw, &mut ctx).expect("process");
        let mut slot_a = Some(("a".to_string(), 5i64));
        instance
            .process_value(TypedElement::new(&mut slot_a), &mut ctx)
            .expect("process_value existing key");
        assert!(slot_a.is_none());
        let mut slot_b = Some(("b".to_string(), 7i64));
        instance
            .process_value(TypedElement::new(&mut slot_b), &mut ctx)
            .expect("process_value new key");
        assert!(slot_b.is_none());
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    let mut out: Vec<(String, i64)> = sink
        .iter()
        .map(|b| <(String, i64)>::decode(b).expect("decode"))
        .collect();
    out.sort();
    assert_eq!(out, vec![("a".to_string(), 15), ("b".to_string(), 7)]);

    // First `process_value`, then `process`.
    let mut sink = Vec::<Vec<u8>>::new();
    instance.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink);
        let mut slot_a = Some(("a".to_string(), 10i64));
        instance
            .process_value(TypedElement::new(&mut slot_a), &mut ctx)
            .expect("process_value");
        assert!(slot_a.is_none());
        let raw_a = ("a".to_string(), 5i64).encode().expect("encode");
        let raw_b = ("b".to_string(), 7i64).encode().expect("encode");
        instance
            .process(&raw_a, &mut ctx)
            .expect("process existing key");
        instance.process(&raw_b, &mut ctx).expect("process new key");
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    let mut out: Vec<(String, i64)> = sink
        .iter()
        .map(|b| <(String, i64)>::decode(b).expect("decode"))
        .collect();
    out.sort();
    assert_eq!(out, vec![("a".to_string(), 15), ("b".to_string(), 7)]);
}

#[test]
fn precombine_evicts_and_drains_bytes_table_over_memory_limit() {
    let handler = precombine_handler("seed".to_string());
    let mut instance = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    instance.start_bundle().expect("start_bundle");

    // 17 keys of 4 MiB exceed the 64 MiB bundle limit, so inserting an 18th triggers eviction.
    let pad = "x".repeat(4 << 20);
    let mut expected = BTreeMap::new();
    {
        let mut ctx = HandlerContext::new(&mut sink);
        for i in 0..18i64 {
            let key = format!("k{i:02}-{pad}");
            let value = i + 1;
            let raw = (key.clone(), value).encode().expect("encode");
            instance.process(&raw, &mut ctx).expect("process");
            *expected.entry(key).or_insert(0) += value;
        }
    }
    assert!(
        !sink.is_empty(),
        "exceeding 64 MiB must evict before finish_bundle"
    );

    {
        let mut ctx = HandlerContext::new(&mut sink);
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }

    let totals = sink.iter().fold(BTreeMap::new(), |mut totals, bytes| {
        let (key, value) = <(String, i64)>::decode(bytes).expect("decode");
        *totals.entry(key).or_insert(0) += value;
        totals
    });
    assert_eq!(totals, expected);
}
