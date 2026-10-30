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

//! Random-chunk-split parity tests for [`SkipCursor`] against [`SkipPlan::skip`] and
//! [`RowCoder::skip_row`].

use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;

use beam_core::coders::{
    CausedByDrain, ElementMetadata, PaneInfo, RowCoder, SkipPlan, SkipStep, Timing, URN_BOOL,
    URN_BYTES, URN_DOUBLE, URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW, URN_ITERABLE, URN_KV,
    URN_LENGTH_PREFIX, URN_NULLABLE, URN_ROW, URN_SHARDED_KEY, URN_STATE_BACKED_ITERABLE,
    URN_STRING_UTF8, URN_TIMER, URN_VARINT, URN_WINDOWED_VALUE, VarIntCoder, WindowedHeader,
    encode_timestamp,
};
use beam_core::schema::{
    Field, FieldType, FieldValue, Row, Schema, TypeInfo, URN_DECIMAL, URN_MILLIS_INSTANT,
};
use beam_model::pipeline::{Coder as ProtoCoder, FunctionSpec};
use proptest::prelude::*;

/// Trailing sentinel byte that verifies the cursor stops at the exact value boundary.
const SENTINEL: u8 = 0xAB;

fn coder(urn: &str, components: &[&str], payload: Vec<u8>) -> ProtoCoder {
    ProtoCoder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload,
        }),
        component_coder_ids: components.iter().map(|s| s.to_string()).collect(),
    }
}

fn base_coders() -> HashMap<String, ProtoCoder> {
    HashMap::from([
        ("varint".to_string(), coder(URN_VARINT, &[], Vec::new())),
        ("bytes".to_string(), coder(URN_BYTES, &[], Vec::new())),
        (
            "string".to_string(),
            coder(URN_STRING_UTF8, &[], Vec::new()),
        ),
        ("bool".to_string(), coder(URN_BOOL, &[], Vec::new())),
        ("double".to_string(), coder(URN_DOUBLE, &[], Vec::new())),
        (
            "gwindow".to_string(),
            coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
        (
            "iwindow".to_string(),
            coder(URN_INTERVAL_WINDOW, &[], Vec::new()),
        ),
    ])
}

fn encode_varint(value: i64, out: &mut Vec<u8>) {
    VarIntCoder::encode_varint_to_vec(value, out);
}

fn encode_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) {
    encode_varint(bytes.len() as i64, out);
    out.extend_from_slice(bytes);
}

/// Checks that `SkipCursor` matches `SkipPlan::skip` across random split boundaries, 1-byte
/// chunks, and a non-zero starting offset.
fn assert_cursor_parity(encoded: &[u8], plan: &SkipPlan, nested: bool, raw_splits: &[usize]) {
    let mut with_sentinel = Vec::with_capacity(encoded.len() + 1);
    with_sentinel.extend_from_slice(encoded);
    with_sentinel.push(SENTINEL);

    // Reference: full-buffer SkipPlan::skip.
    let mut ref_cursor = with_sentinel.as_slice();
    plan.skip(&mut ref_cursor, nested)
        .expect("reference SkipPlan::skip must succeed");
    let expected_len = with_sentinel.len() - ref_cursor.len();
    assert_eq!(expected_len, encoded.len());

    // Random sorted split boundaries (with duplicates exercising empty chunks).
    let mut cuts: Vec<usize> = raw_splits
        .iter()
        .map(|s| s % (with_sentinel.len() + 1))
        .collect();
    cuts.sort_unstable();
    cuts.push(with_sentinel.len());

    run_split_schedule(&with_sentinel, 0, expected_len, plan, nested, &cuts);

    // One-byte chunks across every byte boundary, including inside varints.
    let one_byte_cuts: Vec<usize> = (0..=with_sentinel.len()).collect();
    run_split_schedule(
        &with_sentinel,
        0,
        expected_len,
        plan,
        nested,
        &one_byte_cuts,
    );

    // Non-zero start offset matching a WindowedHeader prefix in read.carry.
    let prefix_len = 17;
    let mut prefixed = vec![0x5Au8; prefix_len];
    prefixed.extend_from_slice(&with_sentinel);
    let shifted_cuts: Vec<usize> = cuts.iter().map(|c| c + prefix_len).collect();
    run_split_schedule(
        &prefixed,
        prefix_len,
        prefix_len + expected_len,
        plan,
        nested,
        &shifted_cuts,
    );
}

fn run_split_schedule(
    full: &[u8],
    start: usize,
    expected_end: usize,
    plan: &SkipPlan,
    nested: bool,
    cuts: &[usize],
) {
    let mut cursor = plan.cursor(start, nested);
    let mut completed = false;
    for &cut in cuts {
        let step = cursor
            .advance(&full[..cut])
            .expect("valid partial buffer must not error");
        if cut < expected_end {
            assert_eq!(step, SkipStep::NeedMore, "cut {cut} < {expected_end}");
        } else {
            assert_eq!(
                step,
                SkipStep::Complete(expected_end),
                "cut {cut} >= {expected_end}"
            );
            completed = true;
        }
    }
    assert!(completed, "cursor never completed");
    // Advancing again after completion is idempotent.
    assert_eq!(
        cursor.advance(full).expect("post-complete advance"),
        SkipStep::Complete(expected_end)
    );
}

fn assert_row_parity(encoded: &[u8], schema: &Arc<Schema>, raw_splits: &[usize]) {
    let mut with_sentinel = Vec::with_capacity(encoded.len() + 1);
    with_sentinel.extend_from_slice(encoded);
    with_sentinel.push(SENTINEL);

    let mut row_cursor = with_sentinel.as_slice();
    RowCoder::skip_row(schema, &mut row_cursor).expect("RowCoder::skip_row must succeed");
    assert_eq!(with_sentinel.len() - row_cursor.len(), encoded.len());

    let direct_plan = SkipPlan::for_row(Arc::clone(schema));
    assert_cursor_parity(encoded, &direct_plan, true, raw_splits);

    let mut coders = base_coders();
    coders.insert(
        "row".to_string(),
        coder(URN_ROW, &[], schema.to_proto_bytes()),
    );
    let proto_plan = SkipPlan::new("row", &coders);
    assert_cursor_parity(encoded, &proto_plan, true, raw_splits);
}

fn encode_interval_window(end_millis: i64, duration_millis: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(&encode_timestamp(end_millis));
    encode_varint(duration_millis, out);
}

fn rich_row_schema(custom_positions: bool) -> Arc<Schema> {
    let inner = Arc::new(Schema::new(vec![
        Field::new("ix", FieldType::int32()),
        Field::nullable("ilabel", FieldType::string()),
        Field::new("iflag", FieldType::boolean()),
    ]));
    let mut fields = vec![
        Field::new("b", FieldType::byte()),
        Field::new("s", FieldType::int16()),
        Field::new("i32", FieldType::int32()),
        Field::new("i64", FieldType::int64()),
        Field::new("f32", FieldType::float()),
        Field::new("f64", FieldType::double()),
        Field::new("flag", FieldType::boolean()),
        Field::nullable("text", FieldType::string()),
        Field::nullable("raw", FieldType::bytes()),
        Field::new(
            "arr",
            FieldType::array(FieldType::string().with_nullable(true)),
        ),
        Field::new(
            "map",
            FieldType::map(FieldType::string(), FieldType::int64().with_nullable(true)),
        ),
        Field::nullable("nested", FieldType::row(inner)),
        Field::new(
            "ts",
            FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64()),
        ),
        Field::new(
            "dec",
            FieldType::logical(URN_DECIMAL, Vec::new(), FieldType::bytes()),
        ),
    ];
    if custom_positions {
        let len = fields.len() as i32;
        for (i, field) in fields.iter_mut().enumerate() {
            field.encoding_position = Some(len - 1 - i as i32);
        }
    }
    Arc::new(Schema::new(fields))
}

proptest! {
    #[test]
    fn string_and_bytes_parity_across_arbitrary_splits(
        s in ".{0,260}",
        raw in proptest::collection::vec(any::<u8>(), 0..260),
        splits in proptest::collection::vec(any::<usize>(), 1..32),
    ) {
        let coders = base_coders();
        let mut s_buf = Vec::new();
        encode_len_prefixed(s.as_bytes(), &mut s_buf);
        assert_cursor_parity(&s_buf, &SkipPlan::new("string", &coders), true, &splits);

        let mut b_buf = Vec::new();
        encode_len_prefixed(&raw, &mut b_buf);
        assert_cursor_parity(&b_buf, &SkipPlan::new("bytes", &coders), true, &splits);
    }

    #[test]
    fn kv_parity_across_arbitrary_splits(
        key in any::<i64>(),
        mid in ".{0,180}",
        tail in proptest::collection::vec(any::<u8>(), 0..180),
        splits in proptest::collection::vec(any::<usize>(), 1..32),
    ) {
        let mut coders = base_coders();
        coders.insert("inner_kv".to_string(), coder(URN_KV, &["string", "bytes"], Vec::new()));
        coders.insert("kv".to_string(), coder(URN_KV, &["varint", "inner_kv"], Vec::new()));

        let mut buf = Vec::new();
        encode_varint(key, &mut buf);
        encode_len_prefixed(mid.as_bytes(), &mut buf);
        encode_len_prefixed(&tail, &mut buf);

        assert_cursor_parity(&buf, &SkipPlan::new("kv", &coders), true, &splits);
    }

    #[test]
    fn iterable_fixed_chunked_and_state_backed_parity(
        chunks in proptest::collection::vec(
            proptest::collection::vec(".{0,150}", 1..6),
            0..5,
        ),
        token in proptest::collection::vec(any::<u8>(), 0..40),
        splits in proptest::collection::vec(any::<usize>(), 1..40),
    ) {
        let mut coders = base_coders();
        coders.insert("iter_s".to_string(), coder(URN_ITERABLE, &["string"], Vec::new()));
        coders.insert(
            "sb_iter_s".to_string(),
            coder(URN_STATE_BACKED_ITERABLE, &["string"], Vec::new()),
        );
        coders.insert("kv".to_string(), coder(URN_KV, &["varint", "string"], Vec::new()));
        coders.insert("iter_kv".to_string(), coder(URN_ITERABLE, &["kv"], Vec::new()));
        coders.insert("iter_i64".to_string(), coder(URN_ITERABLE, &["varint"], Vec::new()));
        coders.insert("nested_iter".to_string(), coder(URN_ITERABLE, &["iter_i64"], Vec::new()));

        // Fixed-count iterable of strings.
        let flat: Vec<&String> = chunks.iter().flatten().collect();
        let mut fixed = (flat.len() as i32).to_be_bytes().to_vec();
        for s in &flat {
            encode_len_prefixed(s.as_bytes(), &mut fixed);
        }
        assert_cursor_parity(&fixed, &SkipPlan::new("iter_s", &coders), true, &splits);

        // Chunked (-1) iterable of strings.
        let mut chunked = (-1i32).to_be_bytes().to_vec();
        for chunk in &chunks {
            encode_varint(chunk.len() as i64, &mut chunked);
            for s in chunk {
                encode_len_prefixed(s.as_bytes(), &mut chunked);
            }
        }
        encode_varint(0, &mut chunked);
        assert_cursor_parity(&chunked, &SkipPlan::new("iter_s", &coders), true, &splits);

        // State-backed (-1, inline chunks, -1, token_len, token).
        let mut state_backed = (-1i32).to_be_bytes().to_vec();
        for chunk in &chunks {
            encode_varint(chunk.len() as i64, &mut state_backed);
            for s in chunk {
                encode_len_prefixed(s.as_bytes(), &mut state_backed);
            }
        }
        encode_varint(-1, &mut state_backed);
        encode_len_prefixed(&token, &mut state_backed);
        assert_cursor_parity(
            &state_backed,
            &SkipPlan::new("sb_iter_s", &coders),
            true,
            &splits,
        );

        // Chunked (-1) iterable of composite KV<i64, String>.
        let mut chunked_kv = (-1i32).to_be_bytes().to_vec();
        for (ci, chunk) in chunks.iter().enumerate() {
            encode_varint(chunk.len() as i64, &mut chunked_kv);
            for (ei, s) in chunk.iter().enumerate() {
                encode_varint(-((ci * 100 + ei) as i64), &mut chunked_kv);
                encode_len_prefixed(s.as_bytes(), &mut chunked_kv);
            }
        }
        encode_varint(0, &mut chunked_kv);
        assert_cursor_parity(&chunked_kv, &SkipPlan::new("iter_kv", &coders), true, &splits);

        // Nested Iterable<Iterable<i64>> with mixed fixed and chunked inner iterables.
        let mut nested_buf = (chunks.len() as i32).to_be_bytes().to_vec();
        for (idx, chunk) in chunks.iter().enumerate() {
            if idx % 2 == 0 {
                nested_buf.extend_from_slice(&(chunk.len() as i32).to_be_bytes());
                for s in chunk {
                    encode_varint(s.len() as i64, &mut nested_buf);
                }
            } else {
                nested_buf.extend_from_slice(&(-1i32).to_be_bytes());
                encode_varint(chunk.len() as i64, &mut nested_buf);
                for s in chunk {
                    encode_varint(-(s.len() as i64), &mut nested_buf);
                }
                encode_varint(0, &mut nested_buf);
            }
        }
        assert_cursor_parity(
            &nested_buf,
            &SkipPlan::new("nested_iter", &coders),
            true,
            &splits,
        );
    }

    #[test]
    fn nullable_parity_across_arbitrary_splits(
        outer_tag in prop_oneof![Just(0u8), Just(1u8), Just(42u8)],
        key in any::<i64>(),
        inner_present in any::<bool>(),
        val in ".{0,160}",
        splits in proptest::collection::vec(any::<usize>(), 1..32),
    ) {
        let mut coders = base_coders();
        coders.insert("opt_s".to_string(), coder(URN_NULLABLE, &["string"], Vec::new()));
        coders.insert("kv".to_string(), coder(URN_KV, &["varint", "opt_s"], Vec::new()));
        coders.insert("opt_kv".to_string(), coder(URN_NULLABLE, &["kv"], Vec::new()));

        let mut buf = vec![outer_tag];
        if outer_tag != 0 {
            encode_varint(key, &mut buf);
            if inner_present {
                buf.push(1);
                encode_len_prefixed(val.as_bytes(), &mut buf);
            } else {
                buf.push(0);
            }
        }
        assert_cursor_parity(&buf, &SkipPlan::new("opt_kv", &coders), true, &splits);
    }

    #[test]
    fn nested_row_parity_across_arbitrary_splits(
        b in any::<i8>(),
        s in any::<i16>(),
        i32_val in any::<i32>(),
        i64_val in any::<i64>(),
        flag in any::<bool>(),
        text in proptest::option::of(".{0,150}"),
        raw in proptest::option::of(proptest::collection::vec(any::<u8>(), 0..150)),
        arr in proptest::collection::vec(proptest::option::of(".{0,40}"), 0..6),
        map_entries in proptest::collection::vec(
            (".{0,20}", proptest::option::of(any::<i64>())),
            0..5,
        ),
        nested_row in proptest::option::of((
            any::<i32>(),
            proptest::option::of(".{0,30}"),
            any::<bool>(),
        )),
        scale in -10i64..10,
        dec_bytes in proptest::collection::vec(any::<u8>(), 1..16),
        custom_positions in any::<bool>(),
        splits in proptest::collection::vec(any::<usize>(), 1..40),
    ) {
        let schema = rich_row_schema(custom_positions);
        let inner_schema = match &schema.fields[11].field_type.type_info {
            TypeInfo::Row(inner) => Arc::clone(inner),
            other => panic!("expected inner row schema, got {other:?}"),
        };
        let nested_val = nested_row.map(|(ix, ilabel, iflag)| {
            FieldValue::Row(
                Row::new(
                    inner_schema,
                    vec![
                        Some(FieldValue::Int32(ix)),
                        ilabel.map(FieldValue::String),
                        Some(FieldValue::Boolean(iflag)),
                    ],
                )
                .expect("inner row"),
            )
        });
        let mut dec_payload = Vec::new();
        encode_varint(scale, &mut dec_payload);
        encode_len_prefixed(&dec_bytes, &mut dec_payload);

        let row = Row::new(
            Arc::clone(&schema),
            vec![
                Some(FieldValue::Byte(b)),
                Some(FieldValue::Int16(s)),
                Some(FieldValue::Int32(i32_val)),
                Some(FieldValue::Int64(i64_val)),
                Some(FieldValue::Float(1.5)),
                Some(FieldValue::Double(-2.75)),
                Some(FieldValue::Boolean(flag)),
                text.map(FieldValue::String),
                raw.map(FieldValue::Bytes),
                Some(FieldValue::Array(
                    arr.into_iter().map(|v| v.map(FieldValue::String)).collect(),
                )),
                Some(FieldValue::Map(
                    map_entries
                        .into_iter()
                        .map(|(k, v)| (FieldValue::String(k), v.map(FieldValue::Int64)))
                        .collect(),
                )),
                nested_val,
                Some(FieldValue::Int64(1_700_000_000_000)),
                Some(FieldValue::Bytes(dec_payload)),
            ],
        )
        .expect("row matches schema");

        let mut encoded = Vec::new();
        RowCoder::encode_row(&row, &mut encoded).expect("row encodes");
        assert_row_parity(&encoded, &schema, &splits);
    }

    #[test]
    fn windowed_value_timer_and_sharded_key_parity(
        windows in proptest::collection::vec((any::<i64>(), 0i64..1_000_000), 0..4),
        with_metadata in any::<bool>(),
        key in any::<i64>(),
        payload in ".{0,150}",
        clear in any::<bool>(),
        shard in proptest::collection::vec(any::<u8>(), 0..32),
        splits in proptest::collection::vec(any::<usize>(), 1..32),
    ) {
        let mut coders = base_coders();
        coders.insert("kv".to_string(), coder(URN_KV, &["varint", "string"], Vec::new()));
        coders.insert(
            "wv".to_string(),
            coder(URN_WINDOWED_VALUE, &["kv", "iwindow"], Vec::new()),
        );
        coders.insert("timer".to_string(), coder(URN_TIMER, &["string", "iwindow"], Vec::new()));
        coders.insert("sharded".to_string(), coder(URN_SHARDED_KEY, &["string"], Vec::new()));

        let encoded_windows: Vec<Vec<u8>> = windows
            .iter()
            .map(|&(end, dur)| {
                let mut w = Vec::new();
                encode_interval_window(end, dur, &mut w);
                w
            })
            .collect();
        let pane = PaneInfo::new(false, false, Timing::Late, Some(300), Some(150));
        let metadata = if with_metadata {
            ElementMetadata {
                drain: CausedByDrain::CausedByDrain,
                traceparent: Some(
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
                ),
                ..ElementMetadata::default()
            }
        } else {
            ElementMetadata::default()
        };
        let header = WindowedHeader::with_metadata(123_456, &encoded_windows, pane, &metadata);
        let mut wv = header.as_bytes().to_vec();
        encode_varint(key, &mut wv);
        encode_len_prefixed(payload.as_bytes(), &mut wv);
        assert_cursor_parity(&wv, &SkipPlan::new("wv", &coders), true, &splits);

        // Timer with windows and two-index pane + optional metadata.
        let mut timer = Vec::new();
        encode_len_prefixed(payload.as_bytes(), &mut timer);
        encode_len_prefixed(b"dyn-tag", &mut timer);
        timer.extend_from_slice(&(encoded_windows.len() as i32).to_be_bytes());
        for w in &encoded_windows {
            timer.extend_from_slice(w);
        }
        if clear {
            timer.push(1);
        } else {
            timer.push(0);
            timer.extend_from_slice(&[0u8; 16]);
            pane.encode(with_metadata, &mut timer).expect("pane encodes");
            if with_metadata {
                metadata.encode(&mut timer).expect("metadata encodes");
            }
        }
        assert_cursor_parity(&timer, &SkipPlan::new("timer", &coders), true, &splits);

        // ShardedKey<String>.
        let mut sharded = Vec::new();
        encode_len_prefixed(&shard, &mut sharded);
        encode_len_prefixed(payload.as_bytes(), &mut sharded);
        assert_cursor_parity(&sharded, &SkipPlan::new("sharded", &coders), true, &splits);
    }
}

#[test]
fn row_with_chunked_iterable_field_skips_across_1_byte_splits() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "items",
            FieldType::array(FieldType::string().with_nullable(true)),
        ),
        Field::new("tail", FieldType::int64()),
    ]));
    // Row header: 2 fields, empty null bitmask.
    let mut encoded = vec![0x02, 0x00];
    // Chunked (-1) iterable field with two chunks of nullable strings.
    encoded.extend_from_slice(&(-1i32).to_be_bytes());
    encode_varint(2, &mut encoded);
    encoded.push(1);
    encode_len_prefixed(&[b'x'; 140], &mut encoded);
    encoded.push(0);
    encode_varint(1, &mut encoded);
    encoded.push(1);
    encode_len_prefixed(b"last", &mut encoded);
    encode_varint(0, &mut encoded);
    encode_varint(-999, &mut encoded);

    assert_row_parity(&encoded, &schema, &[1, 3, 7, 15, 64, 150]);
}

#[test]
fn invalid_inputs_match_skip_plan_and_row_coder_errors() {
    let mut coders = base_coders();
    coders.insert(
        "lp".to_string(),
        coder(URN_LENGTH_PREFIX, &["bytes"], Vec::new()),
    );
    coders.insert(
        "iter".to_string(),
        coder(URN_ITERABLE, &["varint"], Vec::new()),
    );
    coders.insert(
        "wv".to_string(),
        coder(URN_WINDOWED_VALUE, &["varint", "gwindow"], Vec::new()),
    );
    coders.insert(
        "cycle".to_string(),
        coder(URN_NULLABLE, &["cycle"], Vec::new()),
    );

    let mut neg_len = Vec::new();
    encode_varint(-1, &mut neg_len);
    let overlong_varint = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
    let bad_iter_count = (-2i32).to_be_bytes().to_vec();
    let mut bad_chunk_hdr = (-1i32).to_be_bytes().to_vec();
    encode_varint(-2, &mut bad_chunk_hdr);
    let mut neg_token = (-1i32).to_be_bytes().to_vec();
    encode_varint(-1, &mut neg_token);
    encode_varint(-1, &mut neg_token);
    let mut neg_windows = vec![0u8; 8];
    neg_windows.extend_from_slice(&(-1i32).to_be_bytes());
    let mut bad_pane = vec![0u8; 8];
    bad_pane.extend_from_slice(&0i32.to_be_bytes());
    bad_pane.push(0x30);
    let deep_cycle = vec![0x01u8; 80];

    let coder_cases: [(&str, &[u8]); 8] = [
        ("lp", &neg_len),
        ("varint", &overlong_varint),
        ("iter", &bad_iter_count),
        ("iter", &bad_chunk_hdr),
        ("iter", &neg_token),
        ("wv", &neg_windows),
        ("wv", &bad_pane),
        ("cycle", &deep_cycle),
    ];
    for (coder_id, bytes) in coder_cases {
        let plan = SkipPlan::new(coder_id, &coders);
        let expected = plan
            .skip(&mut &bytes[..], true)
            .expect_err("reference skip must fail");
        let mut cursor = plan.cursor(0, true);
        let mut actual = None;
        for cut in 1..=bytes.len() {
            match cursor.advance(&bytes[..cut]) {
                Ok(SkipStep::NeedMore) => {}
                Ok(SkipStep::Complete(end)) => {
                    panic!("{coder_id}: unexpectedly completed at {end}");
                }
                Err(err) => {
                    actual = Some(err);
                    break;
                }
            }
        }
        let actual = actual.expect("cursor must report InvalidData");
        assert_eq!(actual.kind(), expected.kind(), "{coder_id}");
        assert_eq!(actual.to_string(), expected.to_string(), "{coder_id}");
    }

    // Row invalid cases: wrong field count, negative map count, bad nullable tag.
    let map_schema = Arc::new(Schema::new(vec![Field::new(
        "m",
        FieldType::map(FieldType::string(), FieldType::int64().with_nullable(true)),
    )]));
    let wrong_fields = [0x02, 0x00];
    let neg_map = [&[0x01, 0x00][..], &(-1i32).to_be_bytes()].concat();
    let bad_null_tag = [&[0x01, 0x00][..], &1i32.to_be_bytes(), &[0x01, b'k', 0x02]].concat();
    for bytes in [&wrong_fields[..], &neg_map[..], &bad_null_tag[..]] {
        let expected = RowCoder::skip_row(&map_schema, &mut &bytes[..])
            .expect_err("RowCoder::skip_row must fail");
        let plan = SkipPlan::for_row(Arc::clone(&map_schema));
        let plan_err = plan
            .skip(&mut &bytes[..], true)
            .expect_err("SkipPlan::skip must fail");
        assert_eq!(plan_err.kind(), expected.kind());
        assert_eq!(plan_err.to_string(), expected.to_string());
        let mut cursor = plan.cursor(0, true);
        let mut actual = None;
        for cut in 1..=bytes.len() {
            match cursor.advance(&bytes[..cut]) {
                Ok(SkipStep::NeedMore) => {}
                Ok(SkipStep::Complete(end)) => panic!("unexpectedly completed at {end}"),
                Err(err) => {
                    actual = Some(err);
                    break;
                }
            }
        }
        let actual = actual.expect("row cursor must report InvalidData");
        assert_eq!(actual.kind(), expected.kind());
        assert_eq!(actual.to_string(), expected.to_string());
    }
}

#[test]
fn hostile_huge_length_waits_without_allocating_and_shrinking_buffer_errors() {
    let coders = base_coders();
    let plan = SkipPlan::new("bytes", &coders);
    let mut hostile = Vec::new();
    encode_varint(1i64 << 60, &mut hostile);
    hostile.extend_from_slice(b"abc");

    let mut cursor = plan.cursor(0, true);
    assert_eq!(
        cursor.advance(&hostile).expect("huge length waits"),
        SkipStep::NeedMore
    );

    // Shrinking the buffer below the cursor's saved offset reports InvalidData.
    let err = cursor
        .advance(&hostile[..2])
        .expect_err("shrunk buffer must error");
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}
