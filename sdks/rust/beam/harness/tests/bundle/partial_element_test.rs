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

//! Resumable partial-element decoding across split `Elements.Data` chunks.

use std::collections::HashMap;
use std::sync::Arc;

use beam::coders::{
    CausedByDrain, ElementMetadata, PaneInfo, RowCoder, Timing, URN_BYTES, URN_GLOBAL_WINDOW,
    URN_INTERVAL_WINDOW, URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_PARAM_WINDOWED_VALUE,
    URN_ROW, URN_STATE_BACKED_ITERABLE, URN_STRING_UTF8, URN_VARINT, URN_WINDOWED_VALUE,
    VarIntCoder, WindowedHeader, encode_timestamp,
};
use beam::internals::TransformFn;
use beam::schema::{Field, FieldType, FieldValue, Row, Schema, TypeInfo};
use beam_model::pipeline as proto_pipeline;

use crate::common;
use common::{DescriptorBuilder, Observer, SINK_ID, STAGE_ID, run_bundle};

fn coder(urn: &str, components: &[&str], payload: Vec<u8>) -> proto_pipeline::Coder {
    proto_pipeline::Coder {
        spec: Some(proto_pipeline::FunctionSpec {
            urn: urn.to_string(),
            payload,
        }),
        component_coder_ids: components.iter().map(|c| c.to_string()).collect(),
    }
}

fn encode_varint(value: i64, out: &mut Vec<u8>) {
    VarIntCoder::encode_varint_to_vec(value, out);
}

fn encode_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) {
    encode_varint(bytes.len() as i64, out);
    out.extend_from_slice(bytes);
}

fn interval_window(end_millis: i64, duration_millis: i64) -> Vec<u8> {
    let mut buf = encode_timestamp(end_millis).to_vec();
    encode_varint(duration_millis, &mut buf);
    buf
}

fn split_bytes(stream: &[u8], chunk_size: usize, interleave_empty: bool) -> Vec<Vec<u8>> {
    let mut chunks = Vec::new();
    for (idx, slice) in stream.chunks(chunk_size).enumerate() {
        chunks.push(slice.to_vec());
        if interleave_empty && idx % 5 == 0 {
            chunks.push(Vec::new());
        }
    }
    chunks
}

async fn decode_via_bundle(
    descriptor_id: &str,
    source_coder_id: &str,
    coders: HashMap<String, proto_pipeline::Coder>,
    chunks: Vec<Vec<u8>>,
) -> Vec<(WindowedHeader, Vec<u8>)> {
    let observer = Observer::default();
    let handlers: HashMap<String, TransformFn> =
        HashMap::from([(STAGE_ID.to_string(), observer.handler())]);
    let descriptor = DescriptorBuilder::new(descriptor_id)
        .with_coders(source_coder_id, coders)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    let run = run_bundle(handlers, descriptor, chunks).await;
    run.bundle_response();
    observer
        .seen()
        .into_iter()
        .map(|o| (o.header, o.element))
        .collect()
}

/// Checks exact header and payload parity between a single-chunk bundle and a bundle that
/// gets the same stream in chunks of each size.
async fn assert_split_parity(
    descriptor_id: &str,
    source_coder_id: &str,
    coders: &HashMap<String, proto_pipeline::Coder>,
    stream: &[u8],
    expected: &[(WindowedHeader, Vec<u8>)],
    sizes: &[usize],
) {
    let single = decode_via_bundle(
        descriptor_id,
        source_coder_id,
        coders.clone(),
        vec![stream.to_vec()],
    )
    .await;
    assert_eq!(single, expected);
    for &size in sizes {
        let chunks = split_bytes(stream, size, true);
        assert_eq!(
            decode_via_bundle(descriptor_id, source_coder_id, coders.clone(), chunks).await,
            expected,
            "{descriptor_id}: push_chunk split at {size}B"
        );
    }
}

fn kv_iterable_coders() -> HashMap<String, proto_pipeline::Coder> {
    HashMap::from([
        ("varint".to_string(), coder(URN_VARINT, &[], Vec::new())),
        (
            "string".to_string(),
            coder(URN_STRING_UTF8, &[], Vec::new()),
        ),
        (
            "iwindow".to_string(),
            coder(URN_INTERVAL_WINDOW, &[], Vec::new()),
        ),
        (
            "iter".to_string(),
            coder(URN_STATE_BACKED_ITERABLE, &["string"], Vec::new()),
        ),
        (
            "kv".to_string(),
            coder(URN_KV, &["varint", "iter"], Vec::new()),
        ),
        (
            "windowed".to_string(),
            coder(URN_WINDOWED_VALUE, &["kv", "iwindow"], Vec::new()),
        ),
    ])
}

fn build_kv_iterable_stream(big_strings: usize) -> (Vec<u8>, Vec<(WindowedHeader, Vec<u8>)>) {
    let h0 = WindowedHeader::new(
        1_000,
        &[interval_window(10_000, 10_000)],
        PaneInfo::ON_TIME_AND_ONLY_FIRING,
    );
    let mut p0 = Vec::new();
    encode_varint(1, &mut p0);
    p0.extend_from_slice(&2i32.to_be_bytes());
    encode_len_prefixed(b"alpha", &mut p0);
    encode_len_prefixed(b"beta", &mut p0);

    // Large chunked (-1) iterable element with two windows, two-index pane, and metadata.
    let meta = ElementMetadata {
        drain: CausedByDrain::CausedByDrain,
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        ..ElementMetadata::default()
    };
    let h1 = WindowedHeader::with_metadata(
        2_000,
        &[
            interval_window(20_000, 10_000),
            interval_window(30_000, 10_000),
        ],
        PaneInfo::new(false, false, Timing::Late, Some(200), Some(130)),
        &meta,
    );
    let mut p1 = Vec::new();
    encode_varint(-42, &mut p1);
    p1.extend_from_slice(&(-1i32).to_be_bytes());
    encode_varint(big_strings as i64, &mut p1);
    let body = [b'a'; 140];
    for _ in 0..big_strings {
        encode_len_prefixed(&body, &mut p1);
    }
    encode_varint(0, &mut p1);

    // Back-to-back state-backed iterable after the split element.
    let h2 = WindowedHeader::new(
        3_000,
        &[interval_window(40_000, 10_000)],
        PaneInfo::NO_FIRING,
    );
    let mut p2 = Vec::new();
    encode_varint(300, &mut p2);
    p2.extend_from_slice(&(-1i32).to_be_bytes());
    encode_varint(1, &mut p2);
    encode_len_prefixed(b"inline-val", &mut p2);
    encode_varint(-1, &mut p2);
    encode_len_prefixed(b"state-token-bytes", &mut p2);

    // Final trailing element.
    let h3 = WindowedHeader::new(
        4_000,
        &[interval_window(50_000, 10_000)],
        PaneInfo::NO_FIRING,
    );
    let mut p3 = Vec::new();
    encode_varint(999, &mut p3);
    p3.extend_from_slice(&0i32.to_be_bytes());

    let expected = vec![(h0, p0), (h1, p1), (h2, p2), (h3, p3)];
    let mut stream = Vec::new();
    for (h, p) in &expected {
        stream.extend_from_slice(h.as_bytes());
        stream.extend_from_slice(p);
    }
    (stream, expected)
}

#[tokio::test]
async fn kv_iterable_split_across_1b_7b_and_64k_chunks_matches_single_chunk() {
    let coders = kv_iterable_coders();

    // Compact multi-element stream covers 1-byte and 7-byte splits across headers, varints,
    // chunked iterables, state tokens, and back-to-back elements.
    let (compact, compact_expected) = build_kv_iterable_stream(2);
    assert_split_parity(
        "desc_kv_iter_compact",
        "windowed",
        &coders,
        &compact,
        &compact_expected,
        &[1, 7, 64 * 1024],
    )
    .await;

    // > 140 KiB element spans three 64 KiB chunks, followed by back-to-back elements.
    let (large, large_expected) = build_kv_iterable_stream(1_100);
    assert!(large.len() > 2 * 64 * 1024);
    assert_split_parity(
        "desc_kv_iter_large",
        "windowed",
        &coders,
        &large,
        &large_expected,
        &[7, 64 * 1024],
    )
    .await;
}

fn row_schema() -> Arc<Schema> {
    let inner = Arc::new(Schema::new(vec![
        Field::new("code", FieldType::int32()),
        Field::nullable("note", FieldType::string()),
    ]));
    Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::nullable("label", FieldType::string()),
        Field::new("tags", FieldType::array(FieldType::string())),
        Field::new(
            "counts",
            FieldType::map(FieldType::string(), FieldType::int64()),
        ),
        Field::nullable("detail", FieldType::row(inner)),
    ]))
}

fn encode_sample_row(
    schema: &Arc<Schema>,
    id: i64,
    tag_count: usize,
    with_detail: bool,
) -> Vec<u8> {
    let inner_schema = match &schema.fields[4].field_type.type_info {
        TypeInfo::Row(inner) => Arc::clone(inner),
        other => panic!("expected row field, got {other:?}"),
    };
    let tags = (0..tag_count)
        .map(|i| {
            Some(FieldValue::String(format!(
                "tag-{i:04}-{}",
                "x".repeat(120)
            )))
        })
        .collect();
    let detail = with_detail.then(|| {
        FieldValue::Row(
            Row::new(
                inner_schema,
                vec![
                    Some(FieldValue::Int32(-7)),
                    Some(FieldValue::String("nested-note".to_string())),
                ],
            )
            .expect("inner row"),
        )
    });
    let row = Row::new(
        Arc::clone(schema),
        vec![
            Some(FieldValue::Int64(id)),
            with_detail.then(|| FieldValue::String("present".to_string())),
            Some(FieldValue::Array(tags)),
            Some(FieldValue::Map(vec![(
                FieldValue::String("k".to_string()),
                Some(FieldValue::Int64(id * 10)),
            )])),
            detail,
        ],
    )
    .expect("row matches schema");
    let mut buf = Vec::new();
    RowCoder::encode_row(&row, &mut buf).expect("row encodes");
    buf
}

#[tokio::test]
async fn row_elements_split_across_1b_7b_and_64k_chunks_match_single_chunk() {
    let schema = row_schema();
    let coders = HashMap::from([
        (
            "row".to_string(),
            coder(URN_ROW, &[], schema.to_proto_bytes()),
        ),
        (
            "gwindow".to_string(),
            coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
        (
            "windowed".to_string(),
            coder(URN_WINDOWED_VALUE, &["row", "gwindow"], Vec::new()),
        ),
    ]);

    let make_stream = |big_tags: usize| {
        let expected = vec![
            (
                WindowedHeader::global(10, PaneInfo::NO_FIRING),
                encode_sample_row(&schema, 1, 1, false),
            ),
            (
                WindowedHeader::global(20, PaneInfo::ON_TIME_AND_ONLY_FIRING),
                encode_sample_row(&schema, 2, big_tags, true),
            ),
            (
                WindowedHeader::global(30, PaneInfo::NO_FIRING),
                encode_sample_row(&schema, 3, 2, false),
            ),
        ];
        let mut stream = Vec::new();
        for (h, p) in &expected {
            stream.extend_from_slice(h.as_bytes());
            stream.extend_from_slice(p);
        }
        (stream, expected)
    };

    let (compact, compact_expected) = make_stream(2);
    assert_split_parity(
        "desc_row_compact",
        "windowed",
        &coders,
        &compact,
        &compact_expected,
        &[1, 7, 64 * 1024],
    )
    .await;

    let (large, large_expected) = make_stream(1_100);
    assert!(large.len() > 2 * 64 * 1024);
    assert_split_parity(
        "desc_row_large",
        "windowed",
        &coders,
        &large,
        &large_expected,
        &[7, 64 * 1024],
    )
    .await;
}

#[tokio::test]
async fn length_prefixed_and_nested_row_stripping_split_across_chunks() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("name", FieldType::string()),
    ]));
    let coders = HashMap::from([
        ("varint".to_string(), coder(URN_VARINT, &[], Vec::new())),
        ("bytes".to_string(), coder(URN_BYTES, &[], Vec::new())),
        (
            "row".to_string(),
            coder(URN_ROW, &[], schema.to_proto_bytes()),
        ),
        (
            "lp_row".to_string(),
            coder(URN_LENGTH_PREFIX, &["row"], Vec::new()),
        ),
        (
            "kv_lp_row".to_string(),
            coder(URN_KV, &["varint", "lp_row"], Vec::new()),
        ),
        (
            "lp_kv_lp_row".to_string(),
            coder(URN_LENGTH_PREFIX, &["kv_lp_row"], Vec::new()),
        ),
        (
            "lp_bytes".to_string(),
            coder(URN_LENGTH_PREFIX, &["bytes"], Vec::new()),
        ),
        (
            "gwindow".to_string(),
            coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
        (
            "wv_lp_bytes".to_string(),
            coder(URN_WINDOWED_VALUE, &["lp_bytes", "gwindow"], Vec::new()),
        ),
        (
            "wv_walked_nested".to_string(),
            coder(URN_WINDOWED_VALUE, &["kv_lp_row", "gwindow"], Vec::new()),
        ),
        (
            "wv_root_lp_nested".to_string(),
            coder(URN_WINDOWED_VALUE, &["lp_kv_lp_row", "gwindow"], Vec::new()),
        ),
    ]);

    // Plain root LengthPrefixed spanning > 2 chunks of 64 KiB, followed by a tail element.
    let h0 = WindowedHeader::global(100, PaneInfo::NO_FIRING);
    let h1 = WindowedHeader::global(200, PaneInfo::ON_TIME_AND_ONLY_FIRING);
    let big_payload = vec![0x3Cu8; 140_000];
    let small_payload = b"after-big".to_vec();
    let mut lp_stream = Vec::new();
    lp_stream.extend_from_slice(h0.as_bytes());
    encode_len_prefixed(&big_payload, &mut lp_stream);
    lp_stream.extend_from_slice(h1.as_bytes());
    encode_len_prefixed(&small_payload, &mut lp_stream);
    let lp_expected = vec![(h0.clone(), big_payload), (h1.clone(), small_payload)];
    assert_split_parity(
        "desc_lp_large",
        "wv_lp_bytes",
        &coders,
        &lp_stream,
        &lp_expected,
        &[7, 64 * 1024],
    )
    .await;

    // Walked and root-LengthPrefixed with nested Row length-prefix stripping across 1B and 7B.
    let encode_row = |id: i64, name: &str| {
        let row = Row::new(
            Arc::clone(&schema),
            vec![
                Some(FieldValue::Int64(id)),
                Some(FieldValue::String(name.to_string())),
            ],
        )
        .expect("row");
        let mut out = Vec::new();
        RowCoder::encode_row(&row, &mut out).expect("encode");
        out
    };
    let r0 = encode_row(1, &"long-name-".repeat(20));
    let r1 = encode_row(2, "short");

    let mut native0 = Vec::new();
    encode_varint(10, &mut native0);
    native0.extend_from_slice(&r0);
    let mut native1 = Vec::new();
    encode_varint(20, &mut native1);
    native1.extend_from_slice(&r1);

    let mut wire0 = Vec::new();
    encode_varint(10, &mut wire0);
    encode_len_prefixed(&r0, &mut wire0);
    let mut wire1 = Vec::new();
    encode_varint(20, &mut wire1);
    encode_len_prefixed(&r1, &mut wire1);

    let expected_stripped = vec![(h0.clone(), native0), (h1.clone(), native1)];

    let mut walked_stream = Vec::new();
    walked_stream.extend_from_slice(h0.as_bytes());
    walked_stream.extend_from_slice(&wire0);
    walked_stream.extend_from_slice(h1.as_bytes());
    walked_stream.extend_from_slice(&wire1);
    assert_split_parity(
        "desc_walked_nested_strip",
        "wv_walked_nested",
        &coders,
        &walked_stream,
        &expected_stripped,
        &[1, 7, 64 * 1024],
    )
    .await;

    let mut root_lp_stream = Vec::new();
    root_lp_stream.extend_from_slice(h0.as_bytes());
    encode_len_prefixed(&wire0, &mut root_lp_stream);
    root_lp_stream.extend_from_slice(h1.as_bytes());
    encode_len_prefixed(&wire1, &mut root_lp_stream);
    assert_split_parity(
        "desc_root_lp_nested_strip",
        "wv_root_lp_nested",
        &coders,
        &root_lp_stream,
        &expected_stripped,
        &[1, 7, 64 * 1024],
    )
    .await;
}

#[tokio::test]
async fn param_windowed_value_split_across_chunks() {
    let constant_header = WindowedHeader::global(7_777, PaneInfo::ON_TIME_AND_ONLY_FIRING);
    let mut param_payload = constant_header.as_bytes().to_vec();
    param_payload.push(0x00); // empty placeholder bytes
    let coders = HashMap::from([
        ("varint".to_string(), coder(URN_VARINT, &[], Vec::new())),
        (
            "string".to_string(),
            coder(URN_STRING_UTF8, &[], Vec::new()),
        ),
        (
            "iter".to_string(),
            coder(URN_ITERABLE, &["string"], Vec::new()),
        ),
        (
            "kv".to_string(),
            coder(URN_KV, &["varint", "iter"], Vec::new()),
        ),
        (
            "gwindow".to_string(),
            coder(URN_GLOBAL_WINDOW, &[], Vec::new()),
        ),
        (
            "param_wv".to_string(),
            coder(URN_PARAM_WINDOWED_VALUE, &["kv", "gwindow"], param_payload),
        ),
    ]);

    let mut p0 = Vec::new();
    encode_varint(5, &mut p0);
    p0.extend_from_slice(&2i32.to_be_bytes());
    encode_len_prefixed(&[b'p'; 150], &mut p0);
    encode_len_prefixed(b"tail", &mut p0);

    let mut p1 = Vec::new();
    encode_varint(6, &mut p1);
    p1.extend_from_slice(&0i32.to_be_bytes());

    let stream = [p0.as_slice(), p1.as_slice()].concat();
    let expected = vec![(constant_header.clone(), p0), (constant_header, p1)];
    assert_split_parity(
        "desc_param_wv_split",
        "param_wv",
        &coders,
        &stream,
        &expected,
        &[1, 7, 64 * 1024],
    )
    .await;
}

#[tokio::test]
async fn truncated_partial_element_at_eof_fails_bundle_and_framing() {
    let coders = kv_iterable_coders();
    let (full, _) = build_kv_iterable_stream(2);
    let truncated = &full[..full.len() - 3];
    let chunks = split_bytes(truncated, 7, false);

    let observer = Observer::default();
    let handlers: HashMap<String, TransformFn> =
        HashMap::from([(STAGE_ID.to_string(), observer.handler())]);
    let descriptor = DescriptorBuilder::new("desc_truncated_partial")
        .with_coders("windowed", coders)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    let run = run_bundle(handlers, descriptor, chunks).await;
    let error = &run.response.error;
    assert!(
        error.contains("trailing unconsumed bytes"),
        "expected trailing bytes error, got: {error}"
    );
}
