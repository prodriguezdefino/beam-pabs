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

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures unwrap; a failure is a test failure"
)]

//! Tests for the two non-inline framings of `beam:coder:iterable:v1`.
//!
//! The runner chooses the framing at encode time. So a pipeline that works on small data
//! can get a different encoding only because one key has more values. These tests check
//! both framings:
//!
//! * the chunked form (`-1` count, VarInt chunk headers, `0` terminator), and
//! * the state-backed continuation (`beam:coder:state_backed_iterable:v1`). Its tail is in
//!   runner state.
//!
//! The tests cover the decode path ([`IterableCoder`]) and the harness skip path
//! ([`skip_coder_value`]), because each one parses this framing independently.

use std::collections::HashMap;
use std::io::Cursor;

use beam::coders::{
    Coder, Context, IterableCoder, URN_ITERABLE, URN_VARINT, VarIntCoder, skip_coder_value,
};
use model::pipeline as proto;

/// Builds the inline chunked encoding: `-1`, then `(chunk header, elements)` pairs, then a
/// `0` header.
fn chunked_iterable(chunks: &[&[i64]]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend((-1i32).to_be_bytes());
    for chunk in chunks {
        VarIntCoder::encode_varint(chunk.len() as i64, &mut buf).unwrap();
        for value in *chunk {
            VarIntCoder::encode_varint(*value, &mut buf).unwrap();
        }
    }
    VarIntCoder::encode_varint(0, &mut buf).unwrap();
    buf
}

/// Builds the state-backed encoding: inline chunks, then a negative header, then a
/// length-prefixed state token that addresses the remaining elements.
fn state_backed_iterable(inline: &[i64], token: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend((-1i32).to_be_bytes());
    if !inline.is_empty() {
        VarIntCoder::encode_varint(inline.len() as i64, &mut buf).unwrap();
        for value in inline {
            VarIntCoder::encode_varint(*value, &mut buf).unwrap();
        }
    }
    // A negative header: the tail is in state, at the token that follows.
    VarIntCoder::encode_varint(-1, &mut buf).unwrap();
    VarIntCoder::encode_varint(token.len() as i64, &mut buf).unwrap();
    buf.extend(token);
    buf
}

/// Returns a coder table for an iterable of varints, in the form that the harness skip path
/// expects.
fn varint_iterable_coders() -> HashMap<String, proto::Coder> {
    HashMap::from([
        (
            "iter".to_string(),
            proto::Coder {
                spec: Some(proto::FunctionSpec {
                    urn: URN_ITERABLE.to_string(),
                    payload: Vec::new(),
                }),
                component_coder_ids: vec!["elem".to_string()],
            },
        ),
        (
            "elem".to_string(),
            proto::Coder {
                spec: Some(proto::FunctionSpec {
                    urn: URN_VARINT.to_string(),
                    payload: Vec::new(),
                }),
                component_coder_ids: Vec::new(),
            },
        ),
    ])
}

// ---------------------------------------------------------------------------
// Decode path
// ---------------------------------------------------------------------------

#[test]
fn chunked_iterables_decode_to_their_elements() {
    let coder: IterableCoder<i64, VarIntCoder> = IterableCoder::new(VarIntCoder);
    // Chunk boundaries are a transport detail. The decoded value must not show them.
    let cases: [(&[&[i64]], Vec<i64>); 2] =
        [(&[&[1, 2, 3], &[4, 5]], vec![1, 2, 3, 4, 5]), (&[], vec![])];
    for (chunks, expected) in cases {
        let decoded: Vec<i64> = coder
            .decode(&mut &chunked_iterable(chunks)[..], Context::WholeStream)
            .expect("chunked iterable should decode");
        assert_eq!(decoded, expected);
    }
}

/// Decoding a state-backed iterable into a `Vec` must fail with an error.
///
/// The decoder must not read the token bytes as the next chunk header. That would return a
/// truncated iterable silently, or fail on garbage, and hide that the tail is unreachable.
#[test]
fn test_decode_state_backed_iterable_is_rejected() {
    let encoded = state_backed_iterable(&[7, 8], b"state-token");
    let coder: IterableCoder<i64, VarIntCoder> = IterableCoder::new(VarIntCoder);

    let res: Result<Vec<i64>, _> = coder.decode(&mut &encoded[..], Context::WholeStream);
    let err = res.expect_err("state-backed iterable must not decode as a truncated iterable");

    let msg = err.to_string();
    assert!(
        msg.contains("state-backed"),
        "error should name the condition, got: {msg}"
    );
    assert!(
        msg.contains("beam:coder:state_backed_iterable:v1"),
        "error should name the URN so it is searchable, got: {msg}"
    );
    assert!(
        msg.contains(": 2 element(s) were inlined"),
        "error should report how many elements were inlined, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Harness skip path
// ---------------------------------------------------------------------------

#[test]
fn test_skip_chunked_iterable_consumes_exactly_the_iterable() {
    let coders = varint_iterable_coders();

    // A trailing sentinel shows that the cursor stops at the end of the iterable. A caller
    // that decodes a following field depends on this.
    let mut encoded = chunked_iterable(&[&[1, 2], &[3]]);
    let sentinel = 0xABu8;
    encoded.push(sentinel);

    let mut cursor = Cursor::new(&encoded[..]);
    skip_coder_value(&mut cursor, "iter", &coders, true).expect("chunked iterable should skip");

    let consumed = cursor.position() as usize;
    assert_eq!(
        consumed,
        encoded.len() - 1,
        "skip must land exactly on the trailing sentinel"
    );
    assert_eq!(encoded[consumed], sentinel);
}

/// A malformed count is an error. The decoder must not treat it as the chunked form.
#[test]
fn test_skip_rejects_invalid_negative_count() {
    let coders = varint_iterable_coders();
    let encoded = (-2i32).to_be_bytes().to_vec();

    let mut cursor = Cursor::new(&encoded[..]);
    let err = skip_coder_value(&mut cursor, "iter", &coders, true)
        .expect_err("only -1 introduces the chunked encoding");

    assert!(
        err.to_string().contains("Invalid iterable count"),
        "unexpected error: {err}"
    );
}

struct MockStateReader {
    pages: Vec<Vec<u8>>,
}

impl beam::coders::StateStreamReader for MockStateReader {
    fn stream_runner_pages(
        &self,
        token: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<Vec<u8>, String>> + Send>, String> {
        assert_eq!(token, b"state-token");
        let pages = self.pages.clone();
        Ok(Box::new(pages.into_iter().map(Ok)))
    }
}

#[test]
fn test_decode_state_backed_iterable_streaming() {
    use beam::coders::{BeamIterable, DefaultCoder};
    use std::sync::Arc;

    let encoded = state_backed_iterable(&[10, 20], b"state-token");

    // Mock reader provides pages with values `[30, 40]` and `[50]`.
    let mut page1 = Vec::new();
    VarIntCoder::encode_varint(30, &mut page1).unwrap();
    VarIntCoder::encode_varint(40, &mut page1).unwrap();

    let mut page2 = Vec::new();
    VarIntCoder::encode_varint(50, &mut page2).unwrap();

    let reader: Arc<dyn beam::coders::StateStreamReader> = Arc::new(MockStateReader {
        pages: vec![page1, page2],
    });

    let mut cursor = &encoded[..];
    let decoded: Vec<i64> =
        <Vec<i64>>::decode_element_with_context(&mut cursor, None, Some(&reader))
            .expect("should decode state-backed iterable with stream reader");

    assert_eq!(decoded, vec![10, 20, 30, 40, 50]);

    let mut cursor2 = &encoded[..];
    let beam_iter: BeamIterable<i64> =
        BeamIterable::<i64>::decode_element_with_context(&mut cursor2, None, Some(&reader))
            .expect("should decode BeamIterable with stream reader");

    assert_eq!(beam_iter.inlined_prefix(), &[10, 20]);
    assert!(!beam_iter.is_in_memory());

    let collected: Vec<i64> = beam_iter.into_iter().collect();
    assert_eq!(collected, vec![10, 20, 30, 40, 50]);
}

// ---------------------------------------------------------------------------
// State-backed failures
// ---------------------------------------------------------------------------

/// Test state reader that serves a scripted stream page by page, including failures.
struct ScriptedStateReader {
    open: Result<Vec<Result<Vec<u8>, String>>, String>,
}

impl beam::coders::StateStreamReader for ScriptedStateReader {
    fn stream_runner_pages(&self, token: &[u8]) -> Result<beam::coders::PageStream, String> {
        assert_eq!(token, b"state-token");
        self.open
            .clone()
            .map(|pages| Box::new(pages.into_iter()) as beam::coders::PageStream)
    }
}

fn scripted(
    open: Result<Vec<Result<Vec<u8>, String>>, String>,
) -> std::sync::Arc<dyn beam::coders::StateStreamReader> {
    std::sync::Arc::new(ScriptedStateReader { open })
}

fn varints(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        VarIntCoder::encode_varint(*v, &mut out).unwrap();
    }
    out
}

fn format_message<T: std::fmt::Debug>(result: Result<T, beam::coders::CoderError>) -> String {
    match result {
        Err(beam::coders::CoderError::Format(msg)) => msg,
        other => panic!("expected a format error, got {other:?}"),
    }
}

#[test]
fn a_state_stream_that_fails_to_open_is_reported() {
    use beam::coders::{BeamIterable, DefaultCoder};

    let encoded = state_backed_iterable(&[10], b"state-token");
    let reader = scripted(Err("permission denied".to_string()));

    assert_eq!(
        format_message(Vec::<i64>::decode_element_with_context(
            &mut &encoded[..],
            None,
            Some(&reader)
        )),
        "Failed to open runner state stream: permission denied"
    );
    assert_eq!(
        format_message(BeamIterable::<i64>::decode_element_with_context(
            &mut &encoded[..],
            None,
            Some(&reader)
        )),
        "Failed to open runner state stream: permission denied"
    );
}

#[test]
fn a_page_error_fails_the_eager_decode() {
    use beam::coders::DefaultCoder;

    let encoded = state_backed_iterable(&[10], b"state-token");
    let reader = scripted(Ok(vec![
        Ok(varints(&[20])),
        Err("stream reset".to_string()),
    ]));

    assert_eq!(
        format_message(Vec::<i64>::decode_element_with_context(
            &mut &encoded[..],
            None,
            Some(&reader)
        )),
        "Runner state stream error: stream reset"
    );
}

#[test]
fn a_page_error_surfaces_from_the_lazy_iterator_after_the_good_elements() {
    use beam::coders::{BeamIterable, DefaultCoder};

    let encoded = state_backed_iterable(&[10], b"state-token");
    let reader = scripted(Ok(vec![
        Ok(varints(&[20, 30])),
        Err("stream reset".to_string()),
    ]));
    let iterable =
        BeamIterable::<i64>::decode_element_with_context(&mut &encoded[..], None, Some(&reader))
            .unwrap();

    let mut it = iterable.try_into_iter();
    assert_eq!(it.next().unwrap().unwrap(), 10);
    assert_eq!(it.next().unwrap().unwrap(), 20);
    assert_eq!(it.next().unwrap().unwrap(), 30);
    assert_eq!(
        format_message(it.next().unwrap()),
        "Runner state stream read error: stream reset"
    );

    // `into_vec` reports the same error, not a truncated vector. A clone shares the suffix
    // that is already drained, so decode the bytes again.
    let fresh =
        BeamIterable::<i64>::decode_element_with_context(&mut &encoded[..], None, Some(&reader))
            .unwrap();
    assert_eq!(
        format_message(fresh.into_vec()),
        "Runner state stream read error: stream reset"
    );
}

#[test]
#[should_panic(expected = "Failed to stream state-backed iterable element: \
                           Decoding format error: Runner state stream read error: stream reset")]
fn the_infallible_iterator_panics_on_a_page_error() {
    use beam::coders::{BeamIterable, DefaultCoder};

    let encoded = state_backed_iterable(&[], b"state-token");
    let reader = scripted(Ok(vec![Err("stream reset".to_string())]));
    let iterable =
        BeamIterable::<i64>::decode_element_with_context(&mut &encoded[..], None, Some(&reader))
            .unwrap();

    // Infallible iteration panics on stream errors; `try_into_iter` returns results.
    let _: Vec<i64> = iterable.into_iter().collect();
}

#[test]
fn a_state_backed_iterable_refuses_to_encode() {
    use beam::coders::{BeamIterable, DefaultCoder};

    let encoded = state_backed_iterable(&[10], b"state-token");
    let reader = scripted(Ok(vec![Ok(varints(&[20]))]));
    let iterable =
        BeamIterable::<i64>::decode_element_with_context(&mut &encoded[..], None, Some(&reader))
            .unwrap();

    // Encoding only the prefix would write a count of 1 and drop element 20 silently.
    assert_eq!(
        format_message(iterable.encode()),
        "cannot encode a state-backed BeamIterable: drain it with `into_vec` first"
    );
}

#[test]
fn an_element_split_across_state_pages_decodes() {
    use beam::coders::{BeamIterable, DefaultCoder};

    // 300 is the two-byte VarInt [0xAC, 0x02]. The page boundary is between the two bytes.
    let encoded = state_backed_iterable(&[], b"state-token");
    let pages = vec![Ok(vec![0x01, 0xAC]), Ok(vec![0x02, 0x03])];

    let reader = scripted(Ok(pages.clone()));
    let eager = Vec::<i64>::decode_element_with_context(&mut &encoded[..], None, Some(&reader));
    assert_eq!(eager.unwrap(), vec![1, 300, 3]);

    let reader = scripted(Ok(pages));
    let lazy =
        BeamIterable::<i64>::decode_element_with_context(&mut &encoded[..], None, Some(&reader))
            .unwrap();
    assert_eq!(lazy.into_vec().unwrap(), vec![1, 300, 3]);
}

/// Test reader that serves one page holding `20` for an empty continuation token.
struct EmptyTokenReader;

impl beam::coders::StateStreamReader for EmptyTokenReader {
    fn stream_runner_pages(&self, token: &[u8]) -> Result<beam::coders::PageStream, String> {
        assert!(token.is_empty(), "unexpected token {token:?}");
        Ok(Box::new(std::iter::once(Ok(varints(&[20])))))
    }
}

#[test]
fn an_empty_continuation_token_is_valid_and_a_negative_length_is_not() {
    use beam::coders::{BeamIterable, DefaultCoder};

    let reader: std::sync::Arc<dyn beam::coders::StateStreamReader> =
        std::sync::Arc::new(EmptyTokenReader);
    let encoded = state_backed_iterable(&[10], b"");
    assert_eq!(
        Vec::<i64>::decode_element_with_context(&mut &encoded[..], None, Some(&reader)).unwrap(),
        vec![10, 20]
    );
    assert_eq!(
        BeamIterable::<i64>::decode_element_with_context(&mut &encoded[..], None, Some(&reader))
            .unwrap()
            .into_vec()
            .unwrap(),
        vec![10, 20]
    );

    let mut negative = (-1i32).to_be_bytes().to_vec();
    VarIntCoder::encode_varint(-1, &mut negative).unwrap(); // Continuation marker.
    VarIntCoder::encode_varint(-1, &mut negative).unwrap(); // Negative token length.
    assert_eq!(
        format_message(Vec::<i64>::decode_element_with_context(
            &mut &negative[..],
            None,
            Some(&reader)
        )),
        "Invalid continuation token length: -1"
    );
}

#[test]
fn beam_iterables_are_equal_only_with_equal_prefixes_and_the_same_suffix() {
    use beam::coders::{BeamIterable, DefaultCoder};

    let in_memory: BeamIterable<i64> = vec![1, 2].into();
    assert_eq!(in_memory.inlined_prefix(), &[1, 2]);
    assert_eq!(in_memory, BeamIterable::from_vec(vec![1, 2]));
    assert_ne!(in_memory, BeamIterable::from_vec(vec![1, 3]));

    // Suffixes are single-pass streams, so only a shared suffix compares equal.
    let encoded = state_backed_iterable(&[1, 2], b"state-token");
    let reader = scripted(Ok(vec![Ok(varints(&[3]))]));
    let decode = || {
        BeamIterable::<i64>::decode_element_with_context(&mut &encoded[..], None, Some(&reader))
            .unwrap()
    };
    let state_backed = decode();
    assert_eq!(state_backed, state_backed.clone());
    assert_ne!(state_backed, decode());
    assert_ne!(state_backed, in_memory);
}
