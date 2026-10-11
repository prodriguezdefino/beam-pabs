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

//! Decoding untrusted wire input.
//!
//! The Fn API data plane is a trust boundary: a corrupt or truncated frame must fail the
//! element, not abort the harness. These cases claim lengths far larger than the bytes
//! that follow (`2^60`, or a negative VarInt that wraps to `usize::MAX`). A decoder that
//! reserves the claimed length up front aborts the test binary in the allocator; one that
//! reads incrementally reports `UnexpectedEof`.
//!
//! Each hostile case has a well-formed control, here or in the coder tests, so a decoder
//! that rejects all input cannot pass.

use std::collections::HashMap;
use std::io::{Cursor, ErrorKind};
use std::sync::Arc;

use beam::coders::{
    BeamIterable, BoolCoder, BytesCoder, Coder, CoderError, Context, DefaultCoder,
    LengthPrefixCoder, RowCoder, StringUtf8Coder, TimerCoder, URN_GLOBAL_WINDOW, URN_VARINT,
    VarIntCoder,
};
use beam::schema::{FieldType, Schema};
use model::pipeline::{Coder as ProtoCoder, FunctionSpec};

/// A non-negative `i64` VarInt length that exceeds available memory.
const HUGE: i64 = 1 << 60;

fn varint(n: i64) -> Vec<u8> {
    let mut out = Vec::new();
    VarIntCoder::encode_varint(n, &mut out).expect("vec write");
    out
}

/// Returns `prefix`, a VarInt with `claimed` length, and three payload bytes.
fn claims(prefix: &[u8], claimed: i64) -> Vec<u8> {
    let mut out = prefix.to_vec();
    out.extend(varint(claimed));
    out.extend_from_slice(b"abc");
    out
}

fn assert_eof(err: &CoderError) {
    match err {
        CoderError::Io(io) => assert_eq!(io.kind(), ErrorKind::UnexpectedEof, "{io}"),
        other => panic!("expected an UnexpectedEof I/O error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Primitive coders
// ---------------------------------------------------------------------------

#[test]
fn bytes_shorter_than_their_length_fail_with_eof() {
    // A claim of 4 is one byte short. A lazy `read` (not `read_exact`) misses this boundary.
    for claimed in [HUGE, -1, 4] {
        let err = BytesCoder
            .decode(&mut claims(&[], claimed).as_slice(), Context::Nested)
            .expect_err("the claimed length is not present");
        assert_eof(&err);
    }
}

#[test]
fn string_with_a_huge_length_fails_with_eof() {
    let err = StringUtf8Coder
        .decode(&mut claims(&[], HUGE).as_slice(), Context::Nested)
        .expect_err("the claimed length is not present");
    assert_eof(&err);
}

#[test]
fn exact_and_large_lengths_still_decode() {
    // Controls: an exact frame, and one larger than any up-front reservation cap, which
    // the decoder must assemble across several reads.
    assert_eq!(
        BytesCoder
            .decode(&mut [3, b'a', b'b', b'c', 0xFF].as_slice(), Context::Nested)
            .expect("exact frame"),
        b"abc"
    );

    let payload: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let mut framed = varint(payload.len() as i64);
    framed.extend_from_slice(&payload);
    assert_eq!(
        BytesCoder
            .decode(&mut framed.as_slice(), Context::Nested)
            .expect("large frame"),
        payload
    );
}

#[test]
fn length_prefix_with_a_huge_length_fails_with_eof() {
    let coder = LengthPrefixCoder::new(VarIntCoder);
    let err = Coder::<i64>::decode(&coder, &mut claims(&[], HUGE).as_slice(), Context::Nested)
        .expect_err("the claimed frame is not present");
    assert_eof(&err);
}

#[test]
fn an_overlong_varint_is_rejected() {
    // Eleven continuation bytes exceed the ten-byte limit for 64-bit values.
    let mut bytes = vec![0x80u8; 10];
    bytes.push(0x01);
    let err = VarIntCoder::decode_varint(&mut bytes.as_slice()).expect_err("overlong");
    assert_eq!(err.kind(), ErrorKind::InvalidData);
    assert_eq!(err.to_string(), "varint too long or out of range");

    // The tenth byte must only contribute the most significant bit.
    let mut bytes = vec![0xFFu8; 9];
    bytes.push(0x02);
    let err = VarIntCoder::decode_varint(&mut bytes.as_slice()).expect_err("out of range");
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    // Control: -1, the longest legal encoding.
    let mut bytes = vec![0xFFu8; 9];
    bytes.push(0x01);
    assert_eq!(
        VarIntCoder::decode_varint(&mut bytes.as_slice()).expect("-1"),
        -1
    );
}

#[test]
fn invalid_utf8_is_a_utf8_error() {
    let err = StringUtf8Coder
        .decode(&mut [2, 0xC3, 0x28].as_slice(), Context::Nested)
        .expect_err("0xC3 0x28 is not UTF-8");
    assert!(matches!(err, CoderError::Utf8(_)), "{err:?}");
}

#[test]
fn a_bool_byte_other_than_zero_or_one_is_rejected() {
    let err = BoolCoder
        .decode(&mut [2].as_slice(), Context::Nested)
        .expect_err("2 is not a bool");
    assert!(
        matches!(&err, CoderError::Format(msg) if msg == "Invalid boolean byte: 2"),
        "{err:?}"
    );
}

// ---------------------------------------------------------------------------
// Row coder
// ---------------------------------------------------------------------------

fn one_field_schema(field_type: FieldType) -> Arc<Schema> {
    Arc::new(Schema::builder().field("f", field_type).build())
}

#[test]
fn row_fields_with_huge_lengths_fail_with_eof() {
    let decimal = <rust_decimal::Decimal as beam::schema::BeamField>::beam_field_type();
    // (field type, bytes before the 2^60 length: field count, null bitmask, decimal scale)
    let cases: [(FieldType, &[u8]); 4] = [
        (FieldType::int64(), &[1]), // the bitmask itself
        (FieldType::string(), &[1, 0]),
        (FieldType::bytes(), &[1, 0]),
        (decimal, &[1, 0, 2]), // the unscaled value
    ];
    for (field_type, prefix) in cases {
        let schema = one_field_schema(field_type);
        let err = RowCoder::decode_row(&schema, &mut claims(prefix, HUGE).as_slice())
            .expect_err("the claimed bytes are not present");
        assert_eof(&err);
    }
}

#[test]
fn row_decimal_with_a_negative_length_is_a_format_error() {
    let schema =
        one_field_schema(<rust_decimal::Decimal as beam::schema::BeamField>::beam_field_type());
    let err = RowCoder::decode_row(&schema, &mut claims(&[1, 0, 2], -3).as_slice())
        .expect_err("negative length");
    assert!(
        matches!(&err, CoderError::Format(msg) if msg == "Negative decimal length -3"),
        "{err:?}"
    );
}

// ---------------------------------------------------------------------------
// Iterables and timers
// ---------------------------------------------------------------------------

/// Returns a chunked iterable whose continuation token claims `2^60` bytes.
fn iterable_with_huge_token() -> Vec<u8> {
    let mut out = (-1i32).to_be_bytes().to_vec();
    out.extend(varint(-1)); // Continuation marker.
    out.extend(varint(HUGE));
    out.extend_from_slice(b"tok");
    out
}

#[test]
fn iterable_continuation_token_with_a_huge_length_fails_with_eof() {
    let err = Vec::<i64>::decode_element(&mut iterable_with_huge_token().as_slice())
        .expect_err("the token is not present");
    assert_eof(&err);

    let err = BeamIterable::<i64>::decode_element(&mut iterable_with_huge_token().as_slice())
        .expect_err("the token is not present");
    assert_eof(&err);
}

#[test]
fn timer_tag_with_a_huge_length_fails_with_eof() {
    let coders = HashMap::from([
        (
            "key".to_string(),
            ProtoCoder {
                spec: Some(FunctionSpec {
                    urn: URN_VARINT.to_string(),
                    payload: Vec::new(),
                }),
                component_coder_ids: Vec::new(),
            },
        ),
        (
            "window".to_string(),
            ProtoCoder {
                spec: Some(FunctionSpec {
                    urn: URN_GLOBAL_WINDOW.to_string(),
                    payload: Vec::new(),
                }),
                component_coder_ids: Vec::new(),
            },
        ),
    ]);
    // Key 7 followed by a dynamic tag claiming `2^60` bytes.
    let bytes = claims(&[7], HUGE);
    let err = TimerCoder::decode(&mut Cursor::new(bytes.as_slice()), "key", "window", &coders)
        .expect_err("the tag is not present");
    assert_eq!(err.kind(), ErrorKind::UnexpectedEof, "{err}");
}
