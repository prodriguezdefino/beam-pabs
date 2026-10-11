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

//! Hand-written golden and edge-case tests for coders, beyond `standard_coders.yaml`.
//!
//! Covers URNs, length-prefix framing, unit, bool, KV, nullable, iterables and logical types.

use beam::coders::{
    BoolCoder, BytesCoder, Coder, Context, DoubleCoder, IntervalWindow, IntervalWindowCoder,
    IterableCoder, LengthPrefixCoder, ParamWindowedValueCoder, RowCoder, StringUtf8Coder,
    VarIntCoder, WindowedValue, WindowedValueCoder,
};
use beam::schema::Row;

/// A length prefix encodes the inner value in the outer context, so a string has one length,
/// not two.
#[test]
fn length_prefixed_default_coder_matches_the_cross_sdk_bytes_for_strings() {
    use beam::coders::{DefaultCoder, LengthPrefixed};

    let mut golden = vec![11u8];
    golden.extend_from_slice(b"apache beam");

    let value = LengthPrefixed::new("apache beam".to_string());
    assert_eq!(value.encode().unwrap(), golden);
    assert_eq!(
        LengthPrefixed::<String>::decode(&golden)
            .unwrap()
            .into_inner(),
        "apache beam"
    );

    // The type's coder object frames identically to its element methods.
    let coder = LengthPrefixed::<String>::coder();
    let mut buf = Vec::new();
    coder.encode(&value, &mut buf, Context::Nested).unwrap();
    assert_eq!(buf, golden);
    assert_eq!(
        coder.decode(&mut &golden[..], Context::Nested).unwrap(),
        value
    );
}

/// URNs are spelled out, not taken from SDK constants, so a typo in a constant is caught.
#[test]
fn every_coder_reports_its_standard_urn() {
    use beam::coders::{
        GlobalWindow, GlobalWindowCoder, LengthPrefixed, LengthPrefixedCoder, RowStructCoder,
        UnitCoder,
    };
    use beam::schema::BeamRow;

    #[derive(BeamRow)]
    #[beam(crate = "::beam")]
    struct Probe {
        id: i64,
    }

    let cases: [(&str, &str); 17] = [
        (
            Coder::<String>::urn(&StringUtf8Coder),
            "beam:coder:string_utf8:v1",
        ),
        (Coder::<Vec<u8>>::urn(&BytesCoder), "beam:coder:bytes:v1"),
        (Coder::<bool>::urn(&BoolCoder), "beam:coder:bool:v1"),
        (Coder::<f64>::urn(&DoubleCoder), "beam:coder:double:v1"),
        (Coder::<i64>::urn(&VarIntCoder), "beam:coder:varint:v1"),
        (Coder::<i32>::urn(&VarIntCoder), "beam:coder:varint:v1"),
        // `()` has no standard coder of its own; it is written as empty bytes.
        (Coder::<()>::urn(&UnitCoder), "beam:coder:bytes:v1"),
        (
            Coder::<i64>::urn(&LengthPrefixCoder::new(VarIntCoder)),
            "beam:coder:length_prefix:v1",
        ),
        (
            Coder::<LengthPrefixed<i64>>::urn(&LengthPrefixedCoder::new(VarIntCoder)),
            "beam:coder:length_prefix:v1",
        ),
        (
            Coder::<Vec<i64>>::urn(&IterableCoder::new(VarIntCoder)),
            "beam:coder:iterable:v1",
        ),
        (Coder::<Row>::urn(&RowCoder::default()), "beam:coder:row:v1"),
        (
            Coder::<Probe>::urn(&RowStructCoder::<Probe>::new()),
            "beam:coder:row:v1",
        ),
        (
            Coder::<IntervalWindow>::urn(&IntervalWindowCoder),
            "beam:coder:interval_window:v1",
        ),
        (
            Coder::<GlobalWindow>::urn(&GlobalWindowCoder),
            "beam:coder:global_window:v1",
        ),
        (
            Coder::<WindowedValue<i64>>::urn(&WindowedValueCoder::new(VarIntCoder)),
            "beam:coder:windowed_value:v1",
        ),
        (
            Coder::<WindowedValue<i64>>::urn(&WindowedValueCoder::with_window_coder(
                VarIntCoder,
                GlobalWindowCoder,
            )),
            "beam:coder:windowed_value:v1",
        ),
        (
            Coder::<WindowedValue<i64>>::urn(&ParamWindowedValueCoder::new(
                VarIntCoder,
                WindowedValue::global((), 0),
            )),
            "beam:coder:param_windowed_value:v1",
        ),
    ];
    for (actual, expected) in cases {
        assert_eq!(actual, expected);
    }
}

#[test]
fn a_bool_element_is_one_byte() {
    use beam::coders::DefaultCoder;

    for (value, byte) in [(false, 0x00), (true, 0x01)] {
        assert_eq!(DefaultCoder::encode(&value).unwrap(), [byte]);
        assert_eq!(<bool as DefaultCoder>::decode(&[byte]).unwrap(), value);
    }
}

/// A nested `()` still takes a zero length byte, which the decoder must not skip.
#[test]
fn a_unit_element_consumes_its_empty_length_byte() {
    use beam::coders::DefaultCoder;

    let pair = ((), 5i64);
    let bytes = pair.encode().unwrap();
    assert_eq!(bytes, [0x00, 0x05]);
    assert_eq!(<((), i64)>::decode(&bytes).unwrap(), pair);
}

/// For an inner coder whose encoding is the same in either context the framing itself
/// is pinned: VarInt(len) followed by exactly the inner bytes.
#[test]
fn length_prefix_frames_a_context_free_value() {
    use beam::coders::{DefaultCoder, LengthPrefixed};

    // 300 is the two-byte VarInt [0xAC, 0x02].
    let golden = [0x02, 0xAC, 0x02];

    let coder = LengthPrefixCoder::new(VarIntCoder);
    let mut buf = Vec::new();
    Coder::<i64>::encode(&coder, &300, &mut buf, Context::Nested).unwrap();
    assert_eq!(buf, golden);
    assert_eq!(
        Coder::<i64>::decode(&coder, &mut &golden[..], Context::Nested).unwrap(),
        300
    );

    assert_eq!(LengthPrefixed::new(300i64).encode().unwrap(), golden);
    let decoded = LengthPrefixed::<i64>::decode(&golden).unwrap();
    assert_eq!(*decoded, 300);
    assert_eq!(decoded.into_inner(), 300);
}

/// The frame bounds the inner decode: bytes the inner coder does not consume are not
/// taken from the following element, and a frame shorter than its prefix is an error.
#[test]
fn length_prefix_decode_is_bounded_by_the_frame() {
    use beam::coders::CoderError;

    let coder = LengthPrefixCoder::new(VarIntCoder);
    // Frame of 3 holding varint 5 plus two bytes the varint does not need, then 0x09.
    let mut input: &[u8] = &[0x03, 0x05, 0xEE, 0xEE, 0x09];
    assert_eq!(
        Coder::<i64>::decode(&coder, &mut input, Context::Nested).unwrap(),
        5
    );
    assert_eq!(input, &[0x09], "decode must consume exactly the frame");

    let err = Coder::<i64>::decode(&coder, &mut &[0x04, 0x05][..], Context::Nested)
        .expect_err("the frame is truncated");
    assert!(
        matches!(&err, CoderError::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof),
        "{err:?}"
    );
}

#[test]
fn test_composite_kv_and_nullable_coders() {
    use beam::coders::{Coder, Context, DefaultCoder, KvCoder, NullableCoder, VarIntCoder};

    let kv_coder = KvCoder::new(VarIntCoder, VarIntCoder);
    assert_eq!(kv_coder.urn(), "beam:coder:kv:v1");

    let pair = (42i64, 100i64);
    let mut buf = Vec::new();
    kv_coder
        .encode(&pair, &mut buf, Context::WholeStream)
        .unwrap();
    let decoded = kv_coder
        .decode(&mut buf.as_slice(), Context::WholeStream)
        .unwrap();
    assert_eq!(pair, decoded);

    // DefaultCoder for (K, V).
    let tuple_pair = (10i32, "test_tuple".to_string());
    let mut tuple_buf = Vec::new();
    tuple_pair.encode_element(&mut tuple_buf).unwrap();
    let decoded_tuple = <(i32, String)>::decode_element(&mut tuple_buf.as_slice()).unwrap();
    assert_eq!(tuple_pair, decoded_tuple);

    let null_coder = NullableCoder::new(VarIntCoder);
    assert_eq!(null_coder.urn(), "beam:coder:nullable:v1");

    let mut none_buf = Vec::new();
    null_coder
        .encode(&None, &mut none_buf, Context::WholeStream)
        .unwrap();
    assert_eq!(none_buf, vec![0x00]);
    let decoded_none = null_coder
        .decode(&mut none_buf.as_slice(), Context::WholeStream)
        .unwrap();
    assert_eq!(decoded_none, None);

    let mut some_buf = Vec::new();
    null_coder
        .encode(&Some(999i64), &mut some_buf, Context::WholeStream)
        .unwrap();
    assert_eq!(some_buf[0], 0x01);
    let decoded_some = null_coder
        .decode(&mut some_buf.as_slice(), Context::WholeStream)
        .unwrap();
    assert_eq!(decoded_some, Some(999i64));

    // Error on invalid nullable tag
    let invalid_tag = vec![0x02, 0x00];
    let err = null_coder
        .decode(&mut invalid_tag.as_slice(), Context::WholeStream)
        .unwrap_err();
    assert!(format!("{err}").contains("Invalid nullable tag"));

    // Option<T> DefaultCoder decode methods
    let mut some_elem_buf = Vec::new();
    Some(123i64).encode_element(&mut some_elem_buf).unwrap();
    let d1 = <Option<i64>>::decode_element(&mut some_elem_buf.as_slice()).unwrap();
    assert_eq!(d1, Some(123i64));

    let d2 =
        <Option<i64>>::decode_element_with_schema(&mut some_elem_buf.as_slice(), None).unwrap();
    assert_eq!(d2, Some(123i64));

    let d3 = <Option<i64>>::decode_element_with_context(&mut some_elem_buf.as_slice(), None, None)
        .unwrap();
    assert_eq!(d3, Some(123i64));

    // Invalid tag on decode_element variants
    let is_invalid_tag = |r: Result<Option<i64>, beam::coders::CoderError>| matches!(r, Err(beam::coders::CoderError::Format(msg)) if msg == "Invalid nullable tag: 5");
    assert!(is_invalid_tag(<Option<i64>>::decode_element(
        &mut [0x05].as_slice()
    )));
    assert!(is_invalid_tag(<Option<i64>>::decode_element_with_schema(
        &mut [0x05].as_slice(),
        None
    )));
    assert!(is_invalid_tag(<Option<i64>>::decode_element_with_context(
        &mut [0x05].as_slice(),
        None,
        None
    )));
}

#[test]
fn test_beam_iterable_methods_and_edge_cases() {
    use beam::coders::{BeamIterable, Coder, Context, DefaultCoder, IterableCoder, VarIntCoder};

    let iter = BeamIterable::from_vec(vec![1, 2, 3]);
    assert_eq!(iter.in_memory_len(), Some(3));
    assert!(!iter.is_empty());
    assert!(!iter.has_state_suffix());

    // Clone, PartialEq, Debug
    let cloned = iter.clone();
    assert_eq!(iter, cloned);
    let debug = format!("{iter:?}");
    assert_eq!(debug, "BeamIterable { elements: [1, 2, 3] }");

    // try_into_iter and into_iter
    let mut try_it = iter.clone().try_into_iter();
    assert_eq!(try_it.next().unwrap().unwrap(), 1);
    assert_eq!(try_it.next().unwrap().unwrap(), 2);
    assert_eq!(try_it.next().unwrap().unwrap(), 3);
    assert!(try_it.next().is_none());

    let collected: Vec<i32> = iter.clone().into_iter().collect();
    assert_eq!(collected, vec![1, 2, 3]);

    // into_vec
    assert_eq!(iter.into_vec().unwrap(), vec![1, 2, 3]);

    // Default and FromIterator
    let default_iter: BeamIterable<i32> = BeamIterable::default();
    assert!(default_iter.is_empty());

    let from_it: BeamIterable<i32> = vec![10, 20].into_iter().collect();
    assert_eq!(from_it.in_memory_len(), Some(2));

    // DefaultCoder for BeamIterable
    let mut wire = Vec::new();
    from_it.encode_element(&mut wire).unwrap();
    let decoded_it = BeamIterable::<i32>::decode_element(&mut wire.as_slice()).unwrap();
    assert_eq!(decoded_it.into_vec().unwrap(), vec![10, 20]);

    // IterableCoder URN and encoding
    let iterable_coder = IterableCoder::new(VarIntCoder);
    assert_eq!(
        <IterableCoder<i64, VarIntCoder> as Coder<BeamIterable<i64>>>::urn(&iterable_coder),
        "beam:coder:state_backed_iterable:v1"
    );

    let mut it_buf = Vec::new();
    let it_i64 = BeamIterable::from_vec(vec![100i64, 200i64]);
    <IterableCoder<i64, VarIntCoder> as Coder<BeamIterable<i64>>>::encode(
        &iterable_coder,
        &it_i64,
        &mut it_buf,
        Context::WholeStream,
    )
    .unwrap();
    let decoded_it_i64 = <IterableCoder<i64, VarIntCoder> as Coder<BeamIterable<i64>>>::decode(
        &iterable_coder,
        &mut it_buf.as_slice(),
        Context::WholeStream,
    )
    .unwrap();
    assert_eq!(decoded_it_i64.into_vec().unwrap(), vec![100, 200]);

    // Decoding errors. Each input ends right after the offending header, so an
    // implementation that skipped the check would hit EOF instead of this message.
    let format_error = |bytes: &[u8]| match BeamIterable::<i32>::decode_element(&mut &bytes[..]) {
        Err(beam::coders::CoderError::Format(msg)) => msg,
        other => panic!("expected a format error, got {other:?}"),
    };

    // Invalid count: only -1 introduces the chunked form.
    let invalid_count = (-2i32).to_be_bytes();
    assert_eq!(format_error(&invalid_count), "Invalid iterable count: -2");

    // Invalid chunk header
    let mut invalid_chunk = (-1i32).to_be_bytes().to_vec();
    VarIntCoder::encode_varint(-2, &mut invalid_chunk).unwrap();
    assert_eq!(
        format_error(&invalid_chunk),
        "Invalid iterable chunk header: -2"
    );

    // Invalid continuation token length
    let mut invalid_tok = (-1i32).to_be_bytes().to_vec();
    VarIntCoder::encode_varint(-1, &mut invalid_tok).unwrap(); // chunk_len = -1
    VarIntCoder::encode_varint(-1, &mut invalid_tok).unwrap(); // token_len = -1
    assert_eq!(
        format_error(&invalid_tok),
        "Invalid continuation token length: -1"
    );
}

#[test]
fn test_logical_types_error_paths() {
    use beam::schema::{BeamField, FieldValue, SchemaError};
    use chrono::{DateTime, NaiveDate, Utc};
    use rust_decimal::Decimal;

    // DateTime<Utc> error cases.
    assert!(matches!(
        DateTime::<Utc>::from_field_value(None),
        Err(SchemaError::UnexpectedNull { .. })
    ));
    assert!(matches!(
        DateTime::<Utc>::from_field_value(Some(&FieldValue::Int32(42))),
        Err(SchemaError::ValueTypeMismatch { .. })
    ));

    // NaiveDate error cases.
    assert!(matches!(
        NaiveDate::from_field_value(None),
        Err(SchemaError::UnexpectedNull { .. })
    ));
    assert!(matches!(
        NaiveDate::from_field_value(Some(&FieldValue::String("not-a-date".to_string()))),
        Err(SchemaError::ValueTypeMismatch { .. })
    ));
    // Out of range days.
    assert!(matches!(
        NaiveDate::from_field_value(Some(&FieldValue::Int64(i64::MAX))),
        Err(SchemaError::ValueOutOfRange { .. })
    ));

    // Decimal round-trip and errors.
    let pos_dec = Decimal::new(12345, 2); // 123.45
    let neg_dec = Decimal::new(-9876, 3); // -9.876
    let zero_dec = Decimal::ZERO;

    for dec in [pos_dec, neg_dec, zero_dec] {
        let fv = dec.to_field_value().unwrap();
        let roundtrip = Decimal::from_field_value(fv.as_ref()).unwrap();
        assert_eq!(dec, roundtrip);
    }

    assert!(matches!(
        Decimal::from_field_value(None),
        Err(SchemaError::UnexpectedNull { .. })
    ));
    assert!(matches!(
        Decimal::from_field_value(Some(&FieldValue::Boolean(true))),
        Err(SchemaError::ValueTypeMismatch { .. })
    ));
    // Corrupt bytes (empty payload).
    assert!(matches!(
        Decimal::from_field_value(Some(&FieldValue::Bytes(Vec::new()))),
        Err(SchemaError::LogicalTypeEncoding(_))
    ));
}
