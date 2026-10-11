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

//! Property-based tests for type-driven schema derivation and row codecs.
//!
//! The example tests in `schema_derive_test.rs` check specific values and error messages.
//! These tests check invariants that must hold for every value of a type. Encoders often
//! fail on empty collections, boundary integers, unusual scales, non-ASCII strings and rare
//! values.

use std::collections::BTreeMap;

use beam::schema::{BeamField, BeamRow, FieldValue, SchemaError};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use proptest::prelude::*;
use rust_decimal::Decimal;

#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam", id = "test.Leaves")]
struct Leaves {
    flag: bool,
    tiny: i8,
    small: i16,
    medium: i32,
    large: i64,
    unsigned_small: u16,
    unsigned_medium: u32,
    text: String,
    raw: Bytes,
}

#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam", id = "test.Containers")]
struct Containers {
    maybe_text: Option<String>,
    maybe_num: Option<i64>,
    numbers: Vec<i64>,
    labels: Vec<String>,
    tags: BTreeMap<String, i32>,
    nested: Leaves,
}

#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam", id = "test.Temporal")]
struct Temporal {
    at: DateTime<Utc>,
    day: NaiveDate,
    amount: Decimal,
}

/// Floats are in a separate struct and compared by bit pattern.
///
/// NaN is never equal to itself. A derived `PartialEq` would report a round-trip failure
/// for a value that is preserved exactly.
#[derive(Debug, BeamRow)]
#[beam(crate = "::beam", id = "test.Floats")]
struct Floats {
    single: f32,
    double: f64,
}

// Arbitrary text, including non-ASCII. The encoder writes UTF-8, and the interesting
// failures are multi-byte.
fn text() -> impl Strategy<Value = String> {
    any::<String>()
}

prop_compose! {
    fn leaves()(
        flag in any::<bool>(),
        tiny in any::<i8>(),
        small in any::<i16>(),
        medium in any::<i32>(),
        large in any::<i64>(),
        unsigned_small in any::<u16>(),
        unsigned_medium in any::<u32>(),
        text in text(),
        raw in prop::collection::vec(any::<u8>(), 0..64),
    ) -> Leaves {
        Leaves {
            flag,
            tiny,
            small,
            medium,
            large,
            unsigned_small,
            unsigned_medium,
            text,
            raw: Bytes::from(raw),
        }
    }
}

prop_compose! {
    fn containers()(
        maybe_text in prop::option::of(text()),
        maybe_num in prop::option::of(any::<i64>()),
        numbers in prop::collection::vec(any::<i64>(), 0..16),
        labels in prop::collection::vec(text(), 0..16),
        tags in prop::collection::btree_map(text(), any::<i32>(), 0..8),
        nested in leaves(),
    ) -> Containers {
        Containers { maybe_text, maybe_num, numbers, labels, tags, nested }
    }
}

prop_compose! {
    // Bounded to what chrono can represent, so the strategy generates instants
    // rather than panics: roughly the year 1 to the year 9999 in microseconds.
    fn temporal()(
        micros in -62_135_596_800_000_000i64..=253_402_300_799_999_999i64,
        days in 0i64..3_652_058i64,
        mantissa in any::<i64>(),
        scale in 0u32..=28u32,
    ) -> Temporal {
        Temporal {
            at: DateTime::from_timestamp_micros(micros).expect("micros are in range"),
            day: NaiveDate::from_num_days_from_ce_opt(days as i32 + 1).expect("days are in range"),
            amount: Decimal::from_i128_with_scale(mantissa as i128, scale),
        }
    }
}

proptest! {
    /// Values converted to Row round-trip accurately.
    #[test]
    fn containers_round_trip_through_a_row(value in containers()) {
        let row = value.to_row().expect("to_row");
        prop_assert_eq!(Containers::from_row(&row).expect("from_row"), value);
    }

    /// Values round-trip accurately through the row wire format that other SDKs read.
    #[test]
    fn containers_round_trip_over_the_wire(value in containers()) {
        let bytes = value.to_row_bytes().expect("encode");
        prop_assert_eq!(Containers::from_row_bytes(&bytes).expect("decode"), value);
    }

    #[test]
    fn temporal_round_trips_over_the_wire(value in temporal()) {
        let bytes = value.to_row_bytes().expect("encode");
        prop_assert_eq!(Temporal::from_row_bytes(&bytes).expect("decode"), value);
    }

    /// Floats round-trip preserving bit patterns across NaN and signed zero.
    #[test]
    fn floats_round_trip_bit_for_bit(single in any::<f32>(), double in any::<f64>()) {
        let value = Floats { single, double };
        let decoded = Floats::from_row_bytes(&value.to_row_bytes().expect("encode"))
            .expect("decode");
        prop_assert_eq!(decoded.single.to_bits(), value.single.to_bits());
        prop_assert_eq!(decoded.double.to_bits(), value.double.to_bits());
    }

    /// The encoding is canonical: decoded and re-encoded bytes are identical. A runner
    /// compares, groups and shuffles encoded rows. A decoder that normalizes map order, nulls
    /// or nested rows differently from the encoder would split one key into two.
    #[test]
    fn re_encoding_decoded_bytes_is_byte_identical(value in containers()) {
        let bytes = value.to_row_bytes().expect("encode");
        let decoded = Containers::from_row_bytes(&bytes).expect("decode");
        prop_assert_eq!(decoded.to_row_bytes().expect("re-encode"), bytes);
    }

    /// The schema depends only on the type, never on the value. A PCollection has exactly
    /// one schema, so a sparse value and a populated value must agree.
    #[test]
    fn the_schema_never_varies_with_the_value(value in containers()) {
        let row = value.to_row().expect("to_row");
        prop_assert_eq!(row.schema(), Containers::beam_schema());
    }
}

/// Values from another SDK that do not fit the Rust type are rejected, not wrapped.
///
/// This applies in both directions of the unsigned widening.
#[test]
fn out_of_range_values_are_rejected_when_narrowing() {
    let too_large_for_u16 = FieldValue::Int32(i32::from(u16::MAX) + 1);
    assert!(matches!(
        u16::from_field_value(Some(&too_large_for_u16)),
        Err(SchemaError::ValueOutOfRange { .. })
    ));

    let negative = FieldValue::Int32(-1);
    assert!(matches!(
        u16::from_field_value(Some(&negative)),
        Err(SchemaError::ValueOutOfRange { .. })
    ));

    let too_large_for_u32 = FieldValue::Int64(i64::from(u32::MAX) + 1);
    assert!(matches!(
        u32::from_field_value(Some(&too_large_for_u32)),
        Err(SchemaError::ValueOutOfRange { .. })
    ));
}

/// Checks one fixed value against bytes written by hand from the row coder spec.
///
/// So the round trips above cannot pass when the encoder and decoder agree on a mistake.
#[test]
fn leaves_encode_to_the_spec_bytes() {
    let value = Leaves {
        flag: true,
        tiny: -1,
        small: -2,
        medium: 300,
        large: -1,
        unsigned_small: u16::MAX,
        unsigned_medium: u32::MAX,
        text: "é".to_string(),
        raw: Bytes::from_static(&[0x00]),
    };
    let golden: &[u8] = &[
        0x09, // Field count.
        0x00, // Empty null bitmask.
        0x01, // BOOLEAN true.
        0xFF, // BYTE -1.
        0xFF, 0xFE, // INT16 -2, fixed-width big-endian.
        0xAC, 0x02, // INT32 300 as VarInt.
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, // INT64 -1: ten bytes.
        0xFF, 0xFF, 0x03, // u16::MAX widened to INT32 65535.
        0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // u32::MAX widened to INT64.
        0x02, 0xC3, 0xA9, // STRING "é": length in bytes, not chars.
        0x01, 0x00, // BYTES [0].
    ];

    assert_eq!(value.to_row_bytes().expect("encode"), golden);
    assert_eq!(Leaves::from_row_bytes(golden).expect("decode"), value);
}
