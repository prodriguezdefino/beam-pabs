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

//! Tests for type-driven schema derivation.
//!
//! A schema depends only on the Rust type. Beam requires that a PCollection has exactly
//! one schema, so a populated value and an empty value of the same type must give the
//! same schema.

use std::collections::BTreeMap;

use beam::coders::VarIntCoder;
use beam::schema::{
    AtomicType, BeamEnum, BeamField, BeamRow, FieldValue, Row, SchemaError, TypeInfo, URN_DATE,
    URN_DECIMAL, URN_MICROS_INSTANT,
};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct Probe {
    maybe_num: Option<i64>,
    items: Vec<i64>,
    small: i32,
    ratio: f32,
}

#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam", id = "test.Nested")]
struct Nested {
    #[beam(rename = "renamed_field")]
    original: String,
    #[beam(bytes)]
    blob: Vec<u8>,
    raw: Bytes,
    inner: Probe,
    tags: BTreeMap<String, i32>,
    #[beam(skip)]
    cached: u64,
}

/// Verify that a sparse value and a populated value of the same type produce identical schemas.
#[test]
fn schema_is_independent_of_value() {
    let populated = Probe {
        maybe_num: Some(7),
        items: vec![1, 2],
        small: 3,
        ratio: 1.5,
    };
    let sparse = Probe {
        maybe_num: None,
        items: Vec::new(),
        small: 0,
        ratio: 0.0,
    };

    let populated_schema = populated.to_row().expect("to_row must succeed");
    let sparse_schema = sparse.to_row().expect("to_row must succeed");

    assert_eq!(populated_schema.schema(), sparse_schema.schema());
    assert_eq!(
        populated_schema.schema().as_ref(),
        Probe::beam_schema().as_ref()
    );
}

/// Verify primitive types map to their exact schema counterparts without widening.
#[test]
fn narrow_types_are_not_widened() {
    let schema = Probe::beam_schema();

    let field_type = |name: &str| {
        schema
            .field(name)
            .unwrap_or_else(|| panic!("field {name} must exist"))
            .field_type
            .clone()
    };

    assert_eq!(
        field_type("small").type_info,
        TypeInfo::Atomic(AtomicType::Int32)
    );
    assert_eq!(
        field_type("ratio").type_info,
        TypeInfo::Atomic(AtomicType::Float)
    );

    // Nullability comes from `Option<i64>`, not from the presence of a value.
    let maybe_num = field_type("maybe_num");
    assert!(maybe_num.nullable);
    assert_eq!(maybe_num.type_info, TypeInfo::Atomic(AtomicType::Int64));

    // The element type comes from `Vec<i64>`, not from the first element.
    match field_type("items").type_info {
        TypeInfo::Array(elem) => {
            assert_eq!(elem.type_info, TypeInfo::Atomic(AtomicType::Int64));
            assert!(!elem.nullable);
        }
        other => panic!("expected ARRAY, found {other:?}"),
    }
}

#[test]
fn round_trips_through_row() {
    let probe = Probe {
        maybe_num: None,
        items: vec![10, 20, 30],
        small: -5,
        ratio: 2.25,
    };

    let row = probe.to_row().expect("to_row must succeed");
    let recovered = Probe::from_row(&row).expect("from_row must succeed");

    assert_eq!(probe, recovered);
}

#[test]
fn honors_field_attributes() {
    let schema = Nested::beam_schema();

    assert_eq!(schema.id.as_deref(), Some("test.Nested"));
    assert!(schema.field("renamed_field").is_some(), "rename must apply");
    assert!(
        schema.field("original").is_none(),
        "original name must be gone"
    );
    assert!(
        schema.field("cached").is_none(),
        "skip must exclude the field"
    );

    // #[beam(bytes)] overrides the array mapping Vec<u8> would otherwise need.
    assert_eq!(
        schema.field("blob").expect("blob").field_type.type_info,
        TypeInfo::Atomic(AtomicType::Bytes)
    );
    assert_eq!(
        schema.field("raw").expect("raw").field_type.type_info,
        TypeInfo::Atomic(AtomicType::Bytes)
    );
}

#[test]
fn round_trips_nested_rows_and_maps() {
    let nested = Nested {
        original: "hello".to_string(),
        blob: vec![1, 2, 3],
        raw: Bytes::from_static(b"raw"),
        inner: Probe {
            maybe_num: Some(42),
            items: vec![9],
            small: 1,
            ratio: 0.5,
        },
        tags: BTreeMap::from([("a".to_string(), 1), ("b".to_string(), 2)]),
        cached: 0,
    };

    let row = nested.to_row().expect("to_row must succeed");
    let recovered = Nested::from_row(&row).expect("from_row must succeed");

    assert_eq!(nested, recovered);
}

/// A skipped field is reconstructed from `Default`, not from the wire.
#[test]
fn skipped_fields_default_on_decode() {
    let nested = Nested {
        original: "x".to_string(),
        blob: Vec::new(),
        raw: Bytes::new(),
        inner: Probe {
            maybe_num: None,
            items: Vec::new(),
            small: 0,
            ratio: 0.0,
        },
        tags: BTreeMap::new(),
        cached: 99,
    };

    let row = nested.to_row().expect("to_row must succeed");
    let recovered = Nested::from_row(&row).expect("from_row must succeed");

    assert_eq!(
        recovered.cached, 0,
        "skipped field must come back as Default"
    );
}

#[test]
fn null_into_non_nullable_is_rejected() {
    assert_eq!(
        i64::from_field_value(None),
        Err(SchemaError::UnexpectedNull {
            expected: "INT64".to_string()
        })
    );
    assert!(
        Option::<i64>::from_field_value(None)
            .expect("option accepts null")
            .is_none()
    );
}

/// Nested rows must carry the nested schema, not a flattened one.
#[test]
fn nested_row_field_type_carries_inner_schema() {
    let schema = Nested::beam_schema();
    match &schema.field("inner").expect("inner").field_type.type_info {
        TypeInfo::Row(inner) => assert_eq!(inner, Probe::beam_schema().as_ref()),
        other => panic!("expected ROW, found {other:?}"),
    }
}

/// The cached schema must be the same allocation on every call.
#[test]
fn schema_is_cached() {
    let first = Probe::beam_schema();
    let second = Probe::beam_schema();
    assert!(std::sync::Arc::ptr_eq(first, second));
}

#[test]
fn row_values_align_with_schema_order() {
    let probe = Probe {
        maybe_num: Some(1),
        items: vec![2],
        small: 3,
        ratio: 4.0,
    };
    let row: Row = probe.to_row().expect("to_row must succeed");

    assert_eq!(row.values().len(), Probe::beam_schema().num_fields());
    assert_eq!(row.get_i64("maybe_num").expect("maybe_num"), Some(1));
    assert_eq!(row.get_i32("small").expect("small"), Some(3));
}

#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct Temporal {
    at: DateTime<Utc>,
    on: NaiveDate,
    maybe_at: Option<DateTime<Utc>>,
}

#[test]
fn logical_types_use_portable_urns_and_representations() {
    let schema = Temporal::beam_schema();

    match &schema.field("at").expect("at").field_type.type_info {
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => {
            assert_eq!(urn, URN_MICROS_INSTANT);
            // The spec mandates ROW<seconds: INT64, micros: INT64>.
            match &representation.type_info {
                TypeInfo::Row(inner) => {
                    let names: Vec<_> = inner.fields.iter().map(|f| f.name.as_str()).collect();
                    assert_eq!(names, vec!["seconds", "micros"]);
                }
                other => panic!("expected ROW representation, found {other:?}"),
            }
        }
        other => panic!("expected LOGICAL, found {other:?}"),
    }

    match &schema.field("on").expect("on").field_type.type_info {
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => {
            assert_eq!(urn, URN_DATE);
            assert_eq!(
                representation.type_info,
                TypeInfo::Atomic(AtomicType::Int64)
            );
        }
        other => panic!("expected LOGICAL, found {other:?}"),
    }

    // Nullability still composes over a logical type.
    assert!(
        schema
            .field("maybe_at")
            .expect("maybe_at")
            .field_type
            .nullable
    );
}

#[test]
fn logical_types_round_trip() {
    let temporal = Temporal {
        at: DateTime::from_timestamp(1_700_000_000, 123_456_000).expect("valid instant"),
        on: NaiveDate::from_ymd_opt(2024, 2, 29).expect("valid leap day"),
        maybe_at: None,
    };

    let row = temporal.to_row().expect("to_row must succeed");
    let recovered = Temporal::from_row(&row).expect("from_row must succeed");

    assert_eq!(temporal, recovered);
}

/// Before the epoch, `micros` must stay non-negative — the specification calls
/// this out explicitly, and plain truncating division would violate it.
#[test]
fn pre_epoch_instants_keep_micros_non_negative() {
    let before_epoch =
        DateTime::from_timestamp(-2, 500_000_000).expect("1.5s before the epoch is valid");

    let value = before_epoch
        .to_field_value()
        .expect("conversion must succeed")
        .expect("instant is not null");

    match value {
        FieldValue::Row(row) => {
            assert_eq!(row.get_i64("seconds").expect("seconds"), Some(-2));
            let micros = row.get_i64("micros").expect("micros").expect("not null");
            assert!(micros >= 0, "micros must be non-negative, found {micros}");
            assert_eq!(micros, 500_000);
        }
        other => panic!("expected ROW, found {other:?}"),
    }

    let recovered =
        DateTime::<Utc>::from_field_value(Some(&before_epoch.to_field_value().unwrap().unwrap()))
            .expect("round trip must succeed");
    assert_eq!(recovered, before_epoch);
}

#[test]
fn dates_round_trip_across_the_epoch() {
    let cases = [
        NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"),
        NaiveDate::from_ymd_opt(1969, 12, 31).expect("day before epoch"),
        NaiveDate::from_ymd_opt(2024, 2, 29).expect("leap day"),
    ];

    for date in cases {
        let value = date
            .to_field_value()
            .expect("conversion must succeed")
            .expect("date is not null");
        let recovered = NaiveDate::from_field_value(Some(&value)).expect("round trip");
        assert_eq!(recovered, date);
    }

    // Day 0 represents the Unix epoch.
    let epoch_value = NaiveDate::from_ymd_opt(1970, 1, 1)
        .expect("epoch")
        .to_field_value()
        .expect("conversion")
        .expect("not null");
    assert_eq!(epoch_value, FieldValue::Int64(0));
}

/// Checks the decimal binary layout:
/// `VarInt(scale) || VarInt(len) || two's-complement big-endian unscaled`.
#[test]
fn decimal_matches_bigdecimal_coder_layout() {
    let cases: [(&str, &[u8]); 4] = [
        // 1.23 -> scale 2, unscaled 123 (0x7B fits in one byte)
        ("1.23", &[0x02, 0x01, 0x7B]),
        // -1.23 -> unscaled -123 == 0x85 in one signed byte
        ("-1.23", &[0x02, 0x01, 0x85]),
        // 0 -> scale 0, unscaled 0, still one byte of sign information
        ("0", &[0x00, 0x01, 0x00]),
        // 128 needs a leading 0x00 so that it does not read as negative.
        ("128", &[0x00, 0x02, 0x00, 0x80]),
    ];

    for (text, expected) in cases {
        let decimal: Decimal = text.parse().expect("test decimal must parse");
        let value = decimal
            .to_field_value()
            .expect("encoding must succeed")
            .expect("decimal is not null");

        match value {
            FieldValue::Bytes(bytes) => assert_eq!(
                bytes, expected,
                "unexpected encoding for {text}: {bytes:02x?}"
            ),
            other => panic!("expected BYTES, found {other:?}"),
        }
    }
}

#[test]
fn decimals_round_trip() {
    let cases = [
        "0",
        "1.23",
        "-1.23",
        "128",
        "-128",
        "0.0001",
        "12345678901234567890",
    ];

    for text in cases {
        let decimal: Decimal = text.parse().expect("test decimal must parse");
        let value = decimal
            .to_field_value()
            .expect("encoding must succeed")
            .expect("not null");
        let recovered = Decimal::from_field_value(Some(&value)).expect("round trip must succeed");

        assert_eq!(recovered, decimal, "round trip failed for {text}");
    }
}

/// A 16-byte unscaled value is the widest accepted, even with redundant sign bytes.
#[test]
fn a_sixteen_byte_unscaled_value_is_accepted() {
    for (fill, expected) in [(0x00, Decimal::new(1, 2)), (0xFF, Decimal::new(-1, 2))] {
        // Scale 2, length 16, then the sign-extended value ±1.
        let mut payload = vec![0x02, 16];
        payload.extend([fill; 15]);
        payload.push(if fill == 0 { 0x01 } else { 0xFF });
        let decoded = Decimal::from_field_value(Some(&FieldValue::Bytes(payload)));
        assert_eq!(decoded, Ok(expected), "fill {fill:#04x}");
    }
}

/// Decoding must report an error, not truncate or panic, for out-of-range values and corrupt
/// payloads.
#[test]
fn undecodable_decimal_payloads_are_rejected() {
    let decode =
        |payload: &[u8]| Decimal::from_field_value(Some(&FieldValue::Bytes(payload.to_vec())));

    // Scale 29 exceeds rust_decimal's maximum of 28.
    assert!(
        matches!(
            decode(&[29, 0x01, 0x01]),
            Err(SchemaError::ValueOutOfRange { ref target, .. }) if target == "Decimal"
        ),
        "{:?}",
        decode(&[29, 0x01, 0x01])
    );

    // A 17-byte unscaled value does not fit in i128, let alone 96 bits.
    let mut wide = vec![0x00, 17, 0x01];
    wide.extend([0u8; 16]);
    assert_eq!(
        decode(&wide),
        Err(SchemaError::ValueOutOfRange {
            value: "17 byte big integer".to_string(),
            target: "i128".to_string(),
        })
    );

    // 2^96 fits in i128 but not in rust_decimal's 96-bit mantissa.
    let mut too_big = vec![0x00, 13, 0x01];
    too_big.extend([0u8; 12]);
    assert!(matches!(
        decode(&too_big),
        Err(SchemaError::ValueOutOfRange { ref target, .. }) if target == "Decimal"
    ));

    // Declares five unscaled bytes, supplies one.
    assert_eq!(
        decode(&[0x02, 0x05, 0x01]),
        Err(SchemaError::LogicalTypeEncoding(
            "decimal payload declares 5 bytes but only 1 remain".to_string()
        ))
    );

    // A length near usize::MAX must not overflow the bounds arithmetic.
    let mut huge = vec![0x02];
    VarIntCoder::encode_varint(i64::MAX, &mut huge).expect("vec write");
    huge.push(0x01);
    assert!(matches!(
        decode(&huge),
        Err(SchemaError::LogicalTypeEncoding(ref msg)) if msg.starts_with("decimal payload declares")
    ));

    // Negative length, and a zero-length (signless) unscaled value.
    let mut negative = vec![0x02];
    VarIntCoder::encode_varint(-1, &mut negative).expect("vec write");
    assert_eq!(
        decode(&negative),
        Err(SchemaError::LogicalTypeEncoding(
            "negative decimal length -1".to_string()
        ))
    );
    assert!(matches!(
        decode(&[0x02, 0x00]),
        Err(SchemaError::ValueOutOfRange { ref value, .. }) if value == "0 byte big integer"
    ));
}

#[test]
fn decimal_uses_the_portable_urn() {
    match <Decimal as BeamField>::beam_field_type().type_info {
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => {
            assert_eq!(urn, URN_DECIMAL);
            assert_eq!(
                representation.type_info,
                TypeInfo::Atomic(AtomicType::Bytes)
            );
        }
        other => panic!("expected LOGICAL, found {other:?}"),
    }
}

// ===========================================================================
// #[derive(BeamEnum)]
// ===========================================================================

#[derive(Debug, Clone, Copy, PartialEq, BeamEnum)]
#[beam(crate = "::beam")]
enum Tier {
    Bronze,
    Silver,
    Gold,
}

#[derive(Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam", id = "test.Membership")]
struct Membership {
    tier: Tier,
    pending: Option<Tier>,
}

/// Enums map to non-nullable STRING.
#[test]
fn enums_map_to_non_nullable_string() {
    let field_type = <Tier as BeamField>::beam_field_type();

    assert_eq!(field_type.type_info, TypeInfo::Atomic(AtomicType::String));
    assert!(!field_type.nullable);
}

#[test]
fn every_variant_round_trips_through_its_label() {
    for (variant, label) in [
        (Tier::Bronze, "Bronze"),
        (Tier::Silver, "Silver"),
        (Tier::Gold, "Gold"),
    ] {
        let encoded = variant
            .to_field_value()
            .expect("encoding must succeed")
            .expect("variants are never null");

        assert_eq!(encoded, FieldValue::String(label.to_string()));
        assert_eq!(
            Tier::from_field_value(Some(&encoded)).expect("decoding must succeed"),
            variant
        );
    }
}

/// An unrecognised label must name the variants that would have been accepted;
/// this is the error a cross-SDK producer sees when the enums drift apart.
#[test]
fn unknown_labels_are_rejected_and_list_the_variants() {
    let err = Tier::from_field_value(Some(&FieldValue::String("Platinum".to_string())))
        .expect_err("an unknown label must not decode");
    let message = err.to_string();

    assert!(message.contains("Platinum"), "missing actual: {message}");
    for variant in ["Bronze", "Silver", "Gold"] {
        assert!(message.contains(variant), "missing {variant}: {message}");
    }
}

#[test]
fn non_string_and_null_values_are_rejected() {
    let wrong_type = Tier::from_field_value(Some(&FieldValue::Int64(1)))
        .expect_err("an INT64 must not decode as an enum");
    assert!(
        wrong_type.to_string().contains("STRING"),
        "expected STRING in {wrong_type}"
    );

    let null = Tier::from_field_value(None).expect_err("a null must not decode as a bare enum");
    assert!(
        null.to_string().contains("STRING"),
        "expected STRING in {null}"
    );
}

#[test]
fn enums_nest_in_rows_and_options_add_nullability() {
    let schema = Membership::beam_schema();
    let shape: Vec<(&str, String)> = schema
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.to_string()))
        .collect();

    assert_eq!(
        shape,
        vec![
            ("tier", "STRING".to_string()),
            ("pending", "STRING?".to_string())
        ]
    );
}

#[test]
fn enum_rows_round_trip_over_the_wire() {
    for original in [
        Membership {
            tier: Tier::Gold,
            pending: Some(Tier::Silver),
        },
        Membership {
            tier: Tier::Bronze,
            pending: None,
        },
    ] {
        let bytes = original.to_row_bytes().expect("encoding must succeed");
        let decoded = Membership::from_row_bytes(&bytes).expect("decoding must succeed");

        assert_eq!(decoded, original);
    }
}
