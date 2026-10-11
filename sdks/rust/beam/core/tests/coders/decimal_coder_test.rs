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

//! Tests for the `Decimal` coder.

use beam::coders::{Coder, CoderRegistry, Context, DecimalCoder, DefaultCoder};
use proptest::prelude::*;
use rust_decimal::Decimal;

fn encode(value: &Decimal, context: Context) -> Vec<u8> {
    let mut buf = Vec::new();
    DecimalCoder
        .encode(value, &mut buf, context)
        .expect("encoding to a Vec does not fail");
    buf
}

/// The payload is a varint scale and then the minimal two's-complement unscaled value, the
/// whole-stream bytes of the Java `BigDecimalCoder`. In nested context, the bytes coder adds
/// the payload length first.
#[test]
fn a_decimal_has_the_scale_and_the_minimal_unscaled_bytes() {
    // 123.45: scale 2, unscaled 12345 = 0x3039.
    let value = Decimal::new(12345, 2);
    assert_eq!(encode(&value, Context::WholeStream), [0x02, 0x30, 0x39]);
    assert_eq!(encode(&value, Context::Nested), [0x03, 0x02, 0x30, 0x39]);
    assert_eq!(value.encode().unwrap(), [0x03, 0x02, 0x30, 0x39]);

    // -1.5: scale 1, unscaled -15 = 0xF1. 128: one more byte keeps the sign positive.
    assert_eq!(
        encode(&Decimal::new(-15, 1), Context::WholeStream),
        [0x01, 0xF1]
    );
    assert_eq!(
        encode(&Decimal::new(128, 0), Context::WholeStream),
        [0x00, 0x00, 0x80]
    );
}

#[test]
fn a_decimal_registers_the_bytes_coder() {
    struct Urns(std::cell::RefCell<Vec<String>>);
    impl CoderRegistry for Urns {
        fn register_coder(&self, urn: &str, components: Vec<String>) -> String {
            assert!(components.is_empty());
            self.0.borrow_mut().push(urn.to_string());
            "c1".to_string()
        }
    }
    let registry = Urns(std::cell::RefCell::new(Vec::new()));
    Decimal::register_coder(&registry);
    assert_eq!(registry.0.into_inner(), ["beam:coder:bytes:v1"]);
    assert_eq!(DecimalCoder.urn(), "beam:coder:bytes:v1");
}

#[test]
fn a_decimal_with_a_bad_payload_is_an_error() {
    // Scale 1 and no unscaled bytes.
    let err = DecimalCoder
        .decode(&mut &[0x01][..], Context::WholeStream)
        .expect_err("no unscaled bytes");
    assert!(err.to_string().contains("i128"), "{err}");
    // Scale 40 is more than the 28 that `Decimal` holds.
    let err = DecimalCoder
        .decode(&mut &[0x28, 0x01][..], Context::WholeStream)
        .expect_err("scale too large");
    assert!(err.to_string().contains("out of range"), "{err}");
}

proptest! {
    #[test]
    fn a_decimal_round_trips(mantissa in any::<i64>(), scale in 0u32..=28) {
        let value = Decimal::new(mantissa, scale);
        for context in [Context::Nested, Context::WholeStream] {
            let bytes = encode(&value, context);
            let decoded = DecimalCoder.decode(&mut bytes.as_slice(), context).unwrap();
            // The scale is part of the value: 1.50 and 1.5 stay different.
            prop_assert_eq!(decoded, value);
            prop_assert_eq!(decoded.scale(), value.scale());
        }
    }
}
