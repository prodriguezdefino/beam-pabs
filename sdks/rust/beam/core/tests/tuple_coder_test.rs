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

//! Tests for tuples of 3 or more values: the bytes and the registered coders are those of
//! the right-nested KV.

use std::cell::RefCell;

use beam::coders::{
    BytesCoder, Coder, CoderRegistry, Context, DefaultCoder, KvCoder, StringUtf8Coder, TupleCoder,
    VarIntCoder,
};
use proptest::prelude::*;

/// A registry that records each registration and numbers the IDs.
#[derive(Default)]
struct RecordingRegistry {
    registered: RefCell<Vec<(String, Vec<String>)>>,
}

impl CoderRegistry for RecordingRegistry {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String {
        let mut registered = self.registered.borrow_mut();
        registered.push((urn.to_string(), component_coder_ids));
        format!("c{}", registered.len())
    }
}

impl RecordingRegistry {
    fn into_records(self) -> Vec<(String, Vec<String>)> {
        self.registered.into_inner()
    }
}

fn encode<T, C: Coder<T>>(coder: &C, value: &T, context: Context) -> Vec<u8> {
    let mut buf = Vec::new();
    coder
        .encode(value, &mut buf, context)
        .expect("encoding to a Vec does not fail");
    buf
}

proptest! {
    /// The last value is a string or bytes, so the context changes its length prefix.
    #[test]
    fn a_triple_has_the_bytes_of_the_nested_kv(
        a in any::<i64>(),
        b in ".*",
        c in proptest::collection::vec(any::<u8>(), 0..32),
    ) {
        let tuple = TupleCoder::new((VarIntCoder, StringUtf8Coder, BytesCoder));
        let nested = KvCoder::new(VarIntCoder, KvCoder::new(StringUtf8Coder, BytesCoder));
        let flat = (a, b.clone(), c.clone());
        let pairs = (a, (b, c));

        for context in [Context::Nested, Context::WholeStream] {
            let bytes = encode(&tuple, &flat, context);
            prop_assert_eq!(&bytes, &encode(&nested, &pairs, context));
            prop_assert_eq!(&tuple.decode(&mut bytes.as_slice(), context).unwrap(), &flat);
        }
    }

    #[test]
    fn a_tuple_element_has_the_bytes_of_the_nested_pairs(
        a in any::<i64>(),
        b in ".*",
        c in any::<bool>(),
        d in any::<f64>().prop_filter("NaN is not equal to itself", |d| !d.is_nan()),
    ) {
        let flat = (a, b.clone(), c, d);
        let bytes = flat.encode().unwrap();
        prop_assert_eq!(&bytes, &(a, (b, (c, d))).encode().unwrap());
        prop_assert_eq!(&<(i64, String, bool, f64)>::decode(&bytes).unwrap(), &flat);

        // The coder object writes the same bytes as the element methods.
        let coder = <(i64, String, bool, f64)>::coder();
        prop_assert_eq!(&encode(&coder, &flat, Context::Nested), &bytes);
    }
}

#[test]
fn a_tuple_coder_reports_the_kv_urn() {
    let coder = <(i64, String, bool)>::coder();
    assert_eq!(coder.urn(), "beam:coder:kv:v1");
}

/// Each registration has the same order, URNs and components as the nested pairs.
#[test]
fn a_tuple_registers_the_nested_kv_coders() {
    let flat = RecordingRegistry::default();
    let nested = RecordingRegistry::default();
    let flat_id = <(i64, String, bool, Vec<i64>)>::register_coder(&flat);
    let nested_id = <(i64, (String, (bool, Vec<i64>)))>::register_coder(&nested);

    assert_eq!(flat_id, nested_id);
    assert_eq!(flat.into_records(), nested.into_records());
}

#[test]
fn a_tuple_value_registers_the_nested_kv_coders() {
    let flat = RecordingRegistry::default();
    let nested = RecordingRegistry::default();
    let flat_id = (1i64, "a".to_string(), vec![2i64]).register_value_coder(&flat);
    let nested_id = (1i64, ("a".to_string(), vec![2i64])).register_value_coder(&nested);

    assert_eq!(flat_id, nested_id);
    assert_eq!(flat.into_records(), nested.into_records());
}

/// Each arity from 2 to 6 has a coder.
#[test]
fn every_tuple_arity_round_trips() {
    fn round_trip<T: DefaultCoder + PartialEq + std::fmt::Debug>(value: T) {
        let bytes = value.encode().expect("encoding to a Vec does not fail");
        assert_eq!(T::decode(&bytes).expect("the bytes decode"), value);
    }
    round_trip((0i64, 1i64));
    round_trip((0i64, 1i64, 2i64));
    round_trip((0i64, 1i64, 2i64, 3i64));
    round_trip((0i64, 1i64, 2i64, 3i64, 4i64));
    round_trip((0i32, "one".to_string(), true, (), Some(4.5f64), vec![5i64]));
}

/// A tuple can be the value of a KV, so `GroupByKey` outputs and combine accumulators that
/// hold tuples encode.
#[test]
fn a_tuple_nests_in_a_kv() {
    let value = ("key".to_string(), (1i64, 2i64, 3i64));
    let bytes = value.encode().unwrap();
    assert_eq!(<(String, (i64, i64, i64))>::decode(&bytes).unwrap(), value);
    assert_eq!(
        bytes,
        ("key".to_string(), (1i64, (2i64, 3i64))).encode().unwrap()
    );
}
