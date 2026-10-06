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

use beam::coders::DefaultCoder;
use beam::internals::{BundleHandler, TypedElement};
use beam::internals::{DoFnHandler, HandlerContext};
use beam::transforms::{DoFn, ProcessContext};

#[test]
fn take_returns_the_value_only_for_its_own_type() {
    let mut slot = Some("word".to_string());
    let mut element = TypedElement::new(&mut slot);

    assert_eq!(
        element.take::<i64>(),
        None,
        "a different type must not be taken"
    );
    assert_eq!(element.take::<String>(), Some("word".to_string()));
    assert_eq!(element.take::<String>(), None, "a value can be taken once");
}

#[test]
fn encode_writes_the_kv_wire_bytes() {
    // KV<string, varint>: the key length-prefixed (nested), then the varint.
    let mut slot = Some(("key".to_string(), 7_i64));
    let element = TypedElement::new(&mut slot);

    assert_eq!(element.encode(), Ok(vec![3, b'k', b'e', b'y', 7]));
    assert!(slot.is_some(), "encoding must not consume the value");
}

#[test]
fn encode_after_take_is_an_error() {
    let mut slot = Some(1_i64);
    let mut element = TypedElement::new(&mut slot);
    let _ = element.take::<i64>();

    assert_eq!(
        element.encode(),
        Err("Emitted element was already consumed".to_string())
    );
}

#[test]
fn emit_into_a_byte_sink_encodes() {
    let mut sink: Vec<Vec<u8>> = Vec::new();
    let mut ctx = ProcessContext::<String>::new(&mut sink);
    ctx.emit("hello".to_string()).expect("emit");

    assert_eq!(sink, vec![vec![5, b'h', b'e', b'l', b'l', b'o']]);
}

#[derive(Clone)]
struct Double;

impl DoFn for Double {
    type In = i64;
    type Out = i64;

    fn process_element(&mut self, v: i64, out: &mut ProcessContext<i64>) -> beam::Result {
        out.emit(v * 2)
    }
}

#[test]
fn handler_falls_back_to_bytes_for_a_mismatched_type() {
    let mut handler = DoFnHandler::new(Double);
    let mut sink: Vec<Vec<u8>> = Vec::new();

    // Matching type: take directly.
    let mut matching = Some(21_i64);
    handler
        .process_value(
            TypedElement::new(&mut matching),
            &mut HandlerContext::new(&mut sink),
        )
        .expect("typed");
    assert_eq!(matching, None, "the matching value is moved into the DoFn");

    // `i32` differs from the handler input type (`i64`), triggering encode/decode fallback.
    let mut other = Some(5_i32);
    // Both use `beam:coder:varint:v1`; the byte representation ([5]) is valid for i64.
    handler
        .process_value(
            TypedElement::new(&mut other),
            &mut HandlerContext::new(&mut sink),
        )
        .expect("the varint bytes of an i32 decode as an i64");
    assert_eq!(other, Some(5), "a mismatched value is left in place");

    assert_eq!(
        sink,
        vec![42_i64.encode().expect("encode"), vec![10]],
        "21 doubled directly, then 5 doubled via the byte fallback"
    );
}

#[test]
fn handler_fallback_reports_bytes_that_do_not_decode() {
    let mut handler = DoFnHandler::new(Double);
    let mut sink: Vec<Vec<u8>> = Vec::new();

    // All-ones f64 encodes as eight 0xFF bytes (VarInt continuation without terminator).
    // The fallback decode as i64 must fail.
    let mut other = Some(f64::from_bits(u64::MAX));
    let err = handler
        .process_value(
            TypedElement::new(&mut other),
            &mut HandlerContext::new(&mut sink),
        )
        .expect_err("eight 0xFF bytes are not a complete varint");
    assert!(sink.is_empty(), "nothing may be emitted, got {sink:?}");
    assert_eq!(
        err,
        "Failed to decode input element: IO error during encoding/decoding: failed to fill whole buffer"
    );
}
