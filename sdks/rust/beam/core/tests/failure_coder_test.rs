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

//! Wire-format tests for [`Failure`] dead-letter elements in `TryMap` and `TryParDo`.

use beam::coders::{Coder, Context, DefaultCoder, URN_KV};
use beam::transforms::Failure;

fn encode<T, C: Coder<T>>(coder: &C, value: &T, context: Context) -> Vec<u8> {
    let mut bytes = Vec::new();
    coder
        .encode(value, &mut bytes, context)
        .expect("encoding succeeds");
    bytes
}

#[test]
fn string_failure_encodes_as_kv_of_input_and_error() {
    // Encode nested input followed by the error in the outer context.
    let failure = Failure::<i64>::new(42, "boom");
    for context in [Context::Nested, Context::WholeStream] {
        let mut expected = encode(&i64::coder(), &42, Context::Nested);
        expected.extend(encode(&String::coder(), &"boom".to_string(), context));
        assert_eq!(
            encode(&Failure::<i64>::coder(), &failure, context),
            expected
        );
    }
}

#[test]
fn failures_round_trip_through_both_coder_paths() {
    let failure = Failure::<i64>::new(-7, "bad input");
    let coder = Failure::<i64>::coder();
    let bytes = encode(&coder, &failure, Context::Nested);
    assert_eq!(
        coder
            .decode(&mut bytes.as_slice(), Context::Nested)
            .unwrap(),
        failure
    );

    let mut element_bytes = Vec::new();
    failure.encode_element(&mut element_bytes).unwrap();
    assert_eq!(
        Failure::<i64>::decode_element(&mut element_bytes.as_slice()).unwrap(),
        failure
    );
}

#[test]
fn typed_error_round_trips() {
    type Structured = Failure<String, (i64, String)>;
    let failure: Structured = Failure::new("row-9".to_string(), (404, "missing".to_string()));
    let coder = Structured::coder();
    assert_eq!(coder.urn(), URN_KV);
    for context in [Context::Nested, Context::WholeStream] {
        let bytes = encode(&coder, &failure, context);
        assert_eq!(
            coder.decode(&mut bytes.as_slice(), context).unwrap(),
            failure
        );
    }
}
