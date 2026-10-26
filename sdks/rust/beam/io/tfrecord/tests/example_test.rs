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

//! The `tf.train.Example` messages encode as TensorFlow encodes them.

use prost::Message;
use tfrecord_io::example::{Example, Feature};

/// A float feature encodes as the `FloatList` (field 2) of TensorFlow's `Feature`, with the
/// floats packed: TensorFlow parses these bytes.
#[test]
fn a_float_feature_has_the_bytes_of_tensorflow() {
    let bytes = Feature::floats([1.0]).encode_to_vec();
    assert_eq!(bytes, [0x12, 0x06, 0x0a, 0x04, 0x00, 0x00, 0x80, 0x3f]);
}

#[test]
fn an_example_reads_back_each_kind_of_feature() {
    let example = Example::new([
        ("name", Feature::bytes(["doc"])),
        ("scores", Feature::floats([0.5, 1.5])),
        ("ids", Feature::int64s([7, -1])),
    ]);
    let decoded = Example::decode(example.encode_to_vec().as_slice()).expect("an Example");

    assert_eq!(decoded, example);
    assert_eq!(decoded.bytes("name"), Some(&[b"doc".to_vec()][..]));
    assert_eq!(decoded.floats("scores"), Some(&[0.5, 1.5][..]));
    assert_eq!(decoded.int64s("ids"), Some(&[7, -1][..]));
    // A missing feature, or a feature of another kind, reads as `None`.
    assert_eq!(decoded.floats("missing"), None);
    assert_eq!(decoded.int64s("name"), None);
}
