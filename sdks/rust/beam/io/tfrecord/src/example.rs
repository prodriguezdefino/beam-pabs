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

//! The `tf.train.Example` messages of TensorFlow (`tensorflow/core/example/example.proto`
//! and `feature.proto`), with the same field numbers, so TensorFlow parses the encoded
//! bytes with `tf.io.parse_single_example`.

use std::collections::HashMap;

/// A list of byte strings.
#[derive(Clone, PartialEq, prost::Message)]
pub struct BytesList {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub value: Vec<Vec<u8>>,
}

/// A list of floats.
#[derive(Clone, PartialEq, prost::Message)]
pub struct FloatList {
    #[prost(float, repeated, tag = "1")]
    pub value: Vec<f32>,
}

/// A list of 64-bit integers.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Int64List {
    #[prost(int64, repeated, tag = "1")]
    pub value: Vec<i64>,
}

/// One feature: a list of byte strings, floats or integers.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Feature {
    #[prost(oneof = "Kind", tags = "1, 2, 3")]
    pub kind: Option<Kind>,
}

/// The value of a [`Feature`].
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Kind {
    #[prost(message, tag = "1")]
    BytesList(BytesList),
    #[prost(message, tag = "2")]
    FloatList(FloatList),
    #[prost(message, tag = "3")]
    Int64List(Int64List),
}

/// Features by name.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Features {
    #[prost(map = "string, message", tag = "1")]
    pub feature: HashMap<String, Feature>,
}

/// One training example.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Example {
    #[prost(message, optional, tag = "1")]
    pub features: Option<Features>,
}

impl Feature {
    /// A feature that holds `values` as byte strings.
    pub fn bytes<I: IntoIterator<Item = impl Into<Vec<u8>>>>(values: I) -> Self {
        Self {
            kind: Some(Kind::BytesList(BytesList {
                value: values.into_iter().map(Into::into).collect(),
            })),
        }
    }

    /// A feature that holds `values` as floats.
    pub fn floats(values: impl IntoIterator<Item = f32>) -> Self {
        Self {
            kind: Some(Kind::FloatList(FloatList {
                value: values.into_iter().collect(),
            })),
        }
    }

    /// A feature that holds `values` as 64-bit integers.
    pub fn int64s(values: impl IntoIterator<Item = i64>) -> Self {
        Self {
            kind: Some(Kind::Int64List(Int64List {
                value: values.into_iter().collect(),
            })),
        }
    }
}

impl Example {
    /// An example with the features `(name, feature)`.
    pub fn new<N: Into<String>>(features: impl IntoIterator<Item = (N, Feature)>) -> Self {
        Self {
            features: Some(Features {
                feature: features
                    .into_iter()
                    .map(|(name, feature)| (name.into(), feature))
                    .collect(),
            }),
        }
    }

    /// The byte strings of the feature `name`, or `None` if it has no such feature.
    pub fn bytes(&self, name: &str) -> Option<&[Vec<u8>]> {
        match self.feature(name)? {
            Kind::BytesList(list) => Some(&list.value),
            _ => None,
        }
    }

    /// The floats of the feature `name`, or `None` if it has no such feature.
    pub fn floats(&self, name: &str) -> Option<&[f32]> {
        match self.feature(name)? {
            Kind::FloatList(list) => Some(&list.value),
            _ => None,
        }
    }

    /// The 64-bit integers of the feature `name`, or `None` if it has no such feature.
    pub fn int64s(&self, name: &str) -> Option<&[i64]> {
        match self.feature(name)? {
            Kind::Int64List(list) => Some(&list.value),
            _ => None,
        }
    }

    fn feature(&self, name: &str) -> Option<&Kind> {
        self.features.as_ref()?.feature.get(name)?.kind.as_ref()
    }
}
