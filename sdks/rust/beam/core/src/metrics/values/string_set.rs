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

//! The value of a string set metric.

use std::collections::BTreeSet;

use crate::coders::VarIntCoder;

/// The largest total size of the strings in a [`StringSetValue`]. Later strings are dropped.
pub const STRING_SET_MAX_BYTES: usize = 1 << 20;

/// A set of distinct strings, for example the sources that a transform reads.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StringSetValue {
    values: BTreeSet<String>,
    bytes: usize,
}

impl StringSetValue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `value`. Returns `false` if the set is full and `value` is new, so it is dropped.
    pub fn add(&mut self, value: &str) -> bool {
        if self.values.contains(value) {
            return true;
        }
        if self.bytes + value.len() > STRING_SET_MAX_BYTES {
            return false;
        }
        self.bytes += value.len();
        self.values.insert(value.to_string());
        true
    }

    /// Adds the strings of `other`. Returns `false` if some are dropped.
    pub fn merge(&mut self, other: &Self) -> bool {
        let dropped = other.values.iter().filter(|value| !self.add(value)).count();
        dropped == 0
    }

    /// The strings, in order.
    pub fn values(&self) -> &BTreeSet<String> {
        &self.values
    }

    /// Encodes the set as Fn API bytes: a big-endian `i32` count, then each string with a
    /// VarInt length.
    pub fn encode_payload(&self) -> Vec<u8> {
        let count = i32::try_from(self.values.len()).unwrap_or(i32::MAX);
        let mut payload = count.to_be_bytes().to_vec();
        self.values.iter().for_each(|value| {
            // Invariant: encode to an in-memory vector never fails.
            let _ = VarIntCoder::encode_varint(value.len() as i64, &mut payload);
            payload.extend_from_slice(value.as_bytes());
        });
        payload
    }

    /// Decodes a set that [`encode_payload`](Self::encode_payload) wrote.
    pub fn decode_payload(payload: &[u8]) -> Result<Self, String> {
        let (count, mut reader) = payload
            .split_first_chunk::<4>()
            .ok_or("Failed to decode the string set count")?;
        let count = i32::from_be_bytes(*count).max(0);
        (0..count).try_fold(Self::new(), |mut set, _| {
            let len = VarIntCoder::decode_varint(&mut reader)
                .ok()
                .and_then(|len| usize::try_from(len).ok())
                .ok_or("Failed to decode a string set length")?;
            let (bytes, rest) = reader
                .split_at_checked(len)
                .ok_or("A string set entry is longer than its payload")?;
            reader = rest;
            let value = std::str::from_utf8(bytes)
                .map_err(|e| format!("A string set entry is not UTF-8: {e}"))?;
            set.values.insert(value.to_string());
            set.bytes += value.len();
            Ok(set)
        })
    }
}
