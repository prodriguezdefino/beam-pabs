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

//! The value of an int64 gauge metric.

use crate::coders::VarIntCoder;

/// A gauge value and its timestamp in milliseconds. The value with the latest timestamp wins.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GaugeValue {
    pub value: i64,
    pub timestamp_ms: i64,
}

impl GaugeValue {
    pub fn new(value: i64, timestamp_ms: i64) -> Self {
        Self {
            value,
            timestamp_ms,
        }
    }

    pub fn set(&mut self, value: i64, timestamp_ms: i64) {
        if timestamp_ms >= self.timestamp_ms {
            self.value = value;
            self.timestamp_ms = timestamp_ms;
        }
    }

    /// Keeps the later of the two values. A tie goes to `other`.
    pub fn merge(&mut self, other: &Self) {
        self.set(other.value, other.timestamp_ms);
    }

    /// Encodes the gauge as Fn API bytes: `<timestamp><value>` VarInts.
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        // Invariant: encode to an in-memory vector never fails.
        let _ = VarIntCoder::encode_varint(self.timestamp_ms, &mut payload);
        let _ = VarIntCoder::encode_varint(self.value, &mut payload);
        payload
    }

    /// Decodes a gauge from Fn API bytes: `<timestamp><value>` VarInts.
    pub fn decode_payload(payload: &[u8]) -> Result<Self, String> {
        let mut reader = payload;
        let timestamp_ms = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode gauge timestamp_ms: {e}"))?;
        let value = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode gauge value: {e}"))?;
        Ok(Self {
            value,
            timestamp_ms,
        })
    }
}
