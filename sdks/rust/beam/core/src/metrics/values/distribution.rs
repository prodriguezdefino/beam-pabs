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

//! The value of an int64 distribution metric.

use crate::coders::VarIntCoder;

/// Statistics tracked by an integer distribution metric cell.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DistributionValue {
    pub count: i64,
    pub sum: i64,
    pub min: i64,
    pub max: i64,
}

impl DistributionValue {
    pub fn new(value: i64) -> Self {
        Self {
            count: 1,
            sum: value,
            min: value,
            max: value,
        }
    }

    pub fn update(&mut self, value: i64) {
        if self.count == 0 {
            *self = Self::new(value);
        } else {
            // Count saturates at maximum value. Sum wraps on overflow.
            self.count = self.count.saturating_add(1);
            self.sum = self.sum.wrapping_add(value);
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
    }

    pub fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
        }
        // Count saturates at maximum value. Sum wraps on overflow.
        self.count = self.count.saturating_add(other.count);
        self.sum = self.sum.wrapping_add(other.sum);
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Encodes the distribution as Fn API bytes: `<count><sum><min><max>` VarInts.
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        // Invariant: encode to an in-memory vector never fails.
        let _ = VarIntCoder::encode_varint(self.count, &mut payload);
        let _ = VarIntCoder::encode_varint(self.sum, &mut payload);
        let _ = VarIntCoder::encode_varint(self.min, &mut payload);
        let _ = VarIntCoder::encode_varint(self.max, &mut payload);
        payload
    }

    /// Decodes a distribution from Fn API bytes: `<count><sum><min><max>` VarInts.
    pub fn decode_payload(payload: &[u8]) -> Result<Self, String> {
        let mut reader = payload;
        let count = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode distribution count: {e}"))?;
        let sum = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode distribution sum: {e}"))?;
        let min = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode distribution min: {e}"))?;
        let max = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode distribution max: {e}"))?;
        Ok(Self {
            count,
            sum,
            min,
            max,
        })
    }
}
