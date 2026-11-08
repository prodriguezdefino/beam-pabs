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

//! Deterministic 48-bit linear congruential generator.

/// 48-bit linear congruential generator with multiplier `0x5DEECE66D` and addend `0xB`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JavaRandom {
    state: u64,
}

impl JavaRandom {
    const MULTIPLIER: u64 = 0x0005_DEEC_E66D;
    const ADDEND: u64 = 0xB;
    const MASK: u64 = (1_u64 << 48) - 1;
    const DOUBLE_UNIT: f64 = f64::from_bits(0x3ca0_0000_0000_0000);

    /// Creates a generator initialized from `seed`.
    pub const fn new(seed: i64) -> Self {
        Self {
            state: ((seed as u64) ^ Self::MULTIPLIER) & Self::MASK,
        }
    }

    /// Resets the internal 48-bit state from `seed`.
    pub fn set_seed(&mut self, seed: i64) {
        self.state = ((seed as u64) ^ Self::MULTIPLIER) & Self::MASK;
    }

    /// Advances the state and returns the top `bits` (`1..=32`) as a signed 32-bit integer.
    pub fn next_bits(&mut self, bits: u32) -> i32 {
        self.state = (self
            .state
            .wrapping_mul(Self::MULTIPLIER)
            .wrapping_add(Self::ADDEND))
            & Self::MASK;
        if bits == 0 {
            0
        } else {
            let shift = 48_u32.saturating_sub(bits.min(32));
            (self.state >> shift) as i32
        }
    }

    /// Returns a uniformly distributed 32-bit signed integer.
    pub fn next_int(&mut self) -> i32 {
        self.next_bits(32)
    }

    /// Returns a uniformly distributed integer in `0..bound` (or `0` when `bound <= 0`).
    pub fn next_int_bound(&mut self, bound: i32) -> i32 {
        if bound <= 0 {
            return 0;
        }
        if (bound as u32).is_power_of_two() {
            return ((i64::from(bound).wrapping_mul(i64::from(self.next_bits(31)))) >> 31) as i32;
        }
        loop {
            let bits = self.next_bits(31);
            let val = bits % bound;
            if bits.wrapping_sub(val).wrapping_add(bound - 1) >= 0 {
                return val;
            }
        }
    }

    /// Returns a 64-bit signed integer assembled from two consecutive 32-bit draws.
    pub fn next_long(&mut self) -> i64 {
        (i64::from(self.next_bits(32)) << 32).wrapping_add(i64::from(self.next_bits(32)))
    }

    /// Returns a 53-bit precision `f64` in `[0.0, 1.0)`.
    pub fn next_double(&mut self) -> f64 {
        let raw = (i64::from(self.next_bits(26)) << 27) + i64::from(self.next_bits(27));
        (raw as f64) * Self::DOUBLE_UNIT
    }

    /// Returns a pseudorandom boolean value.
    pub fn next_boolean(&mut self) -> bool {
        self.next_bits(1) != 0
    }

    /// Fills `bytes` with four little-endian octets per 32-bit draw.
    pub fn next_bytes(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(4) {
            let le = self.next_int().to_le_bytes();
            chunk.copy_from_slice(&le[..chunk.len()]);
        }
    }
}
