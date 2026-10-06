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

//! The `ApproximateUnique` combine function: an estimate of the number of distinct elements.

use std::hash::{DefaultHasher, Hasher};
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::CombineFn;
use crate::coders::DefaultCoder;

/// The smallest sample size. It gives an estimation error of about 50%.
const MIN_SAMPLE_SIZE: usize = 16;

/// The size of the space of `i64` hash values.
const HASH_SPACE_SIZE: f64 = i64::MAX as f64 - i64::MIN as f64;

/// Estimates the number of distinct elements.
///
/// The function hashes the encoded bytes of each element and keeps the `sample_size` largest
/// distinct hashes. If the input has fewer distinct elements, the result is exact. Else the
/// result extrapolates from the part of the hash space that the sample covers. The
/// estimation error is about `2 / sqrt(sample_size)`. An accumulator holds at most twice
/// `sample_size` hashes.
///
/// The hash is the same on all workers of a job, so partial results merge. Estimates from
/// another SDK can differ, because each SDK uses its own hash function.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ApproximateUnique<T> {
    sample_size: usize,
    #[serde(skip)]
    _marker: PhantomData<fn() -> T>,
}

impl<T> ApproximateUnique<T> {
    /// Keeps `sample_size` hashes, for an estimation error of about `2 / sqrt(sample_size)`.
    ///
    /// # Errors
    ///
    /// Returns an error if `sample_size` is less than 16.
    pub fn with_sample_size(sample_size: usize) -> crate::Result<Self> {
        if sample_size < MIN_SAMPLE_SIZE {
            return Err(crate::Error::msg(format!(
                "ApproximateUnique needs a sample size of at least {MIN_SAMPLE_SIZE}, got \
                 {sample_size}"
            )));
        }
        Ok(Self {
            sample_size,
            _marker: PhantomData,
        })
    }

    /// Keeps enough hashes for an estimation error of about `max_error`.
    ///
    /// # Errors
    ///
    /// Returns an error if `max_error` is not between 0.01 and 0.5.
    pub fn with_max_error(max_error: f64) -> crate::Result<Self> {
        if !(0.01..=0.5).contains(&max_error) {
            return Err(crate::Error::msg(format!(
                "ApproximateUnique needs an estimation error between 0.01 and 0.5, got \
                 {max_error}"
            )));
        }
        // The error is about 2 / sqrt(sample_size), so sample_size = 4 / max_error^2.
        Self::with_sample_size((4.0 / (max_error * max_error)).ceil() as usize)
    }

    /// Reduces `hashes` to the `sample_size` largest distinct hashes, largest first.
    fn trim(&self, hashes: &mut Vec<i64>) {
        hashes.sort_unstable_by(|a, b| b.cmp(a));
        hashes.dedup();
        hashes.truncate(self.sample_size);
    }

    /// Estimates the number of distinct elements from a trimmed sample.
    fn estimate(&self, hashes: &[i64]) -> i64 {
        match hashes.last() {
            Some(&smallest) if hashes.len() >= self.sample_size => {
                let sample_space = i64::MAX as f64 - smallest as f64;
                let sample_size = self.sample_size as f64;
                // The logarithms correct for hash collisions, which are likely for 2^32
                // distinct elements. For a small sample they give about sample_size.
                let estimate = (-sample_size / sample_space).ln_1p()
                    / (-1.0 / sample_space).ln_1p()
                    * HASH_SPACE_SIZE
                    / sample_space;
                estimate.round() as i64
            }
            _ => hashes.len() as i64,
        }
    }
}

/// Hashes the encoded bytes of `element`. `DefaultHasher::new` has fixed keys, so all
/// workers that run the same binary give the same hash.
fn hash_element<T: DefaultCoder>(element: &T) -> crate::Result<i64> {
    crate::coders::with_scratch_buffer(|buf| {
        element.encode_element(buf)?;
        let mut hasher = DefaultHasher::new();
        hasher.write(buf);
        Ok(i64::from_ne_bytes(hasher.finish().to_ne_bytes()))
    })
}

impl<T: DefaultCoder> CombineFn for ApproximateUnique<T> {
    type Input = T;
    type Accum = Vec<i64>;
    type Output = i64;

    fn create_accumulator(&self) -> crate::Result<Vec<i64>> {
        Ok(Vec::new())
    }

    fn add_input(&self, mut accumulator: Vec<i64>, input: T) -> crate::Result<Vec<i64>> {
        accumulator.push(hash_element(&input)?);
        // Trimming at twice the sample size keeps the cost of each element constant on average.
        if accumulator.len() > self.sample_size.saturating_mul(2) {
            self.trim(&mut accumulator);
        }
        Ok(accumulator)
    }

    fn merge_accumulators(&self, accumulators: Vec<Vec<i64>>) -> crate::Result<Vec<i64>> {
        let mut merged = accumulators.into_iter().flatten().collect();
        self.trim(&mut merged);
        Ok(merged)
    }

    fn extract_output(&self, mut accumulator: Vec<i64>) -> crate::Result<i64> {
        self.trim(&mut accumulator);
        Ok(self.estimate(&accumulator))
    }
}
