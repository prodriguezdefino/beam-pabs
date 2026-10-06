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

//! Combine functions that sample elements.

use std::cmp::Ordering;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{CombineFn, Compare, Top};
use crate::coders::DefaultCoder;

/// The ordering of sampled elements: by their random key only.
#[derive(Clone, Copy, Debug, Default)]
struct ByRandomKey;

impl<T> Compare<(i64, T)> for ByRandomKey {
    fn compare(&self, left: &(i64, T), right: &(i64, T)) -> Ordering {
        left.0.cmp(&right.0)
    }
}

/// Random keys from a hasher with a random seed. Each process gets a different seed.
#[derive(Debug)]
struct RandomKeys {
    state: RandomState,
    next: AtomicU64,
}

impl RandomKeys {
    fn new() -> Self {
        Self {
            state: RandomState::new(),
            next: AtomicU64::new(0),
        }
    }

    fn next(&self) -> i64 {
        let bits = self
            .state
            .hash_one(self.next.fetch_add(1, AtomicOrdering::Relaxed));
        i64::from_ne_bytes(bits.to_ne_bytes())
    }
}

/// Selects a uniform random sample of `count` elements, in no order.
///
/// Each element gets a random key, and the sample keeps the elements with the greatest keys,
/// so each element has the same chance to be in the sample. Use it with `CombineGlobally`
/// or `CombinePerKey`. An accumulator holds at most twice `count` elements. The sample
/// changes between runs.
#[derive(Debug)]
pub struct Sample<T> {
    count: usize,
    top: Top<(i64, T), ByRandomKey>,
    keys: RandomKeys,
}

impl<T> Sample<T> {
    /// Creates the combine function for a sample of `count` elements.
    pub fn new(count: usize) -> Self {
        Self {
            count,
            top: Top::of(count, ByRandomKey),
            keys: RandomKeys::new(),
        }
    }
}

/// The serialized fields of a [`Sample`]: only the count. Each process seeds its own keys.
#[derive(Serialize, Deserialize)]
struct SampleFields {
    count: usize,
}

impl<T> Serialize for Sample<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        SampleFields { count: self.count }.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Sample<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        SampleFields::deserialize(deserializer).map(|fields| Self::new(fields.count))
    }
}

impl<T: DefaultCoder> CombineFn for Sample<T> {
    type Input = T;
    type Accum = Vec<(i64, T)>;
    type Output = Vec<T>;

    fn create_accumulator(&self) -> crate::Result<Self::Accum> {
        self.top.create_accumulator()
    }

    fn add_input(&self, accumulator: Self::Accum, input: T) -> crate::Result<Self::Accum> {
        self.top.add_input(accumulator, (self.keys.next(), input))
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> crate::Result<Self::Accum> {
        self.top.merge_accumulators(accumulators)
    }

    fn extract_output(&self, accumulator: Self::Accum) -> crate::Result<Vec<T>> {
        let sampled = self.top.extract_output(accumulator)?;
        Ok(sampled.into_iter().map(|(_, element)| element).collect())
    }
}

/// Keeps any `count` elements, in no order. It is cheaper than [`Sample`], but the elements
/// are not random: they depend on the bundles and the order of the input.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct AnySample<T> {
    count: usize,
    #[serde(skip)]
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> AnySample<T> {
    /// Creates the combine function for `count` elements.
    pub fn new(count: usize) -> Self {
        Self {
            count,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: DefaultCoder> CombineFn for AnySample<T> {
    type Input = T;
    type Accum = Vec<T>;
    type Output = Vec<T>;

    fn create_accumulator(&self) -> crate::Result<Vec<T>> {
        Ok(Vec::new())
    }

    fn add_input(&self, mut accumulator: Vec<T>, input: T) -> crate::Result<Vec<T>> {
        if accumulator.len() < self.count {
            accumulator.push(input);
        }
        Ok(accumulator)
    }

    fn merge_accumulators(&self, accumulators: Vec<Vec<T>>) -> crate::Result<Vec<T>> {
        Ok(accumulators
            .into_iter()
            .flatten()
            .take(self.count)
            .collect())
    }

    fn extract_output(&self, accumulator: Vec<T>) -> crate::Result<Vec<T>> {
        Ok(accumulator)
    }
}
