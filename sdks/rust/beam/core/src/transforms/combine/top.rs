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

//! The `Top` combine function: the greatest elements by an ordering.

use serde::{Deserialize, Serialize};

use super::{CombineFn, Compare, Natural, Reversed};
use crate::coders::DefaultCoder;

/// Keeps the `count` greatest elements by `compare`, in descending order.
///
/// Use it with `CombineGlobally` for the top elements of a collection, or with
/// `CombinePerKey` for the top values of each key. An accumulator holds at most twice
/// `count` elements, so the memory does not grow with the input.
///
/// [`largest`](Top::largest) and [`smallest`](Top::smallest) use the named orderings
/// [`Natural`] and [`Reversed`], so the SDK registers them for the standard element types.
/// A `Top` with a closure ordering uses the rebuild path.
///
/// ```
/// use beam::transforms::Top;
///
/// let longest = Top::of(3, |a: &String, b: &String| a.len().cmp(&b.len()));
/// let largest = Top::<i64, _>::largest(10);
/// # let _ = (longest, largest);
/// ```
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "C: Serialize", deserialize = "C: Deserialize<'de>"))]
pub struct Top<T, C> {
    count: usize,
    compare: C,
    #[serde(skip)]
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T, C: Compare<T>> Top<T, C> {
    /// Keeps the `count` greatest elements by `compare`. Elements that compare equal have
    /// no defined order.
    pub fn of(count: usize, compare: C) -> Self {
        Self {
            count,
            compare,
            _marker: std::marker::PhantomData,
        }
    }

    /// Reduces `elements` to the `count` greatest, in no order. It runs in linear time.
    fn trim(&self, elements: &mut Vec<T>) {
        match self.count {
            0 => elements.clear(),
            count if elements.len() > count => {
                elements.select_nth_unstable_by(count - 1, |a, b| self.compare.compare(b, a));
                elements.truncate(count);
            }
            _ => {}
        }
    }
}

impl<T: Ord> Top<T, Natural> {
    /// Keeps the `count` largest elements, largest first.
    pub fn largest(count: usize) -> Self {
        Self::of(count, Natural)
    }
}

impl<T: Ord> Top<T, Reversed> {
    /// Keeps the `count` smallest elements, smallest first.
    pub fn smallest(count: usize) -> Self {
        Self::of(count, Reversed)
    }
}

impl<T, C> CombineFn for Top<T, C>
where
    T: DefaultCoder,
    C: Compare<T>,
{
    type Input = T;
    type Accum = Vec<T>;
    type Output = Vec<T>;

    fn create_accumulator(&self) -> crate::Result<Vec<T>> {
        Ok(Vec::new())
    }

    fn add_input(&self, mut accumulator: Vec<T>, input: T) -> crate::Result<Vec<T>> {
        accumulator.push(input);
        // Trimming at twice the count keeps the cost of each element constant on average.
        if accumulator.len() > self.count.saturating_mul(2) {
            self.trim(&mut accumulator);
        }
        Ok(accumulator)
    }

    fn merge_accumulators(&self, accumulators: Vec<Vec<T>>) -> crate::Result<Vec<T>> {
        let mut merged = accumulators.into_iter().flatten().collect();
        self.trim(&mut merged);
        Ok(merged)
    }

    fn extract_output(&self, mut accumulator: Vec<T>) -> crate::Result<Vec<T>> {
        self.trim(&mut accumulator);
        accumulator.sort_unstable_by(|a, b| self.compare.compare(b, a));
        Ok(accumulator)
    }
}
