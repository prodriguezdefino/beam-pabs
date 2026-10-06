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

//! The `ApproximateQuantiles` combine function, with the Munro-Paterson algorithm from
//! "Approximate Medians and other Quantiles in One Pass and with Limited Memory" (Manku,
//! Rajagopalan and Lindsay, 1998).

use std::cmp::Ordering;
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::{CombineFn, Compare, Natural};
use crate::coders::DefaultCoder;

/// The input size for which the default error bound holds.
const DEFAULT_MAX_INPUT_SIZE: u64 = 1_000_000_000;

/// The serialized accumulator: the minimum and the maximum, the elements that do not fill a
/// buffer, each buffer as `(level, weight, sorted elements)`, and the offset jitter.
type QuantileAccum<T> = (Option<(T, T)>, Vec<T>, Vec<(i64, i64, Vec<T>)>, i64);

/// A full buffer of sorted elements. Each element stands for `weight` input elements.
struct Buffer<T> {
    level: i64,
    weight: i64,
    elements: Vec<T>,
}

/// The working form of [`QuantileAccum`].
struct State<T> {
    bounds: Option<(T, T)>,
    unbuffered: Vec<T>,
    buffers: Vec<Buffer<T>>,
    /// Alternates the rounding of even weights, so collapses have no bias up or down.
    jitter: i64,
}

impl<T> State<T> {
    fn is_empty(&self) -> bool {
        self.unbuffered.is_empty() && self.buffers.is_empty()
    }
}

impl<T> From<QuantileAccum<T>> for State<T> {
    fn from((bounds, unbuffered, buffers, jitter): QuantileAccum<T>) -> Self {
        let buffers = buffers
            .into_iter()
            .map(|(level, weight, elements)| Buffer {
                level,
                weight,
                elements,
            })
            .collect();
        Self {
            bounds,
            unbuffered,
            buffers,
            jitter,
        }
    }
}

impl<T> From<State<T>> for QuantileAccum<T> {
    fn from(state: State<T>) -> Self {
        let buffers = state
            .buffers
            .into_iter()
            .map(|buffer| (buffer.level, buffer.weight, buffer.elements))
            .collect();
        (state.bounds, state.unbuffered, buffers, state.jitter)
    }
}

/// The number of buffers and their size for an error bound of `epsilon` over at most
/// `max_input_size` elements: the smallest memory that satisfies the two constraints of the
/// paper. The values match the reference table of the paper.
fn buffer_layout(epsilon: f64, max_input_size: u64) -> (usize, usize) {
    let target = epsilon * max_input_size as f64;
    let first_over = (3..63u32)
        .find(|&b| f64::from(b - 2) * 2f64.powi(b as i32 - 2) >= target)
        .unwrap_or(63);
    let num_buffers = first_over - 1;
    // The paper rounds with single precision, and the reference table depends on it.
    let buffer_size = (max_input_size as f32 / 2f32.powi(num_buffers as i32 - 1)).ceil() as usize;
    (num_buffers as usize, buffer_size.max(2))
}

/// Computes `num_quantiles` approximate quantiles: the minimum, `num_quantiles - 2` evenly
/// spaced elements and the maximum, in the order of `compare`.
///
/// For a sorted input `x_1, .., x_N`, each approximate quantile `x_c'` of an exact quantile
/// `x_c` satisfies `|c - c'| < epsilon * N`, as long as `N` is at most the maximum input size.
/// The defaults are an `epsilon` of `1 / num_quantiles` and 10^9 elements. The result is
/// deterministic, and the memory is `O((1 / epsilon) * log^2(epsilon * N))` elements.
///
/// [`new`](ApproximateQuantiles::new) uses the named ordering [`Natural`], so the SDK
/// registers it for the standard element types; [`Reversed`](super::Reversed) is the
/// descending order. An `ApproximateQuantiles` with a closure ordering uses the rebuild path.
///
/// ```
/// use beam::transforms::ApproximateQuantiles;
///
/// // Quartiles: the minimum, the three quartiles and the maximum.
/// let quartiles = ApproximateQuantiles::<i64, _>::new(5)?;
/// // Deciles of the string lengths, with an error bound of 1%.
/// let deciles = ApproximateQuantiles::of(11, |a: &String, b: &String| a.len().cmp(&b.len()))?
///     .with_epsilon(0.01)?;
/// # let _ = (quartiles, deciles);
/// # Ok::<(), beam::Error>(())
/// ```
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "C: Serialize", deserialize = "C: Deserialize<'de>"))]
pub struct ApproximateQuantiles<T, C> {
    num_quantiles: usize,
    epsilon: f64,
    max_input_size: u64,
    num_buffers: usize,
    buffer_size: usize,
    compare: C,
    #[serde(skip)]
    _marker: PhantomData<fn() -> T>,
}

impl<T: Ord> ApproximateQuantiles<T, Natural> {
    /// Computes `num_quantiles` quantiles in the natural order of `T`.
    ///
    /// # Errors
    ///
    /// Returns an error if `num_quantiles` is less than 2.
    pub fn new(num_quantiles: usize) -> crate::Result<Self> {
        Self::of(num_quantiles, Natural)
    }
}

impl<T, C> ApproximateQuantiles<T, C>
where
    C: Compare<T>,
{
    /// Computes `num_quantiles` quantiles in the order of `compare`.
    ///
    /// # Errors
    ///
    /// Returns an error if `num_quantiles` is less than 2.
    pub fn of(num_quantiles: usize, compare: C) -> crate::Result<Self> {
        if num_quantiles < 2 {
            return Err(crate::Error::msg(format!(
                "ApproximateQuantiles needs at least 2 quantiles (the minimum and the maximum), \
                 got {num_quantiles}"
            )));
        }
        Self::sized(
            num_quantiles,
            1.0 / num_quantiles as f64,
            DEFAULT_MAX_INPUT_SIZE,
            compare,
        )
    }

    /// Sets the error bound, as a fraction of the input size.
    ///
    /// # Errors
    ///
    /// Returns an error if `epsilon` is not greater than 0 and at most 1.
    pub fn with_epsilon(self, epsilon: f64) -> crate::Result<Self> {
        Self::sized(
            self.num_quantiles,
            epsilon,
            self.max_input_size,
            self.compare,
        )
    }

    /// Sets the input size for which the error bound holds. A larger input gives a larger
    /// error.
    ///
    /// # Errors
    ///
    /// Returns an error if `max_input_size` is 0.
    pub fn with_max_input_size(self, max_input_size: u64) -> crate::Result<Self> {
        Self::sized(
            self.num_quantiles,
            self.epsilon,
            max_input_size,
            self.compare,
        )
    }

    /// The number of buffers and their size.
    #[doc(hidden)]
    pub fn buffer_layout(&self) -> (usize, usize) {
        (self.num_buffers, self.buffer_size)
    }

    fn sized(
        num_quantiles: usize,
        epsilon: f64,
        max_input_size: u64,
        compare: C,
    ) -> crate::Result<Self> {
        if !(epsilon > 0.0 && epsilon <= 1.0) {
            return Err(crate::Error::msg(format!(
                "ApproximateQuantiles needs an epsilon greater than 0 and at most 1, got {epsilon}"
            )));
        }
        if max_input_size == 0 {
            return Err(crate::Error::msg(
                "ApproximateQuantiles needs a maximum input size greater than 0",
            ));
        }
        let (num_buffers, buffer_size) = buffer_layout(epsilon, max_input_size);
        Ok(Self {
            num_quantiles,
            epsilon,
            max_input_size,
            num_buffers,
            buffer_size,
            compare,
            _marker: PhantomData,
        })
    }
}

impl<T: Clone, C: Compare<T>> ApproximateQuantiles<T, C> {
    /// Takes `count` elements from the sorted union of `buffers`, where each element repeats
    /// as many times as the weight of its buffer: the elements at `j * step + offset` for
    /// `0 <= j < count`.
    fn interpolate(&self, buffers: &[Buffer<T>], count: usize, step: f64, offset: f64) -> Vec<T> {
        let mut weighted: Vec<(&T, i64)> = buffers
            .iter()
            .flat_map(|buffer| buffer.elements.iter().map(move |e| (e, buffer.weight)))
            .collect();
        weighted.sort_by(|(a, _), (b, _)| self.compare.compare(a, b));

        let mut sorted = weighted.into_iter();
        let Some((mut value, weight)) = sorted.next() else {
            return Vec::new();
        };
        let mut position = weight as f64;
        (0..count)
            .map(|j| {
                let target = j as f64 * step + offset;
                while position <= target {
                    let Some((next, weight)) = sorted.next() else {
                        break;
                    };
                    value = next;
                    position += weight as f64;
                }
                value.clone()
            })
            .collect()
    }

    /// The offset of a collapse of total weight `weight`. An even weight rounds up and down
    /// in turn.
    fn offset(jitter: &mut i64, weight: i64) -> i64 {
        match weight % 2 {
            1 => (weight + 1) / 2,
            _ => {
                *jitter = 2 - *jitter;
                (weight + *jitter) / 2
            }
        }
    }

    /// Merges `buffers` into one buffer of the next level.
    fn collapse(&self, jitter: &mut i64, buffers: &[Buffer<T>]) -> Buffer<T> {
        // Buffers from different workers can have different levels: take the largest.
        let level = buffers.iter().map(|b| b.level + 1).max().unwrap_or(1);
        let weight = buffers.iter().map(|b| b.weight).sum();
        let offset = Self::offset(jitter, weight);
        Buffer {
            level,
            weight,
            elements: self.interpolate(buffers, self.buffer_size, weight as f64, offset as f64),
        }
    }

    /// Collapses the buffers of the lowest levels until at most `num_buffers` remain.
    fn collapse_if_needed(&self, state: &mut State<T>) {
        while state.buffers.len() > self.num_buffers {
            state.buffers.sort_by_key(|buffer| buffer.level);
            let Some(second_level) = state.buffers.get(1).map(|b| b.level) else {
                break;
            };
            let count = 2 + state.buffers[2..]
                .iter()
                .take_while(|buffer| buffer.level == second_level)
                .count();
            let to_collapse: Vec<Buffer<T>> = state.buffers.drain(..count).collect();
            let collapsed = self.collapse(&mut state.jitter, &to_collapse);
            state.buffers.push(collapsed);
        }
    }

    /// Adds an element to the unbuffered elements, and makes a buffer of them when they fill
    /// one.
    fn add_unbuffered(&self, state: &mut State<T>, element: T) {
        state.unbuffered.push(element);
        if state.unbuffered.len() >= self.buffer_size {
            let mut elements = std::mem::take(&mut state.unbuffered);
            elements.sort_by(|a, b| self.compare.compare(a, b));
            state.buffers.push(Buffer {
                level: 0,
                weight: 1,
                elements,
            });
            self.collapse_if_needed(state);
        }
    }

    /// Widens `bounds` to include `low` and `high`.
    fn widen(&self, bounds: Option<(T, T)>, low: &T, high: &T) -> (T, T) {
        match bounds {
            None => (low.clone(), high.clone()),
            Some((min, max)) => {
                let min = match self.compare.compare(low, &min) {
                    Ordering::Less => low.clone(),
                    Ordering::Equal | Ordering::Greater => min,
                };
                let max = match self.compare.compare(high, &max) {
                    Ordering::Greater => high.clone(),
                    Ordering::Equal | Ordering::Less => max,
                };
                (min, max)
            }
        }
    }

    /// Adds all elements that `other` summarizes to `state`.
    fn merge_into(&self, mut state: State<T>, other: State<T>) -> State<T> {
        let Some((low, high)) = other.bounds.as_ref() else {
            return state;
        };
        state.bounds = Some(self.widen(state.bounds.take(), low, high));
        other
            .unbuffered
            .into_iter()
            .for_each(|element| self.add_unbuffered(&mut state, element));
        state.buffers.extend(other.buffers);
        self.collapse_if_needed(&mut state);
        state
    }
}

impl<T, C> CombineFn for ApproximateQuantiles<T, C>
where
    T: DefaultCoder + Clone,
    C: Compare<T>,
{
    type Input = T;
    type Accum = QuantileAccum<T>;
    type Output = Vec<T>;

    fn create_accumulator(&self) -> crate::Result<Self::Accum> {
        Ok((None, Vec::new(), Vec::new(), 0))
    }

    fn add_input(&self, accumulator: Self::Accum, input: T) -> crate::Result<Self::Accum> {
        let mut state = State::from(accumulator);
        state.bounds = Some(self.widen(state.bounds.take(), &input, &input));
        self.add_unbuffered(&mut state, input);
        Ok(state.into())
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> crate::Result<Self::Accum> {
        let empty = State::from(self.create_accumulator()?);
        let merged = accumulators
            .into_iter()
            .map(State::from)
            .fold(empty, |state, other| self.merge_into(state, other));
        Ok(merged.into())
    }

    fn extract_output(&self, accumulator: Self::Accum) -> crate::Result<Vec<T>> {
        let mut state = State::from(accumulator);
        let Some((min, max)) = state.bounds.take() else {
            return Ok(Vec::new());
        };
        if state.is_empty() {
            return Ok(Vec::new());
        }
        let buffered: i64 = state
            .buffers
            .iter()
            .map(|buffer| self.buffer_size as i64 * buffer.weight)
            .sum();
        let total = (state.unbuffered.len() as i64 + buffered) as f64;

        let mut unbuffered = std::mem::take(&mut state.unbuffered);
        unbuffered.sort_by(|a, b| self.compare.compare(a, b));
        state.buffers.push(Buffer {
            level: 0,
            weight: 1,
            elements: unbuffered,
        });

        let intervals = (self.num_quantiles - 1) as f64;
        let inner = self.interpolate(
            &state.buffers,
            self.num_quantiles - 2,
            total / intervals,
            (total - 1.0) / intervals,
        );
        Ok(std::iter::once(min)
            .chain(inner)
            .chain(std::iter::once(max))
            .collect())
    }
}
