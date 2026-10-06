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

//! Associative aggregation with partial combining.

use std::sync::Arc;

use crate::coders::DefaultCoder;

mod approximate_quantiles;
mod approximate_unique;
mod builtins;
mod compare;
mod compose;
mod globally;
mod partial;
mod per_key;
pub(crate) mod registered;
mod registrations;
mod sample;
mod state;
mod table;
mod top;

pub use approximate_quantiles::ApproximateQuantiles;
pub use approximate_unique::ApproximateUnique;
pub use builtins::{Max, Mean, Min, Numeric, Sum};
pub use compare::{Compare, Natural, Reversed};
pub use compose::{Compose, ComposePart, MapOutput, On};
pub use globally::CombineGlobally;
pub use per_key::CombinePerKey;
pub use sample::{AnySample, Sample};
pub use state::CombiningState;
use table::AccumulatorTable;
pub use top::Top;

/// An associative, commutative aggregation over the values of a key.
///
/// The intermediate *accumulator* lets a runner aggregate on each worker and shuffle only
/// the accumulators. The SDK cannot check these two properties, but relies on them:
///
/// - **Associativity.** `merge_accumulators` must give the same result for any grouping
///   of the same accumulators. Bundle boundaries change between runs.
/// - **Commutativity.** Elements arrive in no specified order.
///
/// Before implementing the trait, check the built-in functions: [`Sum`], [`Max`], [`Min`] and
/// [`Mean`] for numbers, [`Top`], [`Sample`] and [`AnySample`] for any type. To compute
/// several of them in one pass, use [`Compose`]. This example keeps the longest word, and
/// the first in alphabetical order on a tie, so the order of the elements does not change
/// the result:
///
/// ```
/// use beam::transforms::CombineFn;
///
/// struct LongestWord;
///
/// fn longer(left: String, right: String) -> String {
///     match left.len().cmp(&right.len()).then_with(|| right.cmp(&left)) {
///         std::cmp::Ordering::Less => right,
///         _ => left,
///     }
/// }
///
/// impl CombineFn for LongestWord {
///     type Input = String;
///     type Accum = String;
///     type Output = String;
///
///     fn create_accumulator(&self) -> beam::Result<String> {
///         Ok(String::new())
///     }
///
///     fn add_input(&self, acc: String, word: String) -> beam::Result<String> {
///         Ok(longer(acc, word))
///     }
///
///     fn merge_accumulators(&self, accs: Vec<String>) -> beam::Result<String> {
///         Ok(accs.into_iter().fold(String::new(), longer))
///     }
///
///     fn extract_output(&self, acc: String) -> beam::Result<String> {
///         Ok(acc)
///     }
/// }
/// ```
pub trait CombineFn: Send + Sync + 'static {
    /// The type of the values to aggregate.
    type Input: DefaultCoder;
    /// The intermediate state between elements and across workers.
    type Accum: DefaultCoder;
    /// The final result for a key.
    type Output: DefaultCoder;

    /// Produces the identity accumulator.
    fn create_accumulator(&self) -> crate::Result<Self::Accum>;

    /// Folds one input value into an accumulator.
    ///
    /// An error fails the bundle, and the runner retries it.
    fn add_input(&self, accumulator: Self::Accum, input: Self::Input)
    -> crate::Result<Self::Accum>;

    /// Merges accumulators built independently, usually on different workers.
    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> crate::Result<Self::Accum>;

    /// Converts the fully merged accumulator into the output value for the key.
    fn extract_output(&self, accumulator: Self::Accum) -> crate::Result<Self::Output>;

    /// Combines elements of type `I` by giving `extract(&element)` to this function. Use it
    /// to aggregate one field of a record, or as a part of [`Compose`].
    fn on<I, F>(self, extract: F) -> On<Self, I, F>
    where
        Self: Sized,
        F: Fn(&I) -> Self::Input,
    {
        On::new(self, extract)
    }

    /// Converts the output of this function with `map`, for example from a tuple of
    /// [`Compose`] results to a record.
    fn map_output<O, F>(self, map: F) -> MapOutput<Self, F>
    where
        Self: Sized,
        F: Fn(Self::Output) -> O,
    {
        MapOutput::new(self, map)
    }
}

impl<CF: CombineFn> CombineFn for Arc<CF> {
    type Input = CF::Input;
    type Accum = CF::Accum;
    type Output = CF::Output;

    fn create_accumulator(&self) -> crate::Result<Self::Accum> {
        (**self).create_accumulator()
    }

    fn add_input(
        &self,
        accumulator: Self::Accum,
        input: Self::Input,
    ) -> crate::Result<Self::Accum> {
        (**self).add_input(accumulator, input)
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> crate::Result<Self::Accum> {
        (**self).merge_accumulators(accumulators)
    }

    fn extract_output(&self, accumulator: Self::Accum) -> crate::Result<Self::Output> {
        (**self).extract_output(accumulator)
    }
}

/// Access to the accumulator table of the [`CombinePerKey`] partial combine.
pub(crate) mod combine_internals {
    use std::hash::Hash;

    use super::{AccumulatorTable, CombineFn};
    use crate::coders::{DefaultCoder, WindowedHeader};

    /// The accumulator table of one bundle, keyed by window and then by key.
    pub struct Table<K, A>(AccumulatorTable<K, A>);

    impl<K, A> Default for Table<K, A> {
        fn default() -> Self {
            Self(AccumulatorTable::default())
        }
    }

    impl<K, A> Table<K, A>
    where
        K: DefaultCoder + Eq + Hash,
        A: DefaultCoder,
    {
        /// Adds `value` to the accumulator for `key` in `window`, creating it if needed.
        pub fn add<CF: CombineFn<Accum = A>>(
            &mut self,
            combine_fn: &CF,
            window: &[u8],
            header: &WindowedHeader,
            key: K,
            value: CF::Input,
            timestamp: i64,
        ) -> crate::Result {
            self.0
                .add(combine_fn, window, header, key, value, timestamp)
        }

        /// Removes the least recently used tenth of the entries and gives each to `emit`.
        pub fn evict_coldest(
            &mut self,
            mut emit: impl FnMut(K, Option<A>) -> crate::Result,
        ) -> crate::Result {
            self.0
                .evict_coldest(|_, key, entry| emit(key, entry.accumulator))
        }

        /// Empties the table and gives each entry to `emit`.
        pub fn drain(self, mut emit: impl FnMut(K, Option<A>) -> crate::Result) -> crate::Result {
            self.0.drain(|_, key, entry| emit(key, entry.accumulator))
        }

        /// Returns the number of entries in the table.
        pub fn len(&self) -> usize {
            self.0.len
        }

        /// Returns true if the table contains no entries.
        pub fn is_empty(&self) -> bool {
            self.0.len == 0
        }

        /// Returns the estimated memory of the entries.
        pub fn estimated_bytes(&self) -> usize {
            self.0.estimated_bytes()
        }

        /// Returns the current estimate of the memory of one entry.
        pub fn entry_bytes(&self) -> usize {
            self.0.weigher.entry_bytes()
        }
    }
}
