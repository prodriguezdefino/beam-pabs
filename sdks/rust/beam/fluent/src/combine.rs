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

//! Fluent method syntax for global combines and counts.
//!
//! The shortcuts (`sum_globally`, `top_globally` and so on) apply `CombineGlobally` with one
//! SDK combine function. Like `combine_globally`, they give the identity value for an empty
//! input, so the input must be bounded and globally windowed; for other input, use
//! `combine_globally_without_defaults` with the combine function.

use std::hash::Hash;

use beam::coders::DefaultCoder;
use beam::transforms::{
    ApproximateQuantiles, ApproximateUnique, CombineFn, CombineGlobally, CountGlobally,
    CountPerElement, Max, Mean, Min, Natural, Reversed, Sample, Sum, Top,
};
use beam::values::PCollection;

use crate::fold::FoldCombineFn;

/// Shorthand methods that combine all elements of a collection.
pub trait CombineExt<T: DefaultCoder> {
    /// Counts occurrences of each distinct element.
    fn count_per_element(&self, name: impl Into<String>) -> PCollection<(T, i64)>
    where
        T: Eq + Hash;

    /// Combines all elements globally using an associative [`CombineFn`].
    ///
    /// An empty input gives the identity value. The input must be bounded and globally
    /// windowed, or expansion panics. For other input, use
    /// [`combine_globally_without_defaults`](Self::combine_globally_without_defaults).
    fn combine_globally<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<CF::Output>
    where
        CF: CombineFn<Input = T>;

    /// Combines elements globally without inserting a default value for empty inputs.
    ///
    /// The input can be unbounded and can have any windowing, for example fixed or sliding
    /// windows.
    fn combine_globally_without_defaults<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<CF::Output>
    where
        CF: CombineFn<Input = T>;

    /// Combines elements globally using an associative fold, with partial combining and
    /// combiner lifting.
    ///
    /// An empty input gives `zero`. The input must be bounded and globally windowed, or
    /// expansion panics. For other input, apply [`CombineGlobally`] with
    /// [`without_defaults`](CombineGlobally::without_defaults).
    fn fold_globally<Accum, Fold, Merge>(
        &self,
        name: impl Into<String>,
        zero: Accum,
        fold_fn: Fold,
        merge_fn: Merge,
    ) -> PCollection<Accum>
    where
        Accum: DefaultCoder + Clone + Send + Sync + 'static,
        Fold: Fn(Accum, T) -> Accum + Send + Sync + 'static,
        Merge: Fn(Accum, Accum) -> Accum + Send + Sync + 'static;

    /// Counts the total number of elements in the collection globally.
    ///
    /// An empty input gives zero. The input must be bounded and globally windowed, or
    /// expansion panics. For other input, apply [`CountGlobally`] with
    /// [`without_defaults`](CountGlobally::without_defaults).
    fn count_globally(&self, name: impl Into<String>) -> PCollection<i64>;

    /// The sum of the elements. See [`Sum`].
    fn sum_globally(&self, name: impl Into<String>) -> PCollection<T>
    where
        Sum<T>: CombineFn<Input = T, Output = T>;

    /// The largest element. See [`Max`].
    fn max_globally(&self, name: impl Into<String>) -> PCollection<T>
    where
        Max<T>: CombineFn<Input = T, Output = T>;

    /// The smallest element. See [`Min`].
    fn min_globally(&self, name: impl Into<String>) -> PCollection<T>
    where
        Min<T>: CombineFn<Input = T, Output = T>;

    /// The arithmetic mean: an `f64`, or a `Decimal` for `Decimal` elements. See [`Mean`].
    fn mean_globally(&self, name: impl Into<String>) -> PCollection<<Mean<T> as CombineFn>::Output>
    where
        Mean<T>: CombineFn<Input = T>;

    /// The `count` largest elements, largest first. See [`Top::largest`].
    fn top_globally(&self, name: impl Into<String>, count: usize) -> PCollection<Vec<T>>
    where
        T: Ord;

    /// The `count` smallest elements, smallest first. See [`Top::smallest`].
    fn smallest_globally(&self, name: impl Into<String>, count: usize) -> PCollection<Vec<T>>
    where
        T: Ord;

    /// A uniform random sample of `count` elements. See [`Sample`].
    fn sample_globally(&self, name: impl Into<String>, count: usize) -> PCollection<Vec<T>>;

    /// An estimate of the number of distinct elements, from a sample of `sample_size`
    /// hashes. See [`ApproximateUnique`].
    ///
    /// # Errors
    ///
    /// Returns the error of [`ApproximateUnique::with_sample_size`] for a sample size that
    /// is too small.
    fn approximate_unique_globally(
        &self,
        name: impl Into<String>,
        sample_size: usize,
    ) -> beam::Result<PCollection<i64>>;

    /// `num_quantiles` approximate quantiles in the natural order of `T`, from the minimum
    /// to the maximum. See [`ApproximateQuantiles`].
    ///
    /// # Errors
    ///
    /// Returns the error of [`ApproximateQuantiles::new`] if `num_quantiles` is less than 2.
    fn approximate_quantiles_globally(
        &self,
        name: impl Into<String>,
        num_quantiles: usize,
    ) -> beam::Result<PCollection<Vec<T>>>
    where
        T: Ord + Clone;
}

impl<T: DefaultCoder> CombineExt<T> for PCollection<T> {
    fn count_per_element(&self, name: impl Into<String>) -> PCollection<(T, i64)>
    where
        T: Eq + Hash,
    {
        self.apply(name, CountPerElement::new())
    }

    fn combine_globally<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<CF::Output>
    where
        CF: CombineFn<Input = T>,
    {
        self.apply(name, CombineGlobally::new(combine_fn))
    }

    fn combine_globally_without_defaults<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<CF::Output>
    where
        CF: CombineFn<Input = T>,
    {
        self.apply(name, CombineGlobally::new(combine_fn).without_defaults())
    }

    fn fold_globally<Accum, Fold, Merge>(
        &self,
        name: impl Into<String>,
        zero: Accum,
        fold_fn: Fold,
        merge_fn: Merge,
    ) -> PCollection<Accum>
    where
        Accum: DefaultCoder + Clone + Send + Sync + 'static,
        Fold: Fn(Accum, T) -> Accum + Send + Sync + 'static,
        Merge: Fn(Accum, Accum) -> Accum + Send + Sync + 'static,
    {
        self.combine_globally(name, FoldCombineFn::new(zero, fold_fn, merge_fn))
    }

    fn count_globally(&self, name: impl Into<String>) -> PCollection<i64> {
        self.apply(name, CountGlobally::new())
    }

    fn sum_globally(&self, name: impl Into<String>) -> PCollection<T>
    where
        Sum<T>: CombineFn<Input = T, Output = T>,
    {
        self.combine_globally(name, Sum::new())
    }

    fn max_globally(&self, name: impl Into<String>) -> PCollection<T>
    where
        Max<T>: CombineFn<Input = T, Output = T>,
    {
        self.combine_globally(name, Max::new())
    }

    fn min_globally(&self, name: impl Into<String>) -> PCollection<T>
    where
        Min<T>: CombineFn<Input = T, Output = T>,
    {
        self.combine_globally(name, Min::new())
    }

    fn mean_globally(&self, name: impl Into<String>) -> PCollection<<Mean<T> as CombineFn>::Output>
    where
        Mean<T>: CombineFn<Input = T>,
    {
        self.combine_globally(name, Mean::new())
    }

    fn top_globally(&self, name: impl Into<String>, count: usize) -> PCollection<Vec<T>>
    where
        T: Ord,
    {
        self.combine_globally(name, Top::<T, Natural>::largest(count))
    }

    fn smallest_globally(&self, name: impl Into<String>, count: usize) -> PCollection<Vec<T>>
    where
        T: Ord,
    {
        self.combine_globally(name, Top::<T, Reversed>::smallest(count))
    }

    fn sample_globally(&self, name: impl Into<String>, count: usize) -> PCollection<Vec<T>> {
        self.combine_globally(name, Sample::new(count))
    }

    fn approximate_unique_globally(
        &self,
        name: impl Into<String>,
        sample_size: usize,
    ) -> beam::Result<PCollection<i64>> {
        Ok(self.combine_globally(name, ApproximateUnique::with_sample_size(sample_size)?))
    }

    fn approximate_quantiles_globally(
        &self,
        name: impl Into<String>,
        num_quantiles: usize,
    ) -> beam::Result<PCollection<Vec<T>>>
    where
        T: Ord + Clone,
    {
        Ok(self.combine_globally(name, ApproximateQuantiles::new(num_quantiles)?))
    }
}
