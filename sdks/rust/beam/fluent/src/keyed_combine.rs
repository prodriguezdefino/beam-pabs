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

//! Fluent method syntax for combines per key.

use std::collections::HashMap;
use std::hash::Hash;

use beam::coders::DefaultCoder;
use beam::transforms::{
    ApproximateQuantiles, ApproximateUnique, CombineFn, CombinePerKey, Max, Mean, Min, Natural,
    Reversed, Sample, Sum, Top,
};
use beam::values::PCollection;

use crate::collection::PCollectionExt;
use crate::composite::composite;
use crate::fold::FoldCombineFn;
use crate::keyed::PCollectionKeyedExt;

/// Shorthand methods that combine the values of each key.
pub trait KeyedCombineExt<K: DefaultCoder, V: DefaultCoder> {
    /// Combines values per key using an associative and commutative [`CombineFn`].
    fn combine_per_key<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<(K, CF::Output)>
    where
        K: Eq + Hash,
        CF: CombineFn<Input = V>;

    /// Combines values per key with an associative fold and combiner lifting. `merge_fn`
    /// must be associative: it merges partial accumulators across workers and bundles.
    fn fold_per_key<Accum, Fold, Merge>(
        &self,
        name: impl Into<String>,
        zero: Accum,
        fold_fn: Fold,
        merge_fn: Merge,
    ) -> PCollection<(K, Accum)>
    where
        K: Eq + Hash,
        Accum: DefaultCoder + Clone + Send + Sync + 'static,
        Fold: Fn(Accum, V) -> Accum + Send + Sync + 'static,
        Merge: Fn(Accum, Accum) -> Accum + Send + Sync + 'static;

    /// Groups values per key with [`GroupByKey`] and folds them in sequence with `fold_fn`.
    /// There is no merge function, so no combiner lifting: all elements cross the shuffle.
    fn fold_values<Accum, Fold>(
        &self,
        name: impl Into<String>,
        zero: Accum,
        fold_fn: Fold,
    ) -> PCollection<(K, Accum)>
    where
        K: Eq + Hash + Clone,
        Accum: DefaultCoder + Clone,
        Fold: Fn(Accum, V) -> Accum + Send + Sync + 'static;

    /// The sum of the values of each key. See [`Sum`].
    fn sum_per_key(&self, name: impl Into<String>) -> PCollection<(K, V)>
    where
        K: Eq + Hash,
        Sum<V>: CombineFn<Input = V, Output = V>;

    /// The largest value of each key. See [`Max`].
    fn max_per_key(&self, name: impl Into<String>) -> PCollection<(K, V)>
    where
        K: Eq + Hash,
        Max<V>: CombineFn<Input = V, Output = V>;

    /// The smallest value of each key. See [`Min`].
    fn min_per_key(&self, name: impl Into<String>) -> PCollection<(K, V)>
    where
        K: Eq + Hash,
        Min<V>: CombineFn<Input = V, Output = V>;

    /// The arithmetic mean of the values of each key. See [`Mean`].
    fn mean_per_key(
        &self,
        name: impl Into<String>,
    ) -> PCollection<(K, <Mean<V> as CombineFn>::Output)>
    where
        K: Eq + Hash,
        Mean<V>: CombineFn<Input = V>;

    /// The `count` largest values of each key, largest first. See [`Top::largest`].
    fn top_per_key(&self, name: impl Into<String>, count: usize) -> PCollection<(K, Vec<V>)>
    where
        K: Eq + Hash,
        V: Ord;

    /// The `count` smallest values of each key, smallest first. See [`Top::smallest`].
    fn smallest_per_key(&self, name: impl Into<String>, count: usize) -> PCollection<(K, Vec<V>)>
    where
        K: Eq + Hash,
        V: Ord;

    /// A uniform random sample of `count` values of each key. See [`Sample`].
    fn sample_per_key(&self, name: impl Into<String>, count: usize) -> PCollection<(K, Vec<V>)>
    where
        K: Eq + Hash;

    /// An estimate of the number of distinct values of each key. See [`ApproximateUnique`].
    ///
    /// # Errors
    ///
    /// Returns the error of [`ApproximateUnique::with_sample_size`] for a sample size that
    /// is too small.
    fn approximate_unique_per_key(
        &self,
        name: impl Into<String>,
        sample_size: usize,
    ) -> beam::Result<PCollection<(K, i64)>>
    where
        K: Eq + Hash;

    /// `num_quantiles` approximate quantiles of the values of each key. See
    /// [`ApproximateQuantiles`].
    ///
    /// # Errors
    ///
    /// Returns the error of [`ApproximateQuantiles::new`] if `num_quantiles` is less than 2.
    fn approximate_quantiles_per_key(
        &self,
        name: impl Into<String>,
        num_quantiles: usize,
    ) -> beam::Result<PCollection<(K, Vec<V>)>>
    where
        K: Eq + Hash,
        V: Ord + Clone;
}

impl<K: DefaultCoder, V: DefaultCoder> KeyedCombineExt<K, V> for PCollection<(K, V)> {
    fn combine_per_key<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<(K, CF::Output)>
    where
        K: Eq + Hash,
        CF: CombineFn<Input = V>,
    {
        self.apply(name, CombinePerKey::new(combine_fn))
    }

    fn fold_per_key<Accum, Fold, Merge>(
        &self,
        name: impl Into<String>,
        zero: Accum,
        fold_fn: Fold,
        merge_fn: Merge,
    ) -> PCollection<(K, Accum)>
    where
        K: Eq + Hash,
        Accum: DefaultCoder + Clone + Send + Sync + 'static,
        Fold: Fn(Accum, V) -> Accum + Send + Sync + 'static,
        Merge: Fn(Accum, Accum) -> Accum + Send + Sync + 'static,
    {
        self.combine_per_key(name, FoldCombineFn::new(zero, fold_fn, merge_fn))
    }

    fn fold_values<Accum, Fold>(
        &self,
        name: impl Into<String>,
        zero: Accum,
        fold_fn: Fold,
    ) -> PCollection<(K, Accum)>
    where
        K: Eq + Hash + Clone,
        Accum: DefaultCoder + Clone,
        Fold: Fn(Accum, V) -> Accum + Send + Sync + 'static,
    {
        let inputs = HashMap::from([("in".to_string(), self.id().to_string())]);
        composite(self.pipeline(), &name.into(), inputs, || {
            let grouped = self.group_by_key("Group");
            grouped.par_do_fn("Fold", move |(key, values), ctx| {
                let accum = values
                    .try_into_iter()
                    .try_fold(zero.clone(), |acc, value| {
                        value
                            .map(|value| fold_fn(acc, value))
                            .map_err(|e| format!("fold_values failed to read a grouped value: {e}"))
                    })?;
                ctx.emit((key, accum))
            })
        })
    }

    fn sum_per_key(&self, name: impl Into<String>) -> PCollection<(K, V)>
    where
        K: Eq + Hash,
        Sum<V>: CombineFn<Input = V, Output = V>,
    {
        self.combine_per_key(name, Sum::new())
    }

    fn max_per_key(&self, name: impl Into<String>) -> PCollection<(K, V)>
    where
        K: Eq + Hash,
        Max<V>: CombineFn<Input = V, Output = V>,
    {
        self.combine_per_key(name, Max::new())
    }

    fn min_per_key(&self, name: impl Into<String>) -> PCollection<(K, V)>
    where
        K: Eq + Hash,
        Min<V>: CombineFn<Input = V, Output = V>,
    {
        self.combine_per_key(name, Min::new())
    }

    fn mean_per_key(
        &self,
        name: impl Into<String>,
    ) -> PCollection<(K, <Mean<V> as CombineFn>::Output)>
    where
        K: Eq + Hash,
        Mean<V>: CombineFn<Input = V>,
    {
        self.combine_per_key(name, Mean::new())
    }

    fn top_per_key(&self, name: impl Into<String>, count: usize) -> PCollection<(K, Vec<V>)>
    where
        K: Eq + Hash,
        V: Ord,
    {
        self.combine_per_key(name, Top::<V, Natural>::largest(count))
    }

    fn smallest_per_key(&self, name: impl Into<String>, count: usize) -> PCollection<(K, Vec<V>)>
    where
        K: Eq + Hash,
        V: Ord,
    {
        self.combine_per_key(name, Top::<V, Reversed>::smallest(count))
    }

    fn sample_per_key(&self, name: impl Into<String>, count: usize) -> PCollection<(K, Vec<V>)>
    where
        K: Eq + Hash,
    {
        self.combine_per_key(name, Sample::new(count))
    }

    fn approximate_unique_per_key(
        &self,
        name: impl Into<String>,
        sample_size: usize,
    ) -> beam::Result<PCollection<(K, i64)>>
    where
        K: Eq + Hash,
    {
        Ok(self.combine_per_key(name, ApproximateUnique::with_sample_size(sample_size)?))
    }

    fn approximate_quantiles_per_key(
        &self,
        name: impl Into<String>,
        num_quantiles: usize,
    ) -> beam::Result<PCollection<(K, Vec<V>)>>
    where
        K: Eq + Hash,
        V: Ord + Clone,
    {
        Ok(self.combine_per_key(name, ApproximateQuantiles::new(num_quantiles)?))
    }
}
