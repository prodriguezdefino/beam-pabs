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

//! Fluent method syntax for keyed `(K, V)` [`PCollection`]s.

use std::hash::Hash;

use beam::coders::{BeamIterable, DefaultCoder};
use beam::transforms::{
    BroadcastInnerJoin, BroadcastLeftJoin, CoGbkResult, CoGroupByKey, CombineFn, CombinePerKey,
    FullOuterJoin, GroupByKey, InnerJoin, KeyedPCollectionTuple, LeftJoin, RightJoin,
};
use beam::values::PCollection;

use crate::combinators::PCollectionExt;

/// Shorthand methods for keyed collections `(K, V)`.
pub trait PCollectionKeyedExt<K: DefaultCoder, V: DefaultCoder> {
    /// Groups values sharing the same key into a [`BeamIterable<V>`].
    fn group_by_key(&self, name: impl Into<String>) -> PCollection<(K, BeamIterable<V>)>
    where
        K: Eq + Hash;

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

    /// Groups this collection with another keyed collection sharing the same key.
    #[expect(
        clippy::type_complexity,
        reason = "the grouped iterable types are the public shape of this API"
    )]
    fn co_group_by_key<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (BeamIterable<V>, BeamIterable<V2>))>
    where
        K: Eq + Hash + Clone,
        V2: DefaultCoder;

    /// Inner join with another keyed collection on key `K`.
    fn inner_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (V, V2))>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        V2: DefaultCoder + Clone;

    /// Left outer join with another keyed collection on key `K`.
    fn left_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (V, Option<V2>))>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        V2: DefaultCoder + Clone;

    /// Right outer join with another keyed collection on key `K`.
    fn right_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (Option<V>, V2))>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        V2: DefaultCoder + Clone;

    /// Full outer join with another keyed collection on key `K`.
    #[expect(
        clippy::type_complexity,
        reason = "the grouped iterable types are the public shape of this API"
    )]
    fn full_outer_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (Option<V>, Option<V2>))>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        V2: DefaultCoder + Clone;

    /// Broadcast inner join with `other` as a multimap side input. `self` is not shuffled.
    fn broadcast_inner_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (V, V2))>
    where
        K: Clone + 'static,
        V: Clone + 'static,
        V2: DefaultCoder + 'static;

    /// Broadcast left outer join with `other` as a multimap side input. `self` is not
    /// shuffled.
    fn broadcast_left_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (V, Option<V2>))>
    where
        K: Clone + 'static,
        V: Clone + 'static,
        V2: DefaultCoder + 'static;
}

impl<K: DefaultCoder, V: DefaultCoder> PCollectionKeyedExt<K, V> for PCollection<(K, V)> {
    fn group_by_key(&self, name: impl Into<String>) -> PCollection<(K, BeamIterable<V>)>
    where
        K: Eq + Hash,
    {
        self.apply(GroupByKey::new(name))
    }

    fn combine_per_key<CF>(
        &self,
        name: impl Into<String>,
        combine_fn: CF,
    ) -> PCollection<(K, CF::Output)>
    where
        K: Eq + Hash,
        CF: CombineFn<Input = V>,
    {
        self.apply(CombinePerKey::new(name, combine_fn))
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
        self.combine_per_key(
            name,
            crate::combinators::FoldCombineFn::new(zero, fold_fn, merge_fn),
        )
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
        let name_str = name.into();
        let grouped = self.group_by_key(format!("{name_str}/Group"));
        grouped.par_do_fn(format!("{name_str}/Fold"), move |(key, values), ctx| {
            let accum = values
                .try_into_iter()
                .try_fold(zero.clone(), |acc, value| {
                    value
                        .map(|value| fold_fn(acc, value))
                        .map_err(|e| format!("fold_values failed to read a grouped value: {e}"))
                })?;
            ctx.emit((key, accum))
        })
    }

    fn co_group_by_key<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (BeamIterable<V>, BeamIterable<V2>))>
    where
        K: Eq + Hash + Clone,
        V2: DefaultCoder,
    {
        let name_str = name.into();
        let tuple = KeyedPCollectionTuple::of("left", self).and("right", other);

        let cogbk_name = format!("{name_str}/CoGbk");
        let grouped = tuple.apply(CoGroupByKey::new(cogbk_name));

        let extract_name = format!("{name_str}/ExtractPairs");
        grouped.par_do_fn(extract_name, |(key, result): (K, CoGbkResult), ctx| {
            // Propagate decode errors. Do not report them as an empty side, because an
            // empty side is a valid result that the caller cannot tell apart.
            ctx.emit((key, (result.get::<V>("left")?, result.get::<V2>("right")?)))
        })
    }

    fn inner_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (V, V2))>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        V2: DefaultCoder + Clone,
    {
        self.apply(InnerJoin::new(name, other))
    }

    fn left_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (V, Option<V2>))>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        V2: DefaultCoder + Clone,
    {
        self.apply(LeftJoin::new(name, other))
    }

    fn right_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (Option<V>, V2))>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        V2: DefaultCoder + Clone,
    {
        self.apply(RightJoin::new(name, other))
    }

    fn full_outer_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (Option<V>, Option<V2>))>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        V2: DefaultCoder + Clone,
    {
        self.apply(FullOuterJoin::new(name, other))
    }

    fn broadcast_inner_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (V, V2))>
    where
        K: Clone + 'static,
        V: Clone + 'static,
        V2: DefaultCoder + 'static,
    {
        self.apply(BroadcastInnerJoin::new(name, other))
    }

    fn broadcast_left_join<V2>(
        &self,
        name: impl Into<String>,
        other: &PCollection<(K, V2)>,
    ) -> PCollection<(K, (V, Option<V2>))>
    where
        K: Clone + 'static,
        V: Clone + 'static,
        V2: DefaultCoder + 'static,
    {
        self.apply(BroadcastLeftJoin::new(name, other))
    }
}
