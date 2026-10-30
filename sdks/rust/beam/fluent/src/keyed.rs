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

//! Fluent method syntax for keyed `(K, V)` [`PCollection`]s: grouping, batching and joins.

use std::collections::HashMap;
use std::hash::Hash;

use beam::coders::{BeamIterable, DefaultCoder};
use beam::transforms::{
    BroadcastInnerJoin, BroadcastLeftJoin, CoGbkResult, CoGroupByKey, FullOuterJoin, GroupByKey,
    GroupIntoBatches, InnerJoin, KeyedPCollectionTuple, LeftJoin, RightJoin,
};
use beam::values::PCollection;

use crate::collection::PCollectionExt;
use crate::composite::composite;

/// Shorthand methods for keyed collections `(K, V)`.
pub trait PCollectionKeyedExt<K: DefaultCoder, V: DefaultCoder> {
    /// Groups values sharing the same key into a [`BeamIterable<V>`].
    fn group_by_key(&self, name: impl Into<String>) -> PCollection<(K, BeamIterable<V>)>
    where
        K: Eq + Hash;

    /// Batches the values of each key and window, `batch_size` values at most per batch.
    /// See [`GroupIntoBatches`] for the byte size, buffering duration and sharded keys.
    fn group_into_batches(
        &self,
        name: impl Into<String>,
        batch_size: usize,
    ) -> PCollection<(K, Vec<V>)>
    where
        K: Clone;

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
        self.apply(name, GroupByKey::new())
    }

    fn group_into_batches(
        &self,
        name: impl Into<String>,
        batch_size: usize,
    ) -> PCollection<(K, Vec<V>)>
    where
        K: Clone,
    {
        self.apply(name, GroupIntoBatches::new(batch_size))
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
        let inputs = HashMap::from([
            ("left".to_string(), self.id().to_string()),
            ("right".to_string(), other.id().to_string()),
        ]);
        composite(self.pipeline(), &name.into(), inputs, || {
            let tuple = KeyedPCollectionTuple::of("left", self).and("right", other);
            let grouped = tuple.apply("CoGbk", CoGroupByKey::new());
            grouped.par_do_fn("ExtractPairs", |(key, result): (K, CoGbkResult), ctx| {
                // Propagate decode errors. Do not report them as an empty side, because an
                // empty side is a valid result that the caller cannot tell apart.
                ctx.emit((key, (result.get::<V>("left")?, result.get::<V2>("right")?)))
            })
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
        self.apply(name, InnerJoin::new(other))
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
        self.apply(name, LeftJoin::new(other))
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
        self.apply(name, RightJoin::new(other))
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
        self.apply(name, FullOuterJoin::new(other))
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
        self.apply(name, BroadcastInnerJoin::new(other))
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
        self.apply(name, BroadcastLeftJoin::new(other))
    }
}
