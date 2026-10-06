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

//! Relational joins built on [`CoGroupByKey`] or on side input views.

use std::hash::Hash;
use std::marker::PhantomData;

use super::co_group_by_key::{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple};
use super::composite::expand_as;
use super::{ClosureFn, DisplayDataBuilder, HasDisplayData, PTransform, ParDo};
use crate::coders::DefaultCoder;
use crate::values::PCollection;

/// Pairs each `left` value with each `right` value under a shared `key`.
///
/// The Cartesian product must duplicate the key and values for each output pair.
fn cross_product<'a, K, A, B, O, C>(
    key: K,
    left: &'a [A],
    right: &'a [B],
    combine: C,
) -> impl Iterator<Item = (K, O)> + 'a
where
    K: Clone + 'a,
    A: 'a,
    B: 'a,
    C: Fn(&'a A, &'a B) -> O + Clone + 'a,
{
    left.iter().flat_map(move |l| {
        let key = key.clone();
        let combine = combine.clone();
        right.iter().map(move |r| (key.clone(), combine(l, r)))
    })
}

/// Attaches `key` to each value of a side that has no match on the other side.
fn unmatched<K, V, O>(key: K, values: Vec<V>, pad: impl Fn(V) -> O) -> impl Iterator<Item = (K, O)>
where
    K: Clone,
{
    values.into_iter().map(move |v| (key.clone(), pad(v)))
}

/// Materializes both sides of a two-way join from a [`CoGbkResult`]. Return decode errors and
/// do not treat a failed side as empty: an empty side turns a matched key into an unmatched
/// key or drops the row.
fn join_sides<V1, V2>(res: &CoGbkResult) -> crate::Result<(Vec<V1>, Vec<V2>)>
where
    V1: DefaultCoder,
    V2: DefaultCoder,
{
    let v1s = res
        .get_by_index::<V1>(0)?
        .into_vec()
        .map_err(|e| e.to_string())?;
    let v2s = res
        .get_by_index::<V2>(1)?
        .into_vec()
        .map_err(|e| e.to_string())?;
    Ok((v1s, v2s))
}

/// Inner join on key `K`: for each key in both inputs, the Cartesian product of their values.
pub struct InnerJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> InnerJoin<K, V1, V2> {
    /// Creates an `InnerJoin` transform with the right collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V1, V2> HasDisplayData for InnerJoin<K, V1, V2> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "InnerJoin");
    }
}

impl<K, V1, V2> PTransform<PCollection<(K, V1)>> for InnerJoin<K, V1, V2>
where
    K: DefaultCoder + Eq + Hash + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
{
    type Output = PCollection<(K, (V1, V2))>;

    fn expand(&self, input: &PCollection<(K, V1)>) -> Self::Output {
        {
            let tuple = KeyedPCollectionTuple::of("0", input).and("1", &self.right);
            let cogbk = tuple.apply("CoGbk", CoGroupByKey::new());
            cogbk.apply(
                "CartesianProduct",
                ParDo::new(ClosureFn::new(
                    "InnerJoin",
                    |(k, res): (K, CoGbkResult), ctx| {
                        let (v1s, v2s) = join_sides::<V1, V2>(&res)?;
                        ctx.emit_all(cross_product(k, &v1s, &v2s, |l, r| (l.clone(), r.clone())))
                    },
                )),
            )
        }
    }
}

/// Left outer join on key `K`: `(K, (V1, Some(V2)))` for each matching `V2`, or
/// `(K, (V1, None))` if the right input has no values for `K`.
pub struct LeftJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> LeftJoin<K, V1, V2> {
    /// Creates a `LeftJoin` transform with the right collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V1, V2> HasDisplayData for LeftJoin<K, V1, V2> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "LeftJoin");
    }
}

impl<K, V1, V2> PTransform<PCollection<(K, V1)>> for LeftJoin<K, V1, V2>
where
    K: DefaultCoder + Eq + Hash + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
{
    type Output = PCollection<(K, (V1, Option<V2>))>;

    fn expand(&self, input: &PCollection<(K, V1)>) -> Self::Output {
        {
            let tuple = KeyedPCollectionTuple::of("0", input).and("1", &self.right);
            let cogbk = tuple.apply("CoGbk", CoGroupByKey::new());
            cogbk.apply(
                "LeftCross",
                ParDo::new(ClosureFn::new(
                    "LeftJoin",
                    |(k, res): (K, CoGbkResult), ctx| {
                        let (v1s, v2s) = join_sides::<V1, V2>(&res)?;
                        match v2s.as_slice() {
                            [] => ctx.emit_all(unmatched(k, v1s, |l| (l, None))),
                            right => ctx.emit_all(cross_product(k, &v1s, right, |l, r| {
                                (l.clone(), Some(r.clone()))
                            })),
                        }
                    },
                )),
            )
        }
    }
}

/// Right outer join on key `K`: `(K, (Some(V1), V2))` for each matching `V1`, or
/// `(K, (None, V2))` if the left input has no values for `K`.
pub struct RightJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> RightJoin<K, V1, V2> {
    /// Creates a `RightJoin` transform with the right collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V1, V2> HasDisplayData for RightJoin<K, V1, V2> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "RightJoin");
    }
}

impl<K, V1, V2> PTransform<PCollection<(K, V1)>> for RightJoin<K, V1, V2>
where
    K: DefaultCoder + Eq + Hash + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
{
    type Output = PCollection<(K, (Option<V1>, V2))>;

    fn expand(&self, input: &PCollection<(K, V1)>) -> Self::Output {
        {
            let tuple = KeyedPCollectionTuple::of("0", input).and("1", &self.right);
            let cogbk = tuple.apply("CoGbk", CoGroupByKey::new());
            cogbk.apply(
                "RightCross",
                ParDo::new(ClosureFn::new(
                    "RightJoin",
                    |(k, res): (K, CoGbkResult), ctx| {
                        let (v1s, v2s) = join_sides::<V1, V2>(&res)?;
                        match v1s.as_slice() {
                            [] => ctx.emit_all(unmatched(k, v2s, |r| (None, r))),
                            left => ctx.emit_all(cross_product(k, left, &v2s, |l, r| {
                                (Some(l.clone()), r.clone())
                            })),
                        }
                    },
                )),
            )
        }
    }
}

/// Full outer join on key `K`: `(K, (Some(V1), Some(V2)))` for a key in both inputs,
/// `(K, (Some(V1), None))` for a key only on the left, `(K, (None, Some(V2)))` only on the right.
pub struct FullOuterJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> FullOuterJoin<K, V1, V2> {
    /// Creates a `FullOuterJoin` transform with the right collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V1, V2> HasDisplayData for FullOuterJoin<K, V1, V2> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "FullOuterJoin");
    }
}

impl<K, V1, V2> PTransform<PCollection<(K, V1)>> for FullOuterJoin<K, V1, V2>
where
    K: DefaultCoder + Eq + Hash + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
{
    type Output = PCollection<(K, (Option<V1>, Option<V2>))>;

    fn expand(&self, input: &PCollection<(K, V1)>) -> Self::Output {
        {
            let tuple = KeyedPCollectionTuple::of("0", input).and("1", &self.right);
            let cogbk = tuple.apply("CoGbk", CoGroupByKey::new());
            cogbk.apply(
                "FullCross",
                ParDo::new(ClosureFn::new(
                    "FullOuterJoin",
                    |(k, res): (K, CoGbkResult), ctx| {
                        let (v1s, v2s) = join_sides::<V1, V2>(&res)?;
                        match (v1s.is_empty(), v2s.is_empty()) {
                            (true, true) => Ok(()),
                            (true, false) => ctx.emit_all(unmatched(k, v2s, |r| (None, Some(r)))),
                            (false, true) => ctx.emit_all(unmatched(k, v1s, |l| (Some(l), None))),
                            (false, false) => ctx.emit_all(cross_product(k, &v1s, &v2s, |l, r| {
                                (Some(l.clone()), Some(r.clone()))
                            })),
                        }
                    },
                )),
            )
        }
    }
}

/// Broadcast inner join on key `K`. Reads `right` as a multimap
/// [`MultimapView`](crate::values::MultimapView) and looks up each `left` element in it, so
/// `left` is not shuffled.
pub struct BroadcastInnerJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> BroadcastInnerJoin<K, V1, V2> {
    /// Creates a `BroadcastInnerJoin` transform with the side collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V1, V2> HasDisplayData for BroadcastInnerJoin<K, V1, V2> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "BroadcastInnerJoin");
    }
}

impl<K, V1, V2> PTransform<PCollection<(K, V1)>> for BroadcastInnerJoin<K, V1, V2>
where
    K: DefaultCoder + Clone + 'static,
    V1: DefaultCoder + Clone + 'static,
    V2: DefaultCoder + 'static,
{
    type Output = PCollection<(K, (V1, V2))>;

    fn expand(&self, left: &PCollection<(K, V1)>) -> Self::Output {
        expand_as(
            left,
            ParDo::new(ClosureFn::with_side_inputs(
                "BroadcastInnerJoin",
                self.right.as_multimap(),
                |(k, v1): (K, V1), right, ctx| {
                    for v2 in right.get_all(&k)? {
                        ctx.emit((k.clone(), (v1.clone(), v2?)))?;
                    }
                    Ok(())
                },
            )),
        )
    }
}

/// Broadcast left outer join on key `K`. Reads `right` as a multimap
/// [`MultimapView`](crate::values::MultimapView), so `left` is not shuffled. Emits
/// `(K, (V1, Some(V2)))` for each match, or `(K, (V1, None))` if there is no match.
pub struct BroadcastLeftJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> BroadcastLeftJoin<K, V1, V2> {
    /// Creates a `BroadcastLeftJoin` transform with the side collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V1, V2> HasDisplayData for BroadcastLeftJoin<K, V1, V2> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "BroadcastLeftJoin");
    }
}

impl<K, V1, V2> PTransform<PCollection<(K, V1)>> for BroadcastLeftJoin<K, V1, V2>
where
    K: DefaultCoder + Clone + 'static,
    V1: DefaultCoder + Clone + 'static,
    V2: DefaultCoder + 'static,
{
    type Output = PCollection<(K, (V1, Option<V2>))>;

    fn expand(&self, left: &PCollection<(K, V1)>) -> Self::Output {
        expand_as(
            left,
            ParDo::new(ClosureFn::with_side_inputs(
                "BroadcastLeftJoin",
                self.right.as_multimap(),
                |(k, v1): (K, V1), right, ctx| {
                    let mut matches = right.get_all(&k)?.peekable();
                    if matches.peek().is_none() {
                        ctx.emit((k, (v1, None)))
                    } else {
                        for v2 in matches {
                            ctx.emit((k.clone(), (v1.clone(), Some(v2?))))?;
                        }
                        Ok(())
                    }
                },
            )),
        )
    }
}
