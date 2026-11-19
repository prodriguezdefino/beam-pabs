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

//! Relational joins built on tagged `GroupByKey` shuffles or on side input views.

mod in_memory;
mod streaming;

use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use super::co_group_by_key::{RawUnionValue, value_row_schema};
use super::composite::expand_as;
use super::{
    ClosureFn, DisplayDataBuilder, DoFn, Flatten, GroupByKey, HasDisplayData, PTransform, ParDo,
    ProcessContext,
};
use crate::coders::{BeamIterable, DefaultCoder, FieldMapCache};
use crate::schema::Schema;
use crate::values::PCollection;
use in_memory::{FullOuterMode, InnerMode, JoinMode, LeftMode, RightMode};
use streaming::BufferLimits;

const LEFT_TAG: i32 = 0;
const RIGHT_TAG: i32 = 1;

type JoinMarker<K, V1, V2, M> = PhantomData<fn() -> (K, V1, V2, M)>;

/// Joins tagged `GroupByKey` values directly without building an intermediate `CoGbkResult`.
/// For a state-backed hot key, buffers the smaller side and streams the overflowing side.
struct JoinFn<K, V1, V2, M> {
    left_schema: Option<Arc<Schema>>,
    right_schema: Option<Arc<Schema>>,
    left_field_map: FieldMapCache,
    right_field_map: FieldMapCache,
    limits: BufferLimits,
    _marker: JoinMarker<K, V1, V2, M>,
}

impl<K, V1, V2, M> JoinFn<K, V1, V2, M> {
    fn new(
        left_schema: Option<Arc<Schema>>,
        right_schema: Option<Arc<Schema>>,
        limits: BufferLimits,
    ) -> Self {
        Self {
            left_schema,
            right_schema,
            left_field_map: FieldMapCache::new(),
            right_field_map: FieldMapCache::new(),
            limits,
            _marker: PhantomData,
        }
    }
}

impl<K, V1, V2, M> Clone for JoinFn<K, V1, V2, M> {
    fn clone(&self) -> Self {
        Self {
            left_schema: self.left_schema.as_ref().map(Arc::clone),
            right_schema: self.right_schema.as_ref().map(Arc::clone),
            left_field_map: FieldMapCache::new(),
            right_field_map: FieldMapCache::new(),
            limits: self.limits,
            _marker: PhantomData,
        }
    }
}

impl<K, V1, V2, M> DoFn for JoinFn<K, V1, V2, M>
where
    K: DefaultCoder + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
    M: JoinMode<V1, V2>,
{
    type In = (K, BeamIterable<RawUnionValue>);
    type Out = (K, M::Out);

    fn process_element(
        &mut self,
        (key, raw_values): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        match raw_values.try_into_in_memory() {
            Ok(elements) => self.process_in_memory(key, elements, ctx),
            Err(raw_values) => self.process_state_backed(key, raw_values, ctx),
        }
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", M::KIND);
    }
}

fn tag_side<K, V>(
    input: &PCollection<(K, V)>,
    step_name: &'static str,
    union_tag: i32,
) -> PCollection<(K, RawUnionValue)>
where
    K: DefaultCoder,
    V: DefaultCoder,
{
    let tagged = input.apply(
        step_name,
        ParDo::new(ClosureFn::new("Map", move |(k, v): (K, V), out| {
            let value = v.encode()?;
            out.emit((
                k,
                RawUnionValue {
                    tag: union_tag,
                    value,
                },
            ))
        })),
    );
    let pipeline = input.pipeline();
    if let Some((key_coder, _)) = pipeline.kv_coder_parts(&input.coder_id()) {
        let union_coder = RawUnionValue::register_coder(pipeline);
        let tagged_coder = pipeline.register_kv_coder(key_coder, union_coder);
        return tagged.with_coder_id(tagged_coder);
    }
    tagged
}

fn expand_join<K, V1, V2, M>(
    left: &PCollection<(K, V1)>,
    right: &PCollection<(K, V2)>,
    limits: BufferLimits,
) -> PCollection<(K, M::Out)>
where
    K: DefaultCoder + Eq + Hash + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
    M: JoinMode<V1, V2>,
{
    let left_schema = value_row_schema(left);
    let right_schema = value_row_schema(right);
    let tagged_left = tag_side(left, "TagLeft", LEFT_TAG);
    let tagged_right = tag_side(right, "TagRight", RIGHT_TAG);
    let flattened = Flatten::pcollections("Flatten", &[&tagged_left, &tagged_right]);
    let grouped = flattened.apply("GroupByKey", GroupByKey::new());
    let out = grouped.apply(
        "Join",
        ParDo::new(JoinFn::<K, V1, V2, M>::new(
            left_schema,
            right_schema,
            limits,
        )),
    );
    let pipeline = left.pipeline();
    if let (Some((key_coder, left_coder)), Some((_, right_coder))) = (
        pipeline.kv_coder_parts(&left.coder_id()),
        pipeline.kv_coder_parts(&right.coder_id()),
    ) {
        let left_out = if M::EMIT_UNMATCHED_RIGHT {
            pipeline.register_coder(crate::coders::URN_NULLABLE, vec![left_coder])
        } else {
            left_coder
        };
        let right_out = if M::EMIT_UNMATCHED_LEFT {
            pipeline.register_coder(crate::coders::URN_NULLABLE, vec![right_coder])
        } else {
            right_coder
        };
        let pair_coder = pipeline.register_kv_coder(left_out, right_out);
        let out_coder = pipeline.register_kv_coder(key_coder, pair_coder);
        return out.with_coder_id(out_coder);
    }
    out
}

/// Inner join on key `K`: for each key in both inputs, the Cartesian product of their values.
pub struct InnerJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    limits: BufferLimits,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> InnerJoin<K, V1, V2> {
    /// Creates an `InnerJoin` transform with the right collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            limits: BufferLimits::DEFAULT,
            _marker: PhantomData,
        }
    }

    /// Sets the per-side buffer caps. Tests only: the item-cap chunk edges need more than
    /// 10,000 values on each side, and no test can join that product.
    #[doc(hidden)]
    pub fn with_buffer_limits(mut self, max_items: usize, max_bytes: usize) -> Self {
        self.limits = BufferLimits::new(max_items, max_bytes);
        self
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

    fn expand(self, input: &PCollection<(K, V1)>) -> Self::Output {
        input.pipeline().record_display_data(&self);
        expand_join::<K, V1, V2, InnerMode>(input, &self.right, self.limits)
    }
}

/// Left outer join on key `K`: `(K, (V1, Some(V2)))` for each matching `V2`, or
/// `(K, (V1, None))` if the right input has no values for `K`.
pub struct LeftJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    limits: BufferLimits,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> LeftJoin<K, V1, V2> {
    /// Creates a `LeftJoin` transform with the right collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            limits: BufferLimits::DEFAULT,
            _marker: PhantomData,
        }
    }

    /// Sets the per-side buffer caps. Tests only: the item-cap chunk edges need more than
    /// 10,000 values on each side, and no test can join that product.
    #[doc(hidden)]
    pub fn with_buffer_limits(mut self, max_items: usize, max_bytes: usize) -> Self {
        self.limits = BufferLimits::new(max_items, max_bytes);
        self
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

    fn expand(self, input: &PCollection<(K, V1)>) -> Self::Output {
        input.pipeline().record_display_data(&self);
        expand_join::<K, V1, V2, LeftMode>(input, &self.right, self.limits)
    }
}

/// Right outer join on key `K`: `(K, (Some(V1), V2))` for each matching `V1`, or
/// `(K, (None, V2))` if the left input has no values for `K`.
pub struct RightJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    limits: BufferLimits,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> RightJoin<K, V1, V2> {
    /// Creates a `RightJoin` transform with the right collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            limits: BufferLimits::DEFAULT,
            _marker: PhantomData,
        }
    }

    /// Sets the per-side buffer caps. Tests only: the item-cap chunk edges need more than
    /// 10,000 values on each side, and no test can join that product.
    #[doc(hidden)]
    pub fn with_buffer_limits(mut self, max_items: usize, max_bytes: usize) -> Self {
        self.limits = BufferLimits::new(max_items, max_bytes);
        self
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

    fn expand(self, input: &PCollection<(K, V1)>) -> Self::Output {
        input.pipeline().record_display_data(&self);
        expand_join::<K, V1, V2, RightMode>(input, &self.right, self.limits)
    }
}

/// Full outer join on key `K`: `(K, (Some(V1), Some(V2)))` for a key in both inputs,
/// `(K, (Some(V1), None))` for a key only on the left, `(K, (None, Some(V2)))` only on the right.
pub struct FullOuterJoin<K, V1, V2> {
    right: PCollection<(K, V2)>,
    limits: BufferLimits,
    _marker: PhantomData<(K, V1)>,
}

impl<K, V1, V2> FullOuterJoin<K, V1, V2> {
    /// Creates a `FullOuterJoin` transform with the right collection.
    pub fn new(right: &PCollection<(K, V2)>) -> Self {
        Self {
            right: right.clone(),
            limits: BufferLimits::DEFAULT,
            _marker: PhantomData,
        }
    }

    /// Sets the per-side buffer caps. Tests only: the item-cap chunk edges need more than
    /// 10,000 values on each side, and no test can join that product.
    #[doc(hidden)]
    pub fn with_buffer_limits(mut self, max_items: usize, max_bytes: usize) -> Self {
        self.limits = BufferLimits::new(max_items, max_bytes);
        self
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

    fn expand(self, input: &PCollection<(K, V1)>) -> Self::Output {
        input.pipeline().record_display_data(&self);
        expand_join::<K, V1, V2, FullOuterMode>(input, &self.right, self.limits)
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

    fn expand(self, left: &PCollection<(K, V1)>) -> Self::Output {
        let out = expand_as(
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
        );
        left.pipeline().record_display_data(&self);
        out
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

    fn expand(self, left: &PCollection<(K, V1)>) -> Self::Output {
        let out = expand_as(
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
        );
        left.pipeline().record_display_data(&self);
        out
    }
}
