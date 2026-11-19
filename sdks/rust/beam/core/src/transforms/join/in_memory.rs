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

//! Join kinds and the join of a key whose values fit in memory.

use std::sync::Arc;

use super::{JoinFn, LEFT_TAG, RIGHT_TAG};
use crate::coders::{BeamIterable, DefaultCoder, FieldMapCache};
use crate::schema::Schema;
use crate::transforms::ProcessContext;
use crate::transforms::co_group_by_key::RawUnionValue;

/// The output of one join kind.
pub(super) trait JoinMode<V1, V2>: 'static {
    type Out: DefaultCoder;
    const KIND: &'static str;
    const EMIT_UNMATCHED_LEFT: bool;
    const EMIT_UNMATCHED_RIGHT: bool;

    /// Builds the output of one row from the values that the row has, or `None` for a row
    /// that this kind drops.
    fn output(left: Option<V1>, right: Option<V2>) -> Option<Self::Out>;
}

pub(super) struct InnerMode;
pub(super) struct LeftMode;
pub(super) struct RightMode;
pub(super) struct FullOuterMode;

impl<V1: DefaultCoder, V2: DefaultCoder> JoinMode<V1, V2> for InnerMode {
    type Out = (V1, V2);
    const KIND: &'static str = "InnerJoin";
    const EMIT_UNMATCHED_LEFT: bool = false;
    const EMIT_UNMATCHED_RIGHT: bool = false;

    fn output(left: Option<V1>, right: Option<V2>) -> Option<Self::Out> {
        match (left, right) {
            (Some(left), Some(right)) => Some((left, right)),
            (None, _) | (_, None) => None,
        }
    }
}

impl<V1: DefaultCoder, V2: DefaultCoder> JoinMode<V1, V2> for LeftMode {
    type Out = (V1, Option<V2>);
    const KIND: &'static str = "LeftJoin";
    const EMIT_UNMATCHED_LEFT: bool = true;
    const EMIT_UNMATCHED_RIGHT: bool = false;

    fn output(left: Option<V1>, right: Option<V2>) -> Option<Self::Out> {
        match (left, right) {
            (Some(left), right) => Some((left, right)),
            (None, _) => None,
        }
    }
}

impl<V1: DefaultCoder, V2: DefaultCoder> JoinMode<V1, V2> for RightMode {
    type Out = (Option<V1>, V2);
    const KIND: &'static str = "RightJoin";
    const EMIT_UNMATCHED_LEFT: bool = false;
    const EMIT_UNMATCHED_RIGHT: bool = true;

    fn output(left: Option<V1>, right: Option<V2>) -> Option<Self::Out> {
        match (left, right) {
            (left, Some(right)) => Some((left, right)),
            (_, None) => None,
        }
    }
}

impl<V1: DefaultCoder, V2: DefaultCoder> JoinMode<V1, V2> for FullOuterMode {
    type Out = (Option<V1>, Option<V2>);
    const KIND: &'static str = "FullOuterJoin";
    const EMIT_UNMATCHED_LEFT: bool = true;
    const EMIT_UNMATCHED_RIGHT: bool = true;

    fn output(left: Option<V1>, right: Option<V2>) -> Option<Self::Out> {
        match (left, right) {
            (None, None) => None,
            (left, right) => Some((left, right)),
        }
    }
}

/// Emits the row of `key` with `left` and `right`, if the join kind keeps it.
pub(super) fn emit_row<K, V1, V2, M>(
    key: K,
    left: Option<V1>,
    right: Option<V2>,
    ctx: &mut ProcessContext<'_, (K, M::Out)>,
) -> crate::Result
where
    K: DefaultCoder,
    M: JoinMode<V1, V2>,
{
    match M::output(left, right) {
        Some(out) => ctx.emit((key, out)),
        None => Ok(()),
    }
}

/// Emits a row for each pair of `left` and `right`. Each row owns a copy of the key and of
/// both values.
fn emit_product<K, V1, V2, M>(
    key: &K,
    left: &[V1],
    right: &[V2],
    ctx: &mut ProcessContext<'_, (K, M::Out)>,
) -> crate::Result
where
    K: DefaultCoder + Clone,
    V1: Clone,
    V2: Clone,
    M: JoinMode<V1, V2>,
{
    left.iter().try_for_each(|l| {
        right.iter().try_for_each(|r| {
            emit_row::<K, V1, V2, M>(key.clone(), Some(l.clone()), Some(r.clone()), ctx)
        })
    })
}

pub(super) fn unexpected_tag(tag: i32) -> crate::Error {
    crate::Error::msg(format!("Join read a value with tag {tag} but has 2 inputs"))
}

pub(super) fn decode_side<V: DefaultCoder>(
    cache: &mut FieldMapCache,
    bytes: &[u8],
    schema: Option<&Arc<Schema>>,
    tag: i32,
) -> crate::Result<V> {
    cache.decode::<V>(bytes, schema, None).map_err(|e| {
        crate::Error::from(e).context(format!("Failed to decode Join value for tag index {tag}"))
    })
}

/// Emits all join rows when both sides fit in memory.
pub(super) fn emit_buffered<K, V1, V2, M>(
    key: K,
    left: Vec<V1>,
    right: Vec<V2>,
    ctx: &mut ProcessContext<'_, (K, M::Out)>,
) -> crate::Result
where
    K: DefaultCoder + Clone,
    V1: Clone,
    V2: Clone,
    M: JoinMode<V1, V2>,
{
    match (left.is_empty(), right.is_empty()) {
        (true, true) => Ok(()),
        (false, true) if M::EMIT_UNMATCHED_LEFT => left
            .into_iter()
            .try_for_each(|l| emit_row::<K, V1, V2, M>(key.clone(), Some(l), None, ctx)),
        (true, false) if M::EMIT_UNMATCHED_RIGHT => right
            .into_iter()
            .try_for_each(|r| emit_row::<K, V1, V2, M>(key.clone(), None, Some(r), ctx)),
        (false, true) | (true, false) => Ok(()),
        (false, false) => match (<[V1; 1]>::try_from(left), <[V2; 1]>::try_from(right)) {
            (Ok([l]), Ok([r])) => emit_row::<K, V1, V2, M>(key, Some(l), Some(r), ctx),
            (left, right) => {
                let left = left.map_or_else(|left| left, Vec::from);
                let right = right.map_or_else(|right| right, Vec::from);
                emit_product::<K, V1, V2, M>(&key, &left, &right, ctx)
            }
        },
    }
}

impl<K, V1, V2, M> JoinFn<K, V1, V2, M>
where
    K: DefaultCoder + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
    M: JoinMode<V1, V2>,
{
    pub(super) fn process_in_memory(
        &mut self,
        key: K,
        elements: Vec<RawUnionValue>,
        ctx: &mut ProcessContext<'_, (K, M::Out)>,
    ) -> crate::Result {
        let mut left = Vec::new();
        let mut right = Vec::new();
        for RawUnionValue { tag, value } in elements {
            match tag {
                LEFT_TAG => left.push(decode_side(
                    &mut self.left_field_map,
                    &value,
                    self.left_schema.as_ref(),
                    LEFT_TAG,
                )?),
                RIGHT_TAG => right.push(decode_side(
                    &mut self.right_field_map,
                    &value,
                    self.right_schema.as_ref(),
                    RIGHT_TAG,
                )?),
                other => return Err(unexpected_tag(other)),
            }
        }
        emit_buffered::<K, V1, V2, M>(key, left, right, ctx)
    }

    pub(super) fn process_single_pass(
        &mut self,
        key: K,
        raw_values: BeamIterable<RawUnionValue>,
        ctx: &mut ProcessContext<'_, (K, M::Out)>,
    ) -> crate::Result {
        let mut left = Vec::new();
        let mut right = Vec::new();
        for rv in raw_values.try_into_iter() {
            let RawUnionValue { tag, value } =
                rv.map_err(|e| format!("Join failed to read a grouped value: {e}"))?;
            match tag {
                LEFT_TAG => left.push(decode_side(
                    &mut self.left_field_map,
                    &value,
                    self.left_schema.as_ref(),
                    LEFT_TAG,
                )?),
                RIGHT_TAG => right.push(decode_side(
                    &mut self.right_field_map,
                    &value,
                    self.right_schema.as_ref(),
                    RIGHT_TAG,
                )?),
                other => return Err(unexpected_tag(other)),
            }
        }
        emit_buffered::<K, V1, V2, M>(key, left, right, ctx)
    }
}
