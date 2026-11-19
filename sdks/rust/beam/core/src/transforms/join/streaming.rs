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

//! The join of a key whose values come from a state stream: buffer the smaller side and read
//! the other side again from the stream.

use super::in_memory::{JoinMode, decode_side, emit_buffered, emit_row, unexpected_tag};
use super::{JoinFn, LEFT_TAG, RIGHT_TAG};
use crate::coders::{BeamIterable, DefaultCoder};
use crate::transforms::ProcessContext;
use crate::transforms::co_group_by_key::RawUnionValue;
use crate::transforms::co_group_by_key::{MAX_BUFFERED_BYTES, MAX_BUFFERED_ITEMS, RawUnionOpener};

/// Per-side in-memory buffer caps for a state-backed join group.
#[derive(Clone, Copy, Debug)]
pub(super) struct BufferLimits {
    max_items: usize,
    max_bytes: usize,
}

impl BufferLimits {
    pub(super) const DEFAULT: Self = Self {
        max_items: MAX_BUFFERED_ITEMS,
        max_bytes: MAX_BUFFERED_BYTES,
    };

    pub(super) fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            max_items: max_items.max(1),
            max_bytes: max_bytes.max(1),
        }
    }

    fn exceeded(self, items: usize, bytes: usize) -> bool {
        items > self.max_items || bytes > self.max_bytes
    }
}

/// Pass-1 buffer state for one side of a state-backed join group.
enum SideBuffer<V> {
    Buffered { values: Vec<V>, bytes: usize },
    Overflowed,
}

impl<K, V1, V2, M> JoinFn<K, V1, V2, M>
where
    K: DefaultCoder + Clone,
    V1: DefaultCoder + Clone,
    V2: DefaultCoder + Clone,
    M: JoinMode<V1, V2>,
{
    pub(super) fn process_state_backed(
        &mut self,
        key: K,
        raw_values: BeamIterable<RawUnionValue>,
        ctx: &mut ProcessContext<'_, (K, M::Out)>,
    ) -> crate::Result {
        let Some(reopen) = raw_values.reopen_fn() else {
            return self.process_single_pass(key, raw_values, ctx);
        };

        let mut left = SideBuffer::<V1>::Buffered {
            values: Vec::new(),
            bytes: 0,
        };
        let mut right = SideBuffer::<V2>::Buffered {
            values: Vec::new(),
            bytes: 0,
        };

        for rv in raw_values.try_into_iter() {
            let RawUnionValue { tag, value } =
                rv.map_err(|e| format!("Join failed to read a grouped value: {e}"))?;
            match tag {
                LEFT_TAG => match &mut left {
                    SideBuffer::Buffered { values, bytes } => {
                        let next_items = values.len() + 1;
                        let next_bytes = bytes.saturating_add(value.len());
                        if self.limits.exceeded(next_items, next_bytes) {
                            left = SideBuffer::Overflowed;
                        } else {
                            let decoded = decode_side(
                                &mut self.left_field_map,
                                &value,
                                self.left_schema.as_ref(),
                                LEFT_TAG,
                            )?;
                            values.push(decoded);
                            *bytes = next_bytes;
                        }
                    }
                    SideBuffer::Overflowed => {}
                },
                RIGHT_TAG => match &mut right {
                    SideBuffer::Buffered { values, bytes } => {
                        let next_items = values.len() + 1;
                        let next_bytes = bytes.saturating_add(value.len());
                        if self.limits.exceeded(next_items, next_bytes) {
                            right = SideBuffer::Overflowed;
                        } else {
                            let decoded = decode_side(
                                &mut self.right_field_map,
                                &value,
                                self.right_schema.as_ref(),
                                RIGHT_TAG,
                            )?;
                            values.push(decoded);
                            *bytes = next_bytes;
                        }
                    }
                    SideBuffer::Overflowed => {}
                },
                other => return Err(unexpected_tag(other)),
            }

            // Stop pass 1 early when both sides overflow; block-nested-loop reads all items.
            if matches!(
                (&left, &right),
                (SideBuffer::Overflowed, SideBuffer::Overflowed)
            ) {
                break;
            }
        }

        match (left, right) {
            (
                SideBuffer::Buffered { values: left, .. },
                SideBuffer::Buffered { values: right, .. },
            ) => emit_buffered::<K, V1, V2, M>(key, left, right, ctx),
            (SideBuffer::Buffered { values: left, .. }, SideBuffer::Overflowed) => {
                if left.is_empty() {
                    if M::EMIT_UNMATCHED_RIGHT {
                        self.stream_unmatched_right(&key, &reopen, ctx)?;
                    }
                    return Ok(());
                }
                self.stream_right_against_left(&key, &left, &reopen, ctx)
            }
            (SideBuffer::Overflowed, SideBuffer::Buffered { values: right, .. }) => {
                if right.is_empty() {
                    if M::EMIT_UNMATCHED_LEFT {
                        self.stream_unmatched_left(&key, &reopen, ctx)?;
                    }
                    return Ok(());
                }
                self.stream_left_against_right(&key, &right, &reopen, ctx)
            }
            (SideBuffer::Overflowed, SideBuffer::Overflowed) => {
                self.block_nested_loop(&key, &reopen, ctx)
            }
        }
    }

    fn stream_right_against_left(
        &mut self,
        key: &K,
        left: &[V1],
        reopen: &RawUnionOpener,
        ctx: &mut ProcessContext<'_, (K, M::Out)>,
    ) -> crate::Result {
        let iter = reopen().map_err(|e| format!("Join failed to reopen state stream: {e}"))?;
        for rv in iter {
            let RawUnionValue { tag, value } =
                rv.map_err(|e| format!("Join failed to read a grouped value: {e}"))?;
            match tag {
                LEFT_TAG => {}
                RIGHT_TAG => {
                    let r = decode_side::<V2>(
                        &mut self.right_field_map,
                        &value,
                        self.right_schema.as_ref(),
                        RIGHT_TAG,
                    )?;
                    // Each row owns a copy of the key and both values. The last row takes `r`.
                    if let Some((last, init)) = left.split_last() {
                        for l in init {
                            emit_row::<K, V1, V2, M>(
                                key.clone(),
                                Some(l.clone()),
                                Some(r.clone()),
                                ctx,
                            )?;
                        }
                        emit_row::<K, V1, V2, M>(key.clone(), Some(last.clone()), Some(r), ctx)?;
                    }
                }
                other => return Err(unexpected_tag(other)),
            }
        }
        Ok(())
    }

    fn stream_left_against_right(
        &mut self,
        key: &K,
        right: &[V2],
        reopen: &RawUnionOpener,
        ctx: &mut ProcessContext<'_, (K, M::Out)>,
    ) -> crate::Result {
        let iter = reopen().map_err(|e| format!("Join failed to reopen state stream: {e}"))?;
        for rv in iter {
            let RawUnionValue { tag, value } =
                rv.map_err(|e| format!("Join failed to read a grouped value: {e}"))?;
            match tag {
                LEFT_TAG => {
                    let l = decode_side::<V1>(
                        &mut self.left_field_map,
                        &value,
                        self.left_schema.as_ref(),
                        LEFT_TAG,
                    )?;
                    // Each row owns a copy of the key and both values. The last row takes `l`.
                    if let Some((last, init)) = right.split_last() {
                        for r in init {
                            emit_row::<K, V1, V2, M>(
                                key.clone(),
                                Some(l.clone()),
                                Some(r.clone()),
                                ctx,
                            )?;
                        }
                        emit_row::<K, V1, V2, M>(key.clone(), Some(l), Some(last.clone()), ctx)?;
                    }
                }
                RIGHT_TAG => {}
                other => return Err(unexpected_tag(other)),
            }
        }
        Ok(())
    }

    fn stream_unmatched_left(
        &mut self,
        key: &K,
        reopen: &RawUnionOpener,
        ctx: &mut ProcessContext<'_, (K, M::Out)>,
    ) -> crate::Result {
        let iter = reopen().map_err(|e| format!("Join failed to reopen state stream: {e}"))?;
        for rv in iter {
            let RawUnionValue { tag, value } =
                rv.map_err(|e| format!("Join failed to read a grouped value: {e}"))?;
            match tag {
                LEFT_TAG => {
                    let l = decode_side::<V1>(
                        &mut self.left_field_map,
                        &value,
                        self.left_schema.as_ref(),
                        LEFT_TAG,
                    )?;
                    emit_row::<K, V1, V2, M>(key.clone(), Some(l), None, ctx)?;
                }
                RIGHT_TAG => {}
                other => return Err(unexpected_tag(other)),
            }
        }
        Ok(())
    }

    fn stream_unmatched_right(
        &mut self,
        key: &K,
        reopen: &RawUnionOpener,
        ctx: &mut ProcessContext<'_, (K, M::Out)>,
    ) -> crate::Result {
        let iter = reopen().map_err(|e| format!("Join failed to reopen state stream: {e}"))?;
        for rv in iter {
            let RawUnionValue { tag, value } =
                rv.map_err(|e| format!("Join failed to read a grouped value: {e}"))?;
            match tag {
                LEFT_TAG => {}
                RIGHT_TAG => {
                    let r = decode_side::<V2>(
                        &mut self.right_field_map,
                        &value,
                        self.right_schema.as_ref(),
                        RIGHT_TAG,
                    )?;
                    emit_row::<K, V1, V2, M>(key.clone(), None, Some(r), ctx)?;
                }
                other => return Err(unexpected_tag(other)),
            }
        }
        Ok(())
    }

    /// Reads bounded chunks of `left` from one stream and probes all `right` items per chunk.
    fn block_nested_loop(
        &mut self,
        key: &K,
        reopen: &RawUnionOpener,
        ctx: &mut ProcessContext<'_, (K, M::Out)>,
    ) -> crate::Result {
        let mut left_stream =
            reopen().map_err(|e| format!("Join failed to reopen state stream: {e}"))?;
        let mut left_chunk = Vec::new();
        let mut left_bytes: usize = 0;

        for rv in &mut left_stream {
            let RawUnionValue { tag, value } =
                rv.map_err(|e| format!("Join failed to read a grouped value: {e}"))?;
            match tag {
                LEFT_TAG => {
                    let value_len = value.len();
                    let decoded = decode_side(
                        &mut self.left_field_map,
                        &value,
                        self.left_schema.as_ref(),
                        LEFT_TAG,
                    )?;
                    left_chunk.push(decoded);
                    left_bytes = left_bytes.saturating_add(value_len);
                    if left_chunk.len() >= self.limits.max_items
                        || left_bytes >= self.limits.max_bytes
                    {
                        self.stream_right_against_left(key, &left_chunk, reopen, ctx)?;
                        left_chunk.clear();
                        left_bytes = 0;
                    }
                }
                RIGHT_TAG => {}
                other => return Err(unexpected_tag(other)),
            }
        }
        if !left_chunk.is_empty() {
            self.stream_right_against_left(key, &left_chunk, reopen, ctx)?;
        }
        Ok(())
    }
}
