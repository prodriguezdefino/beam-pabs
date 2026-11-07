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

//! The pre-shuffle partial combine handlers.

use std::hash::Hash;
use std::sync::Arc;

use super::CombineFn;
use super::table::{AccumulatorTable, GroupOutput, KeyAccumulator};
use crate::coders::{DefaultCoder, FieldMapCache};
use crate::transforms::dofn::HandlerContext;
use crate::transforms::{BundleHandler, ElementSink, HandlerInstance, TypedElement};

/// About how much memory the accumulator table of one bundle holds before it evicts. The
/// limit is per running bundle; with word-count entries it is about 800k keys.
const DEFAULT_TABLE_MAX_BYTES: usize = 64 << 20;

/// Splits an encoded `(K, V)` element into the leading key bytes and remaining value bytes.
fn split_key<K: DefaultCoder>(element: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let mut rest = element;
    K::skip_nested(&mut rest).map_err(|e| format!("Failed to decode input element: {e}"))?;
    let split = element.len() - rest.len();
    Ok((&element[..split], rest))
}

/// Checks canonical key bytes on a cache miss by decoding `K` once.
fn validate_key<K: DefaultCoder>(mut key_bytes: &[u8]) -> crate::Result {
    let _ = K::decode_element(&mut key_bytes)
        .map_err(|e| format!("Failed to decode input element: {e}"))?;
    if !key_bytes.is_empty() {
        return Err(crate::Error::msg(
            "Failed to decode input element: trailing bytes in key",
        ));
    }
    Ok(())
}

/// Writes `key` and encoded `accumulator` to `scratch` and sends them to `sink`.
fn emit_bytes_entry<A: DefaultCoder>(
    sink: &mut dyn ElementSink,
    scratch: &mut Vec<u8>,
    group: &GroupOutput<'_>,
    key: &[u8],
    entry: KeyAccumulator<A>,
) -> crate::Result {
    let header = group.header_for(&entry);
    let Some(accumulator) = entry.accumulator else {
        return Ok(());
    };
    scratch.clear();
    scratch.extend_from_slice(key);
    accumulator
        .encode_element(scratch)
        .map_err(|e| format!("Failed to encode emitted element: {e}"))?;
    sink.push_windowed_bytes(&header, scratch)
        .map_err(crate::Error::msg)
}

/// Emits one typed `(K, A)` entry to `sink`.
fn emit_typed_entry<K: DefaultCoder, A: DefaultCoder>(
    sink: &mut dyn ElementSink,
    group: &GroupOutput<'_>,
    key: K,
    entry: KeyAccumulator<A>,
) -> crate::Result {
    let header = group.header_for(&entry);
    let Some(accumulator) = entry.accumulator else {
        return Ok(());
    };
    let mut slot = Some((key, accumulator));
    sink.push_value(None, Some(&header), TypedElement::new(&mut slot))
        .map_err(crate::Error::msg)
}

/// Per-bundle precombine state keyed by encoded key bytes.
pub(super) struct BytesPrecombine<CF: CombineFn> {
    combine_fn: Arc<CF>,
    table: AccumulatorTable<Box<[u8]>, CF::Accum>,
    max_bytes: usize,
    scratch: Vec<u8>,
    field_map: FieldMapCache,
}

impl<CF: CombineFn> BytesPrecombine<CF> {
    pub(super) fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            table: AccumulatorTable::default(),
            max_bytes: DEFAULT_TABLE_MAX_BYTES,
            scratch: Vec::new(),
            field_map: FieldMapCache::default(),
        }
    }

    pub(super) fn combine_fn(&self) -> &Arc<CF> {
        &self.combine_fn
    }

    fn is_empty(&self) -> bool {
        self.table.len == 0
    }

    pub(super) fn start_bundle(&mut self) {
        self.table = AccumulatorTable::default();
    }

    fn evict_if_full(&mut self, sink: &mut dyn ElementSink) -> Result<(), String> {
        if self.table.estimated_bytes() >= self.max_bytes {
            let Self { table, scratch, .. } = self;
            table
                .evict_coldest(|group, key, entry| {
                    emit_bytes_entry(sink, scratch, group, &key, entry)
                })
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Processes split `(key_bytes, value_bytes)`, once per window for multi-window elements.
    pub(super) fn process_split(
        &mut self,
        key_bytes: &[u8],
        value_bytes: &[u8],
        validate_on_miss: impl Fn(&[u8]) -> crate::Result,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        if !ctx.header.is_multi_window() {
            self.evict_if_full(ctx.sink)?;
            let value = self
                .field_map
                .decode::<CF::Input>(value_bytes, ctx.input_schema, ctx.state_stream_reader)
                .map_err(|e| format!("Failed to decode the combine value: {e}"))?;
            return self
                .table
                .add_bytes(
                    self.combine_fn.as_ref(),
                    ctx.header,
                    key_bytes,
                    validate_on_miss,
                    value,
                )
                .map_err(|e| e.to_string());
        }
        let mut validated = false;
        let header = ctx.header;
        header
            .explode()
            .map_err(|e| e.to_string())?
            .try_for_each(|header| {
                self.evict_if_full(ctx.sink)?;
                let value = self
                    .field_map
                    .decode::<CF::Input>(value_bytes, ctx.input_schema, ctx.state_stream_reader)
                    .map_err(|e| format!("Failed to decode the combine value: {e}"))?;
                self.table
                    .add_bytes(
                        self.combine_fn.as_ref(),
                        &header,
                        key_bytes,
                        |bytes| {
                            if !validated {
                                validate_on_miss(bytes)?;
                                validated = true;
                            }
                            Ok(())
                        },
                        value,
                    )
                    .map_err(|e| e.to_string())
            })
    }

    /// Encodes `key` and inserts `(key, value)` into the bytes table for one window.
    fn add_key_value<K: DefaultCoder>(
        &mut self,
        key: &K,
        value: CF::Input,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        self.evict_if_full(ctx.sink)?;
        self.scratch.clear();
        key.encode_element(&mut self.scratch)
            .map_err(|e| format!("Failed to encode key: {e}"))?;
        self.table
            .add_bytes(
                self.combine_fn.as_ref(),
                ctx.header,
                &self.scratch,
                |_| Ok(()),
                value,
            )
            .map_err(|e| e.to_string())
    }

    pub(super) fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let table = std::mem::take(&mut self.table);
        let scratch = &mut self.scratch;
        table
            .drain(|group, key, entry| emit_bytes_entry(ctx.sink, scratch, group, &key, entry))
            .map_err(|e| e.to_string())
    }
}

/// Per-bundle precombine state keyed by decoded Rust keys `K`.
struct TypedPrecombine<K, CF: CombineFn> {
    combine_fn: Arc<CF>,
    table: AccumulatorTable<K, CF::Accum>,
    max_bytes: usize,
    field_map: FieldMapCache,
}

impl<K, CF> TypedPrecombine<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            table: AccumulatorTable::default(),
            max_bytes: DEFAULT_TABLE_MAX_BYTES,
            field_map: FieldMapCache::default(),
        }
    }

    fn is_empty(&self) -> bool {
        self.table.len == 0
    }

    fn start_bundle(&mut self) {
        self.table = AccumulatorTable::default();
    }

    fn evict_if_full(&mut self, sink: &mut dyn ElementSink) -> Result<(), String> {
        if self.table.estimated_bytes() >= self.max_bytes {
            self.table
                .evict_coldest(|group, key, entry| emit_typed_entry(sink, group, key, entry))
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn add_single_window(
        &mut self,
        key: K,
        value: CF::Input,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        self.evict_if_full(ctx.sink)?;
        self.table
            .add(
                self.combine_fn.as_ref(),
                ctx.header.window_bytes(),
                ctx.header,
                key,
                value,
                ctx.header.timestamp_millis(),
            )
            .map_err(|e| e.to_string())
    }

    fn process_encoded(
        &mut self,
        element: &[u8],
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        if !ctx.header.is_multi_window() {
            self.evict_if_full(ctx.sink)?;
            let (key, value) = self
                .field_map
                .decode::<(K, CF::Input)>(element, ctx.input_schema, ctx.state_stream_reader)
                .map_err(|e| format!("Failed to decode input element: {e}"))?;
            return self
                .table
                .add(
                    self.combine_fn.as_ref(),
                    ctx.header.window_bytes(),
                    ctx.header,
                    key,
                    value,
                    ctx.header.timestamp_millis(),
                )
                .map_err(|e| e.to_string());
        }
        let header = ctx.header;
        header
            .explode()
            .map_err(|e| e.to_string())?
            .try_for_each(|header| {
                self.evict_if_full(ctx.sink)?;
                let (key, value) = self
                    .field_map
                    .decode::<(K, CF::Input)>(element, ctx.input_schema, ctx.state_stream_reader)
                    .map_err(|e| format!("Failed to decode input element: {e}"))?;
                self.table
                    .add(
                        self.combine_fn.as_ref(),
                        header.window_bytes(),
                        &header,
                        key,
                        value,
                        header.timestamp_millis(),
                    )
                    .map_err(|e| e.to_string())
            })
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        std::mem::take(&mut self.table)
            .drain(|group, key, entry| emit_typed_entry(ctx.sink, group, key, entry))
            .map_err(|e| e.to_string())
    }
}

/// Returns whether encoded elements of key `K` benefit from byte-slice lookup. Non-drop keys
/// (scalars and scalar tuples) decode without heap allocation and hash faster inline.
const fn use_bytes_table<K: DefaultCoder>() -> bool {
    K::CANONICAL_BYTES && std::mem::needs_drop::<K>()
}

enum PrecombineState<K, CF: CombineFn> {
    Bytes(BytesPrecombine<CF>),
    Typed(TypedPrecombine<K, CF>),
}

impl<K, CF> PrecombineState<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    fn new(combine_fn: Arc<CF>) -> Self {
        if use_bytes_table::<K>() {
            Self::Bytes(BytesPrecombine::new(combine_fn))
        } else {
            Self::Typed(TypedPrecombine::new(combine_fn))
        }
    }

    fn combine_fn(&self) -> &Arc<CF> {
        match self {
            Self::Bytes(bytes) => bytes.combine_fn(),
            Self::Typed(typed) => &typed.combine_fn,
        }
    }
}

/// Aggregates the elements of a bundle in memory before the shuffle.
///
/// Over the memory limit, the handler emits the coldest accumulators; at bundle end, all
/// others. There is one accumulator per key and window assignment. When `K` has canonical
/// wire bytes, `process` looks up borrowed key slices without decoding `K` on a hit. Each
/// bundle processor has its own clone and runs one bundle at a time.
pub(super) struct PartialCombineHandler<K, CF: CombineFn> {
    state: PrecombineState<K, CF>,
    /// Reused buffer when `process_value` falls back to encoded bytes.
    encoded: Vec<u8>,
}

impl<K, CF> PartialCombineHandler<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    pub(super) fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            state: PrecombineState::new(combine_fn),
            encoded: Vec::new(),
        }
    }
}

/// A clone starts with an empty table, because accumulators belong to their bundle.
impl<K, CF> Clone for PartialCombineHandler<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    fn clone(&self) -> Self {
        Self::new(Arc::clone(self.state.combine_fn()))
    }
}

impl<K, CF> BundleHandler for PartialCombineHandler<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    fn start_bundle(&mut self) -> Result<(), String> {
        match &mut self.state {
            PrecombineState::Bytes(bytes) => bytes.start_bundle(),
            PrecombineState::Typed(typed) => typed.start_bundle(),
        }
        Ok(())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        if use_bytes_table::<K>() {
            if let PrecombineState::Typed(typed) = &self.state
                && typed.is_empty()
            {
                let combine_fn = Arc::clone(&typed.combine_fn);
                self.state = PrecombineState::Bytes(BytesPrecombine::new(combine_fn));
            }
        } else if let PrecombineState::Bytes(bytes) = &self.state
            && bytes.is_empty()
        {
            let combine_fn = Arc::clone(bytes.combine_fn());
            self.state = PrecombineState::Typed(TypedPrecombine::new(combine_fn));
        }

        match &mut self.state {
            PrecombineState::Bytes(bytes) => {
                let (key_bytes, value_bytes) = split_key::<K>(element)?;
                bytes.process_split(key_bytes, value_bytes, validate_key::<K>, ctx)
            }
            PrecombineState::Typed(typed) => typed.process_encoded(element, ctx),
        }
    }

    fn process_value(
        &mut self,
        mut element: TypedElement<'_>,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let value = (!ctx.header.is_multi_window())
            .then(|| element.take_fused::<(K, CF::Input)>())
            .flatten();
        match value {
            Some((key, value)) => {
                if let PrecombineState::Bytes(bytes) = &self.state
                    && bytes.is_empty()
                {
                    let combine_fn = Arc::clone(bytes.combine_fn());
                    self.state = PrecombineState::Typed(TypedPrecombine::new(combine_fn));
                }
                match &mut self.state {
                    PrecombineState::Typed(typed) => typed.add_single_window(key, value, ctx),
                    PrecombineState::Bytes(bytes) => bytes.add_key_value(&key, value, ctx),
                }
            }
            None => {
                let mut buf = std::mem::take(&mut self.encoded);
                let result = element
                    .encode_into(&mut buf)
                    .and_then(|()| self.process(&buf, ctx));
                self.encoded = buf;
                result
            }
        }
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        match &mut self.state {
            PrecombineState::Bytes(bytes) => bytes.finish_bundle(ctx),
            PrecombineState::Typed(typed) => typed.finish_bundle(ctx),
        }
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}
