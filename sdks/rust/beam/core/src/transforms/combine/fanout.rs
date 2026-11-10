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

//! Two-phase hot-key fanout expansion and handlers for [`CombinePerKey`](super::CombinePerKey).

use std::any::TypeId;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use beam_model::pipeline as proto;
use serde::{Deserialize, Serialize};

use super::CombineFn;
use super::partial::PartialCombineFn;
use super::per_key::{ExtractOnlyFn, MergeOnlyFn, combine_single_stage, typed_stage};
use super::registered::{CombinePhase, CombineStage};
use crate::coders::{BeamIterable, DefaultCoder};
use crate::internals::ParDoRegistration;
use crate::transforms::dofn::DoFnHandler;
use crate::transforms::dofn::HandlerContext;
use crate::transforms::dofn::registered::{
    FnRegistration, Registered, encode_registered, graph_spec,
};
use crate::transforms::group_by_key::with_continuation_trigger;
use crate::transforms::reshuffle::random_seed;
use crate::transforms::{
    BundleHandler, DoFn, HandlerInstance, PTransform, ProcessContext, TransformFn, TypedElement,
};
use crate::values::PCollection;

/// Process-wide bundle counter seeded per worker so workers start on different shards.
static NEXT_SHARD: LazyLock<AtomicUsize> =
    LazyLock::new(|| AtomicUsize::new(random_seed() as usize));

/// First phase of a fanned-out combine: folds inputs into accumulators and emits accumulators.
pub(super) struct PreCombineFn<CF>(pub(super) Arc<CF>);

impl<CF: CombineFn> CombineFn for PreCombineFn<CF> {
    type Input = CF::Input;
    type Accum = CF::Accum;
    type Output = CF::Accum;

    fn create_accumulator(&self) -> crate::Result<Self::Accum> {
        self.0.create_accumulator()
    }

    fn add_input(&self, acc: Self::Accum, input: Self::Input) -> crate::Result<Self::Accum> {
        self.0.add_input(acc, input)
    }

    fn merge_accumulators(&self, accs: Vec<Self::Accum>) -> crate::Result<Self::Accum> {
        self.0.merge_accumulators(accs)
    }

    fn extract_output(&self, acc: Self::Accum) -> crate::Result<Self::Output> {
        Ok(acc)
    }
}

/// Second phase of a fanned-out combine: merges shard accumulators and extracts the output.
pub(super) struct PostCombineFn<CF>(pub(super) Arc<CF>);

impl<CF: CombineFn> CombineFn for PostCombineFn<CF> {
    type Input = CF::Accum;
    type Accum = CF::Accum;
    type Output = CF::Output;

    fn create_accumulator(&self) -> crate::Result<Self::Accum> {
        self.0.create_accumulator()
    }

    fn add_input(&self, acc: Self::Accum, input: Self::Accum) -> crate::Result<Self::Accum> {
        self.0.merge_accumulators(vec![acc, input])
    }

    fn merge_accumulators(&self, accs: Vec<Self::Accum>) -> crate::Result<Self::Accum> {
        self.0.merge_accumulators(accs)
    }

    fn extract_output(&self, acc: Self::Accum) -> crate::Result<Self::Output> {
        self.0.extract_output(acc)
    }
}

/// Builds the handler of `stage` for the first phase on sharded key `(i32, K)`.
pub(super) fn typed_pre_stage<K, CF>(stage: CombineStage, combine_fn: Arc<CF>) -> TransformFn
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    typed_stage::<(i32, K), PreCombineFn<CF>>(stage, Arc::new(PreCombineFn(combine_fn)))
}

/// Builds the handler of `stage` for the second phase on key `K`.
pub(super) fn typed_post_stage<K, CF>(stage: CombineStage, combine_fn: Arc<CF>) -> TransformFn
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    match stage {
        CombineStage::Precombine => {
            Arc::new(DoFnHandler::new(
                PartialCombineFn::<K, PostCombineFn<CF>>::new(Arc::new(PostCombineFn(combine_fn))),
            ))
        }
        CombineStage::Merge => Arc::new(DoFnHandler::new(MergeOnlyFn::<K, CF>::new(combine_fn))),
        CombineStage::Extract => {
            Arc::new(DoFnHandler::new(ExtractOnlyFn::<K, CF>::new(combine_fn)))
        }
        CombineStage::ConvertToAccumulators => {
            Arc::new(DoFnHandler::new(PassAccumulatorFn::<K, CF::Accum>::new()))
        }
        CombineStage::CombineGroupedValues => Arc::new(DoFnHandler::new(
            PostCombineGroupedValuesFn::<K, CF>::new(combine_fn),
        )),
    }
}

/// Lifted conversion stage for `PostCombine`, whose inputs are already accumulators.
pub(super) struct PassAccumulatorFn<K, A> {
    _marker: PhantomData<(K, A)>,
}

impl<K, A> PassAccumulatorFn<K, A> {
    pub(super) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<K, A> Clone for PassAccumulatorFn<K, A> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<K, A> DoFn for PassAccumulatorFn<K, A>
where
    K: DefaultCoder,
    A: DefaultCoder,
{
    type In = (K, A);
    type Out = (K, A);

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> crate::Result {
        out.emit(element)
    }
}

/// Non-lifted fallback for `PostCombine`: merges grouped accumulators and extracts the output.
pub(super) struct PostCombineGroupedValuesFn<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF> PostCombineGroupedValuesFn<K, CF> {
    pub(super) fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            _marker: PhantomData,
        }
    }
}

impl<K, CF> Clone for PostCombineGroupedValuesFn<K, CF> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.combine_fn))
    }
}

impl<K, CF> DoFn for PostCombineGroupedValuesFn<K, CF>
where
    K: DefaultCoder,
    CF: CombineFn,
{
    type In = (K, BeamIterable<CF::Accum>);
    type Out = (K, CF::Output);

    fn process_element(
        &mut self,
        (key, accumulators): Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> crate::Result {
        let acc_vec = accumulators
            .into_vec()
            .map_err(|e| format!("Failed to fetch accumulators: {e}"))?;
        let merged = if acc_vec.is_empty() {
            self.combine_fn.create_accumulator()?
        } else {
            self.combine_fn.merge_accumulators(acc_vec)?
        };
        out.emit((key, self.combine_fn.extract_output(merged)?))
    }
}

/// Prepends one shard in `0..fanout` per bundle to each encoded `(K, V)` element.
#[derive(Clone)]
struct AddShardHandler {
    fanout: usize,
    shard_bytes: Vec<u8>,
    encoded: Vec<u8>,
    out: Vec<u8>,
}

impl AddShardHandler {
    fn new(fanout: usize) -> Self {
        Self {
            fanout: fanout.max(1),
            shard_bytes: Vec::with_capacity(5),
            encoded: Vec::new(),
            out: Vec::new(),
        }
    }
}

impl BundleHandler for AddShardHandler {
    fn start_bundle(&mut self) -> Result<(), String> {
        let shard = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % self.fanout;
        let shard =
            i32::try_from(shard).map_err(|_| format!("Fanout shard {shard} is not an i32"))?;
        self.shard_bytes.clear();
        shard
            .encode_element(&mut self.shard_bytes)
            .map_err(|e| format!("Failed to encode fanout shard: {e}"))
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        self.out.clear();
        self.out.extend_from_slice(&self.shard_bytes);
        self.out.extend_from_slice(element);
        ctx.sink.push_bytes(&self.out)
    }

    fn process_value(
        &mut self,
        element: TypedElement<'_>,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let mut encoded = std::mem::take(&mut self.encoded);
        let result = element
            .encode_into(&mut encoded)
            .and_then(|()| self.process(&encoded, ctx));
        self.encoded = encoded;
        result
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

/// Strips the leading `i32` shard prefix from each encoded `((i32, K), A)` element.
#[derive(Clone, Default)]
struct StripShardHandler {
    encoded: Vec<u8>,
}

impl BundleHandler for StripShardHandler {
    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let mut rest = element;
        let _ = i32::decode_element(&mut rest)
            .map_err(|e| format!("Failed to decode fanout shard: {e}"))?;
        ctx.sink.push_bytes(rest)
    }

    fn process_value(
        &mut self,
        element: TypedElement<'_>,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let mut encoded = std::mem::take(&mut self.encoded);
        let result = element
            .encode_into(&mut encoded)
            .and_then(|()| self.process(&encoded, ctx));
        self.encoded = encoded;
        result
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct AddShardSpec {
    fanout: usize,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct StripShardSpec;

inventory::submit! {
    FnRegistration {
        id: "beam:rust:combine:add_shard:v1",
        type_id: TypeId::of::<AddShardSpec>,
        encode: encode_registered::<AddShardSpec>,
        build: |fields| {
            let spec: AddShardSpec = serde_json::from_slice(fields)
                .map_err(|e| format!("the function fields do not deserialize: {e}"))?;
            Ok(Registered::Handler(Arc::new(AddShardHandler::new(spec.fanout))))
        },
    }
}

inventory::submit! {
    FnRegistration {
        id: "beam:rust:combine:strip_shard:v1",
        type_id: TypeId::of::<StripShardSpec>,
        encode: encode_registered::<StripShardSpec>,
        build: |fields| {
            let _: StripShardSpec = serde_json::from_slice(fields)
                .map_err(|e| format!("the function fields do not deserialize: {e}"))?;
            Ok(Registered::Handler(Arc::new(StripShardHandler::default())))
        },
    }
}

struct PrePhase<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF> PTransform<PCollection<((i32, K), CF::Input)>> for PrePhase<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    type Output = PCollection<((i32, K), CF::Accum)>;

    fn expand(
        self,
        input: &PCollection<((i32, K), CF::Input)>,
    ) -> PCollection<((i32, K), CF::Accum)> {
        combine_single_stage::<(i32, K), PreCombineFn<CF>, K, CF>(
            input,
            self.combine_fn.as_ref(),
            CombinePhase::Pre,
            |stage| typed_pre_stage::<K, CF>(stage, Arc::clone(&self.combine_fn)),
        )
    }
}

struct PostPhase<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF> PTransform<PCollection<(K, CF::Accum)>> for PostPhase<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    type Output = PCollection<(K, CF::Output)>;

    fn expand(self, input: &PCollection<(K, CF::Accum)>) -> PCollection<(K, CF::Output)> {
        combine_single_stage::<K, PostCombineFn<CF>, K, CF>(
            input,
            self.combine_fn.as_ref(),
            CombinePhase::Post,
            |stage| typed_post_stage::<K, CF>(stage, Arc::clone(&self.combine_fn)),
        )
    }
}

/// Returns the windowing strategies of the first and the second phase for input strategy `ws`.
///
/// First-phase panes discard, so the second phase does not add early panes twice. The second
/// phase keeps the merge status, so session windows merge across shards, and it fires once for
/// each first-phase pane.
fn phase_strategies(
    ws: proto::WindowingStrategy,
) -> (proto::WindowingStrategy, proto::WindowingStrategy) {
    let post = with_continuation_trigger(&ws);
    let pre = match ws.accumulation_mode() {
        proto::accumulation_mode::Enum::Accumulating => proto::WindowingStrategy {
            accumulation_mode: proto::accumulation_mode::Enum::Discarding as i32,
            ..ws
        },
        proto::accumulation_mode::Enum::Unspecified
        | proto::accumulation_mode::Enum::Discarding
        | proto::accumulation_mode::Enum::Retracting => ws,
    };
    (pre, post)
}

/// Expands a two-phase combine across `fanout` per-bundle shards.
pub(super) fn expand_fanout<K, CF>(
    input: &PCollection<(K, CF::Input)>,
    combine_fn: &Arc<CF>,
    fanout: usize,
) -> PCollection<(K, CF::Output)>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    let pipeline = input.pipeline();
    let in_ws_id = input.windowing_strategy_id();
    let input_ws = pipeline
        .lock()
        .components
        .windowing_strategies
        .get(&in_ws_id)
        .cloned();
    let (pre_ws_id, post_ws_id) = match input_ws {
        Some(ws) => {
            let (pre, post) = phase_strategies(ws);
            (
                pipeline.register_windowing_strategy(pre),
                pipeline.register_windowing_strategy(post),
            )
        }
        None => (in_ws_id.clone(), in_ws_id),
    };

    let (k_id, v_id) = pipeline
        .kv_coder_parts(input.coder_id())
        .unwrap_or_else(|| {
            (
                K::register_coder(pipeline),
                CF::Input::register_coder(pipeline),
            )
        });
    // Both phases use the key coder, and `register_kv_coder` takes owned ids.
    let sharded_key_id = pipeline.register_kv_coder(i32::register_coder(pipeline), k_id.clone());
    let sharded_in_id = pipeline.register_kv_coder(sharded_key_id, v_id);

    let sharded = pipeline.add_scoped_primitive("AddShard", |name| {
        let out = pipeline.add_pcollection_with_windowing::<((i32, K), CF::Input)>(
            &format!("{name}_out"),
            &sharded_in_id,
            input.is_bounded(),
            &pre_ws_id,
        );
        let reg = ParDoRegistration::new(pipeline, &name, input.id()).output("out", out.id());
        match graph_spec(pipeline, &AddShardSpec { fanout }) {
            Some(spec) => reg.register_spec(spec),
            None => reg.register(Arc::new(AddShardHandler::new(fanout))),
        };
        out
    });

    let pre_combined = sharded.apply(
        "PreCombine",
        PrePhase::<K, CF> {
            combine_fn: Arc::clone(combine_fn),
            _marker: PhantomData,
        },
    );

    let stripped_id = pipeline.register_kv_coder(k_id, CF::Accum::register_coder(pipeline));
    let stripped = pipeline.add_scoped_primitive("StripShard", |name| {
        let out = pipeline.add_pcollection_with_windowing::<(K, CF::Accum)>(
            &format!("{name}_out"),
            &stripped_id,
            pre_combined.is_bounded(),
            &post_ws_id,
        );
        let reg =
            ParDoRegistration::new(pipeline, &name, pre_combined.id()).output("out", out.id());
        match graph_spec(pipeline, &StripShardSpec) {
            Some(spec) => reg.register_spec(spec),
            None => reg.register(Arc::new(StripShardHandler::default())),
        };
        out
    });

    stripped.apply(
        "PostCombine",
        PostPhase::<K, CF> {
            combine_fn: Arc::clone(combine_fn),
            _marker: PhantomData,
        },
    )
}
