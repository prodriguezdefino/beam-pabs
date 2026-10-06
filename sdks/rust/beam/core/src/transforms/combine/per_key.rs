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

//! `CombinePerKey` and the DoFns of its stages.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;
use prost::Message;

use super::CombineFn;
use super::partial::PartialCombineFn;
use super::registered::{CombineSpecs, CombineStage};
use crate::coders::{BeamIterable, DefaultCoder};
use crate::pipeline::{
    URN_COMBINE_GROUPED_VALUES, URN_COMBINE_PER_KEY, URN_RUST_COMBINE_FN, stage_key,
};
use crate::transforms::dofn::DoFnHandler;
use crate::transforms::{
    DisplayDataBuilder, DoFn, GroupByKey, HasDisplayData, PTransform, ProcessContext, TransformFn,
};
use crate::values::PCollection;

/// Aggregates the values of each key with a [`CombineFn`].
///
/// ```text
/// (K, V)  --GroupByKey-->  (K, [V])  --CombineGroupedValues-->  (K, Out)
/// ```
///
/// The composite carries a `CombinePayload`, so a runner that lifts it combines the values of
/// each key before the shuffle: about one accumulator per key and bundle goes through the
/// shuffle. The lifted stages run in this SDK, with the same functions as the stages above.
pub struct CombinePerKey<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF: CombineFn> CombinePerKey<K, CF> {
    /// Creates a transform that combines the values of each key and window with `combine_fn`.
    pub fn new(combine_fn: CF) -> Self {
        Self::shared(Arc::new(combine_fn))
    }

    /// Creates a per-key transform that shares its combine function with the caller.
    pub(super) fn shared(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            _marker: PhantomData,
        }
    }
}

impl<K, CF: CombineFn> HasDisplayData for CombinePerKey<K, CF> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "CombinePerKey");
        builder.add_text("combine_fn", std::any::type_name::<CF>());
    }
}

impl<K, CF> PTransform<PCollection<(K, CF::Input)>> for CombinePerKey<K, CF>
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    type Output = PCollection<(K, CF::Output)>;

    fn expand(&self, input: &PCollection<(K, CF::Input)>) -> PCollection<(K, CF::Output)> {
        let pipeline = input.pipeline();
        let name = pipeline.scope_name().unwrap_or_default();
        let accum_coder_id = CF::Accum::register_coder(pipeline);
        let key_coder_id = pipeline.coder_components(input.coder_id()).first().cloned();

        // A registered `CombineFn` goes into the payload, so the worker needs no rebuilt
        // pipeline for any stage. If the key coder cannot be determined (e.g. custom KV coder
        // without registered components) or is not registered, fall back to the rebuild path,
        // where the payload holds the composite name as the handler key.
        let specs = CombineSpecs::new::<K, CF>(
            pipeline,
            &name,
            self.combine_fn.as_ref(),
            key_coder_id.as_deref(),
        );
        let combine_fn = specs.as_ref().map_or_else(
            || proto::FunctionSpec {
                urn: URN_RUST_COMBINE_FN.to_string(),
                payload: name.clone().into_bytes(),
            },
            CombineSpecs::combine_fn,
        );
        let payload = proto::CombinePayload {
            combine_fn: Some(combine_fn),
            accumulator_coder_id: accum_coder_id,
        }
        .encode_to_vec();

        let grouped = input.apply("GroupByKey", GroupByKey::new());
        let output = pipeline.add_scoped_primitive("CombineGroupedValues", |stage| {
            combine_grouped_values::<K, CF>(&stage, &grouped, key_coder_id, payload.clone())
        });
        pipeline.set_scope_spec(proto::FunctionSpec {
            urn: URN_COMBINE_PER_KEY.to_string(),
            payload,
        });

        let mut builder = DisplayDataBuilder::with_namespace(name.clone());
        self.populate_display_data(&mut builder);
        pipeline.set_scope_display_data(builder.into_proto());

        if specs.is_some() {
            return output;
        }

        // A runner asks for the function of each stage by the composite name and the stage
        // name, both from the stage payload and URN. Do not use the transform id: runners make
        // their own ids (Prism uses `e{composite}_lift`).
        for stage in [
            CombineStage::Precombine,
            CombineStage::Merge,
            CombineStage::Extract,
            CombineStage::ConvertToAccumulators,
            CombineStage::CombineGroupedValues,
        ] {
            pipeline.register_transform_handler(
                stage_key(&name, stage),
                typed_stage::<K, CF>(stage, Arc::clone(&self.combine_fn)),
            );
        }

        output
    }
}

/// Builds the handler of `stage` for a concrete key type `K`.
pub(super) fn typed_stage<K, CF>(stage: CombineStage, combine_fn: Arc<CF>) -> TransformFn
where
    K: DefaultCoder + Eq + Hash,
    CF: CombineFn,
{
    match stage {
        CombineStage::Precombine => {
            Arc::new(DoFnHandler::new(PartialCombineFn::<K, CF>::new(combine_fn)))
        }
        CombineStage::Merge => Arc::new(DoFnHandler::new(MergeOnlyFn::<K, CF>::new(combine_fn))),
        CombineStage::Extract => {
            Arc::new(DoFnHandler::new(ExtractOnlyFn::<K, CF>::new(combine_fn)))
        }
        CombineStage::ConvertToAccumulators => Arc::new(DoFnHandler::new(
            ConvertToAccumulatorsFn::<K, CF>::new(combine_fn),
        )),
        CombineStage::CombineGroupedValues => Arc::new(DoFnHandler::new(CombineGroupedValuesFn::<
            K,
            CF,
        >::new(combine_fn))),
    }
}

/// Adds the `combine_grouped_values` step `name` after `grouped`, and returns its output. The
/// output keeps the key coder of the input, so a row key keeps its schema.
fn combine_grouped_values<K, CF>(
    name: &str,
    grouped: &PCollection<(K, BeamIterable<CF::Input>)>,
    key_coder_id: Option<String>,
    payload: Vec<u8>,
) -> PCollection<(K, CF::Output)>
where
    K: DefaultCoder,
    CF: CombineFn,
{
    let pipeline = grouped.pipeline();
    let coder_id = match key_coder_id {
        Some(key) => {
            let value = CF::Output::register_coder(pipeline);
            pipeline.register_kv_coder(key, value)
        }
        None => <(K, CF::Output)>::register_coder(pipeline),
    };
    let output = pipeline.add_pcollection_with_windowing::<(K, CF::Output)>(
        &format!("{name}_out"),
        &coder_id,
        grouped.is_bounded(),
        &grouped.windowing_strategy_id(),
    );
    let inputs = HashMap::from([("in".to_string(), grouped.id().to_string())]);
    let outputs = HashMap::from([("out".to_string(), output.id().to_string())]);
    pipeline.add_transform(name, URN_COMBINE_GROUPED_VALUES, payload, inputs, outputs);
    output
}

/// Combines the grouped values of a key into the output value, when no runner lifts the
/// combine.
pub(super) struct CombineGroupedValuesFn<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF> CombineGroupedValuesFn<K, CF> {
    pub(super) fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            _marker: PhantomData,
        }
    }
}

impl<K, CF> Clone for CombineGroupedValuesFn<K, CF> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.combine_fn))
    }
}

impl<K, CF> DoFn for CombineGroupedValuesFn<K, CF>
where
    K: DefaultCoder,
    CF: CombineFn,
{
    type In = (K, BeamIterable<CF::Input>);
    type Out = (K, CF::Output);

    fn process_element(
        &mut self,
        (key, values): Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> crate::Result {
        let accumulator = values.try_into_iter().try_fold(
            self.combine_fn.create_accumulator()?,
            |accumulator, value| {
                let value = value.map_err(|e| format!("Failed to read a grouped value: {e}"))?;
                self.combine_fn.add_input(accumulator, value)
            },
        )?;
        out.emit((key, self.combine_fn.extract_output(accumulator)?))
    }
}

/// Lifted stage that turns each value into an accumulator of its own.
pub(super) struct ConvertToAccumulatorsFn<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF> ConvertToAccumulatorsFn<K, CF> {
    pub(super) fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            _marker: PhantomData,
        }
    }
}

impl<K, CF> Clone for ConvertToAccumulatorsFn<K, CF> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.combine_fn))
    }
}

impl<K, CF> DoFn for ConvertToAccumulatorsFn<K, CF>
where
    K: DefaultCoder,
    CF: CombineFn,
{
    type In = (K, CF::Input);
    type Out = (K, CF::Accum);

    fn process_element(
        &mut self,
        (key, value): Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> crate::Result {
        let accumulator = self
            .combine_fn
            .add_input(self.combine_fn.create_accumulator()?, value)?;
        out.emit((key, accumulator))
    }
}

/// Merge stage of a lifted combine.
pub(super) struct MergeOnlyFn<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF> MergeOnlyFn<K, CF> {
    pub(super) fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            _marker: PhantomData,
        }
    }
}

impl<K, CF> Clone for MergeOnlyFn<K, CF> {
    fn clone(&self) -> Self {
        Self {
            combine_fn: Arc::clone(&self.combine_fn),
            _marker: PhantomData,
        }
    }
}

impl<K, CF> DoFn for MergeOnlyFn<K, CF>
where
    K: DefaultCoder,
    CF: CombineFn,
{
    type In = (K, BeamIterable<CF::Accum>);
    type Out = (K, CF::Accum);

    fn process_element(
        &mut self,
        (key, accumulators): (K, BeamIterable<CF::Accum>),
        out: &mut ProcessContext<(K, CF::Accum)>,
    ) -> crate::Result {
        let acc_vec = accumulators
            .into_vec()
            .map_err(|e| format!("Failed to fetch accumulators: {e}"))?;
        let merged = self.combine_fn.merge_accumulators(acc_vec)?;
        out.emit((key, merged))
    }
}

/// Extract stage of a lifted combine.
pub(super) struct ExtractOnlyFn<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF> ExtractOnlyFn<K, CF> {
    pub(super) fn new(combine_fn: Arc<CF>) -> Self {
        Self {
            combine_fn,
            _marker: PhantomData,
        }
    }
}

impl<K, CF> Clone for ExtractOnlyFn<K, CF> {
    fn clone(&self) -> Self {
        Self {
            combine_fn: Arc::clone(&self.combine_fn),
            _marker: PhantomData,
        }
    }
}

impl<K, CF> DoFn for ExtractOnlyFn<K, CF>
where
    K: DefaultCoder,
    CF: CombineFn,
{
    type In = (K, CF::Accum);
    type Out = (K, CF::Output);

    fn process_element(
        &mut self,
        (key, accumulator): (K, CF::Accum),
        out: &mut ProcessContext<(K, CF::Output)>,
    ) -> crate::Result {
        out.emit((key, self.combine_fn.extract_output(accumulator)?))
    }
}
