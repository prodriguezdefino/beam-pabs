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

//! `CombinePerKey` and the DoFns of its merge and lifted stages.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;
use prost::Message;

use super::CombineFn;
use super::partial::PartialCombineFn;
use crate::coders::{BeamIterable, DefaultCoder};
use crate::pipeline::{
    COMBINE_STAGE_EXTRACT, COMBINE_STAGE_MERGE, COMBINE_STAGE_PRECOMBINE,
    URN_COMBINE_FN_HANDLER_KEY, URN_COMBINE_PER_KEY, combine_stage_key,
};
use crate::transforms::dofn::DoFnHandler;
use crate::transforms::{
    DisplayDataBuilder, DoFn, GroupByKey, HasDisplayData, PTransform, ParDo, ProcessContext,
    TransformFn,
};
use crate::values::PCollection;

/// Aggregates the values of each key with a [`CombineFn`].
///
/// ```text
/// (K, V)  --PartialCombine-->  (K, Accum)  --GroupByKey-->  (K, [Accum])  --Merge-->  (K, Out)
/// ```
///
/// About one accumulator per key and bundle goes through the shuffle, so shuffled data
/// grows with distinct keys, not with elements as with a direct [`GroupByKey`].
pub struct CombinePerKey<K, CF> {
    name: String,
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF: CombineFn> CombinePerKey<K, CF> {
    pub fn new(name: impl Into<String>, combine_fn: CF) -> Self {
        Self::from_arc(name, Arc::new(combine_fn))
    }

    /// Like `new`, with a shared `combine_fn`.
    pub fn from_arc(name: impl Into<String>, combine_fn: Arc<CF>) -> Self {
        Self {
            name: name.into(),
            combine_fn,
            _marker: PhantomData,
        }
    }
}

impl<K, CF: CombineFn> HasDisplayData for CombinePerKey<K, CF> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "CombinePerKey");
        builder.add_text("name", &self.name);
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
        let name = pipeline.unique_transform_name(&self.name);
        let accum_coder_id = CF::Accum::register_coder(pipeline);

        let partial = input.apply(ParDo::new(
            format!("{name}/PartialCombine"),
            PartialCombineFn::new(Arc::clone(&self.combine_fn)),
        ));

        let grouped = partial.apply(GroupByKey::new(format!("{name}/GroupAccumulators")));

        let output = grouped.apply(ParDo::new(
            format!("{name}/MergeAccumulators"),
            MergeAccumulatorsFn {
                combine_fn: Arc::clone(&self.combine_fn),
                _marker: PhantomData::<K>,
            },
        ));

        let partial_id = pipeline
            .producer_transform_id(partial.id())
            .expect("PartialCombine transform must exist");
        let gbk_id = pipeline
            .producer_transform_id(grouped.id())
            .expect("GroupAccumulators transform must exist");
        let merge_id = pipeline
            .producer_transform_id(output.id())
            .expect("MergeAccumulators transform must exist");

        // The combine function does not serialize, so the payload holds the composite name
        // as the handler key for runner-lifted stages. See `URN_COMBINE_FN_HANDLER_KEY`.
        let payload = proto::CombinePayload {
            combine_fn: Some(proto::FunctionSpec {
                urn: URN_COMBINE_FN_HANDLER_KEY.to_string(),
                payload: name.clone().into_bytes(),
            }),
            accumulator_coder_id: accum_coder_id,
        };
        let mut payload_bytes = Vec::new();
        payload
            .encode(&mut payload_bytes)
            .expect("encode CombinePayload");

        let inputs = HashMap::from([("in".to_string(), input.id().to_string())]);
        let outputs = HashMap::from([("out".to_string(), output.id().to_string())]);
        let subtransforms = vec![partial_id, gbk_id, merge_id];

        let transform_id = pipeline.add_composite_transform(
            &name,
            Some(URN_COMBINE_PER_KEY),
            payload_bytes,
            inputs,
            outputs,
            subtransforms,
        );

        let mut builder = DisplayDataBuilder::with_namespace(name.clone());
        self.populate_display_data(&mut builder);
        pipeline.set_transform_display_data(&transform_id, builder.into_proto());

        let lift_handler: TransformFn = Arc::new(DoFnHandler::new(PartialCombineFn::<K, CF>::new(
            Arc::clone(&self.combine_fn),
        )));
        let merge_handler: TransformFn = Arc::new(DoFnHandler::new(MergeOnlyFn {
            combine_fn: Arc::clone(&self.combine_fn),
            _marker: PhantomData::<K>,
        }));
        let extract_handler: TransformFn = Arc::new(DoFnHandler::new(ExtractOnlyFn {
            combine_fn: Arc::clone(&self.combine_fn),
            _marker: PhantomData::<K>,
        }));

        // A runner that lifts this combine (for example Prism) asks for the DoFn of each
        // stage. The key is the composite name plus the standard stage name, both from the
        // stage payload and URN. Do not use the transform id: runners make their own ids
        // (Prism uses `e{composite}_lift`).
        let stage_handlers: [(&str, TransformFn); 3] = [
            (COMBINE_STAGE_PRECOMBINE, lift_handler),
            (COMBINE_STAGE_MERGE, merge_handler),
            (COMBINE_STAGE_EXTRACT, extract_handler),
        ];

        for (stage, handler) in stage_handlers {
            pipeline.register_transform_handler(combine_stage_key(&name, stage), handler);
        }

        output
    }
}

/// Merges the accumulators of a key from all bundles and emits the final value.
struct MergeAccumulatorsFn<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
}

impl<K, CF> Clone for MergeAccumulatorsFn<K, CF> {
    fn clone(&self) -> Self {
        Self {
            combine_fn: Arc::clone(&self.combine_fn),
            _marker: PhantomData,
        }
    }
}

impl<K, CF> DoFn for MergeAccumulatorsFn<K, CF>
where
    K: DefaultCoder,
    CF: CombineFn,
{
    type In = (K, BeamIterable<CF::Accum>);
    type Out = (K, CF::Output);

    fn process_element(
        &mut self,
        (key, accumulators): (K, BeamIterable<CF::Accum>),
        out: &mut ProcessContext<(K, CF::Output)>,
    ) -> crate::Result {
        let acc_vec = accumulators
            .into_vec()
            .map_err(|e| format!("Failed to fetch accumulators: {e}"))?;
        let merged = self.combine_fn.merge_accumulators(acc_vec);
        out.emit((key, self.combine_fn.extract_output(merged)))
    }
}

/// Merge stage of a lifted combine.
struct MergeOnlyFn<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
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
        let merged = self.combine_fn.merge_accumulators(acc_vec);
        out.emit((key, merged))
    }
}

/// Extract stage of a lifted combine.
struct ExtractOnlyFn<K, CF> {
    combine_fn: Arc<CF>,
    _marker: PhantomData<K>,
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
        out.emit((key, self.combine_fn.extract_output(accumulator)))
    }
}
