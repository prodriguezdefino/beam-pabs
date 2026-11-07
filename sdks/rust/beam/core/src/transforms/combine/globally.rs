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

//! `CombineGlobally`: a whole-collection combine built on `CombinePerKey`.

use std::sync::Arc;

use super::{CombineFn, CombinePerKey};
use crate::pipeline::{PipelineError, URN_COMBINE_GLOBALLY, URN_COMBINE_PER_KEY};
use crate::transforms::{
    Create, DisplayDataBuilder, DoFn, Flatten, HasDisplayData, Map, PTransform, ParDo,
    ProcessContext,
};
use beam_model::pipeline::FunctionSpec;

use crate::values::{AnySideInput, IsBounded, IterView, PCollection};
use crate::windowing::is_globally_windowed;

/// Aggregates all values of a [`PCollection`] with a [`CombineFn`].
///
/// Keys each element with `()` and uses [`CombinePerKey`], so it gets partial combining
/// and runner lifting. The output has one element for each pane of the combine. An empty
/// input gives the identity value; [`without_defaults`](Self::without_defaults) emits nothing
/// instead.
/// With defaults, an input outside the global window or an unbounded input is a construction
/// error, which [`Pipeline::validate`](crate::pipeline::Pipeline::validate) returns.
pub struct CombineGlobally<CF> {
    combine_fn: Arc<CF>,
    insert_default: bool,
    fanout: usize,
}

impl<CF: CombineFn> CombineGlobally<CF> {
    /// Creates a transform that combines all elements in each window with `combine_fn`.
    pub fn new(combine_fn: CF) -> Self {
        Self {
            combine_fn: Arc::new(combine_fn),
            insert_default: true,
            fanout: 1,
        }
    }

    /// Emits nothing for an empty input rather than the identity value.
    ///
    /// Call this for input outside the global window or for unbounded input. Only the global
    /// window always exists; in other windowing an empty window does not exist, so there is
    /// no window and no time for the default. An unbounded input does not end, so there is
    /// no time for the default.
    pub fn without_defaults(mut self) -> Self {
        self.insert_default = false;
        self
    }

    /// Splits the input across `fanout` intermediate shards before the final merge.
    ///
    /// Each bundle picks one shard in `0..fanout`, so the spread grows with the number of
    /// bundles. Values `0` and `1` turn fanout off.
    pub fn with_fanout(mut self, fanout: usize) -> Self {
        self.fanout = fanout;
        self
    }
}

impl<CF: CombineFn> HasDisplayData for CombineGlobally<CF> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "CombineGlobally");
        builder.add_text("combine_fn", std::any::type_name::<CF>());
        if self.fanout > 1 {
            builder.add_integer("fanout", i64::try_from(self.fanout).unwrap_or(i64::MAX));
        }
    }
}

/// Emits the identity value of the combine when the input was empty.
///
/// The main input is one synthetic element, so the DoFn runs once. It reads the result of
/// the combine as a side input and emits the identity value only if there is no result.
struct InjectDefaultFn<CF: CombineFn> {
    combine_fn: Arc<CF>,
    view: IterView<CF::Output>,
}

impl<CF: CombineFn> Clone for InjectDefaultFn<CF> {
    fn clone(&self) -> Self {
        Self {
            combine_fn: Arc::clone(&self.combine_fn),
            view: self.view.clone(),
        }
    }
}

impl<CF: CombineFn> DoFn for InjectDefaultFn<CF> {
    type In = ();
    type Out = CF::Output;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.view]
    }

    fn process_element(
        &mut self,
        _seed: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        match ctx.side_input(&self.view)?.iter()?.next() {
            Some(first) => first.map(drop),
            None => ctx.emit(
                self.combine_fn
                    .extract_output(self.combine_fn.create_accumulator()?)?,
            ),
        }
    }
}

impl<CF> PTransform<PCollection<CF::Input>> for CombineGlobally<CF>
where
    CF: CombineFn,
{
    type Output = PCollection<CF::Output>;

    fn expand(self, input: &PCollection<CF::Input>) -> PCollection<CF::Output> {
        let pipeline = input.pipeline();
        let name = pipeline.scope_name().unwrap_or_default();
        let output = self.expand_steps(&name, input);

        // Single-stage global combines carry the payload of their `CombinePerKey`. A
        // fanned-out combine leaves the outer spec unset so runners keep both stages.
        let payload = {
            let lock = pipeline.lock();
            let first = lock
                .name_scopes
                .last()
                .map_or(0, |scope| scope.first_transform);
            lock.outermost_transforms_from(first)
                .iter()
                .filter_map(|id| lock.components.transforms.get(id))
                .filter_map(|t| t.spec.as_ref())
                .find(|spec| spec.urn == URN_COMBINE_PER_KEY)
                .map(|spec| spec.payload.clone())
        };
        if let Some(payload) = payload {
            pipeline.set_scope_spec(FunctionSpec {
                urn: URN_COMBINE_GLOBALLY.to_string(),
                payload,
            });
        }
        let mut builder = DisplayDataBuilder::with_namespace(name);
        self.populate_display_data(&mut builder);
        pipeline.set_scope_display_data(builder.into_proto());
        output
    }
}

impl<CF: CombineFn> CombineGlobally<CF> {
    /// Applies the steps of the combine. `name` is the full name of the combine.
    fn expand_steps(&self, name: &str, input: &PCollection<CF::Input>) -> PCollection<CF::Output> {
        let combined = input
            .apply("KeyWithVoid", Map::new(|element: CF::Input| ((), element)))
            .apply(
                "Combine",
                CombinePerKey::shared(Arc::clone(&self.combine_fn))
                    .with_hot_key_fanout(self.fanout),
            )
            .apply(
                "ExtractValues",
                Map::new(|(_key, val): ((), CF::Output)| val),
            );

        if !self.insert_default {
            return combined;
        }

        // A default value only means something in the global window: a non-global window
        // that nothing arrived in does not exist, so there is nowhere to put the default.
        // The default branch reads the result as a side input in the global window. That
        // side input is ready only when the input ends, so the input must be bounded.
        // Both are configuration errors: record them and return the combined values.
        let reason = if !is_globally_windowed(input) {
            Some(
                "a default value for an empty input needs the global window. Call \
                 .without_defaults() to emit nothing for an empty window instead.",
            )
        } else if input.is_bounded() != IsBounded::Bounded {
            Some(
                "a default value for an empty input needs a bounded input. Call \
                 .without_defaults() to emit only the combined values.",
            )
        } else {
            None
        };
        if let Some(reason) = reason {
            input
                .pipeline()
                .record_construction_error(PipelineError::InvalidConstruction {
                    transform_id: name.to_string(),
                    reason: reason.to_string(),
                });
            return combined;
        }

        // A DoFn on an empty input never runs, so read the result as a side input. The
        // output keeps every pane of the combine, with its pane info. The default branch emits
        // only for an empty input.
        let view = combined.as_iter();
        let default = input
            .pipeline()
            .apply("DefaultSeed", Create::new(vec![()]))
            .apply(
                "InjectDefault",
                ParDo::new(InjectDefaultFn {
                    combine_fn: Arc::clone(&self.combine_fn),
                    view,
                }),
            );
        Flatten::pcollections("WithDefault", &[&combined, &default])
    }
}
