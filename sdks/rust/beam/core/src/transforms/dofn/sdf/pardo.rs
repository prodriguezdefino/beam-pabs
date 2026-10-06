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

//! The ParDo transform that applies a splittable DoFn.

use std::sync::Arc;

use super::handler::SplittableDoFnHandler;
use super::splittable_dofn::{Element, SplittableDoFn, WithWatermarkState};
use crate::coders::DefaultCoder;
use crate::internals::ParDoRegistration;
use crate::transforms::PTransform;
use crate::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use crate::transforms::dofn::outputs::{OutputDeclarer, Outputs};
use crate::transforms::dofn::pardo::{SideInputSpecs, side_input_specs};
use crate::transforms::dofn::registered::graph_spec;
use crate::values::PCollection;

/// Applies a [`SplittableDoFn`] to every element of a [`PCollection`].
pub struct SplittableParDo<S> {
    func: Arc<S>,
    side_inputs: SideInputSpecs,
    resource_hints: crate::pipeline::resources::ResourceHints,
}

impl<S: SplittableDoFn> SplittableParDo<S> {
    pub fn new(func: S) -> Self {
        let side_inputs = side_input_specs(func.side_inputs());
        Self {
            func: Arc::new(func),
            side_inputs,
            resource_hints: crate::pipeline::resources::ResourceHints::new(),
        }
    }

    /// Sets resource hints on this transform, for example the memory of a heavy reader.
    pub fn with_resource_hints(mut self, hints: crate::pipeline::resources::ResourceHints) -> Self {
        self.resource_hints = hints;
        self
    }

    /// Attaches a single resource hint by URN and serialized payload.
    pub fn with_resource_hint(
        mut self,
        urn: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        self.resource_hints = self.resource_hints.with_hint(urn, payload);
        self
    }
}

impl<S: SplittableDoFn> HasDisplayData for SplittableParDo<S> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("fn", std::any::type_name::<S>());
        if !self.side_inputs.is_empty() {
            builder.add_integer("num_side_inputs", self.side_inputs.len() as i64);
        }
        self.func.populate_display_data(builder);
    }
}

impl<S: SplittableDoFn> PTransform<PCollection<Element<S>>> for SplittableParDo<S> {
    type Output = <S::Out as Outputs>::Collections;

    fn expand(&self, input: &PCollection<Element<S>>) -> Self::Output {
        let pipeline = input.pipeline();
        let name = pipeline.primitive_name("SplittableParDo");
        // The runner keeps the state of the watermark estimator with each restriction.
        let restriction_coder_id = WithWatermarkState::<S>::register_coder(pipeline);
        let ws_id = input.windowing_strategy_id();
        let is_bounded = [input.is_bounded(), self.func.boundedness()]
            .into_iter()
            .collect();
        let mut declarer = OutputDeclarer::new(pipeline, &name, &ws_id, is_bounded, 0);
        let collections = S::Out::declare(&mut declarer);

        let mut builder = DisplayDataBuilder::with_namespace(name.clone());
        self.populate_display_data(&mut builder);

        // The harness gets the expanded SDF stages from `stage_handler` of the one handler. A
        // registered type goes into the spec, so the worker needs no rebuilt pipeline.
        let spec = graph_spec(pipeline, &name, self.func.as_ref());
        let outputs = declarer.into_tags();
        let registration = ParDoRegistration::new(pipeline, name, input.id())
            .outputs(outputs)
            .side_inputs(self.side_inputs.clone())
            .restriction_coder(restriction_coder_id)
            .requests_finalization(<S::Out as Outputs>::FINALIZES)
            .resource_hints(self.resource_hints.clone())
            .display_data(builder.into_proto());
        match spec {
            Some(spec) => registration.register_spec(spec),
            None => {
                registration.register(Arc::new(SplittableDoFnHandler::new(S::clone(&self.func))))
            }
        };

        collections
    }
}
