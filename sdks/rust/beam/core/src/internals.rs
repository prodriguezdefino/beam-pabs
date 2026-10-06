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

//! Runner and harness plumbing: byte-level handlers, readers and collectors.
//!
//! The only public path for these items. Pipeline code does not need them.
//! Transforms that register a hand-written [`BundleHandler`] use [`ParDoRegistration`].

use std::collections::HashMap;

use model::pipeline as proto;

use crate::pipeline::constants::URN_RUST_DOFN;
use crate::pipeline::resources::ResourceHints;
use crate::pipeline::{Pipeline, URN_PAR_DO};

pub use crate::transforms::dofn::DoFnHandler;
pub use crate::transforms::dofn::context::{
    BundleFinalizerCollector, FinalizationCallback, HandlerContext, ResidualApplication,
    ResidualCollector,
};
pub use crate::transforms::dofn::side_input::{
    ScopedSideInputReader, SideInputReader, extract_side_input_tags,
};
pub use crate::transforms::dofn::state::{AnyStateSpec, UserStateReader};
pub use crate::transforms::dofn::timer::{TimerCollector, TimerMutation};
pub use crate::transforms::handler::{
    BundleHandler, ElementSink, HandlerInstance, TransformFn, TypedElement,
};
pub use crate::values::SideInputKind;

/// A foldhash `HashMap` for tables hashed per element. Crate-private, so public APIs
/// keep the standard `HashMap` and foldhash stays out of the SDK's semver surface.
pub(crate) type FastHashMap<K, V> = HashMap<K, V, foldhash::fast::RandomState>;

/// Adds one primitive `ParDo` to a pipeline's graph and registers its handler. Every `ParDo` in
/// the SDK, including [`ParDo`](crate::transforms::ParDo), goes through this builder. Only the
/// name, the main input and the handler are required:
///
/// ```
/// use std::sync::Arc;
/// use beam::internals::{ElementSink, ParDoRegistration};
/// use beam::prelude::*;
/// use beam::values::IsBounded;
///
/// let p = Pipeline::new();
/// let input = p.impulse();
/// let output = p.add_pcollection::<Vec<u8>>("Echo.out", input.coder_id(), IsBounded::Bounded);
///
/// let id = ParDoRegistration::new(&p, "Echo", input.id())
///     .output("out", output.id())
///     .register(Arc::new(|e: &[u8], out: &mut dyn ElementSink| out.push(e.to_vec())));
/// assert_eq!(id, "Echo");
/// ```
///
/// The handler key is the transform's unique name, which the `ParDoPayload` `do_fn` also
/// carries. The harness resolves handlers from that payload, so the key survives runners
/// that rename or fuse transforms.
#[must_use = "a ParDoRegistration does nothing until `register` is called"]
pub struct ParDoRegistration<'p> {
    pipeline: &'p Pipeline,
    name: String,
    input_id: String,
    outputs: HashMap<String, String>,
    side_inputs: HashMap<String, (String, proto::SideInput)>,
    state_specs: HashMap<String, proto::StateSpec>,
    timer_family_specs: HashMap<String, proto::TimerFamilySpec>,
    restriction_coder_id: String,
    requests_finalization: bool,
    resource_hints: Option<ResourceHints>,
    display_data: Vec<proto::DisplayData>,
}

impl<'p> ParDoRegistration<'p> {
    /// Starts a `ParDo` named `name` (made unique within the pipeline on
    /// [`register`](Self::register)) reading the PCollection `input_id`.
    pub fn new(
        pipeline: &'p Pipeline,
        name: impl Into<String>,
        input_id: impl Into<String>,
    ) -> Self {
        Self {
            pipeline,
            name: name.into(),
            input_id: input_id.into(),
            outputs: HashMap::new(),
            side_inputs: HashMap::new(),
            state_specs: HashMap::new(),
            timer_family_specs: HashMap::new(),
            restriction_coder_id: String::new(),
            requests_finalization: false,
            resource_hints: None,
            display_data: Vec::new(),
        }
    }

    /// Adds the output PCollection `pcollection_id` under output tag `tag`.
    pub fn output(mut self, tag: impl Into<String>, pcollection_id: impl Into<String>) -> Self {
        self.outputs.insert(tag.into(), pcollection_id.into());
        self
    }

    /// Adds several outputs, as `(tag, pcollection_id)` pairs.
    pub fn outputs(mut self, outputs: impl IntoIterator<Item = (String, String)>) -> Self {
        self.outputs.extend(outputs);
        self
    }

    /// Declares side inputs, keyed by tag, as `(pcollection_id, spec)`.
    pub fn side_inputs(
        mut self,
        side_inputs: impl IntoIterator<Item = (String, (String, proto::SideInput))>,
    ) -> Self {
        self.side_inputs.extend(side_inputs);
        self
    }

    /// Declares user state cells, keyed by state id.
    pub fn state_specs(
        mut self,
        specs: impl IntoIterator<Item = (String, proto::StateSpec)>,
    ) -> Self {
        self.state_specs.extend(specs);
        self
    }

    /// Declares timer families, keyed by family name.
    pub fn timer_family_specs(
        mut self,
        specs: impl IntoIterator<Item = (String, proto::TimerFamilySpec)>,
    ) -> Self {
        self.timer_family_specs.extend(specs);
        self
    }

    /// Marks the `ParDo` as splittable, with restrictions encoded by `coder_id`.
    pub fn restriction_coder(mut self, coder_id: impl Into<String>) -> Self {
        self.restriction_coder_id = coder_id.into();
        self
    }

    /// Declares that the handler registers bundle finalization callbacks. The stage then carries
    /// `beam:requirement:pardo:finalization:v1`, so the runner sends `FinalizeBundle` after a
    /// bundle of this transform commits. Without it, the callbacks never run.
    pub fn requests_finalization(mut self, requests: bool) -> Self {
        self.requests_finalization = requests;
        self
    }

    /// Attaches resource hints, merged over any scoped hints. Empty hints are ignored.
    pub fn resource_hints(mut self, hints: ResourceHints) -> Self {
        self.resource_hints = (!hints.is_empty()).then_some(hints);
        self
    }

    pub fn display_data(mut self, display_data: Vec<proto::DisplayData>) -> Self {
        self.display_data = display_data;
        self
    }

    /// Records the `ParDo`, registers `handler` for it, and returns its transform id.
    pub fn register(self, handler: TransformFn) -> String {
        use prost::Message;

        let (side_input_pcolls, side_input_specs): (HashMap<_, _>, HashMap<_, _>) = self
            .side_inputs
            .into_iter()
            .map(|(tag, (pcoll_id, spec))| ((tag.clone(), pcoll_id), (tag, spec)))
            .unzip();
        let inputs = std::iter::once(("in".to_string(), self.input_id))
            .chain(side_input_pcolls)
            .collect();

        let transform_id = {
            let mut graph = self.pipeline.lock();
            // The handler key goes into the payload, so it must be the final unique name.
            let unique_name = graph.unique_transform_name(&self.name);
            let payload = proto::ParDoPayload {
                do_fn: Some(proto::FunctionSpec {
                    urn: URN_RUST_DOFN.to_string(),
                    payload: unique_name.as_bytes().to_vec(),
                }),
                side_inputs: side_input_specs,
                state_specs: self.state_specs,
                timer_family_specs: self.timer_family_specs,
                restriction_coder_id: self.restriction_coder_id,
                requests_finalization: self.requests_finalization,
                ..Default::default()
            }
            .encode_to_vec();
            graph.add_transform_with_hints_and_display_data(
                &unique_name,
                URN_PAR_DO,
                payload,
                inputs,
                self.outputs,
                self.resource_hints.as_ref(),
                self.display_data,
            )
        };

        self.pipeline
            .register_transform_handler(transform_id.clone(), handler);
        transform_id
    }
}
