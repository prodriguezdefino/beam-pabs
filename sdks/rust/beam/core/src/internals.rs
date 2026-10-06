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

//! Runner and harness plumbing: byte-level handlers, readers, collectors, and metrics.
//!
//! The only public path for these items. Pipeline code does not need them.
//! Transforms that register a hand-written [`BundleHandler`] use [`ParDoRegistration`].

use std::collections::HashMap;
use std::sync::MutexGuard;

use model::pipeline as proto;

use crate::pipeline::constants::URN_RUST_DOFN;
use crate::pipeline::resources::ResourceHints;
use crate::pipeline::{Pipeline, URN_PAR_DO};
use crate::values::{PCollection, POutput};

pub use crate::metrics::scope::process_wide_container;
pub use crate::transforms::create::create_handler;
pub use crate::transforms::dofn::context::{
    BundleFinalizerCollector, FinalizationCallback, HandlerContext, PendingFinalizer,
    ResidualApplication, ResidualCollector,
};
pub use crate::transforms::dofn::outputs::{Emitter, Indexed, OutputDeclarer};
pub use crate::transforms::dofn::sdf::dynamic_split::{
    DynamicSplitGuard, DynamicSplitHandler, DynamicSplitRegistrar, DynamicSplitResult,
    SdfDynamicSplitter,
};
pub use crate::transforms::dofn::sdf::splittable_dofn::{
    SdfPairWithRestrictionHandler, SdfProcessSizedElementsHandler, SdfSplitAndSizeHandler,
    SdfTruncateSizedRestrictionsHandler, SplittableDoFnHandler,
};
pub use crate::transforms::dofn::side_input::{
    ScopedSideInputReader, SideInputElements, SideInputError, SideInputReader,
    extract_side_input_tags, extract_side_inputs,
};
pub use crate::transforms::dofn::state::{AnyStateSpec, UserStateReader};
pub use crate::transforms::dofn::timer::{
    TimeDomain, TimerCollector, TimerFamilySpec, TimerMutation,
};
pub use crate::transforms::dofn::{
    DoFnHandler, FnRegistration, IntoParDoFn, PerWindow, Registered, RegisteredFn, Splittable,
    StateBinding, StateCell, StateCells, Stateful, Stateless, TimerFamilies, build_registered,
    encode_registered, registered_handler,
};
pub use crate::transforms::handler::{
    BundleHandler, ElementSink, HandlerInstance, TransformFn, TypedElement,
};
pub use crate::windowing::transform::{WindowIntoHandler, window_into_handler_key};
pub use crate::windowing::window_fn::{MergeStatus, decode_window_fn, duration_to_proto};
#[doc(hidden)]
pub use inventory;

/// A foldhash `HashMap` for tables hashed per element. Crate-private, so public APIs
/// keep the standard `HashMap` and foldhash stays out of the SDK's semver surface.
pub(crate) type FastHashMap<K, V> = HashMap<K, V, foldhash::fast::RandomState>;

pub use crate::coders::display::ElementFormatter;
pub use crate::metrics::context::MetricsContainer;
pub use crate::metrics::scope::{MetricsScope, TransformMetrics, TransformSelection};
pub use crate::metrics::short_ids::ShortIdCache;
pub use crate::metrics::system::{
    data_channel_read_index, element_count, finish_bundle_msecs, process_bundle_msecs,
    sampled_byte_size, start_bundle_msecs, total_msecs, work_completed, work_remaining,
};
pub use crate::pipeline::graph::PipelineInner;

/// Adds one primitive `ParDo` to a pipeline's graph and registers its handler.
/// Every `ParDo` primitive in the SDK goes through this builder. Only the name from
/// [`PipelineInternals::primitive_name`], the main input and the handler are required:
///
/// ```
/// use std::sync::Arc;
/// use beam::internals::{ElementSink, ParDoRegistration, PipelineInternals};
/// use beam::prelude::*;
/// use beam::transforms::Impulse;
///
/// let p = Pipeline::new();
/// let input = p.apply("Impulse", Impulse::new());
/// let output = p.add_pcollection::<Vec<u8>>("Echo.out", input.coder_id(), input.is_bounded());
///
/// let echoed = p.add_scoped_primitive("Echo", |name| {
///     ParDoRegistration::new(&p, name, input.id())
///         .output("out", output.id())
///         .register(Arc::new(|e: &[u8], out: &mut dyn ElementSink| out.push(e.to_vec())));
///     output
/// });
/// assert!(p.lock().components.transforms.contains_key("Echo"));
/// # let _ = echoed;
/// ```
///
/// With [`register`](Self::register), the `ParDoPayload` `do_fn` carries the handler key: the
/// transform's unique name. The harness resolves handlers from that payload, so the key
/// survives runners that rename or fuse transforms. With [`register_spec`](Self::register_spec),
/// the `do_fn` holds all that the worker needs to build the handler.
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
    requires_stable_input: bool,
    requires_time_sorted_input: bool,
    resource_hints: Option<ResourceHints>,
    display_data: Vec<proto::DisplayData>,
}

impl<'p> ParDoRegistration<'p> {
    /// Starts a `ParDo` that reads the PCollection `input_id`. `name` comes from
    /// [`PipelineInternals::primitive_name`].
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
            requires_stable_input: false,
            requires_time_sorted_input: false,
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

    /// Marks that the transform must see the same input when the runner retries a bundle.
    /// The pipeline then carries `beam:requirement:pardo:stable_input:v1`.
    pub fn requires_stable_input(mut self, requires: bool) -> Self {
        self.requires_stable_input = requires;
        self
    }

    /// Marks that the transform must see the elements of each key in timestamp order. The
    /// pipeline then carries `beam:requirement:pardo:time_sorted_input:v1`.
    pub fn requires_time_sorted_input(mut self, requires: bool) -> Self {
        self.requires_time_sorted_input = requires;
        self
    }

    /// Attaches resource hints, merged over any scoped hints. Empty hints are ignored.
    pub fn resource_hints(mut self, hints: ResourceHints) -> Self {
        self.resource_hints = (!hints.is_empty()).then_some(hints);
        self
    }

    /// Attaches display data to the transform.
    pub fn display_data(mut self, display_data: Vec<proto::DisplayData>) -> Self {
        self.display_data = display_data;
        self
    }

    /// Records the `ParDo`, registers `handler` for it, and returns its transform id.
    pub fn register(self, handler: TransformFn) -> String {
        let pipeline = self.pipeline;
        let transform_id = self.add(None);
        pipeline.register_transform_handler(transform_id.clone(), handler);
        transform_id
    }

    /// Records the `ParDo` with `do_fn` as its function spec and returns its transform id.
    /// The worker builds the handler from `do_fn` alone, so no handler is registered.
    pub fn register_spec(self, do_fn: proto::FunctionSpec) -> String {
        self.add(Some(do_fn))
    }

    /// Adds the transform. Without `do_fn`, the spec is the handler key: the unique name.
    fn add(self, do_fn: Option<proto::FunctionSpec>) -> String {
        use prost::Message;

        let (side_input_pcolls, side_input_specs): (HashMap<_, _>, HashMap<_, _>) = self
            .side_inputs
            .into_iter()
            .map(|(tag, (pcoll_id, spec))| ((tag.clone(), pcoll_id), (tag, spec)))
            .unzip();
        let inputs = std::iter::once(("in".to_string(), self.input_id))
            .chain(side_input_pcolls)
            .collect();

        let mut graph = self.pipeline.lock();
        // The handler key goes into the payload: the unique name of the transform.
        let unique_name = self.name;
        let do_fn = do_fn.unwrap_or_else(|| proto::FunctionSpec {
            urn: URN_RUST_DOFN.to_string(),
            payload: unique_name.as_bytes().to_vec(),
        });
        let payload = proto::ParDoPayload {
            do_fn: Some(do_fn),
            side_inputs: side_input_specs,
            state_specs: self.state_specs,
            timer_family_specs: self.timer_family_specs,
            restriction_coder_id: self.restriction_coder_id,
            requests_finalization: self.requests_finalization,
            requires_stable_input: self.requires_stable_input,
            requires_time_sorted_input: self.requires_time_sorted_input,
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
    }
}

/// The encoded header of the element that `ctx` processes: its timestamp, windows, pane and
/// metadata. For SDK transforms that key on raw windows or re-emit under a stored header;
/// user code reads the window through [`Windowed`](crate::transforms::Windowed).
pub fn element_header<'a, T>(
    ctx: &crate::transforms::ProcessContext<'a, T>,
) -> &'a crate::coders::WindowedHeader {
    ctx.header()
}

/// Starts an output of `finish_bundle` under a stored `header`, for SDK transforms that kept
/// the header of each buffered element.
pub fn finish_output_windowed<'h, 'a, T: crate::coders::DefaultCoder>(
    ctx: &'h mut crate::transforms::FinishBundleContext<'a, T>,
    value: T,
    header: &'h crate::coders::WindowedHeader,
) -> crate::transforms::OutputBuilder<'h, 'a, T> {
    ctx.output_windowed(value, header)
}

/// Starts an output of `process_element` under a stored header instead of the header of the
/// current element.
pub fn output_windowed<'h, 'a, T: crate::coders::DefaultCoder>(
    ctx: &'h mut crate::transforms::ProcessContext<'a, T>,
    value: T,
    header: &'h crate::coders::WindowedHeader,
) -> crate::transforms::OutputBuilder<'h, 'a, T> {
    ctx.output(value).windowed(header)
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for crate::pipeline::Pipeline {}
}

/// Runner and transform-crate plumbing for pipeline graph inspection and registration; not part of the pipeline-author API.
pub trait PipelineInternals: sealed::Sealed {
    /// Acquires a lock on the pipeline's internal graph representation.
    fn lock(&self) -> MutexGuard<'_, PipelineInner>;

    /// Registers a transform handler for `key`.
    fn register_transform_handler(&self, key: impl Into<String>, handler: TransformFn);

    /// Returns the currently registered transform handlers.
    fn transform_handlers(&self) -> HashMap<String, TransformFn>;

    /// Applies `build` as the transform `name`. A primitive that `build` adds first takes the
    /// name; otherwise the scope becomes a composite that reads `inputs`.
    fn apply_scoped<O: POutput>(
        &self,
        name: &str,
        inputs: HashMap<String, String>,
        build: impl FnOnce() -> O,
    ) -> O;

    /// [`apply_scoped`](Self::apply_scoped) for a `build` that can fail.
    fn try_apply_scoped<O: POutput, E>(
        &self,
        name: &str,
        inputs: HashMap<String, String>,
        build: impl FnOnce() -> Result<O, E>,
    ) -> Result<O, E>;

    /// Applies one primitive as the transform `name`. `build` gets the resolved name, adds
    /// exactly one primitive with it, and returns the output.
    fn add_scoped_primitive<O: POutput>(&self, name: &str, build: impl FnOnce(String) -> O) -> O;

    /// Returns the name of a primitive that is about to be added: the name of the scope for
    /// the first transform of the scope, otherwise the child name `kind`.
    fn primitive_name(&self, kind: &str) -> String;

    /// Adds an impulse primitive in the current scope. Returns its ID and output.
    fn add_impulse(&self) -> (String, PCollection<Vec<u8>>);

    /// The full name of the transform that is expanding, or `None` at the root.
    fn scope_name(&self) -> Option<String>;

    /// Sets the spec of the composite node of the transform that is expanding.
    fn set_scope_spec(&self, spec: proto::FunctionSpec);

    /// Sets the display data of the node of the transform that is expanding.
    fn set_scope_display_data(&self, display_data: Vec<proto::DisplayData>);

    /// Adds an annotation to the node of the transform that is expanding.
    fn set_scope_annotation(&self, key: &str, value: Vec<u8>);
}

impl PipelineInternals for Pipeline {
    fn lock(&self) -> MutexGuard<'_, PipelineInner> {
        self.lock()
    }

    fn register_transform_handler(&self, key: impl Into<String>, handler: TransformFn) {
        self.register_transform_handler(key, handler);
    }

    fn transform_handlers(&self) -> HashMap<String, TransformFn> {
        self.transform_handlers()
    }

    fn apply_scoped<O: POutput>(
        &self,
        name: &str,
        inputs: HashMap<String, String>,
        build: impl FnOnce() -> O,
    ) -> O {
        self.apply_scoped(name, inputs, build)
    }

    fn try_apply_scoped<O: POutput, E>(
        &self,
        name: &str,
        inputs: HashMap<String, String>,
        build: impl FnOnce() -> Result<O, E>,
    ) -> Result<O, E> {
        self.try_apply_scoped(name, inputs, build)
    }

    fn add_scoped_primitive<O: POutput>(&self, name: &str, build: impl FnOnce(String) -> O) -> O {
        self.add_scoped_primitive(name, build)
    }

    fn primitive_name(&self, kind: &str) -> String {
        self.primitive_name(kind)
    }

    fn add_impulse(&self) -> (String, PCollection<Vec<u8>>) {
        self.add_impulse()
    }

    fn scope_name(&self) -> Option<String> {
        self.scope_name()
    }

    fn set_scope_spec(&self, spec: proto::FunctionSpec) {
        self.set_scope_spec(spec);
    }

    fn set_scope_display_data(&self, display_data: Vec<proto::DisplayData>) {
        self.set_scope_display_data(display_data);
    }

    fn set_scope_annotation(&self, key: &str, value: Vec<u8>) {
        self.set_scope_annotation(key, value);
    }
}
