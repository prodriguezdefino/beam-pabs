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

//! The typed element-processing function and the transform that applies it.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use super::context::{FinishBundleContext, HandlerContext, ProcessContext};
use super::input::DoFnInput;
use super::outputs::{OutputDeclarer, Outputs};
use super::registered::graph_spec;
use super::state::AnyStateSpec;
use super::timer::TimerFamilySpec;
use crate::coders::{DefaultCoder, URN_GLOBAL_WINDOW, URN_TIMER};
use crate::internals::ParDoRegistration;
use crate::transforms::{
    BundleHandler, DisplayDataBuilder, HandlerInstance, HasDisplayData, PTransform, TransformFn,
    TypedElement,
};
use crate::values::AnySideInput;
use crate::values::PCollection;

/// Processes the elements of a [`PCollection`] one at a time, with zero or more outputs for
/// each through [`ProcessContext`]. This is the main extension point for user code and I/O
/// connectors. Implementors work with typed values and read side inputs and timestamps. The
/// SDK does the encoding, decoding and graph construction. For user state and timers,
/// implement [`StatefulDoFn`](crate::transforms::StatefulDoFn) instead.
///
/// Set `type In = Windowed<T>` to receive the window; the function then runs once per window
/// of a multi-window element. With `type In = T` it runs once per element.
///
/// # Lifecycle
///
/// The `DoFn` given to [`ParDo`] is a prototype. The worker clones it once for each bundle
/// processor and calls [`setup`](DoFn::setup) on the copy. The copy runs the bundles of that
/// processor one at a time: [`start_bundle`](DoFn::start_bundle), then
/// [`process_element`](DoFn::process_element) for each element, then
/// [`finish_bundle`](DoFn::finish_bundle). [`teardown`](DoFn::teardown) runs when the
/// processor is discarded.
///
/// Two bundles never share a copy, so state kept from `start_bundle` to `finish_bundle` is
/// private to the current bundle and the lifecycle methods take `&mut self`. Keep this state
/// in plain fields, with no lock or atomic. `Clone` must produce an independent instance:
/// derive it for plain configuration, and give each field that holds bundle state a fresh
/// value instead of sharing it behind an `Arc`.
///
/// # Errors
///
/// Every hook returns [`beam::Result`](crate::Result), so `?` works on I/O, parse, coder
/// and other standard errors. An `Err` fails the bundle, and the runner retries it. To send
/// bad elements to a dead-letter output, use [`TryMap`](crate::transforms::TryMap), or set
/// `type Out = Result<T, F>` and emit `Err(failure)`.
///
/// # Examples
///
/// ```
/// use beam::transforms::{DoFn, ProcessContext};
///
/// #[derive(Clone)]
/// struct SplitWords;
///
/// impl DoFn for SplitWords {
///     type In = String;
///     type Out = String;
///
///     fn process_element(&mut self, line: String, ctx: &mut ProcessContext<'_, String>) -> beam::Result {
///         line.split_whitespace().try_for_each(|w| ctx.emit(w.to_string()))
///     }
/// }
/// ```
pub trait DoFn: Clone + Send + Sync + 'static {
    /// The input element `T`, or [`Windowed<T>`](crate::transforms::Windowed).
    type In: DoFnInput;
    /// What this function emits: an element type, `Result<T, F>` for an error output, or an
    /// enum with `#[derive(Outputs)]`. See [`Outputs`].
    type Out: Outputs;

    /// Set to `true` if the function must see the same input when the runner retries a
    /// bundle, for example a sink that commits to an external system. The runner then
    /// checkpoints the input before this step.
    const REQUIRES_STABLE_INPUT: bool = false;

    /// Called once on each copy, when the bundle processor that owns it is created.
    fn setup(&mut self) -> crate::Result {
        Ok(())
    }

    /// Called once before the first element of a bundle.
    fn start_bundle(&mut self) -> crate::Result {
        Ok(())
    }

    /// Processes one input element and emits any outputs through `ctx`.
    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result;

    /// Called once after the last element of a bundle, the last chance to emit. Outputs name
    /// their window; see [`FinishBundleContext`].
    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> crate::Result {
        let _ = ctx;
        Ok(())
    }

    /// Called once on each copy, when the bundle processor that owns it is discarded.
    fn teardown(&mut self) -> crate::Result {
        Ok(())
    }

    /// Adds display data for runner UIs.
    fn populate_display_data(&self, _builder: &mut DisplayDataBuilder) {}

    /// The views read with [`ProcessContext::side_input`]; [`ParDo`] wires them into the graph.
    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        Vec::new()
    }
}

/// Selects how [`ParDo`] runs a [`DoFn`]. The compiler infers it.
pub struct Stateless;

/// Selects how [`ParDo`] runs a [`StatefulDoFn`](crate::transforms::StatefulDoFn). The
/// compiler infers it.
pub struct Stateful;

pub(crate) mod sealed {
    /// Keeps [`IntoParDoFn`](super::IntoParDoFn) to the two function traits of this crate.
    pub trait Sealed<M> {}
}

/// A function that [`ParDo`] applies: a [`DoFn`] (`M` = [`Stateless`]) or a
/// [`StatefulDoFn`](crate::transforms::StatefulDoFn) (`M` = [`Stateful`]). Implement one of
/// those traits; this trait has no other implementations.
pub trait IntoParDoFn<M>: sealed::Sealed<M> + Clone + Send + Sync + 'static {
    /// The element type that the function consumes.
    type In: DefaultCoder;
    /// What the function emits.
    type Out: Outputs;

    /// Whether the function must see the same input when the runner retries a bundle.
    const REQUIRES_STABLE_INPUT: bool;

    /// Whether the function must see the elements of each key in timestamp order.
    const REQUIRES_TIME_SORTED_INPUT: bool;

    /// The user state cells that the transform declares.
    fn state_specs() -> Vec<Arc<dyn AnyStateSpec>>;

    /// The timer families that the transform declares.
    fn timer_families() -> Vec<TimerFamilySpec>;

    /// The side input views that the function reads.
    fn side_inputs(&self) -> Vec<&dyn AnySideInput>;

    /// Wraps the function in the handler that the worker runs.
    fn into_handler(self) -> TransformFn;

    /// Adds display data for runner UIs.
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder);
}

impl<T: DoFn> sealed::Sealed<Stateless> for T {}

impl<T: DoFn> IntoParDoFn<Stateless> for T {
    type In = <T::In as DoFnInput>::Value;
    type Out = T::Out;
    const REQUIRES_STABLE_INPUT: bool = T::REQUIRES_STABLE_INPUT;
    // Only a stateful function has keys to sort the input of.
    const REQUIRES_TIME_SORTED_INPUT: bool = false;

    fn state_specs() -> Vec<Arc<dyn AnyStateSpec>> {
        Vec::new()
    }

    fn timer_families() -> Vec<TimerFamilySpec> {
        Vec::new()
    }

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        DoFn::side_inputs(self)
    }

    fn into_handler(self) -> TransformFn {
        Arc::new(DoFnHandler::new(self))
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        DoFn::populate_display_data(self, builder);
    }
}

/// Applies a [`DoFn`] or a [`StatefulDoFn`](crate::transforms::StatefulDoFn) to every
/// element of a [`PCollection`]. It returns what the [`Outputs`] of the function declare: a
/// `PCollection<Out>` for a single output.
pub struct ParDo<F, M = Stateless> {
    name: String,
    func: Arc<F>,
    side_inputs: SideInputSpecs,
    resource_hints: crate::pipeline::resources::ResourceHints,
    /// The number of outputs of [`Indexed`](super::outputs::Indexed) functions.
    output_count: usize,
    _kind: PhantomData<fn() -> M>,
}

impl<M, F: IntoParDoFn<M>> ParDo<F, M> {
    /// Applies `func` under the given transform name.
    pub fn new(name: impl Into<String>, func: F) -> Self {
        let side_inputs = side_input_specs(func.side_inputs());
        Self {
            name: name.into(),
            func: Arc::new(func),
            side_inputs,
            resource_hints: crate::pipeline::resources::ResourceHints::new(),
            output_count: 0,
            _kind: PhantomData,
        }
    }

    /// Sets the number of outputs of a function that emits
    /// [`Indexed`](super::outputs::Indexed) values.
    pub(crate) fn with_output_count(mut self, count: usize) -> Self {
        self.output_count = count;
        self
    }

    /// Sets resource hints on this transform.
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

impl<M, F: IntoParDoFn<M>> HasDisplayData for ParDo<F, M> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("fn", std::any::type_name::<F>());
        builder.add_text("transform_name", &self.name);
        if !self.side_inputs.is_empty() {
            builder.add_integer("num_side_inputs", self.side_inputs.len() as i64);
        }
        self.func.populate_display_data(builder);
    }
}

impl<M, F: IntoParDoFn<M>> PTransform<PCollection<F::In>> for ParDo<F, M> {
    type Output = <F::Out as Outputs>::Collections;

    fn expand(&self, input: &PCollection<F::In>) -> Self::Output {
        let pipeline = input.pipeline();
        let in_ws_id = input.windowing_strategy_id();
        let mut declarer = OutputDeclarer::new(
            pipeline,
            &self.name,
            &in_ws_id,
            input.is_bounded(),
            self.output_count,
        );
        let collections = F::Out::declare(&mut declarer);
        let outputs = declarer.into_tags();

        let mut builder = DisplayDataBuilder::with_namespace(self.name.clone());
        self.populate_display_data(&mut builder);
        if outputs.len() > 1 {
            let mut tags: Vec<_> = outputs.keys().collect();
            tags.sort();
            builder.add_integer("num_outputs", tags.len() as i64);
            for (i, tag) in tags.into_iter().enumerate() {
                builder.add_text(format!("output_tag_{i}"), tag);
            }
        }

        let proto_state_specs = F::state_specs()
            .iter()
            .map(|spec| (spec.name().to_string(), spec.register_and_encode(pipeline)))
            .collect::<Vec<_>>();

        let existing_window_coder = pipeline
            .lock()
            .components
            .windowing_strategies
            .get(&in_ws_id)
            .map(|ws| ws.window_coder_id.clone())
            .filter(|id| !id.is_empty());
        let window_coder_id = existing_window_coder
            .unwrap_or_else(|| pipeline.register_coder(URN_GLOBAL_WINDOW, Vec::new()));
        let key_coder_id = pipeline
            .coder_components(input.coder_id())
            .first()
            .cloned()
            .unwrap_or_else(|| input.coder_id().to_string());

        let proto_timer_specs = F::timer_families()
            .into_iter()
            .map(|spec| {
                let timer_coder_id = pipeline.register_coder(
                    URN_TIMER,
                    vec![key_coder_id.clone(), window_coder_id.clone()],
                );
                let proto = spec.to_proto(&timer_coder_id);
                (spec.family_name, proto)
            })
            .collect::<Vec<_>>();

        let registration = ParDoRegistration::new(pipeline, &self.name, input.id())
            .outputs(outputs)
            .side_inputs(self.side_inputs.clone())
            .state_specs(proto_state_specs)
            .timer_family_specs(proto_timer_specs)
            .requests_finalization(<F::Out as Outputs>::FINALIZES)
            .requires_stable_input(F::REQUIRES_STABLE_INPUT)
            .requires_time_sorted_input(F::REQUIRES_TIME_SORTED_INPUT)
            .resource_hints(self.resource_hints.clone())
            .display_data(builder.into_proto());

        // A registered type goes into the spec, so the worker needs no rebuilt pipeline.
        match graph_spec(pipeline, &self.name, self.func.as_ref()) {
            Some(spec) => registration.register_spec(spec),
            None => registration.register(F::clone(&self.func).into_handler()),
        };

        collections
    }
}

/// Side input entries of a transform, keyed by local tag.
pub(crate) type SideInputSpecs = HashMap<String, (String, model::pipeline::SideInput)>;

/// Builds the side input entries of a transform from the views its function reads.
pub(crate) fn side_input_specs(views: Vec<&dyn AnySideInput>) -> SideInputSpecs {
    views
        .into_iter()
        .map(|view| {
            (
                view.tag().to_string(),
                (view.pcollection_id().to_string(), view.to_proto()),
            )
        })
        .collect()
}

/// Calls `process` for `element`, once per window if `explode` is `true`.
pub(crate) fn process_each_window<I: DoFnInput, Out>(
    element: &[u8],
    ctx: &mut HandlerContext<'_>,
    explode: bool,
    mut process: impl FnMut(I, ProcessContext<'_, Out>) -> crate::Result,
) -> Result<(), String> {
    // Schema types such as `Row` need the schema from the input PCollection coder to
    // decode. State-backed iterables need the State API stream reader.
    let header = ctx.header;
    header
        .explode_if(explode)
        .map_err(|e| e.to_string())?
        .try_for_each(|header| {
            let value =
                I::Value::decode_with_context(element, ctx.input_schema, ctx.state_stream_reader)
                    .map_err(|e| format!("Failed to decode input element: {e}"))?;
            let input = I::from_element(value, &header)?;
            Ok(process(
                input,
                ctx.as_process_context::<Out>().with_header(&header),
            )?)
        })
}

/// Calls `process` for an element that a fused upstream operator gives by value.
pub(crate) fn process_value_each_window<I: DoFnInput, Out>(
    mut element: TypedElement<'_>,
    ctx: &mut HandlerContext<'_>,
    explode: bool,
    mut process: impl FnMut(I, ProcessContext<'_, Out>) -> crate::Result,
) -> Result<(), String> {
    // An element that needs no split is taken as-is, with no decode. A multi-window element
    // that must be split takes the byte path, which decodes once per window.
    let value = (!(explode && ctx.header.is_multi_window()))
        .then(|| element.take_fused::<I::Value>())
        .flatten();
    match value {
        Some(value) => {
            let input = I::from_element(value, ctx.header)?;
            Ok(process(input, ctx.as_process_context::<Out>())?)
        }
        None => {
            let encoded = element.encode()?;
            process_each_window(&encoded, ctx, explode, process)
        }
    }
}

/// Adapts a typed [`DoFn`] to the byte-level [`BundleHandler`] that a runner calls. It owns
/// its `DoFn`, so each instance that a bundle processor runs has its own copy.
#[derive(Clone)]
pub struct DoFnHandler<F> {
    func: F,
    /// Process a multi-window element once per window. Fixed per function, so decided once.
    explode: bool,
}

impl<F: DoFn> DoFnHandler<F> {
    pub fn new(func: F) -> Self {
        let explode = <F::In as DoFnInput>::OBSERVES_WINDOW || !func.side_inputs().is_empty();
        Self { func, explode }
    }
}

impl<F: DoFn> BundleHandler for DoFnHandler<F> {
    fn setup(&mut self) -> Result<(), String> {
        Ok(self.func.setup()?)
    }

    fn start_bundle(&mut self) -> Result<(), String> {
        Ok(self.func.start_bundle()?)
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let Self { func, explode } = self;
        process_each_window(element, ctx, *explode, |input, mut pctx| {
            func.process_element(input, &mut pctx)
        })
    }

    fn process_value(
        &mut self,
        element: TypedElement<'_>,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let Self { func, explode } = self;
        process_value_each_window(element, ctx, *explode, |input, mut pctx| {
            func.process_element(input, &mut pctx)
        })
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let mut fctx = FinishBundleContext::new(ctx.as_process_context::<F::Out>());
        Ok(self.func.finish_bundle(&mut fctx)?)
    }

    fn teardown(&mut self) -> Result<(), String> {
        Ok(self.func.teardown()?)
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}
