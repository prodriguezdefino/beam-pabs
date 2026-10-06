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
use std::sync::Arc;

use super::context::{HandlerContext, ProcessContext};
use super::state::AnyStateSpec;
use super::timer::TimerFamilySpec;
use crate::coders::{DefaultCoder, TimerRecord, URN_GLOBAL_WINDOW, URN_TIMER, WindowedHeader};
use crate::internals::ParDoRegistration;
use crate::transforms::{
    BundleHandler, DisplayDataBuilder, HandlerInstance, HasDisplayData, PTransform, TransformFn,
    TypedElement,
};
use crate::values::AnySideInput;
use crate::values::{IsBounded, PCollection, PCollectionList};

/// Processes the elements of a [`PCollection`] one at a time, with zero or more outputs for
/// each through [`ProcessContext`]. This is the main extension point for user code and I/O
/// connectors. Implementors work with typed values and read side inputs, state, timers,
/// timestamps and windows. The SDK does the encoding, decoding and graph construction.
///
/// # Lifecycle
///
/// The `DoFn` given to [`ParDo`] is a prototype. The worker clones it once for each bundle
/// processor and calls [`setup`](DoFn::setup) on the copy. The copy runs the bundles of that
/// processor one at a time: [`start_bundle`](DoFn::start_bundle), then
/// [`process_element`](DoFn::process_element) for each element and
/// [`on_timer`](DoFn::on_timer) for each fired timer, then
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
/// bad elements to a dead-letter output, use [`TryMap`](crate::transforms::TryMap) or
/// [`TryParDo`](crate::transforms::TryParDo).
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
    /// The element type that this function consumes.
    type In: DefaultCoder;
    /// The element type that this function produces.
    type Out: DefaultCoder;

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

    /// Called when a timer in `timer_family` fires for the current key and window. Read the
    /// key with [`ProcessContext::current_key`], and read or clear state with
    /// [`ProcessContext::bag_state`] or [`ProcessContext::value_state`].
    fn on_timer(
        &mut self,
        timer_family: &str,
        tag: &str,
        timestamp: i64,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        let _ = (timer_family, tag, timestamp, ctx);
        Ok(())
    }

    /// Called once after the last element of a bundle. It is the last chance to emit.
    fn finish_bundle(&mut self, ctx: &mut ProcessContext<'_, Self::Out>) -> crate::Result {
        let _ = ctx;
        Ok(())
    }

    /// Called once on each copy, when the bundle processor that owns it is discarded.
    fn teardown(&mut self) -> crate::Result {
        Ok(())
    }

    /// Adds display data for runner UIs.
    fn populate_display_data(&self, _builder: &mut DisplayDataBuilder) {}

    /// Return `true` if [`process_element`](DoFn::process_element) or any other method calls
    /// `register_finalizer`. The runner then sends `FinalizeBundle` after it commits the
    /// output of each bundle, and the callbacks run. If this returns `false`, runners never
    /// finalize the bundles of this `DoFn` and the callbacks are dropped.
    fn requests_finalization(&self) -> bool {
        false
    }
}

/// Applies a [`DoFn`] that emits to multiple output [`PCollection`]s.
pub struct ParDoMulti<F> {
    name: String,
    tags: Vec<String>,
    func: Arc<F>,
    side_inputs: HashMap<String, (String, model::pipeline::SideInput)>,
    state_specs: Vec<Arc<dyn AnyStateSpec>>,
    timer_family_specs: Vec<TimerFamilySpec>,
    resource_hints: crate::pipeline::resources::ResourceHints,
}

impl<F: DoFn> ParDoMulti<F> {
    /// Creates a multi-output ParDo with named output tags.
    pub fn new<S: AsRef<str>>(
        name: impl Into<String>,
        tags: impl IntoIterator<Item = S>,
        func: F,
    ) -> Self {
        Self {
            name: name.into(),
            tags: tags.into_iter().map(|s| s.as_ref().to_string()).collect(),
            func: Arc::new(func),
            side_inputs: HashMap::new(),
            state_specs: Vec::new(),
            timer_family_specs: Vec::new(),
            resource_hints: crate::pipeline::resources::ResourceHints::new(),
        }
    }

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

    /// Attaches a side input view to this transform.
    pub fn with_side_input(mut self, view: &(dyn AnySideInput + '_)) -> Self {
        self.side_inputs.insert(
            view.tag().to_string(),
            (view.pcollection_id().to_string(), view.to_proto()),
        );
        self
    }

    /// Attaches multiple side input views to this transform.
    pub fn with_side_inputs(mut self, views: &[&(dyn AnySideInput + '_)]) -> Self {
        views.iter().for_each(|view| {
            self.side_inputs.insert(
                view.tag().to_string(),
                (view.pcollection_id().to_string(), view.to_proto()),
            );
        });
        self
    }

    /// Attaches a persistent user state specification to this transform.
    pub fn with_state_spec<S: AnyStateSpec + Clone + 'static>(mut self, spec: &S) -> Self {
        self.state_specs.push(Arc::new(spec.clone()));
        self
    }

    /// Attaches a timer family specification to this transform.
    pub fn with_timer_family(mut self, spec: &TimerFamilySpec) -> Self {
        self.timer_family_specs.push(spec.clone());
        self
    }
}

impl<F: DoFn> HasDisplayData for ParDoMulti<F> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("fn", std::any::type_name::<F>());
        builder.add_text("transform_name", &self.name);
        if self.tags.len() > 1 {
            builder.add_integer("num_outputs", self.tags.len() as i64);
        }
        for (i, tag) in self.tags.iter().enumerate() {
            builder.add_text(format!("output_tag_{i}"), tag);
        }
        if !self.side_inputs.is_empty() {
            builder.add_integer("num_side_inputs", self.side_inputs.len() as i64);
        }
        self.func.populate_display_data(builder);
    }
}

impl<F: DoFn> PTransform<PCollection<F::In>> for ParDoMulti<F> {
    type Output = PCollectionList<F::Out>;

    fn expand(&self, input: &PCollection<F::In>) -> PCollectionList<F::Out> {
        let pipeline = input.pipeline();
        let coder_id = F::Out::register_coder(pipeline);

        let (pcolls, outputs): (Vec<_>, HashMap<_, _>) = self
            .tags
            .iter()
            .map(|tag| {
                let out_pcoll = pipeline.add_pcollection_with_windowing::<F::Out>(
                    &format!("{}_{}", self.name, tag),
                    &coder_id,
                    IsBounded::Bounded,
                    &input.windowing_strategy_id(),
                );
                (out_pcoll.clone(), (tag.clone(), out_pcoll.id().to_string()))
            })
            .unzip();

        let mut builder = DisplayDataBuilder::with_namespace(self.name.clone());
        self.populate_display_data(&mut builder);

        let proto_state_specs = self
            .state_specs
            .iter()
            .map(|spec| (spec.name().to_string(), spec.register_and_encode(pipeline)))
            .collect::<Vec<_>>();

        let in_ws_id = input.windowing_strategy_id();
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

        let proto_timer_specs = self
            .timer_family_specs
            .iter()
            .map(|spec| {
                let timer_coder_id = pipeline.register_coder(
                    URN_TIMER,
                    vec![key_coder_id.clone(), window_coder_id.clone()],
                );
                (spec.family_name.clone(), spec.to_proto(&timer_coder_id))
            })
            .collect::<Vec<_>>();

        let handler: TransformFn = Arc::new(DoFnHandler::new(F::clone(&self.func)));
        ParDoRegistration::new(pipeline, &self.name, input.id())
            .outputs(outputs)
            .side_inputs(self.side_inputs.clone())
            .state_specs(proto_state_specs)
            .timer_family_specs(proto_timer_specs)
            .requests_finalization(self.func.requests_finalization())
            .resource_hints(self.resource_hints.clone())
            .display_data(builder.into_proto())
            .register(handler);

        PCollectionList::from_vec(pipeline.clone(), pcolls)
    }
}

/// Applies a single-output [`DoFn`] to every element of a [`PCollection`]. Wraps
/// [`ParDoMulti`].
pub struct ParDo<F> {
    inner: ParDoMulti<F>,
}

impl<F: DoFn> ParDo<F> {
    /// Applies `func` under the given transform name, with the output tag `"out"`.
    pub fn new(name: impl Into<String>, func: F) -> Self {
        Self {
            inner: ParDoMulti::new(name, ["out"], func),
        }
    }

    pub fn with_resource_hints(mut self, hints: crate::pipeline::resources::ResourceHints) -> Self {
        self.inner = self.inner.with_resource_hints(hints);
        self
    }

    /// Attaches a single resource hint by URN and serialized payload.
    pub fn with_resource_hint(
        mut self,
        urn: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        self.inner = self.inner.with_resource_hint(urn, payload);
        self
    }

    /// Attaches a side input view to this `ParDo`.
    pub fn with_side_input(mut self, view: &(dyn AnySideInput + '_)) -> Self {
        self.inner = self.inner.with_side_input(view);
        self
    }

    /// Attaches multiple side input views to this `ParDo`.
    pub fn with_side_inputs(mut self, views: &[&(dyn AnySideInput + '_)]) -> Self {
        self.inner = self.inner.with_side_inputs(views);
        self
    }

    /// Attaches a persistent user state specification to this `ParDo`.
    pub fn with_state_spec<S: AnyStateSpec + Clone + 'static>(mut self, spec: &S) -> Self {
        self.inner = self.inner.with_state_spec(spec);
        self
    }

    /// Attaches a timer family specification to this `ParDo`.
    pub fn with_timer_family(mut self, spec: &TimerFamilySpec) -> Self {
        self.inner = self.inner.with_timer_family(spec);
        self
    }
}

impl<F: DoFn> HasDisplayData for ParDo<F> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.inner.populate_display_data(builder);
    }
}

impl<F: DoFn> PTransform<PCollection<F::In>> for ParDo<F> {
    type Output = PCollection<F::Out>;

    fn expand(&self, input: &PCollection<F::In>) -> PCollection<F::Out> {
        self.inner
            .expand(input)
            .into_iter()
            .next()
            .expect("ParDo produces exactly one output")
    }
}

/// Adapts a typed [`DoFn`] to the byte-level [`BundleHandler`] that a runner calls. It owns
/// its `DoFn`, so each instance that a bundle processor runs has its own copy.
#[derive(Clone)]
pub struct DoFnHandler<F> {
    pub func: F,
}

impl<F> DoFnHandler<F> {
    pub fn new(func: F) -> Self {
        Self { func }
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
        // Schema types such as `Row` need the schema from the input PCollection coder to
        // decode. State-backed iterables need the State API stream reader.
        let header = ctx.header;
        header.explode()?.try_for_each(|header| {
            let value =
                F::In::decode_with_context(element, ctx.input_schema, ctx.state_stream_reader)
                    .map_err(|e| format!("Failed to decode input element: {e}"))?;
            let mut pctx = ctx.as_process_context::<F::Out>().with_header(&header);
            Ok(self.func.process_element(value, &mut pctx)?)
        })
    }

    fn process_value(
        &mut self,
        mut element: TypedElement<'_>,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        // A single-window element of the `DoFn` input type is used as-is, with no decode.
        // Others take the byte path, which decodes once per window.
        let value = (!ctx.header.is_multi_window())
            .then(|| element.take::<F::In>())
            .flatten();
        match value {
            Some(value) => {
                let mut pctx = ctx.as_process_context::<F::Out>();
                Ok(self.func.process_element(value, &mut pctx)?)
            }
            None => {
                let encoded = element.encode()?;
                self.process(&encoded, ctx)
            }
        }
    }

    fn on_timer(
        &mut self,
        timer_family: &str,
        record: &TimerRecord,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        if record.clear {
            return Ok(());
        }
        let header = WindowedHeader::new(record.fire_timestamp, &record.windows, record.pane);
        let mut pctx = ctx
            .as_process_context::<F::Out>()
            .with_key_bytes(record.user_key.clone())
            .with_header(&header);
        Ok(self.func.on_timer(
            timer_family,
            &record.dynamic_tag,
            record.fire_timestamp,
            &mut pctx,
        )?)
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let mut pctx = ctx.as_process_context::<F::Out>();
        Ok(self.func.finish_bundle(&mut pctx)?)
    }

    fn teardown(&mut self) -> Result<(), String> {
        Ok(self.func.teardown()?)
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}
