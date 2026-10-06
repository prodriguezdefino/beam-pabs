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

//! Splittable DoFn definition, execution handlers, and ParDo transform.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::process_continuation::ProcessContinuation;
use super::tracker::RestrictionTracker;
use crate::coders::{DefaultCoder, WindowedHeader};
use crate::internals::ParDoRegistration;
use crate::pipeline::constants::{
    URN_SDF_PAIR_WITH_RESTRICTION, URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
    URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS, URN_SDF_TRUNCATE_SIZED_RESTRICTIONS,
};
use crate::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use crate::transforms::dofn::context::{
    FinishBundleContext, HandlerContext, ProcessContext, ResidualApplication,
};
use crate::transforms::dofn::input::DoFnInput;
use crate::transforms::dofn::outputs::{OutputDeclarer, Outputs};
use crate::transforms::dofn::pardo::{SideInputSpecs, side_input_specs};
use crate::transforms::dofn::registered::graph_spec;
use crate::transforms::{BundleHandler, HandlerInstance, PTransform};
use crate::values::AnySideInput;
use crate::values::{IsBounded, PCollection};

/// Processes elements paired with a restriction that the runner can split dynamically.
///
/// Each bundle processor runs its own clone, so `Clone` must produce an independent
/// instance. The lifecycle methods and [`process_element`](Self::process_element) take
/// `&mut self`, as in [`DoFn`](crate::transforms::DoFn), so state kept across a bundle is
/// plain fields. The restriction methods take `&self`: the dynamic splitter calls
/// [`restriction_size`](Self::restriction_size) on a separate copy, from another thread,
/// while `process_element` runs. Make it depend only on the element and the restriction.
///
/// Set `type In = Windowed<T>` to receive the window, in `process_element` and in the
/// restriction methods; every stage then runs once per window of a multi-window element.
pub trait SplittableDoFn: Clone + Send + Sync + 'static {
    /// The input element `T`, or [`Windowed<T>`](crate::transforms::Windowed).
    type In: DoFnInput + Clone + Sync;
    /// What this function emits; see [`Outputs`].
    type Out: Outputs;
    /// Holds the units of work for an element.
    type Restriction: DefaultCoder + Clone + Send + Sync + 'static;
    type Tracker: RestrictionTracker<Restriction = Self::Restriction>;

    /// Whether this SDF produces a bounded PCollection: bounded by default, as Java's
    /// `@BoundedPerElement`.
    fn boundedness(&self) -> IsBounded {
        IsBounded::Bounded
    }

    /// Returns the restriction that holds all work for `element`.
    fn initial_restriction(&self, element: &Self::In) -> Self::Restriction;

    /// Splits `restriction` before processing starts. The default does not split.
    fn split_restriction(
        &self,
        element: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        let _ = element;
        vec![restriction.clone()]
    }

    /// Estimates the work for `(element, restriction)`. Runners use it to balance work across
    /// workers. The default is `1.0`. The dynamic splitter calls it on a separate copy, from
    /// another thread, so it must depend only on its arguments.
    fn restriction_size(&self, element: &Self::In, restriction: &Self::Restriction) -> f64 {
        let _ = (element, restriction);
        1.0
    }

    /// Truncates a restriction during a pipeline drain. By default a bounded SDF keeps the
    /// whole restriction and completes it; an unbounded SDF returns `None` and stops at once.
    fn truncate_restriction(
        &self,
        element: &Self::In,
        restriction: &Self::Restriction,
    ) -> Option<Self::Restriction> {
        let _ = element;
        if self.boundedness() == IsBounded::Bounded {
            Some(restriction.clone())
        } else {
            None
        }
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker;

    /// Processes `element` under the control of `tracker`. The [`ProcessContinuation`] tells
    /// whether processing is complete or must resume later.
    fn process_element(
        &mut self,
        element: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result<ProcessContinuation>;

    /// Called once when the owning bundle processor is created.
    fn setup(&mut self) -> crate::Result {
        Ok(())
    }

    /// Called before the first element of a bundle.
    fn start_bundle(&mut self) -> crate::Result {
        Ok(())
    }

    /// Called after the last element of a bundle. Outputs name their window; see
    /// [`FinishBundleContext`].
    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> crate::Result {
        let _ = ctx;
        Ok(())
    }

    /// Called once when the bundle processor is discarded.
    fn teardown(&mut self) -> crate::Result {
        Ok(())
    }

    /// Adds display data for runner UIs.
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        let _ = builder;
    }

    /// The views read with [`ProcessContext::side_input`]; [`SplittableParDo`] wires them into
    /// the graph.
    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        Vec::new()
    }
}

/// Applies a [`SplittableDoFn`] to every element of a [`PCollection`].
pub struct SplittableParDo<S> {
    name: String,
    func: Arc<S>,
    side_inputs: SideInputSpecs,
}

impl<S: SplittableDoFn> SplittableParDo<S> {
    pub fn new(name: impl Into<String>, func: S) -> Self {
        let side_inputs = side_input_specs(func.side_inputs());
        Self {
            name: name.into(),
            func: Arc::new(func),
            side_inputs,
        }
    }
}

impl<S: SplittableDoFn> HasDisplayData for SplittableParDo<S> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("fn", std::any::type_name::<S>());
        builder.add_text("transform_name", &self.name);
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
        let restriction_coder_id = S::Restriction::register_coder(pipeline);
        let ws_id = input.windowing_strategy_id();
        let mut declarer =
            OutputDeclarer::new(pipeline, &self.name, &ws_id, self.func.boundedness(), 0);
        let collections = S::Out::declare(&mut declarer);

        let mut builder = DisplayDataBuilder::with_namespace(self.name.clone());
        self.populate_display_data(&mut builder);

        // The harness gets the expanded SDF stages from `stage_handler` of the one handler. A
        // registered type goes into the spec, so the worker needs no rebuilt pipeline.
        let registration = ParDoRegistration::new(pipeline, &self.name, input.id())
            .outputs(declarer.into_tags())
            .side_inputs(self.side_inputs.clone())
            .restriction_coder(restriction_coder_id)
            .requests_finalization(<S::Out as Outputs>::FINALIZES)
            .display_data(builder.into_proto());
        match graph_spec(pipeline, &self.name, self.func.as_ref()) {
            Some(spec) => registration.register_spec(spec),
            None => {
                registration.register(Arc::new(SplittableDoFnHandler::new(S::clone(&self.func))))
            }
        };

        collections
    }
}

/// `SPLIT_AND_SIZE` step: the initial splits of `restriction`, each with its size.
fn split_and_size<S: SplittableDoFn>(
    func: &S,
    element: &S::In,
    restriction: &S::Restriction,
) -> Vec<(S::Restriction, f64)> {
    func.split_restriction(element, restriction)
        .into_iter()
        .map(|split| {
            let size = func.restriction_size(element, &split);
            (split, size)
        })
        .collect()
}

/// `TRUNCATE` step: the truncated restriction and its size, or `None` to drop it.
fn truncate_and_size<S: SplittableDoFn>(
    func: &S,
    element: &S::In,
    restriction: &S::Restriction,
) -> Option<(S::Restriction, f64)> {
    func.truncate_restriction(element, restriction)
        .map(|truncated| {
            let size = func.restriction_size(element, &truncated);
            (truncated, size)
        })
}

/// Checkpoints `tracker` and returns the sized residual, if any work is left.
fn checkpoint_and_size<S: SplittableDoFn>(
    func: &S,
    tracker: &S::Tracker,
    element: &S::In,
) -> Option<(S::Restriction, f64)> {
    tracker.try_checkpoint().map(|residual| {
        let size = func.restriction_size(element, &residual);
        (residual, size)
    })
}

/// Fails if a bounded tracker left work unclaimed.
fn check_done<T: RestrictionTracker>(tracker: &T) -> Result<(), String> {
    if tracker.boundedness() == IsBounded::Bounded {
        tracker
            .check_done()
            .map_err(|e| format!("SDF restriction validation error: {e}"))?;
    }
    Ok(())
}

/// Prepends the windowed value header to an encoded residual.
fn with_header(header: &[u8], residual: Vec<u8>) -> Vec<u8> {
    if header.is_empty() {
        residual
    } else {
        [header, &residual].concat()
    }
}

/// The element type of the input `PCollection` of `S`.
type Element<S> = <<S as SplittableDoFn>::In as DoFnInput>::Value;

/// Whether `S` reads the window, so every stage runs once per window.
fn observes_window<S: SplittableDoFn>() -> bool {
    <S::In as DoFnInput>::OBSERVES_WINDOW
}

/// Whether `process_element` runs once per window: when it reads the window or side inputs.
fn explodes_windows<S: SplittableDoFn>(func: &S) -> bool {
    observes_window::<S>() || !func.side_inputs().is_empty()
}

/// Pushes the output of a restriction stage. A window-observing SDF processes each window on
/// its own, so its outputs carry the single-window header.
fn push_stage_output<S: SplittableDoFn>(
    ctx: &mut HandlerContext<'_>,
    header: &WindowedHeader,
    encoded: Vec<u8>,
) -> Result<(), String> {
    if observes_window::<S>() {
        ctx.sink.push_windowed(header, encoded)
    } else {
        ctx.sink.push(encoded)
    }
}

/// Processes `restriction` in place, resuming on each checkpoint until no work is left.
fn run_to_completion<S: SplittableDoFn>(
    func: &mut S,
    element: &S::In,
    restriction: S::Restriction,
    ctx: &mut ProcessContext<'_, S::Out>,
) -> Result<(), String> {
    let mut tracker = func.create_tracker(&restriction);
    loop {
        let continuation = func.process_element(element.clone(), &tracker, ctx)?;
        if !continuation.is_resume() {
            break;
        }
        if let Some(delay) = continuation.delay() {
            std::thread::sleep(delay);
        }
        match tracker.try_checkpoint() {
            Some(residual) => tracker = func.create_tracker(&residual),
            None => break,
        }
    }
    check_done(&tracker)
}

/// Runs the full SDF lifecycle per element when the stage is not expanded, and gives the
/// handlers for the expanded SDF stages. Each instance owns its function.
#[derive(Clone)]
pub struct SplittableDoFnHandler<S> {
    func: S,
    explode: bool,
}

impl<S: SplittableDoFn> SplittableDoFnHandler<S> {
    pub fn new(func: S) -> Self {
        let explode = explodes_windows(&func);
        Self { func, explode }
    }
}

impl<S: SplittableDoFn> BundleHandler for SplittableDoFnHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn setup(&mut self) -> Result<(), String> {
        Ok(self.func.setup()?)
    }

    fn start_bundle(&mut self) -> Result<(), String> {
        Ok(self.func.start_bundle()?)
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        ctx.header.explode_if(self.explode)?.try_for_each(|header| {
            let value = Element::<S>::decode(element)
                .map_err(|e| format!("Failed to decode SDF input: {e}"))?;
            let input = S::In::from_element(value, &header)?;
            let mut pctx = ctx.as_process_context::<S::Out>().with_header(&header);
            let initial = self.func.initial_restriction(&input);
            self.func
                .split_restriction(&input, &initial)
                .into_iter()
                .try_for_each(|split| run_to_completion(&mut self.func, &input, split, &mut pctx))
        })
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let mut fctx = FinishBundleContext::new(ctx.as_process_context::<S::Out>());
        Ok(self.func.finish_bundle(&mut fctx)?)
    }

    fn teardown(&mut self) -> Result<(), String> {
        Ok(self.func.teardown()?)
    }

    fn stage_handler(&self, stage_urn: &str) -> Option<Arc<dyn BundleHandler>> {
        let func = self.func.clone();
        match stage_urn {
            URN_SDF_PAIR_WITH_RESTRICTION => Some(Arc::new(SdfPairWithRestrictionHandler { func })),
            URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS => Some(Arc::new(SdfSplitAndSizeHandler { func })),
            URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS => {
                Some(Arc::new(SdfProcessSizedElementsHandler::new(func)))
            }
            URN_SDF_TRUNCATE_SIZED_RESTRICTIONS => {
                Some(Arc::new(SdfTruncateSizedRestrictionsHandler { func }))
            }
            _ => None,
        }
    }
}

/// Expanded `PAIR_WITH_RESTRICTION` step: `element` -> `KV(element, restriction)`.
#[derive(Clone)]
pub struct SdfPairWithRestrictionHandler<S> {
    pub func: S,
}

impl<S: SplittableDoFn> BundleHandler for SdfPairWithRestrictionHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let header = ctx.header;
        header
            .explode_if(observes_window::<S>())?
            .try_for_each(|header| {
                let value = Element::<S>::decode(element)
                    .map_err(|e| format!("Failed to decode PairWithRestriction element: {e}"))?;
                let input = S::In::from_element(value, &header)?;
                let restriction = self.func.initial_restriction(&input);
                let encoded = (input.into_element(), restriction)
                    .encode()
                    .map_err(|e| format!("Failed to encode (element, restriction): {e}"))?;
                push_stage_output::<S>(ctx, &header, encoded)
            })
    }
}

/// Expanded `SPLIT_AND_SIZE_RESTRICTIONS` step: `KV(element, restriction)` ->
/// `KV(KV(element, restriction), size: f64)`.
#[derive(Clone)]
pub struct SdfSplitAndSizeHandler<S> {
    pub func: S,
}

impl<S: SplittableDoFn> BundleHandler for SdfSplitAndSizeHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let header = ctx.header;
        header
            .explode_if(observes_window::<S>())?
            .try_for_each(|header| {
                let (value, restriction) =
                    <(Element<S>, S::Restriction)>::decode_with_schema(element, ctx.input_schema)
                        .map_err(|e| format!("Failed to decode (element, restriction): {e}"))?;
                let input = S::In::from_element(value, &header)?;
                split_and_size(&self.func, &input, &restriction)
                    .into_iter()
                    .try_for_each(|(split, size)| {
                        let encoded = ((input.clone().into_element(), split), size)
                            .encode()
                            .map_err(|e| {
                                format!("Failed to encode ((element, restriction), size): {e}")
                            })?;
                        push_stage_output::<S>(ctx, &header, encoded)
                    })
            })
    }
}

/// Expanded `PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS` step:
/// `KV(KV(element, restriction), size: f64)` -> `S::Out`.
///
/// The dynamic splitter sizes restrictions from another thread while `process_element` runs.
/// It uses `sizer`, a second copy of the function, so the copy that processes needs no lock.
pub struct SdfProcessSizedElementsHandler<S> {
    pub func: S,
    sizer: Arc<S>,
    explode: bool,
}

impl<S: SplittableDoFn> SdfProcessSizedElementsHandler<S> {
    pub fn new(func: S) -> Self {
        let sizer = Arc::new(func.clone());
        let explode = explodes_windows(&func);
        Self {
            func,
            sizer,
            explode,
        }
    }
}

impl<S: SplittableDoFn> BundleHandler for SdfProcessSizedElementsHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(Self::new(self.func.clone()))
    }

    fn setup(&mut self) -> Result<(), String> {
        Ok(self.func.setup()?)
    }

    fn start_bundle(&mut self) -> Result<(), String> {
        Ok(self.func.start_bundle()?)
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let schema = ctx.input_schema;
        let decode = || {
            <((Element<S>, S::Restriction), f64)>::decode_with_schema(element, schema)
                .map_err(|e| format!("Failed to decode ((element, restriction), size): {e}"))
        };
        let input_header = ctx.header;
        let headers: Vec<_> = input_header.explode_if(self.explode)?.collect();
        for (i, header) in headers.iter().enumerate() {
            let ((value, restriction), _size) = decode()?;
            let rest = remaining_windows(&headers[i + 1..], element)?;
            let value = S::In::from_element(value, header)?;
            let tracker = Arc::new(self.func.create_tracker(&restriction));
            let splitter = super::dynamic_split::SdfDynamicSplitter::new(
                ctx.transform_id().to_string(),
                Arc::clone(&self.sizer),
                tracker.clone(),
                value.clone(),
                (**header).clone(),
            )
            .with_remaining_windows(rest.clone());
            let split_guard = ctx.register_dynamic_split(Arc::new(splitter));

            let mut pctx = ctx.as_process_context::<S::Out>().with_header(header);
            let continuation = self
                .func
                .process_element(value.clone(), &tracker, &mut pctx)?;
            drop(split_guard);

            let checkpoint = continuation
                .is_resume()
                .then(|| checkpoint_and_size(&self.func, &*tracker, &value))
                .flatten();
            if let Some((residual, size)) = checkpoint {
                let residual_bytes = ((value.into_element(), residual), size)
                    .encode()
                    .map_err(|e| format!("Failed to encode SDF residual: {e}"))?;
                let output_watermarks = tracker
                    .current_watermark()
                    .map(|wm| HashMap::from([("out".to_string(), wm)]))
                    .unwrap_or_default();
                let residual = |element| ResidualApplication {
                    transform_id: ctx.transform_id().to_string(),
                    input_id: "in".to_string(),
                    element,
                    output_watermarks: output_watermarks.clone(),
                    boundedness: tracker.boundedness(),
                    delay: continuation.delay(),
                };
                // Runners decode the input's windowed value header to reschedule it.
                ctx.add_residual(residual(with_header(header.as_bytes(), residual_bytes)));
                // The windows after this one wait for the runner too, as in Java.
                if let Some(rest) = rest.filter(|r| !r.taken.swap(true, Ordering::AcqRel)) {
                    ctx.add_residual(residual(rest.element));
                }
                return Ok(());
            }
            if !continuation.is_resume() {
                check_done(&*tracker)?;
            }
            // A dynamic split handed the windows after this one to the runner.
            if rest.is_some_and(|r| r.taken.load(Ordering::Acquire)) {
                return Ok(());
            }
        }
        Ok(())
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let mut fctx = FinishBundleContext::new(ctx.as_process_context::<S::Out>());
        Ok(self.func.finish_bundle(&mut fctx)?)
    }

    fn teardown(&mut self) -> Result<(), String> {
        Ok(self.func.teardown()?)
    }
}

/// Expanded `TRUNCATE_SIZED_RESTRICTIONS` step: `KV(KV(element, restriction), size: f64)` in
/// and out. Emits nothing if the restriction is dropped.
#[derive(Clone)]
pub struct SdfTruncateSizedRestrictionsHandler<S> {
    pub func: S,
}

impl<S: SplittableDoFn> BundleHandler for SdfTruncateSizedRestrictionsHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let header = ctx.header;
        header
            .explode_if(observes_window::<S>())?
            .try_for_each(|header| {
                let ((value, restriction), _size) =
                    <((Element<S>, S::Restriction), f64)>::decode_with_schema(
                        element,
                        ctx.input_schema,
                    )
                    .map_err(|e| {
                        format!("Failed to decode ((element, restriction), size) in truncate: {e}")
                    })?;
                let input = S::In::from_element(value, &header)?;
                truncate_and_size(&self.func, &input, &restriction)
                    .map(|(truncated, size)| {
                        let encoded = ((input.into_element(), truncated), size).encode().map_err(
                            |e| {
                                format!(
                                    "Failed to encode truncated ((element, restriction), size): {e}"
                                )
                            },
                        )?;
                        push_stage_output::<S>(ctx, &header, encoded)
                    })
                    .unwrap_or(Ok(()))
            })
    }
}

/// The input element restricted to `rest`, the windows not processed yet, as a residual with
/// its initial restriction. `None` if no window is left.
fn remaining_windows(
    rest: &[std::borrow::Cow<'_, WindowedHeader>],
    element: &[u8],
) -> Result<Option<super::dynamic_split::RemainingWindows>, String> {
    let Some(first) = rest.first() else {
        return Ok(None);
    };
    let windows: Vec<Vec<u8>> = rest.iter().map(|h| h.window_bytes().to_vec()).collect();
    let header = WindowedHeader::with_metadata(
        first.timestamp_millis(),
        &windows,
        first.pane(),
        &first.metadata(),
    );
    Ok(Some(super::dynamic_split::RemainingWindows {
        element: with_header(header.as_bytes(), element.to_vec()),
        taken: Arc::new(AtomicBool::new(false)),
    }))
}
