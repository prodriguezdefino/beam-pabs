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

use super::process_continuation::ProcessContinuation;
use super::tracker::RestrictionTracker;
use crate::coders::DefaultCoder;
use crate::internals::ParDoRegistration;
use crate::pipeline::constants::{
    URN_SDF_PAIR_WITH_RESTRICTION, URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
    URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS, URN_SDF_TRUNCATE_SIZED_RESTRICTIONS,
};
use crate::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use crate::transforms::dofn::context::{HandlerContext, ProcessContext, ResidualApplication};
use crate::transforms::{BundleHandler, HandlerInstance, PTransform, TransformFn};
use crate::values::AnySideInput;
use crate::values::{IsBounded, PCollection};

/// Processes elements paired with a restriction that the runner can split dynamically.
///
/// Each bundle processor runs its own clone, so `Clone` must produce an independent
/// instance. The methods take `&self`, not `&mut self` as in [`DoFn`](crate::transforms::DoFn):
/// the dynamic splitter calls [`restriction_size`](Self::restriction_size) from another thread
/// while [`process_element`](Self::process_element) runs.
pub trait SplittableDoFn: Clone + Send + Sync + 'static {
    type In: DefaultCoder + Clone;
    type Out: DefaultCoder;
    /// Holds the units of work for an element.
    type Restriction: DefaultCoder + Clone + Send + Sync + 'static;
    type Position: Send + Sync + 'static;
    type Tracker: RestrictionTracker<Position = Self::Position, Restriction = Self::Restriction>;

    /// Returns `true` (the default) if this SDF produces a bounded PCollection.
    fn is_bounded(&self) -> bool {
        true
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
    /// workers. The default is `1.0`.
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
        if self.is_bounded() {
            Some(restriction.clone())
        } else {
            None
        }
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker;

    /// Processes `element` under the control of `tracker`. The [`ProcessContinuation`] tells
    /// whether processing is complete or must resume later.
    fn process_element(
        &self,
        element: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result<ProcessContinuation>;

    /// Called once when the owning bundle processor is created.
    fn setup(&self) -> crate::Result {
        Ok(())
    }

    /// Called before the first element of a bundle.
    fn start_bundle(&self) -> crate::Result {
        Ok(())
    }

    /// Called after the last element of a bundle. It can emit, but `ctx.emit` uses the last
    /// header that the runner sent, often the global window of the impulse. To flush buffered
    /// elements, store each [`ProcessContext::header`] and emit with
    /// [`ctx.output(v).windowed(&header)`](crate::transforms::OutputBuilder::windowed).
    fn finish_bundle(&self, ctx: &mut ProcessContext<'_, Self::Out>) -> crate::Result {
        let _ = ctx;
        Ok(())
    }

    /// Called once when the bundle processor is discarded.
    fn teardown(&self) -> crate::Result {
        Ok(())
    }

    /// Adds display data for runner UIs.
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        let _ = builder;
    }
}

/// Applies a [`SplittableDoFn`] to every element of a [`PCollection`].
pub struct SplittableParDo<S> {
    name: String,
    func: Arc<S>,
    side_inputs: HashMap<String, (String, model::pipeline::SideInput)>,
}

impl<S: SplittableDoFn> SplittableParDo<S> {
    pub fn new(name: impl Into<String>, func: S) -> Self {
        Self {
            name: name.into(),
            func: Arc::new(func),
            side_inputs: HashMap::new(),
        }
    }

    pub fn with_side_input(mut self, view: &(dyn AnySideInput + '_)) -> Self {
        self.side_inputs.insert(
            view.tag().to_string(),
            (view.pcollection_id().to_string(), view.to_proto()),
        );
        self
    }

    pub fn with_side_inputs(self, views: &[&(dyn AnySideInput + '_)]) -> Self {
        views
            .iter()
            .fold(self, |acc, view| acc.with_side_input(*view))
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

impl<S: SplittableDoFn> PTransform<PCollection<S::In>> for SplittableParDo<S> {
    type Output = PCollection<S::Out>;

    fn expand(&self, input: &PCollection<S::In>) -> PCollection<S::Out> {
        let pipeline = input.pipeline();
        let out_coder_id = S::Out::register_coder(pipeline);
        let restriction_coder_id = S::Restriction::register_coder(pipeline);

        let is_bounded = if self.func.is_bounded() {
            IsBounded::Bounded
        } else {
            IsBounded::Unbounded
        };

        let out_pcoll = pipeline.add_pcollection_with_windowing::<S::Out>(
            &format!("{}.out", self.name),
            &out_coder_id,
            is_bounded,
            &input.windowing_strategy_id(),
        );

        let mut builder = DisplayDataBuilder::with_namespace(self.name.clone());
        self.populate_display_data(&mut builder);

        // Register one handler. The harness gets the expanded SDF stages from `stage_handler`.
        let handler: TransformFn = Arc::new(SplittableDoFnHandler {
            func: Arc::clone(&self.func),
        });
        ParDoRegistration::new(pipeline, &self.name, input.id())
            .output("out", out_pcoll.id())
            .side_inputs(self.side_inputs.clone())
            .restriction_coder(restriction_coder_id)
            .display_data(builder.into_proto())
            .register(handler);

        out_pcoll
    }
}

/// Returns an unshared copy of the SDF for a new handler instance. The `Arc` is for dynamic
/// split requests; each instance still needs a function of its own.
fn own_copy<S: Clone>(func: &Arc<S>) -> Arc<S> {
    Arc::new(S::clone(func))
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
    if tracker.is_bounded() {
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

/// Processes `restriction` in place, resuming on each checkpoint until no work is left.
fn run_to_completion<S: SplittableDoFn>(
    func: &S,
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
/// handlers for the expanded SDF stages.
pub struct SplittableDoFnHandler<S> {
    pub func: Arc<S>,
}

impl<S: SplittableDoFn> BundleHandler for SplittableDoFnHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(Self {
            func: own_copy(&self.func),
        })
    }

    fn setup(&mut self) -> Result<(), String> {
        Ok(self.func.setup()?)
    }

    fn start_bundle(&mut self) -> Result<(), String> {
        Ok(self.func.start_bundle()?)
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let value =
            S::In::decode(element).map_err(|e| format!("Failed to decode SDF input: {e}"))?;

        let header = ctx.header;
        header.explode()?.try_for_each(|header| {
            let mut pctx = ctx.as_process_context::<S::Out>().with_header(&header);
            let initial = self.func.initial_restriction(&value);
            self.func
                .split_restriction(&value, &initial)
                .into_iter()
                .try_for_each(|split| run_to_completion(&*self.func, &value, split, &mut pctx))
        })
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let mut pctx = ctx.as_process_context::<S::Out>();
        Ok(self.func.finish_bundle(&mut pctx)?)
    }

    fn teardown(&mut self) -> Result<(), String> {
        Ok(self.func.teardown()?)
    }

    fn stage_handler(&self, stage_urn: &str) -> Option<Arc<dyn BundleHandler>> {
        match stage_urn {
            URN_SDF_PAIR_WITH_RESTRICTION => Some(Arc::new(SdfPairWithRestrictionHandler {
                func: Arc::clone(&self.func),
            })),
            URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS => Some(Arc::new(SdfSplitAndSizeHandler {
                func: Arc::clone(&self.func),
            })),
            URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS => {
                Some(Arc::new(SdfProcessSizedElementsHandler {
                    func: Arc::clone(&self.func),
                }))
            }
            URN_SDF_TRUNCATE_SIZED_RESTRICTIONS => {
                Some(Arc::new(SdfTruncateSizedRestrictionsHandler {
                    func: Arc::clone(&self.func),
                }))
            }
            _ => None,
        }
    }
}

/// Expanded `PAIR_WITH_RESTRICTION` step: `element` -> `KV(element, restriction)`.
pub struct SdfPairWithRestrictionHandler<S> {
    pub func: Arc<S>,
}

impl<S: SplittableDoFn> BundleHandler for SdfPairWithRestrictionHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(Self {
            func: own_copy(&self.func),
        })
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let value = S::In::decode(element)
            .map_err(|e| format!("Failed to decode PairWithRestriction element: {e}"))?;
        let restriction = self.func.initial_restriction(&value);

        let encoded = (value, restriction)
            .encode()
            .map_err(|e| format!("Failed to encode (element, restriction): {e}"))?;
        ctx.sink.push(encoded)
    }
}

/// Expanded `SPLIT_AND_SIZE_RESTRICTIONS` step: `KV(element, restriction)` ->
/// `KV(KV(element, restriction), size: f64)`.
pub struct SdfSplitAndSizeHandler<S> {
    pub func: Arc<S>,
}

impl<S: SplittableDoFn> BundleHandler for SdfSplitAndSizeHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(Self {
            func: own_copy(&self.func),
        })
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let (value, restriction) =
            <(S::In, S::Restriction)>::decode_with_schema(element, ctx.input_schema)
                .map_err(|e| format!("Failed to decode (element, restriction): {e}"))?;

        split_and_size(&*self.func, &value, &restriction)
            .into_iter()
            .try_for_each(|(split, size)| {
                let encoded = ((value.clone(), split), size)
                    .encode()
                    .map_err(|e| format!("Failed to encode ((element, restriction), size): {e}"))?;
                ctx.sink.push(encoded)
            })
    }
}

/// Expanded `PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS` step:
/// `KV(KV(element, restriction), size: f64)` -> `S::Out`.
pub struct SdfProcessSizedElementsHandler<S> {
    pub func: Arc<S>,
}

impl<S: SplittableDoFn> BundleHandler for SdfProcessSizedElementsHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(Self {
            func: own_copy(&self.func),
        })
    }

    fn setup(&mut self) -> Result<(), String> {
        Ok(self.func.setup()?)
    }

    fn start_bundle(&mut self) -> Result<(), String> {
        Ok(self.func.start_bundle()?)
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let ((value, restriction), _size) =
            <((S::In, S::Restriction), f64)>::decode_with_schema(element, ctx.input_schema)
                .map_err(|e| format!("Failed to decode ((element, restriction), size): {e}"))?;

        let header = ctx.header;
        header.explode()?.try_for_each(|header| {
            let tracker = Arc::new(self.func.create_tracker(&restriction));
            let splitter = Arc::new(super::dynamic_split::SdfDynamicSplitter::new(
                ctx.transform_id().to_string(),
                self.func.clone(),
                tracker.clone(),
                value.clone(),
                (*header).clone(),
            ));
            let _split_guard = ctx.register_dynamic_split(splitter);

            let mut pctx = ctx.as_process_context::<S::Out>().with_header(&header);
            let continuation = self
                .func
                .process_element(value.clone(), &tracker, &mut pctx)?;

            if continuation.is_resume()
                && let Some((residual, size)) = checkpoint_and_size(&*self.func, &*tracker, &value)
            {
                let residual_bytes = ((value.clone(), residual), size)
                    .encode()
                    .map_err(|e| format!("Failed to encode SDF residual: {e}"))?;
                let output_watermarks = tracker
                    .current_watermark()
                    .map(|wm| HashMap::from([("out".to_string(), wm)]))
                    .unwrap_or_default();

                ctx.add_residual(ResidualApplication {
                    transform_id: ctx.transform_id().to_string(),
                    input_id: "in".to_string(),
                    // Runners decode the input's windowed value header to reschedule it.
                    element: with_header(header.as_bytes(), residual_bytes),
                    output_watermarks,
                    is_bounded: tracker.is_bounded(),
                    delay: continuation.delay(),
                });
            }

            if continuation.is_resume() {
                Ok(())
            } else {
                check_done(&*tracker)
            }
        })
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let mut pctx = ctx.as_process_context::<S::Out>();
        Ok(self.func.finish_bundle(&mut pctx)?)
    }

    fn teardown(&mut self) -> Result<(), String> {
        Ok(self.func.teardown()?)
    }
}

/// Expanded `TRUNCATE_SIZED_RESTRICTIONS` step: `KV(KV(element, restriction), size: f64)` in
/// and out. Emits nothing if the restriction is dropped.
pub struct SdfTruncateSizedRestrictionsHandler<S> {
    pub func: Arc<S>,
}

impl<S: SplittableDoFn> BundleHandler for SdfTruncateSizedRestrictionsHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(Self {
            func: own_copy(&self.func),
        })
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let ((value, restriction), _size) =
            <((S::In, S::Restriction), f64)>::decode_with_schema(element, ctx.input_schema)
                .map_err(|e| {
                    format!("Failed to decode ((element, restriction), size) in truncate: {e}")
                })?;

        truncate_and_size(&*self.func, &value, &restriction)
            .map(|(truncated, size)| {
                let encoded = ((value, truncated), size).encode().map_err(|e| {
                    format!("Failed to encode truncated ((element, restriction), size): {e}")
                })?;
                ctx.sink.push(encoded)
            })
            .unwrap_or(Ok(()))
    }
}
