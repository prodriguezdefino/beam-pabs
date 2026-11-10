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

//! Handlers of the expanded splittable DoFn stages that a runner schedules.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::handler::{check_done, resumed_tracker, with_state_of};
use super::splittable_dofn::{
    Element, SplittableDoFn, WithWatermarkState, explodes_windows, observes_window,
};
use super::tracker::RestrictionTracker;
use crate::coders::{DefaultCoder, FieldMapCache, WindowedHeader};
use crate::transforms::dofn::context::{FinishBundleContext, HandlerContext, ResidualApplication};
use crate::transforms::dofn::input::DoFnInput;
use crate::transforms::{BundleHandler, HandlerInstance};

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

/// Prepends the windowed value header to an encoded residual.
fn with_header(header: &[u8], residual: Vec<u8>) -> Vec<u8> {
    if header.is_empty() {
        residual
    } else {
        [header, &residual].concat()
    }
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

/// Expanded `PAIR_WITH_RESTRICTION` step: `element` -> `KV(element, restriction)`.
#[derive(Clone)]
pub struct SdfPairWithRestrictionHandler<S> {
    pub func: S,
    field_map: FieldMapCache,
}

impl<S> SdfPairWithRestrictionHandler<S> {
    /// The handler of the stage that pairs each element of `func` with its initial restriction.
    pub fn new(func: S) -> Self {
        Self {
            func,
            field_map: FieldMapCache::default(),
        }
    }
}

impl<S: SplittableDoFn> BundleHandler for SdfPairWithRestrictionHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let header = ctx.header;
        header
            .explode_if(observes_window::<S>())
            .map_err(|e| e.to_string())?
            .try_for_each(|header| {
                // Decodes per window header because from_element consumes the value and Element<S>
                // does not require Clone.
                let value = self
                    .field_map
                    .decode::<Element<S>>(element, ctx.input_schema, ctx.state_stream_reader)
                    .map_err(|e| format!("Failed to decode PairWithRestriction element: {e}"))?;
                let input = S::In::from_element(value, &header)?;
                let restriction = self.func.initial_restriction(&input)?;
                let encoded = (input.into_element(), (restriction, Vec::<u8>::new()))
                    .encode()
                    .map_err(|e| format!("Failed to encode (element, restriction): {e}"))?;
                push_stage_output::<S>(ctx, &header, encoded)
            })
    }
}

/// Expanded `SPLIT_AND_SIZE_RESTRICTIONS` step: `KV(element, restriction)` ->
/// `KV(KV(element, restriction), size: f64)`.
#[derive(Clone)]
pub(crate) struct SdfSplitAndSizeHandler<S> {
    pub func: S,
    field_map: FieldMapCache,
}

impl<S> SdfSplitAndSizeHandler<S> {
    pub(crate) fn new(func: S) -> Self {
        Self {
            func,
            field_map: FieldMapCache::default(),
        }
    }
}

impl<S: SplittableDoFn> BundleHandler for SdfSplitAndSizeHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let header = ctx.header;
        header
            .explode_if(observes_window::<S>())
            .map_err(|e| e.to_string())?
            .try_for_each(|header| {
                let (value, (restriction, state)) = self
                    .field_map
                    .decode::<(Element<S>, WithWatermarkState<S>)>(
                        element,
                        ctx.input_schema,
                        ctx.state_stream_reader,
                    )
                    .map_err(|e| format!("Failed to decode (element, restriction): {e}"))?;
                let input = S::In::from_element(value, &header)?;
                // Each split starts from the estimator state of the restriction it came from.
                split_and_size(&self.func, &input, &restriction)
                    .into_iter()
                    .try_for_each(|(split, size)| {
                        let encoded =
                            ((input.clone().into_element(), (split, state.clone())), size)
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
    field_map: FieldMapCache,
}

impl<S: SplittableDoFn> SdfProcessSizedElementsHandler<S> {
    pub fn new(func: S) -> Self {
        let sizer = Arc::new(func.clone());
        let explode = explodes_windows(&func);
        Self {
            func,
            sizer,
            explode,
            field_map: FieldMapCache::default(),
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
        let state_reader = ctx.state_stream_reader;
        let input_header = ctx.header;
        let headers: Vec<_> = input_header
            .explode_if(self.explode)
            .map_err(|e| e.to_string())?
            .collect();
        for (i, header) in headers.iter().enumerate() {
            let ((value, restriction), _size) = self
                .field_map
                .decode::<((Element<S>, WithWatermarkState<S>), f64)>(element, schema, state_reader)
                .map_err(|e| format!("Failed to decode ((element, restriction), size): {e}"))?;
            let rest = remaining_windows(&headers[i + 1..], element)?;
            let value = S::In::from_element(value, header)?;
            let tracker = Arc::new(resumed_tracker(&self.func, &restriction)?);
            // The windows after this one restart from the initial restriction.
            let initial_watermark = tracker.current_watermark();
            let splitter = super::dynamic_split::SdfDynamicSplitter::new(
                ctx.transform_id().to_string(),
                Arc::clone(&self.sizer),
                Arc::clone(&tracker),
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
                let residual = with_state_of(&*tracker, residual)?;
                let residual_bytes = ((value.into_element(), residual), size)
                    .encode()
                    .map_err(|e| format!("Failed to encode SDF residual: {e}"))?;
                let residual = |element, output_watermark| ResidualApplication {
                    transform_id: ctx.transform_id().to_string(),
                    input_id: "in".to_string(),
                    element,
                    output_watermark,
                    delay: continuation.delay(),
                };
                // Runners decode the input's windowed value header to reschedule it.
                ctx.add_residual(residual(
                    with_header(header.as_bytes(), residual_bytes),
                    tracker.current_watermark(),
                ));
                // The windows after this one wait for the runner too.
                if let Some(rest) = rest.filter(|r| !r.taken.swap(true, Ordering::AcqRel)) {
                    ctx.add_residual(residual(rest.element, initial_watermark));
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
pub(crate) struct SdfTruncateSizedRestrictionsHandler<S> {
    pub func: S,
    field_map: FieldMapCache,
}

impl<S> SdfTruncateSizedRestrictionsHandler<S> {
    pub(crate) fn new(func: S) -> Self {
        Self {
            func,
            field_map: FieldMapCache::default(),
        }
    }
}

impl<S: SplittableDoFn> BundleHandler for SdfTruncateSizedRestrictionsHandler<S> {
    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let header = ctx.header;
        header
            .explode_if(observes_window::<S>())
            .map_err(|e| e.to_string())?
            .try_for_each(|header| {
                let ((value, (restriction, state)), _size) = self
                    .field_map
                    .decode::<((Element<S>, WithWatermarkState<S>), f64)>(
                        element,
                        ctx.input_schema,
                        ctx.state_stream_reader,
                    )
                    .map_err(|e| {
                        format!("Failed to decode ((element, restriction), size) in truncate: {e}")
                    })?;
                let input = S::In::from_element(value, &header)?;
                truncate_and_size(&self.func, &input, &restriction)
                    .map(|(truncated, size)| {
                        let encoded = ((input.into_element(), (truncated, state)), size)
                            .encode()
                            .map_err(|e| {
                                format!(
                                    "Failed to encode truncated ((element, restriction), size): {e}"
                                )
                            })?;
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
