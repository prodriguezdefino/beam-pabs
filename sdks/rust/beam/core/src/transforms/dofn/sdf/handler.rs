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

//! The handler that runs a whole splittable DoFn in place, and the tracker helpers that
//! the expanded stages share.

use std::sync::Arc;

use super::splittable_dofn::{Element, SplittableDoFn, WithWatermarkState, explodes_windows};
use super::stages::{
    SdfPairWithRestrictionHandler, SdfProcessSizedElementsHandler, SdfSplitAndSizeHandler,
    SdfTruncateSizedRestrictionsHandler,
};
use super::tracker::RestrictionTracker;
use crate::coders::FieldMapCache;
use crate::pipeline::constants::{
    URN_SDF_PAIR_WITH_RESTRICTION, URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
    URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS, URN_SDF_TRUNCATE_SIZED_RESTRICTIONS,
};
use crate::transforms::dofn::context::{FinishBundleContext, HandlerContext, ProcessContext};
use crate::transforms::dofn::input::DoFnInput;
use crate::transforms::{BundleHandler, HandlerInstance};
use crate::values::IsBounded;

/// Fails if a bounded tracker left work unclaimed.
pub(super) fn check_done<T: RestrictionTracker>(tracker: &T) -> Result<(), String> {
    if tracker.boundedness() == IsBounded::Bounded {
        tracker
            .check_done()
            .map_err(|e| format!("SDF restriction validation error: {e}"))?;
    }
    Ok(())
}

/// Creates the tracker of `restriction` and continues its estimator from `state`.
pub(super) fn resumed_tracker<S: SplittableDoFn>(
    func: &S,
    (restriction, state): &WithWatermarkState<S>,
) -> Result<S::Tracker, String> {
    let tracker = func.create_tracker(restriction);
    tracker
        .restore_watermark_state(state)
        .map_err(|e| format!("Failed to restore the watermark estimator state: {e}"))?;
    Ok(tracker)
}

/// Pairs `restriction` with the current estimator state of `tracker`.
pub(super) fn with_state_of<T: RestrictionTracker>(
    tracker: &T,
    restriction: T::Restriction,
) -> Result<(T::Restriction, Vec<u8>), String> {
    tracker
        .watermark_state()
        .map(|state| (restriction, state))
        .map_err(|e| format!("Failed to encode the watermark estimator state: {e}"))
}

/// Processes `restriction` in place and resumes on checkpoint until done.
///
/// For runners that do not expand SDFs, this fallback path executes
/// synchronously on the bundle thread.
fn run_to_completion<S: SplittableDoFn>(
    func: &mut S,
    element: &S::In,
    restriction: &S::Restriction,
    ctx: &mut ProcessContext<'_, S::Out>,
) -> Result<(), String> {
    let mut tracker = func.create_tracker(restriction);
    loop {
        let continuation = func.process_element(element.clone(), &tracker, ctx)?;
        if !continuation.is_resume() {
            break;
        }
        if let Some(delay) = continuation.delay() {
            std::thread::sleep(delay);
        }
        match tracker.try_checkpoint() {
            Some(residual) => {
                let residual = with_state_of(&tracker, residual)?;
                tracker = resumed_tracker(func, &residual)?;
            }
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
    field_map: FieldMapCache,
}

impl<S: SplittableDoFn> SplittableDoFnHandler<S> {
    pub fn new(func: S) -> Self {
        let explode = explodes_windows(&func);
        Self {
            func,
            explode,
            field_map: FieldMapCache::default(),
        }
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
        ctx.header
            .explode_if(self.explode)
            .map_err(|e| e.to_string())?
            .try_for_each(|header| {
                // Decodes per window header because from_element consumes the value and Element<S>
                // does not require Clone.
                let value = self
                    .field_map
                    .decode::<Element<S>>(element, ctx.input_schema, ctx.state_stream_reader)
                    .map_err(|e| format!("Failed to decode SDF input: {e}"))?;
                let input = S::In::from_element(value, &header)?;
                let mut pctx = ctx.as_process_context::<S::Out>().with_header(&header);
                let initial = self.func.initial_restriction(&input)?;
                self.func
                    .split_restriction(&input, &initial)
                    .into_iter()
                    .try_for_each(|split| {
                        run_to_completion(&mut self.func, &input, &split, &mut pctx)
                    })
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
            URN_SDF_PAIR_WITH_RESTRICTION => {
                Some(Arc::new(SdfPairWithRestrictionHandler::new(func)))
            }
            URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS => {
                Some(Arc::new(SdfSplitAndSizeHandler::new(func)))
            }
            URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS => {
                Some(Arc::new(SdfProcessSizedElementsHandler::new(func)))
            }
            URN_SDF_TRUNCATE_SIZED_RESTRICTIONS => {
                Some(Arc::new(SdfTruncateSizedRestrictionsHandler::new(func)))
            }
            _ => None,
        }
    }
}
