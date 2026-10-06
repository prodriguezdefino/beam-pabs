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

//! Runner-initiated splitting of the restriction of an active SDF element during a bundle.
//! The split returns a primary root and residual applications; the `DoFn` does not need to
//! self-checkpoint.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use model::fn_execution::BundleApplication;

use crate::Instant;
use crate::coders::{DefaultCoder, WindowedHeader};
use crate::transforms::dofn::context::ResidualApplication;
use crate::transforms::dofn::input::DoFnInput;
use crate::transforms::dofn::sdf::splittable_dofn::SplittableDoFn;
use crate::transforms::dofn::sdf::tracker::{RestrictionProgress, RestrictionTracker};

fn prepend_header(header: &WindowedHeader, encoded: Vec<u8>) -> Vec<u8> {
    if header.is_empty() {
        encoded
    } else {
        [header.as_bytes(), &encoded].concat()
    }
}

/// A dynamic split: this worker keeps `primary` in the current bundle; the runner reschedules
/// `residuals`: the rest of the current restriction, and the windows of the element that are
/// not processed yet.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicSplitResult {
    pub primary: BundleApplication,
    pub residuals: Vec<ResidualApplication>,
}

/// Implemented by the executor of an active splittable DoFn element.
pub trait DynamicSplitHandler: Send + Sync + 'static {
    fn transform_id(&self) -> &str;

    /// Returns the completed and remaining work of the current element, in the units of the
    /// tracker.
    fn current_progress(&self) -> RestrictionProgress;

    /// Splits the current restriction. `fraction_of_remainder`, in `[0.0, 1.0]`, is the part of
    /// the remaining work that the SDK keeps. Returns `None` if no split is possible.
    fn try_split(&self, fraction_of_remainder: f64) -> Option<DynamicSplitResult>;
}

/// Dynamic split handler for an active [`SplittableDoFn`] element. The split applications use
/// the input id `in`, which the harness renames to the input of the runner. The residual has
/// no requested time delay.
pub struct SdfDynamicSplitter<S: SplittableDoFn> {
    transform_id: String,
    func: Arc<S>,
    tracker: Arc<S::Tracker>,
    /// The watermark of the tracker before the element ran, for the windows after this one.
    initial_watermark: Option<Instant>,
    value: S::In,
    header: WindowedHeader,
    rest: Option<RemainingWindows>,
}

/// The windows of a multi-window element after the one being processed, with the element
/// and its initial restriction, encoded as a residual.
#[derive(Clone)]
pub(crate) struct RemainingWindows {
    /// The input with the windows not processed yet, and the initial restriction.
    pub(crate) element: Vec<u8>,
    /// Set when a split hands these windows to the runner, so processing stops after the
    /// current window.
    pub(crate) taken: Arc<AtomicBool>,
}

impl<S: SplittableDoFn> SdfDynamicSplitter<S> {
    pub fn new(
        transform_id: impl Into<String>,
        func: Arc<S>,
        tracker: Arc<S::Tracker>,
        value: S::In,
        header: WindowedHeader,
    ) -> Self {
        let initial_watermark = tracker.current_watermark();
        Self {
            transform_id: transform_id.into(),
            func,
            tracker,
            initial_watermark,
            value,
            header,
            rest: None,
        }
    }

    /// Adds the windows after the current one to the residual of a split.
    pub(crate) fn with_remaining_windows(mut self, rest: Option<RemainingWindows>) -> Self {
        self.rest = rest;
        self
    }
}

impl<S: SplittableDoFn> DynamicSplitHandler for SdfDynamicSplitter<S> {
    fn transform_id(&self) -> &str {
        &self.transform_id
    }

    fn current_progress(&self) -> RestrictionProgress {
        self.tracker.current_progress()
    }

    fn try_split(&self, fraction_of_remainder: f64) -> Option<DynamicSplitResult> {
        // The tracker narrows its restriction when it splits, and it cannot undo that. So check
        // that the element and the restriction encode first: a later failure would lose work.
        // Both parts keep the estimator state at the split, so each continues from it.
        let state = self
            .tracker
            .watermark_state()
            .inspect_err(|e| {
                tracing::error!(
                    "{}: cannot encode the watermark estimator state: {e}",
                    self.transform_id
                );
            })
            .ok()?;
        let encode = |restriction| {
            let size = self.func.restriction_size(&self.value, &restriction);
            (
                (
                    self.value.clone().into_element(),
                    (restriction, state.clone()),
                ),
                size,
            )
                .encode()
                .inspect_err(|e| {
                    tracing::error!(
                        "{}: cannot encode a split restriction: {e}",
                        self.transform_id
                    );
                })
                .ok()
        };
        encode(self.tracker.current_restriction())?;

        let (primary_restriction, residual_restriction) =
            self.tracker.try_split(fraction_of_remainder)?;
        let primary_encoded = encode(primary_restriction)?;
        let residual_encoded = encode(residual_restriction)?;

        let full_primary = prepend_header(&self.header, primary_encoded);
        let full_residual = prepend_header(&self.header, residual_encoded);

        // The primary stays in this bundle, so only the residuals carry a watermark. Roots
        // leave `is_bounded` unspecified: the proto permits BOUNDED only for a finite
        // residual, and the harness does not know if the residual is finite.
        let primary_app = BundleApplication {
            transform_id: self.transform_id.clone(),
            input_id: "in".to_string(),
            element: full_primary,
            ..BundleApplication::default()
        };

        let residual = |element, output_watermark| ResidualApplication {
            transform_id: self.transform_id.clone(),
            input_id: "in".to_string(),
            element,
            output_watermark,
            delay: None,
        };
        let mut residuals = vec![residual(full_residual, self.tracker.current_watermark())];
        if let Some(rest) = &self.rest
            && !rest.taken.swap(true, Ordering::AcqRel)
        {
            residuals.push(residual(rest.element.clone(), self.initial_watermark));
        }

        Some(DynamicSplitResult {
            primary: primary_app,
            residuals,
        })
    }
}

/// Registry for the active dynamic split handler of an in-flight bundle.
///
/// The Mutex coordinates the active handler across worker threads.
/// The critical section swaps an Arc pointer at restriction boundaries and split
/// requests.
/// Contention is rare in normal execution and this design avoids extra crate
/// dependencies.
#[derive(Clone, Default)]
pub struct DynamicSplitRegistrar {
    active: Arc<Mutex<Option<Arc<dyn DynamicSplitHandler>>>>,
}

impl DynamicSplitRegistrar {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `handler`, replacing any earlier one. The returned guard unregisters it on drop.
    pub fn register(&self, handler: Arc<dyn DynamicSplitHandler>) -> DynamicSplitGuard {
        let mut lock = self.active.lock().unwrap_or_else(PoisonError::into_inner);
        *lock = Some(Arc::clone(&handler));
        DynamicSplitGuard {
            registrar: self.clone(),
            handler,
        }
    }

    pub fn unregister(&self) {
        let mut lock = self.active.lock().unwrap_or_else(PoisonError::into_inner);
        *lock = None;
    }

    pub fn current_handler(&self) -> Option<Arc<dyn DynamicSplitHandler>> {
        self.active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Unregisters a [`DynamicSplitHandler`] on drop. If a later handler replaced it, that handler
/// stays active.
pub struct DynamicSplitGuard {
    registrar: DynamicSplitRegistrar,
    handler: Arc<dyn DynamicSplitHandler>,
}

impl Drop for DynamicSplitGuard {
    fn drop(&mut self) {
        let mut lock = self
            .registrar
            .active
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if lock.as_ref().is_some_and(|active| {
            std::ptr::addr_eq(Arc::as_ptr(active), Arc::as_ptr(&self.handler))
        }) {
            *lock = None;
        }
    }
}
