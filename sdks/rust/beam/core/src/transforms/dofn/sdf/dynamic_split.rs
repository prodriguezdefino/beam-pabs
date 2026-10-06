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
//! The split returns primary and residual `BundleApplication` roots; the `DoFn` does not need
//! to self-checkpoint.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use model::fn_execution::{BundleApplication, DelayedBundleApplication};

use crate::coders::{DefaultCoder, WindowedHeader};
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
/// `residual`.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicSplitResult {
    pub primary: BundleApplication,
    pub residual: DelayedBundleApplication,
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
/// input id `in` and output name `out`. The residual has no requested time delay.
pub struct SdfDynamicSplitter<S: SplittableDoFn> {
    transform_id: String,
    func: Arc<S>,
    tracker: Arc<S::Tracker>,
    value: S::In,
    header: WindowedHeader,
}

impl<S: SplittableDoFn> SdfDynamicSplitter<S> {
    pub fn new(
        transform_id: impl Into<String>,
        func: Arc<S>,
        tracker: Arc<S::Tracker>,
        value: S::In,
        header: WindowedHeader,
    ) -> Self {
        Self {
            transform_id: transform_id.into(),
            func,
            tracker,
            value,
            header,
        }
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
        let (primary_restriction, residual_restriction) =
            self.tracker.try_split(fraction_of_remainder)?;

        let primary_size = self
            .func
            .restriction_size(&self.value, &primary_restriction);
        let residual_size = self
            .func
            .restriction_size(&self.value, &residual_restriction);

        let primary_encoded = ((self.value.clone(), primary_restriction), primary_size)
            .encode()
            .ok()?;
        let residual_encoded = ((self.value.clone(), residual_restriction), residual_size)
            .encode()
            .ok()?;

        let full_primary = prepend_header(&self.header, primary_encoded);
        let full_residual = prepend_header(&self.header, residual_encoded);

        let output_watermarks = self
            .tracker
            .current_watermark()
            .map(|wm| {
                HashMap::from([("out".to_string(), crate::windowing::watermark_to_proto(wm))])
            })
            .unwrap_or_default();

        let is_bounded = if self.tracker.is_bounded() {
            model::pipeline::is_bounded::Enum::Bounded
        } else {
            model::pipeline::is_bounded::Enum::Unbounded
        } as i32;

        let primary_app = BundleApplication {
            transform_id: self.transform_id.clone(),
            input_id: "in".to_string(),
            element: full_primary,
            output_watermarks: output_watermarks.clone(),
            is_bounded,
        };

        let residual_app = DelayedBundleApplication {
            application: Some(BundleApplication {
                transform_id: self.transform_id.clone(),
                input_id: "in".to_string(),
                element: full_residual,
                output_watermarks,
                is_bounded,
            }),
            requested_time_delay: None,
        };

        Some(DynamicSplitResult {
            primary: primary_app,
            residual: residual_app,
        })
    }
}

/// Registry for the active dynamic split handler of an in-flight bundle.
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
        if let Ok(mut lock) = self.active.lock() {
            *lock = Some(Arc::clone(&handler));
        }
        DynamicSplitGuard {
            registrar: self.clone(),
            handler,
        }
    }

    pub fn unregister(&self) {
        if let Ok(mut lock) = self.active.lock() {
            *lock = None;
        }
    }

    pub fn current_handler(&self) -> Option<Arc<dyn DynamicSplitHandler>> {
        self.active.lock().ok().and_then(|guard| guard.clone())
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
        if let Ok(mut lock) = self.registrar.active.lock()
            && lock.as_ref().is_some_and(|active| {
                std::ptr::addr_eq(Arc::as_ptr(active), Arc::as_ptr(&self.handler))
            })
        {
            *lock = None;
        }
    }
}
