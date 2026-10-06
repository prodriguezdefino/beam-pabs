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

//! The splittable DoFn trait and the type aliases that its execution shares.

use super::process_continuation::ProcessContinuation;
use super::tracker::RestrictionTracker;
use crate::coders::DefaultCoder;
use crate::transforms::display_data::DisplayDataBuilder;
use crate::transforms::dofn::context::{FinishBundleContext, ProcessContext};
use crate::transforms::dofn::input::DoFnInput;
use crate::transforms::dofn::outputs::Outputs;
use crate::values::AnySideInput;
use crate::values::IsBounded;

/// Processes elements paired with a restriction that the runner can split dynamically.
///
/// Each bundle processor runs its own clone, so `Clone` must produce an independent
/// instance. The lifecycle methods and [`process_element`](Self::process_element) take
/// `&mut self`, as in [`DoFn`](crate::transforms::DoFn), so state kept across a bundle is
/// plain fields. Restriction methods take `&self` and run on fresh copies without
/// `setup`, `start_bundle`, or `teardown`. The dynamic splitter also calls
/// [`restriction_size`](Self::restriction_size) on a separate copy, from another thread,
/// while `process_element` runs. Make all restriction methods depend only on their
/// arguments.
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

    /// Whether the work of one element is bounded. The default is bounded. The output is
    /// unbounded if this is unbounded or the input is unbounded.
    fn boundedness(&self) -> IsBounded {
        IsBounded::Bounded
    }

    /// Returns the restriction that holds all work for `element`.
    ///
    /// Runs on a fresh copy without `setup`, `start_bundle`, or `teardown`.
    /// An error fails the bundle, and the runner retries it.
    fn initial_restriction(&self, element: &Self::In) -> crate::Result<Self::Restriction>;

    /// Splits `restriction` before processing starts. The default does not split.
    ///
    /// Runs on a fresh copy without `setup`, `start_bundle`, or `teardown`.
    fn split_restriction(
        &self,
        element: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        let _ = element;
        vec![restriction.clone()]
    }

    /// Estimates the work for `(element, restriction)`. Runners use it to balance work across
    /// workers. The default is the remaining work of a new tracker for `restriction`. Runs on
    /// a fresh copy without `setup`, `start_bundle`, or `teardown`, often from another thread
    /// during execution.
    fn restriction_size(&self, element: &Self::In, restriction: &Self::Restriction) -> f64 {
        let _ = element;
        self.create_tracker(restriction)
            .current_progress()
            .work_remaining
    }

    /// Truncates a restriction during a pipeline drain.
    ///
    /// Runs on a fresh copy without `setup`, `start_bundle`, or `teardown`.
    /// By default a bounded SDF keeps the whole restriction; an unbounded SDF stops.
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

    /// The views read with [`ProcessContext::side_input`].
    /// [`SplittableParDo`](super::SplittableParDo) wires them into the graph.
    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        Vec::new()
    }
}

/// The element type of the input `PCollection` of `S`.
pub(super) type Element<S> = <<S as SplittableDoFn>::In as DoFnInput>::Value;

/// A restriction with the encoded state of the watermark estimator that processed it, as
/// [`RestrictionTracker::watermark_state`] gives it. Empty for a restriction that no estimator
/// processed yet.
pub(super) type WithWatermarkState<S> = (<S as SplittableDoFn>::Restriction, Vec<u8>);

/// Whether `S` reads the window, so every stage runs once per window.
pub(super) fn observes_window<S: SplittableDoFn>() -> bool {
    <S::In as DoFnInput>::OBSERVES_WINDOW
}

/// Whether `process_element` runs once per window: when it reads the window or side inputs.
pub(super) fn explodes_windows<S: SplittableDoFn>(func: &S) -> bool {
    observes_window::<S>() || !func.side_inputs().is_empty()
}
