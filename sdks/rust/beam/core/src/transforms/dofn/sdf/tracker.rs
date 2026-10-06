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

//! Restriction tracking primitives for splittable DoFns (SDF).

use thiserror::Error;

/// Errors from restriction tracking and validation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RestrictionError {
    /// A claim was for a position outside the restriction.
    #[error("Work position {0} is out of bounds for restriction")]
    OutOfBounds(String),

    /// A claim came after the tracker stopped.
    #[error("Cannot claim work after restriction tracker has stopped")]
    AlreadyStopped,

    /// A claim was for a position that is not strictly greater than the previous position.
    #[error(
        "Cannot claim position {0} because it is not strictly greater than previous position {1}"
    )]
    NonMonotonicClaim(String, String),

    /// Processing completed before all work in the restriction was claimed.
    #[error("Tracker finished before claiming all work in restriction: {0}")]
    IncompleteWork(String),

    #[error("{0}")]
    Custom(String),
}

/// Progress reported by a restriction tracker. The tracker chooses the units of both fields.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RestrictionProgress {
    pub work_completed: f64,
    pub work_remaining: f64,
}

impl RestrictionProgress {
    /// Creates a progress report. Negative values become `0.0`.
    pub fn new(work_completed: f64, work_remaining: f64) -> Self {
        Self {
            work_completed: work_completed.max(0.0),
            work_remaining: work_remaining.max(0.0),
        }
    }

    /// Returns the fraction of work completed, in `[0.0, 1.0]`. Returns `1.0` if there is no work.
    pub fn fraction_completed(&self) -> f64 {
        let total = self.work_completed + self.work_remaining;
        if total <= 0.0 {
            1.0
        } else {
            (self.work_completed / total).clamp(0.0, 1.0)
        }
    }
}

/// Tracks the progress of processing the restriction of one element.
///
/// The runner or the harness can call `try_split` or read the progress while element
/// processing runs, so implementations must be `Send + Sync`.
pub trait RestrictionTracker: Send + Sync + 'static {
    /// A position within the restriction, for example `i64` for byte offsets.
    type Position: Send + Sync + 'static;
    type Restriction: Clone + Send + Sync + 'static;

    /// Attempts to claim the work unit at `position`. Returns `false` if the position is at or
    /// after the current restriction end (after a split or when all work is done). On `false`,
    /// the DoFn must stop processing.
    fn try_claim(&self, position: &Self::Position) -> bool;

    /// Splits the remaining work at `fraction_of_remainder`, in `[0.0, 1.0]`, into
    /// `(primary, residual)`. This tracker narrows its restriction to `primary`; `residual` is
    /// rescheduled. Returns `None` if all work is claimed or a part would be empty.
    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Option<(Self::Restriction, Self::Restriction)>;

    /// Checkpoints at the current position and returns the residual to reschedule. Later
    /// claims on this tracker fail.
    fn try_checkpoint(&self) -> Option<Self::Restriction> {
        self.try_split(0.0).map(|(_primary, residual)| residual)
    }

    /// Returns a copy of the current restriction, as narrowed by splits.
    fn current_restriction(&self) -> Self::Restriction;

    fn current_progress(&self) -> RestrictionProgress;

    /// Returns an error if work is unclaimed or if an earlier claim failed.
    fn check_done(&self) -> Result<(), RestrictionError>;

    /// Returns `true` if this restriction holds a finite amount of work.
    fn is_bounded(&self) -> bool {
        true
    }

    /// Returns the current output watermark for this restriction, if the tracker has one.
    fn current_watermark(&self) -> Option<i64> {
        None
    }
}

/// A tracker that pairs a [`RestrictionTracker`] with a
/// [`WatermarkEstimator`](crate::windowing::WatermarkEstimator).
pub struct WatermarkedTracker<T, E> {
    pub tracker: T,
    pub estimator: E,
    pub is_bounded: bool,
}

impl<T, E> WatermarkedTracker<T, E> {
    /// Creates a bounded `WatermarkedTracker`.
    pub fn new(tracker: T, estimator: E) -> Self {
        Self {
            tracker,
            estimator,
            is_bounded: true,
        }
    }

    /// Sets whether this tracker holds bounded or unbounded (streaming) work.
    pub fn with_bounded(mut self, is_bounded: bool) -> Self {
        self.is_bounded = is_bounded;
        self
    }
}

impl<T: RestrictionTracker, E: crate::windowing::WatermarkEstimator> RestrictionTracker
    for WatermarkedTracker<T, E>
{
    type Position = T::Position;
    type Restriction = T::Restriction;

    fn try_claim(&self, position: &Self::Position) -> bool {
        self.tracker.try_claim(position)
    }

    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Option<(Self::Restriction, Self::Restriction)> {
        self.tracker.try_split(fraction_of_remainder)
    }

    fn try_checkpoint(&self) -> Option<Self::Restriction> {
        self.tracker.try_checkpoint()
    }

    fn current_restriction(&self) -> Self::Restriction {
        self.tracker.current_restriction()
    }

    fn current_progress(&self) -> RestrictionProgress {
        self.tracker.current_progress()
    }

    fn check_done(&self) -> Result<(), RestrictionError> {
        self.tracker.check_done()
    }

    fn is_bounded(&self) -> bool {
        self.is_bounded
    }

    fn current_watermark(&self) -> Option<i64> {
        Some(self.estimator.current_watermark())
    }
}
