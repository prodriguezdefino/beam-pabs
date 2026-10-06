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

//! Offset range restriction and tracker for splittable DoFns: byte offsets in files, indices
//! in collections, or numeric ranges.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::tracker::{RestrictionError, RestrictionProgress, RestrictionTracker};
use crate::coders::{Coder, CoderError, CoderRegistry, DefaultCoder};

/// A half-closed range of work `[start, end)`: `start` is inclusive, `end` is exclusive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OffsetRange {
    pub start: i64,
    pub end: i64,
}

impl OffsetRange {
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// Returns `end - start`, minimum 0.
    pub fn size(&self) -> f64 {
        self.end.saturating_sub(self.start).max(0) as f64
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    pub fn contains(&self, position: i64) -> bool {
        position >= self.start && position < self.end
    }

    /// Splits into `num` sub-ranges of about equal size. Returns `[self]` if `num <= 1` or the
    /// range is empty.
    pub fn even_splits(&self, num: i64) -> Vec<OffsetRange> {
        if num <= 1 || self.is_empty() {
            return vec![*self];
        }

        let size = self.end - self.start;
        let splits: Vec<OffsetRange> = (0..num)
            .map(|i| {
                let s = self.start + (i * size / num);
                let e = self.start + ((i + 1) * size / num);
                OffsetRange::new(s, e)
            })
            .filter(|r| !r.is_empty())
            .collect();

        if splits.is_empty() {
            vec![*self]
        } else {
            splits
        }
    }

    /// Splits into sub-ranges of at most `split_size`. Returns `[self]` if `split_size <= 0` or
    /// the range is empty.
    pub fn sized_splits(&self, split_size: i64) -> Vec<OffsetRange> {
        if split_size <= 0 || self.is_empty() {
            return vec![*self];
        }

        let splits: Vec<OffsetRange> = std::iter::successors(Some(self.start), |&curr| {
            let next = curr.saturating_add(split_size);
            (next < self.end).then_some(next)
        })
        .map(|curr| OffsetRange::new(curr, (curr + split_size).min(self.end)))
        .collect();

        if splits.is_empty() {
            vec![*self]
        } else {
            splits
        }
    }
}

/// Default coder for [`OffsetRange`]. The wire format is a KV of two `i64`: `start`, `end`.
#[derive(Clone, Copy, Debug, Default)]
pub struct OffsetRangeCoder;

impl Coder<OffsetRange> for OffsetRangeCoder {
    fn urn(&self) -> &'static str {
        crate::coders::URN_KV
    }

    fn encode(
        &self,
        value: &OffsetRange,
        writer: &mut dyn Write,
        _context: crate::coders::Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(
        &self,
        reader: &mut dyn Read,
        _context: crate::coders::Context,
    ) -> Result<OffsetRange, CoderError> {
        OffsetRange::decode_element(reader)
    }
}

impl DefaultCoder for OffsetRange {
    type Coder = OffsetRangeCoder;

    fn coder() -> Self::Coder {
        OffsetRangeCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        self.start.encode_element(writer)?;
        self.end.encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        let start = i64::decode_element(reader)?;
        let end = i64::decode_element(reader)?;
        Ok(OffsetRange::new(start, end))
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <(i64, i64)>::register_coder(registry)
    }
}

/// Cold-path state of an [`OffsetRangeTracker`], guarded by the split lock.
#[derive(Debug, Default)]
struct SplitAndErrorState {
    error: Option<RestrictionError>,
}

#[derive(Debug)]
struct OffsetRangeTrackerInner {
    start: i64,
    end: AtomicI64,
    last_attempted: AtomicI64,
    last_claimed: AtomicI64,
    stopped: AtomicBool,
    split_lock: Mutex<SplitAndErrorState>,
}

/// Thread-safe tracker for an [`OffsetRange`] restriction.
///
/// [`try_claim`](RestrictionTracker::try_claim) from element processing and
/// [`try_split`](RestrictionTracker::try_split) from the runner can run concurrently. A claim
/// takes the split lock after its lock-free checks, so a claim and a split never interleave.
#[derive(Clone, Debug)]
pub struct OffsetRangeTracker {
    inner: Arc<OffsetRangeTrackerInner>,
}

impl OffsetRangeTracker {
    pub fn new(range: OffsetRange) -> Self {
        Self {
            inner: Arc::new(OffsetRangeTrackerInner {
                start: range.start,
                end: AtomicI64::new(range.end),
                last_attempted: AtomicI64::new(i64::MIN),
                last_claimed: AtomicI64::new(i64::MIN),
                stopped: AtomicBool::new(false),
                split_lock: Mutex::new(SplitAndErrorState::default()),
            }),
        }
    }

    pub fn last_claimed(&self) -> Option<i64> {
        let val = self.inner.last_claimed.load(Ordering::Acquire);
        (val != i64::MIN).then_some(val)
    }

    pub fn last_attempted(&self) -> Option<i64> {
        let val = self.inner.last_attempted.load(Ordering::Acquire);
        (val != i64::MIN).then_some(val)
    }

    /// Returns the tracking error that stopped processing, if any.
    pub fn error(&self) -> Option<RestrictionError> {
        self.inner
            .split_lock
            .lock()
            .ok()
            .and_then(|s| s.error.clone())
    }

    // Inherent copies of the trait methods, so callers do not need to import the trait.
    pub fn try_claim(&self, position: &i64) -> bool {
        RestrictionTracker::try_claim(self, position)
    }

    pub fn try_split(&self, fraction_of_remainder: f64) -> Option<(OffsetRange, OffsetRange)> {
        RestrictionTracker::try_split(self, fraction_of_remainder)
    }

    pub fn current_restriction(&self) -> OffsetRange {
        RestrictionTracker::current_restriction(self)
    }

    pub fn current_progress(&self) -> RestrictionProgress {
        RestrictionTracker::current_progress(self)
    }

    pub fn check_done(&self) -> Result<(), RestrictionError> {
        RestrictionTracker::check_done(self)
    }

    fn fail_with_error(&self, err: RestrictionError) {
        self.inner.stopped.store(true, Ordering::Release);
        if let Ok(mut lock) = self.inner.split_lock.lock()
            && lock.error.is_none()
        {
            lock.error = Some(err);
        }
    }
}

impl RestrictionTracker for OffsetRangeTracker {
    type Position = i64;
    type Restriction = OffsetRange;

    fn try_claim(&self, position: &Self::Position) -> bool {
        let pos = *position;

        if self.inner.stopped.load(Ordering::Relaxed) {
            return false;
        }

        if pos < self.inner.start {
            self.fail_with_error(RestrictionError::OutOfBounds(format!(
                "{pos} < start {}",
                self.inner.start
            )));
            return false;
        }

        let Ok(_guard) = self.inner.split_lock.lock() else {
            return false;
        };

        let prev = self.inner.last_attempted.load(Ordering::Relaxed);
        if prev != i64::MIN && pos <= prev {
            drop(_guard);
            self.fail_with_error(RestrictionError::NonMonotonicClaim(
                pos.to_string(),
                prev.to_string(),
            ));
            return false;
        }
        self.inner.last_attempted.store(pos, Ordering::Release);

        let end = self.inner.end.load(Ordering::Acquire);
        if pos < end {
            self.inner.last_claimed.store(pos, Ordering::Release);
            true
        } else {
            self.inner.stopped.store(true, Ordering::Release);
            false
        }
    }

    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Option<(Self::Restriction, Self::Restriction)> {
        let _guard = self.inner.split_lock.lock().ok()?;
        if self.inner.stopped.load(Ordering::Acquire) {
            return None;
        }

        let end = self.inner.end.load(Ordering::Acquire);
        if self.inner.start >= end {
            return None;
        }

        let last_att = self.inner.last_attempted.load(Ordering::Acquire);
        let cur = if last_att == i64::MIN {
            self.inner.start - 1
        } else {
            last_att
        };

        if cur >= end - 1 {
            return None;
        }

        let fraction = fraction_of_remainder.clamp(0.0, 1.0);
        let split_pos = if fraction <= 0.0 {
            cur.saturating_add(1)
        } else {
            let remaining = (end as f64) - (cur as f64);
            let delta = (fraction * remaining).ceil();
            if delta >= (i64::MAX as f64) {
                end
            } else {
                cur.saturating_add((delta as i64).max(1))
            }
        };

        if split_pos < end {
            let latest_att = self.inner.last_attempted.load(Ordering::Acquire);
            if latest_att != i64::MIN && split_pos <= latest_att {
                return None;
            }
            self.inner.end.store(split_pos, Ordering::Release);
            let primary = OffsetRange::new(self.inner.start, split_pos);
            let residual = OffsetRange::new(split_pos, end);
            Some((primary, residual))
        } else {
            None
        }
    }

    fn current_restriction(&self) -> Self::Restriction {
        OffsetRange::new(self.inner.start, self.inner.end.load(Ordering::Acquire))
    }

    fn current_progress(&self) -> RestrictionProgress {
        let end = self.inner.end.load(Ordering::Acquire);
        let last_att = self.inner.last_attempted.load(Ordering::Acquire);
        if last_att == i64::MIN {
            RestrictionProgress::new(0.0, (end - self.inner.start).max(0) as f64)
        } else {
            let done = (last_att - self.inner.start + 1).max(0) as f64;
            let remaining = (end - last_att - 1).max(0) as f64;
            RestrictionProgress::new(done, remaining)
        }
    }

    fn check_done(&self) -> Result<(), RestrictionError> {
        let guard = self
            .inner
            .split_lock
            .lock()
            .map_err(|_| RestrictionError::Custom("Lock poison".to_string()))?;

        if let Some(ref err) = guard.error {
            return Err(err.clone());
        }

        let end = self.inner.end.load(Ordering::Acquire);
        if self.inner.start >= end {
            return Ok(());
        }

        let last = self.inner.last_attempted.load(Ordering::Acquire);
        if last == i64::MIN {
            return Err(RestrictionError::IncompleteWork(format!(
                "No work was claimed in non-empty range {:?}",
                OffsetRange::new(self.inner.start, end)
            )));
        }

        if last < end - 1 {
            Err(RestrictionError::IncompleteWork(format!(
                "Last attempted offset was {} in range {:?}, work in [{}, {}) was not claimed",
                last,
                OffsetRange::new(self.inner.start, end),
                last + 1,
                end
            )))
        } else {
            Ok(())
        }
    }

    fn is_bounded(&self) -> bool {
        true
    }
}
