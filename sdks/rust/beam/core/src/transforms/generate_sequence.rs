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

//! Root transform that generates a bounded (`[start, end)`) or unbounded (`[start, ∞)`) integer
//! sequence with a splittable DoFn.
//!
//! A rate-limited sequence stamps each element with its target time. An unbounded sequence
//! without a rate stamps each element with the current time. In both cases the watermark
//! follows the timestamps, so no element is behind it. A bounded sequence keeps the
//! timestamp of the impulse.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::transforms::PTransform;
use crate::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use crate::transforms::dofn::context::ProcessContext;
use crate::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    SplittableParDo, WatermarkedTracker,
};
use crate::values::IsBounded;
use crate::values::{PBegin, PCollection};
use crate::windowing::{ManualWatermarkEstimator, WatermarkEstimator};

/// Default size of the initial splits of a bounded range.
pub const DEFAULT_SEQUENCE_SPLIT_SIZE: i64 = 10_000;

/// Generates a [`PCollection<i64>`] sequence with a [`SplittableDoFn`].
///
/// The numbers are produced at execution time, not stored in the pipeline graph as with
/// [`Create`](crate::transforms::Create), so the graph stays small and the runner can rebalance
/// the work. Without [`with_end`](Self::with_end) the sequence is unbounded. Limit it with
/// [`with_rate`](Self::with_rate), [`with_period`](Self::with_period) and
/// [`with_max_read_time`](Self::with_max_read_time).
#[derive(Clone, Debug)]
pub struct GenerateSequence {
    start: i64,
    end: Option<i64>,
    split_size: i64,
    rate: Option<(i64, Duration)>,
    max_read_time: Option<Duration>,
}

impl GenerateSequence {
    /// Creates a sequence that produces `[start, ∞)` until
    /// [`with_end`](Self::with_end) sets an upper bound.
    pub fn new(start: i64) -> Self {
        Self {
            start,
            end: None,
            split_size: DEFAULT_SEQUENCE_SPLIT_SIZE,
            rate: None,
            max_read_time: None,
        }
    }

    /// Sets the exclusive upper bound `end`. The sequence becomes bounded.
    pub fn with_end(mut self, end: i64) -> Self {
        self.end = Some(end);
        self
    }

    /// Sets the maximum size of the initial splits of a bounded range, and of the elements in one
    /// call of an unbounded or rate-limited sequence. Values below 1 become 1.
    pub fn with_split_size(mut self, split_size: i64) -> Self {
        self.split_size = split_size.max(1);
        self
    }

    /// Limits the rate to `elements` per `period`. Values of `elements` below 1 become 1.
    pub fn with_rate(mut self, elements: i64, period: Duration) -> Self {
        self.rate = Some((elements.max(1), period));
        self
    }

    /// Emits 1 element per `period`. This is the same as `.with_rate(1, period)`.
    pub fn with_period(self, period: Duration) -> Self {
        self.with_rate(1, period)
    }

    /// Stops the sequence when `max_read_time` has passed since the first processed element.
    pub fn with_max_read_time(mut self, max_read_time: Duration) -> Self {
        self.max_read_time = Some(max_read_time);
        self
    }

    /// Returns the start of the sequence.
    pub fn start(&self) -> i64 {
        self.start
    }

    /// Returns the exclusive end of the sequence, or `None` if unbounded.
    pub fn end(&self) -> Option<i64> {
        self.end
    }

    /// Returns `true` if this sequence is unbounded.
    pub fn is_unbounded(&self) -> bool {
        self.end.is_none()
    }

    /// Returns the configured rate limit, if any.
    pub fn rate(&self) -> Option<(i64, Duration)> {
        self.rate
    }

    /// Returns the maximum read time, if configured.
    pub fn max_read_time(&self) -> Option<Duration> {
        self.max_read_time
    }
}

impl HasDisplayData for GenerateSequence {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "GenerateSequence");
        builder.add_integer("start", self.start);
        if let Some(end) = self.end {
            builder.add_integer("end", end);
        } else {
            builder.add_text("end", "unbounded");
        }
        builder.add_integer("split_size", self.split_size);
        if let Some((elements, period)) = self.rate {
            builder.add_integer("rate_elements", elements);
            builder.add_integer("rate_period_ms", period.as_millis() as i64);
        }
        if let Some(max_time) = self.max_read_time {
            builder.add_integer("max_read_time_ms", max_time.as_millis() as i64);
        }
    }
}

impl PTransform<PBegin> for GenerateSequence {
    type Output = PCollection<i64>;

    fn expand(self, input: &PBegin) -> PCollection<i64> {
        let pipeline = input.pipeline();
        let name = pipeline.scope_name().unwrap_or_default();

        let sdf = GenerateSequenceFn {
            start: self.start,
            end: self.end,
            split_size: self.split_size,
            rate: self.rate,
            max_read_time: self.max_read_time,
            start_time_millis: Arc::new(AtomicI64::new(-1)),
        };

        let pcoll = {
            let impulse = pipeline.apply("Impulse", crate::transforms::Impulse::new());
            impulse.apply("Generate", SplittableParDo::new(sdf))
        };

        let mut builder = DisplayDataBuilder::with_namespace(name);
        self.populate_display_data(&mut builder);
        pipeline.set_scope_display_data(builder.into_proto());

        pcoll
    }
}

/// The tracker of [`GenerateSequenceFn`]. A checkpoint keeps the watermark with the residual.
type SequenceTracker = WatermarkedTracker<OffsetRangeTracker, ManualWatermarkEstimator>;

/// Moves the watermark of `tracker` forward to `millis`. The watermark never moves back.
fn advance_watermark(tracker: &SequenceTracker, millis: i64) {
    if millis > tracker.estimator.current_watermark().as_millis() {
        tracker
            .estimator
            .set_watermark(crate::Instant::from_millis(millis));
    }
}

/// Splittable DoFn that generates the values of an `OffsetRange`.
#[derive(Clone, Debug)]
struct GenerateSequenceFn {
    start: i64,
    end: Option<i64>,
    split_size: i64,
    rate: Option<(i64, Duration)>,
    max_read_time: Option<Duration>,
    start_time_millis: Arc<AtomicI64>,
}

impl GenerateSequenceFn {
    fn current_time_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn get_or_init_start_time(&self) -> i64 {
        let current = self.start_time_millis.load(Ordering::Relaxed);
        if current >= 0 {
            current
        } else {
            let now = Self::current_time_millis();
            match self.start_time_millis.compare_exchange(
                -1,
                now,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => now,
                Err(actual) => actual,
            }
        }
    }

    fn target_time_millis(&self, cur: i64, interval_millis: f64) -> i64 {
        let cur_offset = cur.saturating_sub(self.start).max(0);
        self.get_or_init_start_time() + (cur_offset as f64 * interval_millis) as i64
    }

    fn check_expiration(&self, now_millis: i64) -> Option<ProcessContinuation> {
        let start_time = self.get_or_init_start_time();
        self.max_read_time
            .map(|max_time| start_time + max_time.as_millis() as i64)
            .filter(|&deadline| now_millis >= deadline)
            .map(|_| ProcessContinuation::Stop)
    }

    fn check_delay(target_time_millis: i64, now_millis: i64) -> Option<ProcessContinuation> {
        const MAX_IN_BUNDLE_SLEEP_MILLIS: i64 = 2000;
        if target_time_millis > now_millis {
            let delay_millis = target_time_millis - now_millis;
            if delay_millis <= MAX_IN_BUNDLE_SLEEP_MILLIS {
                std::thread::sleep(Duration::from_millis(delay_millis as u64));
                None
            } else {
                Some(ProcessContinuation::ResumeAfter(Duration::from_millis(
                    delay_millis as u64,
                )))
            }
        } else {
            None
        }
    }

    fn process_rate_limited<I>(
        &self,
        candidates: I,
        elements: i64,
        period: Duration,
        tracker: &<Self as SplittableDoFn>::Tracker,
        ctx: &mut ProcessContext<'_, <Self as SplittableDoFn>::Out>,
    ) -> crate::Result<ProcessContinuation>
    where
        I: Iterator<Item = i64>,
    {
        let interval_millis = period.as_millis() as f64 / (elements as f64);
        let batch_size = self.split_size.max(1) as usize;
        let mut count = 0usize;

        for cur in candidates {
            // Start the clock first so `now` is never earlier than the start time.
            self.get_or_init_start_time();
            let now_millis = Self::current_time_millis();
            if let Some(cont) = self.check_expiration(now_millis) {
                return Ok(cont);
            }

            let target_time = self.target_time_millis(cur, interval_millis);
            if let Some(cont) = Self::check_delay(target_time, now_millis) {
                advance_watermark(tracker, target_time);
                return Ok(cont);
            }

            if !tracker.try_claim(&cur) {
                return Ok(ProcessContinuation::Stop);
            }

            // A worker that restarts the clock computes earlier target times. The watermark
            // of the restored checkpoint is a lower bound for them.
            let timestamp = target_time.max(tracker.estimator.current_watermark().as_millis());
            ctx.output(cur)
                .at(crate::Instant::from_millis(timestamp))
                .emit()?;
            let next_target = self.target_time_millis(cur.saturating_add(1), interval_millis);
            advance_watermark(tracker, next_target);
            count += 1;

            if count >= batch_size {
                return Ok(ProcessContinuation::Resume);
            }
        }

        Ok(ProcessContinuation::Stop)
    }

    fn process_unrated<I>(
        &self,
        candidates: I,
        batch_limit: Option<usize>,
        tracker: &<Self as SplittableDoFn>::Tracker,
        ctx: &mut ProcessContext<'_, <Self as SplittableDoFn>::Out>,
    ) -> crate::Result<ProcessContinuation>
    where
        I: Iterator<Item = i64>,
    {
        let limit = batch_limit.unwrap_or(usize::MAX);
        // Only an unbounded sequence has a batch limit.
        let unbounded = batch_limit.is_some();
        let mut emitted = 0usize;
        for cur in candidates.take(limit) {
            // Start the clock first so `now` is never earlier than the start time.
            self.get_or_init_start_time();
            let now_millis = Self::current_time_millis();
            if let Some(cont) = self.check_expiration(now_millis) {
                return Ok(cont);
            }
            if !tracker.try_claim(&cur) {
                return Ok(ProcessContinuation::Stop);
            }
            if unbounded {
                // The clock can move back, so the timestamp is at least the watermark.
                let timestamp = now_millis.max(tracker.estimator.current_watermark().as_millis());
                ctx.output(cur)
                    .at(crate::Instant::from_millis(timestamp))
                    .emit()?;
                tracker
                    .estimator
                    .set_watermark(crate::Instant::from_millis(timestamp));
            } else {
                ctx.emit(cur)?;
            }
            emitted += 1;
        }
        if batch_limit == Some(emitted) {
            Ok(ProcessContinuation::Resume)
        } else {
            Ok(ProcessContinuation::Stop)
        }
    }
}

impl SplittableDoFn for GenerateSequenceFn {
    type In = Vec<u8>;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = SequenceTracker;

    fn boundedness(&self) -> IsBounded {
        match self.end {
            Some(_) => IsBounded::Bounded,
            None => IsBounded::Unbounded,
        }
    }

    fn initial_restriction(&self, _element: &Self::In) -> crate::Result<Self::Restriction> {
        Ok(OffsetRange::new(self.start, self.end.unwrap_or(i64::MAX)))
    }

    fn split_restriction(
        &self,
        _element: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        match self.end {
            Some(_) => restriction.sized_splits(self.split_size),
            None => vec![*restriction],
        }
    }

    fn restriction_size(&self, _element: &Self::In, restriction: &Self::Restriction) -> f64 {
        match self.end {
            Some(_) => restriction.size(),
            None => 1.0,
        }
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        let tracker = OffsetRangeTracker::new(*restriction);
        let estimator = ManualWatermarkEstimator::default();
        WatermarkedTracker::new(tracker, estimator).with_boundedness(self.boundedness())
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result<ProcessContinuation> {
        let candidates = || {
            std::iter::successors(Some(tracker.current_restriction().start), |&c| {
                c.checked_add(1)
            })
        };

        self.rate
            .map(|(elements, period)| {
                self.process_rate_limited(candidates(), elements, period, tracker, ctx)
            })
            .unwrap_or_else(|| {
                let batch_limit = self
                    .end
                    .is_none()
                    .then_some(self.split_size.max(1) as usize);
                self.process_unrated(candidates(), batch_limit, tracker, ctx)
            })
    }
}
