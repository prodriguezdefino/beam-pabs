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
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionError, RestrictionProgress,
    RestrictionTracker, SplittableDoFn, SplittableParDo, WatermarkedTracker,
};
use crate::values::IsBounded;
use crate::values::{PBegin, PCollection};
use crate::windowing::{ManualWatermarkEstimator, WatermarkEstimator};

/// Default size of the initial splits of a bounded range.
pub const DEFAULT_SEQUENCE_SPLIT_SIZE: i64 = 10_000;

/// Maximum initial splits of an unbounded sequence.
const MAX_UNBOUNDED_SPLITS: i64 = 20;

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

/// Offset tracker that keeps unbounded splits checkpoint-only and unit-sized.
struct SequenceOffsetTracker {
    inner: OffsetRangeTracker,
    unbounded: bool,
}

impl RestrictionTracker for SequenceOffsetTracker {
    type Position = i64;
    type Restriction = OffsetRange;

    fn try_claim(&self, position: &Self::Position) -> bool {
        self.inner.try_claim(position)
    }

    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Option<(Self::Restriction, Self::Restriction)> {
        if self.unbounded && fraction_of_remainder > 0.0 {
            return None;
        }
        self.inner.try_split(fraction_of_remainder)
    }

    fn current_restriction(&self) -> Self::Restriction {
        self.inner.current_restriction()
    }

    fn current_progress(&self) -> RestrictionProgress {
        let progress = self.inner.current_progress();
        if self.unbounded {
            RestrictionProgress::new(progress.work_completed, 1.0)
        } else {
            progress
        }
    }

    fn check_done(&self) -> Result<(), RestrictionError> {
        self.inner.check_done()
    }
}

/// The tracker of [`GenerateSequenceFn`]. A checkpoint keeps the watermark with the residual.
type SequenceTracker = WatermarkedTracker<SequenceOffsetTracker, ManualWatermarkEstimator>;

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
    fn unbounded_splits(&self) -> i64 {
        if self.end.is_some() {
            return 1;
        }
        let Some((elements, period)) = self.rate else {
            return MAX_UNBOUNDED_SPLITS;
        };
        let secs = period.as_secs_f64();
        if secs <= 0.0 {
            return MAX_UNBOUNDED_SPLITS;
        }
        let rate_per_sec = ((elements.max(1) as f64) / secs).ceil() as i64;
        rate_per_sec.clamp(1, MAX_UNBOUNDED_SPLITS)
    }

    /// Returns `(splits, segment_len)` when this unbounded sequence partitions its initial range.
    fn unbounded_striding(&self) -> Option<(i64, i128)> {
        let splits = self.unbounded_splits();
        if splits <= 1 {
            return None;
        }
        let span = i128::from(i64::MAX - 1) - i128::from(self.start);
        let seg_len = span / i128::from(splits);
        (seg_len > 0).then_some((splits, seg_len))
    }

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

    fn check_delay(
        target_time_millis: i64,
        now_millis: i64,
        slept_millis: &mut i64,
        emitted: bool,
    ) -> Option<ProcessContinuation> {
        const MAX_FIRST_ELEMENT_SLEEP_MILLIS: i64 = 2_000;
        const MAX_BUNDLE_SLEEP_MILLIS: i64 = 50;
        if target_time_millis <= now_millis {
            return None;
        }
        let delay_millis = target_time_millis - now_millis;
        let can_sleep = if emitted {
            *slept_millis + delay_millis <= MAX_BUNDLE_SLEEP_MILLIS
        } else {
            delay_millis <= MAX_FIRST_ELEMENT_SLEEP_MILLIS
        };
        let delay = Duration::from_millis(u64::try_from(delay_millis).unwrap_or(0));
        if can_sleep {
            std::thread::sleep(delay);
            *slept_millis += delay_millis;
            None
        } else {
            Some(ProcessContinuation::ResumeAfter(delay))
        }
    }

    fn process_rate_limited<I>(
        &self,
        candidates: I,
        stride: i64,
        elements: i64,
        period: Duration,
        tracker: &<Self as SplittableDoFn>::Tracker,
        ctx: &mut ProcessContext<'_, <Self as SplittableDoFn>::Out>,
    ) -> crate::Result<ProcessContinuation>
    where
        I: Iterator<Item = (i64, i64)>,
    {
        let interval_millis = period.as_millis() as f64 / (elements as f64);
        let batch_size = self.split_size.max(1) as usize;
        let mut count = 0usize;
        let mut slept_millis = 0i64;

        for (pos, seq) in candidates {
            // Start the clock first so `now` is never earlier than the start time.
            self.get_or_init_start_time();
            let now_millis = Self::current_time_millis();
            if let Some(cont) = self.check_expiration(now_millis) {
                return Ok(cont);
            }

            let target_time = self.target_time_millis(seq, interval_millis);
            if let Some(cont) =
                Self::check_delay(target_time, now_millis, &mut slept_millis, count > 0)
            {
                advance_watermark(tracker, target_time);
                return Ok(cont);
            }

            if !tracker.try_claim(&pos) {
                return Ok(ProcessContinuation::Stop);
            }

            // A worker that restarts the clock computes earlier target times. The watermark
            // of the restored checkpoint is a lower bound for them.
            let timestamp = target_time.max(tracker.estimator.current_watermark().as_millis());
            ctx.output(seq)
                .at(crate::Instant::from_millis(timestamp))
                .emit()?;
            let next_target = self.target_time_millis(seq.saturating_add(stride), interval_millis);
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
        I: Iterator<Item = (i64, i64)>,
    {
        let limit = batch_limit.unwrap_or(usize::MAX);
        // Only an unbounded sequence has a batch limit.
        let unbounded = batch_limit.is_some();
        let mut emitted = 0usize;
        for (pos, seq) in candidates.take(limit) {
            // Start the clock first so `now` is never earlier than the start time.
            self.get_or_init_start_time();
            let now_millis = Self::current_time_millis();
            if let Some(cont) = self.check_expiration(now_millis) {
                return Ok(cont);
            }
            if !tracker.try_claim(&pos) {
                return Ok(ProcessContinuation::Stop);
            }
            if unbounded {
                // The clock can move back, so the timestamp is at least the watermark.
                let timestamp = now_millis.max(tracker.estimator.current_watermark().as_millis());
                ctx.output(seq)
                    .at(crate::Instant::from_millis(timestamp))
                    .emit()?;
                tracker
                    .estimator
                    .set_watermark(crate::Instant::from_millis(timestamp));
            } else {
                ctx.emit(seq)?;
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
        if self.end.is_some() {
            return restriction.sized_splits(self.split_size);
        }
        if let Some((splits, seg_len)) = self.unbounded_striding()
            && restriction.start == self.start
            && restriction.end == i64::MAX
        {
            return (0..splits)
                .filter_map(|k| {
                    let start =
                        i64::try_from(i128::from(self.start) + i128::from(k) * seg_len).ok()?;
                    let end =
                        i64::try_from(i128::from(self.start) + i128::from(k + 1) * seg_len).ok()?;
                    (start < end).then_some(OffsetRange::new(start, end))
                })
                .collect();
        }
        vec![*restriction]
    }

    fn restriction_size(&self, _element: &Self::In, restriction: &Self::Restriction) -> f64 {
        match self.end {
            Some(_) => restriction.size(),
            None => 1.0,
        }
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        let tracker = SequenceOffsetTracker {
            inner: OffsetRangeTracker::new(*restriction),
            unbounded: self.end.is_none(),
        };
        let estimator = ManualWatermarkEstimator::default();
        WatermarkedTracker::new(tracker, estimator).with_boundedness(self.boundedness())
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result<ProcessContinuation> {
        let range = tracker.current_restriction();
        let striding = self.unbounded_striding().filter(|_| range.end < i64::MAX);
        let stride = striding.map_or(1, |(splits, _)| splits);
        let start = self.start;
        let candidates = std::iter::successors(Some(range.start), |&pos| pos.checked_add(1))
            .take_while(move |&pos| pos < range.end)
            .map(move |pos| {
                let Some((splits, seg_len)) = striding else {
                    return (pos, pos);
                };
                let rel = i128::from(pos) - i128::from(start);
                let split_idx = rel / seg_len;
                let step = rel % seg_len;
                let seq = i64::try_from(i128::from(start) + step * i128::from(splits) + split_idx)
                    .unwrap_or(i64::MAX);
                (pos, seq)
            });

        match self.rate {
            Some((elements, period)) => {
                self.process_rate_limited(candidates, stride, elements, period, tracker, ctx)
            }
            None => {
                let batch_limit = self
                    .end
                    .is_none()
                    .then_some(self.split_size.max(1) as usize);
                self.process_unrated(candidates, batch_limit, tracker, ctx)
            }
        }
    }
}
