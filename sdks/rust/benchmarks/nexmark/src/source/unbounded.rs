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

//! Unbounded streaming [`Event`] source with watermark tracking, rate pacing, and late events.

use std::time::Duration;

use beam::prelude::*;
use beam::transforms::Impulse;
use beam::transforms::sdf::{
    ProcessContinuation, RestrictionTracker, SplittableDoFn, SplittableParDo, WatermarkedTracker,
};
use beam::windowing::{ManualWatermarkEstimator, WatermarkEstimator};
use serde::{Deserialize, Serialize};

use super::generator_segments;
use super::restriction::{GeneratorRestriction, GeneratorRestrictionTracker};
use crate::config::NexmarkConfig;
use crate::generator::{
    Generator, GeneratorConfig, JavaRandom, NextEvent, current_wallclock_ms, next_long,
};
use crate::model::Event;

const DEFAULT_BATCH_SIZE: usize = 128;

fn delay_seed(event_id: i64) -> i64 {
    let mut z = (event_id as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (z ^ (z >> 31)) as i64
}

/// Unbounded root transform that synthesizes streaming [`Event`] records with watermarks.
#[derive(Clone, Copy, Debug)]
pub struct UnboundedEventSource {
    root: GeneratorConfig,
    num_generators: usize,
    watermark_holdback_ms: i64,
    is_rate_limited: bool,
    batch_size: usize,
}

impl UnboundedEventSource {
    /// Builds a streaming event generator from benchmark settings.
    pub fn new(config: &NexmarkConfig) -> Self {
        Self {
            root: GeneratorConfig::from_nexmark_config(config),
            num_generators: config.num_event_generators.max(1) as usize,
            watermark_holdback_ms: config.watermark_holdback_sec.max(0).saturating_mul(1_000),
            is_rate_limited: config.is_rate_limited,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Sets the maximum number of events emitted per bundle before self-checkpointing.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }
}

impl PTransform<PBegin> for UnboundedEventSource {
    type Output = PCollection<Event>;

    fn expand(&self, input: &PBegin) -> PCollection<Event> {
        let sdf = UnboundedGeneratorFn {
            root: self.root,
            num_generators: self.num_generators,
            watermark_holdback_ms: self.watermark_holdback_ms,
            is_rate_limited: self.is_rate_limited,
            batch_size: self.batch_size,
        };
        input
            .pipeline()
            .apply("Impulse", Impulse::new())
            .apply("GenerateUnboundedEvents", SplittableParDo::new(sdf))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Register)]
struct UnboundedGeneratorFn {
    root: GeneratorConfig,
    num_generators: usize,
    watermark_holdback_ms: i64,
    is_rate_limited: bool,
    batch_size: usize,
}

type UnboundedTracker = WatermarkedTracker<GeneratorRestrictionTracker, ManualWatermarkEstimator>;

impl UnboundedGeneratorFn {
    fn event_at(&self, pos: i64, wallclock_base_ms: i64) -> Option<(i64, NextEvent)> {
        generator_segments(&self.root, self.num_generators)
            .find(|&(seg_start, seg_end, _, _)| pos >= seg_start && pos < seg_end)
            .and_then(|(seg_start, _, first_id, sub_max)| {
                let local_offset = pos - seg_start;
                let seg_cfg = self
                    .root
                    .copy_with(first_id, sub_max, self.root.first_event_number);
                let mut generator = Generator::with_state(seg_cfg, local_offset, wallclock_base_ms);
                let event_id = generator.next_event_id();
                generator.next().map(|next| (event_id, next))
            })
    }

    fn sample_delay_ms(&self, event_id: i64) -> Option<i64> {
        let cfg = &self.root.configuration;
        if cfg.prob_delayed_event <= 0.0 || cfg.occasional_delay_sec <= 0 {
            return None;
        }
        let mut rng = JavaRandom::new(delay_seed(event_id));
        (rng.next_double() < cfg.prob_delayed_event)
            .then(|| next_long(&mut rng, cfg.occasional_delay_sec.saturating_mul(1_000)) + 1)
    }

    fn advance_watermark(&self, tracker: &UnboundedTracker, raw_watermark_ms: i64) {
        let candidate =
            Instant::from_millis(raw_watermark_ms.saturating_sub(self.watermark_holdback_ms));
        if candidate > tracker.estimator.current_watermark() {
            tracker.estimator.set_watermark(candidate);
        }
    }
}

fn wait_duration(due_ms: i64, now_ms: i64) -> Duration {
    let delta = u64::try_from(due_ms.saturating_sub(now_ms))
        .unwrap_or(1)
        .max(1);
    Duration::from_millis(delta)
}

impl SplittableDoFn for UnboundedGeneratorFn {
    type In = Vec<u8>;
    type Out = Event;
    type Restriction = GeneratorRestriction;
    type Tracker = UnboundedTracker;

    fn boundedness(&self) -> IsBounded {
        IsBounded::Unbounded
    }

    fn initial_restriction(&self, _element: &Self::In) -> Result<Self::Restriction> {
        Ok(GeneratorRestriction::new(0, self.root.max_events.max(0)))
    }

    fn split_restriction(
        &self,
        _element: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        // Clone transfers any buffered delayed events into the first non-empty split.
        let mut carry_delayed = Some(restriction.held_back.clone());
        let splits: Vec<GeneratorRestriction> = generator_segments(&self.root, self.num_generators)
            .filter_map(|(seg_start, seg_end, _, _)| {
                let start = restriction.start.max(seg_start);
                let end = restriction.end.min(seg_end);
                (start < end).then(|| {
                    let delayed = carry_delayed.take().unwrap_or_default();
                    GeneratorRestriction::with_state(
                        start,
                        end,
                        restriction.wallclock_base_ms,
                        delayed,
                    )
                })
            })
            .collect();
        if splits.is_empty() {
            // SplittableDoFn::split_restriction takes &Restriction and returns owned splits.
            vec![restriction.clone()]
        } else {
            splits
        }
    }

    fn restriction_size(&self, _element: &Self::In, restriction: &Self::Restriction) -> f64 {
        let count = restriction.end.saturating_sub(restriction.start).max(0)
            + (restriction.held_back.len() as i64);
        self.root.estimated_bytes_for_events(count).max(1) as f64
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        WatermarkedTracker::new(
            // SplittableDoFn::create_tracker borrows &Restriction while the tracker mutates its queue.
            GeneratorRestrictionTracker::new(restriction.clone()),
            ManualWatermarkEstimator::default(),
        )
        .with_boundedness(IsBounded::Unbounded)
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result<ProcessContinuation> {
        if !tracker.tracker.has_remaining_work() {
            tracker.estimator.set_watermark(Instant::MAX);
            return Ok(ProcessContinuation::Stop);
        }

        let wallclock_base_ms = tracker.tracker.wallclock_base_ms(current_wallclock_ms);
        let mut emitted = 0_usize;

        while emitted < self.batch_size {
            let (pos, end, offset_stopped) = tracker.tracker.offset_bounds();
            let candidate = (!offset_stopped && pos < end)
                .then(|| self.event_at(pos, wallclock_base_ms))
                .flatten();
            let now_ms = if self.is_rate_limited {
                current_wallclock_ms()
            } else {
                candidate
                    .as_ref()
                    .map_or(i64::MAX, |(_, ev)| ev.wallclock_timestamp)
            };

            if let Some(delayed) = tracker.tracker.pop_due_delayed(now_ms) {
                self.advance_watermark(tracker, delayed.watermark);
                ctx.output(delayed.event)
                    .at(Instant::from_millis(delayed.event_timestamp))
                    .emit()?;
                emitted += 1;
                continue;
            }

            if let Some((event_id, next)) = candidate {
                if self.is_rate_limited && next.wallclock_timestamp > now_ms {
                    let due_ms = tracker
                        .tracker
                        .peek_delayed_wallclock()
                        .map_or(next.wallclock_timestamp, |d| {
                            d.min(next.wallclock_timestamp)
                        });
                    return Ok(ProcessContinuation::ResumeAfter(wait_duration(
                        due_ms, now_ms,
                    )));
                }
                if !tracker.try_claim(&pos) {
                    break;
                }
                if let Some(delay_ms) = self.sample_delay_ms(event_id) {
                    tracker.tracker.defer_delayed(next.with_delay(delay_ms));
                    continue;
                }
                self.advance_watermark(tracker, next.watermark);
                ctx.output(next.event)
                    .at(Instant::from_millis(next.event_timestamp))
                    .emit()?;
                emitted += 1;
            } else if let Some(due_ms) = tracker.tracker.peek_delayed_wallclock() {
                return Ok(ProcessContinuation::ResumeAfter(wait_duration(
                    due_ms, now_ms,
                )));
            } else {
                break;
            }
        }

        if tracker.tracker.has_remaining_work() {
            Ok(ProcessContinuation::Resume)
        } else if emitted > 0 {
            tracker.tracker.schedule_watermark_drain();
            Ok(ProcessContinuation::ResumeAfter(Duration::from_millis(50)))
        } else {
            tracker.estimator.set_watermark(Instant::MAX);
            Ok(ProcessContinuation::Stop)
        }
    }
}
