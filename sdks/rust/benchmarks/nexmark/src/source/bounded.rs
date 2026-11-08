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

//! Bounded splittable [`Event`] source backed by [`OffsetRange`].

use beam::prelude::*;
use beam::transforms::Impulse;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    SplittableParDo,
};
use serde::{Deserialize, Serialize};

use super::generator_segments;
use crate::config::NexmarkConfig;
use crate::generator::{Generator, GeneratorConfig};
use crate::model::Event;

/// Bounded root transform that synthesizes [`Event`] records across `num_event_generators` splits.
#[derive(Clone, Copy, Debug)]
pub struct BoundedEventSource {
    root: GeneratorConfig,
    num_generators: usize,
}

impl BoundedEventSource {
    /// Builds a bounded event source from `config`.
    pub fn new(config: &NexmarkConfig) -> Self {
        Self {
            root: GeneratorConfig::from_nexmark_config(config),
            num_generators: config.num_event_generators.max(1) as usize,
        }
    }
}

impl PTransform<PBegin> for BoundedEventSource {
    type Output = PCollection<Event>;

    fn expand(self, input: &PBegin) -> PCollection<Event> {
        let sdf = BoundedGeneratorFn {
            root: self.root,
            num_generators: self.num_generators,
        };
        input
            .pipeline()
            .apply("Impulse", Impulse::new())
            .apply("GenerateBoundedEvents", SplittableParDo::new(sdf))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Register)]
struct BoundedGeneratorFn {
    root: GeneratorConfig,
    num_generators: usize,
}

impl SplittableDoFn for BoundedGeneratorFn {
    type In = Vec<u8>;
    type Out = Event;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn boundedness(&self) -> IsBounded {
        IsBounded::Bounded
    }

    fn initial_restriction(&self, _element: &Self::In) -> Result<Self::Restriction> {
        Ok(OffsetRange::new(0, self.root.max_events.max(0)))
    }

    fn split_restriction(
        &self,
        _element: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        let splits: Vec<OffsetRange> = generator_segments(&self.root, self.num_generators)
            .filter_map(|(seg_start, seg_end, _, _)| {
                let start = restriction.start.max(seg_start);
                let end = restriction.end.min(seg_end);
                (start < end).then(|| OffsetRange::new(start, end))
            })
            .collect();
        if splits.is_empty() {
            vec![*restriction]
        } else {
            splits
        }
    }

    fn restriction_size(&self, _element: &Self::In, restriction: &Self::Restriction) -> f64 {
        let count = restriction.end.saturating_sub(restriction.start).max(0);
        self.root.estimated_bytes_for_events(count).max(1) as f64
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result<ProcessContinuation> {
        let range = tracker.current_restriction();
        if range.is_empty() {
            return Ok(ProcessContinuation::Stop);
        }

        for (seg_start, seg_end, first_id, sub_max) in
            generator_segments(&self.root, self.num_generators)
        {
            if range.start >= seg_end {
                continue;
            }
            let start_pos = range.start.max(seg_start);
            let local_offset = start_pos - seg_start;
            let slice = self.root.copy_with(
                first_id,
                sub_max - local_offset,
                self.root.first_event_number + local_offset,
            );
            for (pos, next) in (start_pos..seg_end).zip(Generator::new(slice)) {
                if !tracker.try_claim(&pos) {
                    return Ok(ProcessContinuation::Stop);
                }
                ctx.output(next.event)
                    .at(Instant::from_millis(next.event_timestamp))
                    .emit()?;
            }
        }

        Ok(ProcessContinuation::Stop)
    }
}
