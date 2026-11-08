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

//! Splittable restriction, binary coder, and tracker for [`UnboundedEventSource`](super::UnboundedEventSource).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use beam::coders::{
    Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_BYTES, VarIntCoder,
};
use beam::transforms::sdf::{RestrictionError, RestrictionProgress, RestrictionTracker};
use serde::{Deserialize, Serialize};

use crate::generator::NextEvent;
use crate::model::{EventCoder, KnownSize, decode_framed, encode_framed};

fn held_back_key(event: &NextEvent) -> (i64, i64) {
    (event.wallclock_timestamp, event.event_timestamp)
}

fn offset_bytes(from: i64, to: i64, avg_event_bytes: f64) -> f64 {
    (i128::from(to) - i128::from(from)).max(0) as f64 * avg_event_bytes
}

fn held_back_bytes<'a>(events: impl IntoIterator<Item = &'a NextEvent>) -> f64 {
    events
        .into_iter()
        .map(|next| next.event.estimated_size_bytes() as f64)
        .sum()
}

/// Half-open generator offset range `[start, end)` paired with wallclock anchor and delayed events.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GeneratorRestriction {
    pub start: i64,
    pub end: i64,
    /// Wallclock anchor in milliseconds since the Unix epoch, or `-1` before initialization.
    pub wallclock_base_ms: i64,
    /// Delayed events ordered by `(wallclock_timestamp, event_timestamp)`.
    pub held_back: Vec<NextEvent>,
}

impl GeneratorRestriction {
    /// Builds an unstarted range over a half-open offset interval with no buffered events.
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            start,
            end,
            wallclock_base_ms: -1,
            held_back: Vec::new(),
        }
    }

    /// Builds a range with an initialized wallclock anchor and due-sorted event queue.
    pub fn with_state(
        start: i64,
        end: i64,
        wallclock_base_ms: i64,
        mut held_back: Vec<NextEvent>,
    ) -> Self {
        held_back.sort_by_key(held_back_key);
        Self {
            start,
            end,
            wallclock_base_ms,
            held_back,
        }
    }

    /// Reports true when both the offset interval and buffered queue have no items.
    pub fn is_empty(&self) -> bool {
        self.start >= self.end && self.held_back.is_empty()
    }

    /// Estimated bytes of the work left: each offset at `avg_event_bytes`, plus the known
    /// size of each held-back event.
    pub fn estimated_bytes(&self, avg_event_bytes: f64) -> f64 {
        offset_bytes(self.start, self.end, avg_event_bytes) + held_back_bytes(&self.held_back)
    }

    fn encode_raw(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        VarIntCoder::encode_varint(self.start, writer)?;
        VarIntCoder::encode_varint(self.end, writer)?;
        VarIntCoder::encode_varint(self.wallclock_base_ms, writer)?;
        let count = i64::try_from(self.held_back.len()).map_err(|_| {
            CoderError::Format(format!(
                "held_back length {} overflows i64",
                self.held_back.len()
            ))
        })?;
        VarIntCoder::encode_varint(count, writer)?;
        self.held_back.iter().try_for_each(|next| {
            VarIntCoder::encode_varint(next.wallclock_timestamp, writer)?;
            VarIntCoder::encode_varint(next.event_timestamp, writer)?;
            VarIntCoder::encode_varint(next.watermark, writer)?;
            EventCoder.encode(&next.event, writer, Context::Nested)
        })
    }

    fn decode_raw(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let start = VarIntCoder::decode_varint(reader)?;
        let end = VarIntCoder::decode_varint(reader)?;
        let wallclock_base_ms = VarIntCoder::decode_varint(reader)?;
        let raw_count = VarIntCoder::decode_varint(reader)?;
        let count = usize::try_from(raw_count)
            .map_err(|_| CoderError::Format(format!("negative held_back count: {raw_count}")))?;
        let held_back = (0..count)
            .map(|_| {
                let wallclock_timestamp = VarIntCoder::decode_varint(reader)?;
                let event_timestamp = VarIntCoder::decode_varint(reader)?;
                let watermark = VarIntCoder::decode_varint(reader)?;
                let event = EventCoder.decode(reader, Context::Nested)?;
                Ok(NextEvent {
                    wallclock_timestamp,
                    event_timestamp,
                    event,
                    watermark,
                })
            })
            .collect::<Result<Vec<_>, CoderError>>()?;
        Ok(Self::with_state(start, end, wallclock_base_ms, held_back))
    }
}

/// Direct binary coder (`beam:coder:bytes:v1`) for [`GeneratorRestriction`].
#[derive(Clone, Copy, Debug, Default)]
pub struct GeneratorRestrictionCoder;

impl Coder<GeneratorRestriction> for GeneratorRestrictionCoder {
    fn urn(&self) -> &'static str {
        URN_BYTES
    }

    fn encode(
        &self,
        value: &GeneratorRestriction,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        encode_framed(writer, context, |out| value.encode_raw(out))
    }

    fn decode(
        &self,
        reader: &mut &[u8],
        context: Context,
    ) -> Result<GeneratorRestriction, CoderError> {
        decode_framed(reader, context, GeneratorRestriction::decode_raw)
    }
}

impl DefaultCoder for GeneratorRestriction {
    type Coder = GeneratorRestrictionCoder;

    fn coder() -> Self::Coder {
        GeneratorRestrictionCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        GeneratorRestrictionCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        GeneratorRestrictionCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BYTES, Vec::new())
    }
}

#[derive(Debug)]
struct TrackerState {
    start: i64,
    next_offset: i64,
    end: i64,
    wallclock_base_ms: i64,
    held_back: VecDeque<NextEvent>,
    offset_stopped: bool,
    drain_pending: bool,
    checkpointed: bool,
    error: Option<RestrictionError>,
}

/// Thread-safe [`RestrictionTracker`] over a [`GeneratorRestriction`].
///
/// Progress is in estimated bytes, the unit of the source's restriction size, so a runner
/// reads the remaining work as backlog bytes.
#[derive(Clone, Debug)]
pub struct GeneratorRestrictionTracker {
    state: Arc<Mutex<TrackerState>>,
    avg_event_bytes: f64,
}

impl GeneratorRestrictionTracker {
    /// Initializes mutable cursor and queue state from a snapshot. `avg_event_bytes` prices
    /// each unclaimed offset in [`current_progress`](RestrictionTracker::current_progress).
    pub fn new(restriction: GeneratorRestriction, avg_event_bytes: f64) -> Self {
        let mut sorted = restriction.held_back;
        sorted.sort_by_key(held_back_key);
        Self {
            avg_event_bytes,
            state: Arc::new(Mutex::new(TrackerState {
                start: restriction.start,
                next_offset: restriction.start,
                end: restriction.end,
                wallclock_base_ms: restriction.wallclock_base_ms,
                held_back: VecDeque::from(sorted),
                offset_stopped: false,
                drain_pending: false,
                checkpointed: false,
                error: None,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, TrackerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reads the wallclock anchor, computing and storing it once when negative.
    pub fn wallclock_base_ms(&self, init: impl FnOnce() -> i64) -> i64 {
        let mut guard = self.lock();
        if guard.wallclock_base_ms < 0 {
            guard.wallclock_base_ms = init();
        }
        guard.wallclock_base_ms
    }

    /// Current `(next, end, stopped)` cursor triple.
    pub fn offset_bounds(&self) -> (i64, i64, bool) {
        let guard = self.lock();
        (guard.next_offset, guard.end, guard.offset_stopped)
    }

    /// Reports true when unclaimed positions or buffered events remain.
    pub fn has_remaining_work(&self) -> bool {
        let guard = self.lock();
        !guard.checkpointed
            && ((!guard.offset_stopped && guard.next_offset < guard.end)
                || !guard.held_back.is_empty())
    }

    /// Reports true when at least one buffered late event is queued.
    pub fn has_delayed(&self) -> bool {
        !self.lock().held_back.is_empty()
    }

    /// Earliest due timestamp in milliseconds among buffered events.
    pub fn peek_delayed_wallclock(&self) -> Option<i64> {
        self.lock().held_back.front().map(|e| e.wallclock_timestamp)
    }

    /// Pops the earliest buffered event whose target timestamp has passed.
    pub fn pop_due_delayed(&self, now_ms: i64) -> Option<NextEvent> {
        let mut guard = self.lock();
        if guard.checkpointed {
            return None;
        }
        guard
            .held_back
            .front()
            .is_some_and(|e| e.wallclock_timestamp <= now_ms)
            .then(|| guard.held_back.pop_front())
            .flatten()
    }

    /// Inserts a buffered event in `(wallclock_timestamp, event_timestamp)` order.
    pub fn defer_delayed(&self, event: NextEvent) {
        let mut guard = self.lock();
        let key = held_back_key(&event);
        let idx = guard.held_back.partition_point(|e| held_back_key(e) <= key);
        guard.held_back.insert(idx, event);
    }

    /// Marks the tracker to emit one empty residual on checkpoint before final close.
    pub fn schedule_watermark_drain(&self) {
        self.lock().drain_pending = true;
    }
}

impl RestrictionTracker for GeneratorRestrictionTracker {
    type Position = i64;
    type Restriction = GeneratorRestriction;

    fn try_claim(&self, position: &Self::Position) -> bool {
        let pos = *position;
        let mut guard = self.lock();
        if guard.offset_stopped || guard.checkpointed {
            return false;
        }
        if pos < guard.start {
            guard.offset_stopped = true;
            guard.error = Some(RestrictionError::OutOfBounds(format!(
                "{pos} < start {}",
                guard.start
            )));
            return false;
        }
        if pos < guard.next_offset {
            let prev = guard.next_offset - 1;
            guard.offset_stopped = true;
            guard.error = Some(RestrictionError::NonMonotonicClaim(
                pos.to_string(),
                prev.to_string(),
            ));
            return false;
        }
        if pos >= guard.end {
            guard.offset_stopped = true;
            return false;
        }
        guard.next_offset = pos + 1;
        true
    }

    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Option<(Self::Restriction, Self::Restriction)> {
        let mut guard = self.lock();
        if guard.checkpointed || guard.error.is_some() {
            return None;
        }

        if fraction_of_remainder <= 0.0 {
            let remaining_offsets = !guard.offset_stopped && guard.next_offset < guard.end;
            if !remaining_offsets && guard.held_back.is_empty() && !guard.drain_pending {
                return None;
            }
            guard.drain_pending = false;
            let residual_held: Vec<NextEvent> = std::mem::take(&mut guard.held_back).into();
            let split_at = guard.next_offset.min(guard.end);
            let old_end = guard.end.max(split_at);
            guard.end = split_at;
            guard.offset_stopped = true;
            guard.checkpointed = true;
            let primary = GeneratorRestriction {
                start: guard.start,
                end: split_at,
                wallclock_base_ms: guard.wallclock_base_ms,
                held_back: Vec::new(),
            };
            let residual = GeneratorRestriction {
                start: split_at,
                end: old_end,
                wallclock_base_ms: guard.wallclock_base_ms,
                held_back: residual_held,
            };
            return Some((primary, residual));
        }

        if guard.offset_stopped || fraction_of_remainder >= 1.0 {
            return None;
        }
        let rem = i128::from(guard.end) - i128::from(guard.next_offset);
        let min_step = i128::from(guard.held_back.is_empty());
        if rem <= min_step {
            return None;
        }
        let step = ((fraction_of_remainder * (rem as f64)).ceil() as i128).clamp(min_step, rem - 1);
        let mid = (i128::from(guard.next_offset) + step) as i64;
        let old_end = guard.end;
        guard.end = mid;
        // Clone snapshots buffered delayed events into the primary restriction returned to the runner.
        let primary_held: Vec<NextEvent> = guard.held_back.iter().cloned().collect();
        let primary = GeneratorRestriction {
            start: guard.start,
            end: mid,
            wallclock_base_ms: guard.wallclock_base_ms,
            held_back: primary_held,
        };
        let residual = GeneratorRestriction {
            start: mid,
            end: old_end,
            wallclock_base_ms: guard.wallclock_base_ms,
            held_back: Vec::new(),
        };
        Some((primary, residual))
    }

    fn current_restriction(&self) -> Self::Restriction {
        let guard = self.lock();
        // Clone snapshots buffered delayed events for restriction inspection.
        let held_back: Vec<NextEvent> = guard.held_back.iter().cloned().collect();
        GeneratorRestriction {
            start: guard.start,
            end: guard.end,
            wallclock_base_ms: guard.wallclock_base_ms,
            held_back,
        }
    }

    fn current_progress(&self) -> RestrictionProgress {
        let guard = self.lock();
        let claimed_end = guard.next_offset.min(guard.end);
        let completed = offset_bytes(guard.start, claimed_end, self.avg_event_bytes);
        let remaining_offsets = if guard.offset_stopped || guard.checkpointed {
            0.0
        } else {
            offset_bytes(guard.next_offset, guard.end, self.avg_event_bytes)
        };
        let remaining = remaining_offsets + held_back_bytes(&guard.held_back);
        RestrictionProgress::new(completed, remaining)
    }

    fn check_done(&self) -> Result<(), RestrictionError> {
        let guard = self.lock();
        if let Some(err) = &guard.error {
            return Err(err.clone());
        }
        if !guard.checkpointed && guard.next_offset < guard.end {
            return Err(RestrictionError::IncompleteWork(format!(
                "offsets [{}..{}) were not claimed",
                guard.next_offset, guard.end
            )));
        }
        if !guard.held_back.is_empty() {
            return Err(RestrictionError::IncompleteWork(format!(
                "{} delayed events were not emitted",
                guard.held_back.len()
            )));
        }
        Ok(())
    }
}
