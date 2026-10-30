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

//! The restriction of one partition, its coder, the tracker that claims its offsets, and the
//! backlog estimate of each partition.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use beam::Instant;
use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, RestrictionError, RestrictionProgress, RestrictionTracker,
};
use beam::values::IsBounded;

/// The high watermark of a partition before the reader asks the broker for it.
const UNKNOWN: i64 = -1;

/// The number of records that the average record size remembers. A batch of `n` records gets
/// the weight `n / (n + remembered)`.
const RECORD_BYTES_WINDOW: u64 = 1_000;

/// The offsets `[start, end)` of one partition, and the output watermark when the reader
/// stopped. `end` is `i64::MAX` for a partition with no end. The watermark goes with the
/// restriction, because each residual gets a new tracker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionRestriction {
    pub start: i64,
    pub end: i64,
    pub watermark: Instant,
}

impl PartitionRestriction {
    /// All offsets from `start`, with no watermark yet.
    pub fn from_offset(start: i64) -> Self {
        Self {
            start,
            end: i64::MAX,
            watermark: Instant::MIN,
        }
    }

    /// The records from `start` to the end of the restriction or to `high_watermark`, the
    /// offset after the last record of the partition. `0.0` when the high watermark is not
    /// known and the restriction has no end.
    pub fn backlog(&self, high_watermark: Option<i64>) -> f64 {
        let limit = match (high_watermark, self.end) {
            (Some(high), end) => high.min(end),
            (None, i64::MAX) => self.start,
            (None, end) => end,
        };
        (limit - self.start).max(0) as f64
    }
}

/// Wire format: a KV of `(start, end)` and the watermark in milliseconds.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartitionRestrictionCoder;

impl Coder<PartitionRestriction> for PartitionRestrictionCoder {
    fn urn(&self) -> &'static str {
        beam::coders::URN_KV
    }

    fn encode(
        &self,
        value: &PartitionRestriction,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(
        &self,
        reader: &mut &[u8],
        _context: Context,
    ) -> Result<PartitionRestriction, CoderError> {
        PartitionRestriction::decode_element(reader)
    }
}

impl DefaultCoder for PartitionRestriction {
    type Coder = PartitionRestrictionCoder;

    fn coder() -> Self::Coder {
        PartitionRestrictionCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        ((self.start, self.end), self.watermark.as_millis()).encode_element(writer)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let ((start, end), watermark) = <((i64, i64), i64)>::decode_element(reader)?;
        Ok(Self {
            start,
            end,
            watermark: Instant::from_millis(watermark),
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <((i64, i64), i64)>::register_coder(registry)
    }
}

/// Claims the offsets of one partition and keeps its output watermark and its high
/// watermark. A position is the offset of a record. Offsets can have gaps, for example after
/// compaction or at transaction markers, so a claim needs only a larger offset.
///
/// Only checkpoints split the restriction, so a read has one restriction of each partition at
/// a time.
pub struct PartitionTracker {
    offsets: OffsetRangeTracker,
    watermark: AtomicI64,
    high_watermark: AtomicI64,
    boundedness: IsBounded,
    /// Whether the runner split the restriction through [`RestrictionTracker::try_split`].
    split: AtomicBool,
}

impl PartitionTracker {
    pub fn new(restriction: PartitionRestriction, boundedness: IsBounded) -> Self {
        let PartitionRestriction {
            start,
            end,
            watermark,
        } = restriction;
        Self {
            offsets: OffsetRangeTracker::new(OffsetRange::new(start, end)),
            watermark: AtomicI64::new(watermark.as_millis()),
            high_watermark: AtomicI64::new(UNKNOWN),
            boundedness,
            split: AtomicBool::new(false),
        }
    }

    /// The offset of the last claimed record.
    pub fn last_claimed(&self) -> Option<i64> {
        self.offsets.last_claimed()
    }

    /// Moves the output watermark to `timestamp` if it is later. The watermark never moves
    /// back.
    pub fn advance_watermark(&self, timestamp: Instant) {
        self.watermark
            .fetch_max(timestamp.as_millis(), Ordering::AcqRel);
    }

    /// Records the offset after the last record of the partition, from the broker.
    pub fn set_high_watermark(&self, offset: i64) {
        self.high_watermark.store(offset, Ordering::Release);
    }

    /// The offset after the last record of the partition, if the reader has asked the broker.
    pub fn high_watermark(&self) -> Option<i64> {
        Some(self.high_watermark.load(Ordering::Acquire)).filter(|offset| *offset != UNKNOWN)
    }

    /// Whether the reader has claimed every record up to the high watermark.
    pub fn is_caught_up(&self) -> bool {
        let next = self.next_offset();
        self.high_watermark().is_some_and(|high| next >= high)
    }

    /// The error that a claim recorded, for example a claim at or before the last attempted
    /// offset. A claim that fails only because the restriction ended records none.
    pub fn claim_error(&self) -> Option<RestrictionError> {
        self.offsets.error()
    }

    /// Whether the runner split the restriction. [`PartitionTracker::finish`] is not a split.
    pub fn was_split(&self) -> bool {
        self.split.load(Ordering::Acquire)
    }

    /// Ends the restriction after the claimed offsets and drops the rest, so a bounded read
    /// can stop.
    pub fn finish(&self) {
        let _dropped = self.offsets.try_split(0.0);
    }

    /// The offset after the last attempted claim, or the start of the restriction. A
    /// checkpoint starts its residual here.
    pub fn next_offset(&self) -> i64 {
        self.offsets
            .last_attempted()
            .map_or(self.offsets.current_restriction().start, |offset| {
                offset + 1
            })
    }

    fn with_watermark(&self, range: OffsetRange) -> PartitionRestriction {
        PartitionRestriction {
            start: range.start,
            end: range.end,
            watermark: Instant::from_millis(self.watermark.load(Ordering::Acquire)),
        }
    }
}

impl RestrictionTracker for PartitionTracker {
    type Position = i64;
    type Restriction = PartitionRestriction;

    fn try_claim(&self, offset: &i64) -> bool {
        self.offsets.try_claim(offset)
    }

    /// Splits only at the claimed offsets. The residual starts at the next offset and keeps
    /// the watermark.
    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Option<(PartitionRestriction, PartitionRestriction)> {
        match fraction_of_remainder > 0.0 {
            true => None,
            false => self.offsets.try_split(0.0).map(|(primary, residual)| {
                self.split.store(true, Ordering::Release);
                (self.with_watermark(primary), self.with_watermark(residual))
            }),
        }
    }

    fn current_restriction(&self) -> PartitionRestriction {
        self.with_watermark(self.offsets.current_restriction())
    }

    /// Reports records. The remaining records go up to the high watermark, so autoscalers see
    /// the lag of the partition. Until the restriction ends, at least one record remains: a
    /// partition that is caught up can get new records, so its work is not complete.
    fn current_progress(&self) -> RestrictionProgress {
        let restriction = self.current_restriction();
        let next = self.next_offset();
        let backlog = PartitionRestriction {
            start: next,
            ..restriction
        }
        .backlog(self.high_watermark());
        let remaining = match next < restriction.end {
            true => backlog.max(1.0),
            false => backlog,
        };
        RestrictionProgress::new((next - restriction.start) as f64, remaining)
    }

    fn check_done(&self) -> Result<(), RestrictionError> {
        self.offsets.check_done()
    }

    fn boundedness(&self) -> IsBounded {
        self.boundedness
    }

    /// `None` until a record or the restriction gives a watermark. The runner keeps its own
    /// watermark then, and does not take `Instant::MIN` as the output watermark.
    fn current_watermark(&self) -> Option<Instant> {
        Some(Instant::from_millis(self.watermark.load(Ordering::Acquire)))
            .filter(|watermark| *watermark > Instant::MIN)
    }
}

/// The last known high watermark and the average record size of one partition.
#[derive(Clone, Copy, Debug, Default)]
struct PartitionBacklog {
    high_watermark: Option<i64>,
    record_bytes: f64,
    sampled_records: u64,
}

impl PartitionBacklog {
    fn observe_records(&mut self, bytes: usize, records: u64) {
        if records == 0 {
            return;
        }
        let total = self.sampled_records + records;
        let weight = records as f64 / total as f64;
        let batch_average = bytes as f64 / records as f64;
        self.record_bytes += (batch_average - self.record_bytes) * weight;
        self.sampled_records = total.min(RECORD_BYTES_WINDOW);
    }
}

/// The backlog estimate of each partition that a reader of this process read: the last
/// known high watermark and the average record size (key and value bytes).
#[derive(Debug, Default)]
pub struct PartitionBacklogs(Mutex<HashMap<i32, PartitionBacklog>>);

impl PartitionBacklogs {
    /// The last known high watermark of `partition`.
    pub fn high_watermark(&self, partition: i32) -> Option<i64> {
        self.0
            .lock()
            .ok()
            .and_then(|backlogs| backlogs.get(&partition).and_then(|b| b.high_watermark))
    }

    pub fn set_high_watermark(&self, partition: i32, offset: i64) {
        if let Ok(mut backlogs) = self.0.lock() {
            backlogs.entry(partition).or_default().high_watermark = Some(offset);
        }
    }

    /// Adds a batch of `records` with `bytes` of keys and values to the average record size
    /// of `partition`.
    pub fn observe_records(&self, partition: i32, bytes: usize, records: u64) {
        if records > 0
            && let Ok(mut backlogs) = self.0.lock()
        {
            backlogs
                .entry(partition)
                .or_default()
                .observe_records(bytes, records);
        }
    }

    /// The bytes of `restriction` that are not read yet, from the last known high watermark
    /// and the average record size.
    ///
    /// The size is `0.0` when no record is left. It is `1.0` when it is not known: before the
    /// first record of the partition, or with no high watermark and no end. Dataflow ignores a
    /// backlog of 1 byte, so it keeps its last value. The progress of the tracker counts
    /// records and keeps at least one, so a caught-up partition is not complete.
    pub fn restriction_size(&self, partition: i32, restriction: &PartitionRestriction) -> f64 {
        let backlog = self
            .0
            .lock()
            .ok()
            .and_then(|backlogs| backlogs.get(&partition).copied())
            .unwrap_or_default();
        let known = backlog.high_watermark.is_some() || restriction.end != i64::MAX;
        let records = restriction.backlog(backlog.high_watermark);
        match (known, backlog.sampled_records) {
            (true, _) if records <= 0.0 => 0.0,
            (false, _) | (true, 0) => 1.0,
            (true, _) => records * backlog.record_bytes.max(1.0),
        }
    }
}
