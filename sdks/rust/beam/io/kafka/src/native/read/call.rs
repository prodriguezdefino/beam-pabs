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

//! One reader call of a partition: its loop, when it ends, where its assignment parks, how
//! an idle call moves the watermark, and the counter of new assignments.
//!
//! A call polls in short slices. It ends when its read time is used up, when no record came
//! for the max idle time, or when the runner splits its restriction. So a short gap between
//! two batches of a busy partition does not end the call, and the read resumes less often.
//!
//! After an idle call that read every record, the watermark moves to the wall clock less the
//! fetch wait of the consumer (`fetch.wait.max.ms`). A record that the broker got during the
//! last fetch wait, with a timestamp before that point, is late.

use std::time::{Duration, Instant};

use beam::metrics::Metrics;
use beam::transforms::sdf::{ProcessContinuation, RestrictionError, RestrictionTracker};

use super::restriction::PartitionTracker;

/// The longest single poll. A call checks its stop time, a split and its time limits between
/// polls.
pub const POLL_SLICE: Duration = Duration::from_millis(100);

/// The shortest time between two broker requests for the high watermark of an assignment
/// with no fetch response yet. Until a fetch response comes, the watermark does not move
/// anyway, and a broker that does not answer blocks each request for seconds.
pub const BROKER_REQUEST_INTERVAL: Duration = Duration::from_secs(10);

/// The shortest wait for the first record after the reader assigns a partition. The consumer
/// starts to fetch the partition at the assignment.
pub const FIRST_POLL_TIMEOUT: Duration = Duration::from_secs(2);

/// The namespace of the metric of the read calls.
pub const CALL_METRICS: &str = "kafka_read";

/// The counter of the calls that made a new assignment of their partition, not a parked one.
pub const NEW_ASSIGNMENTS: &str = "new_assignments";

/// Why a reader call ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallEnd {
    /// The call used up its max read time. The read resumes.
    MaxReadTime,
    /// No record came for the max idle time. The read resumes.
    Idle,
    /// The runner split the restriction, for example at a checkpoint. The runner has the
    /// residual.
    Split,
    /// A claim failed at the end of a restriction that the runner did not split.
    EndOfRestriction,
    /// The read reached its stop time and is complete.
    Stopped,
}

impl CallEnd {
    /// The continuation of a call that ended for this reason. A call resumes only after its
    /// read time or its idle time.
    pub fn continuation(self) -> ProcessContinuation {
        match self {
            Self::MaxReadTime | Self::Idle => ProcessContinuation::Resume,
            Self::Split | Self::EndOfRestriction | Self::Stopped => ProcessContinuation::Stop,
        }
    }
}

/// Counts a call that made a new assignment of its partition.
pub fn record_new_assignment() {
    Metrics::counter(CALL_METRICS, NEW_ASSIGNMENTS).inc();
}

/// The longest wait for the first record of a call. `parked` tells whether the call takes a
/// parked assignment, which fetched during the wait. A call that makes a new assignment waits
/// for its first fetch, and counts in [`NEW_ASSIGNMENTS`].
pub fn first_wait(parked: bool) -> Duration {
    match parked {
        true => Duration::ZERO,
        false => {
            record_new_assignment();
            FIRST_POLL_TIMEOUT
        }
    }
}

/// The offset where the next call of the partition starts with the assignment of a call
/// that ended for `end`, or `None` when the call has no residual and drops the assignment. A
/// call that fails drops its assignment too.
///
/// After a resume, the residual starts after the last attempted claim. After a split, it
/// starts at the end of the primary restriction: a refused claim also counts as an
/// attempt, so the offset after the last attempt is one too far. The reader keeps the
/// refused record with the assignment, so the next call emits it first.
pub fn park_offset(end: CallEnd, tracker: &PartitionTracker) -> Option<i64> {
    match end {
        CallEnd::MaxReadTime | CallEnd::Idle => Some(tracker.next_offset()),
        CallEnd::Split => Some(tracker.current_restriction().end),
        CallEnd::EndOfRestriction | CallEnd::Stopped => None,
    }
}

/// How an idle call checks that the partition has no record after the claimed ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdleCheck {
    /// The fetch responses of the assignment keep the cached high watermark current.
    Cached,
    /// Only the broker can tell: the cached value can be older than the assignment.
    Broker,
    /// The broker answered for this assignment less than [`BROKER_REQUEST_INTERVAL`] ago, so
    /// the watermark does not move.
    Skip,
}

/// The check of an idle call. `fetch_seen` tells whether a fetch response came for the
/// assignment: a record, or the end of the partition. `last_broker_request` tells when an
/// idle call last asked the broker for the assignment.
pub fn idle_check(
    fetch_seen: bool,
    last_broker_request: Option<Instant>,
    now: Instant,
) -> IdleCheck {
    match (fetch_seen, last_broker_request) {
        (true, _) => IdleCheck::Cached,
        (false, Some(at)) if now.saturating_duration_since(at) < BROKER_REQUEST_INTERVAL => {
            IdleCheck::Skip
        }
        (false, _) => IdleCheck::Broker,
    }
}

/// The watermark of an idle partition with every record read: `now` less `fetch_wait`.
pub fn idle_watermark(now: beam::Instant, fetch_wait: Duration) -> beam::Instant {
    let wait = i64::try_from(fetch_wait.as_millis()).unwrap_or(i64::MAX);
    beam::Instant::from_millis(now.as_millis().saturating_sub(wait))
}

/// What a call does next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Poll for at most this time.
    Poll(Duration),
    /// End the call.
    End(CallEnd),
}

/// The read time and the idle time of one call.
#[derive(Clone, Copy, Debug)]
pub struct CallLimits {
    read_deadline: Instant,
    max_idle: Duration,
    idle_deadline: Instant,
    /// A record came since the last step, so the idle time starts again at the next step.
    record_seen: bool,
}

impl CallLimits {
    /// A call that starts at `started`. The first record can take `first_wait` if that is
    /// longer than `max_idle`, because a new assignment needs a first fetch.
    pub fn new(
        started: Instant,
        max_read_time: Duration,
        max_idle: Duration,
        first_wait: Duration,
    ) -> Self {
        Self {
            read_deadline: started + max_read_time,
            max_idle,
            idle_deadline: started + max_idle.max(first_wait),
            record_seen: false,
        }
    }

    /// Records that a record came.
    pub fn record_seen(&mut self) {
        self.record_seen = true;
    }

    /// The next step at `now`. The read time ends the call before the idle time.
    pub fn step(&mut self, now: Instant) -> Step {
        if std::mem::take(&mut self.record_seen) {
            self.idle_deadline = now + self.max_idle;
        }
        let read_left = self.read_deadline.saturating_duration_since(now);
        let idle_left = self.idle_deadline.saturating_duration_since(now);
        match (read_left.is_zero(), idle_left.is_zero()) {
            (true, _) => Step::End(CallEnd::MaxReadTime),
            (false, true) => Step::End(CallEnd::Idle),
            (false, false) => Step::Poll(POLL_SLICE.min(read_left).min(idle_left)),
        }
    }
}

/// The most records that one batch takes from the queue with no wait. A call then reads the
/// clock and checks its time limits again. A batch is short: about one millisecond of records.
pub const MAX_BATCH: usize = 1024;

/// What one poll gave.
#[derive(Debug)]
pub enum Polled<R> {
    /// No record came in the time of the poll.
    Nothing,
    /// A fetch response reached the end of the partition. This is not a record, so the idle
    /// time goes on.
    EndOfPartition,
    /// A record.
    Record(R),
}

/// The end of a call loop, the record whose claim failed, if any, and whether a poll gave
/// the end of the partition.
#[derive(Debug)]
pub struct Ended<R> {
    pub end: CallEnd,
    pub refused: Option<R>,
    pub reached_end: bool,
}

/// How one batch of a call ended.
enum BatchEnd<R> {
    /// The queue had no record left, or the batch is full.
    Done { emitted: bool, reached_end: bool },
    /// The claim of `record` failed, so the call ends.
    Refused { end: CallEnd, record: R },
}

/// Runs one call: polls `poll` for records and gives each claimed record to `emit`. The call
/// ends at a time limit of `limits`, after `stop_at`, or when the runner splits the
/// restriction.
///
/// `poll` waits at most its argument. `offset` gives the offset of a record. `source` names
/// the partition in errors. Only the first poll of a batch waits, at most one poll time. The
/// next polls of the batch do not wait, so the call reads the clock and checks for a split
/// once for each batch, not for each record. After a split, the next claim fails, so the call
/// ends at the next record. A split during an idle wait ends the call within one
/// [`POLL_SLICE`].
///
/// # Errors
///
/// Fails when `poll` or `emit` fails, or when a record is not after the claimed offsets.
/// That happens when the log of the partition was truncated or the topic was created again,
/// so the fetch position moved back.
pub fn read_loop<R>(
    source: &str,
    limits: &mut CallLimits,
    tracker: &PartitionTracker,
    stop_at: Option<beam::Instant>,
    mut poll: impl FnMut(Duration) -> beam::Result<Polled<R>>,
    offset: impl Fn(&R) -> i64,
    mut emit: impl FnMut(R) -> beam::Result,
) -> beam::Result<Ended<R>> {
    let mut reached_end = false;
    loop {
        if stop_at.is_some_and(|stop_at| beam::Instant::now() >= stop_at) {
            tracker.finish();
            return Ok(Ended {
                end: CallEnd::Stopped,
                refused: None,
                reached_end,
            });
        }
        let step = match tracker.was_split() {
            true => Step::End(CallEnd::Split),
            false => limits.step(Instant::now()),
        };
        let timeout = match step {
            Step::Poll(timeout) => timeout,
            Step::End(end) => {
                return Ok(Ended {
                    end,
                    refused: None,
                    reached_end,
                });
            }
        };
        let first = poll(timeout)?;
        match emit_batch(source, tracker, first, &mut poll, &offset, &mut emit)? {
            BatchEnd::Done {
                emitted,
                reached_end: batch_end,
            } => {
                reached_end |= batch_end;
                if emitted {
                    limits.record_seen();
                }
            }
            BatchEnd::Refused { end, record } => {
                return Ok(Ended {
                    end,
                    refused: Some(record),
                    reached_end,
                });
            }
        }
    }
}

/// Claims and emits `first` and the records that the queue holds after it, at most
/// [`MAX_BATCH`]. Polls with no wait after `first`.
fn emit_batch<R>(
    source: &str,
    tracker: &PartitionTracker,
    first: Polled<R>,
    poll: &mut impl FnMut(Duration) -> beam::Result<Polled<R>>,
    offset: &impl Fn(&R) -> i64,
    emit: &mut impl FnMut(R) -> beam::Result,
) -> beam::Result<BatchEnd<R>> {
    let mut polled = first;
    for count in 0..MAX_BATCH {
        let record = match polled {
            Polled::Nothing => {
                return Ok(BatchEnd::Done {
                    emitted: count > 0,
                    reached_end: false,
                });
            }
            Polled::EndOfPartition => {
                return Ok(BatchEnd::Done {
                    emitted: count > 0,
                    reached_end: true,
                });
            }
            Polled::Record(record) => record,
        };
        let position = offset(&record);
        if !tracker.try_claim(&position) {
            if let Some(error) = tracker.claim_error() {
                return Err(offsets_went_back(source, position, tracker, &error));
            }
            let end = match tracker.was_split() {
                true => CallEnd::Split,
                false => CallEnd::EndOfRestriction,
            };
            return Ok(BatchEnd::Refused { end, record });
        }
        emit(record)?;
        polled = match count + 1 < MAX_BATCH {
            true => poll(Duration::ZERO)?,
            false => Polled::Nothing,
        };
    }
    Ok(BatchEnd::Done {
        emitted: true,
        reached_end: false,
    })
}

/// The error of a record at `refused` that is not after the claimed offsets of `tracker`.
fn offsets_went_back(
    source: &str,
    refused: i64,
    tracker: &PartitionTracker,
    error: &RestrictionError,
) -> beam::Error {
    let last = tracker.last_claimed().map_or_else(
        || format!("the start offset {}", tracker.current_restriction().start),
        |last| format!("the last read offset {last}"),
    );
    beam::Error::msg(format!(
        "Cannot read {source}: the record at offset {refused} is not after {last} ({error}). \
         The log of the partition was truncated, or the topic was created again. Start the \
         read again from a new start offset, for example with a new consumer group or an \
         explicit start offset."
    ))
}
