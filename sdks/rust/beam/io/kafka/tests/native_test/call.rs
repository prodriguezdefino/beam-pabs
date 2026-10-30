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

//! One read call: its time limits, its loop over a fake queue, where its assignment parks,
//! and how an idle call checks the high watermark and moves the watermark.

use std::collections::VecDeque;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use beam::transforms::sdf::{ProcessContinuation, RestrictionTracker};
use beam::values::IsBounded;
use kafka_io::native::read::call::{
    BROKER_REQUEST_INTERVAL, CALL_METRICS, CallEnd, CallLimits, Ended, FIRST_POLL_TIMEOUT,
    IdleCheck, MAX_BATCH, NEW_ASSIGNMENTS, POLL_SLICE, Polled, Step, first_wait, idle_check,
    idle_watermark, park_offset, read_loop,
};
use kafka_io::native::read::restriction::{PartitionRestriction, PartitionTracker};

const READ: Duration = Duration::from_secs(5);
const SOURCE: &str = "partition 3 of Kafka topic 'events'";
const IDLE: Duration = Duration::from_secs(1);

/// Every reason, so each test can check all of them.
const ENDS: [CallEnd; 5] = [
    CallEnd::MaxReadTime,
    CallEnd::Idle,
    CallEnd::Split,
    CallEnd::EndOfRestriction,
    CallEnd::Stopped,
];

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn tracker_from(start: i64) -> PartitionTracker {
    PartitionTracker::new(
        PartitionRestriction::from_offset(start),
        IsBounded::Unbounded,
    )
}

/// A queue that gives `polls` in order and then waits out each poll with no record.
fn fake_polls(polls: Vec<Polled<i64>>) -> impl FnMut(Duration) -> beam::Result<Polled<i64>> {
    let mut queue: VecDeque<Polled<i64>> = polls.into();
    move |timeout| match queue.pop_front() {
        Some(polled) => Ok(polled),
        None => {
            thread::sleep(timeout);
            Ok(Polled::Nothing)
        }
    }
}

/// A queue that gives the records at `offsets` in order and then waits out each poll.
fn fake_queue(offsets: &[i64]) -> impl FnMut(Duration) -> beam::Result<Polled<i64>> {
    fake_polls(offsets.iter().copied().map(Polled::Record).collect())
}

/// Runs a call over `queue` and returns how it ended and the emitted offsets.
fn run(
    limits: &mut CallLimits,
    tracker: &PartitionTracker,
    queue: impl FnMut(Duration) -> beam::Result<Polled<i64>>,
) -> (Ended<i64>, Vec<i64>) {
    let mut emitted = Vec::new();
    let ended = read_loop(
        SOURCE,
        limits,
        tracker,
        None,
        queue,
        |offset| *offset,
        |offset| {
            emitted.push(offset);
            Ok(())
        },
    )
    .expect("the call does not fail");
    (ended, emitted)
}

#[test]
fn a_call_polls_in_slices_and_ends_after_the_idle_time_with_no_record() {
    let t0 = Instant::now();
    let mut limits = CallLimits::new(t0, READ, IDLE, Duration::ZERO);
    assert_eq!(limits.step(t0), Step::Poll(POLL_SLICE));
    assert_eq!(limits.step(t0 + ms(950)), Step::Poll(ms(50)));
    assert_eq!(limits.step(t0 + IDLE), Step::End(CallEnd::Idle));
}

#[test]
fn a_record_starts_the_idle_time_again_so_short_gaps_do_not_end_the_call() {
    let t0 = Instant::now();
    let mut limits = CallLimits::new(t0, READ, IDLE, Duration::ZERO);
    // Records arrive every 800 ms: each gap is shorter than the idle time.
    let ends: Vec<Step> = (1..=5)
        .map(|n| {
            limits.record_seen();
            limits.step(t0 + ms(800 * n))
        })
        .collect();
    assert!(
        ends.iter().all(|step| matches!(step, Step::Poll(_))),
        "{ends:?}"
    );
    assert_eq!(limits.step(t0 + ms(4_000 + 999)), Step::Poll(ms(1)));
    assert_eq!(limits.step(t0 + READ), Step::End(CallEnd::MaxReadTime));
}

#[test]
fn the_read_time_ends_a_busy_call_and_bounds_the_last_poll() {
    let t0 = Instant::now();
    let mut limits = CallLimits::new(t0, READ, IDLE, Duration::ZERO);
    limits.record_seen();
    assert_eq!(limits.step(t0 + ms(4_970)), Step::Poll(ms(30)));
    limits.record_seen();
    assert_eq!(limits.step(t0 + ms(5_001)), Step::End(CallEnd::MaxReadTime));
}

#[test]
fn a_new_assignment_waits_longer_for_its_first_record_but_not_after_it() {
    let t0 = Instant::now();
    let first = Duration::from_secs(2);
    let mut limits = CallLimits::new(t0, READ, IDLE, first);
    assert_eq!(limits.step(t0 + ms(1_500)), Step::Poll(POLL_SLICE));
    assert_eq!(limits.step(t0 + first), Step::End(CallEnd::Idle));

    let mut fed = CallLimits::new(t0, READ, IDLE, first);
    fed.record_seen();
    assert_eq!(fed.step(t0 + ms(100)), Step::Poll(POLL_SLICE));
    assert_eq!(fed.step(t0 + ms(1_100)), Step::End(CallEnd::Idle));
}

#[test]
fn an_idle_time_longer_than_the_read_time_ends_the_call_at_the_read_time() {
    let t0 = Instant::now();
    let mut limits = CallLimits::new(t0, ms(300), IDLE, Duration::ZERO);
    assert_eq!(limits.step(t0 + ms(250)), Step::Poll(ms(50)));
    assert_eq!(limits.step(t0 + ms(300)), Step::End(CallEnd::MaxReadTime));
}

#[test]
fn only_the_read_time_and_the_idle_time_resume_the_read() {
    let resumed: Vec<CallEnd> = ENDS
        .into_iter()
        .filter(|end| end.continuation() == ProcessContinuation::Resume)
        .collect();
    assert_eq!(resumed, [CallEnd::MaxReadTime, CallEnd::Idle]);
}

#[test]
fn a_call_emits_each_record_and_ends_idle_after_the_last_one() {
    let tracker = tracker_from(10);
    let mut limits = CallLimits::new(Instant::now(), READ, ms(150), Duration::ZERO);
    let (ended, emitted) = run(&mut limits, &tracker, fake_queue(&[10, 11, 13]));
    assert_eq!(ended.end, CallEnd::Idle);
    assert!(ended.refused.is_none());
    assert_eq!(emitted, [10, 11, 13]);
    assert_eq!(park_offset(ended.end, &tracker), Some(14));
}

#[test]
fn only_the_first_poll_of_a_batch_waits() {
    let tracker = tracker_from(10);
    let mut limits = CallLimits::new(Instant::now(), READ, ms(150), Duration::ZERO);
    let mut queue = fake_polls(vec![
        Polled::Record(10),
        Polled::Record(11),
        Polled::Record(12),
        Polled::Nothing,
        Polled::Record(13),
        Polled::EndOfPartition,
    ]);
    let mut waits = Vec::new();
    let (ended, emitted) = run(&mut limits, &tracker, |timeout| {
        waits.push(timeout);
        queue(timeout)
    });
    assert_eq!(emitted, [10, 11, 12, 13]);
    assert!(ended.reached_end);
    let waited: Vec<bool> = waits.iter().take(6).map(|wait| !wait.is_zero()).collect();
    assert_eq!(waited, [true, false, false, false, true, false]);
}

#[test]
fn a_full_batch_checks_the_time_limits_before_the_next_poll() {
    let tracker = tracker_from(0);
    let mut limits = CallLimits::new(Instant::now(), READ, ms(150), Duration::ZERO);
    let offsets: Vec<i64> = (0..=i64::try_from(MAX_BATCH).expect("MAX_BATCH fits")).collect();
    let mut queue = fake_queue(&offsets);
    let mut waits = Vec::new();
    let (_, emitted) = run(&mut limits, &tracker, |timeout| {
        waits.push(timeout);
        queue(timeout)
    });
    assert_eq!(emitted, offsets);
    assert!(waits[1..MAX_BATCH].iter().all(Duration::is_zero));
    assert!(!waits[MAX_BATCH].is_zero(), "{:?}", waits[MAX_BATCH]);
}

#[test]
fn a_split_during_an_idle_wait_ends_the_call_within_one_poll_slice() {
    let tracker = Arc::new(tracker_from(10));
    let mut limits = CallLimits::new(Instant::now(), READ, READ, Duration::ZERO);
    let splitter = thread::spawn({
        let tracker = Arc::clone(&tracker);
        move || {
            thread::sleep(ms(250));
            tracker.try_split(0.0).map(|(_, residual)| residual.start)
        }
    });
    let started = Instant::now();
    let (ended, emitted) = run(&mut limits, &tracker, fake_queue(&[10, 11]));
    let waited = started.elapsed();
    let residual_start = splitter
        .join()
        .expect("the splitter does not panic")
        .expect("the runner checkpoints");

    assert_eq!(ended.end, CallEnd::Split);
    assert!(ended.refused.is_none());
    assert_eq!(emitted, [10, 11]);
    assert!(waited < ms(250) + POLL_SLICE + ms(100), "{waited:?}");
    assert_eq!(park_offset(ended.end, &tracker), Some(residual_start));
    assert_eq!(residual_start, 12);
}

#[test]
fn a_record_refused_after_a_split_is_kept_and_the_assignment_parks_at_the_residual() {
    let tracker = Arc::new(tracker_from(10));
    let mut limits = CallLimits::new(Instant::now(), READ, IDLE, Duration::ZERO);
    // The runner splits after the record at 11, while the record at 15 is in flight.
    let mut queue = fake_queue(&[10, 11, 15, 16]);
    let (ended, emitted) = run(&mut limits, &tracker, |timeout| {
        let polled = queue(timeout)?;
        if matches!(polled, Polled::Record(15)) {
            assert!(tracker.try_split(0.0).is_some());
        }
        Ok(polled)
    });

    assert_eq!(ended.end, CallEnd::Split);
    assert_eq!(ended.refused, Some(15));
    assert_eq!(emitted, [10, 11]);
    // The refused claim counts as attempted, so the residual is not at the next offset.
    assert_eq!(tracker.next_offset(), 16);
    assert_eq!(park_offset(ended.end, &tracker), Some(12));
}

#[test]
fn a_split_in_the_middle_of_a_batch_ends_the_call_at_the_next_record() {
    let tracker = tracker_from(10);
    let mut limits = CallLimits::new(Instant::now(), READ, IDLE, Duration::ZERO);
    let mut emitted = Vec::new();
    let ended = read_loop(
        SOURCE,
        &mut limits,
        &tracker,
        None,
        fake_queue(&[10, 11, 12, 13]),
        |offset| *offset,
        |offset| {
            emitted.push(offset);
            if offset == 11 {
                assert!(tracker.try_split(0.0).is_some());
            }
            Ok(())
        },
    )
    .expect("the call does not fail");

    assert_eq!(ended.end, CallEnd::Split);
    assert_eq!(ended.refused, Some(12));
    assert_eq!(emitted, [10, 11]);
    assert_eq!(park_offset(ended.end, &tracker), Some(12));
}

#[test]
fn a_claim_past_an_unsplit_restriction_ends_the_call_with_no_residual() {
    let tracker = PartitionTracker::new(
        PartitionRestriction {
            end: 12,
            ..PartitionRestriction::from_offset(10)
        },
        IsBounded::Bounded,
    );
    let mut limits = CallLimits::new(Instant::now(), READ, IDLE, Duration::ZERO);
    let (ended, emitted) = run(&mut limits, &tracker, fake_queue(&[10, 11, 12]));
    assert_eq!(ended.end, CallEnd::EndOfRestriction);
    assert_eq!(ended.refused, Some(12));
    assert_eq!(emitted, [10, 11]);
    assert_eq!(park_offset(ended.end, &tracker), None);
}

#[test]
fn a_call_past_its_stop_time_finishes_the_restriction() {
    let tracker = tracker_from(10);
    let mut limits = CallLimits::new(Instant::now(), READ, IDLE, Duration::ZERO);
    let ended = read_loop(
        SOURCE,
        &mut limits,
        &tracker,
        Some(beam::Instant::now()),
        fake_queue(&[10]),
        |offset| *offset,
        |_| Ok(()),
    )
    .expect("the call does not fail");
    assert_eq!(ended.end, CallEnd::Stopped);
    assert!(!tracker.try_claim(&10));
    assert_eq!(park_offset(ended.end, &tracker), None);
}

#[test]
fn a_failed_poll_or_emit_fails_the_call() {
    let tracker = tracker_from(10);
    let mut limits = CallLimits::new(Instant::now(), READ, IDLE, Duration::ZERO);
    let polled = read_loop(
        SOURCE,
        &mut limits,
        &tracker,
        None,
        |_| Err::<Polled<i64>, _>(beam::Error::msg("broker gone")),
        |offset| *offset,
        |_| Ok(()),
    );
    assert!(polled.is_err());
    let emitted = read_loop(
        SOURCE,
        &mut limits,
        &tracker,
        None,
        fake_queue(&[10]),
        |offset| *offset,
        |_| Err(beam::Error::msg("no output")),
    );
    assert!(emitted.is_err());
}

#[test]
fn a_fetched_assignment_trusts_its_cached_high_watermark() {
    let now = Instant::now();
    assert_eq!(idle_check(true, None, now), IdleCheck::Cached);
    assert_eq!(idle_check(true, Some(now), now), IdleCheck::Cached);
}

#[test]
fn an_assignment_with_no_record_asks_the_broker_at_most_once_in_the_interval() {
    // Before a record, the cached high watermark can come from before the assignment: a
    // stale value that would move the watermark ahead of records.
    let now = Instant::now();
    assert_eq!(idle_check(false, None, now), IdleCheck::Broker);
    assert_eq!(
        idle_check(false, Some(now), now + BROKER_REQUEST_INTERVAL - ms(1)),
        IdleCheck::Skip
    );
    assert_eq!(
        idle_check(false, Some(now), now + BROKER_REQUEST_INTERVAL),
        IdleCheck::Broker
    );
}

#[test]
fn an_idle_partition_holds_its_watermark_one_fetch_wait_behind_the_wall_clock() {
    let now = beam::Instant::from_millis(10_000);
    assert_eq!(
        idle_watermark(now, ms(500)),
        beam::Instant::from_millis(9_500)
    );
    assert_eq!(idle_watermark(now, Duration::ZERO), now);
}

#[test]
fn an_empty_partition_reports_its_end_and_the_end_does_not_extend_the_idle_time() {
    let tracker = tracker_from(10);
    let idle = ms(200);
    let mut limits = CallLimits::new(Instant::now(), READ, idle, Duration::ZERO);
    let polls = vec![
        Polled::Nothing,
        Polled::EndOfPartition,
        Polled::Nothing,
        Polled::EndOfPartition,
    ];
    let started = Instant::now();
    let (ended, emitted) = run(&mut limits, &tracker, fake_polls(polls));
    let waited = started.elapsed();
    assert_eq!(ended.end, CallEnd::Idle);
    assert!(ended.reached_end);
    assert!(emitted.is_empty());
    assert!(waited < idle + POLL_SLICE, "{waited:?}");
    assert_eq!(park_offset(ended.end, &tracker), Some(10));
}

#[test]
fn a_call_with_records_and_no_end_event_does_not_report_the_end() {
    let tracker = tracker_from(10);
    let mut limits = CallLimits::new(Instant::now(), READ, ms(150), Duration::ZERO);
    let (ended, _) = run(&mut limits, &tracker, fake_queue(&[10, 11]));
    assert!(!ended.reached_end);
}

#[test]
fn an_empty_partition_moves_its_watermark_at_each_idle_call_after_its_end_event() {
    // The first call of a new assignment: no record and no fetch response yet.
    let t0 = Instant::now();
    assert_eq!(idle_check(false, None, t0), IdleCheck::Broker);
    // That call or the next one gets the end of the partition, so each idle call 1 s apart
    // uses the cached high watermark, also right after the broker request.
    let watermarks: Vec<beam::Instant> = (1..=3)
        .map(|n| {
            let now = t0 + IDLE * n;
            assert_eq!(idle_check(true, Some(t0), now), IdleCheck::Cached);
            idle_watermark(
                beam::Instant::from_millis(10_000 + 1_000 * i64::from(n)),
                ms(500),
            )
        })
        .collect();
    assert!(
        watermarks.windows(2).all(|pair| pair[0] < pair[1]),
        "{watermarks:?}"
    );
}

#[test]
fn a_record_at_or_before_the_claimed_offsets_fails_the_call() {
    // A truncated log or a topic created again moves the fetch position back: to an offset
    // that the call already passed, or to one before the start of the restriction.
    for (back, last) in [
        (11, "the last read offset 12"),
        (3, "the last read offset 12"),
    ] {
        let tracker = tracker_from(10);
        let mut limits = CallLimits::new(Instant::now(), READ, IDLE, Duration::ZERO);
        let mut emitted = Vec::new();
        let error = read_loop(
            SOURCE,
            &mut limits,
            &tracker,
            None,
            fake_queue(&[10, 11, 12, back]),
            |offset| *offset,
            |offset| {
                emitted.push(offset);
                Ok(())
            },
        )
        .expect_err("the call fails")
        .to_string();
        assert_eq!(emitted, [10, 11, 12]);
        let refused = format!("offset {back}");
        for part in [
            SOURCE,
            refused.as_str(),
            last,
            "truncated",
            "new start offset",
        ] {
            assert!(error.contains(part), "{part}: {error}");
        }
    }
}

#[test]
fn a_first_record_before_the_start_names_the_start_offset() {
    let tracker = tracker_from(10);
    let mut limits = CallLimits::new(Instant::now(), READ, IDLE, Duration::ZERO);
    let error = read_loop(
        SOURCE,
        &mut limits,
        &tracker,
        None,
        fake_queue(&[4]),
        |offset| *offset,
        |_| Ok(()),
    )
    .expect_err("the call fails")
    .to_string();
    assert!(error.contains("the start offset 10"), "{error}");
}

#[test]
fn a_gap_after_deleted_records_does_not_stop_the_call() {
    // Retention deleted offsets 12 to 49, so the fetch position moved forward to 50.
    let tracker = tracker_from(10);
    let mut limits = CallLimits::new(Instant::now(), READ, ms(150), Duration::ZERO);
    let (ended, emitted) = run(&mut limits, &tracker, fake_queue(&[10, 11, 50, 51]));
    assert_eq!(ended.end, CallEnd::Idle);
    assert_eq!(emitted, [10, 11, 50, 51]);
    assert!(tracker.claim_error().is_none());
    assert_eq!(park_offset(ended.end, &tracker), Some(52));
}

#[test]
fn only_a_new_assignment_counts_in_new_assignments() {
    use beam::internals::{MetricsContainer, MetricsScope};
    use beam::metrics::MetricResults;

    let container = Arc::new(MetricsContainer::new());
    let waits = {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("Read"));
        [first_wait(false), first_wait(true), first_wait(true)]
    };
    assert_eq!(waits, [FIRST_POLL_TIMEOUT, Duration::ZERO, Duration::ZERO]);
    assert_eq!(
        MetricResults::from_container(&container).counter_for_transform(
            "Read",
            CALL_METRICS,
            NEW_ASSIGNMENTS
        ),
        Some(1)
    );
}
