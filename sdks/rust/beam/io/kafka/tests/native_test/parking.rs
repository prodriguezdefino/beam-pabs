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

//! The assignments that the reader parks between two calls of a partition, and the leases
//! that keep two assignments of one partition from being alive at the same time.

use std::convert::Infallible;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use beam::transforms::sdf::RestrictionTracker;
use beam::values::IsBounded;
use kafka_io::native::read::call::CallEnd;
use kafka_io::native::read::parking::{Parking, StillHeld};
use kafka_io::native::read::restriction::{PartitionRestriction, PartitionTracker};

const EXPIRY: Duration = Duration::from_secs(15);

/// A parked value that counts its drops, as an assignment unassigns its partition.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counted() -> (Arc<AtomicUsize>, impl Fn() -> Counted) {
    let drops = Arc::new(AtomicUsize::new(0));
    let make = {
        let drops = Arc::clone(&drops);
        move || Counted(Arc::clone(&drops))
    };
    (drops, make)
}

/// Parks `value` of `partition` with its next record at `next_offset`, as a call that
/// assigns the partition and resumes.
fn park<T>(parking: &Parking<i32, T>, partition: i32, next_offset: i64, value: T, now: Instant) {
    let mut lease = parking
        .take(partition, next_offset, now)
        .expect("no call holds the partition");
    let mut value = Some(value);
    let _value = lease
        .get_or_try_insert_with(|| Ok::<_, Infallible>(value.take().expect("one value")))
        .expect("the value is set");
    assert!(lease.park(next_offset, now));
}

/// Whether a take of `partition` from `start` gets a parked value. The lease ends at once.
fn takes_value<T>(parking: &Parking<i32, T>, partition: i32, start: i64, now: Instant) -> bool {
    parking
        .take(partition, start, now)
        .expect("no call holds the partition")
        .value()
        .is_some()
}

#[test]
fn a_call_that_starts_at_the_next_offset_takes_the_parked_value() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let now = Instant::now();
    park(&parking, 3, 100, make(), now);
    assert!(takes_value(&parking, 3, 100, now));
    assert!(parking.is_empty());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a_call_that_starts_at_another_offset_drops_the_parked_value() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let now = Instant::now();
    park(&parking, 3, 100, make(), now);
    // A retried bundle starts earlier, and a split restriction can start later.
    let lease = parking
        .take(3, 90, now)
        .expect("no call holds the partition");
    assert!(lease.value().is_none());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "dropped before the take returns"
    );
    drop(lease);
    park(&parking, 3, 100, make(), now);
    assert!(!takes_value(&parking, 3, 110, now));
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(parking.is_empty());
}

#[test]
fn a_value_of_another_partition_is_not_taken() {
    let (_, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let now = Instant::now();
    park(&parking, 3, 100, make(), now);
    assert!(!takes_value(&parking, 4, 100, now));
    assert_eq!(parking.len(), 1);
}

#[test]
fn back_to_back_calls_of_a_partition_never_share_a_value() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let now = Instant::now();
    park(&parking, 3, 100, make(), now);
    let first = parking.take(3, 100, now).expect("the first call takes");
    assert!(first.value().is_some());
    assert!(parking.is_empty());
    assert!(first.park(250, now));
    let second = parking.take(3, 250, now).expect("the second call takes");
    assert!(second.value().is_some());
    drop(second);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a_take_of_a_held_partition_waits_until_the_holder_parks() {
    let (drops, make) = counted();
    let parking: Arc<Parking<i32, Counted>> = Arc::new(Parking::new(EXPIRY));
    let now = Instant::now();
    park(&parking, 3, 100, make(), now);
    let first = parking.take(3, 100, now).expect("the first call takes");
    let second = thread::spawn({
        let parking = Arc::clone(&parking);
        move || takes_value(&parking, 3, 180, now)
    });
    thread::sleep(Duration::from_millis(50));
    assert!(!second.is_finished(), "the second call waits for the first");
    assert!(first.park(180, now));
    assert!(second.join().expect("the second call ends"));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a_take_of_a_held_partition_fails_after_the_wait_time() {
    let (_, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY).with_wait(Duration::from_millis(30));
    let now = Instant::now();
    park(&parking, 3, 100, make(), now);
    let _first = parking.take(3, 100, now).expect("the first call takes");
    assert_eq!(parking.take(3, 100, now).err(), Some(StillHeld));
}

#[test]
fn a_lease_without_a_parked_value_still_holds_its_partition() {
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY).with_wait(Duration::from_millis(30));
    let now = Instant::now();
    let first = parking
        .take(3, 100, now)
        .expect("nothing holds the partition");
    assert!(first.value().is_none());
    assert_eq!(parking.take(3, 100, now).err(), Some(StillHeld));
    drop(first);
    assert!(parking.take(3, 100, now).is_ok());
}

#[test]
fn expired_values_are_dropped_at_the_next_park_or_take() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let start = Instant::now();
    park(&parking, 1, 10, make(), start);
    park(&parking, 2, 20, make(), start);
    let late = start + EXPIRY + Duration::from_millis(1);
    assert!(!takes_value(&parking, 1, 10, late));
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(parking.is_empty());

    park(&parking, 1, 10, make(), start);
    park(&parking, 5, 50, make(), late);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    assert_eq!(parking.len(), 1);
}

#[test]
fn a_sweep_drops_the_expired_values_without_a_park_or_take() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let start = Instant::now();
    park(&parking, 1, 10, make(), start);
    park(&parking, 2, 20, make(), start + EXPIRY);
    parking.sweep(start + EXPIRY + Duration::from_millis(1));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(parking.len(), 1);
}

/// A parked value that counts its drop, or panics in its drop when it has no counter.
struct Fragile(Option<Counted>);

impl Drop for Fragile {
    fn drop(&mut self) {
        if self.0.is_none() {
            panic!("the unassign panics");
        }
    }
}

#[test]
fn a_drop_that_panics_still_releases_every_expired_partition() {
    let (drops, make) = counted();
    let parking: Parking<i32, Fragile> = Parking::new(EXPIRY).with_wait(Duration::from_millis(30));
    let start = Instant::now();
    park(&parking, 1, 10, Fragile(None), start);
    park(&parking, 2, 20, Fragile(Some(make())), start);
    let late = start + EXPIRY + Duration::from_millis(1);
    let swept = panic::catch_unwind(AssertUnwindSafe(|| parking.sweep(late)));
    assert!(swept.is_err());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the other value also drops"
    );
    assert!(parking.take(1, 10, late).is_ok());
    assert!(parking.take(2, 20, late).is_ok());
}

#[test]
fn a_lease_whose_value_panics_on_drop_still_releases_its_partition() {
    let parking: Parking<i32, Fragile> = Parking::new(EXPIRY).with_wait(Duration::from_millis(30));
    let now = Instant::now();
    let ended = panic::catch_unwind(AssertUnwindSafe(|| {
        let mut lease = parking
            .take(3, 100, now)
            .expect("nothing holds the partition");
        let _value = lease.get_or_try_insert_with(|| Ok::<_, Infallible>(Fragile(None)));
        drop(lease);
    }));
    assert!(ended.is_err());
    assert!(parking.take(3, 100, now).is_ok());
}

#[test]
fn two_reads_hold_the_same_partition_apart() {
    let parking: Parking<(u64, i32), Counted> =
        Parking::new(EXPIRY).with_wait(Duration::from_millis(30));
    let now = Instant::now();
    let _first = parking
        .take((1, 3), 100, now)
        .expect("the first read takes");
    assert!(
        parking.take((2, 3), 100, now).is_ok(),
        "another read is not held"
    );
    assert_eq!(parking.take((1, 3), 100, now).err(), Some(StillHeld));
}

/// A parked value whose drop takes time, as an unassign that waits for the consumer. It
/// records when its drop starts and ends.
struct SlowDrop(Arc<Mutex<Vec<&'static str>>>);

impl Drop for SlowDrop {
    fn drop(&mut self) {
        self.0.lock().expect("events").push("drop started");
        thread::sleep(Duration::from_millis(100));
        self.0.lock().expect("events").push("drop ended");
    }
}

#[test]
fn a_call_assigns_only_after_a_sweep_has_dropped_the_expired_value() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let parking: Arc<Parking<i32, SlowDrop>> = Arc::new(Parking::new(EXPIRY));
    let start = Instant::now();
    park(&parking, 3, 100, SlowDrop(Arc::clone(&events)), start);
    let late = start + EXPIRY + Duration::from_millis(1);
    let sweep = thread::spawn({
        let parking = Arc::clone(&parking);
        move || parking.sweep(late)
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while events.lock().expect("events").is_empty() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    // The next call of the partition comes while the sweep unassigns it.
    let lease = parking.take(3, 100, late).expect("the sweep ends in time");
    events.lock().expect("events").push("taken");
    assert!(lease.value().is_none());
    sweep.join().expect("the sweep ends");
    assert_eq!(
        *events.lock().expect("events"),
        ["drop started", "drop ended", "taken"]
    );
}

#[test]
fn a_call_assigns_only_after_another_partition_take_has_dropped_the_expired_value() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let parking: Arc<Parking<i32, SlowDrop>> = Arc::new(Parking::new(EXPIRY));
    let start = Instant::now();
    park(&parking, 3, 100, SlowDrop(Arc::clone(&events)), start);
    let late = start + EXPIRY + Duration::from_millis(1);
    let other = thread::spawn({
        let parking = Arc::clone(&parking);
        move || takes_value(&parking, 7, 0, late)
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while events.lock().expect("events").is_empty() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    let lease = parking.take(3, 100, late).expect("the drop ends in time");
    events.lock().expect("events").push("taken");
    assert!(lease.value().is_none());
    assert!(!other.join().expect("the other call ends"));
    assert_eq!(
        *events.lock().expect("events"),
        ["drop started", "drop ended", "taken"]
    );
}

/// The tracker of a reader call whose restriction starts at `start`.
fn call_from(start: i64) -> PartitionTracker {
    PartitionTracker::new(
        PartitionRestriction::from_offset(start),
        IsBounded::Unbounded,
    )
}

/// A lease of partition 3 from `start` that holds a new value, as a call that assigned.
fn assigned<'a>(
    parking: &'a Parking<i32, Counted>,
    start: i64,
    value: Counted,
) -> kafka_io::native::read::parking::Lease<'a, i32, Counted> {
    let mut lease = parking
        .take(3, start, Instant::now())
        .expect("nothing holds partition 3");
    let mut value = Some(value);
    let _value = lease
        .get_or_try_insert_with(|| Ok::<_, Infallible>(value.take().expect("one value")))
        .expect("the value is set");
    lease
}

#[test]
fn a_call_that_resumes_parks_its_value_at_the_start_of_its_residual() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let now = Instant::now();
    let call = call_from(10);
    assert!(call.try_claim(&10) && call.try_claim(&14));
    let lease = assigned(&parking, 10, make());
    assert!(lease.end_call(CallEnd::MaxReadTime, &call, now));
    let residual = call.try_checkpoint().expect("work is left");
    assert_eq!(residual.start, 15);
    let next = call_from(residual.start);
    assert!(takes_value(
        &parking,
        3,
        next.current_restriction().start,
        now
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a_call_that_claims_nothing_parks_its_value_at_its_start() {
    let (_, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let now = Instant::now();
    let call = call_from(10);
    let lease = assigned(&parking, 10, make());
    assert!(lease.end_call(CallEnd::MaxReadTime, &call, now));
    let residual = call.try_checkpoint().expect("work is left");
    assert_eq!(residual.start, 10);
    assert!(takes_value(&parking, 3, residual.start, now));
}

#[test]
fn a_call_that_stops_drops_its_value_and_releases_its_partition() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY).with_wait(Duration::ZERO);
    let call = call_from(10);
    assert!(call.try_claim(&10));
    let lease = assigned(&parking, 10, make());
    assert!(!lease.end_call(CallEnd::EndOfRestriction, &call, Instant::now()));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(parking.is_empty());
    assert!(parking.take(3, 11, Instant::now()).is_ok());
}

#[test]
fn a_call_that_the_runner_splits_parks_its_value_at_the_start_of_the_residual() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let now = Instant::now();
    let call = call_from(10);
    assert!(call.try_claim(&10) && call.try_claim(&14));
    let (_, residual) = call.try_split(0.0).expect("the runner checkpoints");
    assert_eq!(residual.start, 15);
    // The record after the split is refused, and its offset counts as attempted.
    assert!(!call.try_claim(&17));
    assert_eq!(call.next_offset(), 18);
    let lease = assigned(&parking, 10, make());
    assert!(lease.end_call(CallEnd::Split, &call, now));
    assert!(takes_value(&parking, 3, residual.start, now));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a_value_parked_after_a_split_expires_when_no_call_takes_it() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let now = Instant::now();
    let call = call_from(10);
    assert!(call.try_claim(&10));
    assert!(call.try_split(0.0).is_some());
    let lease = assigned(&parking, 10, make());
    assert!(lease.end_call(CallEnd::Split, &call, now));
    // The residual runs in another process, so no call of this process takes the value.
    parking.sweep(now + EXPIRY + Duration::from_secs(1));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(parking.is_empty());
}

#[test]
fn a_call_with_no_residual_drops_its_value() {
    for end in [CallEnd::EndOfRestriction, CallEnd::Stopped] {
        let (drops, make) = counted();
        let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
        let call = call_from(10);
        assert!(call.try_claim(&10));
        let lease = assigned(&parking, 10, make());
        assert!(!lease.end_call(end, &call, Instant::now()), "{end:?}");
        assert_eq!(drops.load(Ordering::SeqCst), 1, "{end:?}");
        assert!(parking.is_empty(), "{end:?}");
    }
}

#[test]
fn a_call_that_fails_drops_its_value_and_releases_its_partition() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY).with_wait(Duration::ZERO);
    drop(assigned(&parking, 10, make()));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(parking.take(3, 10, Instant::now()).is_ok());
}

#[test]
fn a_retry_of_a_bundle_drops_the_parked_value() {
    let (drops, make) = counted();
    let parking: Parking<i32, Counted> = Parking::new(EXPIRY);
    let now = Instant::now();
    let call = call_from(10);
    assert!(call.try_claim(&10) && call.try_claim(&14));
    let lease = assigned(&parking, 10, make());
    assert!(lease.end_call(CallEnd::MaxReadTime, &call, now));
    // The runner rejects the bundle and retries it from the same restriction.
    let retry = call_from(10);
    assert!(!takes_value(
        &parking,
        3,
        retry.current_restriction().start,
        now
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(parking.is_empty());
}

/// A short expiry, so the background thread sweeps several times during the test.
const SHORT_EXPIRY: Duration = Duration::from_millis(20);

static BACKGROUND: LazyLock<Parking<i32, Counted>> = LazyLock::new(|| Parking::new(SHORT_EXPIRY));

/// Waits up to 10 s for `done`.
fn eventually(done: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    done()
}

#[test]
fn the_background_thread_drops_expired_values_and_stops_when_none_is_parked() {
    let (drops, make) = counted();
    park(&BACKGROUND, 1, 10, make(), Instant::now());
    BACKGROUND.sweep_in_background();
    assert!(BACKGROUND.is_sweeping());
    assert!(eventually(|| drops.load(Ordering::SeqCst) == 1));
    assert!(eventually(|| !BACKGROUND.is_sweeping()));
    assert!(BACKGROUND.is_empty());

    park(&BACKGROUND, 2, 20, make(), Instant::now());
    BACKGROUND.sweep_in_background();
    assert!(eventually(|| drops.load(Ordering::SeqCst) == 2));
    assert!(eventually(|| !BACKGROUND.is_sweeping()));
}
