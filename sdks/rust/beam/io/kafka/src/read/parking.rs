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

//! The partition assignments that a reader keeps between two calls for the same partition.
//!
//! A reader call reads a partition for a few seconds. Then the runner starts a new call from
//! the next offset. An unassign drops the records that the consumer fetched, so a call parks
//! its assignment with the offset of the next record in its queue. The next call takes the
//! assignment only if its restriction starts at that offset, so it never reads a record
//! outside its restriction. Otherwise the parked assignment drops, which unassigns the
//! partition.
//!
//! A call holds its partition through a [`Lease`] from [`Parking::take`] until it parks or
//! drops its value. A parked value that expires or does not match is also held until its
//! drop is complete. A take of a held partition waits for at most 1 second, and then fails.
//! So a call assigns a partition only when no other assignment of it is alive.
//!
//! The parked assignments of a read serve the whole process, as its consumers do. So the
//! next call of a partition can run in any DoFn copy.
//!
//! When a worker gets no more calls for a partition, its parked assignment keeps the consumer
//! fetching. So a background thread drops the expired values while values are parked.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use tracing::warn;

use super::call::{CallEnd, park_offset};
use super::restriction::PartitionTracker;

/// The default longest time that a take waits for a held partition. An unassign in progress
/// ends in milliseconds. A partition that another call holds for longer fails the take, so a
/// reader call never blocks its thread for long.
const DEFAULT_WAIT: Duration = Duration::from_secs(1);

/// A value that waits for the next call of its partition.
struct Parked<T> {
    value: T,
    /// The offset of the next record that the value gives.
    next_offset: i64,
    since: Instant,
}

/// The state of one partition key.
enum Slot<T> {
    /// A value waits for the next call.
    Parked(Parked<T>),
    /// A call holds the partition, or a value of it is being dropped.
    Held,
}

/// A take that waited for the whole wait time while another holder kept the partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StillHeld;

impl fmt::Display for StillHeld {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("another reader call of the partition did not end in time")
    }
}

/// The parked values, at most one for each partition key `K`, and the partitions that calls
/// hold. A call takes its value out, so two calls never share it.
pub struct Parking<K, T> {
    slots: Mutex<HashMap<K, Slot<T>>>,
    /// Signals the release of a partition.
    released: Condvar,
    /// A value that waits longer than this is dropped at the next park, take or sweep.
    expiry: Duration,
    /// The longest time that a take waits for a held partition.
    wait: Duration,
    /// Whether the background thread of [`Parking::sweep_in_background`] runs.
    sweeping: AtomicBool,
}

/// Releases a held partition when it drops, also during a panic of the drop before it.
struct Hold<'a, K: Eq + Hash + Clone, T> {
    parking: &'a Parking<K, T>,
    /// `None` after the partition is parked.
    partition: Option<K>,
}

impl<K: Eq + Hash + Clone, T> Drop for Hold<'_, K, T> {
    fn drop(&mut self) {
        if let Some(partition) = self.partition.take() {
            self.parking.release(partition);
        }
    }
}

/// The hold of one call on its partition, with the value that the call reads with. Dropping
/// the lease drops the value first and then releases the partition, so the next call
/// assigns it only after the unassign.
pub struct Lease<'a, K: Eq + Hash + Clone, T> {
    // The fields drop in this order: the value, then the hold.
    value: Option<T>,
    hold: Hold<'a, K, T>,
}

impl<K: Eq + Hash + Clone, T> Parking<K, T> {
    /// An empty parking whose values expire after `expiry`. A take waits at most 1 second
    /// for a held partition.
    pub fn new(expiry: Duration) -> Self {
        Self {
            slots: Mutex::default(),
            released: Condvar::new(),
            expiry,
            wait: DEFAULT_WAIT,
            sweeping: AtomicBool::new(false),
        }
    }

    /// Sets the longest time that a take waits for a held partition.
    pub fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// Holds `partition` for a call whose restriction starts at `start`. The lease has the
    /// parked value if its next record is at `start`. A parked value with another next
    /// offset is dropped before the take returns. Also drops the values that expired.
    ///
    /// # Errors
    ///
    /// Fails if another lease or a drop holds the partition for longer than the wait time.
    pub fn take(
        &self,
        partition: K,
        start: i64,
        now: Instant,
    ) -> Result<Lease<'_, K, T>, StillHeld> {
        let deadline = Instant::now() + self.wait;
        let (previous, expired) = {
            let mut slots = self.lock();
            while matches!(slots.get(&partition), Some(Slot::Held)) {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(StillHeld);
                }
                slots = self
                    .released
                    .wait_timeout(slots, left)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
            let previous = slots.insert(partition.clone(), Slot::Held);
            (previous, self.hold_expired(&mut slots, now))
        };
        // The hold exists before any drop, so a drop that panics still releases the partition.
        let hold = Hold {
            parking: self,
            partition: Some(partition),
        };
        self.drop_held(expired);
        let value = match previous {
            Some(Slot::Parked(parked))
                if parked.next_offset == start && !self.expired(&parked, now) =>
            {
                Some(parked.value)
            }
            // The lease holds the partition until this drop has unassigned it.
            Some(Slot::Parked(parked)) => {
                drop(parked);
                None
            }
            Some(Slot::Held) | None => None,
        };
        Ok(Lease { value, hold })
    }

    /// The number of parked values.
    pub fn len(&self) -> usize {
        self.lock()
            .values()
            .filter(|slot| matches!(slot, Slot::Parked(_)))
            .count()
    }

    /// Whether no value is parked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the background thread of [`Parking::sweep_in_background`] runs.
    pub fn is_sweeping(&self) -> bool {
        self.sweeping.load(Ordering::Acquire)
    }

    /// Drops the values that expired at `now`.
    pub fn sweep(&self, now: Instant) {
        let expired = self.hold_expired(&mut self.lock(), now);
        self.drop_held(expired);
    }

    /// Parks `parked` for the lease that holds `partition`, and drops the values that
    /// expired.
    fn park(&self, partition: K, parked: Parked<T>) {
        let now = parked.since;
        let (previous, expired) = {
            let mut slots = self.lock();
            let previous = slots.insert(partition, Slot::Parked(parked));
            (previous, self.hold_expired(&mut slots, now))
        };
        self.released.notify_all();
        // The lease held the partition, so this is `Held`. A value would unassign, so it
        // drops after the unlock.
        drop(previous);
        self.drop_held(expired);
    }

    /// Ends the hold on `partition`.
    fn release(&self, partition: K) {
        if let Entry::Occupied(slot) = self.lock().entry(partition)
            && matches!(slot.get(), Slot::Held)
        {
            slot.remove();
        }
        self.released.notify_all();
    }

    /// Marks the partitions of the expired values as held and returns the values. Drop them
    /// with [`Parking::drop_held`] after the lock is released, because dropping an assignment
    /// waits for the consumer.
    fn hold_expired(&self, slots: &mut HashMap<K, Slot<T>>, now: Instant) -> Vec<(K, T)> {
        let expired: Vec<K> = slots
            .iter()
            .filter(|(_, slot)| matches!(slot, Slot::Parked(parked) if self.expired(parked, now)))
            .map(|(partition, _)| partition.clone())
            .collect();
        expired
            .into_iter()
            .filter_map(
                |partition| match slots.insert(partition.clone(), Slot::Held) {
                    Some(Slot::Parked(parked)) => Some((partition, parked.value)),
                    Some(Slot::Held) | None => None,
                },
            )
            .collect()
    }

    /// Drops each value and then releases its partition. A pair drops its value first, then
    /// its hold, and the hold also releases the partition when the value drop panics.
    fn drop_held(&self, held: Vec<(K, T)>) {
        let pairs: Vec<(T, Hold<'_, K, T>)> = held
            .into_iter()
            .map(|(partition, value)| {
                let hold = Hold {
                    parking: self,
                    partition: Some(partition),
                };
                (value, hold)
            })
            .collect();
        drop(pairs);
    }

    fn expired(&self, parked: &Parked<T>, now: Instant) -> bool {
        now.saturating_duration_since(parked.since) > self.expiry
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<K, Slot<T>>> {
        // The map stays consistent when a holder panics: each change is one insert or remove.
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<K: Eq + Hash + Clone + Send + 'static, T: Send + 'static> Parking<K, T> {
    /// Starts a thread that drops the expired values once in each expiry time, unless one runs.
    ///
    /// The thread stops when no value is parked, so an idle process has no thread. The next
    /// park starts it again. A value stays at most two expiry times after its park.
    pub fn sweep_in_background(&'static self) {
        if self.sweeping.swap(true, Ordering::AcqRel) {
            return;
        }
        let spawned = thread::Builder::new()
            .name("kafka-parking-sweep".to_string())
            .spawn(move || self.run_sweeps());
        if let Err(e) = spawned {
            self.sweeping.store(false, Ordering::Release);
            warn!("Cannot start the thread that drops parked Kafka assignments: {e}");
        }
    }

    fn run_sweeps(&self) {
        loop {
            thread::sleep(self.expiry);
            self.sweep(Instant::now());
            if !self.is_empty() {
                continue;
            }
            self.sweeping.store(false, Ordering::Release);
            // A park between the check and the store found the flag set and started no
            // thread. So this thread continues if a value arrived and no other thread runs.
            let restart = !self.is_empty() && !self.sweeping.swap(true, Ordering::AcqRel);
            if !restart {
                return;
            }
        }
    }
}

impl<K: Eq + Hash + Clone, T> Lease<'_, K, T> {
    /// The parked value that the call took, if its next record is at the start of the call.
    pub fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// The value of the lease, or the value of `make` when the lease has none.
    ///
    /// # Errors
    ///
    /// Fails if `make` fails. The lease then still holds the partition until it drops.
    pub fn get_or_try_insert_with<E>(
        &mut self,
        make: impl FnOnce() -> Result<T, E>,
    ) -> Result<&mut T, E> {
        let value = match self.value.take() {
            Some(value) => value,
            None => make()?,
        };
        Ok(self.value.insert(value))
    }

    /// Parks the value, whose next record is at `next_offset`, for the next call. Returns
    /// whether a value is parked. Without a value, the lease releases the partition.
    pub fn park(self, next_offset: i64, now: Instant) -> bool {
        let Self { value, mut hold } = self;
        match (value, hold.partition.take()) {
            (Some(value), Some(partition)) => {
                let parked = Parked {
                    value,
                    next_offset,
                    since: now,
                };
                hold.parking.park(partition, parked);
                true
            }
            (value, partition) => {
                hold.partition = partition;
                // The unassign comes before the release, so the next holder can assign again.
                drop(value);
                false
            }
        }
    }

    /// Ends a call that read with `tracker` and ended for `end`. A call with a residual parks
    /// the value at the start of the residual, see [`park_offset`]. A call with no residual
    /// drops the value. Returns whether a value is parked.
    pub fn end_call(self, end: CallEnd, tracker: &PartitionTracker, now: Instant) -> bool {
        match park_offset(end, tracker) {
            Some(next_offset) => self.park(next_offset, now),
            None => {
                drop(self);
                false
            }
        }
    }
}
