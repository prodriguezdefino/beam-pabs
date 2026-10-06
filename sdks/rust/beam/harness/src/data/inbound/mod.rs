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

//! Inbound routing: the runner's data and timers, queued per instruction.
//!
//! The stream reader never waits on one instruction. Each chunk goes into its bundle's queue
//! and takes room there until the bundle reads it: first in the queue's own room of
//! [`INBOUND_QUEUE_CHUNKS`], then in the stream's limit. A bundle that stops reading, for
//! example because the runner does not read its output, holds up only its own data until
//! the limit is full: [`INBOUND_QUEUE_CHUNKS`] for each open queue, plus up to as much again
//! while output waits on the runner (see [`InboundLimit::raise`]). Only then does the
//! stream wait.
//!
//! - [`routes`]: the per-instruction queues and the reader's side of them.
//! - [`limit`]: the stream's limit.

mod limit;
mod routes;

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use tracing::debug;

use model::fn_execution::{
    Elements, elements::Data as ElementData, elements::Timers as ElementTimers,
};

pub(super) use limit::InboundLimit;
pub use routes::InboundReceiver;
use routes::{Batch, Routes};

/// Chunks in an instruction's own room. Each open queue also adds this many chunks to the
/// stream's limit on chunks held beyond the queues' own room.
pub const INBOUND_QUEUE_CHUNKS: usize = 100;

/// Ended instructions remembered so later data for them is dropped, not buffered forever.
const ENDED_INSTRUCTIONS: usize = 10_000;

/// How long an instruction may stay unregistered while the stream waits for room. Then the
/// stream drops its data, and a late registration fails.
pub const UNREGISTERED_DATA_TIMEOUT: Duration = Duration::from_secs(3 * 60 * 60);

/// How often a wait for room, in the stream's limit or in one queue, is logged.
pub(super) const INBOUND_STALL_WARNING: Duration = Duration::from_secs(60);

/// Chunks for one instruction; `None` marks the end of its stream.
pub(super) type Chunks<T> = Vec<Option<T>>;

/// What the stream received, for stall reports. Updated once per message, never per chunk.
#[derive(Default)]
struct InboundStats {
    messages: u64,
    last_message: Option<Instant>,
}

/// Inbound routes hold data and timer queues per instruction, keyed by `instruction_id`.
/// Chunks reach the bundle whether the runner labels them with the source
/// transform or with the consumer.
pub struct DataChannelState {
    data: Routes<Vec<u8>>,
    timers: Routes<ElementTimers>,
    limit: Arc<InboundLimit>,
    /// Recently ended instructions, oldest first, and the same as a set.
    ended: VecDeque<String>,
    ended_set: HashSet<String>,
    /// Why the default data stream ended. Once set, no more data can arrive, so new
    /// registrations start out closed.
    closed: Option<String>,
    stats: InboundStats,
}

impl DataChannelState {
    pub(super) fn new(limit: Arc<InboundLimit>) -> Self {
        Self {
            data: Routes::default(),
            timers: Routes::default(),
            limit,
            ended: VecDeque::new(),
            ended_set: HashSet::new(),
            closed: None,
            stats: InboundStats::default(),
        }
    }

    /// Splits one inbound message into batches per instruction, dropping the chunks of
    /// instructions that have ended.
    fn dispatch(&mut self, elements: Elements) -> (Vec<Batch<Vec<u8>>>, Vec<Batch<ElementTimers>>) {
        let Self {
            data: data_routes,
            timers: timer_routes,
            limit,
            ended_set,
            stats,
            ..
        } = self;
        stats.messages += 1;
        stats.last_message = Some(Instant::now());
        let live = |id: &str| {
            let live = !ended_set.contains(id);
            if !live {
                debug!("Dropping inbound data for ended instruction '{id}'");
            }
            live
        };
        let data = elements
            .data
            .into_iter()
            .filter(|data| live(&data.instruction_id))
            .map(
                |ElementData {
                     instruction_id,
                     data,
                     is_last,
                     ..
                 }| {
                    let chunks = [
                        (!data.is_empty()).then_some(Some(data)),
                        is_last.then_some(None),
                    ];
                    data_routes.route(
                        &instruction_id,
                        chunks.into_iter().flatten().collect(),
                        limit,
                    )
                },
            )
            .collect();
        let timers = elements
            .timers
            .into_iter()
            .filter(|timer| live(&timer.instruction_id))
            .map(|timer| {
                // The message moves into its chunk, so the id is copied once per message.
                let id = timer.instruction_id.clone();
                let is_last = timer.is_last;
                let chunks = [
                    (!timer.timers.is_empty()).then_some(Some(timer)),
                    is_last.then_some(None),
                ];
                timer_routes.route(&id, chunks.into_iter().flatten().collect(), limit)
            })
            .collect();
        (data, timers)
    }

    /// Remembers that `id` ended, forgetting the oldest once [`ENDED_INSTRUCTIONS`] are kept.
    fn end(&mut self, id: &str) {
        if self.ended_set.insert(id.to_string()) {
            self.ended.push_back(id.to_string());
        }
        if self.ended.len() > ENDED_INSTRUCTIONS {
            self.ended
                .pop_front()
                .map(|oldest| self.ended_set.remove(&oldest));
        }
    }

    /// Drops the instructions that have not registered within [`UNREGISTERED_DATA_TIMEOUT`]:
    /// their data is dropped and they fail if they register later.
    fn drop_unregistered(&mut self) {
        let (data, timers) = (&self.data, &self.timers);
        let abandoned: HashSet<Arc<str>> = data
            .waiting_for(UNREGISTERED_DATA_TIMEOUT)
            .chain(timers.waiting_for(UNREGISTERED_DATA_TIMEOUT))
            .filter(|id| !data.is_registered(id) && !timers.is_registered(id))
            .cloned()
            .collect();
        abandoned.iter().for_each(|id| {
            tracing::warn!(
                "Dropping inbound data for instruction '{id}': it has not registered \
                     within {UNREGISTERED_DATA_TIMEOUT:?}"
            );
            self.data.unregister(id, &self.limit);
            self.timers.unregister(id, &self.limit);
            self.end(id);
        });
    }

    /// Whether the stream is up and `id` has not ended or been dropped.
    fn accepts(&self, id: &str) -> bool {
        self.closed.is_none() && !self.ended_set.contains(id)
    }

    /// Takes `id`'s data queue, with whatever arrived early.
    pub(super) fn register_data(&mut self, id: &str) -> InboundReceiver<Vec<u8>> {
        let open = self.accepts(id);
        self.data.register(id, open, &self.limit)
    }

    /// Takes `id`'s timer queue, with whatever arrived early.
    pub(super) fn register_timers(&mut self, id: &str) -> InboundReceiver<ElementTimers> {
        let open = self.accepts(id);
        self.timers.register(id, open, &self.limit)
    }

    pub(super) fn unregister_timers(&mut self, id: &str) {
        self.timers.unregister(id, &self.limit);
    }

    /// Drops `id`'s data queue; anything the runner still sends it is dropped from then on.
    pub(super) fn unregister_data(&mut self, id: &str) {
        self.data.unregister(id, &self.limit);
        self.end(id);
    }

    /// Describes the stream and the queues, for reports about a bundle that waits for input.
    /// Computed on demand: the element path keeps no per-chunk statistics.
    pub fn describe(&self, id: &str) -> String {
        format!(
            "stream: {} messages, last {}; queues: [{}]; {}; closed: {:?}",
            self.stats.messages,
            self.stats.last_message.map_or_else(
                || "never".to_string(),
                |t| format!("{}s ago", t.elapsed().as_secs())
            ),
            self.data.report(id),
            self.limit,
            self.closed
        )
    }

    /// Fails every waiting bundle after a data stream ends. Closing their queues before
    /// `is_last` makes the bundles report
    /// [`DataError::StreamEnded`](super::DataError::StreamEnded).
    pub fn fail_all(&mut self, reason: &str) {
        let waiting = self.data.clear(&self.limit) + self.timers.clear(&self.limit);
        if waiting > 0 {
            tracing::warn!("Failing {waiting} inbound channel(s): {reason}");
        }
    }

    /// Fails every waiting bundle and every later one: the default data stream is gone.
    pub fn close(&mut self, reason: String) {
        self.fail_all(&reason);
        self.closed = Some(reason);
    }
}

/// Locks the routing state. A panic of another holder does not stop the data stream.
pub fn lock_state(state: &Mutex<DataChannelState>) -> MutexGuard<'_, DataChannelState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Routes one inbound message to the bundles it is for. The routing lock is released before
/// sending, and the send waits only for room in the stream's limit.
pub(super) async fn deliver(state: &Arc<Mutex<DataChannelState>>, elements: Elements) {
    let (data, timers) = lock_state(state).dispatch(elements);
    // While the stream waits, an instruction that never registers must not hold the room.
    let stalled = || lock_state(state).drop_unregistered();
    send_all(data, stalled).await;
    send_all(timers, stalled).await;
}

/// Sends each batch to its bundle's queue, in order.
async fn send_all<T>(batches: Vec<Batch<T>>, stalled: impl Fn() + Copy) {
    for batch in batches {
        batch.send(stalled).await;
    }
}
