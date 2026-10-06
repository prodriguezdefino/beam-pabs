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

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tracing::debug;

use model::fn_execution::{Elements, elements::Timers as ElementTimers};

/// Chunks an instruction can queue before its stream waits for it. This bounds memory, at
/// the cost of one slow bundle pausing its stream.
pub const INBOUND_QUEUE_CHUNKS: usize = 100;

/// Ended instructions remembered so later data for them is dropped, not buffered forever.
const ENDED_INSTRUCTIONS: usize = 10_000;

/// How long a full queue may wait for its instruction to register before the stream gives
/// up on it: its data is dropped and, should it register after all, it fails.
const UNCLAIMED_DATA_TIMEOUT: Duration = Duration::from_secs(3 * 60 * 60);

/// Chunks for one instruction; `None` marks the end of its stream.
type Chunks<T> = Vec<Option<T>>;

/// Chunks bound for one instruction's queue, sent once the routing lock is released.
struct Batch<T> {
    id: String,
    tx: mpsc::Sender<Option<T>>,
    chunks: Chunks<T>,
    /// Whether the instruction had registered when the chunks arrived.
    claimed: bool,
}

/// One instruction's queue. The receiver stays here until the instruction registers.
struct Route<T> {
    tx: mpsc::Sender<Option<T>>,
    unclaimed: Option<mpsc::Receiver<Option<T>>>,
}

/// Per-instruction inbound queues for one kind of payload. Data that arrives before its
/// instruction registers goes into the same bounded queue, so early data is bounded and
/// never dropped.
struct Routes<T> {
    routes: HashMap<String, Route<T>>,
}

impl<T> Default for Routes<T> {
    fn default() -> Self {
        Self {
            routes: HashMap::new(),
        }
    }
}

impl<T> Routes<T> {
    /// Addresses `chunks` to `id`'s queue, creating it if `id` has not registered yet.
    fn route(&mut self, id: String, chunks: Chunks<T>) -> Batch<T> {
        let route = self.routes.entry(id.clone()).or_insert_with(|| {
            debug!("Queueing early-arriving inbound data for instruction '{id}'");
            let (tx, rx) = mpsc::channel(INBOUND_QUEUE_CHUNKS);
            Route {
                tx,
                unclaimed: Some(rx),
            }
        });
        Batch {
            claimed: route.unclaimed.is_none(),
            tx: route.tx.clone(),
            id,
            chunks,
        }
    }

    /// Claims `id`'s queue with whatever arrived early. If `open` is false, the queue closes
    /// once the early chunks are read.
    fn register(&mut self, id: &str, open: bool) -> mpsc::Receiver<Option<T>> {
        let (tx, rx) = match self.routes.remove(id) {
            Some(Route {
                tx,
                unclaimed: Some(rx),
            }) => (tx, rx),
            _ => mpsc::channel(INBOUND_QUEUE_CHUNKS),
        };
        if open {
            self.routes.insert(
                id.to_string(),
                Route {
                    tx,
                    unclaimed: None,
                },
            );
        }
        rx
    }

    fn claimed(&self, id: &str) -> bool {
        self.routes
            .get(id)
            .is_some_and(|route| route.unclaimed.is_none())
    }

    fn unregister(&mut self, id: &str) {
        self.routes.remove(id);
    }

    /// Drops every queue, returning how many there were.
    fn clear(&mut self) -> usize {
        let open = self.routes.len();
        self.routes.clear();
        open
    }
}

/// Sends each batch in order, waiting while a queue is full and skipping finished bundles.
/// An instruction whose queue stays full and unclaimed for [`UNCLAIMED_DATA_TIMEOUT`] is
/// abandoned.
async fn send_all<T>(state: &Mutex<DataChannelState>, batches: Vec<Batch<T>>) {
    for Batch {
        id,
        tx,
        chunks,
        mut claimed,
    } in batches
    {
        for chunk in chunks {
            // Waits for room before taking the chunk, so a timeout loses nothing.
            let permit = if claimed {
                tx.reserve().await
            } else {
                match tokio::time::timeout(UNCLAIMED_DATA_TIMEOUT, tx.reserve()).await {
                    Ok(permit) => permit,
                    Err(_) => {
                        // `claimed` may be stale: an instruction that registered meanwhile
                        // still gets its chunk.
                        if state.lock().await.abandon(&id) {
                            break;
                        }
                        claimed = true;
                        tx.reserve().await
                    }
                }
            };
            let Ok(permit) = permit else {
                break;
            };
            permit.send(chunk);
        }
    }
}

/// Inbound routing: data and timer queues per instruction, keyed by `instruction_id`
/// alone so that chunks reach the bundle whether the runner labels them with the source
/// transform or with the consumer.
#[derive(Default)]
pub struct DataChannelState {
    data: Routes<Vec<u8>>,
    timers: Routes<ElementTimers>,
    /// Recently ended instructions, oldest first, and the same as a set.
    ended: VecDeque<String>,
    ended_set: HashSet<String>,
    /// Why the default data stream ended. Once set, no more data can arrive, so new
    /// registrations start out closed.
    closed: Option<String>,
}

impl DataChannelState {
    /// Splits one inbound message into batches per instruction, dropping the chunks of
    /// instructions that have ended.
    fn dispatch(&mut self, elements: Elements) -> (Vec<Batch<Vec<u8>>>, Vec<Batch<ElementTimers>>) {
        let Self {
            data: data_routes,
            timers: timer_routes,
            ended_set,
            ..
        } = self;
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
            .map(|data| {
                let chunks = [
                    (!data.data.is_empty()).then_some(Some(data.data)),
                    data.is_last.then_some(None),
                ];
                data_routes.route(data.instruction_id, chunks.into_iter().flatten().collect())
            })
            .collect();
        let timers = elements
            .timers
            .into_iter()
            .filter(|timer| live(&timer.instruction_id))
            .map(|timer| {
                let id = timer.instruction_id.clone();
                let is_last = timer.is_last;
                let chunks = [
                    (!timer.timers.is_empty()).then_some(Some(timer)),
                    is_last.then_some(None),
                ];
                timer_routes.route(id, chunks.into_iter().flatten().collect())
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

    /// Abandons an unregistered instruction whose queue stayed full: its data is dropped and
    /// it fails if it registers later. Returns false if it registered by now.
    fn abandon(&mut self, id: &str) -> bool {
        if self.data.claimed(id) || self.timers.claimed(id) {
            return false;
        }
        tracing::warn!(
            "Dropping inbound data for instruction '{id}': it has not registered within \
             {UNCLAIMED_DATA_TIMEOUT:?}"
        );
        self.data.unregister(id);
        self.timers.unregister(id);
        self.end(id);
        true
    }

    /// Whether the stream is up and `id` has not ended or been abandoned.
    fn accepts(&self, id: &str) -> bool {
        self.closed.is_none() && !self.ended_set.contains(id)
    }

    /// Claims `id`'s data queue, with whatever arrived early.
    pub(super) fn register_data(&mut self, id: &str) -> mpsc::Receiver<Option<Vec<u8>>> {
        let open = self.accepts(id);
        self.data.register(id, open)
    }

    /// Claims `id`'s timer queue, with whatever arrived early.
    pub(super) fn register_timers(&mut self, id: &str) -> mpsc::Receiver<Option<ElementTimers>> {
        let open = self.accepts(id);
        self.timers.register(id, open)
    }

    pub(super) fn unregister_timers(&mut self, id: &str) {
        self.timers.unregister(id);
    }

    /// Drops `id`'s data queue; anything the runner still sends it is dropped from then on.
    pub(super) fn unregister_data(&mut self, id: &str) {
        self.data.unregister(id);
        self.end(id);
    }

    /// Fails every waiting bundle after a data stream ends. Dropping their senders closes their
    /// receivers before `is_last`, and the bundles report
    /// [`DataError::StreamEnded`](super::DataError::StreamEnded).
    pub fn fail_all(&mut self, reason: &str) {
        let waiting = self.data.clear() + self.timers.clear();
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

/// Routes one inbound message to the bundles it is for. The routing lock is released before
/// sending, so a full queue holds up this stream but never the bundles registering.
pub async fn deliver(state: &Mutex<DataChannelState>, elements: Elements) {
    let (data, timers) = state.lock().await.dispatch(elements);
    send_all(state, data).await;
    send_all(state, timers).await;
}
