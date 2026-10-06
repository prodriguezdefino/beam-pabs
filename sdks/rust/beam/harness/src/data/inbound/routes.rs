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

//! Per-instruction queues, and the batches the stream reader sends to them.

use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::Instant;
use tracing::debug;

use super::limit::InboundLimit;
use super::{Chunks, INBOUND_QUEUE_CHUNKS};

/// Where a queued chunk has its room.
enum Room {
    /// In the queue's own [`INBOUND_QUEUE_CHUNKS`].
    Own,
    /// In the stream's limit, shared by all queues.
    Shared,
}

/// A chunk in a queue, with where it has its room.
type Held<T> = (Option<T>, Room);

/// The room of one queue. The sender and the receiver of the queue share it, so a chunk
/// in the queue's own room touches no state of other queues.
struct QueueRoom {
    own: AtomicUsize,
    limit: Arc<InboundLimit>,
}

impl QueueRoom {
    fn new(limit: &Arc<InboundLimit>) -> Arc<Self> {
        Arc::new(Self {
            own: AtomicUsize::new(0),
            limit: Arc::clone(limit),
        })
    }

    /// Room for one chunk if there is some now: first the queue's own, then the limit's.
    fn take(&self) -> Option<Room> {
        let own = self
            .own
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                (used < INBOUND_QUEUE_CHUNKS).then_some(used + 1)
            })
            .is_ok();
        match (own, own || self.limit.try_hold()) {
            (true, _) => Some(Room::Own),
            (false, true) => Some(Room::Shared),
            (false, false) => None,
        }
    }

    fn give_back(&self, room: Room) {
        match room {
            Room::Own => {
                self.own.fetch_sub(1, Ordering::SeqCst);
                self.limit.wake();
            }
            Room::Shared => self.limit.release(),
        }
    }
}

/// The chunks of one instruction, in arrival order. `None` marks the end of its stream.
pub struct InboundReceiver<T> {
    queue: mpsc::UnboundedReceiver<Held<T>>,
    room: Arc<QueueRoom>,
}

impl<T> InboundReceiver<T> {
    /// The next chunk. `None` once the queue is closed and empty.
    pub async fn recv(&mut self) -> Option<Option<T>> {
        let (chunk, room) = self.queue.recv().await?;
        self.room.give_back(room);
        Some(chunk)
    }

    /// The next chunk if one is queued.
    pub fn try_recv(&mut self) -> Result<Option<T>, TryRecvError> {
        let (chunk, room) = self.queue.try_recv()?;
        self.room.give_back(room);
        Ok(chunk)
    }

    /// Takes no more chunks. Chunks already queued stay readable.
    pub fn close(&mut self) {
        self.queue.close();
    }

    /// A receiver whose queue is already closed.
    fn closed(limit: &Arc<InboundLimit>) -> Self {
        Self {
            queue: mpsc::unbounded_channel().1,
            room: QueueRoom::new(limit),
        }
    }
}

impl<T> Drop for InboundReceiver<T> {
    /// Gives back the room of the chunks that nobody reads.
    fn drop(&mut self) {
        self.queue.close();
        while let Ok((_, room)) = self.queue.try_recv() {
            self.room.give_back(room);
        }
    }
}

/// Whether the instruction has taken its queue's receiver.
enum Registration<T> {
    /// Data arrived first; the receiver waits here for the instruction.
    Waiting {
        receiver: InboundReceiver<T>,
        since: Instant,
    },
    Registered,
}

/// One instruction's queue.
struct Route<T> {
    queue: mpsc::UnboundedSender<Held<T>>,
    room: Arc<QueueRoom>,
    registration: Registration<T>,
}

/// Chunks bound for one instruction's queue. The batch is sent after the routing lock is
/// released, so it owns clones of the queue's handles.
pub(super) struct Batch<T> {
    queue: mpsc::UnboundedSender<Held<T>>,
    room: Arc<QueueRoom>,
    chunks: Chunks<T>,
}

/// Per-instruction inbound queues for one kind of payload. Data that arrives before its
/// instruction registers is queued and held the same way, so it is bounded and never
/// dropped.
pub(super) struct Routes<T> {
    routes: HashMap<Arc<str>, Route<T>>,
}

impl<T> Default for Routes<T> {
    fn default() -> Self {
        Self {
            routes: HashMap::new(),
        }
    }
}

impl<T> Routes<T> {
    /// Opens a queue for `id` and returns its receiver.
    fn open(
        &mut self,
        id: &str,
        waiting: bool,
        limit: &Arc<InboundLimit>,
    ) -> Option<InboundReceiver<T>> {
        let (queue, receiver) = mpsc::unbounded_channel();
        let room = QueueRoom::new(limit);
        let receiver = InboundReceiver {
            queue: receiver,
            room: Arc::clone(&room),
        };
        let (registration, receiver) = if waiting {
            let since = Instant::now();
            (Registration::Waiting { receiver, since }, None)
        } else {
            (Registration::Registered, Some(receiver))
        };
        limit.open_queue();
        self.routes.insert(
            Arc::from(id),
            Route {
                queue,
                room,
                registration,
            },
        );
        receiver
    }

    /// Addresses `chunks` to `id`'s queue, opening it if `id` has not registered yet.
    pub(super) fn route(
        &mut self,
        id: &str,
        chunks: Chunks<T>,
        limit: &Arc<InboundLimit>,
    ) -> Batch<T> {
        if !self.routes.contains_key(id) {
            debug!("Queueing early-arriving inbound data for instruction '{id}'");
            self.open(id, true, limit);
        }
        let Route { queue, room, .. } = &self.routes[id];
        Batch {
            queue: queue.clone(),
            room: Arc::clone(room),
            chunks,
        }
    }

    /// Takes `id`'s queue with whatever arrived early. If `open` is false, the stream takes
    /// no more data for `id`: the queue closes once the early chunks are read.
    pub(super) fn register(
        &mut self,
        id: &str,
        open: bool,
        limit: &Arc<InboundLimit>,
    ) -> InboundReceiver<T> {
        let early = self.routes.get_mut(id).and_then(|route| {
            match mem::replace(&mut route.registration, Registration::Registered) {
                Registration::Waiting { receiver, .. } => Some(receiver),
                Registration::Registered => None,
            }
        });
        match (early, open) {
            (Some(receiver), true) => receiver,
            (Some(receiver), false) => {
                self.unregister(id, limit);
                receiver
            }
            (None, true) => self
                .open(id, false, limit)
                .unwrap_or_else(|| InboundReceiver::closed(limit)),
            (None, false) => InboundReceiver::closed(limit),
        }
    }

    pub(super) fn is_registered(&self, id: &str) -> bool {
        self.routes
            .get(id)
            .is_some_and(|route| matches!(route.registration, Registration::Registered))
    }

    /// The instructions that have waited at least `timeout` to register.
    pub(super) fn waiting_for(&self, timeout: Duration) -> impl Iterator<Item = &Arc<str>> {
        self.routes
            .iter()
            .filter_map(move |(id, route)| match route.registration {
                Registration::Waiting { since, .. } if since.elapsed() >= timeout => Some(id),
                Registration::Waiting { .. } | Registration::Registered => None,
            })
    }

    /// Closes `id`'s queue. Chunks already queued stay readable.
    pub(super) fn unregister(&mut self, id: &str, limit: &InboundLimit) {
        if self.routes.remove(id).is_some() {
            limit.close_queue();
        }
    }

    /// Drops every queue, returning how many there were.
    pub(super) fn clear(&mut self, limit: &InboundLimit) -> usize {
        let open = self.routes.len();
        self.routes.clear();
        (0..open).for_each(|_| limit.close_queue());
        open
    }

    /// The queues, for stall reports; see [`QueuesReport`].
    pub(super) fn report<'a>(&'a self, id: &'a str) -> QueuesReport<'a, T> {
        QueuesReport { routes: self, id }
    }
}

/// One entry per queue, `id`'s marked with `*`: the chunks in the queue's own room, and
/// whether the instruction registered.
pub(super) struct QueuesReport<'a, T> {
    routes: &'a Routes<T>,
    id: &'a str,
}

impl<T> fmt::Display for QueuesReport<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.routes
            .routes
            .iter()
            .enumerate()
            .try_for_each(|(i, (other, route))| {
                let separator = if i == 0 { "" } else { ", " };
                let me = if **other == *self.id { "*" } else { "" };
                let state = match route.registration {
                    Registration::Waiting { .. } => "unregistered",
                    Registration::Registered => "registered",
                };
                let own = route.room.own.load(Ordering::Relaxed);
                write!(f, "{separator}{me}{other}={own}({state})")
            })
    }
}

impl<T> Batch<T> {
    /// Sends the chunks in order. Each takes room in its queue or in the limit, waiting for
    /// room if needed; see [`InboundLimit::wait_reporting`] for `stalled`. Stops when the
    /// queue closes: the bundle finished or was dropped.
    pub(super) async fn send(self, stalled: impl Fn()) {
        let Self {
            queue,
            room,
            chunks,
        } = self;
        for chunk in chunks {
            if queue.is_closed() {
                return;
            }
            let taken = match room.take() {
                Some(taken) => taken,
                // Boxed, so the future of each message stays small when there is room.
                None => match Box::pin(wait_or_close(&queue, &room, &stalled)).await {
                    Some(taken) => taken,
                    None => return,
                },
            };
            if let Err(mpsc::error::SendError((_, taken))) = queue.send((chunk, taken)) {
                room.give_back(taken);
                return;
            }
        }
    }
}

/// Waits for room for one chunk in `room`. `None` if the queue closes first.
async fn wait_or_close<T>(
    queue: &mpsc::UnboundedSender<Held<T>>,
    room: &QueueRoom,
    stalled: &impl Fn(),
) -> Option<Room> {
    tokio::select! {
        taken = room.limit.wait_reporting(|| room.take(), stalled) => Some(taken),
        () = queue.closed() => None,
    }
}
