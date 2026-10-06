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

//! The limit on chunks held beyond the per-instruction queues, shared by all queues.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::debug;

use super::{INBOUND_QUEUE_CHUNKS, INBOUND_STALL_WARNING};

/// Open queues and extra room, in one word so that the limit changes atomically.
#[derive(Clone, Copy)]
struct Size {
    queues: u32,
    extra: u32,
}

impl Size {
    fn unpack(word: u64) -> Self {
        Self {
            queues: (word >> 32) as u32,
            extra: word as u32,
        }
    }

    fn pack(self) -> u64 {
        (u64::from(self.queues) << 32) | u64::from(self.extra)
    }

    fn limit(self) -> usize {
        base_room(self.queues) + self.extra as usize
    }
}

/// The room of `queues` open queues. The extra room is at most this much.
fn base_room(queues: u32) -> usize {
    queues as usize * INBOUND_QUEUE_CHUNKS
}

/// Chunks held beyond the queues' own room, against a limit of [`INBOUND_QUEUE_CHUNKS`] for
/// each open queue plus the extra room that [`InboundLimit::raise`] adds.
#[derive(Default)]
pub(in crate::data) struct InboundLimit {
    size: AtomicU64,
    held: AtomicUsize,
    room: Notify,
    /// Tasks in [`InboundLimit::hold`]. A resize wakes them only when there are some.
    waiters: AtomicUsize,
}

/// One task in [`InboundLimit::hold`], counted until it returns or is cancelled.
struct Waiting<'a>(&'a AtomicUsize);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl InboundLimit {
    // The room counters and `waiters` use `SeqCst`: a waiter counts itself before it reads
    // the room, and a resize or release changes the room before it reads `waiters`, so one of
    // the two sees the other.
    fn size(&self) -> Size {
        Size::unpack(self.size.load(Ordering::SeqCst))
    }

    /// Changes the size with `f`, then wakes the waiters to check the new limit.
    fn resize(&self, f: impl Fn(Size) -> Size) -> (Size, Size) {
        let (Ok(word) | Err(word)) =
            self.size
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |word| {
                    Some(f(Size::unpack(word)).pack())
                });
        let (old, new) = (Size::unpack(word), f(Size::unpack(word)));
        if self.waiters.load(Ordering::SeqCst) > 0 {
            self.room.notify_waiters();
        }
        (old, new)
    }

    pub(super) fn open_queue(&self) {
        self.resize(|Size { queues, extra }| Size {
            queues: queues + 1,
            extra,
        });
    }

    /// Removes a queue's room. Chunks held above the new limit stay until they are read.
    pub(super) fn close_queue(&self) {
        self.resize(|Size { queues, extra }| {
            let queues = queues.saturating_sub(1);
            Size {
                queues,
                extra: extra.min(base_room(queues) as u32),
            }
        });
    }

    /// Adds [`INBOUND_QUEUE_CHUNKS`] of extra room, up to the base room of the open queues.
    /// Called while output waits on the runner: a runner that does not read output until
    /// it has sent its input then gets to send it.
    pub(in crate::data) fn raise(&self) {
        let (old, new) = self.resize(|Size { queues, extra }| Size {
            queues,
            extra: (extra as usize + INBOUND_QUEUE_CHUNKS).min(base_room(queues)) as u32,
        });
        if new.extra > old.extra {
            debug!(
                "Output waits on the runner: the inbound limit is now {} chunks",
                new.limit()
            );
        }
    }

    /// Takes room for one chunk if there is some now.
    pub(super) fn try_hold(&self) -> bool {
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                (held < self.size().limit()).then_some(held + 1)
            })
            .is_ok()
    }

    /// Gives back the room of one chunk.
    pub(super) fn release(&self) {
        self.held.fetch_sub(1, Ordering::SeqCst);
        self.wake();
    }

    /// Wakes the senders that wait for room, to check again. Streams can wait for different
    /// queues, so all wake.
    pub(super) fn wake(&self) {
        if self.waiters.load(Ordering::SeqCst) > 0 {
            self.room.notify_waiters();
        }
    }

    /// Waits until `take` finds room. `take` runs again after each [`Self::wake`].
    async fn wait_for<R>(&self, take: &impl Fn() -> Option<R>) -> R {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let _waiting = Waiting(&self.waiters);
        loop {
            let room = self.room.notified();
            tokio::pin!(room);
            // Registered before the check, so a wake between the two is not lost.
            room.as_mut().enable();
            if let Some(taken) = take() {
                return taken;
            }
            room.await;
        }
    }

    /// Waits until `take` finds room. Every [`INBOUND_STALL_WARNING`] it warns, because no
    /// instruction on the stream gets data while it waits, and calls `stalled`.
    pub(super) async fn wait_reporting<R>(
        &self,
        take: impl Fn() -> Option<R>,
        stalled: impl Fn(),
    ) -> R {
        let started = Instant::now();
        loop {
            match tokio::time::timeout(INBOUND_STALL_WARNING, self.wait_for(&take)).await {
                Ok(taken) => return taken,
                Err(_) => {
                    tracing::warn!(
                        "Inbound data has waited {}s for room in the stream's buffer ({self}); \
                         the chunks of all instructions on this stream wait too",
                        started.elapsed().as_secs()
                    );
                    stalled();
                }
            }
        }
    }
}

impl fmt::Display for InboundLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let size @ Size { queues, extra } = self.size();
        write!(
            f,
            "limit: {} held of {} for {queues} queues, {extra} of them extra",
            self.held.load(Ordering::Acquire),
            size.limit()
        )
    }
}
