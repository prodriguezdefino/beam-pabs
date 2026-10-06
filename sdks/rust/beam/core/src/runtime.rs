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

//! Shared multi-threaded Tokio runtime for async I/O clients, and bridges from synchronous
//! DoFn code.

use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};

use tokio::sync::mpsc::{self, Receiver};
use tokio::task::JoinError;

static IO_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();

/// Returns the shared multi-threaded runtime for asynchronous network and storage I/O.
pub fn io_runtime() -> io::Result<&'static tokio::runtime::Runtime> {
    IO_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("beam-async-io")
                .build()
                .map_err(|e| format!("Failed to start the async I/O runtime: {e}"))
        })
        .as_ref()
        .map_err(|e| io::Error::other(e.clone()))
}

/// Waits for `fut` from synchronous code, in any runtime context. `fut` can borrow.
///
/// `fut` runs inside the [`io_runtime`] context, so its I/O and timers use that runtime.
/// `fut` runs on the calling thread, so its metrics record against the running transform.
/// The wait parks the thread with `std::thread::park`. An outer Tokio `block_on` or
/// `block_on_io` on the same thread does not lose a wakeup.
///
/// # Errors
///
/// Fails if the I/O runtime cannot start.
///
/// # Panics
///
/// A panic in `fut` unwinds into the caller.
pub fn block_on_io<F: Future>(fut: F) -> io::Result<F::Output> {
    let rt = io_runtime()?;
    let run = || {
        let _context = rt.enter();
        park_on(tokio::task::unconstrained(fut))
    };
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => Ok(tokio::task::block_in_place(run)),
        Ok(_) | Err(_) => Ok(run()),
    }
}

/// Wakes a thread that waits in [`park_on`].
struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

thread_local! {
    /// The waker of [`park_on`] for this thread, built once so a wait does not allocate.
    static PARK_WAKER: Waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    /// The number of [`park_on`] calls that run on this thread.
    static PARK_DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// Leaves one [`park_on`] level when dropped.
struct ParkLevel<'a>(&'a Waker);

impl Drop for ParkLevel<'_> {
    fn drop(&mut self) {
        let depth = PARK_DEPTH.get() - 1;
        PARK_DEPTH.set(depth);
        // A nested wait can use up the unpark token of an outer level, so give it back.
        if depth > 0 {
            self.0.wake_by_ref();
        }
    }
}

/// Polls `fut` on this thread until it is ready. A spurious unpark only polls once more.
fn park_on<F: Future>(fut: F) -> F::Output {
    PARK_WAKER.with(|waker| {
        PARK_DEPTH.set(PARK_DEPTH.get() + 1);
        let _level = ParkLevel(waker);
        let mut cx = Context::from_waker(waker);
        let mut fut = std::pin::pin!(fut);
        loop {
            if let Poll::Ready(value) = fut.as_mut().poll(&mut cx) {
                return value;
            }
            std::thread::park();
        }
    })
}

/// Receives the next value of `receiver` from synchronous code, in any runtime context.
/// Returns `None` if the I/O runtime cannot start, and `Some(None)` when the channel is closed.
pub fn recv_blocking<T: Send>(receiver: &mut Receiver<T>) -> Option<Option<T>> {
    match receiver.try_recv() {
        Ok(value) => Some(Some(value)),
        Err(mpsc::error::TryRecvError::Disconnected) => Some(None),
        Err(mpsc::error::TryRecvError::Empty) => block_on_io(receiver.recv()).ok(),
    }
}

/// Error of [`send_blocking`].
#[derive(Debug)]
pub enum SendBlockingError<T> {
    /// The receiver is closed. The error gives back the value.
    Closed(T),
    /// The async I/O runtime cannot start. The value is lost.
    Runtime(io::Error),
}

impl<T> std::fmt::Display for SendBlockingError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed(_) => f.write_str("the receiver is closed"),
            Self::Runtime(e) => write!(f, "the async I/O runtime failed: {e}"),
        }
    }
}

/// Sends `value` to `sender` from synchronous code, in any runtime context.
pub fn send_blocking<T: Send>(
    sender: &mpsc::Sender<T>,
    value: T,
) -> Result<(), SendBlockingError<T>> {
    let value = match sender.try_send(value) {
        Ok(()) => return Ok(()),
        Err(mpsc::error::TrySendError::Closed(v)) => return Err(SendBlockingError::Closed(v)),
        Err(mpsc::error::TrySendError::Full(v)) => v,
    };
    block_on_io(sender.send(value))
        .map_err(SendBlockingError::Runtime)?
        .map_err(|mpsc::error::SendError(v)| SendBlockingError::Closed(v))
}

/// Aborts a spawned background Tokio task when dropped.
#[derive(Debug)]
pub struct AbortOnDrop<T = ()>(pub tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Tracks active request count and byte weight against per-bundle in-flight limits.
#[derive(Clone, Debug)]
pub struct InFlightLimit {
    active_requests: usize,
    active_bytes: usize,
    max_requests: usize,
    max_bytes: usize,
}

impl InFlightLimit {
    /// Creates a limit bounded by `max_requests` (at least 1) and `max_bytes` (at least 1).
    pub const fn new(max_requests: usize, max_bytes: usize) -> Self {
        let max_requests = if max_requests == 0 { 1 } else { max_requests };
        let max_bytes = if max_bytes == 0 { 1 } else { max_bytes };
        Self {
            active_requests: 0,
            active_bytes: 0,
            max_requests,
            max_bytes,
        }
    }

    /// Records a newly dispatched request of `bytes` size.
    pub fn add(&mut self, bytes: usize) {
        self.active_requests += 1;
        self.active_bytes = self.active_bytes.saturating_add(bytes);
    }

    /// Records completion of an in-flight request of `bytes` size.
    pub fn remove(&mut self, bytes: usize) {
        self.active_requests = self.active_requests.saturating_sub(1);
        self.active_bytes = self.active_bytes.saturating_sub(bytes);
    }

    /// Returns whether active requests or bytes have reached the configured cap.
    pub const fn is_full(&self) -> bool {
        self.active_requests >= self.max_requests || self.active_bytes >= self.max_bytes
    }
}

/// Waits for `task` from synchronous code, in any runtime context.
fn join_task<R>(task: &mut AbortOnDrop<R>) -> Result<R, JoinError> {
    if task.0.is_finished() {
        return park_on(tokio::task::unconstrained(&mut task.0));
    }
    let mut run = || park_on(tokio::task::unconstrained(&mut task.0));
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(run),
        Ok(_) | Err(_) => run(),
    }
}

/// One spawned task tracked by [`InFlightWindow`].
#[derive(Debug)]
struct InFlightEntry<M, R> {
    meta: M,
    bytes: usize,
    task: AbortOnDrop<R>,
}

/// Bounded FIFO window of concurrent Tokio tasks tracked by request count and byte weight.
///
/// Tasks sit inside [`AbortOnDrop`], so dropping the window or calling [`clear`](Self::clear)
/// aborts every unfinished task.
#[derive(Debug)]
pub struct InFlightWindow<M, R> {
    entries: VecDeque<InFlightEntry<M, R>>,
    limit: InFlightLimit,
}

impl<M, R> InFlightWindow<M, R> {
    /// Creates an empty window bounded by `max_requests` (at least 1) and `max_bytes` (at
    /// least 1).
    pub fn new(max_requests: usize, max_bytes: usize) -> Self {
        Self::from_limit(InFlightLimit::new(max_requests, max_bytes))
    }

    /// Creates an empty window with the request and byte caps of `limit`.
    pub fn from_limit(mut limit: InFlightLimit) -> Self {
        limit.active_requests = 0;
        limit.active_bytes = 0;
        Self {
            entries: VecDeque::new(),
            limit,
        }
    }

    /// Returns whether no tasks are in flight.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of tasks in flight.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether active requests or bytes have reached the configured cap.
    pub const fn is_full(&self) -> bool {
        self.limit.is_full()
    }

    /// Spawns `fut` on `handle` and tracks it with `meta` and `bytes` weight.
    pub fn spawn<F>(&mut self, handle: &tokio::runtime::Handle, meta: M, bytes: usize, fut: F)
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let task = AbortOnDrop(handle.spawn(fut));
        self.limit.add(bytes);
        self.entries.push_back(InFlightEntry { meta, bytes, task });
    }

    /// Pops the oldest entry, releases its limit weight, and awaits its task.
    fn pop_next(&mut self) -> Option<(M, Result<R, JoinError>)> {
        let mut entry = self.entries.pop_front()?;
        self.limit.remove(entry.bytes);
        if self.entries.is_empty() {
            self.limit.active_requests = 0;
            self.limit.active_bytes = 0;
        }
        let result = join_task(&mut entry.task);
        Some((entry.meta, result))
    }

    /// Drains finished front entries in FIFO order without blocking, passing each to `on_done`.
    pub fn drain_ready<E>(
        &mut self,
        mut on_done: impl FnMut(M, Result<R, JoinError>) -> Result<(), E>,
    ) -> Result<(), E> {
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.task.0.is_finished())
        {
            if let Some((meta, result)) = self.pop_next() {
                on_done(meta, result)?;
            }
        }
        Ok(())
    }

    /// Awaits oldest entries in FIFO order while [`is_full`](Self::is_full) is true, passing
    /// each to `on_done`.
    pub fn drain_while_full<E>(
        &mut self,
        mut on_done: impl FnMut(M, Result<R, JoinError>) -> Result<(), E>,
    ) -> Result<(), E> {
        while self.is_full() {
            let Some((meta, result)) = self.pop_next() else {
                break;
            };
            on_done(meta, result)?;
        }
        Ok(())
    }

    /// Awaits all remaining entries in FIFO order, passing each to `on_done`.
    pub fn drain_all<E>(
        &mut self,
        mut on_done: impl FnMut(M, Result<R, JoinError>) -> Result<(), E>,
    ) -> Result<(), E> {
        while let Some((meta, result)) = self.pop_next() {
            on_done(meta, result)?;
        }
        Ok(())
    }

    /// Awaits and returns all remaining entries in FIFO order.
    pub fn take_all(&mut self) -> Vec<(M, Result<R, JoinError>)> {
        let mut out = Vec::with_capacity(self.entries.len());
        while let Some(item) = self.pop_next() {
            out.push(item);
        }
        out
    }

    /// Drops all entries, aborting every unfinished task, and resets active counts to zero.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.limit.active_requests = 0;
        self.limit.active_bytes = 0;
    }
}
