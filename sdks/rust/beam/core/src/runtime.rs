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
use std::future::Future;
use std::io;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};

use tokio::sync::mpsc::{self, Receiver};

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
