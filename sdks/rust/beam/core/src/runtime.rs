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

use std::future::Future;
use std::io;
use std::sync::OnceLock;

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

/// Runs `fut` on [`io_runtime`] and waits for it, from any runtime context.
///
/// Inside a runtime, `fut` runs on its own thread. A nested `block_on` on the calling thread
/// shares the thread parker with an outer `block_on` and can take its wakeup.
pub fn block_on_io<F, T>(fut: F) -> io::Result<T>
where
    F: Future<Output = T> + Send,
    T: Send,
{
    let rt = io_runtime()?;
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => {
            tokio::task::block_in_place(|| block_on_own_thread(rt, fut))
        }
        Ok(_) => block_on_own_thread(rt, fut),
        Err(_) => Ok(rt.block_on(fut)),
    }
}

/// Runs `fut` on `rt` from a new thread, so the caller's thread parker is not used.
fn block_on_own_thread<F, T>(rt: &tokio::runtime::Runtime, fut: F) -> io::Result<T>
where
    F: Future<Output = T> + Send,
    T: Send,
{
    std::thread::scope(|s| {
        s.spawn(|| rt.block_on(fut))
            .join()
            .map_err(|_| io::Error::other("The async I/O thread panicked"))
    })
}

/// Receives the next value of `receiver` from synchronous code, in any runtime context.
/// Returns `None` if the receiving thread panicked, and `Some(None)` when the channel is closed.
pub fn recv_blocking<T: Send>(receiver: &mut Receiver<T>) -> Option<Option<T>> {
    match receiver.try_recv() {
        Ok(value) => return Some(Some(value)),
        Err(mpsc::error::TryRecvError::Disconnected) => return Some(None),
        Err(mpsc::error::TryRecvError::Empty) => {}
    }

    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => {
            Some(tokio::task::block_in_place(|| receiver.blocking_recv()))
        }
        Ok(_) => block_on_io(receiver.recv()).ok(),
        Err(_) => Some(receiver.blocking_recv()),
    }
}

/// Error of [`send_blocking`].
#[derive(Debug)]
pub enum SendBlockingError<T> {
    /// The receiver is closed. The error gives back the value.
    Closed(T),
    /// The async I/O runtime failed before the send completed. The value is lost.
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
pub fn send_blocking<T: Send + 'static>(
    sender: &mpsc::Sender<T>,
    value: T,
) -> Result<(), SendBlockingError<T>> {
    let value = match sender.try_send(value) {
        Ok(()) => return Ok(()),
        Err(mpsc::error::TrySendError::Closed(v)) => return Err(SendBlockingError::Closed(v)),
        Err(mpsc::error::TrySendError::Full(v)) => v,
    };
    let closed = |mpsc::error::SendError(v)| SendBlockingError::Closed(v);

    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => {
            tokio::task::block_in_place(|| sender.blocking_send(value)).map_err(closed)
        }
        Ok(_) => {
            // A current-thread runtime cannot block in place, and `blocking_send` panics
            // inside a runtime. So a task on the I/O runtime waits for capacity, and this
            // thread waits for the result. Cloning a `Sender` only increments a counter.
            let rt = io_runtime().map_err(SendBlockingError::Runtime)?;
            let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
            let sender = sender.clone();
            rt.spawn(async move {
                let _ = done_tx.send(sender.send(value).await);
            });
            done_rx
                .recv()
                .map_err(|_| {
                    SendBlockingError::Runtime(io::Error::other(
                        "The async I/O runtime dropped the send task",
                    ))
                })?
                .map_err(closed)
        }
        Err(_) => sender.blocking_send(value).map_err(closed),
    }
}
