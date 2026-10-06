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

//! The runtime of the Google Cloud clients, and helpers that wait for it from the synchronous
//! code of a DoFn.

use std::io;
use std::sync::OnceLock;

use tokio::sync::mpsc::{self, Receiver};

static CLIENT_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// The runtime of the Google Cloud clients of this crate. A client must run on the runtime that
/// built it.
pub fn client_runtime() -> &'static tokio::runtime::Runtime {
    CLIENT_RUNTIME.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("beam-gcp-io")
            .build()
            .expect("Failed to start the Google Cloud client runtime")
    })
}

/// Runs `fut` on [`client_runtime`] and waits for it, in any runtime context.
pub fn block_on_async<F, T>(fut: F) -> io::Result<T>
where
    F: Future<Output = io::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let rt = client_runtime();
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| rt.block_on(fut))
            }
            _ => std::thread::scope(|s| {
                s.spawn(|| rt.block_on(fut))
                    .join()
                    .map_err(|_| io::Error::other("The Google Cloud client thread panicked"))?
            }),
        }
    } else {
        rt.block_on(fut)
    }
}

/// Receives the next value of `receiver` from synchronous code, in any runtime context.
/// Returns `None` if the receiving thread panicked, and `Some(None)` when the channel is closed.
pub fn recv_blocking<T: Send>(receiver: &mut Receiver<T>) -> Option<Option<T>> {
    match receiver.try_recv() {
        Ok(value) => return Some(Some(value)),
        Err(mpsc::error::TryRecvError::Disconnected) => return Some(None),
        Err(mpsc::error::TryRecvError::Empty) => {}
    }

    // The channel is empty, so block.
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                Some(tokio::task::block_in_place(|| receiver.blocking_recv()))
            }
            _ => {
                let fut = receiver.recv();
                let rt = client_runtime();
                std::thread::scope(|s| s.spawn(|| rt.block_on(fut)).join().ok())
            }
        }
    } else {
        Some(receiver.blocking_recv())
    }
}
