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

//! Shared helpers for the harness integration tests.

#![allow(
    dead_code,
    reason = "shared by several test binaries, each using a different subset"
)]

pub mod state_mock;

/// How long a test waits for an event that should arrive promptly. Correct code takes
/// milliseconds; the deadline makes a lost input fail the test, not hang it.
pub const WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Awaits `fut`, panicking with `what` if it does not complete within [`WAIT`].
pub async fn within<F: IntoFuture>(what: &str, fut: F) -> F::Output {
    tokio::time::timeout(WAIT, fut)
        .await
        .unwrap_or_else(|_| panic!("timed out after {WAIT:?} waiting for {what}"))
}

/// A one-shot latch that threads block on until another thread opens it. Waits are bounded
/// by [`WAIT`], so a latch that never opens fails instead of hanging.
#[derive(Default)]
pub struct Gate {
    opened: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
}

impl Gate {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::default()
    }

    /// Opens the gate, releasing every current and future waiter. Idempotent.
    pub fn open(&self) {
        *self.opened.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }

    pub fn is_open(&self) -> bool {
        *self.opened.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Blocks until the gate opens; panics if it is still closed after [`WAIT`].
    pub fn wait(&self, what: &str) {
        let guard = self.opened.lock().unwrap_or_else(|e| e.into_inner());
        let (guard, _) = self
            .changed
            .wait_timeout_while(guard, WAIT, |opened| !*opened)
            .unwrap_or_else(|e| e.into_inner());
        assert!(*guard, "timed out after {WAIT:?} waiting for {what}");
    }

    /// A guard that opens this gate when dropped, even during a panic, so a failing
    /// assertion cannot leave a blocked handler parked.
    pub fn open_on_drop(self: &std::sync::Arc<Self>) -> OpenOnDrop {
        OpenOnDrop(std::sync::Arc::clone(self))
    }
}

/// Opens its [`Gate`] on drop. See [`Gate::open_on_drop`].
pub struct OpenOnDrop(std::sync::Arc<Gate>);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.open();
    }
}
