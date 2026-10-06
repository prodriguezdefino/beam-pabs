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

//! Shared handle bridging the tracing layer and the active `BeamFnLogging` client.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use beam_model::fn_execution::LogEntry;

use super::LoggingClient;

/// Log entries buffered before the `BeamFnLogging` client connects.
const MAX_BUFFERED_STARTUP_ENTRIES: usize = 2048;

/// Handle shared by the `BeamFnLoggingLayer` and the `LoggingClient`. The client is in an
/// [`ArcSwapOption`] so reads are lock-free: the stdout filter calls [`Self::has_client`] on
/// every event.
#[derive(Clone)]
pub struct BeamFnLoggingHandle {
    client: Arc<ArcSwapOption<LoggingClient>>,
    /// Entries emitted before the client connects, replayed by [`Self::set_client`].
    buffered: Arc<Mutex<VecDeque<LogEntry>>>,
}

impl BeamFnLoggingHandle {
    /// Creates a new logging handle with an empty buffer.
    pub fn new() -> Self {
        Self {
            client: Arc::new(ArcSwapOption::empty()),
            buffered: Arc::new(Mutex::new(VecDeque::with_capacity(256))),
        }
    }

    /// Sends `entry` if a client is connected, returning it unsent otherwise.
    fn try_send(&self, entry: LogEntry) -> Option<LogEntry> {
        match &*self.client.load() {
            Some(client) => {
                let _ = client.log_entry(entry);
                None
            }
            None => Some(entry),
        }
    }

    /// Sends `entry` to the client, or buffers it until the client connects.
    pub fn send(&self, entry: LogEntry) {
        let Some(entry) = self.try_send(entry) else {
            return;
        };
        let Ok(mut buffered) = self.buffered.lock() else {
            return;
        };
        // `set_client` may have drained while this thread waited; an entry pushed after the
        // drain would never be sent.
        let Some(entry) = self.try_send(entry) else {
            return;
        };

        if buffered.len() >= MAX_BUFFERED_STARTUP_ENTRIES {
            buffered.pop_front();
        }
        buffered.push_back(entry);
    }

    /// Sets the active client and drains all buffered startup entries to it. Replaces the
    /// client that is set.
    pub fn set_client(&self, client: LoggingClient) {
        let Ok(mut buffered) = self.buffered.lock() else {
            return;
        };
        // Hold the lock through the drain and the store, so `send` cannot buffer an entry
        // in between. `log_entry` only calls `try_send` and does not block.
        buffered.drain(..).for_each(|entry| {
            let _ = client.log_entry(entry);
        });
        self.client.store(Some(Arc::new(client)));
    }

    /// Removes the active client if it is `client`. Then `send` buffers entries for the
    /// next client. A client that replaced `client` stays.
    pub fn clear_client(&self, client: &LoggingClient) {
        let current = self.client.load();
        if let Some(active) = &*current
            && active.same_client(client)
        {
            self.client.compare_and_swap(&*current, None);
        }
    }

    /// Returns true if an active logging client is connected.
    pub fn has_client(&self) -> bool {
        self.client.load().is_some()
    }

    /// Returns the number of currently buffered startup log entries.
    #[doc(hidden)]
    pub fn buffered_count(&self) -> usize {
        self.buffered.lock().map(|b| b.len()).unwrap_or(0)
    }
}

impl Default for BeamFnLoggingHandle {
    fn default() -> Self {
        Self::new()
    }
}
