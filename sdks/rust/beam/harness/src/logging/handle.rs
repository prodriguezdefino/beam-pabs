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
use std::sync::{Arc, Mutex, OnceLock};

use model::fn_execution::LogEntry;

use super::LoggingClient;

/// Log entries buffered before the `BeamFnLogging` client connects.
const MAX_BUFFERED_STARTUP_ENTRIES: usize = 2048;

/// Handle shared by the `BeamFnLoggingLayer` and the `LoggingClient`. The client is in a
/// [`OnceLock`] so reads are lock-free: the stdout filter calls [`Self::has_client`] on every
/// event.
#[derive(Clone)]
pub struct BeamFnLoggingHandle {
    client: Arc<OnceLock<LoggingClient>>,
    /// Entries emitted before the client connects, replayed by [`Self::set_client`].
    buffered: Arc<Mutex<VecDeque<LogEntry>>>,
}

impl BeamFnLoggingHandle {
    pub fn new() -> Self {
        Self {
            client: Arc::new(OnceLock::new()),
            buffered: Arc::new(Mutex::new(VecDeque::with_capacity(256))),
        }
    }

    /// Sends `entry` if a client is connected, returning it unsent otherwise.
    fn try_send(&self, entry: LogEntry) -> Option<LogEntry> {
        match self.client.get() {
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

    /// Sets the active client and drains all buffered startup entries to it.
    pub fn set_client(&self, client: LoggingClient) {
        let Ok(mut buffered) = self.buffered.lock() else {
            return;
        };
        // Drain before publishing to keep buffered entries ahead of concurrent sends.
        buffered.drain(..).for_each(|entry| {
            let _ = client.log_entry(entry);
        });
        let _ = self.client.set(client);
    }

    pub fn has_client(&self) -> bool {
        self.client.get().is_some()
    }

    pub fn buffered_count(&self) -> usize {
        self.buffered.lock().map(|b| b.len()).unwrap_or(0)
    }
}

impl Default for BeamFnLoggingHandle {
    fn default() -> Self {
        Self::new()
    }
}
