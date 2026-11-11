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

//! The delivery results of the records that one writer sent, with no allocation for each
//! record.
//!
//! [`Deliveries`] has a fixed number of entries. The writer gives each record the next
//! entry, in send order, and sends a clone of the entry with the record. The clone only adds
//! to the reference count. The delivery callback of the producer sets the result in the
//! entry. The writer reads the entries in send order. It uses an entry again only after it
//! read the result.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use rdkafka::error::RDKafkaErrorCode;

/// The entries of one block. A block is allocated at its first use, so a writer with few
/// records in flight uses little memory.
const BLOCK: usize = 1024;

/// The longest wait before a waiting writer reads an entry again. The delivery callback
/// wakes the writer before this time.
const CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// The delivery result of one record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Kafka did not answer yet.
    Pending,
    Delivered,
    Failed(RDKafkaErrorCode),
}

const PENDING: u8 = 0;
const DELIVERED: u8 = 1;
const FAILED: u8 = 2;

/// Wakes the writer when the delivery callback sets a result.
#[derive(Default)]
struct Signal {
    /// Whether the writer waits for a result. Only then does the callback use the lock.
    waiting: AtomicBool,
    lock: Mutex<()>,
    changed: Condvar,
}

/// The delivery state of one record in flight. The producer gets a clone of the entry as the
/// opaque value of the record, and gives it to the delivery callback.
pub struct Entry {
    state: AtomicU8,
    /// The error of a failed record. Failures are rare, so only a failure uses the lock.
    failure: Mutex<Option<RDKafkaErrorCode>>,
    signal: Arc<Signal>,
}

impl Entry {
    /// Sets the result of Kafka, and wakes the writer if it waits.
    pub fn set_result(&self, result: Result<(), RDKafkaErrorCode>) {
        match result {
            Ok(()) => self.state.store(DELIVERED, Ordering::SeqCst),
            Err(code) => {
                *self.failure.lock().unwrap_or_else(PoisonError::into_inner) = Some(code);
                self.state.store(FAILED, Ordering::SeqCst);
            }
        }
        // The writer writes `waiting` before it reads the state, and this code reads
        // `waiting` after it writes the state. So one of the two sees the other.
        if self.signal.waiting.load(Ordering::SeqCst) {
            let _guard = self
                .signal
                .lock
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.signal.changed.notify_one();
        }
    }

    /// The result. Read a failure only one time.
    fn result(&self) -> Delivery {
        match self.state.load(Ordering::SeqCst) {
            PENDING => Delivery::Pending,
            FAILED => Delivery::Failed(
                self.failure
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                    .unwrap_or(RDKafkaErrorCode::Fail),
            ),
            _ => Delivery::Delivered,
        }
    }
}

/// The entries of one block, allocated together at the first use of the block.
type Block = Box<[Arc<Entry>]>;

/// The delivery results of the records of one writer, in [`Deliveries::CAPACITY`] entries
/// that the writer uses again in order.
///
/// Only one writer uses a `Deliveries`. It gets an entry for each record, and reads the
/// results in the same order. A record in flight keeps its entry, also after the writer
/// dropped the `Deliveries`.
pub struct Deliveries {
    blocks: Box<[OnceLock<Block>]>,
    signal: Arc<Signal>,
}

impl Default for Deliveries {
    fn default() -> Self {
        Self {
            blocks: (0..Self::CAPACITY / BLOCK)
                .map(|_| OnceLock::new())
                .collect(),
            signal: Arc::default(),
        }
    }
}

impl Deliveries {
    /// The most records in flight for one writer.
    pub const CAPACITY: usize = 32 * BLOCK;

    fn entry(&self, index: usize) -> &Arc<Entry> {
        let index = index % Self::CAPACITY;
        let block = self.blocks[index / BLOCK].get_or_init(|| {
            (0..BLOCK)
                .map(|_| {
                    Arc::new(Entry {
                        state: AtomicU8::new(DELIVERED),
                        failure: Mutex::new(None),
                        signal: Arc::clone(&self.signal),
                    })
                })
                .collect()
        });
        &block[index % BLOCK]
    }

    /// Entry `index` for a new record, to send as the opaque value of the record.
    pub fn send_entry(&self, index: usize) -> Arc<Entry> {
        let entry = self.entry(index);
        entry.state.store(PENDING, Ordering::SeqCst);
        // The producer keeps this reference until the delivery callback. It only adds to
        // the reference count.
        Arc::clone(entry)
    }

    /// The result of entry `index`. Read a failure only one time.
    pub fn result(&self, index: usize) -> Delivery {
        self.entry(index).result()
    }

    /// Waits for the result of entry `index`. Read a failure only one time.
    pub fn wait_for_result(&self, index: usize) -> Delivery {
        let entry = self.entry(index);
        match entry.result() {
            Delivery::Pending => {
                let signal = &self.signal;
                signal.waiting.store(true, Ordering::SeqCst);
                let mut guard = signal.lock.lock().unwrap_or_else(PoisonError::into_inner);
                let result = loop {
                    match entry.result() {
                        Delivery::Pending => {
                            guard = signal
                                .changed
                                .wait_timeout(guard, CHECK_INTERVAL)
                                .unwrap_or_else(PoisonError::into_inner)
                                .0;
                        }
                        result => break result,
                    }
                };
                signal.waiting.store(false, Ordering::SeqCst);
                result
            }
            result => result,
        }
    }
}
