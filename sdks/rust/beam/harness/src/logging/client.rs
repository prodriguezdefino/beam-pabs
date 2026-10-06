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

//! `BeamFnLogging` client and its outbound micro-batching task.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};

use model::fn_execution::{LogEntry, log_entry};

use super::LoggingError;

/// Entries buffered before new ones are dropped, so a stalled stream bounds memory and
/// never blocks a bundle.
pub const LOG_QUEUE_ENTRIES: usize = 10_000;

/// Entries sent to the runner per message.
const LOG_BATCH_ENTRIES: usize = 100;

/// Batches queued for gRPC. Past this the batching task waits, and new entries are dropped.
pub const LOG_QUEUE_BATCHES: usize = 100;

/// Longest [`LoggingClient::flush`] waits, so shutdown does not hang on a runner that
/// stopped reading the stream without closing it.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

/// Client for streaming worker logs to the runner's logging service.
#[derive(Clone)]
pub struct LoggingClient {
    entry_tx: mpsc::Sender<LogEntry>,
    flush_tx: mpsc::UnboundedSender<oneshot::Sender<()>>,
    dropped: Arc<AtomicU64>,
}

impl LoggingClient {
    /// `outbound_tx` should be bounded, at about [`LOG_QUEUE_BATCHES`].
    pub fn new(outbound_tx: mpsc::Sender<log_entry::List>) -> Self {
        let (entry_tx, entry_rx) = mpsc::channel::<LogEntry>(LOG_QUEUE_ENTRIES);
        let (flush_tx, flush_rx) = mpsc::unbounded_channel();
        let dropped = Arc::new(AtomicU64::new(0));
        spawn_batching_task(entry_rx, outbound_tx, flush_rx, dropped.clone());
        Self {
            entry_tx,
            flush_tx,
            dropped,
        }
    }

    /// Queues `entry` for batching. When [`LOG_QUEUE_ENTRIES`] are waiting, writes `entry` to
    /// stderr instead and increments the dropped-entry counter.
    pub fn log_entry(&self, entry: LogEntry) -> Result<(), LoggingError> {
        match self.entry_tx.try_send(entry) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(entry)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                let severity = log_entry::severity::Enum::try_from(entry.severity)
                    .map_or("UNSPECIFIED", |s| s.as_str_name());
                eprintln!("{severity} {}", entry.message);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(LoggingError::ChannelClosed),
        }
    }

    pub fn log(
        &self,
        severity: log_entry::severity::Enum,
        message: impl Into<String>,
        instruction_id: Option<String>,
        transform_id: Option<String>,
    ) -> Result<(), LoggingError> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        let entry = LogEntry {
            severity: severity as i32,
            timestamp: Some(prost_types::Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            message: message.into(),
            trace: String::new(),
            instruction_id: instruction_id.unwrap_or_default(),
            transform_id: transform_id.unwrap_or_default(),
            log_location: String::new(),
            thread: std::thread::current().name().unwrap_or("").to_string(),
            custom_data: None,
        };

        self.log_entry(entry)
    }

    pub fn info(&self, message: impl Into<String>) -> Result<(), LoggingError> {
        self.log(log_entry::severity::Enum::Info, message, None, None)
    }

    pub fn warn(&self, message: impl Into<String>) -> Result<(), LoggingError> {
        self.log(log_entry::severity::Enum::Warn, message, None, None)
    }

    pub fn error(&self, message: impl Into<String>) -> Result<(), LoggingError> {
        self.log(log_entry::severity::Enum::Error, message, None, None)
    }

    /// Hands every entry logged before this call to gRPC without waiting for the next tick.
    /// Returns early if the stream is gone, or after 2s if the runner stopped reading it.
    pub async fn flush(&self) {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.flush_tx.send(ack_tx).is_err() {
            return;
        }
        // `Err` means the task, and with it the stream, has ended.
        let _ = tokio::time::timeout(FLUSH_TIMEOUT, ack_rx).await;
    }
}

/// A warning that `count` entries were dropped, sent ahead of the next batch.
fn dropped_entries_warning(count: u64) -> LogEntry {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    LogEntry {
        severity: log_entry::severity::Enum::Warn as i32,
        timestamp: Some(prost_types::Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        }),
        message: format!("Dropped {count} log entries: the logging stream fell behind"),
        ..Default::default()
    }
}

/// Runs a background micro-batching loop for log entries.
fn spawn_batching_task(
    mut entry_rx: mpsc::Receiver<LogEntry>,
    grpc_tx: mpsc::Sender<log_entry::List>,
    mut flush_rx: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
    dropped: Arc<AtomicU64>,
) {
    tokio::spawn(async move {
        let mut batch = Vec::new();
        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Sends the batch after a note of entries dropped since the last one; false once the
        // stream is gone.
        let send = |batch: &mut Vec<LogEntry>| {
            let lost = dropped.swap(0, Ordering::Relaxed);
            let entries: Vec<LogEntry> = (lost > 0)
                .then(|| dropped_entries_warning(lost))
                .into_iter()
                .chain(std::mem::take(batch))
                .collect();
            let grpc_tx = grpc_tx.clone();
            async move {
                entries.is_empty()
                    || grpc_tx
                        .send(log_entry::List {
                            log_entries: entries,
                        })
                        .await
                        .is_ok()
            }
        };

        loop {
            // Biased: a waiting flush is served before more entries are batched, and a
            // steady stream of entries cannot starve the tick.
            let open = tokio::select! {
                biased;
                Some(ack) = flush_rx.recv() => {
                    // Everything logged before the flush was queued before its request.
                    while let Ok(e) = entry_rx.try_recv() {
                        batch.push(e);
                    }
                    let open = send(&mut batch).await;
                    // The caller may have stopped waiting.
                    let _ = ack.send(());
                    open
                }
                _ = ticker.tick() => send(&mut batch).await,
                entry = entry_rx.recv() => match entry {
                    Some(e) => {
                        batch.push(e);
                        batch.len() < LOG_BATCH_ENTRIES || send(&mut batch).await
                    }
                    None => {
                        send(&mut batch).await;
                        false
                    }
                },
            };
            if !open {
                break;
            }
        }
    });
}
