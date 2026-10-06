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

//! Outbound data: one bundle's elements, batched per sink.

use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use model::fn_execution::{
    Elements, elements::Data as ElementData, elements::Timers as ElementTimers,
};

use super::DataError;

/// Maximum bytes a bundle buffers before flushing to the outbound stream.
pub const OUTBOUND_FLUSH_BYTES: usize = 1_000_000;

/// Messages an outbound stream queues for gRPC. A full queue pushes back on the bundles
/// writing to it, so a slow runner bounds memory instead of growing it.
pub const OUTBOUND_QUEUE_MESSAGES: usize = 16;

/// Messages a bundle holds back for a full queue before it blocks on the element path
/// rather than buffering more (about 64 MB at [`OUTBOUND_FLUSH_BYTES`]).
const OUTBOUND_PENDING_MESSAGES: usize = 64;

/// How long outbound data waits on the runner between warnings that it is stalled.
const OUTBOUND_STALL_WARNING: Duration = Duration::from_secs(60);

/// One bundle's outbound data.
///
/// Elements go to a buffer per sink. Every [`OUTBOUND_FLUSH_BYTES`] the buffers go out as
/// one `Elements` message, and [`Outbound::finish`] sends the rest with each sink's
/// end-of-stream marker, so a bundle costs a message per megabyte, not one per element.
pub struct Outbound {
    sender: mpsc::Sender<Elements>,
    instruction_id: String,
    /// Transform id and buffered bytes, per sink.
    sinks: Vec<(String, Vec<u8>)>,
    buffered: usize,
    /// Messages the queue had no room for, in order. Writes happen on the synchronous
    /// element path, so these wait for [`Outbound::drain`] rather than blocking there.
    pending: VecDeque<Elements>,
}

impl Outbound {
    pub(super) fn new<'a>(
        sender: mpsc::Sender<Elements>,
        instruction_id: &str,
        sink_ids: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        Self {
            sender,
            instruction_id: instruction_id.to_string(),
            sinks: sink_ids
                .into_iter()
                .map(|id| (id.to_string(), Vec::new()))
                .collect(),
            buffered: 0,
            pending: VecDeque::new(),
        }
    }

    /// Appends one element to `sink`'s buffer through `write`, sending the buffers once
    /// they reach [`OUTBOUND_FLUSH_BYTES`]. Fails if `sink` is unknown.
    pub fn write(
        &mut self,
        sink: usize,
        write: impl FnOnce(&mut Vec<u8>) -> std::io::Result<()>,
    ) -> Result<(), DataError> {
        let Some((_, buf)) = self.sinks.get_mut(sink) else {
            return Err(DataError::UnknownSink(sink));
        };
        let before = buf.len();
        write(buf)?;
        self.buffered += buf.len() - before;
        if self.buffered >= OUTBOUND_FLUSH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    /// Whether messages are waiting for room in the queue.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Waits until every pending message is queued.
    pub async fn drain(&mut self) -> Result<(), DataError> {
        while let Some(elements) = self.pending.pop_front() {
            self.send(elements).await?;
        }
        Ok(())
    }

    /// Queues `elements`, warning every [`OUTBOUND_STALL_WARNING`] that the runner is not
    /// reading them.
    async fn send(&self, elements: Elements) -> Result<(), DataError> {
        let send = self.sender.send(elements);
        tokio::pin!(send);
        let started = Instant::now();
        loop {
            match tokio::time::timeout(OUTBOUND_STALL_WARNING, &mut send).await {
                Ok(sent) => return sent.map_err(|_| DataError::ChannelClosed),
                Err(_) => tracing::warn!(
                    "Outbound data for instruction '{}' has waited {}s for the runner to read it",
                    self.instruction_id,
                    started.elapsed().as_secs()
                ),
            }
        }
    }

    /// Sends whatever is buffered, then every sink's end-of-stream marker together with
    /// `timers`, in one message.
    pub async fn finish(mut self, timers: Vec<ElementTimers>) -> Result<(), DataError> {
        self.drain().await?;
        let instruction_id = &self.instruction_id;
        let chunk = |transform_id: &String, data: Vec<u8>, is_last: bool| ElementData {
            instruction_id: instruction_id.clone(),
            transform_id: transform_id.clone(),
            data,
            is_last,
        };
        // The buffers are not written again, so they go out as they are.
        let data: Vec<ElementData> = std::mem::take(&mut self.sinks)
            .into_iter()
            .flat_map(|(id, buf)| {
                let rest = (!buf.is_empty()).then(|| chunk(&id, buf, false));
                rest.into_iter().chain([chunk(&id, Vec::new(), true)])
            })
            .collect();
        if data.is_empty() && timers.is_empty() {
            return Ok(());
        }
        self.send(Elements { data, timers }).await
    }

    /// Sends the buffers, or queues them behind earlier messages still pending.
    fn flush(&mut self) -> Result<(), DataError> {
        let data = self.take_buffered();
        if data.is_empty() {
            return Ok(());
        }
        let elements = Elements {
            data,
            timers: Vec::new(),
        };
        let unsent = if self.pending.is_empty() {
            match self.sender.try_send(elements) {
                Ok(()) => None,
                Err(mpsc::error::TrySendError::Full(elements)) => Some(elements),
                Err(mpsc::error::TrySendError::Closed(_)) => return Err(DataError::ChannelClosed),
            }
        } else {
            Some(elements)
        };
        self.pending.extend(unsent);
        if self.pending.len() > OUTBOUND_PENDING_MESSAGES {
            self.block_on_pending()?;
        }
        Ok(())
    }

    /// Blocks the element path until every pending message is queued. Only a worker thread
    /// of the multi-threaded runtime can block; anywhere else the list keeps growing.
    fn block_on_pending(&mut self) -> Result<(), DataError> {
        let flavor = tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor());
        if !matches!(flavor, Ok(tokio::runtime::RuntimeFlavor::MultiThread)) {
            return Ok(());
        }
        let Self {
            sender, pending, ..
        } = self;
        tokio::task::block_in_place(|| {
            pending
                .drain(..)
                .try_for_each(|elements| sender.blocking_send(elements))
                .map_err(|_| DataError::ChannelClosed)
        })
    }

    /// Takes every non-empty sink buffer as a data chunk.
    ///
    /// gRPC takes ownership of the bytes, so the sink gets a new buffer of the same capacity
    /// and the next megabyte is written without regrowing.
    fn take_buffered(&mut self) -> Vec<ElementData> {
        self.buffered = 0;
        let Self {
            sinks,
            instruction_id,
            ..
        } = self;
        sinks
            .iter_mut()
            .filter(|(_, buf)| !buf.is_empty())
            .map(|(id, buf)| {
                let capacity = buf.len();
                ElementData {
                    instruction_id: instruction_id.clone(),
                    transform_id: id.clone(),
                    data: std::mem::replace(buf, Vec::with_capacity(capacity)),
                    is_last: false,
                }
            })
            .collect()
    }
}
