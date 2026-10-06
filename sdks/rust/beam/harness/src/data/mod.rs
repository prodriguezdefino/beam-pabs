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

//! Beam Fn Data stream multiplexer and channel router.
//!
//! Manages the bidirectional gRPC `Elements` streams between the runner and the worker:
//! `inbound` demultiplexes by `instruction_id`, `outbound` buffers and sends end markers.

mod inbound;
mod outbound;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, error};

use model::fn_execution::{
    Elements, elements::Data as ElementData, elements::Timers as ElementTimers,
};

use inbound::InboundLimit;
use inbound::deliver;
pub use inbound::{
    DataChannelState, INBOUND_QUEUE_CHUNKS, InboundReceiver, UNREGISTERED_DATA_TIMEOUT, lock_state,
};
pub use outbound::{OUTBOUND_FLUSH_BYTES, OUTBOUND_QUEUE_MESSAGES, Outbound};

#[derive(Error, Debug)]
pub enum DataError {
    #[error("Outbound data channel closed")]
    ChannelClosed,
    #[error("Data stream connection error: {0}")]
    Connection(#[source] tonic::Status),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("No outbound data sink at index {0}")]
    UnknownSink(usize),
    #[error("No outbound timer family '{family}' for transform '{transform}'")]
    UnknownTimerFamily { transform: String, family: String },
    #[error("Inbound data stream ended before the runner finished instruction '{0}'")]
    StreamEnded(String),
}

/// Opens named data streams on demand.
#[async_trait::async_trait]
pub trait DataStreamConnector: Send + Sync {
    /// Opens stream `data_stream_id` at `endpoint`, or at the connector's own endpoint for
    /// `None`.
    async fn connect(
        &self,
        data_stream_id: &str,
        endpoint: Option<&str>,
    ) -> Result<NamedStream, DataError>;
}

/// The runner's messages on one data stream. An error ends the stream.
pub type InboundStream = Pin<Box<dyn Stream<Item = Result<Elements, String>> + Send>>;

/// One open named data stream.
pub struct NamedStream {
    /// Takes the bundles' output.
    pub outbound: mpsc::Sender<Elements>,
    /// The runner's data for the bundles.
    pub inbound: InboundStream,
}

/// The inbound queues of one bundle. `timers` is `None` when the bundle reads no timers.
pub struct BundleInbound {
    pub data: InboundReceiver<Vec<u8>>,
    pub timers: Option<InboundReceiver<ElementTimers>>,
    pub registration: Registration,
}

/// Unregisters a bundle's inbound data and timers when dropped, also when the bundle fails or
/// panics. Later data for the instruction is dropped.
pub struct Registration {
    state: Arc<Mutex<DataChannelState>>,
    instruction_id: String,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = lock_state(&self.state);
        state.unregister_data(&self.instruction_id);
        state.unregister_timers(&self.instruction_id);
    }
}

/// Manages data streams over the Beam Fn Data service.
#[derive(Clone)]
pub struct DataManager {
    state: Arc<Mutex<DataChannelState>>,
    /// The inbound limit of `state`, raised while output waits on the runner.
    inbound_limit: Arc<InboundLimit>,
    /// Sends `Elements` to the writer loop of the default gRPC stream.
    outbound_tx: mpsc::Sender<Elements>,
    /// Outbound channels of the named data streams, keyed by data_stream_id.
    named_streams: Arc<RwLock<HashMap<String, mpsc::Sender<Elements>>>>,
    /// Opens named data streams; without it, all data uses the default stream.
    connector: Arc<RwLock<Option<Arc<dyn DataStreamConnector>>>>,
    /// The longest time output waits in a buffer. `None`: only size and bundle end send it.
    flush_time_limit: Option<Duration>,
}

impl DataManager {
    /// `outbound_tx` feeds the default stream; bound it to about [`OUTBOUND_QUEUE_MESSAGES`].
    pub fn new(outbound_tx: mpsc::Sender<Elements>) -> Self {
        let inbound_limit = Arc::new(InboundLimit::default());
        let state = Arc::new(Mutex::new(DataChannelState::new(Arc::clone(
            &inbound_limit,
        ))));
        Self {
            state,
            inbound_limit,
            outbound_tx,
            named_streams: Arc::new(RwLock::new(HashMap::new())),
            connector: Arc::new(RwLock::new(None)),
            flush_time_limit: None,
        }
    }

    /// Sends buffered bundle output after at most `limit`, also below the size limit.
    pub fn with_flush_time_limit(self, limit: Option<Duration>) -> Self {
        Self {
            flush_time_limit: limit,
            ..self
        }
    }

    /// Returns a shared handle to the channel state for inbound routing.
    pub fn channel_state(&self) -> Arc<Mutex<DataChannelState>> {
        self.state.clone()
    }

    /// Returns the stream IDs of all open named data streams.
    pub fn open_named_streams(&self) -> Vec<String> {
        self.named_streams
            .read()
            .map(|streams| streams.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Sets the connector that opens named data streams.
    pub fn set_connector(&self, connector: Arc<dyn DataStreamConnector>) {
        if let Ok(mut guard) = self.connector.write() {
            *guard = Some(connector);
        }
    }

    /// Opens stream `data_stream_id` if it is not open, at `endpoint` or at the connector's
    /// own endpoint. When the stream ends, the next call opens it again.
    pub async fn ensure_stream_endpoint(
        &self,
        data_stream_id: &str,
        endpoint: Option<&str>,
    ) -> Result<(), DataError> {
        let is_already_open = data_stream_id.is_empty()
            || self
                .named_streams
                .read()
                .is_ok_and(|streams| streams.contains_key(data_stream_id));

        if is_already_open {
            return Ok(());
        }

        let connector = self.connector.read().ok().and_then(|guard| guard.clone());

        if let Some(conn) = connector {
            let NamedStream { outbound, inbound } = conn.connect(data_stream_id, endpoint).await?;
            if let Ok(mut streams) = self.named_streams.write() {
                streams.insert(data_stream_id.to_string(), outbound.clone());
            }
            tokio::spawn(read_named_stream(
                Arc::clone(&self.state),
                Arc::clone(&self.named_streams),
                data_stream_id.to_string(),
                outbound,
                inbound,
            ));
        } else {
            debug!(
                "No dynamic stream connector registered; falling back to default stream for '{data_stream_id}'"
            );
        }
        Ok(())
    }

    /// Opens stream `data_stream_id` if needed. An empty id is the always-open default
    /// stream, which also carries named streams' data when there is no connector.
    pub async fn ensure_stream(&self, data_stream_id: &str) -> Result<(), DataError> {
        self.ensure_stream_endpoint(data_stream_id, None).await
    }

    /// Registers an instruction's inbound data, including any that arrived early.
    pub fn register_inbound(&self, instruction_id: &str) -> InboundReceiver<Vec<u8>> {
        lock_state(&self.state).register_data(instruction_id)
    }

    /// Registers an instruction's inbound timers, including any that arrived early.
    pub fn register_inbound_timers(&self, instruction_id: &str) -> InboundReceiver<ElementTimers> {
        lock_state(&self.state).register_timers(instruction_id)
    }

    /// Unregisters an instruction's inbound timers when its bundle completes.
    pub fn unregister_inbound_timers(&self, instruction_id: &str) {
        lock_state(&self.state).unregister_timers(instruction_id);
    }

    /// Unregisters an instruction's inbound data when its bundle completes; later data for
    /// it is dropped.
    pub fn unregister_inbound(&self, instruction_id: &str) {
        lock_state(&self.state).unregister_data(instruction_id);
    }

    /// Registers the inbound data of a bundle, and its timers if it reads timers, under one
    /// lock. Dropping the returned registration unregisters both.
    pub fn register_bundle_inbound(&self, instruction_id: &str, timers: bool) -> BundleInbound {
        let mut state = lock_state(&self.state);
        BundleInbound {
            data: state.register_data(instruction_id),
            timers: timers.then(|| state.register_timers(instruction_id)),
            registration: Registration {
                state: Arc::clone(&self.state),
                instruction_id: instruction_id.to_string(),
            },
        }
    }

    /// Describes the default data stream and `instruction_id`'s queue, for stall reports.
    pub fn describe_inbound(&self, instruction_id: &str) -> String {
        lock_state(&self.state).describe(instruction_id)
    }

    /// Routes inbound `Elements` from the runner to their bundles.
    pub async fn handle_inbound_elements(&self, elements: Elements) {
        deliver(&self.state, elements).await;
    }

    /// Sends a data chunk for a given instruction and transform to the runner.
    ///
    /// Bundles write through [`DataManager::outbound`], which batches; this sends one chunk
    /// alone.
    pub async fn send_data(
        &self,
        data_stream_id: &str,
        instruction_id: &str,
        transform_id: &str,
        data: Vec<u8>,
    ) -> Result<(), DataError> {
        let elements = Elements {
            data: vec![ElementData {
                instruction_id: instruction_id.to_string(),
                transform_id: transform_id.to_string(),
                data,
                is_last: false,
            }],
            timers: Vec::new(),
        };
        outbound::send_reporting_stall(
            &self.outbound_sender(data_stream_id),
            elements,
            &self.inbound_limit,
            instruction_id,
        )
        .await
    }

    /// Opens one bundle's outbound data, buffered per sink.
    pub fn outbound<'a>(
        &self,
        data_stream_id: &str,
        instruction_id: &str,
        sink_ids: impl IntoIterator<Item = &'a str>,
        timer_families: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Outbound {
        Outbound::new(
            self.outbound_sender(data_stream_id),
            Arc::clone(&self.inbound_limit),
            instruction_id,
            sink_ids,
            timer_families,
            self.flush_time_limit,
        )
    }

    /// The named stream's channel if it is open, else the default one.
    fn outbound_sender(&self, data_stream_id: &str) -> mpsc::Sender<Elements> {
        Some(data_stream_id)
            .filter(|id| !id.is_empty())
            .and_then(|id| self.named_streams.read().ok()?.get(id).cloned())
            .unwrap_or_else(|| self.outbound_tx.clone())
    }
}

/// Delivers the messages of `inbound` to the bundles until it ends, and returns why it ended.
pub(crate) async fn read_stream(
    state: &Arc<Mutex<DataChannelState>>,
    mut inbound: impl Stream<Item = Result<Elements, String>> + Unpin,
    name: &str,
) -> String {
    loop {
        match inbound.next().await {
            Some(Ok(elements)) => deliver(state, elements).await,
            Some(Err(e)) => break format!("{name} failed: {e}"),
            None => break format!("{name} closed by runner"),
        }
    }
}

/// Reads named stream `id` until it ends. Then forgets the stream, so the next bundle on it
/// opens it again, and fails the waiting bundles.
async fn read_named_stream(
    state: Arc<Mutex<DataChannelState>>,
    streams: Arc<RwLock<HashMap<String, mpsc::Sender<Elements>>>>,
    id: String,
    outbound: mpsc::Sender<Elements>,
    inbound: InboundStream,
) {
    debug!("Reading named data stream '{id}'");
    let reason = read_stream(&state, inbound, &format!("Named data stream '{id}'")).await;
    error!("{reason}");
    if let Ok(mut streams) = streams.write() {
        // A newer stream with the same id stays.
        if streams
            .get(&id)
            .is_some_and(|open| open.same_channel(&outbound))
        {
            streams.remove(&id);
        }
    }
    // Inbound queues are keyed by instruction only, so the bundles of this stream cannot be
    // told apart: all fail, and the runner retries them.
    lock_state(&state).fail_all(&reason);
}
