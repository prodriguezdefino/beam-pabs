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
use std::sync::{Arc, RwLock};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};
use tracing::debug;

use model::fn_execution::{
    Elements, elements::Data as ElementData, elements::Timers as ElementTimers,
};

pub use inbound::{DataChannelState, INBOUND_QUEUE_CHUNKS, deliver};
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
    #[error("Inbound data stream ended before the runner finished instruction '{0}'")]
    StreamEnded(String),
}

/// Opens named data streams on demand.
#[async_trait::async_trait]
pub trait DataStreamConnector: Send + Sync {
    async fn connect(&self, data_stream_id: &str) -> Result<mpsc::Sender<Elements>, DataError>;
}

/// Manages data streams over the Beam Fn Data service.
#[derive(Clone)]
pub struct DataManager {
    state: Arc<Mutex<DataChannelState>>,
    /// Sends `Elements` to the writer loop of the default gRPC stream.
    outbound_tx: mpsc::Sender<Elements>,
    /// Outbound channels of the named data streams, keyed by data_stream_id.
    named_streams: Arc<RwLock<HashMap<String, mpsc::Sender<Elements>>>>,
    /// Opens named data streams; without it, all data uses the default stream.
    connector: Arc<RwLock<Option<Arc<dyn DataStreamConnector>>>>,
}

impl DataManager {
    /// `outbound_tx` feeds the default stream; bound it to about [`OUTBOUND_QUEUE_MESSAGES`].
    pub fn new(outbound_tx: mpsc::Sender<Elements>) -> Self {
        Self {
            state: Arc::new(Mutex::new(DataChannelState::default())),
            outbound_tx,
            named_streams: Arc::new(RwLock::new(HashMap::new())),
            connector: Arc::new(RwLock::new(None)),
        }
    }

    /// Returns a shared handle to the channel state for inbound routing.
    pub fn channel_state(&self) -> Arc<Mutex<DataChannelState>> {
        self.state.clone()
    }

    /// Sets the connector that opens named data streams.
    pub fn set_connector(&self, connector: Arc<dyn DataStreamConnector>) {
        if let Ok(mut guard) = self.connector.write() {
            *guard = Some(connector);
        }
    }

    /// Opens stream `data_stream_id` if needed. An empty id is the always-open default
    /// stream, which also carries named streams' data when there is no connector.
    pub async fn ensure_stream(&self, data_stream_id: &str) -> Result<(), DataError> {
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
            let tx = conn.connect(data_stream_id).await?;
            if let Ok(mut streams) = self.named_streams.write() {
                streams.insert(data_stream_id.to_string(), tx);
            }
        } else {
            debug!(
                "No dynamic stream connector registered; falling back to default stream for '{data_stream_id}'"
            );
        }
        Ok(())
    }

    /// Registers an instruction's inbound data, including any that arrived early.
    pub async fn register_inbound(&self, instruction_id: &str) -> mpsc::Receiver<Option<Vec<u8>>> {
        self.state.lock().await.register_data(instruction_id)
    }

    /// Registers an instruction's inbound timers, including any that arrived early.
    pub async fn register_inbound_timers(
        &self,
        instruction_id: &str,
    ) -> mpsc::Receiver<Option<ElementTimers>> {
        self.state.lock().await.register_timers(instruction_id)
    }

    /// Unregisters an instruction's inbound timers when its bundle completes.
    pub async fn unregister_inbound_timers(&self, instruction_id: &str) {
        self.state.lock().await.unregister_timers(instruction_id);
    }

    /// Unregisters an instruction's inbound data when its bundle completes; later data for
    /// it is dropped.
    pub async fn unregister_inbound(&self, instruction_id: &str) {
        self.state.lock().await.unregister_data(instruction_id);
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
        self.outbound_sender(data_stream_id)
            .send(elements)
            .await
            .map_err(|_| DataError::ChannelClosed)
    }

    /// Opens one bundle's outbound data, buffered per sink.
    pub fn outbound<'a>(
        &self,
        data_stream_id: &str,
        instruction_id: &str,
        sink_ids: impl IntoIterator<Item = &'a str>,
    ) -> Outbound {
        Outbound::new(
            self.outbound_sender(data_stream_id),
            instruction_id,
            sink_ids,
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
