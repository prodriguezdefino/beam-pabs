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

//! Sends [`tracing`] events from the worker to the runner over `BeamFnLogging`.

mod client;
mod handle;
mod layer;

use std::sync::OnceLock;
use std::time::SystemTime;
use thiserror::Error;

pub use client::{LOG_QUEUE_BATCHES, LOG_QUEUE_ENTRIES, LoggingClient};
pub use handle::BeamFnLoggingHandle;
pub use layer::BeamFnLoggingLayer;

/// Returns the current time as a proto timestamp.
pub(crate) fn now_timestamp() -> prost_types::Timestamp {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}

/// Errors that occur during logging operations.
#[derive(Error, Debug)]
pub enum LoggingError {
    #[error("Logging channel closed")]
    ChannelClosed,
}

static GLOBAL_HANDLE: OnceLock<BeamFnLoggingHandle> = OnceLock::new();

/// The process-wide logging handle.
pub fn global_handle() -> &'static BeamFnLoggingHandle {
    GLOBAL_HANDLE.get_or_init(BeamFnLoggingHandle::new)
}

/// Sets the active `LoggingClient` on the global handle and flushes buffered logs.
pub fn set_global_client(client: LoggingClient) {
    global_handle().set_client(client);
}

/// A `BeamFnLoggingLayer` on the global handle.
pub fn create_layer() -> BeamFnLoggingLayer {
    BeamFnLoggingLayer::new(global_handle().clone())
}

/// Initializes the global tracing subscriber with stdout output and
/// `BeamFnLoggingLayer`.
///
/// A no-op if a global subscriber is already set. Stdout is used only while there is no
/// Fn log client to avoid duplicate records from runners that capture stdout. Because
/// the endpoint may arrive later, this is checked per event.
///
/// `RUST_LOG` accepts a single level name (`trace`, `debug`, `info`, `warn`, `error`).
/// Unrecognized values or unset variables fall back to `INFO`.
pub fn init_logging() {
    use tracing_subscriber::layer::{Layer, SubscriberExt};
    use tracing_subscriber::util::SubscriberInitExt;

    let rust_log = std::env::var("RUST_LOG").unwrap_or_default();
    let filter = match rust_log.trim().to_ascii_lowercase().as_str() {
        "trace" => tracing_subscriber::filter::LevelFilter::TRACE,
        "debug" => tracing_subscriber::filter::LevelFilter::DEBUG,
        "info" => tracing_subscriber::filter::LevelFilter::INFO,
        "warn" => tracing_subscriber::filter::LevelFilter::WARN,
        "error" => tracing_subscriber::filter::LevelFilter::ERROR,
        _ => tracing_subscriber::filter::LevelFilter::INFO,
    };

    // `filter_fn` caches its verdict per callsite; only `dynamic_filter_fn` re-evaluates.
    let stdout_layer = tracing_subscriber::fmt::layer().with_filter(
        tracing_subscriber::filter::dynamic_filter_fn(|_meta, _cx| !global_handle().has_client()),
    );

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(create_layer());

    let _ = subscriber.try_init();
}
