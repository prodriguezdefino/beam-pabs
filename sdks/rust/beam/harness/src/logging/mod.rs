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

use std::sync::{LazyLock, OnceLock};
use std::time::SystemTime;
use thiserror::Error;
use tracing_subscriber::filter::{LevelFilter, Targets};

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

/// What the worker logs: a default level and levels per target.
#[derive(Clone, Debug)]
pub struct LogLevels(Targets);

impl LogLevels {
    /// `rust_log` (the `RUST_LOG` value) when it is set, else `default` and `overrides`.
    ///
    /// `rust_log` is a level (`info`) or a list of `target=level` with an optional bare level
    /// (`info,harness::data=debug`). Each override is `target=level`. A value that does not
    /// parse is ignored. Without a default level, the level is INFO.
    pub fn resolve(rust_log: Option<&str>, default: Option<&str>, overrides: &[String]) -> Self {
        let from_env = rust_log
            .map(str::trim)
            .filter(|spec| !spec.is_empty())
            .and_then(|spec| spec.parse::<Targets>().ok());
        let levels = from_env.unwrap_or_else(|| {
            let default = default
                .and_then(|level| level.trim().parse::<LevelFilter>().ok())
                .unwrap_or(LevelFilter::INFO);
            overrides
                .iter()
                .filter_map(|entry| parse_override(entry))
                .fold(
                    Targets::new().with_default(default),
                    |levels, (target, level)| levels.with_target(target, level),
                )
        });
        Self(levels)
    }

    /// The levels of `RUST_LOG`, or INFO.
    pub fn from_env() -> Self {
        Self::resolve(std::env::var("RUST_LOG").ok().as_deref(), None, &[])
    }

    /// True if an event of `level` from `target` is logged.
    pub fn would_log(&self, target: &str, level: &tracing::Level) -> bool {
        self.0.would_enable(target, level)
    }

    /// True if an event or span with `metadata` is logged. The spans of the harness carry the
    /// instruction id, so they are on at every level.
    fn enables(&self, metadata: &tracing::Metadata<'_>) -> bool {
        (metadata.is_span() && is_harness_target(metadata.target()))
            || self.would_log(metadata.target(), metadata.level())
    }

    /// The most verbose level that can pass, with the harness spans at DEBUG.
    fn max_level(&self) -> LevelFilter {
        let spans = LevelFilter::DEBUG;
        self.0
            .iter()
            .map(|(_, level)| level)
            .chain(self.0.default_level())
            .fold(spans, LevelFilter::max)
    }
}

/// `target=level` as a target and a level, or `None` if it does not parse.
fn parse_override(entry: &str) -> Option<(String, LevelFilter)> {
    let (target, level) = entry.split_once('=')?;
    let target = target.trim();
    let level = level.trim().parse::<LevelFilter>().ok()?;
    (!target.is_empty()).then(|| (target.to_string(), level))
}

fn is_harness_target(target: &str) -> bool {
    target == "harness" || target.starts_with("harness::")
}

/// The levels that [`set_log_levels`] set. Until then, the levels of `RUST_LOG`.
static LEVELS: OnceLock<LogLevels> = OnceLock::new();
static ENV_LEVELS: LazyLock<LogLevels> = LazyLock::new(LogLevels::from_env);

fn current_levels() -> &'static LogLevels {
    LEVELS.get().unwrap_or_else(|| &ENV_LEVELS)
}

/// Sets the log levels of the process, for example from the worker options. The first call
/// wins; later calls do nothing.
pub fn set_log_levels(levels: LogLevels) {
    if LEVELS.set(levels).is_ok() {
        // Callsites cache whether they are on, so ask them again.
        tracing::callsite::rebuild_interest_cache();
    }
}

/// The global filter: the current levels, and the harness spans at every level.
struct LevelsFilter;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LevelsFilter {
    fn enabled(
        &self,
        metadata: &tracing::Metadata<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        current_levels().enables(metadata)
    }

    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if current_levels().enables(metadata) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(current_levels().max_level())
    }
}

/// Initializes the global tracing subscriber with stdout output and `BeamFnLoggingLayer`.
///
/// A no-op if a global subscriber is already set. Stdout is used only while there is no
/// Fn log client to avoid duplicate records from runners that capture stdout. Because
/// the endpoint may arrive later, this is checked per event. The levels come from `RUST_LOG`
/// until [`set_log_levels`] sets them.
pub fn init_logging() {
    use tracing_subscriber::layer::{Layer, SubscriberExt};
    use tracing_subscriber::util::SubscriberInitExt;

    // `filter_fn` caches its verdict per callsite; only `dynamic_filter_fn` re-evaluates.
    let stdout_layer = tracing_subscriber::fmt::layer().with_filter(
        tracing_subscriber::filter::dynamic_filter_fn(|_meta, _cx| !global_handle().has_client()),
    );

    let subscriber = tracing_subscriber::registry()
        .with(LevelsFilter)
        .with(stdout_layer)
        .with(create_layer());

    let _ = subscriber.try_init();
}
