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

//! The librdkafka context of every producer and consumer of the connector: Google tokens,
//! offset commit callbacks, global errors and log lines.
//!
//! # Logging
//!
//! librdkafka reports through two callbacks, and the context logs both with `tracing`:
//!
//! - Global errors ([`ClientContext::error`]) log at `ERROR`, except the copies of the
//!   end-of-partition event, which are not errors (see [`is_partition_eof`]) and log at
//!   `TRACE`.
//! - Log lines ([`ClientContext::log`]) log at the `tracing` level of their syslog level.
//!
//! A broker that is down makes every client report the same error once per connection
//! attempt. So an error or a warning that repeats within [`REPEAT_WINDOW`] logs once; the
//! next one after the window logs with the number of lines that were left out.

use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use rdkafka::TopicPartitionList;
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryResult, ProducerContext};
use tracing::{debug, error, info, trace, warn};

use super::auth::google_token;
use super::client::{Auth, is_partition_eof};
use super::read::commits::CommitTracker;
use super::write::delivery::Entry;

/// The window in which an error or a warning that repeats logs once.
pub const REPEAT_WINDOW: Duration = Duration::from_secs(60);

/// The most distinct lines that [`RepeatLimiter`] remembers. Past it, it forgets them all.
const MAX_REMEMBERED_LINES: usize = 1024;

/// The repeated lines of this process, across all clients.
static REPEATS: LazyLock<RepeatLimiter> = LazyLock::new(|| RepeatLimiter::new(REPEAT_WINDOW));

/// The context of every producer and consumer of the connector. With Google authentication it
/// gives librdkafka the tokens; without it, librdkafka never asks for one. For a consumer it
/// also tracks the offset commits.
#[derive(Default)]
pub struct KafkaContext {
    auth: Auth,
    /// Whether librdkafka got a token, so a consumer can wait for the first one.
    issued: AtomicBool,
    /// The number of token requests and commit callbacks of librdkafka.
    callbacks: AtomicU64,
    commits: CommitTracker,
}

impl KafkaContext {
    /// The context of a client that authenticates as `auth` says.
    pub fn new(auth: Auth) -> Self {
        Self {
            auth,
            ..Self::default()
        }
    }

    /// Whether this client authenticates with Google credentials.
    pub fn is_google(&self) -> bool {
        match self.auth {
            Auth::GoogleAdc => true,
            Auth::Properties => false,
        }
    }

    /// Whether librdkafka got a token from this context.
    pub fn has_issued_token(&self) -> bool {
        self.issued.load(Ordering::Acquire)
    }

    /// The number of token requests and commit callbacks of librdkafka. A poll that serves
    /// one of them returns no event, so a caller compares this count to see that it did.
    #[doc(hidden)]
    pub fn callbacks(&self) -> u64 {
        self.callbacks.load(Ordering::Acquire)
    }

    /// The offset commits of this consumer.
    #[doc(hidden)]
    pub fn commits(&self) -> &CommitTracker {
        &self.commits
    }
}

impl ClientContext for KafkaContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        let line = format!("librdkafka: {fac} {log_message}");
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                if let Some(line) = REPEATS.admit(&line, Instant::now()) {
                    error!("{line}");
                }
            }
            RDKafkaLogLevel::Warning => {
                if let Some(line) = REPEATS.admit(&line, Instant::now()) {
                    warn!("{line}");
                }
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => info!("{line}"),
            RDKafkaLogLevel::Debug => debug!("{line}"),
        }
    }

    fn error(&self, kafka_error: KafkaError, reason: &str) {
        let line = format!("librdkafka: {kafka_error}: {reason}");
        if is_partition_eof(&kafka_error) {
            trace!("{line}");
        } else if let Some(line) = REPEATS.admit(&line, Instant::now()) {
            error!("{line}");
        }
    }

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        self.callbacks.fetch_add(1, Ordering::AcqRel);
        if !self.is_google() {
            return Err("This Kafka client does not use Google authentication".into());
        }
        let token =
            google_token().inspect_err(|e| warn!("No Google token for a Kafka client: {e}"))?;
        self.issued.store(true, Ordering::Release);
        Ok(token)
    }
}

impl ConsumerContext for KafkaContext {
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        self.commits.complete(&result, offsets);
        self.callbacks.fetch_add(1, Ordering::AcqRel);
    }
}

/// Gives the result of each record to the writer that sent it.
impl ProducerContext for KafkaContext {
    type DeliveryOpaque = Arc<Entry>;

    fn delivery(&self, result: &DeliveryResult<'_>, entry: Arc<Entry>) {
        entry.set_result(match result {
            Ok(_) => Ok(()),
            Err((error, _)) => Err(error.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail)),
        });
    }
}

/// Lets a line through once per window. Lines that differ only in their digits (offsets,
/// ports, broker IDs, counts) count as the same line.
#[doc(hidden)]
pub struct RepeatLimiter {
    window: Duration,
    lines: Mutex<HashMap<String, Repeat>>,
}

/// When a line last logged, and how many times it came since then.
struct Repeat {
    logged: Instant,
    left_out: u64,
}

impl RepeatLimiter {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            lines: Mutex::new(HashMap::new()),
        }
    }

    /// The line to log for `line` at `now`, or `None` to leave it out. The first line after a
    /// window that left lines out tells how many.
    pub fn admit(&self, line: &str, now: Instant) -> Option<String> {
        let key = line
            .chars()
            .map(|c| if c.is_ascii_digit() { '#' } else { c })
            .collect::<String>();
        let mut lines = self.lines.lock().unwrap_or_else(PoisonError::into_inner);
        if lines.len() >= MAX_REMEMBERED_LINES && !lines.contains_key(&key) {
            lines.clear();
        }
        match lines.get_mut(&key) {
            Some(repeat) if now.duration_since(repeat.logged) < self.window => {
                repeat.left_out += 1;
                None
            }
            Some(repeat) => {
                let left_out = std::mem::take(&mut repeat.left_out);
                repeat.logged = now;
                Some(match left_out {
                    0 => line.to_string(),
                    n => format!(
                        "{line} ({n} more like it in the last {} s were not logged)",
                        self.window.as_secs()
                    ),
                })
            }
            None => {
                lines.insert(
                    key,
                    Repeat {
                        logged: now,
                        left_out: 0,
                    },
                );
                Some(line.to_string())
            }
        }
    }
}
