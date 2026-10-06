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

//! Event-time and processing-time timers for stateful DoFns on keyed elements.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use model::pipeline as proto;

use crate::coders::{DefaultCoder, PaneInfo, TimerRecord};

/// Time domain governing when a timer is triggered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimeDomain {
    /// Event-time timers advance with the input watermark.
    EventTime = 1,
    /// Processing-time timers advance with the system clock.
    ProcessingTime = 2,
}

impl From<TimeDomain> for proto::time_domain::Enum {
    fn from(domain: TimeDomain) -> Self {
        match domain {
            TimeDomain::EventTime => proto::time_domain::Enum::EventTime,
            TimeDomain::ProcessingTime => proto::time_domain::Enum::ProcessingTime,
        }
    }
}

/// Specification for a family of timers declared by a DoFn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerFamilySpec {
    pub family_name: String,
    pub time_domain: TimeDomain,
}

impl TimerFamilySpec {
    pub fn new(family_name: impl Into<String>, time_domain: TimeDomain) -> Self {
        Self {
            family_name: family_name.into(),
            time_domain,
        }
    }

    /// Creates an event-time timer family specification.
    pub fn event_time(family_name: impl Into<String>) -> Self {
        Self::new(family_name, TimeDomain::EventTime)
    }

    /// Creates a processing-time timer family specification.
    pub fn processing_time(family_name: impl Into<String>) -> Self {
        Self::new(family_name, TimeDomain::ProcessingTime)
    }

    /// Converts this specification into a proto `TimerFamilySpec`.
    pub fn to_proto(&self, coder_id: impl Into<String>) -> proto::TimerFamilySpec {
        proto::TimerFamilySpec {
            time_domain: proto::time_domain::Enum::from(self.time_domain) as i32,
            timer_family_coder_id: coder_id.into(),
        }
    }
}

/// Mutation action recorded against a timer family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerMutation {
    Set {
        family: String,
        tag: String,
        user_key: Vec<u8>,
        windows: Vec<u8>,
        fire_timestamp: i64,
        hold_timestamp: i64,
    },
    Clear {
        family: String,
        tag: String,
        user_key: Vec<u8>,
        windows: Vec<u8>,
    },
}

impl TimerMutation {
    fn into_family_record(self) -> (String, TimerRecord) {
        match self {
            Self::Set {
                family,
                tag,
                user_key,
                windows,
                fire_timestamp,
                hold_timestamp,
            } => (
                family,
                TimerRecord {
                    user_key,
                    dynamic_tag: tag,
                    windows: vec![windows],
                    clear: false,
                    fire_timestamp,
                    hold_timestamp,
                    pane: PaneInfo::NO_FIRING,
                },
            ),
            Self::Clear {
                family,
                tag,
                user_key,
                windows,
            } => (
                family,
                TimerRecord {
                    user_key,
                    dynamic_tag: tag,
                    windows: vec![windows],
                    clear: true,
                    fire_timestamp: 0,
                    hold_timestamp: 0,
                    pane: PaneInfo::NO_FIRING,
                },
            ),
        }
    }
}

/// Thread-safe collector for timer mutations generated during element processing.
#[derive(Default, Clone, Debug)]
pub struct TimerCollector {
    mutations: Arc<Mutex<Vec<TimerMutation>>>,
}

impl TimerCollector {
    pub fn new() -> Self {
        Self {
            mutations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Records a timer set operation.
    pub fn set(
        &self,
        family: impl Into<String>,
        tag: impl Into<String>,
        fire_timestamp: i64,
        hold_timestamp: i64,
    ) {
        self.set_with_context(
            family,
            tag,
            Vec::new(),
            Vec::new(),
            fire_timestamp,
            hold_timestamp,
        );
    }

    /// Records a timer set operation with explicit user key and window.
    pub fn set_with_context(
        &self,
        family: impl Into<String>,
        tag: impl Into<String>,
        user_key: Vec<u8>,
        windows: Vec<u8>,
        fire_timestamp: i64,
        hold_timestamp: i64,
    ) {
        if let Ok(mut guard) = self.mutations.lock() {
            guard.push(TimerMutation::Set {
                family: family.into(),
                tag: tag.into(),
                user_key,
                windows,
                fire_timestamp,
                hold_timestamp,
            });
        }
    }

    /// Records a timer clear operation.
    pub fn clear(&self, family: impl Into<String>, tag: impl Into<String>) {
        self.clear_with_context(family, tag, Vec::new(), Vec::new());
    }

    /// Records a timer clear operation with explicit user key and window.
    pub fn clear_with_context(
        &self,
        family: impl Into<String>,
        tag: impl Into<String>,
        user_key: Vec<u8>,
        windows: Vec<u8>,
    ) {
        if let Ok(mut guard) = self.mutations.lock() {
            guard.push(TimerMutation::Clear {
                family: family.into(),
                tag: tag.into(),
                user_key,
                windows,
            });
        }
    }

    /// Drains all collected timer mutations grouped by family name.
    pub fn drain_family_records(&self) -> HashMap<String, Vec<TimerRecord>> {
        self.mutations
            .lock()
            .map(|mut guard| {
                guard.drain(..).fold(
                    HashMap::<String, Vec<TimerRecord>>::new(),
                    |mut map, mutation| {
                        let (family, record) = mutation.into_family_record();
                        map.entry(family).or_default().push(record);
                        map
                    },
                )
            })
            .unwrap_or_default()
    }

    /// Drains all timer mutations. An empty user key becomes `key`; an empty window, `window`.
    pub fn drain_records(&self, key: &[u8], window: &[u8]) -> Vec<TimerRecord> {
        self.drain_family_records()
            .into_values()
            .flatten()
            .map(|mut rec| {
                if rec.user_key.is_empty() && !key.is_empty() {
                    rec.user_key = key.to_vec();
                }
                if (rec.windows.is_empty() || rec.windows[0].is_empty()) && !window.is_empty() {
                    rec.windows = vec![window.to_vec()];
                }
                rec
            })
            .collect()
    }
}

/// User-facing handle for inspecting and updating a timer in `ProcessContext`.
pub struct Timer {
    family: String,
    tag: String,
    user_key: Vec<u8>,
    windows: Vec<u8>,
    time_domain: TimeDomain,
    collector: Arc<TimerCollector>,
}

impl Timer {
    pub fn new(
        family: impl Into<String>,
        tag: impl Into<String>,
        time_domain: TimeDomain,
        collector: Arc<TimerCollector>,
    ) -> Self {
        Self::with_context(family, tag, Vec::new(), Vec::new(), time_domain, collector)
    }

    /// Creates a `Timer` with an explicit key and window.
    pub fn with_context(
        family: impl Into<String>,
        tag: impl Into<String>,
        user_key: Vec<u8>,
        windows: Vec<u8>,
        time_domain: TimeDomain,
        collector: Arc<TimerCollector>,
    ) -> Self {
        Self {
            family: family.into(),
            tag: tag.into(),
            user_key,
            windows,
            time_domain,
            collector,
        }
    }

    /// Addresses the timer with the given dynamic tag within its family.
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = tag.into();
        self
    }

    /// Binds the timer to `key` instead of the key of the current element.
    pub fn key<K: DefaultCoder>(mut self, key: &K) -> crate::Result<Self> {
        self.user_key = key
            .encode()
            .map_err(|e| crate::Error::from(e).context("Failed to encode timer key"))?;
        Ok(self)
    }

    pub fn family(&self) -> &str {
        &self.family
    }

    pub fn dynamic_tag(&self) -> &str {
        &self.tag
    }

    pub fn time_domain(&self) -> TimeDomain {
        self.time_domain
    }

    /// Sets this timer to fire at the absolute epoch timestamp (in milliseconds).
    pub fn set(&self, timestamp_ms: i64) {
        self.collector.set_with_context(
            &self.family,
            &self.tag,
            self.user_key.clone(),
            self.windows.clone(),
            timestamp_ms,
            timestamp_ms,
        );
    }

    /// Sets this timer with an explicit output watermark hold timestamp.
    pub fn set_with_hold(&self, fire_timestamp_ms: i64, hold_timestamp_ms: i64) {
        self.collector.set_with_context(
            &self.family,
            &self.tag,
            self.user_key.clone(),
            self.windows.clone(),
            fire_timestamp_ms,
            hold_timestamp_ms,
        );
    }

    /// Sets this timer to fire `offset` after the system clock time, in all time domains.
    pub fn set_relative(&self, offset: Duration) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let fire_ms = now_ms.saturating_add(offset.as_millis() as i64);
        self.set(fire_ms);
    }

    pub fn clear(&self) {
        self.collector.clear_with_context(
            &self.family,
            &self.tag,
            self.user_key.clone(),
            self.windows.clone(),
        );
    }
}
