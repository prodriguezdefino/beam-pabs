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
use std::sync::Mutex;

use model::pipeline as proto;

use crate::Instant;
use crate::coders::{PaneInfo, TimerAction, TimerRecord};

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
        user_key: Option<Vec<u8>>,
        windows: Option<Vec<u8>>,
        fire_timestamp: Instant,
        hold_timestamp: Instant,
    },
    Clear {
        family: String,
        tag: String,
        user_key: Option<Vec<u8>>,
        windows: Option<Vec<u8>>,
    },
}

impl TimerMutation {
    fn into_record(self, default_key: &[u8], default_window: &[u8]) -> (String, TimerRecord) {
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
                    user_key: user_key.unwrap_or_else(|| default_key.to_vec()),
                    dynamic_tag: tag,
                    windows: vec![windows.unwrap_or_else(|| default_window.to_vec())],
                    action: TimerAction::Set {
                        fire_timestamp,
                        hold_timestamp,
                        pane: PaneInfo::NO_FIRING,
                    },
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
                    user_key: user_key.unwrap_or_else(|| default_key.to_vec()),
                    dynamic_tag: tag,
                    windows: vec![windows.unwrap_or_else(|| default_window.to_vec())],
                    action: TimerAction::Clear,
                },
            ),
        }
    }
}

/// Thread-safe collector for timer mutations generated during element processing.
#[derive(Default, Debug)]
pub struct TimerCollector {
    mutations: Mutex<Vec<TimerMutation>>,
}

impl TimerCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a timer set operation.
    pub fn set(
        &self,
        family: impl Into<String>,
        tag: impl Into<String>,
        fire_timestamp: Instant,
        hold_timestamp: Instant,
    ) {
        self.set_with_context(family, tag, None, None, fire_timestamp, hold_timestamp);
    }

    /// Records a timer set operation with explicit user key and window.
    pub fn set_with_context(
        &self,
        family: impl Into<String>,
        tag: impl Into<String>,
        user_key: Option<Vec<u8>>,
        windows: Option<Vec<u8>>,
        fire_timestamp: Instant,
        hold_timestamp: Instant,
    ) {
        let mut guard = self
            .mutations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.push(TimerMutation::Set {
            family: family.into(),
            tag: tag.into(),
            user_key,
            windows,
            fire_timestamp,
            hold_timestamp,
        });
    }

    /// Records a timer clear operation.
    pub fn clear(&self, family: impl Into<String>, tag: impl Into<String>) {
        self.clear_with_context(family, tag, None, None);
    }

    /// Records a timer clear operation with explicit user key and window.
    pub fn clear_with_context(
        &self,
        family: impl Into<String>,
        tag: impl Into<String>,
        user_key: Option<Vec<u8>>,
        windows: Option<Vec<u8>>,
    ) {
        let mut guard = self
            .mutations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.push(TimerMutation::Clear {
            family: family.into(),
            tag: tag.into(),
            user_key,
            windows,
        });
    }

    /// Drains all collected timer mutations grouped by family name.
    pub fn drain_family_records(&self) -> HashMap<String, Vec<TimerRecord>> {
        let mut guard = self
            .mutations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.drain(..).fold(
            HashMap::<String, Vec<TimerRecord>>::new(),
            |mut map, mutation| {
                let (family, record) = mutation.into_record(&[], &[]);
                map.entry(family).or_default().push(record);
                map
            },
        )
    }

    /// Drains all timer mutations. An unset user key becomes `key`; an unset window, `window`.
    pub fn drain_records(&self, key: &[u8], window: &[u8]) -> Vec<TimerRecord> {
        let mut guard = self
            .mutations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .drain(..)
            .map(|mutation| mutation.into_record(key, window).1)
            .collect()
    }
}
