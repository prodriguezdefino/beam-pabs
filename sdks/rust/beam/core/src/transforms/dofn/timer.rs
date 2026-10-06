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

use std::sync::Mutex;

use model::pipeline as proto;

use crate::coders::TimerRecord;

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

/// A timer that a handler set or cleared, with the transform that declares its family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerChange {
    pub transform_id: String,
    pub family: String,
    pub record: TimerRecord,
}

/// Collects the timers that handlers set or clear while a bundle runs.
#[derive(Default, Debug)]
pub struct TimerCollector {
    changes: Mutex<Vec<TimerChange>>,
}

impl TimerCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `record` for `family` of the transform `transform_id`.
    pub fn push(&self, transform_id: &str, family: &str, record: TimerRecord) {
        self.changes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(TimerChange {
                transform_id: transform_id.to_string(),
                family: family.to_string(),
                record,
            });
    }

    /// Takes the recorded changes, in the order that the handlers made them.
    pub fn drain(&self) -> Vec<TimerChange> {
        std::mem::take(
            &mut *self
                .changes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}
