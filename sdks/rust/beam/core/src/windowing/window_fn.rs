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

//! Standard window functions and the bounded window type.

use std::sync::Arc;
use std::time::Duration;

use model::pipeline as proto;
use prost::Message;

use crate::coders::{
    Coder, Context, GlobalWindow, GlobalWindowCoder, IntervalWindow, IntervalWindowCoder,
    URN_GLOBAL_WINDOW, URN_INTERVAL_WINDOW,
};
use crate::pipeline::constants::{
    URN_WINDOW_FN_FIXED_WINDOWS, URN_WINDOW_FN_GLOBAL_WINDOWS, URN_WINDOW_FN_SESSION_WINDOWS,
    URN_WINDOW_FN_SLIDING_WINDOWS,
};

/// A bounded window representing a finite or global span of event time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BoundedWindow {
    /// The single global window covering all of time.
    Global(GlobalWindow),
    /// A half-open interval `[start_millis, end_millis)`.
    Interval(IntervalWindow),
}

impl BoundedWindow {
    /// Maximum inclusive timestamp for the global window (9999-12-31T23:59:59.999Z).
    pub const TIMESTAMP_MAX_VALUE: i64 = 253_402_300_799_999;

    /// Returns the maximum inclusive event timestamp in milliseconds for this window.
    pub fn max_timestamp(&self) -> i64 {
        match self {
            Self::Global(_) => Self::TIMESTAMP_MAX_VALUE,
            Self::Interval(w) => w.max_timestamp(),
        }
    }

    /// Returns the underlying `IntervalWindow` if this is an interval window.
    pub fn interval(&self) -> Option<IntervalWindow> {
        match self {
            Self::Interval(w) => Some(*w),
            Self::Global(_) => None,
        }
    }

    /// Encodes this window into `writer` in nested context.
    pub fn encode(&self, writer: &mut dyn std::io::Write) -> Result<(), crate::coders::CoderError> {
        match self {
            Self::Global(g) => GlobalWindowCoder.encode(g, writer, Context::Nested),
            Self::Interval(i) => IntervalWindowCoder.encode(i, writer, Context::Nested),
        }
    }

    /// Decodes a window from `reader` given its coder URN.
    pub fn decode(
        urn: &str,
        reader: &mut dyn std::io::Read,
    ) -> Result<Self, crate::coders::CoderError> {
        match urn {
            URN_GLOBAL_WINDOW => {
                let w = GlobalWindowCoder.decode(reader, Context::Nested)?;
                Ok(Self::Global(w))
            }
            URN_INTERVAL_WINDOW => {
                let w = IntervalWindowCoder.decode(reader, Context::Nested)?;
                Ok(Self::Interval(w))
            }
            other => Err(crate::coders::CoderError::Format(format!(
                "Unsupported window coder URN: {other}"
            ))),
        }
    }
}

/// Defines how the elements of a PCollection are assigned to windows.
pub trait WindowFn: Send + Sync + std::fmt::Debug + 'static {
    /// Assigns windows to an element given its event timestamp in milliseconds.
    fn assign_windows(&self, timestamp_millis: i64) -> Vec<BoundedWindow>;

    /// Assigns windows and encodes each one with the window coder in nested context.
    fn assign_windows_encoded(&self, timestamp_millis: i64) -> Vec<Vec<u8>> {
        self.assign_windows(timestamp_millis)
            .into_iter()
            .map(|w| {
                let mut buf = Vec::new();
                w.encode(&mut buf)
                    .expect("encoding a window into a Vec<u8> cannot fail");
                buf
            })
            .collect()
    }

    /// URN of this window function.
    fn urn(&self) -> &'static str;

    /// Protobuf payload of the FunctionSpec for this window function.
    fn payload(&self) -> Vec<u8>;

    /// URN of the coder used to encode this window function's windows.
    fn window_coder_urn(&self) -> &'static str;

    /// Merge status of this window function.
    fn merge_status(&self) -> proto::merge_status::Enum;

    /// Whether this window function assigns every input to exactly one window.
    fn assigns_to_one_window(&self) -> bool;

    /// Clones this window function as a boxed trait object.
    fn box_clone(&self) -> Box<dyn WindowFn>;
}

impl Clone for Box<dyn WindowFn> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// The default window function. It assigns all elements to the single global window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GlobalWindows;

impl WindowFn for GlobalWindows {
    fn assign_windows(&self, _timestamp_millis: i64) -> Vec<BoundedWindow> {
        vec![BoundedWindow::Global(GlobalWindow)]
    }

    fn assign_windows_encoded(&self, _timestamp_millis: i64) -> Vec<Vec<u8>> {
        vec![Vec::new()]
    }

    fn urn(&self) -> &'static str {
        URN_WINDOW_FN_GLOBAL_WINDOWS
    }

    fn payload(&self) -> Vec<u8> {
        Vec::new()
    }

    fn window_coder_urn(&self) -> &'static str {
        URN_GLOBAL_WINDOW
    }

    fn merge_status(&self) -> proto::merge_status::Enum {
        proto::merge_status::Enum::NonMerging
    }

    fn assigns_to_one_window(&self) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn WindowFn> {
        Box::new(*self)
    }
}

/// A window function that divides event time into equal, non-overlapping intervals
/// `[N * size + offset, (N + 1) * size + offset)`. A zero `size` assigns each element to a
/// 1 ms window at its timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FixedWindows {
    size: Duration,
    offset: Duration,
}

impl FixedWindows {
    /// Creates fixed windows of the given duration size.
    pub fn of(size: Duration) -> Self {
        Self {
            size,
            offset: Duration::ZERO,
        }
    }

    /// Sets an offset for the start of the windows.
    pub fn with_offset(mut self, offset: Duration) -> Self {
        self.offset = offset;
        self
    }

    pub fn size(&self) -> Duration {
        self.size
    }

    pub fn offset(&self) -> Duration {
        self.offset
    }

    fn size_millis(&self) -> i64 {
        self.size.as_millis() as i64
    }

    fn offset_millis(&self) -> i64 {
        self.offset.as_millis() as i64
    }
}

impl WindowFn for FixedWindows {
    fn assign_windows(&self, timestamp_millis: i64) -> Vec<BoundedWindow> {
        let size = self.size_millis();
        let offset = self.offset_millis();
        if size <= 0 {
            return vec![BoundedWindow::Interval(IntervalWindow::new(
                timestamp_millis,
                timestamp_millis + 1,
            ))];
        }
        let start = timestamp_millis - (timestamp_millis - offset).rem_euclid(size);
        let end = start + size;
        vec![BoundedWindow::Interval(IntervalWindow::new(start, end))]
    }

    fn urn(&self) -> &'static str {
        URN_WINDOW_FN_FIXED_WINDOWS
    }

    fn payload(&self) -> Vec<u8> {
        proto::FixedWindowsPayload {
            size: Some(duration_to_proto(self.size)),
            offset: Some(timestamp_to_proto(self.offset_millis())),
        }
        .encode_to_vec()
    }

    fn window_coder_urn(&self) -> &'static str {
        URN_INTERVAL_WINDOW
    }

    fn merge_status(&self) -> proto::merge_status::Enum {
        proto::merge_status::Enum::NonMerging
    }

    fn assigns_to_one_window(&self) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn WindowFn> {
        Box::new(*self)
    }
}

/// A window function that divides event time into intervals of `size` that start every
/// `period`, so windows overlap when `period < size`. A zero `size` or `period` assigns each
/// element to a 1 ms window at its timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlidingWindows {
    size: Duration,
    period: Duration,
    offset: Duration,
}

impl SlidingWindows {
    /// Creates sliding windows of the given duration size.
    pub fn of(size: Duration) -> Self {
        Self {
            size,
            period: size,
            offset: Duration::ZERO,
        }
    }

    /// Sets the period between the starts of consecutive windows.
    pub fn every(mut self, period: Duration) -> Self {
        self.period = period;
        self
    }

    /// Sets an offset for the start of the windows.
    pub fn with_offset(mut self, offset: Duration) -> Self {
        self.offset = offset;
        self
    }

    pub fn size(&self) -> Duration {
        self.size
    }

    pub fn period(&self) -> Duration {
        self.period
    }

    pub fn offset(&self) -> Duration {
        self.offset
    }

    fn size_millis(&self) -> i64 {
        self.size.as_millis() as i64
    }

    fn period_millis(&self) -> i64 {
        self.period.as_millis() as i64
    }

    fn offset_millis(&self) -> i64 {
        self.offset.as_millis() as i64
    }
}

impl WindowFn for SlidingWindows {
    fn assign_windows(&self, timestamp_millis: i64) -> Vec<BoundedWindow> {
        let size = self.size_millis();
        let period = self.period_millis();
        let offset = self.offset_millis();

        if size <= 0 || period <= 0 {
            return vec![BoundedWindow::Interval(IntervalWindow::new(
                timestamp_millis,
                timestamp_millis + 1,
            ))];
        }

        let last_start = timestamp_millis - (timestamp_millis - offset).rem_euclid(period);
        std::iter::successors(Some(last_start), |&start| Some(start - period))
            .take_while(|&start| start + size > timestamp_millis)
            .map(|start| BoundedWindow::Interval(IntervalWindow::new(start, start + size)))
            .collect()
    }

    fn urn(&self) -> &'static str {
        URN_WINDOW_FN_SLIDING_WINDOWS
    }

    fn payload(&self) -> Vec<u8> {
        proto::SlidingWindowsPayload {
            size: Some(duration_to_proto(self.size)),
            offset: Some(timestamp_to_proto(self.offset_millis())),
            period: Some(duration_to_proto(self.period)),
        }
        .encode_to_vec()
    }

    fn window_coder_urn(&self) -> &'static str {
        URN_INTERVAL_WINDOW
    }

    fn merge_status(&self) -> proto::merge_status::Enum {
        proto::merge_status::Enum::NonMerging
    }

    fn assigns_to_one_window(&self) -> bool {
        self.period >= self.size
    }

    fn box_clone(&self) -> Box<dyn WindowFn> {
        Box::new(*self)
    }
}

/// A window function that groups elements separated by less than `gap_size`. Each element gets
/// the window `[timestamp, timestamp + gap_size)`; grouping merges overlapping windows per key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sessions {
    gap_size: Duration,
}

impl Sessions {
    /// Creates session windows with the given minimum gap duration.
    pub fn with_gap_duration(gap_size: Duration) -> Self {
        Self { gap_size }
    }

    pub fn gap_size(&self) -> Duration {
        self.gap_size
    }

    fn gap_size_millis(&self) -> i64 {
        self.gap_size.as_millis() as i64
    }
}

impl WindowFn for Sessions {
    fn assign_windows(&self, timestamp_millis: i64) -> Vec<BoundedWindow> {
        let gap = self.gap_size_millis();
        vec![BoundedWindow::Interval(IntervalWindow::new(
            timestamp_millis,
            timestamp_millis + gap,
        ))]
    }

    fn urn(&self) -> &'static str {
        URN_WINDOW_FN_SESSION_WINDOWS
    }

    fn payload(&self) -> Vec<u8> {
        proto::SessionWindowsPayload {
            gap_size: Some(duration_to_proto(self.gap_size)),
        }
        .encode_to_vec()
    }

    fn window_coder_urn(&self) -> &'static str {
        URN_INTERVAL_WINDOW
    }

    fn merge_status(&self) -> proto::merge_status::Enum {
        proto::merge_status::Enum::NeedsMerge
    }

    fn assigns_to_one_window(&self) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn WindowFn> {
        Box::new(*self)
    }
}

/// Decodes a [`WindowFn`] from a Runner API [`proto::FunctionSpec`]. Missing payload fields
/// decode to zero; a missing sliding `period` decodes to `size`. Only standard URNs decode.
pub fn decode_window_fn(spec: &proto::FunctionSpec) -> Result<Arc<dyn WindowFn>, String> {
    match spec.urn.as_str() {
        URN_WINDOW_FN_GLOBAL_WINDOWS => Ok(Arc::new(GlobalWindows)),
        URN_WINDOW_FN_FIXED_WINDOWS => {
            let payload = proto::FixedWindowsPayload::decode(spec.payload.as_slice())
                .map_err(|e| format!("Failed to decode FixedWindowsPayload: {e}"))?;
            let size = payload
                .size
                .as_ref()
                .map(duration_from_proto)
                .unwrap_or(Duration::ZERO);
            let offset_millis = payload
                .offset
                .as_ref()
                .map(timestamp_from_proto)
                .unwrap_or(0);
            let offset = Duration::from_millis(offset_millis.max(0) as u64);
            Ok(Arc::new(FixedWindows::of(size).with_offset(offset)))
        }
        URN_WINDOW_FN_SLIDING_WINDOWS => {
            let payload = proto::SlidingWindowsPayload::decode(spec.payload.as_slice())
                .map_err(|e| format!("Failed to decode SlidingWindowsPayload: {e}"))?;
            let size = payload
                .size
                .as_ref()
                .map(duration_from_proto)
                .unwrap_or(Duration::ZERO);
            let period = payload
                .period
                .as_ref()
                .map(duration_from_proto)
                .unwrap_or(size);
            let offset_millis = payload
                .offset
                .as_ref()
                .map(timestamp_from_proto)
                .unwrap_or(0);
            let offset = Duration::from_millis(offset_millis.max(0) as u64);
            Ok(Arc::new(
                SlidingWindows::of(size).every(period).with_offset(offset),
            ))
        }
        URN_WINDOW_FN_SESSION_WINDOWS => {
            let payload = proto::SessionWindowsPayload::decode(spec.payload.as_slice())
                .map_err(|e| format!("Failed to decode SessionWindowsPayload: {e}"))?;
            let gap_size = payload
                .gap_size
                .as_ref()
                .map(duration_from_proto)
                .unwrap_or(Duration::ZERO);
            Ok(Arc::new(Sessions::with_gap_duration(gap_size)))
        }
        other => Err(format!("Unsupported window fn URN: '{other}'")),
    }
}

pub fn duration_to_proto(d: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

pub fn duration_from_proto(d: &prost_types::Duration) -> Duration {
    Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32)
}

pub fn timestamp_to_proto(millis: i64) -> prost_types::Timestamp {
    let seconds = millis.div_euclid(1000);
    let nanos = (millis.rem_euclid(1000) * 1_000_000) as i32;
    prost_types::Timestamp { seconds, nanos }
}

pub fn timestamp_from_proto(ts: &prost_types::Timestamp) -> i64 {
    ts.seconds.saturating_mul(1000) + (ts.nanos as i64) / 1_000_000
}
