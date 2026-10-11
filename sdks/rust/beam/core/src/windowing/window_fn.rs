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

use super::error::WindowingError;
use crate::Instant;
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
    /// A half-open interval `[start, end)`.
    Interval(IntervalWindow),
}

impl BoundedWindow {
    /// The last timestamp inside the window.
    pub fn max_timestamp(&self) -> Instant {
        match self {
            Self::Global(_) => Instant::END_OF_GLOBAL_WINDOW,
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
    pub fn encode(
        &self,
        writer: &mut impl std::io::Write,
    ) -> Result<(), crate::coders::CoderError> {
        match self {
            Self::Global(g) => GlobalWindowCoder.encode(g, writer, Context::Nested),
            Self::Interval(i) => IntervalWindowCoder.encode(i, writer, Context::Nested),
        }
    }

    /// Decodes a window from `reader` given its coder URN.
    pub fn decode(
        urn: &str,
        reader: &mut impl std::io::Read,
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

/// Formats an interval window as `[start..end)` and the global window as `global`.
impl std::fmt::Display for BoundedWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Global(_) => f.write_str("global"),
            Self::Interval(w) => w.fmt(f),
        }
    }
}

/// Merge status of a window function.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MergeStatus {
    NonMerging,
    NeedsMerge,
    AlreadyMerged,
}

impl From<MergeStatus> for proto::merge_status::Enum {
    fn from(status: MergeStatus) -> Self {
        match status {
            MergeStatus::NonMerging => proto::merge_status::Enum::NonMerging,
            MergeStatus::NeedsMerge => proto::merge_status::Enum::NeedsMerge,
            MergeStatus::AlreadyMerged => proto::merge_status::Enum::AlreadyMerged,
        }
    }
}

impl TryFrom<proto::merge_status::Enum> for MergeStatus {
    type Error = WindowingError;

    fn try_from(proto: proto::merge_status::Enum) -> Result<Self, Self::Error> {
        match proto {
            proto::merge_status::Enum::NonMerging => Ok(Self::NonMerging),
            proto::merge_status::Enum::NeedsMerge => Ok(Self::NeedsMerge),
            proto::merge_status::Enum::AlreadyMerged => Ok(Self::AlreadyMerged),
            proto::merge_status::Enum::Unspecified => {
                Err(WindowingError::UnknownMergeStatus(proto as i32))
            }
        }
    }
}

impl TryFrom<i32> for MergeStatus {
    type Error = WindowingError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let proto_enum = proto::merge_status::Enum::try_from(value)
            .map_err(|_| WindowingError::UnknownMergeStatus(value))?;
        Self::try_from(proto_enum)
    }
}

/// Defines how the elements of a PCollection are assigned to windows.
pub trait WindowFn: Send + Sync + std::fmt::Debug + 'static {
    /// Assigns windows to an element given its event timestamp.
    fn assign_windows(&self, timestamp: Instant) -> Vec<BoundedWindow> {
        let mut windows = Vec::new();
        self.assign_windows_into(timestamp, &mut windows);
        windows
    }

    /// Assigns windows to an element at `timestamp`, and appends them to `out`.
    fn assign_windows_into(&self, timestamp: Instant, out: &mut Vec<BoundedWindow>);

    /// URN of this window function.
    fn urn(&self) -> &'static str;

    /// Protobuf payload of the FunctionSpec for this window function.
    fn payload(&self) -> Vec<u8>;

    /// URN of the coder used to encode this window function's windows.
    fn window_coder_urn(&self) -> &'static str;

    /// Merge status of this window function.
    fn merge_status(&self) -> MergeStatus;

    /// Whether this window function assigns every input to at most one window.
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
    fn assign_windows_into(&self, _timestamp: Instant, out: &mut Vec<BoundedWindow>) {
        out.push(BoundedWindow::Global(GlobalWindow));
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

    fn merge_status(&self) -> MergeStatus {
        MergeStatus::NonMerging
    }

    fn assigns_to_one_window(&self) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn WindowFn> {
        Box::new(*self)
    }
}

/// A window function that divides event time into equal, non-overlapping intervals
/// `[N * size + offset, (N + 1) * size + offset)`.
///
/// Local assignment operates at millisecond resolution (`Instant`).
/// Sub-millisecond or zero durations clamp to 1 ms.
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

    /// Returns the window duration.
    pub fn size(&self) -> Duration {
        self.size
    }

    /// Returns the window start offset.
    pub fn offset(&self) -> Duration {
        self.offset
    }

    fn size_millis(&self) -> i64 {
        duration_millis(self.size)
    }

    fn offset_millis(&self) -> i64 {
        duration_millis(self.offset)
    }
}

impl WindowFn for FixedWindows {
    fn assign_windows_into(&self, timestamp: Instant, out: &mut Vec<BoundedWindow>) {
        let timestamp_millis = timestamp.as_millis();
        let size = self.size_millis();
        let offset = self.offset_millis();
        if size <= 0 {
            out.push(interval(timestamp_millis, timestamp_millis + 1));
            return;
        }
        let start = timestamp_millis - (timestamp_millis - offset).rem_euclid(size);
        let end = start + size;
        out.push(interval(start, end));
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

    fn merge_status(&self) -> MergeStatus {
        MergeStatus::NonMerging
    }

    fn assigns_to_one_window(&self) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn WindowFn> {
        Box::new(*self)
    }
}

/// A window function that divides event time into intervals of `size` that start every
/// `period`, so windows overlap when `period < size`.
///
/// Local assignment operates at millisecond resolution (`Instant`).
/// Sub-millisecond or zero durations clamp to 1 ms.
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

    /// Returns the window duration.
    pub fn size(&self) -> Duration {
        self.size
    }

    /// Returns the period between consecutive window starts.
    pub fn period(&self) -> Duration {
        self.period
    }

    /// Returns the window start offset.
    pub fn offset(&self) -> Duration {
        self.offset
    }

    fn size_millis(&self) -> i64 {
        duration_millis(self.size)
    }

    fn period_millis(&self) -> i64 {
        duration_millis(self.period)
    }

    fn offset_millis(&self) -> i64 {
        duration_millis(self.offset)
    }
}

impl WindowFn for SlidingWindows {
    fn assign_windows_into(&self, timestamp: Instant, out: &mut Vec<BoundedWindow>) {
        let timestamp_millis = timestamp.as_millis();
        let size = self.size_millis();
        let period = self.period_millis();
        let offset = self.offset_millis();

        if size <= 0 || period <= 0 {
            out.push(interval(timestamp_millis, timestamp_millis + 1));
            return;
        }

        let last_start = timestamp_millis - (timestamp_millis - offset).rem_euclid(period);
        let iter = std::iter::successors(Some(last_start), |&start| Some(start - period))
            .take_while(|&start| start + size > timestamp_millis)
            .map(|start| interval(start, start + size));
        out.extend(iter);
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

    fn merge_status(&self) -> MergeStatus {
        MergeStatus::NonMerging
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
///
/// Local assignment operates at millisecond resolution (`Instant`).
/// Sub-millisecond or zero durations clamp to 1 ms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sessions {
    gap_size: Duration,
}

impl Sessions {
    /// Creates session windows with the given minimum gap duration.
    pub fn with_gap_duration(gap_size: Duration) -> Self {
        Self { gap_size }
    }

    /// Returns the minimum gap duration between sessions.
    pub fn gap_size(&self) -> Duration {
        self.gap_size
    }

    fn gap_size_millis(&self) -> i64 {
        duration_millis(self.gap_size).max(1)
    }
}

impl WindowFn for Sessions {
    fn assign_windows_into(&self, timestamp: Instant, out: &mut Vec<BoundedWindow>) {
        let timestamp_millis = timestamp.as_millis();
        let gap = self.gap_size_millis();
        out.push(interval(timestamp_millis, timestamp_millis + gap));
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

    fn merge_status(&self) -> MergeStatus {
        MergeStatus::NeedsMerge
    }

    fn assigns_to_one_window(&self) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn WindowFn> {
        Box::new(*self)
    }
}

/// The interval window `[start_millis, end_millis)`.
fn interval(start_millis: i64, end_millis: i64) -> BoundedWindow {
    BoundedWindow::Interval(IntervalWindow::new(
        Instant::from_millis(start_millis),
        Instant::from_millis(end_millis),
    ))
}

/// Decodes a [`WindowFn`] from a Runner API [`proto::FunctionSpec`]. Missing payload fields
/// decode to zero; a missing sliding `period` decodes to `size`. Only standard URNs decode.
/// Negative offsets clamp to zero.
pub fn decode_window_fn(spec: &proto::FunctionSpec) -> Result<Arc<dyn WindowFn>, WindowingError> {
    match spec.urn.as_str() {
        URN_WINDOW_FN_GLOBAL_WINDOWS => Ok(Arc::new(GlobalWindows)),
        URN_WINDOW_FN_FIXED_WINDOWS => {
            let payload =
                proto::FixedWindowsPayload::decode(spec.payload.as_slice()).map_err(|e| {
                    WindowingError::DecodePayload(format!(
                        "Failed to decode FixedWindowsPayload: {e}"
                    ))
                })?;
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
            let payload =
                proto::SlidingWindowsPayload::decode(spec.payload.as_slice()).map_err(|e| {
                    WindowingError::DecodePayload(format!(
                        "Failed to decode SlidingWindowsPayload: {e}"
                    ))
                })?;
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
            let payload =
                proto::SessionWindowsPayload::decode(spec.payload.as_slice()).map_err(|e| {
                    WindowingError::DecodePayload(format!(
                        "Failed to decode SessionWindowsPayload: {e}"
                    ))
                })?;
            let gap_size = payload
                .gap_size
                .as_ref()
                .map(duration_from_proto)
                .unwrap_or(Duration::ZERO);
            Ok(Arc::new(Sessions::with_gap_duration(gap_size)))
        }
        other => Err(WindowingError::UnsupportedWindowFn(other.to_string())),
    }
}

/// Converts a [`Duration`] into a Protobuf [`prost_types::Duration`].
pub fn duration_to_proto(d: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

/// Converts a Protobuf [`prost_types::Duration`] into a [`Duration`].
pub(crate) fn duration_from_proto(d: &prost_types::Duration) -> Duration {
    Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32)
}

/// Converts epoch milliseconds into a Protobuf [`prost_types::Timestamp`].
pub(crate) fn timestamp_to_proto(millis: i64) -> prost_types::Timestamp {
    let seconds = millis.div_euclid(1000);
    let nanos = (millis.rem_euclid(1000) * 1_000_000) as i32;
    prost_types::Timestamp { seconds, nanos }
}

/// Converts a Protobuf [`prost_types::Timestamp`] into epoch milliseconds.
pub(crate) fn timestamp_from_proto(ts: &prost_types::Timestamp) -> i64 {
    ts.seconds
        .saturating_mul(1000)
        .saturating_add((ts.nanos as i64) / 1_000_000)
}

fn duration_millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}
