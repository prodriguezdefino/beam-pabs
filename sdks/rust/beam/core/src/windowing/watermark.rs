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

//! Watermark estimators for streaming sources and Splittable DoFns.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Minimum timestamp in Beam event time, in milliseconds (0001-01-01T00:00:00Z).
pub const BEAM_MIN_TIMESTAMP: i64 = -62_135_596_800_000;

/// Maximum timestamp in Beam event time, in milliseconds (9999-12-31T23:59:59.999Z).
pub const BEAM_MAX_TIMESTAMP: i64 = 253_402_300_799_999;

/// Estimates the lower bound on output timestamps of a streaming source or Splittable DoFn.
pub trait WatermarkEstimator: Send + Sync + 'static {
    /// Returns the current estimated output watermark in milliseconds.
    fn current_watermark(&self) -> i64;
}

/// A watermark estimator updated manually by user code.
#[derive(Debug)]
pub struct ManualWatermarkEstimator {
    watermark: AtomicI64,
}

impl Default for ManualWatermarkEstimator {
    fn default() -> Self {
        Self::new(BEAM_MIN_TIMESTAMP)
    }
}

impl ManualWatermarkEstimator {
    pub fn new(initial_watermark: i64) -> Self {
        Self {
            watermark: AtomicI64::new(initial_watermark),
        }
    }

    pub fn set_watermark(&self, watermark: i64) {
        self.watermark.store(watermark, Ordering::Release);
    }
}

impl WatermarkEstimator for ManualWatermarkEstimator {
    fn current_watermark(&self) -> i64 {
        self.watermark.load(Ordering::Acquire)
    }
}

/// A watermark estimator that advances to the largest output timestamp it observes. The
/// watermark never moves back.
#[derive(Debug)]
pub struct TimestampObservingWatermarkEstimator {
    watermark: AtomicI64,
}

impl Default for TimestampObservingWatermarkEstimator {
    fn default() -> Self {
        Self::new(BEAM_MIN_TIMESTAMP)
    }
}

impl TimestampObservingWatermarkEstimator {
    pub fn new(initial_watermark: i64) -> Self {
        Self {
            watermark: AtomicI64::new(initial_watermark),
        }
    }

    /// Observes the event timestamp of an emitted element. A timestamp below the current
    /// watermark has no effect.
    pub fn observe_timestamp(&self, timestamp: i64) {
        let mut current = self.watermark.load(Ordering::Acquire);
        while timestamp > current {
            match self.watermark.compare_exchange_weak(
                current,
                timestamp,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

impl WatermarkEstimator for TimestampObservingWatermarkEstimator {
    fn current_watermark(&self) -> i64 {
        self.watermark.load(Ordering::Acquire)
    }
}

/// A watermark estimator that reports the system wall-clock time.
#[derive(Debug, Default, Clone, Copy)]
pub struct WallTimeWatermarkEstimator;

impl WallTimeWatermarkEstimator {
    pub fn new() -> Self {
        Self
    }
}

impl WatermarkEstimator for WallTimeWatermarkEstimator {
    fn current_watermark(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// Converts a millisecond timestamp to a Protobuf Timestamp.
pub fn watermark_to_proto(watermark_millis: i64) -> prost_types::Timestamp {
    let seconds = watermark_millis.div_euclid(1000);
    let nanos = (watermark_millis.rem_euclid(1000) * 1_000_000) as i32;
    prost_types::Timestamp { seconds, nanos }
}

/// Converts a Protobuf Timestamp to a millisecond timestamp.
pub fn watermark_from_proto(ts: &prost_types::Timestamp) -> i64 {
    ts.seconds.saturating_mul(1000) + (ts.nanos as i64) / 1_000_000
}
