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

use crate::Instant;

/// Estimates the lower bound on output timestamps of a streaming source or Splittable DoFn.
pub trait WatermarkEstimator: Send + Sync + 'static {
    /// Returns the current estimated output watermark.
    fn current_watermark(&self) -> Instant;
}

/// A watermark estimator updated manually by user code.
#[derive(Debug)]
pub struct ManualWatermarkEstimator {
    watermark: AtomicI64,
}

impl Default for ManualWatermarkEstimator {
    fn default() -> Self {
        Self::new(Instant::MIN)
    }
}

impl ManualWatermarkEstimator {
    pub fn new(initial_watermark: Instant) -> Self {
        Self {
            watermark: AtomicI64::new(initial_watermark.as_millis()),
        }
    }

    pub fn set_watermark(&self, watermark: Instant) {
        self.watermark
            .store(watermark.as_millis(), Ordering::Release);
    }
}

impl WatermarkEstimator for ManualWatermarkEstimator {
    fn current_watermark(&self) -> Instant {
        Instant::from_millis(self.watermark.load(Ordering::Acquire))
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
        Self::new(Instant::MIN)
    }
}

impl TimestampObservingWatermarkEstimator {
    pub fn new(initial_watermark: Instant) -> Self {
        Self {
            watermark: AtomicI64::new(initial_watermark.as_millis()),
        }
    }

    /// Observes the event timestamp of an emitted element. A timestamp below the current
    /// watermark has no effect.
    pub fn observe_timestamp(&self, timestamp: Instant) {
        self.watermark
            .fetch_max(timestamp.as_millis(), Ordering::AcqRel);
    }
}

impl WatermarkEstimator for TimestampObservingWatermarkEstimator {
    fn current_watermark(&self) -> Instant {
        Instant::from_millis(self.watermark.load(Ordering::Acquire))
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
    fn current_watermark(&self) -> Instant {
        Instant::now()
    }
}

/// Converts a watermark to a Protobuf Timestamp.
pub fn watermark_to_proto(watermark: Instant) -> prost_types::Timestamp {
    let millis = watermark.as_millis();
    let seconds = millis.div_euclid(1000);
    let nanos = (millis.rem_euclid(1000) * 1_000_000) as i32;
    prost_types::Timestamp { seconds, nanos }
}

/// Converts a Protobuf Timestamp to a watermark.
pub fn watermark_from_proto(ts: &prost_types::Timestamp) -> Instant {
    Instant::from_millis(ts.seconds.saturating_mul(1000) + (ts.nanos as i64) / 1_000_000)
}
