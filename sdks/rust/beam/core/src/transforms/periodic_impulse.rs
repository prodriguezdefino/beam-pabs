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

//! Periodic impulse transform: emits an event at regular wall-clock intervals, for heartbeats,
//! slowly-changing dimension updates and periodic polls of external services.

use std::time::Duration;

use crate::transforms::PTransform;
use crate::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use crate::transforms::generate_sequence::GenerateSequence;
use crate::values::{PBegin, PCollection};

/// Produces impulses at a fixed interval. The expansion uses [`GenerateSequence`] and the
/// watermark advances with the events. Use it for slowly-changing side inputs, such as cache
/// tables, configuration or model weights that refresh periodically.
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use beam::pipeline::Pipeline;
/// use beam::transforms::PeriodicImpulse;
///
/// let p = Pipeline::new();
/// let impulses = p.apply(PeriodicImpulse::new("Heartbeat", Duration::from_secs(60)));
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct PeriodicImpulse {
    name: String,
    interval: Duration,
    max_read_time: Option<Duration>,
    limit: Option<i64>,
}

impl PeriodicImpulse {
    /// Creates a periodic impulse named `name` that fires every `interval`.
    pub fn new(name: impl Into<String>, interval: Duration) -> Self {
        Self {
            name: name.into(),
            interval,
            max_read_time: None,
            limit: None,
        }
    }

    /// Sets the maximum time that the transform emits impulses.
    pub fn with_max_read_time(mut self, max_read_time: Duration) -> Self {
        self.max_read_time = Some(max_read_time);
        self
    }

    /// Sets the total number of impulses. The output is then bounded.
    pub fn with_limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Returns the configured interval between impulses.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Returns the optional maximum read duration.
    pub fn max_read_time(&self) -> Option<Duration> {
        self.max_read_time
    }

    /// Returns the optional total impulse limit.
    pub fn limit(&self) -> Option<i64> {
        self.limit
    }
}

impl HasDisplayData for PeriodicImpulse {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "PeriodicImpulse");
        builder.add_integer("interval_ms", self.interval.as_millis() as i64);
        if let Some(max_time) = self.max_read_time {
            builder.add_integer("max_read_time_ms", max_time.as_millis() as i64);
        }
        if let Some(limit) = self.limit {
            builder.add_integer("limit", limit);
        }
    }
}

impl PTransform<PBegin> for PeriodicImpulse {
    type Output = PCollection<i64>;

    fn expand(&self, input: &PBegin) -> Self::Output {
        let mut seq = GenerateSequence::new(self.name.clone(), 0);
        if let Some(limit) = self.limit {
            seq = seq.with_end(limit);
        }
        seq = seq.with_period(self.interval).with_split_size(1);

        if let Some(max_time) = self.max_read_time {
            seq = seq.with_max_read_time(max_time);
        }

        input.apply(seq)
    }
}
