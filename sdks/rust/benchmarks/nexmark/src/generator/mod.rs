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

//! Deterministic Nexmark event synthesis and splittable generator state.

pub(crate) mod auction;
mod bid;
mod config;
mod java_random;
mod person;
mod strings;

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub use self::config::GeneratorConfig;
pub use self::java_random::JavaRandom;
pub(crate) use self::strings::next_long;
use crate::model::Event;

const AUCTION_END_REM: i64 =
    GeneratorConfig::PERSON_PROPORTION + GeneratorConfig::AUCTION_PROPORTION;

/// Snapshot of a [`Generator`] position and wallclock anchor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GeneratorCheckpoint {
    pub num_events: i64,
    /// Wallclock base timestamp in milliseconds since the Unix epoch (`-1` if unstarted).
    pub wallclock_base_time: i64,
}

impl GeneratorCheckpoint {
    pub const fn new(num_events: i64, wallclock_base_time: i64) -> Self {
        Self {
            num_events,
            wallclock_base_time,
        }
    }

    pub const fn to_generator(self, config: GeneratorConfig) -> Generator {
        Generator::with_state(config, self.num_events, self.wallclock_base_time)
    }
}

/// Synthesized event together with its wallclock target, event timestamp, and watermark.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NextEvent {
    pub wallclock_timestamp: i64,
    pub event_timestamp: i64,
    pub event: Event,
    pub watermark: i64,
}

impl NextEvent {
    /// Returns this event delayed by `delay_ms` on the wallclock timeline and marked `"LATE"`.
    pub fn with_delay(self, delay_ms: i64) -> Self {
        Self {
            wallclock_timestamp: self.wallclock_timestamp + delay_ms,
            event: self.event.with_annotation("LATE"),
            ..self
        }
    }
}

/// Deterministic iterator over synthesized [`NextEvent`] items.
#[derive(Clone, Debug, PartialEq)]
pub struct Generator {
    config: GeneratorConfig,
    events_count_so_far: i64,
    wallclock_base_time: i64,
}

impl Generator {
    /// Creates a generator starting at event `0` with an uninitialized wallclock anchor.
    pub const fn new(config: GeneratorConfig) -> Self {
        Self::with_state(config, 0, -1)
    }

    /// Creates a generator at `events_count_so_far` and `wallclock_base_time`.
    pub const fn with_state(
        config: GeneratorConfig,
        events_count_so_far: i64,
        wallclock_base_time: i64,
    ) -> Self {
        Self {
            config,
            events_count_so_far,
            wallclock_base_time,
        }
    }

    pub const fn to_checkpoint(&self) -> GeneratorCheckpoint {
        GeneratorCheckpoint::new(self.events_count_so_far, self.wallclock_base_time)
    }

    pub const fn current_config(&self) -> &GeneratorConfig {
        &self.config
    }

    pub const fn events_count_so_far(&self) -> i64 {
        self.events_count_so_far
    }

    pub const fn wallclock_base_time(&self) -> i64 {
        self.wallclock_base_time
    }

    /// Truncates this generator to stop at `event_id` and returns the remaining sub-range.
    pub fn split_at_event_id(&mut self, event_id: i64) -> GeneratorConfig {
        let new_max = event_id - (self.config.first_event_id + self.config.first_event_number);
        let remain = self.config.copy_with(
            self.config.first_event_id,
            self.config.max_events - new_max,
            self.config.first_event_number + new_max,
        );
        self.config = self.config.copy_with(
            self.config.first_event_id,
            new_max,
            self.config.first_event_number,
        );
        remain
    }

    /// Adjusted event ID that seeds the next synthesized event.
    pub const fn next_event_id(&self) -> i64 {
        self.config.first_event_id
            + self
                .config
                .next_adjusted_event_number(self.events_count_so_far)
    }

    pub const fn has_next(&self) -> bool {
        self.events_count_so_far < self.config.max_events
    }

    /// Microseconds until the next scheduled event on this generator's rate curve.
    pub fn current_inter_event_delay_us(&self) -> i64 {
        self.config
            .timestamp_and_inter_event_delay_us_for_event(
                self.config.next_event_number(self.events_count_so_far),
            )
            .1
    }
}

impl Iterator for Generator {
    type Item = NextEvent;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }
        if self.wallclock_base_time < 0 {
            self.wallclock_base_time = current_wallclock_ms();
        }

        let event_timestamp = self
            .config
            .timestamp_and_inter_event_delay_us_for_event(
                self.config.next_event_number(self.events_count_so_far),
            )
            .0;
        let adjusted_ts = self
            .config
            .timestamp_and_inter_event_delay_us_for_event(
                self.config
                    .next_adjusted_event_number(self.events_count_so_far),
            )
            .0;
        let watermark = self
            .config
            .timestamp_and_inter_event_delay_us_for_event(
                self.config
                    .next_event_number_for_watermark(self.events_count_so_far),
            )
            .0;
        let wallclock_timestamp =
            self.wallclock_base_time + (event_timestamp - self.config.base_time);

        let event_id = self.next_event_id();
        let mut random = JavaRandom::new(event_id);
        let event = match event_id % GeneratorConfig::PROPORTION_DENOMINATOR {
            ..GeneratorConfig::PERSON_PROPORTION => Event::Person(person::next_person(
                event_id,
                &mut random,
                adjusted_ts,
                &self.config,
            )),
            GeneratorConfig::PERSON_PROPORTION..AUCTION_END_REM => {
                Event::Auction(auction::next_auction(
                    self.events_count_so_far,
                    event_id,
                    &mut random,
                    adjusted_ts,
                    &self.config,
                ))
            }
            _ => Event::Bid(bid::next_bid(
                event_id,
                &mut random,
                adjusted_ts,
                &self.config,
            )),
        };

        self.events_count_so_far += 1;
        Some(NextEvent {
            wallclock_timestamp,
            event_timestamp: adjusted_ts,
            event,
            watermark,
        })
    }
}

pub(crate) fn current_wallclock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
