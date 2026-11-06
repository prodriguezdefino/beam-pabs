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

//! Splittable event-range and rate-curve configuration for [`Generator`](super::Generator).

use serde::{Deserialize, Serialize};

use super::current_wallclock_ms;
use crate::config::{BASE_TIME_MS, NexmarkConfig, RateShape};

/// Parameters controlling how [`Generator`](super::Generator) synthesizes events and timestamps.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct GeneratorConfig {
    pub configuration: NexmarkConfig,
    delays_us: [i64; RateShape::MAX_STEPS],
    delay_count: usize,
    pub step_length_sec: i64,
    pub base_time: i64,
    pub first_event_id: i64,
    pub max_events: i64,
    pub first_event_number: i64,
    pub epoch_period_ms: i64,
    pub events_per_epoch: i64,
}

impl GeneratorConfig {
    pub const FIRST_AUCTION_ID: i64 = 1_000;
    pub const FIRST_PERSON_ID: i64 = 1_000;
    pub const FIRST_CATEGORY_ID: i64 = 10;

    pub const PERSON_PROPORTION: i64 = 1;
    pub const AUCTION_PROPORTION: i64 = 3;
    pub const BID_PROPORTION: i64 = 46;
    pub const PROPORTION_DENOMINATOR: i64 =
        Self::PERSON_PROPORTION + Self::AUCTION_PROPORTION + Self::BID_PROPORTION;

    /// Builds a generator configuration; `max_events_or_zero == 0` selects the largest
    /// count that fits within 64-bit byte-size estimation.
    pub fn new(
        configuration: NexmarkConfig,
        base_time: i64,
        first_event_id: i64,
        max_events_or_zero: i64,
        first_event_number: i64,
    ) -> Self {
        let (delays_us, delay_count) = configuration.rate_shape.delay_curve_us(
            configuration.first_event_rate,
            configuration.next_event_rate,
            configuration.rate_unit,
            configuration.num_event_generators,
        );
        let step_length_sec = i64::from(
            configuration
                .rate_shape
                .step_length_sec(configuration.rate_period_sec),
        );
        let max_events = if max_events_or_zero == 0 {
            let max_avg = i64::from(
                configuration
                    .avg_person_byte_size
                    .max(configuration.avg_auction_byte_size)
                    .max(configuration.avg_bid_byte_size),
            )
            .max(1);
            i64::MAX / (Self::PROPORTION_DENOMINATOR * max_avg)
        } else {
            max_events_or_zero
        };

        let delays = &delays_us[..delay_count];
        let (events_per_epoch, epoch_period_ms) = if delays.len() > 1 {
            delays
                .iter()
                .fold((0_i64, 0_i64), |(events, period_ms), &delay_us| {
                    let cycle_events = (step_length_sec * 1_000_000) / delay_us;
                    (
                        events + cycle_events,
                        period_ms + (cycle_events * delay_us) / 1_000,
                    )
                })
        } else {
            (0, 0)
        };

        Self {
            configuration,
            delays_us,
            delay_count,
            step_length_sec,
            base_time,
            first_event_id,
            max_events,
            first_event_number,
            epoch_period_ms,
            events_per_epoch,
        }
    }

    /// Builds the root generator configuration for `configuration`.
    pub fn from_nexmark_config(configuration: &NexmarkConfig) -> Self {
        let base_time = if configuration.use_wallclock_event_time {
            current_wallclock_ms()
        } else {
            BASE_TIME_MS
        };
        Self::new(*configuration, base_time, 0, configuration.num_events, 0)
    }

    /// Per-generator inter-event delays in microseconds across one rate cycle.
    pub fn inter_event_delays_us(&self) -> &[i64] {
        &self.delays_us[..self.delay_count]
    }

    /// Returns a copy covering `(first_event_id, max_events, first_event_number)`.
    pub const fn copy_with(
        &self,
        first_event_id: i64,
        max_events: i64,
        first_event_number: i64,
    ) -> Self {
        Self {
            first_event_id,
            max_events,
            first_event_number,
            ..*self
        }
    }

    /// Computes `(sub_first_event_id, sub_max_events)` for each of `n` parallel splits.
    pub fn split_bounds(&self, n: usize) -> impl Iterator<Item = (i64, i64)> + use<> {
        let count = n.max(1);
        let count_i64 = count as i64;
        let base_sub_max = self.max_events / count_i64;
        let total = self.max_events;
        (0..count).scan(self.first_event_id, move |next_id, idx| {
            let sub_max = if idx + 1 == count {
                total - base_sub_max * (count_i64 - 1)
            } else {
                base_sub_max
            };
            let start_id = *next_id;
            *next_id += sub_max;
            Some((start_id, sub_max))
        })
    }

    /// Splits this configuration into `n` parallel sub-configurations with disjoint event IDs.
    pub fn split(&self, n: usize) -> Vec<Self> {
        let base = *self;
        self.split_bounds(n)
            .map(move |(first_id, max_events)| {
                base.copy_with(first_id, max_events, base.first_event_number)
            })
            .collect()
    }

    /// Estimates the encoded byte size of `num_events` synthesized events.
    pub const fn estimated_bytes_for_events(&self, num_events: i64) -> i64 {
        let num_persons = (num_events * Self::PERSON_PROPORTION) / Self::PROPORTION_DENOMINATOR;
        let num_auctions = (num_events * Self::AUCTION_PROPORTION) / Self::PROPORTION_DENOMINATOR;
        let num_bids = (num_events * Self::BID_PROPORTION) / Self::PROPORTION_DENOMINATOR;
        num_persons * (self.configuration.avg_person_byte_size as i64)
            + num_auctions * (self.configuration.avg_auction_byte_size as i64)
            + num_bids * (self.configuration.avg_bid_byte_size as i64)
    }

    /// First unadjusted event ID covered by this configuration.
    pub const fn start_event_id(&self) -> i64 {
        self.first_event_id + self.first_event_number
    }

    /// Exclusive upper bound on unadjusted event IDs covered by this configuration.
    pub const fn stop_event_id(&self) -> i64 {
        self.first_event_id + self.first_event_number + self.max_events
    }

    /// Unadjusted event number after emitting `num_events` events.
    pub const fn next_event_number(&self, num_events: i64) -> i64 {
        self.first_event_number + num_events
    }

    /// Event number after applying deterministic `out_of_order_group_size` permutation.
    pub const fn next_adjusted_event_number(&self, num_events: i64) -> i64 {
        let group = if self.configuration.out_of_order_group_size > 1 {
            self.configuration.out_of_order_group_size
        } else {
            1
        };
        let event_number = self.next_event_number(num_events);
        (event_number / group) * group + (event_number * 953) % group
    }

    /// Event number whose timestamp bounds the watermark after emitting `num_events` events.
    pub const fn next_event_number_for_watermark(&self, num_events: i64) -> i64 {
        let group = if self.configuration.out_of_order_group_size > 1 {
            self.configuration.out_of_order_group_size
        } else {
            1
        };
        (self.next_event_number(num_events) / group) * group
    }

    /// Computes `(timestamp_ms, inter_event_delay_us)` for `event_number`.
    pub fn timestamp_and_inter_event_delay_us_for_event(&self, event_number: i64) -> (i64, i64) {
        let delays = self.inter_event_delays_us();
        if delays.len() <= 1 || self.events_per_epoch <= 0 {
            let delay_us = delays.first().copied().unwrap_or(1);
            return (self.base_time + (event_number * delay_us) / 1_000, delay_us);
        }

        let epoch_base =
            self.base_time + (event_number / self.events_per_epoch) * self.epoch_period_ms;
        let rem_in_epoch = event_number % self.events_per_epoch;
        let step_us = self.step_length_sec * 1_000_000;

        delays
            .iter()
            .scan((rem_in_epoch, 0_i64), |(rem, offset_ms), &delay_us| {
                let cycle_events = step_us / delay_us;
                if *rem < cycle_events {
                    let ts = epoch_base + *offset_ms + (*rem * delay_us) / 1_000;
                    Some(Some((ts, delay_us)))
                } else {
                    *rem -= cycle_events;
                    *offset_ms += (cycle_events * delay_us) / 1_000;
                    Some(None)
                }
            })
            .flatten()
            .next()
            .unwrap_or_else(|| {
                let last_delay = delays.last().copied().unwrap_or(1);
                (epoch_base + self.epoch_period_ms, last_delay)
            })
    }
}
