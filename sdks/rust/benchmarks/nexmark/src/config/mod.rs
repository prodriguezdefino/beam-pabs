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

//! Configuration, rate curves, query identifiers, and benchmark suites for Nexmark.

mod rate;
mod suite;

use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub(crate) use self::rate::round_half_up;
pub use self::rate::{RateShape, RateUnit};
pub use self::suite::{NexmarkQueryName, NexmarkSuite};

/// Base event timestamp (`2015-07-15T00:00:00.000Z`) in milliseconds since the Unix epoch.
pub const BASE_TIME_MS: i64 = 1_436_918_400_000;

/// Event source kind for a Nexmark pipeline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceType {
    /// Synthesize events directly in the pipeline.
    #[default]
    Direct,
}

impl FromStr for SourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "DIRECT" => Ok(Self::Direct),
            _ => Err(format!("unknown source type: {s}")),
        }
    }
}

/// Output sink kind for query results.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SinkType {
    /// Count results without formatting strings.
    CountOnly,
    /// Format results as strings and discard them.
    #[default]
    Devnull,
    /// Write formatted results to text files.
    Text,
}

impl FromStr for SinkType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "COUNT_ONLY" => Ok(Self::CountOnly),
            "DEVNULL" => Ok(Self::Devnull),
            "TEXT" => Ok(Self::Text),
            _ => Err(format!("unknown sink type: {s}")),
        }
    }
}

/// Parameters for event synthesis and query execution.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct NexmarkConfig {
    pub query: Option<NexmarkQueryName>,
    pub source_type: SourceType,
    pub sink_type: SinkType,
    pub side_input_row_count: i32,
    pub session_gap_sec: i64,
    /// Total events to synthesize (`0` selects the largest count that fits byte estimates).
    pub num_events: i64,
    pub num_event_generators: i32,
    pub rate_shape: RateShape,
    pub first_event_rate: i32,
    pub next_event_rate: i32,
    pub rate_unit: RateUnit,
    pub rate_period_sec: i32,
    /// Seconds that a streaming run may last after its first event. The launcher then
    /// cancels the job.
    pub stream_timeout: i32,
    pub is_rate_limited: bool,
    /// Anchor event timestamps to wallclock time instead of [`BASE_TIME_MS`].
    pub use_wallclock_event_time: bool,
    pub avg_person_byte_size: i32,
    pub avg_auction_byte_size: i32,
    pub avg_bid_byte_size: i32,
    /// Reciprocal of non-hot selection probability (`1 - 1 / ratio` hot).
    pub hot_auction_ratio: i32,
    pub hot_sellers_ratio: i32,
    pub hot_bidders_ratio: i32,
    pub window_size_sec: i64,
    pub window_period_sec: i64,
    pub watermark_holdback_sec: i64,
    pub num_in_flight_auctions: i32,
    pub num_active_people: i32,
    pub auction_skip: i32,
    pub fanout: i32,
    /// State retention in seconds for `Person` entries in Query 3.
    pub max_auctions_waiting_time: i32,
    pub occasional_delay_sec: i64,
    pub prob_delayed_event: f64,
    pub max_log_events: i32,
    /// Group size for deterministic out-of-order permutation (`1` keeps strict order).
    pub out_of_order_group_size: i64,
    pub num_key_buckets: i32,
    pub pardo_cpu_factor: f64,
}

impl Default for NexmarkConfig {
    fn default() -> Self {
        Self {
            query: None,
            source_type: SourceType::Direct,
            sink_type: SinkType::Devnull,
            side_input_row_count: 500,
            session_gap_sec: 600,
            num_events: 100_000,
            num_event_generators: 100,
            rate_shape: RateShape::Sine,
            first_event_rate: 10_000,
            next_event_rate: 10_000,
            rate_unit: RateUnit::PerSecond,
            rate_period_sec: 600,
            stream_timeout: 240,
            is_rate_limited: false,
            use_wallclock_event_time: false,
            avg_person_byte_size: 200,
            avg_auction_byte_size: 500,
            avg_bid_byte_size: 100,
            hot_auction_ratio: 2,
            hot_sellers_ratio: 4,
            hot_bidders_ratio: 4,
            window_size_sec: 10,
            window_period_sec: 5,
            watermark_holdback_sec: 0,
            num_in_flight_auctions: 100,
            num_active_people: 1_000,
            auction_skip: 123,
            fanout: 5,
            max_auctions_waiting_time: 600,
            occasional_delay_sec: 3,
            prob_delayed_event: 0.1,
            max_log_events: 100_000,
            out_of_order_group_size: 1,
            num_key_buckets: 20_000,
            pardo_cpu_factor: 1.0,
        }
    }
}
