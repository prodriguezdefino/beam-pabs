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

//! Query identifiers and predefined suite configurations.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::NexmarkConfig;

/// Identifier of a query in the Nexmark suite.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NexmarkQueryName {
    Passthrough,
    CurrencyConversion,
    Selection,
    LocalItemSuggestion,
    AveragePriceForCategory,
    HotItems,
    AverageSellingPriceBySeller,
    HighestBid,
    MonitorNewUsers,
    WinningBids,
    LogToShardedFiles,
    UserSessions,
    ProcessingTimeWindows,
    PortabilityBatch,
    Reshuffle,
    BoundedSideInputJoin,
    SessionSideInputJoin,
}

impl NexmarkQueryName {
    /// All queries in suite execution order.
    pub const ALL: &'static [Self] = &[
        Self::Passthrough,
        Self::CurrencyConversion,
        Self::Selection,
        Self::LocalItemSuggestion,
        Self::AveragePriceForCategory,
        Self::HotItems,
        Self::AverageSellingPriceBySeller,
        Self::HighestBid,
        Self::MonitorNewUsers,
        Self::WinningBids,
        Self::LogToShardedFiles,
        Self::UserSessions,
        Self::ProcessingTimeWindows,
        Self::PortabilityBatch,
        Self::Reshuffle,
        Self::BoundedSideInputJoin,
        Self::SessionSideInputJoin,
    ];

    /// Numeric identifier (`0..=16`).
    pub const fn number(self) -> i32 {
        match self {
            Self::Passthrough => 0,
            Self::CurrencyConversion => 1,
            Self::Selection => 2,
            Self::LocalItemSuggestion => 3,
            Self::AveragePriceForCategory => 4,
            Self::HotItems => 5,
            Self::AverageSellingPriceBySeller => 6,
            Self::HighestBid => 7,
            Self::MonitorNewUsers => 8,
            Self::WinningBids => 9,
            Self::LogToShardedFiles => 10,
            Self::UserSessions => 11,
            Self::ProcessingTimeWindows => 12,
            Self::BoundedSideInputJoin => 13,
            Self::SessionSideInputJoin => 14,
            Self::PortabilityBatch => 15,
            Self::Reshuffle => 16,
        }
    }

    /// Uppercase identifier string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passthrough => "PASSTHROUGH",
            Self::CurrencyConversion => "CURRENCY_CONVERSION",
            Self::Selection => "SELECTION",
            Self::LocalItemSuggestion => "LOCAL_ITEM_SUGGESTION",
            Self::AveragePriceForCategory => "AVERAGE_PRICE_FOR_CATEGORY",
            Self::HotItems => "HOT_ITEMS",
            Self::AverageSellingPriceBySeller => "AVERAGE_SELLING_PRICE_BY_SELLER",
            Self::HighestBid => "HIGHEST_BID",
            Self::MonitorNewUsers => "MONITOR_NEW_USERS",
            Self::WinningBids => "WINNING_BIDS",
            Self::LogToShardedFiles => "LOG_TO_SHARDED_FILES",
            Self::UserSessions => "USER_SESSIONS",
            Self::ProcessingTimeWindows => "PROCESSING_TIME_WINDOWS",
            Self::PortabilityBatch => "PORTABILITY_BATCH",
            Self::Reshuffle => "RESHUFFLE",
            Self::BoundedSideInputJoin => "BOUNDED_SIDE_INPUT_JOIN",
            Self::SessionSideInputJoin => "SESSION_SIDE_INPUT_JOIN",
        }
    }

    /// Looks up a query by its numeric identifier.
    pub fn from_number(number: i32) -> Option<Self> {
        Self::ALL.iter().copied().find(|q| q.number() == number)
    }
}

impl fmt::Display for NexmarkQueryName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NexmarkQueryName {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<i32>() {
            Ok(num) => Self::from_number(num).ok_or_else(|| format!("unknown query number: {num}")),
            Err(_) => Self::ALL
                .iter()
                .copied()
                .find(|q| q.as_str().eq_ignore_ascii_case(s))
                .ok_or_else(|| format!("unknown query: {s}")),
        }
    }
}

/// Predefined Nexmark configuration presets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NexmarkSuite {
    /// Single default configuration (`100_000` events).
    #[default]
    Default,
    /// All queries with `100_000` events (`10_000` for Queries 4, 6, and 9).
    Smoke,
    /// All queries scaled 100x above [`Self::Smoke`].
    Stress,
    /// All queries scaled 10,000x above [`Self::Smoke`].
    FullThrottle,
}

impl NexmarkSuite {
    /// Builds the baseline configurations in this suite.
    pub fn configs(self) -> Vec<NexmarkConfig> {
        match self {
            Self::Default => vec![NexmarkConfig::default()],
            Self::Smoke => smoke_configs(1),
            Self::Stress => smoke_configs(100),
            Self::FullThrottle => smoke_configs(10_000),
        }
    }
}

impl FromStr for NexmarkSuite {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "DEFAULT" => Ok(Self::Default),
            "SMOKE" => Ok(Self::Smoke),
            "STRESS" => Ok(Self::Stress),
            "FULL_THROTTLE" => Ok(Self::FullThrottle),
            _ => Err(format!("unknown Nexmark suite: {s}")),
        }
    }
}

fn smoke_configs(multiplier: i64) -> Vec<NexmarkConfig> {
    NexmarkQueryName::ALL
        .iter()
        .copied()
        .map(|query| {
            let base_events = match query {
                NexmarkQueryName::AveragePriceForCategory
                | NexmarkQueryName::AverageSellingPriceBySeller
                | NexmarkQueryName::WinningBids => 10_000,
                NexmarkQueryName::Passthrough
                | NexmarkQueryName::CurrencyConversion
                | NexmarkQueryName::Selection
                | NexmarkQueryName::LocalItemSuggestion
                | NexmarkQueryName::HotItems
                | NexmarkQueryName::HighestBid
                | NexmarkQueryName::MonitorNewUsers
                | NexmarkQueryName::LogToShardedFiles
                | NexmarkQueryName::UserSessions
                | NexmarkQueryName::ProcessingTimeWindows
                | NexmarkQueryName::PortabilityBatch
                | NexmarkQueryName::Reshuffle
                | NexmarkQueryName::BoundedSideInputJoin
                | NexmarkQueryName::SessionSideInputJoin => 100_000,
            };
            NexmarkConfig {
                query: Some(query),
                num_events: base_events * multiplier,
                ..NexmarkConfig::default()
            }
        })
        .collect()
}
