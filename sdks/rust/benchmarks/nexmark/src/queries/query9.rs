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

//! Query 9 (`WINNING_BIDS`): emits the winning [`AuctionBid`] for every closed auction.

use beam::prelude::*;

use super::winning_bids::{WinningBids, expected_auction_duration_ms};
use crate::config::NexmarkConfig;
use crate::model::{AuctionBid, Event};

/// Selects the winning [`AuctionBid`] for each closed auction using [`WinningBids`].
#[derive(Clone, Copy, Debug)]
pub struct Query9 {
    expected_auction_duration_ms: i64,
}

impl Query9 {
    /// Creates a winning-bids query transform with `expected_auction_duration_ms` orphan retention.
    pub fn new(expected_auction_duration_ms: i64) -> Self {
        Self {
            expected_auction_duration_ms: expected_auction_duration_ms.max(1),
        }
    }

    /// Creates a winning-bids query transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(expected_auction_duration_ms(config))
    }
}

impl PTransform<PCollection<Event>> for Query9 {
    type Output = PCollection<AuctionBid>;

    fn expand(self, input: &PCollection<Event>) -> PCollection<AuctionBid> {
        input.apply(
            "WinningBids",
            WinningBids::new(self.expected_auction_duration_ms),
        )
    }
}
