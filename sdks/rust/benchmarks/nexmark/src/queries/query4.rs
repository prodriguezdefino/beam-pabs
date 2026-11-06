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

//! Query 4 (`AVERAGE_PRICE_FOR_CATEGORY`): sliding-window mean winning price per category.

use std::time::Duration;

use beam::prelude::*;

use super::winning_bids::{WinningBids, expected_auction_duration_ms};
use crate::config::{NexmarkConfig, round_half_up};
use crate::model::{CategoryPrice, Event};

/// Computes the rounded mean winning bid price per category across sliding windows.
#[derive(Clone, Copy, Debug)]
pub struct Query4 {
    expected_auction_duration_ms: i64,
    window_size_sec: i64,
    window_period_sec: i64,
    fanout: i32,
}

impl Query4 {
    /// Creates a category-mean transform over sliding windows of `window_size_sec` every `window_period_sec`.
    pub fn new(
        expected_auction_duration_ms: i64,
        window_size_sec: i64,
        window_period_sec: i64,
    ) -> Self {
        Self {
            expected_auction_duration_ms: expected_auction_duration_ms.max(1),
            window_size_sec: window_size_sec.max(1),
            window_period_sec: window_period_sec.max(1),
            fanout: 1,
        }
    }

    /// Configures pre-aggregation shard count for category sums and counts (`<= 1` disables sharding).
    pub fn with_fanout(mut self, fanout: i32) -> Self {
        self.fanout = fanout;
        self
    }

    /// Creates a category-mean transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(
            expected_auction_duration_ms(config),
            config.window_size_sec,
            config.window_period_sec,
        )
        .with_fanout(config.fanout)
    }
}

impl PTransform<PCollection<Event>> for Query4 {
    type Output = PCollection<CategoryPrice>;

    fn expand(&self, input: &PCollection<Event>) -> PCollection<CategoryPrice> {
        let size = Duration::from_secs(u64::try_from(self.window_size_sec).unwrap_or(1));
        let period = Duration::from_secs(u64::try_from(self.window_period_sec).unwrap_or(1));
        let windows = SlidingWindows::of(size).every(period);

        let winning = input.apply(
            "WinningBids",
            WinningBids::new(self.expected_auction_duration_ms),
        );

        let means = if self.fanout > 1 {
            let fanout = i64::from(self.fanout);
            winning
                .map("ShardByAuction", move |ab| {
                    (
                        (ab.auction.category, ab.auction.id.rem_euclid(fanout)),
                        ab.bid.price,
                    )
                })
                .window_into("Window", windows)
                .fold_per_key(
                    "PreCombineMean",
                    (0_i64, 0_i64),
                    |(sum, count), price| (sum + price, count + 1),
                    add_sum_count,
                )
                .map("DropShard", |((category, _shard), partial)| {
                    (category, partial)
                })
                .fold_per_key("MergeMean", (0_i64, 0_i64), add_sum_count, add_sum_count)
                .map("ComputeMean", |(category, (sum, count))| {
                    let mean = if count == 0 {
                        0.0
                    } else {
                        (sum as f64) / (count as f64)
                    };
                    (category, mean)
                })
        } else {
            winning
                .map("Rekey", |ab| (ab.auction.category, ab.bid.price))
                .window_into("Window", windows)
                .mean_per_key("MeanPerCategory")
        };

        means.par_do_fn("Project", |(category, mean): (i64, f64), ctx| {
            ctx.emit(CategoryPrice {
                category,
                price: round_half_up(mean),
                is_last: ctx.pane().is_last(),
            })
        })
    }
}

const fn add_sum_count((ls, lc): (i64, i64), (rs, rc): (i64, i64)) -> (i64, i64) {
    (ls + rs, lc + rc)
}
