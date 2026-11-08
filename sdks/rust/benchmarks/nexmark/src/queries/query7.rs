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

//! Query 7 (`HIGHEST_BID`): bids matching the maximum price in each fixed window.

use std::time::Duration;

use beam::prelude::*;
use beam::transforms::Max;

use super::just_bids;
use crate::config::NexmarkConfig;
use crate::model::{Bid, Event};

/// Selects all [`Bid`] events equal to the per-window maximum price using a singleton side input.
#[derive(Clone, Copy, Debug)]
pub struct Query7 {
    window_size_sec: i64,
    fanout: i32,
}

impl Query7 {
    /// Creates a highest-bid transform over fixed windows of `window_size_sec` seconds.
    pub fn new(window_size_sec: i64) -> Self {
        Self {
            window_size_sec: window_size_sec.max(1),
            fanout: 1,
        }
    }

    /// Configures pre-aggregation shard count for the maximum-price combine (`<= 1` disables sharding).
    pub fn with_fanout(mut self, fanout: i32) -> Self {
        self.fanout = fanout;
        self
    }

    /// Creates a highest-bid transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.window_size_sec).with_fanout(config.fanout)
    }
}

impl PTransform<PCollection<Event>> for Query7 {
    type Output = PCollection<Bid>;

    fn expand(self, input: &PCollection<Event>) -> PCollection<Bid> {
        let size = Duration::from_secs(u64::try_from(self.window_size_sec).unwrap_or(1));
        let bids = just_bids(input).window_into("WindowBids", FixedWindows::of(size));

        let fanout = usize::try_from(self.fanout).unwrap_or(1);
        let max_prices = bids.map("BidToPrice", |bid| bid.price).apply(
            "MaxPrice",
            CombineGlobally::new(Max::new())
                .without_defaults()
                .with_fanout(fanout),
        );

        bids.par_do_fn_with_side_inputs(
            "Select",
            max_prices.as_singleton(),
            |bid: Bid, max_price, ctx| {
                if bid.price == max_price {
                    ctx.emit(bid)?;
                }
                Ok(())
            },
        )
    }
}
