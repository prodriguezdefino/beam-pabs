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

//! Reference model for [`Query7`](crate::queries::Query7).

use std::collections::BTreeMap;
use std::time::Duration;

use beam::Instant;
use beam::coders::IntervalWindow;
use beam::windowing::FixedWindows;

use super::{assign_intervals, bid_stream};
use crate::config::NexmarkConfig;
use crate::model::Bid;

/// Computes expected `(Bid, Instant)` outputs for Query 7 (`HIGHEST_BID`).
pub fn query7(config: &NexmarkConfig) -> Vec<(Bid, Instant)> {
    let size = Duration::from_secs(u64::try_from(config.window_size_sec.max(1)).unwrap_or(1));
    let windows = FixedWindows::of(size);

    let by_window = bid_stream(config).fold(
        BTreeMap::<IntervalWindow, Vec<(Bid, Instant)>>::new(),
        |mut acc, (bid, ts)| {
            if let Some(window) = assign_intervals(&windows, ts).next() {
                acc.entry(window).or_default().push((bid, ts));
            }
            acc
        },
    );

    by_window
        .into_values()
        .flat_map(|bids| {
            let max_price = bids.iter().map(|(bid, _)| bid.price).max().unwrap_or(0);
            bids.into_iter()
                .filter(move |(bid, _)| bid.price == max_price)
        })
        .collect()
}
