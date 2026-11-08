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

//! Reference model for [`Query2`](crate::queries::Query2).

use beam::Instant;

use super::bid_stream;
use crate::config::NexmarkConfig;
use crate::model::AuctionPrice;

/// Computes expected `(AuctionPrice, Instant)` outputs for Query 2 (`SELECTION`).
pub fn query2(config: &NexmarkConfig) -> Vec<(AuctionPrice, Instant)> {
    let skip = i64::from(config.auction_skip.max(1));
    bid_stream(config)
        .filter(|(bid, _)| bid.auction % skip == 0)
        .map(|(bid, ts)| {
            (
                AuctionPrice {
                    auction: bid.auction,
                    price: bid.price,
                },
                ts,
            )
        })
        .collect()
}
