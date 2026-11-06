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

//! Reference model for [`Query6`](crate::queries::Query6).

use std::collections::BTreeMap;

use super::winning_bids;
use crate::config::NexmarkConfig;
use crate::model::{Bid, SellerPrice};
use crate::queries::query6::{MAX_MOVING_BIDS, mean_bid_price, retain_latest_bids};

/// Computes final per-seller moving-average [`SellerPrice`] outputs for Query 6.
pub fn query6(config: &NexmarkConfig) -> Vec<SellerPrice> {
    let per_seller = winning_bids(config).into_iter().fold(
        BTreeMap::<i64, Vec<Bid>>::new(),
        |mut sellers, (ab, _ts)| {
            sellers.entry(ab.auction.seller).or_default().push(ab.bid);
            sellers
        },
    );

    per_seller
        .into_iter()
        .map(|(seller, bids)| {
            let recent = retain_latest_bids(bids, MAX_MOVING_BIDS);
            SellerPrice {
                seller,
                price: mean_bid_price(&recent),
            }
        })
        .collect()
}
