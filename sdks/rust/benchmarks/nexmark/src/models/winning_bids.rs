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

//! Reference model for [`WinningBids`](crate::queries::WinningBids).

use std::collections::BTreeMap;

use beam::Instant;

use super::standard_event_stream;
use crate::config::NexmarkConfig;
use crate::model::{Auction, AuctionBid, Bid, Event};
use crate::queries::winning_bids::{auction_output_timestamp, select_better_bid};

/// Computes expected `(AuctionBid, Instant)` outputs for closed auctions with valid winning bids.
pub fn winning_bids(config: &NexmarkConfig) -> Vec<(AuctionBid, Instant)> {
    let (auctions, bids) = standard_event_stream(config).into_iter().fold(
        (BTreeMap::<i64, Auction>::new(), Vec::<Bid>::new()),
        |(mut auctions, mut bids), (event, _ts)| {
            match event {
                Event::Auction(auction) => {
                    auctions.entry(auction.id).or_insert(auction);
                }
                Event::Bid(bid) => bids.push(bid),
                Event::Person(_) => {}
            }
            (auctions, bids)
        },
    );

    let mut best_bids = bids
        .into_iter()
        .fold(BTreeMap::<i64, Bid>::new(), |mut best, bid| {
            if let Some(auction) = auctions.get(&bid.auction)
                && bid.date_time < auction.expires
                && bid.price >= auction.reserve
            {
                let prev = best.remove(&bid.auction);
                best.insert(bid.auction, select_better_bid(prev, bid));
            }
            best
        });

    let mut resolved: Vec<(AuctionBid, Instant)> = auctions
        .into_iter()
        .filter_map(|(id, auction)| {
            let bid = best_bids.remove(&id)?;
            let emit_ts = auction_output_timestamp(&auction);
            Some((AuctionBid { auction, bid }, emit_ts))
        })
        .collect();
    resolved.sort_by_key(|(ab, ts)| (*ts, ab.auction.id));
    resolved
}
