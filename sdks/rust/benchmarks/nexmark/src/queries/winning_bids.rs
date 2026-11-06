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

//! Stateful winning-bid resolution across open [`Auction`] and [`Bid`] streams.

use std::cmp::Ordering;
use std::time::Duration;

use beam::prelude::*;
use serde::{Deserialize, Serialize};

use super::{just_auctions, just_bids};
use crate::config::NexmarkConfig;
use crate::generator::GeneratorConfig;
use crate::model::{Auction, AuctionBid, Bid, Event};

/// Joins [`Auction`] and [`Bid`] events by auction ID and emits the winning bid when each auction expires.
#[derive(Clone, Copy, Debug)]
pub struct WinningBids {
    expected_auction_duration_ms: i64,
}

impl WinningBids {
    /// Creates a winning-bids transform using `expected_auction_duration_ms` to expire orphan bids.
    pub fn new(expected_auction_duration_ms: i64) -> Self {
        Self {
            expected_auction_duration_ms: expected_auction_duration_ms.max(1),
        }
    }

    /// Creates a winning-bids transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(expected_auction_duration_ms(config))
    }
}

impl PTransform<PCollection<Event>> for WinningBids {
    type Output = PCollection<AuctionBid>;

    fn expand(&self, input: &PCollection<Event>) -> PCollection<AuctionBid> {
        let auctions = just_auctions(input).map("AuctionById", |auction| {
            (auction.id, Event::Auction(auction))
        });
        let bids = just_bids(input).map("BidByAuction", |bid| (bid.auction, Event::Bid(bid)));

        auctions.flatten("MergeAuctionsAndBids", &bids).apply(
            "Join",
            ParDo::new(WinningBidsDoFn {
                expected_auction_duration_ms: self.expected_auction_duration_ms,
            }),
        )
    }
}

/// Computes the upper-bound auction duration in milliseconds at the lowest configured event rate.
pub fn expected_auction_duration_ms(config: &NexmarkConfig) -> i64 {
    let (delays, len) = config.rate_shape.delay_curve_us(
        config.first_event_rate.max(1),
        config.next_event_rate.max(1),
        config.rate_unit,
        config.num_event_generators.max(1),
    );
    let longest_delay_us = delays[..len].iter().copied().max().unwrap_or(0);
    let auction_span_us = ((longest_delay_us * GeneratorConfig::PROPORTION_DENOMINATOR)
        / GeneratorConfig::AUCTION_PROPORTION)
        * i64::from(config.num_in_flight_auctions.max(0));
    (auction_span_us + 999) / 1_000
}

pub(crate) fn auction_output_timestamp(auction: &Auction) -> Instant {
    (auction.expires - Duration::from_millis(1)).max(auction.date_time)
}

pub(crate) fn select_better_bid(current_best: Option<Bid>, candidate: Bid) -> Bid {
    match current_best {
        Some(best) if candidate.price_then_descending_time(&best) != Ordering::Greater => best,
        Some(_) | None => candidate,
    }
}

fn consider_bid(auction: &Auction, current_best: Option<Bid>, bid: Bid) -> Option<Bid> {
    if bid.date_time >= auction.expires {
        return current_best;
    }
    if bid.price < auction.reserve {
        Metrics::counter("WinningBids", "underReserve").inc();
        return current_best;
    }
    Some(select_better_bid(current_best, bid))
}

#[derive(State)]
struct WinningBidsState {
    auction: ValueState<Auction>,
    best_bid: ValueState<Bid>,
    pending_bids: BagState<Bid>,
    closed: ValueState<bool>,
}

#[derive(Timers)]
enum WinningBidsTimers {
    #[timer(event_time)]
    AuctionExpiry,
    #[timer(event_time)]
    OrphanBidCleanup,
    #[timer(event_time)]
    ClosedCleanup,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Register)]
struct WinningBidsDoFn {
    expected_auction_duration_ms: i64,
}

impl WinningBidsDoFn {
    fn cleanup_window(self) -> Duration {
        let ms =
            u64::try_from(self.expected_auction_duration_ms.saturating_mul(2).max(1)).unwrap_or(1);
        Duration::from_millis(ms)
    }
}

impl StatefulDoFn for WinningBidsDoFn {
    type Key = i64;
    type Value = Event;
    type Out = AuctionBid;
    type State = WinningBidsState;
    type Timer = WinningBidsTimers;

    fn process_element(
        &mut self,
        _key: &i64,
        value: Event,
        state: &mut WinningBidsState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        if state.closed.read()?.unwrap_or(false) {
            return Ok(());
        }
        match value {
            Event::Auction(auction) => {
                if state.auction.read()?.is_some() {
                    return Ok(());
                }
                let pending = state.pending_bids.read()?;
                let had_pending = !pending.is_empty();
                let best = pending
                    .into_iter()
                    .fold(state.best_bid.read()?, |acc, bid| {
                        consider_bid(&auction, acc, bid)
                    });
                if let Some(winner) = best {
                    state.best_bid.write(winner)?;
                }
                if had_pending {
                    state.pending_bids.clear()?;
                    ctx.timer(WinningBidsTimers::OrphanBidCleanup).clear()?;
                }

                let hold = auction_output_timestamp(&auction);
                let expires = auction.expires;
                state.auction.write(auction)?;
                ctx.timer(WinningBidsTimers::AuctionExpiry)
                    .hold(hold)
                    .at(expires)?;
                ctx.timer(WinningBidsTimers::ClosedCleanup)
                    .hold(Instant::END_OF_GLOBAL_WINDOW)
                    .at(expires + self.cleanup_window())
            }
            Event::Bid(bid) => match state.auction.read()? {
                Some(auction) => {
                    let prev = state.best_bid.read()?;
                    if let Some(winner) = consider_bid(&auction, prev, bid) {
                        state.best_bid.write(winner)?;
                    }
                    Ok(())
                }
                None => {
                    let deadline = bid.date_time + self.cleanup_window();
                    state.pending_bids.append(bid)?;
                    ctx.timer(WinningBidsTimers::OrphanBidCleanup).at(deadline)
                }
            },
            Event::Person(_) => Ok(()),
        }
    }

    fn on_timer(
        &mut self,
        timer: WinningBidsTimers,
        _key: &i64,
        state: &mut WinningBidsState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            WinningBidsTimers::AuctionExpiry => {
                if let Some(auction) = state.auction.read()? {
                    let emit_ts = auction_output_timestamp(&auction);
                    match state.best_bid.read()? {
                        Some(bid) => {
                            ctx.output(AuctionBid { auction, bid }).at(emit_ts).emit()?;
                        }
                        None => {
                            Metrics::counter("WinningBids", "noValidBids").inc();
                        }
                    }
                    state.auction.clear()?;
                    state.best_bid.clear()?;
                    state.pending_bids.clear()?;
                    state.closed.write(true)?;
                }
                Ok(())
            }
            WinningBidsTimers::OrphanBidCleanup => {
                if state.auction.read()?.is_none() && !state.closed.read()?.unwrap_or(false) {
                    let pending = state.pending_bids.read()?;
                    if !pending.is_empty() {
                        Metrics::counter("WinningBids", "noAuction").inc();
                        state.pending_bids.clear()?;
                    }
                }
                Ok(())
            }
            WinningBidsTimers::ClosedCleanup => state.closed.clear(),
        }
    }
}
