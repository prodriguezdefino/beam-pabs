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

//! Nexmark event model, query output types, size accounting, and binary coders.

mod coders;
mod results;

use std::cmp::Ordering;

use beam::Instant;
use serde::{Deserialize, Serialize};

pub use self::coders::{
    AuctionBidCoder, AuctionCoder, AuctionCountCoder, AuctionPriceCoder, BidCoder,
    BidsPerSessionCoder, CategoryPriceCoder, DoneCoder, EventCoder, IdNameReserveCoder,
    NameCityStateIdCoder, PersonCoder, SellerPriceCoder,
};
pub(crate) use self::coders::{decode_framed, encode_framed};
pub use self::results::{
    AuctionBid, AuctionCount, AuctionPrice, BidsPerSession, CategoryPrice, Done, IdNameReserve,
    NameCityStateId, SellerPrice,
};

/// Element with a deterministic byte-size estimate for throughput monitoring.
pub trait KnownSize {
    /// Estimated encoded byte size of this value.
    fn estimated_size_bytes(&self) -> u64;
}

/// Newly registered person in the auction system.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Person {
    pub id: i64,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: Instant,
    pub extra: String,
}

impl KnownSize for Person {
    fn estimated_size_bytes(&self) -> u64 {
        22 + (self.name.len()
            + self.email_address.len()
            + self.credit_card.len()
            + self.city.len()
            + self.state.len()
            + self.extra.len()) as u64
    }
}

/// Newly opened auction created by a seller.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Auction {
    pub id: i64,
    pub item_name: String,
    pub description: String,
    pub initial_bid: i64,
    pub reserve: i64,
    pub date_time: Instant,
    pub expires: Instant,
    pub seller: i64,
    pub category: i64,
    pub extra: String,
}

impl KnownSize for Auction {
    fn estimated_size_bytes(&self) -> u64 {
        59 + (self.item_name.len() + self.description.len() + self.extra.len()) as u64
    }
}

/// Bid placed on an auction by a bidder.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Bid {
    pub auction: i64,
    pub bidder: i64,
    pub price: i64,
    pub date_time: Instant,
    pub extra: String,
}

impl Bid {
    /// Orders bids by ascending price, breaking ties in favor of earlier timestamps.
    pub fn price_then_descending_time(&self, other: &Self) -> Ordering {
        self.price
            .cmp(&other.price)
            .then_with(|| other.date_time.cmp(&self.date_time))
    }

    /// Orders bids by ascending timestamp, then ascending price.
    pub fn ascending_time_then_price(&self, other: &Self) -> Ordering {
        self.date_time
            .cmp(&other.date_time)
            .then_with(|| self.price.cmp(&other.price))
    }
}

impl KnownSize for Bid {
    fn estimated_size_bytes(&self) -> u64 {
        33 + self.extra.len() as u64
    }
}

/// Union of the three Nexmark input events.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Event {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}

impl Event {
    /// Event time that the generator wrote into the inner event. It equals the timestamp of
    /// the element that the generator emitted.
    pub fn date_time(&self) -> Instant {
        match self {
            Self::Person(p) => p.date_time,
            Self::Auction(a) => a.date_time,
            Self::Bid(b) => b.date_time,
        }
    }

    /// Returns the inner event's `extra` payload.
    pub fn extra(&self) -> &str {
        match self {
            Self::Person(p) => &p.extra,
            Self::Auction(a) => &a.extra,
            Self::Bid(b) => &b.extra,
        }
    }

    /// Transforms the inner event's `extra` field in place and returns the updated event.
    pub fn map_extra(mut self, f: impl FnOnce(String) -> String) -> Self {
        match &mut self {
            Self::Person(p) => p.extra = f(std::mem::take(&mut p.extra)),
            Self::Auction(a) => a.extra = f(std::mem::take(&mut a.extra)),
            Self::Bid(b) => b.extra = f(std::mem::take(&mut b.extra)),
        }
        self
    }

    /// Prepends `"{annotation}: "` to the inner event's `extra` field.
    pub fn with_annotation(self, annotation: &str) -> Self {
        self.map_extra(|extra| format!("{annotation}: {extra}"))
    }

    /// Checks whether the inner event's `extra` field starts with `"{annotation}: "`.
    pub fn has_annotation(&self, annotation: &str) -> bool {
        strip_prefix_tag(annotation, self.extra()).is_some()
    }

    /// Removes a leading `"{annotation}: "` prefix from `extra` if present.
    pub fn without_annotation(self, annotation: &str) -> Self {
        self.map_extra(|extra| match strip_prefix_tag(annotation, &extra) {
            Some(rest) => rest.to_owned(),
            None => extra,
        })
    }
}

impl KnownSize for Event {
    fn estimated_size_bytes(&self) -> u64 {
        match self {
            Self::Person(p) => 1 + p.estimated_size_bytes(),
            Self::Auction(a) => 1 + a.estimated_size_bytes(),
            Self::Bid(b) => 1 + b.estimated_size_bytes(),
        }
    }
}

fn strip_prefix_tag<'a>(annotation: &str, extra: &'a str) -> Option<&'a str> {
    extra.strip_prefix(annotation)?.strip_prefix(": ")
}
