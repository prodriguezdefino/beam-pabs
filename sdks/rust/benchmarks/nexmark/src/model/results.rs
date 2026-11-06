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

//! Output record types emitted by Nexmark queries.

use serde::{Deserialize, Serialize};

use super::{Auction, Bid, KnownSize};

/// Winning `(Auction, Bid)` pair emitted for each closed auction (Query 9).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuctionBid {
    pub auction: Auction,
    pub bid: Bid,
}

impl KnownSize for AuctionBid {
    fn estimated_size_bytes(&self) -> u64 {
        self.auction.estimated_size_bytes() + self.bid.estimated_size_bytes()
    }
}

/// Auction identifier with its bid count (Query 5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuctionCount {
    pub auction: i64,
    pub num: i64,
}

impl KnownSize for AuctionCount {
    fn estimated_size_bytes(&self) -> u64 {
        16
    }
}

/// Auction identifier with a bid price (Query 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuctionPrice {
    pub auction: i64,
    pub price: i64,
}

impl KnownSize for AuctionPrice {
    fn estimated_size_bytes(&self) -> u64 {
        16
    }
}

/// Bidder identifier with bid count in a session or window (Queries 11 and 12).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BidsPerSession {
    pub person_id: i64,
    pub bids_per_session: i64,
}

impl KnownSize for BidsPerSession {
    fn estimated_size_bytes(&self) -> u64 {
        16
    }
}

/// Average winning price for a category in a window (Query 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CategoryPrice {
    pub category: i64,
    pub price: i64,
    pub is_last: bool,
}

impl KnownSize for CategoryPrice {
    fn estimated_size_bytes(&self) -> u64 {
        17
    }
}

/// Completion marker emitted by Query 10.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Done {
    pub message: String,
}

impl KnownSize for Done {
    fn estimated_size_bytes(&self) -> u64 {
        self.message.len() as u64
    }
}

/// Person identifier, name, and auction reserve price (Query 8).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdNameReserve {
    pub id: i64,
    pub name: String,
    pub reserve: i64,
}

impl KnownSize for IdNameReserve {
    fn estimated_size_bytes(&self) -> u64 {
        17 + self.name.len() as u64
    }
}

/// Seller name, city, state, and auction identifier (Query 3).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NameCityStateId {
    pub name: String,
    pub city: String,
    pub state: String,
    pub id: i64,
}

impl KnownSize for NameCityStateId {
    fn estimated_size_bytes(&self) -> u64 {
        11 + (self.name.len() + self.city.len() + self.state.len()) as u64
    }
}

/// Seller identifier with moving-average selling price (Query 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SellerPrice {
    pub seller: i64,
    pub price: i64,
}

impl KnownSize for SellerPrice {
    fn estimated_size_bytes(&self) -> u64 {
        16
    }
}
