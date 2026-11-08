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

//! Nexmark query transforms.

mod bounded_side_input_join;
mod query0;
pub(crate) mod query1;
mod query10;
mod query11;
mod query12;
mod query13;
mod query14;
mod query2;
pub(crate) mod query3;
mod query4;
mod query5;
pub(crate) mod query6;
mod query7;
mod query8;
mod query9;
mod session_side_input_join;
pub(crate) mod side_input;
pub(crate) mod winning_bids;

use beam::prelude::*;

pub use self::bounded_side_input_join::BoundedSideInputJoin;
pub use self::query0::Query0;
pub use self::query1::Query1;
pub use self::query2::Query2;
pub use self::query3::Query3;
pub use self::query4::Query4;
pub use self::query5::Query5;
pub use self::query6::Query6;
pub use self::query7::Query7;
pub use self::query8::Query8;
pub use self::query9::Query9;
pub use self::query10::Query10;
pub use self::query11::Query11;
pub use self::query12::Query12;
pub use self::query13::Query13;
pub use self::query14::Query14;
pub use self::session_side_input_join::SessionSideInputJoin;
pub use self::side_input::SideInput;
pub use self::winning_bids::{WinningBids, expected_auction_duration_ms};
use crate::model::{Auction, Bid, Event, Person};

pub(crate) fn just_bids(events: &PCollection<Event>) -> PCollection<Bid> {
    events.flat_map("JustBids", |event| match event {
        Event::Bid(bid) => Some(bid),
        Event::Person(_) | Event::Auction(_) => None,
    })
}

pub(crate) fn just_auctions(events: &PCollection<Event>) -> PCollection<Auction> {
    events.flat_map("JustAuctions", |event| match event {
        Event::Auction(auction) => Some(auction),
        Event::Person(_) | Event::Bid(_) => None,
    })
}

pub(crate) fn just_persons(events: &PCollection<Event>) -> PCollection<Person> {
    events.flat_map("JustPersons", |event| match event {
        Event::Person(person) => Some(person),
        Event::Auction(_) | Event::Bid(_) => None,
    })
}
