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

//! Schema row forms of the input events, for Avro files and the `ROW` coder strategy.
//!
//! Each row type mirrors a domain type field by field, with the camelCase field names of the
//! Java Nexmark model. Times are epoch milliseconds, because the row schema has no field type
//! for [`Instant`]. The conversions are lossless in both directions.

use std::fmt;

use beam::Instant;
use beam::schema::BeamRow;

use super::{Auction, Bid, Event, Person};

/// Converts one field between its domain type and its row type.
trait RowField<D> {
    fn from_domain(value: D) -> Self;
    fn into_domain(self) -> D;
}

impl RowField<i64> for i64 {
    fn from_domain(value: i64) -> Self {
        value
    }
    fn into_domain(self) -> i64 {
        self
    }
}

impl RowField<String> for String {
    fn from_domain(value: String) -> Self {
        value
    }
    fn into_domain(self) -> String {
        self
    }
}

impl RowField<Instant> for i64 {
    fn from_domain(value: Instant) -> Self {
        value.as_millis()
    }
    fn into_domain(self) -> Instant {
        Instant::from_millis(self)
    }
}

/// Declares a row type that mirrors a domain struct, and the `From` conversions both ways.
macro_rules! mirror_row {
    (
        $(#[$meta:meta])*
        $row:ident <=> $domain:ident {
            $($field:ident: $ty:ty = $name:literal),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
        pub struct $row {
            $(
                #[beam(rename = $name)]
                pub $field: $ty,
            )*
        }

        impl From<$domain> for $row {
            fn from(value: $domain) -> Self {
                Self {
                    $($field: RowField::from_domain(value.$field),)*
                }
            }
        }

        impl From<$row> for $domain {
            fn from(row: $row) -> Self {
                Self {
                    $($field: RowField::into_domain(row.$field),)*
                }
            }
        }
    };
}

mirror_row! {
    /// Row form of [`Person`].
    PersonRow <=> Person {
        id: i64 = "id",
        name: String = "name",
        email_address: String = "emailAddress",
        credit_card: String = "creditCard",
        city: String = "city",
        state: String = "state",
        date_time: i64 = "dateTime",
        extra: String = "extra",
    }
}

mirror_row! {
    /// Row form of [`Auction`].
    AuctionRow <=> Auction {
        id: i64 = "id",
        item_name: String = "itemName",
        description: String = "description",
        initial_bid: i64 = "initialBid",
        reserve: i64 = "reserve",
        date_time: i64 = "dateTime",
        expires: i64 = "expires",
        seller: i64 = "seller",
        category: i64 = "category",
        extra: String = "extra",
    }
}

mirror_row! {
    /// Row form of [`Bid`].
    BidRow <=> Bid {
        auction: i64 = "auction",
        bidder: i64 = "bidder",
        price: i64 = "price",
        date_time: i64 = "dateTime",
        extra: String = "extra",
    }
}

/// Row form of [`Event`]: exactly one of the three fields is set.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct EventRow {
    #[beam(rename = "newPerson")]
    pub new_person: Option<PersonRow>,
    #[beam(rename = "newAuction")]
    pub new_auction: Option<AuctionRow>,
    #[beam(rename = "bid")]
    pub bid: Option<BidRow>,
}

impl From<Event> for EventRow {
    fn from(event: Event) -> Self {
        let empty = Self {
            new_person: None,
            new_auction: None,
            bid: None,
        };
        match event {
            Event::Person(person) => Self {
                new_person: Some(person.into()),
                ..empty
            },
            Event::Auction(auction) => Self {
                new_auction: Some(auction.into()),
                ..empty
            },
            Event::Bid(bid) => Self {
                bid: Some(bid.into()),
                ..empty
            },
        }
    }
}

/// An [`EventRow`] that does not hold exactly one event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidEventRow {
    /// Number of the three event fields that are set.
    pub set_fields: usize,
}

impl fmt::Display for InvalidEventRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "an event row needs exactly one of newPerson, newAuction and bid; {} are set",
            self.set_fields
        )
    }
}

impl std::error::Error for InvalidEventRow {}

impl TryFrom<EventRow> for Event {
    type Error = InvalidEventRow;

    fn try_from(row: EventRow) -> Result<Self, Self::Error> {
        match (row.new_person, row.new_auction, row.bid) {
            (Some(person), None, None) => Ok(Self::Person(person.into())),
            (None, Some(auction), None) => Ok(Self::Auction(auction.into())),
            (None, None, Some(bid)) => Ok(Self::Bid(bid.into())),
            (person, auction, bid) => Err(InvalidEventRow {
                set_fields: usize::from(person.is_some())
                    + usize::from(auction.is_some())
                    + usize::from(bid.is_some()),
            }),
        }
    }
}
