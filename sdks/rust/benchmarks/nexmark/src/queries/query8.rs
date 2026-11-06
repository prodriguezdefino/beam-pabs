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

//! Query 8 (`MONITOR_NEW_USERS`): windowed join of newly registered sellers and their auctions.

use std::time::Duration;

use beam::prelude::*;

use super::{just_auctions, just_persons};
use crate::config::NexmarkConfig;
use crate::model::{Event, IdNameReserve};

/// Joins [`Person`](crate::model::Person) and [`Auction`](crate::model::Auction) streams within fixed windows.
#[derive(Clone, Copy, Debug)]
pub struct Query8 {
    window_size_sec: i64,
}

impl Query8 {
    /// Creates a new-user monitor transform over fixed windows of `window_size_sec` seconds.
    pub fn new(window_size_sec: i64) -> Self {
        Self {
            window_size_sec: window_size_sec.max(1),
        }
    }

    /// Creates a new-user monitor transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.window_size_sec)
    }
}

impl PTransform<PCollection<Event>> for Query8 {
    type Output = PCollection<IdNameReserve>;

    fn expand(&self, input: &PCollection<Event>) -> PCollection<IdNameReserve> {
        let windows = FixedWindows::of(Duration::from_secs(
            u64::try_from(self.window_size_sec).unwrap_or(1),
        ));
        let persons = just_persons(input)
            .window_into("WindowPersons", windows)
            .key_by("PersonById", |person| person.id);
        let auctions = just_auctions(input)
            .window_into("WindowAuctions", windows)
            .key_by("AuctionBySeller", |auction| auction.seller);

        persons
            .inner_join("Join", &auctions)
            .map("Select", |(_seller_id, (person, auction))| IdNameReserve {
                id: person.id,
                name: person.name,
                reserve: auction.reserve,
            })
    }
}
