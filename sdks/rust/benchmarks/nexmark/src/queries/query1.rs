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

//! Query 1 (`CURRENCY_CONVERSION`): converts bid prices from dollars to euros.

use beam::prelude::*;

use super::just_bids;
use crate::model::{Bid, Event};

/// Converts each [`Bid`] price from dollars to euros (`(price * 89) / 100`).
#[derive(Clone, Copy, Debug, Default)]
pub struct Query1;

impl PTransform<PCollection<Event>> for Query1 {
    type Output = PCollection<Bid>;

    fn expand(&self, input: &PCollection<Event>) -> PCollection<Bid> {
        just_bids(input).map("ToEuros", |bid| Bid {
            price: dollars_to_euros(bid.price),
            ..bid
        })
    }
}

pub(crate) const fn dollars_to_euros(price: i64) -> i64 {
    (price * 89) / 100
}
