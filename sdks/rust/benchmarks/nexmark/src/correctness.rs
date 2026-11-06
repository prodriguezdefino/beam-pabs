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

//! In-pipeline checks that compare query output with a reference model in [`crate::models`].
//!
//! A failed check fails the job. Use the checks only on input with a finite number of events:
//! they read the whole output in the global window, which closes at the end of the input.

use std::collections::BTreeMap;
use std::fmt::Debug;

use beam::coders::DefaultCoder;
use beam::prelude::*;
use beam::testing::passert;

use crate::model::SellerPrice;

/// Adds a check that `output` holds exactly `expected`, values and timestamps, in any order.
///
/// `name` names the check step and must be unique in the pipeline.
pub fn assert_timestamped<T>(name: &str, output: &PCollection<T>, expected: Vec<(T, Instant)>)
where
    T: DefaultCoder + PartialEq + Debug + Send + Sync + 'static,
{
    let stamped = output.par_do_fn(format!("{name}Timestamps"), |element: T, ctx| {
        let timestamp = ctx.timestamp();
        ctx.emit((element, timestamp))
    });
    let _ = passert::that(name, &stamped).contains_in_any_order(expected);
}

/// Adds a check that the last pane of each seller in `output` equals `expected`.
///
/// Query 6 fires a pane for each winning bid, so only the last pane of a seller is final.
/// `expected` holds one price for each seller, sorted by seller.
pub fn assert_final_seller_prices(
    name: &str,
    output: &PCollection<SellerPrice>,
    expected: Vec<SellerPrice>,
) {
    let panes = output.par_do_fn(format!("{name}PaneIndex"), |price: SellerPrice, ctx| {
        let index = ctx
            .pane()
            .index()
            .and_then(|index| i64::try_from(index).ok())
            .unwrap_or(0);
        ctx.emit((price, index))
    });
    let _ =
        passert::that(name, &panes).satisfies(move |actual| check_final_panes(actual, &expected));
}

fn check_final_panes(actual: &[(SellerPrice, i64)], expected: &[SellerPrice]) -> Result {
    let latest = actual.iter().fold(
        BTreeMap::<i64, (i64, SellerPrice)>::new(),
        |mut latest, &(price, index)| {
            match latest.get(&price.seller) {
                Some(&(best, _)) if index < best => {}
                Some(_) | None => {
                    latest.insert(price.seller, (index, price));
                }
            }
            latest
        },
    );
    let final_prices: Vec<SellerPrice> = latest.into_values().map(|(_, price)| price).collect();
    if final_prices == expected {
        Ok(())
    } else {
        Err(format!("final seller panes {final_prices:?} != expected {expected:?}").into())
    }
}
