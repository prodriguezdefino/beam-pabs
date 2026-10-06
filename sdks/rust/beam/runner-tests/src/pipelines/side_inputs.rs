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

//! Side inputs: every view type, windowed views, and the singleton error paths.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};

use crate::Expectation;
use crate::dofns::ValidatesAssignTimestampDoFn;

/// Validates the singleton, iterable and multimap views, each read by the main input.
pub fn build_side_input_views(p: &TestPipeline) {
    let cart = p.apply(Create::new(
        "Cart",
        ["apple", "milk", "bread", "caviar"]
            .map(String::from)
            .to_vec(),
    ));
    let tax_percent = p.apply(Create::new("TaxPercent", vec![20i64]));
    let exempt = p.apply(Create::new(
        "Exempt",
        ["milk", "bread"].map(String::from).to_vec(),
    ));
    // Two prices for bread: a multimap returns every value of a key.
    let prices = p.apply(Create::new(
        "Prices",
        vec![
            ("apple".to_string(), 100i64),
            ("milk".to_string(), 200),
            ("bread".to_string(), 150),
            ("bread".to_string(), 170),
        ],
    ));

    let singleton = cart.with_side_singleton("Singleton", &tax_percent, |item, tax| {
        format!("{item}:{tax}")
    });
    passert::that("SingletonView", &singleton)
        .contains_in_any_order(["apple:20", "milk:20", "bread:20", "caviar:20"].map(String::from));

    let iterable = cart.with_side_iter("Iterable", &exempt, |item, exempt| {
        let mut sorted = exempt.clone();
        sorted.sort();
        (item.clone(), (exempt.contains(&item), sorted.join("+")))
    });
    passert::that("IterableView", &iterable).contains_in_any_order([
        ("apple".to_string(), (false, "bread+milk".to_string())),
        ("milk".to_string(), (true, "bread+milk".to_string())),
        ("bread".to_string(), (true, "bread+milk".to_string())),
        ("caviar".to_string(), (false, "bread+milk".to_string())),
    ]);

    let multimap = cart.with_side_map("Multimap", &prices, |item, lookup| {
        let mut found = lookup(&item)?;
        found.sort_unstable();
        Ok((item, found))
    });
    passert::that("MultimapView", &multimap).contains_in_any_order([
        ("apple".to_string(), vec![100]),
        ("milk".to_string(), vec![200]),
        ("bread".to_string(), vec![150, 170]),
        // A missing key reads as no values, not as an error.
        ("caviar".to_string(), vec![]),
    ]);
}

/// Validates an empty iterable side input: it reads as an empty list.
pub fn build_empty_iterable_side_input(p: &TestPipeline) {
    let nothing = p
        .apply(Create::new("Nothing", vec![0i64]))
        .filter("DropAll", |_: &i64| false);
    let main = p.apply(Create::new("Main", vec![1i64, 2]));
    let out = main.with_side_iter("ReadEmpty", &nothing, |x, side| (x, side.len() as i64));
    passert::that("AssertOut", &out).contains_in_any_order([(1, 0), (2, 0)]);
}

/// Validates windowed side inputs: each main-input element reads the side input of its
/// own window, not the whole side collection.
pub fn build_windowed_side_input(p: &TestPipeline) {
    let fixed = |name: &str| WindowInto::new(name, FixedWindows::of(Duration::from_secs(10)));

    let rates = p
        .apply(Create::new(
            "Rates",
            vec![
                ("rate".to_string(), (10i64, 1_000i64)),
                ("rate".to_string(), (20i64, 11_000i64)),
            ],
        ))
        .par_do("TimestampRates", ValidatesAssignTimestampDoFn)
        .apply(fixed("WindowRates"))
        .map("RateValue", |(_, v): (String, i64)| v);

    let orders = p
        .apply(Create::new(
            "Orders",
            vec![
                ("a".to_string(), (1i64, 2_000i64)),
                ("b".to_string(), (2i64, 12_000i64)),
                ("c".to_string(), (3i64, 9_999i64)),
            ],
        ))
        .par_do("TimestampOrders", ValidatesAssignTimestampDoFn)
        .apply(fixed("WindowOrders"));

    let priced = orders.with_side_singleton("ApplyRate", &rates, |(k, v), rate| (k, v * rate));
    passert::that("AssertPriced", &priced).contains_in_any_order([
        ("a".to_string(), 10),
        ("b".to_string(), 40),
        ("c".to_string(), 30),
    ]);

    let ts_data = p
        .apply(Create::new(
            "CrossWindowData",
            vec![
                ("k".to_string(), (1i64, 990i64)),
                ("k".to_string(), (2i64, 1_990i64)),
                ("k".to_string(), (3i64, 2_990i64)),
            ],
        ))
        .par_do("TimestampCrossWindow", ValidatesAssignTimestampDoFn)
        .map("DropCrossKey", |(_, v): (String, i64)| v);

    let w1 = Duration::from_secs(1);
    let check_sums =
        |label: &str, main: PCollection<i64>, side: PCollection<i64>, expected: &[i64]| {
            let sums = main
                .with_side_iter(format!("{label}/SumSide"), &side, |v, side_vals| {
                    v + side_vals.into_iter().sum::<i64>()
                })
                .apply(WindowInto::new(format!("{label}/Global"), GlobalWindows));
            passert::that(format!("{label}/Assert"), &sums)
                .contains_in_any_order(expected.iter().copied());
        };

    check_sums(
        "FixedGlobal",
        ts_data.apply(WindowInto::new("FG/Main", FixedWindows::of(w1))),
        ts_data.apply(WindowInto::new("FG/Side", GlobalWindows)),
        &[7, 8, 9],
    );
    check_sums(
        "FixedSame",
        ts_data.apply(WindowInto::new("FS/Main", FixedWindows::of(w1))),
        ts_data.apply(WindowInto::new("FS/Side", FixedWindows::of(w1))),
        &[2, 4, 6],
    );
    check_sums(
        "FixedBig",
        ts_data.apply(WindowInto::new("FB/Main", FixedWindows::of(w1))),
        ts_data.apply(WindowInto::new(
            "FB/Side",
            FixedWindows::of(Duration::from_secs(10)),
        )),
        &[7, 8, 9],
    );
    check_sums(
        "FixedSliding",
        ts_data.apply(WindowInto::new("FSl/Main", FixedWindows::of(w1))),
        ts_data.apply(WindowInto::new(
            "FSl/Side",
            SlidingWindows::of(w1 * 2).every(w1),
        )),
        &[2, 5, 8],
    );
    check_sums(
        "SlidingFixed",
        ts_data.apply(WindowInto::new(
            "SlF/Main",
            SlidingWindows::of(w1 * 2).every(w1),
        )),
        ts_data.apply(WindowInto::new("SlF/Side", FixedWindows::of(w1))),
        &[2, 3, 4, 5, 6, 3],
    );
}

/// Reading an empty collection as a singleton fails the pipeline, saying the singleton
/// was empty.
pub const EMPTY_SINGLETON_SIDE_INPUT_FAILS: Expectation =
    Expectation::FailsWith(&["Empty singleton side input"]);

/// Builds a pipeline reading an empty collection as a singleton; see
/// [`EMPTY_SINGLETON_SIDE_INPUT_FAILS`].
pub fn build_empty_singleton_side_input_fails(p: &TestPipeline) {
    let nothing = p
        .apply(Create::new("Nothing", vec![0i64]))
        .filter("DropAll", |_: &i64| false);
    let main = p.apply(Create::new("Main", vec![1i64]));
    let out = main.with_side_singleton("ReadEmpty", &nothing, |x, s| x + s);
    passert::that("Unreachable", &out).empty();
}

/// Reading a collection of several elements as a singleton fails the pipeline. The
/// pipeline does not pick one of the elements.
pub const MULTI_ELEMENT_SINGLETON_SIDE_INPUT_FAILS: Expectation =
    Expectation::FailsWith(&["singleton", "more than one"]);

/// Builds a pipeline reading two elements as a singleton; see
/// [`MULTI_ELEMENT_SINGLETON_SIDE_INPUT_FAILS`].
pub fn build_multi_element_singleton_side_input_fails(p: &TestPipeline) {
    let two = p.apply(Create::new("Two", vec![1i64, 2]));
    let main = p.apply(Create::new("Main", vec![10i64]));
    let out = main.with_side_singleton("ReadTwo", &two, |x, s| x + s);
    passert::that("Unreachable", &out).empty();
}
