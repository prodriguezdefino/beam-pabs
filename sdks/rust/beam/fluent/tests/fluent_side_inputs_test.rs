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

//! Graph construction and execution tests for `beam-fluent` side inputs and broadcast joins.

use beam::internals::extract_side_input_tags;
use beam::pipeline::{URN_GROUP_BY_KEY, URN_PAR_DO, URN_RESHUFFLE};
use fluent::prelude::*;
use testing::{TestPipeline, passert};

mod common;
use common::s;

type Events = PCollection<(String, i64)>;
type Regions = PCollection<(String, String)>;

/// Prices a cart using multimap, iterable and singleton side inputs. Then formats
/// each item with the tax rate read through a `ParDo` side input.
fn priced_cart(p: &Pipeline) -> PCollection<String> {
    let tax_rate = p.apply(Create::new("TaxRate", vec![20_i64]));
    let exempt_items = p.apply(Create::new("ExemptItems", vec![s("milk")]));
    let price_table = p.apply(Create::new(
        "PriceTable",
        vec![(s("apple"), 100_i64), (s("milk"), 200_i64)],
    ));

    // "pear" has no price, so the multimap lookup yields no values for it.
    let cart = p.apply(Create::new("Cart", vec![s("apple"), s("milk"), s("pear")]));

    let priced =
        cart.map_with_side_inputs("LookupPrice", price_table.as_multimap(), |item, prices| {
            let base = prices.get(&item)?.unwrap_or(0);
            Ok((item, base))
        });

    let taxed_flag = priced.map_with_side_inputs(
        "CheckExempt",
        exempt_items.as_iter(),
        |(item, price), exempt| {
            let is_exempt = exempt.iter()?.any(|e| e.is_ok_and(|e| e == item));
            Ok((item, (price, is_exempt)))
        },
    );

    let tax_rate_view = tax_rate.as_singleton();
    let final_totals = taxed_flag.map_with_side_inputs(
        "ApplyTax",
        &tax_rate_view,
        |(item, (price, is_exempt)), rate| {
            let final_price = if is_exempt {
                price
            } else {
                price + (price * rate) / 100
            };
            Ok(format!("{item}:{final_price}"))
        },
    );

    final_totals.par_do_fn_with_side_inputs(
        "FormatWithRate",
        &tax_rate_view,
        |s: String, rate, ctx| ctx.emit(format!("{s}@{rate}%")),
    )
}

/// `us` matches two regions, `fr` matches no region, and region `de` has no events.
fn events_and_regions(p: &Pipeline) -> (Events, Regions) {
    let events = p.apply(Create::new(
        "Events",
        vec![(s("us"), 1_i64), (s("us"), 2), (s("fr"), 3)],
    ));
    let regions = p.apply(Create::new(
        "Regions",
        vec![
            (s("us"), s("United States")),
            (s("us"), s("USA")),
            (s("de"), s("Germany")),
        ],
    ));
    (events, regions)
}

#[test]
fn test_dsl_side_inputs_and_broadcast_joins_graph_construction() {
    let p = Pipeline::new();
    priced_cart(&p);

    let (events, regions) = events_and_regions(&p);
    events.broadcast_inner_join("BroadcastInner", &regions);
    events.broadcast_left_join("BroadcastLeft", &regions);

    // Verify proto graph structure attaches side inputs without GroupByKey shuffle.
    let proto = p.to_proto();
    let transforms = &proto.components.as_ref().unwrap().transforms;
    // `Create` ends in a reshuffle, which has its own GroupByKey.
    let in_reshuffle: std::collections::HashSet<&String> = transforms
        .values()
        .filter(|t| t.spec.as_ref().is_some_and(|s| s.urn == URN_RESHUFFLE))
        .flat_map(|t| &t.subtransforms)
        .collect();
    assert!(
        !transforms.iter().any(|(id, t)| {
            !in_reshuffle.contains(id) && t.spec.as_ref().is_some_and(|s| s.urn == URN_GROUP_BY_KEY)
        }),
        "Side input and broadcast join pipelines must not introduce GroupByKey shuffles"
    );

    for name in ["BroadcastInner", "BroadcastLeft"] {
        let tx = transforms
            .values()
            .find(|t| t.unique_name == name)
            .unwrap_or_else(|| panic!("{name} transform must exist"));
        assert_eq!(tx.spec.as_ref().unwrap().urn, URN_PAR_DO, "{name}");
        assert_eq!(extract_side_input_tags(tx).len(), 1, "{name}");
    }
}

#[tokio::test]
async fn side_input_closures_compute_exact_outputs() {
    let p = TestPipeline::new();
    let formatted = priced_cart(&p);

    passert::that("AssertFormatted", &formatted).contains_in_any_order([
        s("apple:120@20%"),
        s("milk:200@20%"),
        s("pear:0@20%"),
    ]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn broadcast_joins_match_every_side_input_value() {
    let p = TestPipeline::new();
    let (events, regions) = events_and_regions(&p);

    let inner = events.broadcast_inner_join("BroadcastInner", &regions);
    let left = events.broadcast_left_join("BroadcastLeft", &regions);

    let expected_inner = vec![
        (s("us"), (1, s("United States"))),
        (s("us"), (1, s("USA"))),
        (s("us"), (2, s("United States"))),
        (s("us"), (2, s("USA"))),
    ];
    passert::that("AssertInner", &inner).contains_in_any_order(expected_inner.clone());

    let mut expected_left: Vec<(String, (i64, Option<String>))> = expected_inner
        .into_iter()
        .map(|(k, (v, r))| (k, (v, Some(r))))
        .collect();
    expected_left.push((s("fr"), (3, None)));
    passert::that("AssertLeft", &left).contains_in_any_order(expected_left);

    p.run().await.expect("pipeline + assertions");
}
