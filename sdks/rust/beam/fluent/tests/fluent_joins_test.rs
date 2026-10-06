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

//! Runs the fluent cogroups and relational joins on Prism and checks their exact
//! outputs with `passert`.
//!
//! The fixture covers every case that a join must tell apart:
//! - `u1`: one user, two orders (one-to-many).
//! - `u2`: user without orders (unmatched on left).
//! - `u3`: orders without user (unmatched on right).
//! - `u4`: two users and two orders (duplicate keys on both sides: a 2x2 cross product).

use fluent::prelude::*;
use testing::{TestPipeline, passert};

mod common;
use common::s;

type Users = PCollection<(String, String)>;
type Orders = PCollection<(String, i64)>;
type Grouped = (String, (Vec<String>, Vec<i64>));
type CoGrouped = PCollection<(String, (BeamIterable<String>, BeamIterable<i64>))>;

fn fixture(p: &TestPipeline) -> (Users, Orders) {
    let users = p.apply(Create::new(
        "Users",
        vec![
            (s("u1"), s("Alice")),
            (s("u2"), s("Bob")),
            (s("u4"), s("Dan")),
            (s("u4"), s("Dave")),
        ],
    ));
    let orders = p.apply(Create::new(
        "Orders",
        vec![
            (s("u1"), 100i64),
            (s("u1"), 200),
            (s("u3"), 300),
            (s("u4"), 400),
            (s("u4"), 500),
        ],
    ));
    (users, orders)
}

/// Pairs present on both sides: `u1` one-to-many and the `u4` cross product.
fn matched<L, R>(left: impl Fn(String) -> L, right: impl Fn(i64) -> R) -> Vec<(String, (L, R))> {
    [
        ("u1", "Alice", 100),
        ("u1", "Alice", 200),
        ("u4", "Dan", 400),
        ("u4", "Dan", 500),
        ("u4", "Dave", 400),
        ("u4", "Dave", 500),
    ]
    .into_iter()
    .map(|(k, name, amount)| (s(k), (left(s(name)), right(amount))))
    .collect()
}

#[tokio::test]
async fn joins_on_shared_fixture() {
    let p = TestPipeline::new();
    let (users, orders) = fixture(&p);

    let inner = users.inner_join("Inner", &orders);
    let left = users.left_join("Left", &orders);
    let right = users.right_join("Right", &orders);
    let full = users.full_outer_join("Full", &orders);

    // Inner: cross product of matching keys only.
    passert::that("AssertInner", &inner).contains_in_any_order(matched(|n| n, |a| a));
    // Left: unmatched left rows (`u2`) paired with None.
    let mut expected_left = matched(|n| n, Some);
    expected_left.push((s("u2"), (s("Bob"), None)));
    passert::that("AssertLeft", &left).contains_in_any_order(expected_left);
    // Right: unmatched right rows (`u3`) paired with None.
    let mut expected_right = matched(Some, |a| a);
    expected_right.push((s("u3"), (None, 300)));
    passert::that("AssertRight", &right).contains_in_any_order(expected_right);
    // Full outer: unmatched rows from both sides.
    let mut expected_full = matched(Some, Some);
    expected_full.push((s("u2"), (Some(s("Bob")), None)));
    expected_full.push((s("u3"), (None, Some(300))));
    passert::that("AssertFull", &full).contains_in_any_order(expected_full);

    p.run().await.expect("pipeline + assertions");
}

fn sorted_cogroup(grouped: &CoGrouped, name: &str) -> PCollection<Grouped> {
    grouped.map(
        name,
        |(k, (names, amounts)): (String, (BeamIterable<String>, BeamIterable<i64>))| {
            let mut names: Vec<String> = names.into_iter().collect();
            let mut amounts: Vec<i64> = amounts.into_iter().collect();
            names.sort();
            amounts.sort();
            (k, (names, amounts))
        },
    )
}

#[tokio::test]
async fn co_group_by_key_groups_both_sides_per_key_including_empty_sides() {
    let p = TestPipeline::new();
    let (users, orders) = fixture(&p);

    let co_gbk = sorted_cogroup(&users.co_group_by_key("CoGbk", &orders), "SortCoGbk");

    let expected: Vec<Grouped> = vec![
        (s("u1"), (vec![s("Alice")], vec![100, 200])),
        (s("u2"), (vec![s("Bob")], vec![])),
        (s("u3"), (vec![], vec![300])),
        (s("u4"), (vec![s("Dan"), s("Dave")], vec![400, 500])),
    ];
    passert::that("AssertCoGbk", &co_gbk).contains_in_any_order(expected);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn co_group_by_key_and_joins_keep_self_on_the_left_when_both_sides_share_a_type() {
    // Both sides have the same value type, so swapped sides still decode. Only the
    // values show that `self` stays on the left.
    let p = TestPipeline::new();
    let lefts = p.apply(Create::new("Lefts", vec![(s("k"), 1i64), (s("l"), 2)]));
    let rights = p.apply(Create::new("Rights", vec![(s("k"), 10i64), (s("r"), 20)]));

    let grouped = lefts.co_group_by_key("Cogroup", &rights).map(
        "Collect",
        |(k, (l, r)): (String, (BeamIterable<i64>, BeamIterable<i64>))| {
            (
                k,
                (
                    l.into_iter().collect::<Vec<_>>(),
                    r.into_iter().collect::<Vec<_>>(),
                ),
            )
        },
    );
    let inner = lefts.inner_join("Inner", &rights);
    let left = lefts.left_join("Left", &rights);
    let right = lefts.right_join("Right", &rights);
    let full = lefts.full_outer_join("Full", &rights);

    passert::that("AssertGrouped", &grouped).contains_in_any_order([
        (s("k"), (vec![1], vec![10])),
        (s("l"), (vec![2], vec![])),
        (s("r"), (vec![], vec![20])),
    ]);
    passert::that("AssertInner", &inner).contains_in_any_order([(s("k"), (1, 10))]);
    passert::that("AssertLeft", &left)
        .contains_in_any_order([(s("k"), (1, Some(10))), (s("l"), (2, None))]);
    passert::that("AssertRight", &right)
        .contains_in_any_order([(s("k"), (Some(1), 10)), (s("r"), (None, 20))]);
    passert::that("AssertFull", &full).contains_in_any_order([
        (s("k"), (Some(1), Some(10))),
        (s("l"), (Some(2), None)),
        (s("r"), (None, Some(20))),
    ]);

    p.run().await.expect("pipeline + assertions");
}

#[tokio::test]
async fn joins_with_an_empty_side_keep_or_drop_every_row() {
    let p = TestPipeline::new();
    let (users, _) = fixture(&p);
    let no_orders = p.apply(Create::new("NoOrders", Vec::<(String, i64)>::new()));

    let inner = users.inner_join("Inner", &no_orders);
    let left = users.left_join("Left", &no_orders);
    let right = users.right_join("Right", &no_orders);

    passert::that("AssertInner", &inner).empty();
    passert::that("AssertLeft", &left).contains_in_any_order([
        (s("u1"), (s("Alice"), None)),
        (s("u2"), (s("Bob"), None)),
        (s("u4"), (s("Dan"), None)),
        (s("u4"), (s("Dave"), None)),
    ]);
    passert::that("AssertRight", &right).empty();

    p.run().await.expect("pipeline + assertions");
}
