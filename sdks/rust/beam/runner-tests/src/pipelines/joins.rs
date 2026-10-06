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

//! `CoGroupByKey` and the join transforms built on top of it.

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};

/// Users keyed by id: 1 and 2 have orders, 3 has none, and there is no user 4.
fn users(p: &Pipeline) -> PCollection<(i64, String)> {
    p.apply(Create::new(
        "Users",
        vec![
            (1i64, "Alice".to_string()),
            (2i64, "Bob".to_string()),
            (3i64, "Charlie".to_string()),
        ],
    ))
}

/// Orders keyed by user id: two for user 1, one for user 2, one for the missing user 4.
fn orders(p: &Pipeline) -> PCollection<(i64, i64)> {
    p.apply(Create::new(
        "Orders",
        vec![
            (1i64, 100i64),
            (1i64, 250i64),
            (2i64, 50i64),
            (4i64, 999i64),
        ],
    ))
}

/// Validates CoGroupByKey grouping two keyed PCollections.
///
/// Every key present on either side yields exactly one group, holding all of that
/// key's values from each side, in the side they came from.
pub fn build_cogroup_by_key(p: &TestPipeline) {
    let grouped = users(p).co_group_by_key("GroupUsersOrders", &orders(p));

    let sorted = grouped.par_do_fn(
        "SortGroups",
        |(id, (names, amounts)): (i64, (BeamIterable<String>, BeamIterable<i64>)), ctx| {
            let mut names = names.into_vec().map_err(|e| e.to_string())?;
            names.sort();
            let mut amounts = amounts.into_vec().map_err(|e| e.to_string())?;
            amounts.sort_unstable();
            ctx.emit((id, (names, amounts)))
        },
    );
    passert::that("AssertSorted", &sorted).contains_in_any_order([
        (1, (vec!["Alice".to_string()], vec![100, 250])),
        (2, (vec!["Bob".to_string()], vec![50])),
        (3, (vec!["Charlie".to_string()], vec![])),
        (4, (vec![], vec![999])),
    ]);
}

/// Validates the four relational joins, with the exact output of each.
///
/// The data covers a key matching several right-hand values (1), exactly one (2),
/// none on the right (3) and none on the left (4), which is what tells the joins apart.
pub fn build_joins(p: &TestPipeline) {
    let users = users(p);
    let orders = orders(p);

    passert::that("InnerJoin", &users.inner_join("InnerJoin", &orders)).contains_in_any_order([
        (1, ("Alice".to_string(), 100)),
        (1, ("Alice".to_string(), 250)),
        (2, ("Bob".to_string(), 50)),
    ]);

    passert::that("LeftJoin", &users.left_join("LeftJoin", &orders)).contains_in_any_order([
        (1, ("Alice".to_string(), Some(100))),
        (1, ("Alice".to_string(), Some(250))),
        (2, ("Bob".to_string(), Some(50))),
        (3, ("Charlie".to_string(), None)),
    ]);

    passert::that("RightJoin", &users.right_join("RightJoin", &orders)).contains_in_any_order([
        (1, (Some("Alice".to_string()), 100)),
        (1, (Some("Alice".to_string()), 250)),
        (2, (Some("Bob".to_string()), 50)),
        (4, (None, 999)),
    ]);

    passert::that(
        "FullOuterJoin",
        &users.full_outer_join("FullOuterJoin", &orders),
    )
    .contains_in_any_order([
        (1, (Some("Alice".to_string()), Some(100))),
        (1, (Some("Alice".to_string()), Some(250))),
        (2, (Some("Bob".to_string()), Some(50))),
        (3, (Some("Charlie".to_string()), None)),
        (4, (None, Some(999))),
    ]);
}

/// Validates a join where both sides hold several values for a key: the output is
/// their cross product.
pub fn build_join_cross_product(p: &TestPipeline) {
    let left = p.apply(Create::new(
        "Left",
        vec![("k".to_string(), 1i64), ("k".to_string(), 2)],
    ));
    let right = p.apply(Create::new(
        "Right",
        vec![
            ("k".to_string(), "x".to_string()),
            ("k".to_string(), "y".to_string()),
            ("k".to_string(), "z".to_string()),
        ],
    ));

    let joined = left.inner_join("CrossJoin", &right);
    passert::that("AssertJoined", &joined).contains_in_any_order(
        [1i64, 2]
            .into_iter()
            .flat_map(|l| ["x", "y", "z"].map(move |r| ("k".to_string(), (l, r.to_string())))),
    );
}
