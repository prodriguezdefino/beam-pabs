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

//! Join example: demonstrates `CoGroupByKey`, inner join, and left outer join.
//!
//! Relational joins across multiple keyed PCollections.
//! Given a stream of user profiles and a stream of user orders, the pipeline demonstrates:
//! - Inner join: matching users with orders; unmatched users and orders are dropped.
//! - Left join: keeping all users with optional orders (`Some(order)` or `None`).
//! - `CoGroupByKey`: grouping both collections by key.

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// Command line arguments for the Join example pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(name = "join", about = "Apache Beam Rust CoGroupByKey & Joins Example")]
pub struct JoinArgs {
    /// Optional output file path for results.
    #[arg(long)]
    pub output: Option<String>,
}

impl PipelineOptionGroup for JoinArgs {}

/// Returns sample user profiles `(user_id, user_name)`.
pub fn default_users() -> Vec<(String, String)> {
    vec![
        ("user_1".to_string(), "Alice".to_string()),
        ("user_2".to_string(), "Bob".to_string()),
        ("user_3".to_string(), "Charlie".to_string()),
        ("user_4".to_string(), "Diana".to_string()),
    ]
}

/// Returns sample order events `(user_id, item_description)`.
pub fn default_orders() -> Vec<(String, String)> {
    vec![
        ("user_1".to_string(), "Laptop".to_string()),
        ("user_1".to_string(), "Mouse".to_string()),
        ("user_2".to_string(), "Keyboard".to_string()),
        ("user_3".to_string(), "Monitor".to_string()),
        ("user_5".to_string(), "Headphones".to_string()),
    ]
}

/// Constructs the join demonstration pipeline.
///
/// - Read users and orders as keyed `PCollection`s.
/// - Inner join: `users.inner_join("InnerJoin", &orders)`.
/// - Left outer join: `users.left_join("LeftJoin", &orders)`.
/// - `CoGroupByKey` with `KeyedPCollectionTuple`.
/// - Merge all formatted outputs with `Flatten` and log them.
pub fn build_join_pipeline(
    pipeline: &Pipeline,
    users: Vec<(String, String)>,
    orders: Vec<(String, String)>,
) -> PCollection<String> {
    let users_pcol = pipeline.apply(Create::new("CreateUsers", users));
    let orders_pcol = pipeline.apply(Create::new("CreateOrders", orders));

    // Inner join: users who placed orders.
    let inner = users_pcol.inner_join("InnerJoinOrders", &orders_pcol);
    let inner_formatted = inner.map("FormatInner", |(uid, (name, item))| {
        format!("INNER: user {uid} ({name}) ordered {item}")
    });

    // Left outer join: all users, with or without orders.
    let left = users_pcol.left_join("LeftJoinOrders", &orders_pcol);
    let left_formatted = left.map("FormatLeft", |(uid, (name, order_opt))| match order_opt {
        Some(item) => format!("LEFT: user {uid} ({name}) ordered {item}"),
        None => format!("LEFT: user {uid} ({name}) placed no orders"),
    });

    // CoGroupByKey: multi-collection grouping.
    let cogbk = KeyedPCollectionTuple::empty(pipeline.clone())
        .and("users", &users_pcol)
        .and("orders", &orders_pcol)
        .apply(CoGroupByKey::new("UserOrdersCoGbk"));

    let cogbk_formatted = cogbk.map("FormatCoGbk", |(uid, result)| {
        let mut names: Vec<String> = result.get_vec("users").expect("users tag");
        let mut items: Vec<String> = result.get_vec("orders").expect("orders tag");
        names.sort();
        items.sort();
        format!("COGBK: user {uid} -> names={names:?}, orders={items:?}")
    });

    // Flatten all outputs into a single stream.
    let merged = inner_formatted.flatten("MergeOutputs", &[&left_formatted, &cogbk_formatted]);

    merged.inspect("LogJoinResults", |line: &String| {
        tracing::info!("{line}");
    })
}
