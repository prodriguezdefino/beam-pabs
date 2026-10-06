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

//! Join: users and their orders, grouped with `co_group_by_key` and joined with the
//! inner, left, right and full outer joins.

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(name = "join", about = "Apache Beam join example", version)]
pub struct Args {
    /// Path of the CSV file of users: `user_id,name`.
    #[arg(long)]
    pub users: String,

    /// Path of the CSV file of orders: `user_id,item`.
    #[arg(long)]
    pub orders: String,

    /// Path of the file to write to.
    #[arg(long, default_value = "/tmp/join.txt")]
    pub output: String,
}

impl PipelineOptionGroup for Args {}

/// Parses a `key,value` line.
pub fn parse_pair(line: &str) -> Result<(String, String), String> {
    line.split_once(',')
        .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        .ok_or_else(|| format!("expected key,value: {line:?}"))
}

/// Joins users `(user_id, name)` with `orders` `(user_id, item)`. Gives one line per
/// result, prefixed with the operation. `-` marks a missing side.
pub struct JoinOrders {
    pub orders: PCollection<(String, String)>,
}

impl PTransform<PCollection<(String, String)>> for JoinOrders {
    type Output = PCollection<String>;

    fn expand(&self, users: &PCollection<(String, String)>) -> Self::Output {
        let orders = &self.orders;

        let grouped = users.co_group_by_key("CoGroupByKey", orders).par_do_fn(
            "FormatGroups",
            |(id, (names, items)): (String, (BeamIterable<String>, BeamIterable<String>)), ctx| {
                let (names, items) = (names.into_vec()?, items.into_vec()?);
                ctx.emit(format!("grouped {id}: users={names:?} orders={items:?}"))
            },
        );
        let inner = users
            .inner_join("InnerJoin", orders)
            .map("FormatInner", |(id, (name, item))| {
                format!("inner {id}: {name}, {item}")
            });
        let left = users
            .left_join("LeftJoin", orders)
            .map("FormatLeft", |(id, (name, item))| {
                format!("left {id}: {name}, {}", or_dash(item))
            });
        let right = users
            .right_join("RightJoin", orders)
            .map("FormatRight", |(id, (name, item))| {
                format!("right {id}: {}, {item}", or_dash(name))
            });
        let full = users
            .full_outer_join("FullOuterJoin", orders)
            .map("FormatFullOuter", |(id, (name, item))| {
                format!("full {id}: {}, {}", or_dash(name), or_dash(item))
            });

        grouped.flatten("Merge", &[&inner, &left, &right, &full])
    }
}

fn or_dash(value: Option<String>) -> String {
    value.unwrap_or_else(|| "-".to_string())
}
