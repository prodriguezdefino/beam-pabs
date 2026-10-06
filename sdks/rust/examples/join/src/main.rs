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

//! Joins a CSV file of users with a CSV file of orders.
//!
//! Run on Prism, the default runner:
//!
//! ```text
//! cargo run -p join -- --users=users.csv --orders=orders.csv --output=/tmp/join.txt
//! ```

use beam::prelude::*;
use join::{Args, JoinOrders, parse_pair};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::logging::init_logging();
    let (options, args) = beam::options::parse::<Args>();

    let p = Pipeline::create(&options);
    let read_pairs = |name: &str, path: &str| {
        p.apply(format!("Read{name}"), textio::Read::new(path))
            .par_do_fn(format!("Parse{name}"), |line: String, ctx| {
                ctx.emit(parse_pair(&line)?)
            })
    };
    let orders = read_pairs("Orders", &args.orders);
    read_pairs("Users", &args.users)
        .apply("JoinOrders", JoinOrders { orders })
        .apply("WriteLines", textio::Write::new(&args.output));
    p.run().await?;

    Ok(())
}
