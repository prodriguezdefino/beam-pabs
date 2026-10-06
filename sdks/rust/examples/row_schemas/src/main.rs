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

//! Summarizes customer accounts read from JSON lines.
//!
//! Run on Prism, the default runner:
//!
//! ```text
//! cargo run -p row_schemas -- --input=customers.jsonl --output=/tmp/row_schemas.txt
//! ```
//!
//! A line of the input:
//!
//! ```text
//! {"id":101,"name":"Acme","tier":"Enterprise","address":{"street":"100 Market St",
//!  "city":"San Francisco","postal_code":"94105"},"balance":"15420.50",
//!  "signup_date":"2022-03-15","last_login":"2023-11-14T22:13:20Z","tags":["b2b"],
//!  "phone":null}
//! ```

use beam::prelude::*;
use row_schemas::{
    Args, CustomerRecord, FinancialSummary, KeyedSummary, SubscriptionTier, SummarizeAccounts,
    SummarizeByTier,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::logging::init_logging();
    let (options, args) = beam::options::parse::<Args>();

    let p = Pipeline::create(&options);
    let customers = p
        .apply("ReadLines", textio::Read::new(&args.input))
        .par_do_fn("ParseCustomers", |line: String, ctx| {
            ctx.emit(serde_json::from_str::<CustomerRecord>(&line)?)
        });

    let global = customers
        .apply("SummarizeAccounts", SummarizeAccounts)
        .map("FormatGlobal", |summary: FinancialSummary| {
            format!("ALL: {summary}")
        });
    let by_tier = customers.apply("SummarizeByTier", SummarizeByTier).map(
        "FormatTier",
        |row: KeyedSummary<SubscriptionTier, FinancialSummary>| row.to_string(),
    );

    global
        .flatten("MergeSummaries", &by_tier)
        .apply("WriteLines", textio::Write::new(&args.output));
    p.run().await?;

    Ok(())
}
