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

//! Row schemas: typed records with `#[derive(BeamRow)]` and `#[derive(BeamEnum)]`.
//!
//! The records have nested rows, arrays, optional fields and logical types: `NaiveDate`
//! (`beam:logical_type:date:v1`), `DateTime<Utc>` (`beam:logical_type:micros_instant:v1`)
//! and `Decimal` (`beam:logical_type:decimal:v1`). [`SummarizeAccounts`] combines them
//! in one pass with `Compose(...).map_output`.

use std::fmt;

use beam::prelude::*;
use chrono::{DateTime, NaiveDate, Utc};
use clap::Args as ClapArgs;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "row_schemas",
    about = "Apache Beam row schemas example",
    version
)]
pub struct Args {
    /// Path of the file of customers to read from, one JSON [`CustomerRecord`] per line.
    #[arg(long)]
    pub input: String,

    /// Path of the file to write to.
    #[arg(long, default_value = "/tmp/row_schemas.txt")]
    pub output: String,
}

impl PipelineOptionGroup for Args {}

/// Subscription tier of a customer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, BeamEnum)]
pub enum SubscriptionTier {
    Basic,
    Premium,
    Enterprise,
}

/// Postal address, a row nested in [`CustomerRecord`].
#[derive(Clone, Debug, PartialEq, Deserialize, BeamRow)]
pub struct Address {
    pub street: String,
    pub city: String,
    pub postal_code: String,
}

/// Customer account.
#[derive(Clone, Debug, PartialEq, Deserialize, BeamRow)]
pub struct CustomerRecord {
    pub id: i64,
    pub name: String,
    pub tier: SubscriptionTier,
    pub address: Address,
    pub balance: Decimal,
    pub signup_date: NaiveDate,
    pub last_login: DateTime<Utc>,
    pub tags: Vec<String>,
    pub phone: Option<String>,
}

impl CustomerRecord {
    /// True for the paid tiers.
    pub fn is_premium(&self) -> bool {
        match self.tier {
            SubscriptionTier::Premium | SubscriptionTier::Enterprise => true,
            SubscriptionTier::Basic => false,
        }
    }
}

/// Totals over all customers. `Decimal` keeps the balances exact.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct FinancialSummary {
    pub total_customers: i64,
    pub premium_customers: i64,
    pub total_balance: Decimal,
    pub average_balance: Decimal,
}

impl fmt::Display for FinancialSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            total_customers,
            premium_customers,
            total_balance,
            average_balance,
        } = self;
        write!(
            f,
            "{total_customers} customers ({premium_customers} premium), \
             total balance {total_balance}, average {average_balance}"
        )
    }
}

/// Computes the [`FinancialSummary`] of all customers.
pub struct SummarizeAccounts;

impl PTransform<PCollection<CustomerRecord>> for SummarizeAccounts {
    type Output = PCollection<FinancialSummary>;

    fn expand(&self, customers: &PCollection<CustomerRecord>) -> Self::Output {
        let summary = Compose::new((
            Sum::new().on(|_: &CustomerRecord| 1_i64),
            Sum::new().on(|customer: &CustomerRecord| i64::from(customer.is_premium())),
            Mean::new().on(|customer: &CustomerRecord| customer.balance),
            Sum::new().on(|customer: &CustomerRecord| customer.balance),
        ))
        .map_output(
            |(total_customers, premium_customers, average_balance, total_balance)| {
                FinancialSummary {
                    total_customers,
                    premium_customers,
                    total_balance,
                    average_balance,
                }
            },
        );
        customers.combine_globally("Summarize", summary)
    }
}
