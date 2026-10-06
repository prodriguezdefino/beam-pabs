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

//! Row Schemas example: demonstrates Apache Beam's type-driven schema system in Rust.
//!
//! Beam Schemas provide portable, language-agnostic structured typing for elements.
//! This example shows how to:
//! - Derive schemas automatically with `#[derive(BeamRow)]` and `#[derive(BeamEnum)]`.
//! - Represent complex structures with nested structs, arrays, and optional fields.
//! - Use standard logical types:
//!   - `chrono::NaiveDate` (`beam:logical_type:date:v1`)
//!   - `chrono::DateTime<Utc>` (`beam:logical_type:micros_instant:v1`)
//!   - `rust_decimal::Decimal` (`beam:logical_type:decimal:v1`) for exact financial math.
//! - Process strongly-typed `BeamRow` collections with `.map()`, `.filter()`, and `.combine_globally()`.

use beam::prelude::*;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

/// Customer subscription tier represented as a fieldless enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, BeamEnum)]
pub enum SubscriptionTier {
    Basic,
    Premium,
    Enterprise,
}

/// Physical postal address nested within customer records.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct Address {
    pub street: String,
    pub city: String,
    pub postal_code: String,
}

/// Full customer account record demonstrating primitive, logical, and nested fields.
#[derive(Clone, Debug, PartialEq, BeamRow)]
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

/// Intermediate accumulator for account aggregations.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct FinancialAccum {
    pub total_customers: i64,
    pub total_balance: Decimal,
    pub premium_customers: i64,
}

/// Aggregated financial summary computed across all customer records.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct FinancialSummary {
    pub total_customers: i64,
    pub total_balance: Decimal,
    pub premium_customers: i64,
    pub average_balance: Decimal,
}

/// Associative combiner computing financial metrics across customer records.
#[derive(Clone, Debug, Default)]
pub struct AccountAggregator;

impl CombineFn for AccountAggregator {
    type Input = CustomerRecord;
    type Accum = FinancialAccum;
    type Output = FinancialSummary;

    fn create_accumulator(&self) -> Self::Accum {
        FinancialAccum {
            total_customers: 0,
            total_balance: Decimal::ZERO,
            premium_customers: 0,
        }
    }

    fn add_input(&self, mut acc: Self::Accum, input: Self::Input) -> Self::Accum {
        let is_premium = match input.tier {
            SubscriptionTier::Premium | SubscriptionTier::Enterprise => 1,
            SubscriptionTier::Basic => 0,
        };
        acc.total_customers += 1;
        acc.total_balance += input.balance;
        acc.premium_customers += is_premium;
        acc
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> Self::Accum {
        accumulators
            .into_iter()
            .fold(self.create_accumulator(), |mut a, b| {
                a.total_customers += b.total_customers;
                a.total_balance += b.total_balance;
                a.premium_customers += b.premium_customers;
                a
            })
    }

    fn extract_output(&self, acc: Self::Accum) -> Self::Output {
        let avg = if acc.total_customers > 0 {
            acc.total_balance / Decimal::from(acc.total_customers)
        } else {
            Decimal::ZERO
        };

        FinancialSummary {
            total_customers: acc.total_customers,
            total_balance: acc.total_balance,
            premium_customers: acc.premium_customers,
            average_balance: avg,
        }
    }
}

/// Generates a curated set of sample customer accounts for demonstration.
pub fn sample_customers() -> Vec<CustomerRecord> {
    vec![
        CustomerRecord {
            id: 101,
            name: "Acme Corp".to_string(),
            tier: SubscriptionTier::Enterprise,
            address: Address {
                street: "100 Market St".to_string(),
                city: "San Francisco".to_string(),
                postal_code: "94105".to_string(),
            },
            balance: Decimal::new(1542050, 2), // $15,420.50
            signup_date: NaiveDate::from_ymd_opt(2022, 3, 15).expect("valid date"),
            last_login: DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp"),
            tags: vec!["b2b".to_string(), "cloud".to_string()],
            phone: Some("+1-555-0100".to_string()),
        },
        CustomerRecord {
            id: 102,
            name: "Beta Labs".to_string(),
            tier: SubscriptionTier::Premium,
            address: Address {
                street: "456 Tech Blvd".to_string(),
                city: "Austin".to_string(),
                postal_code: "78701".to_string(),
            },
            balance: Decimal::new(480000, 2), // $4,800.00
            signup_date: NaiveDate::from_ymd_opt(2023, 7, 1).expect("valid date"),
            last_login: DateTime::from_timestamp(1_705_000_000, 0).expect("valid timestamp"),
            tags: vec!["ai".to_string()],
            phone: None,
        },
        CustomerRecord {
            id: 103,
            name: "Charlie Dev".to_string(),
            tier: SubscriptionTier::Basic,
            address: Address {
                street: "789 Pine Ave".to_string(),
                city: "Seattle".to_string(),
                postal_code: "98101".to_string(),
            },
            balance: Decimal::new(25000, 2), // $250.00
            signup_date: NaiveDate::from_ymd_opt(2024, 1, 10).expect("valid date"),
            last_login: DateTime::from_timestamp(1_708_000_000, 0).expect("valid timestamp"),
            tags: vec!["starter".to_string()],
            phone: Some("+1-555-0199".to_string()),
        },
    ]
}

/// Builds the row schemas pipeline: loads records, filters, aggregates, and emits the summary.
pub fn build_row_schemas_pipeline(
    pipeline: &Pipeline,
    customers: Vec<CustomerRecord>,
) -> PCollection<FinancialSummary> {
    pipeline
        .apply(Create::new("CreateCustomers", customers))
        .filter("FilterActiveWithTags", |cust: &CustomerRecord| {
            !cust.tags.is_empty()
        })
        .combine_globally("AggregateAccounts", AccountAggregator)
}
