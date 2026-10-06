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

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::FieldType;
use beam::testing::{TestPipeline, passert};
use chrono::{DateTime, NaiveDate};
use row_schemas::{
    Address, CustomerRecord, FinancialSummary, KeyedSummary, SubscriptionTier, SummarizeAccounts,
    SummarizeByTier,
};
use rust_decimal::Decimal;

fn customer(id: i64, tier: SubscriptionTier, cents: i64, phone: Option<&str>) -> CustomerRecord {
    CustomerRecord {
        id,
        name: format!("customer {id}"),
        tier,
        address: Address {
            street: "100 Market St".to_string(),
            city: "San Francisco".to_string(),
            postal_code: "94105".to_string(),
        },
        balance: Decimal::new(cents, 2),
        signup_date: NaiveDate::from_ymd_opt(2024, 1, 10).expect("valid date"),
        last_login: DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp"),
        seen_at: Instant::from_millis(1_700_000_500_000),
        tags: vec![format!("tag{id}")],
        phone: phone.map(str::to_string),
    }
}

/// The average is exact: 20 470.50 / 3 = 6 823.50.
#[tokio::test]
async fn summarizes_all_accounts_and_by_tier() {
    let p = TestPipeline::new();
    let customers = p.apply(
        "Customers",
        Create::new(vec![
            customer(
                101,
                SubscriptionTier::Enterprise,
                1_542_050,
                Some("+1-555-0100"),
            ),
            customer(102, SubscriptionTier::Premium, 480_000, None),
            customer(103, SubscriptionTier::Basic, 25_000, Some("+1-555-0199")),
        ]),
    );

    let summary = customers.apply("SummarizeAccounts", SummarizeAccounts);
    let by_tier = customers.apply("SummarizeByTier", SummarizeByTier);

    passert::that_singleton("AssertSummary", &summary).is_equal_to(FinancialSummary {
        total_customers: 3,
        premium_customers: 2,
        total_balance: Decimal::new(2_047_050, 2),
        average_balance: Decimal::new(682_350, 2),
    });
    passert::that("AssertByTier", &by_tier).contains_in_any_order([
        KeyedSummary {
            key: SubscriptionTier::Enterprise,
            summary: FinancialSummary {
                total_customers: 1,
                premium_customers: 1,
                total_balance: Decimal::new(1_542_050, 2),
                average_balance: Decimal::new(1_542_050, 2),
            },
        },
        KeyedSummary {
            key: SubscriptionTier::Premium,
            summary: FinancialSummary {
                total_customers: 1,
                premium_customers: 1,
                total_balance: Decimal::new(480_000, 2),
                average_balance: Decimal::new(480_000, 2),
            },
        },
        KeyedSummary {
            key: SubscriptionTier::Basic,
            summary: FinancialSummary {
                total_customers: 1,
                premium_customers: 0,
                total_balance: Decimal::new(25_000, 2),
                average_balance: Decimal::new(25_000, 2),
            },
        },
    ]);

    p.run().await.expect("pipeline and assertions must pass");
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
struct EvolvedAccount {
    id: i64,
    name: String,
    nickname: Option<String>,
}

#[tokio::test]
async fn from_rows_maps_reordered_writer_columns_and_defaults_optional_fields() {
    let writer_schema = Arc::new(
        Schema::builder()
            .field("legacy_region", FieldType::string())
            .field("name", FieldType::string())
            .field("id", FieldType::int64())
            .build(),
    );
    let rows = vec![
        Row::builder(Arc::clone(&writer_schema))
            .with_value("us-west")
            .with_value("Acme")
            .with_value(101_i64)
            .build()
            .expect("valid row"),
        Row::builder(Arc::clone(&writer_schema))
            .with_value("eu-central")
            .with_value("Globex")
            .with_value(102_i64)
            .build()
            .expect("valid row"),
    ];

    let p = TestPipeline::new();
    let accounts = p
        .apply("UpstreamRows", Create::new(rows))
        .with_row_schema(&writer_schema)
        .from_rows::<EvolvedAccount>("ProjectAccounts");

    passert::that("AssertAccounts", &accounts).contains_in_any_order([
        EvolvedAccount {
            id: 101,
            name: "Acme".to_string(),
            nickname: None,
        },
        EvolvedAccount {
            id: 102,
            name: "Globex".to_string(),
            nickname: None,
        },
    ]);

    p.run()
        .await
        .expect("schema evolution pipeline must succeed");
}
