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

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use chrono::{DateTime, NaiveDate};
use row_schemas::{Address, CustomerRecord, FinancialSummary, SubscriptionTier, SummarizeAccounts};
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
        tags: vec![format!("tag{id}")],
        phone: phone.map(str::to_string),
    }
}

/// The average is exact: 20 470.50 / 3 = 6 823.50.
#[tokio::test]
async fn summarizes_all_accounts() {
    let p = TestPipeline::new();
    let customers = vec![
        customer(
            101,
            SubscriptionTier::Enterprise,
            1_542_050,
            Some("+1-555-0100"),
        ),
        customer(102, SubscriptionTier::Premium, 480_000, None),
        customer(103, SubscriptionTier::Basic, 25_000, Some("+1-555-0199")),
    ];

    let summary = p
        .apply("Customers", Create::new(customers))
        .apply("SummarizeAccounts", SummarizeAccounts);

    passert::that_singleton("AssertSummary", &summary).is_equal_to(FinancialSummary {
        total_customers: 3,
        premium_customers: 2,
        total_balance: Decimal::new(2_047_050, 2),
        average_balance: Decimal::new(682_350, 2),
    });

    p.run().await.expect("pipeline and assertions must pass");
}
