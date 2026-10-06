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

//! `Option` elements through `beam:coder:nullable:v1` across stage boundaries.

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};

use crate::dofns::*;

/// Two values and two nulls.
fn options() -> Vec<Option<String>> {
    vec![Some("a".to_string()), None, Some("b".to_string()), None]
}

/// Validates `Option<String>` elements, values and nulls, through the assertion's
/// `GroupByKey`.
///
/// Java: `CreateTest.testCreateWithNullsAndValues`.
pub fn build_create_with_nulls_and_values(p: &TestPipeline) {
    let values = p.apply("Create", Create::new(options()));
    passert::that("AssertOptions", &values).contains_in_any_order(options());
}

fn event(account: &str, amount: i64) -> Option<AuditEvent> {
    Some(AuditEvent {
        account_id: account.to_string(),
        amount,
        tags: vec!["t".to_string()],
        is_verified: amount > 0,
    })
}

fn optional_events() -> Vec<Option<AuditEvent>> {
    vec![event("acct-1", 5), None, event("acct-2", -3)]
}

/// Validates `Option` elements of a `BeamRow` type across a `Reshuffle`. The runner does not
/// read the row coder, so it sends `nullable<length_prefix<row>>`: a present Row has a length
/// on the wire and none in the SDK bytes.
pub fn build_nullable_rows_through_reshuffle(p: &TestPipeline) {
    let events = p
        .apply("Create", Create::new(optional_events()))
        .reshuffle("Reshuffle");
    passert::that("AssertEvents", &events).contains_in_any_order(optional_events());
}

/// Validates KV values of type `Option<i64>` through a `GroupByKey`.
pub fn build_nullable_values_grouped_by_key(p: &TestPipeline) {
    let counts = p
        .apply(
            "Create",
            Create::new(vec![
                ("k".to_string(), Some(1i64)),
                ("k".to_string(), None),
                ("j".to_string(), None),
            ]),
        )
        .group_by_key("Group")
        .par_do_fn(
            "CountNulls",
            |(key, values): (String, BeamIterable<Option<i64>>),
             ctx: &mut ProcessContext<'_, (String, (i64, i64))>| {
                let values = values.into_vec()?;
                let nulls = values.iter().filter(|v| v.is_none()).count() as i64;
                ctx.emit((key, (values.len() as i64, nulls)))
            },
        );
    passert::that("AssertCounts", &counts)
        .contains_in_any_order([("k".to_string(), (2, 1)), ("j".to_string(), (1, 1))]);
}
