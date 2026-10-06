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

//! Row types and DoFns for the schema pipelines.

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::{BeamRow, FieldType, Schema};
use beam::transforms::StateRowSchemas;

#[derive(Debug, Clone, PartialEq, Eq, BeamRow)]
#[beam(crate = "::beam", id = "beam.runner_tests.AuditEvent")]
pub struct AuditEvent {
    pub account_id: String,
    pub amount: i64,
    pub tags: Vec<String>,
    pub is_verified: bool,
}

/// The key of a multimap side input of rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash, BeamRow)]
#[beam(crate = "::beam", id = "beam.runner_tests.RegionKey")]
pub struct RegionKey {
    pub region: String,
}

/// The value of a multimap side input of rows.
#[derive(Debug, Clone, PartialEq, Eq, BeamRow)]
#[beam(crate = "::beam", id = "beam.runner_tests.Quota")]
pub struct Quota {
    pub limit: i64,
    pub burst: Option<i64>,
}

/// The state of [`DynamicRowStateDoFn`]. The field names are the state ids.
#[derive(State)]
pub struct DynamicRowState {
    total: ValueState<Row>,
    history: BagState<Row>,
    by_tier: MapState<Row, i64>,
    tiers: SetState<Row>,
    tier_rows: MultimapState<Row, Row>,
}

#[derive(Timers)]
pub enum DynamicRowTimer {
    #[timer(event_time, id = "report")]
    Report,
}

/// Keeps dynamic `Row` values and `Row` keys in every kind of state cell. A timer reads the
/// cells back in a later bundle, so the rows cross the runner state API.
///
/// The input rows have the schema `input`, and the tier keys have one field `tier`.
/// [`StatefulDoFn::state_row_schemas`] gives both schemas to the cells.
#[derive(Clone)]
pub struct DynamicRowStateDoFn {
    input: Arc<Schema>,
    tier: Arc<Schema>,
}

impl DynamicRowStateDoFn {
    /// A function for input rows of `input`, with the fields `acct` and `amount`.
    pub fn new(input: Arc<Schema>) -> Self {
        Self {
            input,
            tier: Arc::new(Schema::builder().field("tier", FieldType::string()).build()),
        }
    }
}

/// The value of the `tier` field of `row`.
fn tier_name(row: &Row) -> Result<String> {
    Ok(row
        .get_string("tier")?
        .ok_or("the tier is null")?
        .to_string())
}

impl StatefulDoFn for DynamicRowStateDoFn {
    type Key = String;
    type Value = Row;
    type Out = String;
    type State = DynamicRowState;
    type Timer = DynamicRowTimer;

    fn process_element(
        &mut self,
        account: &String,
        value: Row,
        state: &mut DynamicRowState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let amount = value.get_i64("amount")?.ok_or("the amount is null")?;
        let total = match state.total.read()? {
            Some(row) => row.get_i64("amount")?.unwrap_or(0),
            None => 0,
        };
        let updated = Row::builder(Arc::clone(&self.input))
            .with_value(account.as_str())
            .with_value(total + amount)
            .build()?;
        state.total.write(&updated)?;
        state.history.append(&value)?;
        let tier = Row::builder(Arc::clone(&self.tier))
            .with_value(if amount >= 10 { "high" } else { "low" })
            .build()?;
        let count = state.by_tier.get(&tier)?.unwrap_or(0);
        state.by_tier.put(&tier, &(count + 1))?;
        state.tiers.insert(&tier)?;
        state.tier_rows.put(&tier, &value)?;
        ctx.timer(DynamicRowTimer::Report)
            .at(Instant::from_millis(1_000))
    }

    fn on_timer(
        &mut self,
        timer: DynamicRowTimer,
        account: &String,
        state: &mut DynamicRowState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            DynamicRowTimer::Report => {
                let total = match state.total.read()? {
                    Some(row) => row.get_i64("amount")?.unwrap_or(0),
                    None => 0,
                };
                let history = state.history.read()?.len();
                let mut counts = state
                    .by_tier
                    .entries()?
                    .into_iter()
                    .map(|(tier, count)| Ok(format!("{}:{count}", tier_name(&tier)?)))
                    .collect::<Result<Vec<_>>>()?;
                counts.sort();
                let mut tiers = state
                    .tiers
                    .read()?
                    .iter()
                    .map(tier_name)
                    .collect::<Result<Vec<_>>>()?;
                tiers.sort();
                let mut sums = state
                    .tier_rows
                    .entries()?
                    .into_iter()
                    .map(|(tier, rows)| {
                        let sum = rows
                            .iter()
                            .map(|row| Ok(row.get_i64("amount")?.unwrap_or(0)))
                            .sum::<Result<i64>>()?;
                        Ok(format!("{}:{sum}", tier_name(&tier)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                sums.sort();
                ctx.emit(format!(
                    "{account}|total={total}|history={history}|counts={}|tiers={}|sums={}",
                    counts.join(","),
                    tiers.join(","),
                    sums.join(",")
                ))
            }
        }
    }

    fn state_row_schemas(&self) -> StateRowSchemas {
        StateRowSchemas::new()
            .with_value("total", Arc::clone(&self.input))
            .with_value("history", Arc::clone(&self.input))
            .with_key("by_tier", Arc::clone(&self.tier))
            .with_key("tiers", Arc::clone(&self.tier))
            .with_key("tier_rows", Arc::clone(&self.tier))
            .with_value("tier_rows", Arc::clone(&self.input))
    }
}
