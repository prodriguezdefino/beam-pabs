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

//! Schema-aware `Row` elements and the row coder.

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};

use crate::dofns::*;

/// URN of the portable schema'd row coder.
const ROW_CODER_URN: &str = "beam:coder:row:v1";

fn event(account: &str, amount: i64, tags: &[&str], verified: bool) -> AuditEvent {
    AuditEvent {
        account_id: account.to_string(),
        amount,
        tags: tags.iter().map(|t| t.to_string()).collect(),
        is_verified: verified,
    }
}

/// Validates `#[derive(BeamRow)]` elements travelling through the runner as
/// `beam:coder:row:v1`.
///
/// The events cross two runner-materialised boundaries: a `GroupByKey`, where they
/// are the values of a KV and then the elements of a grouped iterable, and a
/// `Reshuffle`, where they are bare elements. Each boundary encodes and decodes them
/// with the row coder, and the runner has to frame them, so a field written in the
/// wrong order or a mis-framed row fails the assertion (or the decode). The
/// collections' declared coder is checked first, so the test cannot silently fall
/// back to an opaque bytes coder.
pub fn build_row_schema_coder(p: &TestPipeline) {
    let events = p.apply(Create::new(
        "Create",
        vec![
            event("acct-1", 500, &["fintech", "vip"], true),
            event("acct-2", 1500, &["ecommerce"], false),
            event("acct-1", -20, &[], false),
        ],
    ));

    let grouped = events
        .key_by("KeyByAccount", |e: &AuditEvent| e.account_id.clone())
        .group_by_key("GroupByAccount");
    let adjusted = grouped
        .flat_map(
            "AddFee",
            |(_, group): (String, BeamIterable<AuditEvent>)| {
                group
                    .into_iter()
                    .map(|e| AuditEvent {
                        amount: e.amount + 100,
                        ..e
                    })
                    .collect::<Vec<_>>()
            },
        )
        .reshuffle("Reshuffle");

    {
        let proto = p.to_proto();
        let components = proto.components.as_ref().expect("pipeline components");
        for pcoll in [&events, &adjusted] {
            let coder_id = &components.pcollections[pcoll.id()].coder_id;
            let urn = components.coders[coder_id]
                .spec
                .as_ref()
                .map(|s| s.urn.as_str());
            assert_eq!(
                urn,
                Some(ROW_CODER_URN),
                "PCollection {} must use the row coder",
                pcoll.id()
            );
        }
    }

    passert::that("AssertAdjusted", &adjusted).contains_in_any_order([
        event("acct-1", 600, &["fintech", "vip"], true),
        event("acct-2", 1600, &["ecommerce"], false),
        event("acct-1", 80, &[], false),
    ]);
}
