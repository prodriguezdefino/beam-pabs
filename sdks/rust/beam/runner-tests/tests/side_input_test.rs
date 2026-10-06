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

//! Runner-backed execution tests for side inputs, `ProcessContext`, and broadcast joins.

use std::sync::{Arc, Mutex};

use beam::pipeline::{Pipeline, URN_GROUP_BY_KEY, URN_PAR_DO};
use beam::prelude::*;
use prism::PrismRunner;

#[tokio::test]
async fn test_dsl_tier2_and_tier3_unified_dofn_with_process_context() {
    #[derive(Clone)]
    struct EnrichOrderDoFn {
        multiplier_view: PCollectionView<i64>,
        vip_users_view: PCollectionView<String>,
    }

    impl DoFn for EnrichOrderDoFn {
        type In = (String, i64);
        type Out = String;

        fn process_element(
            &mut self,
            (user, points): Self::In,
            ctx: &mut ProcessContext<'_, Self::Out>,
        ) -> Result {
            let mult = ctx.side_input(&self.multiplier_view)?;
            let vips = ctx.side_input_iter(&self.vip_users_view)?;
            let total = if vips.contains(&user) {
                points * mult
            } else {
                points
            };
            ctx.emit(format!("{user}={total}"))
        }
    }

    let p = Pipeline::new();
    let mult_view = p
        .apply(Create::new("Multiplier", vec![3_i64]))
        .as_singleton();
    let vip_view = p
        .apply(Create::new("VIPs", vec!["alice".to_string()]))
        .as_iter();

    let orders = p.apply(Create::new(
        "Orders",
        vec![("alice".to_string(), 10_i64), ("bob".to_string(), 10_i64)],
    ));

    // Tier 3: Custom DoFn struct with multiple side inputs
    let enriched = orders.apply(
        ParDo::new(
            "EnrichOrders",
            EnrichOrderDoFn {
                multiplier_view: mult_view.clone(),
                vip_users_view: vip_view.clone(),
            },
        )
        .with_side_input(&mult_view)
        .with_side_input(&vip_view),
    );

    // Tier 2: Closure with ProcessContext
    let prefix_view = p
        .apply(Create::new("Prefix", vec!["[REWARD]".to_string()]))
        .as_singleton();
    let pv_clone = prefix_view.clone();
    let formatted = enriched.apply(
        ParDo::from_fn("FormatRewards", move |s, ctx| {
            let pfx = ctx.side_input(&pv_clone)?;
            ctx.emit(format!("{pfx} {s}"))
        })
        .with_side_input(&prefix_view),
    );

    let captured = Arc::new(Mutex::new(Vec::new()));
    let c = Arc::clone(&captured);
    formatted.inspect("Capture", move |s: &String| {
        c.lock().unwrap().push(s.clone());
    });

    let runner = PrismRunner::new();
    p.run_with_runner(&runner)
        .await
        .expect("Tier 2/3 pipeline should succeed");

    let mut got = captured.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec![
            "[REWARD] alice=30".to_string(),
            "[REWARD] bob=10".to_string(),
        ]
    );
}

#[tokio::test]
async fn test_dsl_broadcast_joins_no_shuffle_and_execution() {
    let p = Pipeline::new();

    let events = p.apply(Create::new(
        "Events",
        vec![
            ("us".to_string(), 1_i64),
            ("eu".to_string(), 2_i64),
            ("apac".to_string(), 3_i64),
        ],
    ));
    let regions = p.apply(Create::new(
        "Regions",
        vec![
            ("us".to_string(), "United States".to_string()),
            ("eu".to_string(), "Europe".to_string()),
        ],
    ));

    let inner = events.broadcast_inner_join("BroadcastInner", &regions);
    let left = events.broadcast_left_join("BroadcastLeft", &regions);

    // Verify graph normalization: NO GroupByKey transform exists in the pipeline!
    let proto = p.to_proto();
    let transforms = &proto.components.as_ref().unwrap().transforms;
    assert!(
        !transforms
            .values()
            .any(|t| t.spec.as_ref().is_some_and(|s| s.urn == URN_GROUP_BY_KEY)),
        "Broadcast joins must not introduce a GroupByKey shuffle"
    );
    assert!(
        transforms.values().any(
            |t| t.unique_name == "BroadcastInner" && t.spec.as_ref().unwrap().urn == URN_PAR_DO
        )
    );

    let inner_cap = Arc::new(Mutex::new(Vec::new()));
    let ic = Arc::clone(&inner_cap);
    inner.inspect("CapInner", move |(k, (id, name))| {
        ic.lock().unwrap().push(format!("{k}:{id}:{name}"));
    });

    let left_cap = Arc::new(Mutex::new(Vec::new()));
    let lc = Arc::clone(&left_cap);
    left.inspect("CapLeft", move |(k, (id, name_opt))| {
        let label = name_opt.clone().unwrap_or_else(|| "UNKNOWN".to_string());
        lc.lock().unwrap().push(format!("{k}:{id}:{label}"));
    });

    let runner = PrismRunner::new();
    p.run_with_runner(&runner)
        .await
        .expect("Broadcast joins should succeed");

    let mut got_inner = inner_cap.lock().unwrap().clone();
    got_inner.sort();
    assert_eq!(
        got_inner,
        vec!["eu:2:Europe".to_string(), "us:1:United States".to_string(),]
    );

    let mut got_left = left_cap.lock().unwrap().clone();
    got_left.sort();
    assert_eq!(
        got_left,
        vec![
            "apac:3:UNKNOWN".to_string(),
            "eu:2:Europe".to_string(),
            "us:1:United States".to_string(),
        ]
    );
}
