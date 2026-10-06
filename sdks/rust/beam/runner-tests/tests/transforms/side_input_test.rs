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

//! Side input checks that cannot run in the ValidatesRunner registry: the graph shape of
//! the broadcast joins, and side inputs of a `TestStream` (Prism only).

use std::collections::HashSet;

use beam::pipeline::{Pipeline, URN_GROUP_BY_KEY, URN_PAR_DO, URN_RESHUFFLE};
use beam::prelude::*;
use model::pipeline as proto;
use tests::*;

fn urn_of(t: &proto::PTransform) -> Option<&str> {
    t.spec.as_ref().map(|s| s.urn.as_str())
}

/// The broadcast joins add no GroupByKey, other than the one in the reshuffle of `Create`.
#[test]
fn test_broadcast_joins_add_no_shuffle() {
    let p = Pipeline::new();
    let events = p.apply(
        "Events",
        Create::new(vec![("us".to_string(), 1_i64), ("eu".to_string(), 2_i64)]),
    );
    let regions = p.apply(
        "Regions",
        Create::new(vec![("us".to_string(), "United States".to_string())]),
    );
    events.broadcast_inner_join("BroadcastInner", &regions);
    events.broadcast_left_join("BroadcastLeft", &regions);

    let proto = p.to_proto();
    let transforms = &proto.components.as_ref().expect("components").transforms;
    let in_reshuffle: HashSet<&String> = transforms
        .values()
        .filter(|t| urn_of(t) == Some(URN_RESHUFFLE))
        .flat_map(|t| &t.subtransforms)
        .collect();
    assert!(
        !transforms
            .iter()
            .any(|(id, t)| !in_reshuffle.contains(id) && urn_of(t) == Some(URN_GROUP_BY_KEY)),
        "broadcast joins must not add a GroupByKey"
    );
    assert!(
        transforms
            .values()
            .any(|t| t.unique_name == "BroadcastInner" && urn_of(t) == Some(URN_PAR_DO))
    );
}

/// A main window with no side input data reads the iterable view as empty.
///
/// Java: `ViewTest.testWindowedSideInputNotPresent`.
#[tokio::test]
async fn test_windowed_side_input_not_present() {
    run_pipeline(
        "windowed_side_input_not_present",
        build_windowed_side_input_not_present,
        Expectation::Succeeds,
        &prism_runner_from_env(),
    )
    .await;
}
