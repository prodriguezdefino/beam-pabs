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
//! the broadcast joins, side inputs of a `TestStream` (Prism only), and view checks that run
//! only in the SDK (map duplicates, singleton defaults).

use std::collections::HashSet;

use beam::pipeline::{Pipeline, URN_GROUP_BY_KEY, URN_PAR_DO, URN_RESHUFFLE};
use beam::prelude::*;
use beam_model::pipeline as proto;
use beam_runner_tests::*;

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

/// Early and on-time panes of a triggered `CombinePerKey` side input materialize per window.
///
/// Python: `SideInputsTest.test_multi_triggered_gbk_side_input`.
#[tokio::test]
async fn test_test_stream_triggered_side_input() {
    run_pipeline(
        "test_stream_triggered_side_input",
        build_test_stream_triggered_side_input,
        Expectation::Succeeds,
        &prism_runner_from_env(),
    )
    .await;
}

/// A map side input fails the bundle on a key with two values. The check runs in the SDK, so
/// it is not a ValidatesRunner test.
///
/// Java: `MapViewTest.testMapSideInputWithNullValuesCatchesDuplicates`.
#[tokio::test]
async fn test_map_side_input_fails_on_duplicate_key() {
    run_pipeline(
        "map_side_input_fails_on_duplicate_key",
        build_map_side_input_fails_on_duplicate_key,
        Expectation::FailsWith(&["more than one value"]),
        &prism_runner_from_env(),
    )
    .await;
}

/// A singleton view with a default reads the default in a main window whose side window is
/// empty.
///
/// Java: `ViewTest.testWindowedSideInputFixedToFixedWithDefault`.
#[tokio::test]
async fn test_windowed_singleton_with_default() {
    run_pipeline(
        "windowed_singleton_with_default",
        build_windowed_singleton_with_default,
        Expectation::Succeeds,
        &prism_runner_from_env(),
    )
    .await;
}

/// A singleton view of an empty input reads its `None` default.
///
/// Java: `ViewTest.testSideInputWithNullDefault`.
#[tokio::test]
async fn test_singleton_with_none_default() {
    run_pipeline(
        "singleton_with_none_default",
        build_singleton_with_none_default,
        Expectation::Succeeds,
        &prism_runner_from_env(),
    )
    .await;
}

/// A list side input reads every element by index and in order.
///
/// Java: `ViewTest.testListSideInput`, `ViewTest.testListWithRandomAccessSideInput`.
#[tokio::test]
async fn test_list_side_input() {
    run_pipeline(
        "list_side_input",
        build_list_side_input,
        Expectation::Succeeds,
        &prism_runner_from_env(),
    )
    .await;
}

/// A list side input in fixed windows reads the list of the main window.
///
/// Java: `ViewTest.testWindowedListSideInput`.
#[tokio::test]
async fn test_windowed_list_side_input() {
    run_pipeline(
        "windowed_list_side_input",
        build_windowed_list_side_input,
        Expectation::Succeeds,
        &prism_runner_from_env(),
    )
    .await;
}

/// An empty list side input has no element.
///
/// Java: `ViewTest.testEmptyListSideInput`.
#[tokio::test]
async fn test_empty_list_side_input() {
    run_pipeline(
        "empty_list_side_input",
        build_empty_list_side_input,
        Expectation::Succeeds,
        &prism_runner_from_env(),
    )
    .await;
}
