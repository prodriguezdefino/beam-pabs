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

//! In-pipeline assertion (`passert`) and negative error handling tests.
//!
//! Verifies that assertions properly detect failures, correctly report mismatched
//! elements, truncate voluminous errors, and that worker runtime panics propagate
//! failure back to the driver.
//!
//! Executed against the local Prism runner where driver errors and worker panics
//! are reported immediately without remote retry loops.

use prism::PrismRunner;
use tests::*;

fn runner() -> PrismRunner {
    PrismRunner::new()
}

#[tokio::test]
async fn test_passert_failure() {
    run_pipeline(
        "passert_failure",
        build_passert_failure,
        PASSERT_FAILURE,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_passert_failure_elides_large_collections() {
    run_pipeline(
        "passert_failure_elides_large_collections",
        build_passert_failure_elides_large_collections,
        PASSERT_FAILURE_ELIDES_LARGE_COLLECTIONS,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_passert_on_empty_collection() {
    run_pipeline(
        "passert_on_empty_collection",
        build_passert_on_empty_collection,
        PASSERT_ON_EMPTY_COLLECTION,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_partition_out_of_range_fails() {
    run_pipeline(
        "partition_out_of_range_fails",
        build_partition_out_of_range_fails,
        PARTITION_OUT_OF_RANGE_FAILS,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_empty_singleton_side_input_fails() {
    run_pipeline(
        "empty_singleton_side_input_fails",
        build_empty_singleton_side_input_fails,
        EMPTY_SINGLETON_SIDE_INPUT_FAILS,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_multi_element_singleton_side_input_fails() {
    run_pipeline(
        "multi_element_singleton_side_input_fails",
        build_multi_element_singleton_side_input_fails,
        MULTI_ELEMENT_SINGLETON_SIDE_INPUT_FAILS,
        &runner(),
    )
    .await;
}
