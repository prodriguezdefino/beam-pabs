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

//! Event-time triggers and TestStream test suite.
//!
//! Exercises watermark advancement, event-time timers, early/on-time/late pane
//! firings, and accumulation modes with in-memory `TestStream`.
//!
//! Executed against runners that implement `TestStream` support (e.g. PrismRunner).

use prism::PrismRunner;
use tests::*;

fn runner() -> PrismRunner {
    prism_runner_from_env()
}

#[tokio::test]
async fn test_trigger_repeated_count_discarding() {
    run_pipeline(
        "trigger_repeated_count_discarding",
        build_trigger_repeated_count_discarding,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_trigger_early_firings_accumulating() {
    run_pipeline(
        "trigger_early_firings_accumulating",
        build_trigger_early_firings_accumulating,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_trigger_late_firings_discarding() {
    run_pipeline(
        "trigger_late_firings_discarding",
        build_trigger_late_firings_discarding,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_test_stream_windowing() {
    run_pipeline(
        "test_stream_windowing",
        build_test_stream_windowing,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}
