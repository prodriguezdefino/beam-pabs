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

#[tokio::test]
async fn test_test_stream_state_and_timers() {
    run_pipeline(
        "test_stream_state_and_timers",
        build_test_stream_state_and_timers,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_test_stream_pane_info() {
    run_pipeline(
        "test_stream_pane_info",
        build_test_stream_pane_info,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[ignore = "Prism re-adds fired elements in accumulating mode and fires extra count panes"]
#[tokio::test]
async fn test_test_stream_pane_info_accumulating() {
    run_pipeline(
        "test_stream_pane_info_accumulating",
        build_test_stream_pane_info_accumulating,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

// Late data, processing-time triggers, chained stages and time-sorted input.

#[tokio::test]
async fn test_trigger_late_firings_accumulating() {
    run_pipeline(
        "trigger_late_firings_accumulating",
        build_trigger_late_firings_accumulating,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_test_stream_first_element_late() {
    run_pipeline(
        "test_stream_first_element_late",
        build_test_stream_first_element_late,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_test_stream_elements_at_end_of_global_window() {
    run_pipeline(
        "test_stream_elements_at_end_of_global_window",
        build_test_stream_elements_at_end_of_global_window,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_trigger_processing_time_early_panes() {
    run_pipeline(
        "trigger_processing_time_early_panes",
        build_trigger_processing_time_early_panes,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_test_stream_multi_stage() {
    run_pipeline(
        "test_stream_multi_stage",
        build_test_stream_multi_stage,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_test_stream_watermark_advance_inside_bundle() {
    run_pipeline(
        "test_stream_watermark_advance_inside_bundle",
        build_test_stream_watermark_advance_inside_bundle,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_trigger_processing_time_accumulating() {
    run_pipeline(
        "trigger_processing_time_accumulating",
        build_trigger_processing_time_accumulating,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[ignore = "Prism ignores OnTimeBehavior::FireIfNonempty in accumulating mode"]
#[tokio::test]
async fn test_trigger_processing_time_continuation() {
    run_pipeline(
        "trigger_processing_time_continuation",
        build_trigger_processing_time_continuation,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_time_sorted_input_from_test_stream() {
    run_pipeline(
        "time_sorted_input_from_test_stream",
        build_time_sorted_input_from_test_stream,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_time_sorted_input_drops_late_data() {
    run_pipeline(
        "time_sorted_input_drops_late_data",
        build_time_sorted_input_drops_late_data,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_two_time_sorted_stages_drop_late_data() {
    run_pipeline(
        "two_time_sorted_stages_drop_late_data",
        build_two_time_sorted_stages_drop_late_data,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_timer_firing_order_test_stream() {
    run_pipeline(
        "timer_firing_order_test_stream",
        build_timer_firing_order_test_stream,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_timer_within_allowed_lateness() {
    run_pipeline(
        "timer_within_allowed_lateness",
        build_timer_within_allowed_lateness,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_processing_time_timer_reset() {
    run_pipeline(
        "processing_time_timer_reset",
        build_processing_time_timer_reset,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

#[tokio::test]
async fn test_sdf_late_data() {
    run_pipeline(
        "sdf_late_data",
        build_sdf_late_data,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}

// Time bounds: the watermark at the end of the global window.

#[tokio::test]
async fn test_test_stream_watermark_past_end_of_global_window() {
    run_pipeline(
        "test_stream_watermark_past_end_of_global_window",
        build_test_stream_watermark_past_end_of_global_window,
        Expectation::Succeeds,
        &runner(),
    )
    .await;
}
