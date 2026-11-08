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

//! Batch Prism tests for the `CSV` side-input table and parallel suite runs.

use std::fs;

use beam::options::PipelineOptions;
use beam::testing::TestPipelineError;
use nexmark::config::{NexmarkConfig, NexmarkQueryName, NexmarkSuite, SideInputType};
use nexmark::launcher::{
    MonitorTiming, NexmarkArgs, NexmarkRunOutcome, StopReason, build_side_input_pipeline,
    run_nexmark,
};
use nexmark::models;
use nexmark::queries::{BoundedSideInputJoin, SessionSideInputJoin, SideInput};

use crate::assert_query_matches_model;

#[tokio::test]
async fn side_input_joins_read_the_csv_table_and_match_the_model() -> Result<(), TestPipelineError>
{
    let dir = tempfile::tempdir().expect("temp dir");
    let url = format!("{}/side", dir.path().display());
    let bounded = NexmarkConfig {
        query: Some(NexmarkQueryName::BoundedSideInputJoin),
        num_events: 400,
        num_event_generators: 2,
        side_input_type: SideInputType::Csv,
        side_input_row_count: 37,
        side_input_num_shards: 2,
        ..NexmarkConfig::default()
    };
    let session = NexmarkConfig {
        query: Some(NexmarkQueryName::SessionSideInputJoin),
        num_events: 500,
        num_event_generators: 2,
        first_event_rate: 50,
        next_event_rate: 50,
        num_active_people: 20,
        side_input_type: SideInputType::Csv,
        side_input_row_count: 29,
        side_input_num_shards: 2,
        session_gap_sec: 1,
        ..NexmarkConfig::default()
    };
    let args = NexmarkArgs {
        side_input_url: Some(url.clone()),
        ..NexmarkArgs::default()
    };
    build_side_input_pipeline(&PipelineOptions::default(), &args, &[bounded, session])
        .expect("side input job")
        .expect("CSV joins need a job")
        .run()
        .await
        .expect("side input job runs");
    let shards = fs::read_dir(dir.path())
        .expect("read temp dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("side"))
        .count();
    assert_eq!(shards, 2);

    let csv = || SideInput::Csv { url: url.clone() };
    assert_query_matches_model(
        "BoundedSideInputJoin",
        &bounded,
        BoundedSideInputJoin::from_config(&bounded).with_side_input(csv()),
        models::bounded_side_input_join(&bounded),
    )
    .await?;
    assert_query_matches_model(
        "SessionSideInputJoin",
        &session,
        SessionSideInputJoin::from_config(&session).with_side_input(csv()),
        models::session_side_input_join(&session),
    )
    .await
}

#[tokio::test]
async fn parallel_suite_keeps_the_order_and_skips_the_listed_queries() -> beam::Result {
    let dir = tempfile::tempdir().expect("temp dir");
    let args = NexmarkArgs {
        suite: NexmarkSuite::Smoke,
        num_events: Some(300),
        num_event_generators: Some(2),
        assert_correctness: true,
        side_input_type: Some(SideInputType::Csv),
        side_input_url: Some(format!("{}/side", dir.path().display())),
        skip_queries: vec![NexmarkQueryName::HotItems, NexmarkQueryName::Reshuffle],
        nexmark_parallel: 4,
        ..NexmarkArgs::default()
    };
    let summaries = run_nexmark(
        &PipelineOptions::default(),
        &args,
        &MonitorTiming::default(),
    )
    .await?;

    let order: Vec<NexmarkQueryName> = summaries.iter().map(|summary| summary.query).collect();
    assert_eq!(order, NexmarkQueryName::ALL);
    for summary in &summaries {
        match (summary.query, &summary.outcome) {
            (
                NexmarkQueryName::HotItems | NexmarkQueryName::Reshuffle,
                NexmarkRunOutcome::Skipped { reason },
            ) => assert_eq!(*reason, "--skip_queries"),
            (NexmarkQueryName::UserSessions, NexmarkRunOutcome::Skipped { .. }) => {}
            (
                _,
                NexmarkRunOutcome::Executed {
                    stop: StopReason::Finished,
                    ..
                },
            ) => {}
            (query, outcome) => panic!("unexpected outcome for {query}: {outcome:?}"),
        }
    }
    Ok(())
}
