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

//! Streaming Prism integration tests for `Query10` sharded log and index file archival.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use beam::coders::{Coder, Context};
use beam::prelude::*;
use beam::testing::{TestPipelineError, passert};
use nexmark::config::{NexmarkConfig, NexmarkQueryName};
use nexmark::model::{Done, Event, EventCoder};
use nexmark::models;
use nexmark::queries::Query10;
use nexmark::source::UnboundedEventSource;

use crate::{base_streaming_config, streaming_pipeline};

fn event_sort_key(ev: &Event) -> Vec<u8> {
    let mut buf = Vec::new();
    EventCoder
        .encode(ev, &mut buf, Context::WholeStream)
        .expect("encode event key");
    buf
}

fn decode_log_directory(dir: &Path) -> (Vec<String>, Vec<String>, Vec<Event>) {
    let (mut index_files, mut log_files, events) = fs::read_dir(dir)
        .expect("read Query10 output dir")
        .map(|entry| entry.expect("dir entry"))
        .fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut indices, mut logs, mut events), entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with("INDEX-") {
                    indices.push(name);
                } else if name.starts_with("LOG-") {
                    let bytes = fs::read(entry.path()).expect("read LOG file");
                    let mut reader = bytes.as_slice();
                    events.extend(std::iter::from_fn(|| {
                        (!reader.is_empty()).then(|| {
                            EventCoder
                                .decode(&mut reader, Context::Nested)
                                .expect("decode Event from LOG file")
                        })
                    }));
                    logs.push(name);
                }
                (indices, logs, events)
            },
        );
    index_files.sort();
    log_files.sort();
    (index_files, log_files, events)
}

/// Fails when an INDEX line names a LOG file that does not exist.
fn assert_index_references_exist(dir: &Path, index_files: &[String]) {
    let referenced_logs: BTreeSet<String> = index_files
        .iter()
        .flat_map(|idx_name| {
            let content = fs::read_to_string(dir.join(idx_name)).expect("read INDEX file");
            content
                .lines()
                .filter_map(|line| line.split_whitespace().nth(4).map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .collect();
    for path in &referenced_logs {
        assert!(
            Path::new(path).exists(),
            "INDEX referenced missing LOG file {path}"
        );
    }
}

fn check_done_markers(actual: &[Done]) -> Result {
    if actual.is_empty() {
        return Err("expected at least one Done output from Query10".into());
    }
    if let Some(bad) = actual
        .iter()
        .find(|d| !d.message.starts_with("written for timestamp "))
    {
        return Err(format!("unexpected Done message: {bad:?}").into());
    }
    Ok(())
}

#[tokio::test]
async fn query10_writes_sharded_logs_and_indices_for_on_time_stream()
-> std::result::Result<(), TestPipelineError> {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let out_dir = temp_dir.path().to_string_lossy().into_owned();

    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::LogToShardedFiles),
        num_events: 240,
        num_event_generators: 2,
        first_event_rate: 80,
        next_event_rate: 80,
        window_size_sec: 1,
        max_log_events: 50,
        ..base_streaming_config()
    };

    let p = streaming_pipeline();
    let done = p
        .apply(
            "ReadUnbounded",
            UnboundedEventSource::new(&config).with_batch_size(30),
        )
        .apply(
            "Query10",
            Query10::from_config(&config)
                .with_output_path(Some(out_dir))
                .with_max_num_workers(2),
        );
    passert::that("Query10DoneMarkers", &done).satisfies(check_done_markers);

    let result = p.run().await?;
    let metrics = result.metrics().expect("Prism reports pipeline metrics");
    // No exact counts: prism/internal/jobservices/metrics.go contributeMetrics can drop updates.
    assert_eq!(metrics.counter("Query10", "actuallyLateEvent"), None);
    assert!(metrics.counter("Query10", "savedFile").unwrap_or(0) > 0);
    assert!(metrics.counter("Query10", "indexed").unwrap_or(0) > 0);
    assert_eq!(metrics.counter("Query10", "ERROR_unexpectedLatePane"), None);
    assert_eq!(
        metrics.counter("Query10", "ERROR_unexpectedOnTimeElement"),
        None
    );
    assert_eq!(metrics.counter("Query10", "ERROR_unexpectedLate"), None);
    assert_eq!(metrics.counter("Query10", "ERROR_unexpectedEarly"), None);
    assert_eq!(metrics.counter("Query10", "ERROR_unexpectedIndex"), None);

    let (index_files, log_files, mut decoded_events) = decode_log_directory(temp_dir.path());
    assert!(!index_files.is_empty(), "expected INDEX-* files on disk");
    assert!(!log_files.is_empty(), "expected LOG-* files on disk");
    assert_index_references_exist(temp_dir.path(), &index_files);

    let mut expected_events: Vec<Event> = models::query0(&config)
        .into_iter()
        .map(|(ev, _)| ev)
        .collect();
    decoded_events.sort_by_key(event_sort_key);
    expected_events.sort_by_key(event_sort_key);
    assert_eq!(decoded_events, expected_events);
    Ok(())
}

#[tokio::test]
async fn query10_archives_delayed_late_events_and_early_count_panes()
-> std::result::Result<(), TestPipelineError> {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let out_dir = temp_dir.path().to_string_lossy().into_owned();

    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::LogToShardedFiles),
        num_events: 240,
        num_event_generators: 2,
        first_event_rate: 60,
        next_event_rate: 60,
        window_size_sec: 1,
        max_log_events: 8,
        occasional_delay_sec: 3,
        prob_delayed_event: 0.25,
        ..base_streaming_config()
    };

    let p = streaming_pipeline();
    let done = p
        .apply(
            "ReadUnboundedDelayed",
            UnboundedEventSource::new(&config).with_batch_size(12),
        )
        .apply(
            "Query10Delayed",
            Query10::from_config(&config)
                .with_output_path(Some(out_dir))
                .with_max_num_workers(1),
        );
    passert::that("Query10DelayedDone", &done).satisfies(check_done_markers);

    // Counts come from the files: prism/internal/jobservices/metrics.go contributeMetrics can drop updates.
    p.run().await?;

    let (index_files, log_files, decoded_events) = decode_log_directory(temp_dir.path());
    assert!(!index_files.is_empty(), "expected INDEX-* files on disk");
    assert!(!log_files.is_empty(), "expected LOG-* files on disk");
    assert_index_references_exist(temp_dir.path(), &index_files);
    assert!(
        decoded_events.iter().any(|ev| ev.has_annotation("LATE")),
        "expected the delayed stream to write LATE events"
    );

    let mut stripped_actual: Vec<Event> = decoded_events
        .into_iter()
        .map(|ev| ev.without_annotation("LATE"))
        .collect();
    let mut expected_events: Vec<Event> = models::query0(&config)
        .into_iter()
        .map(|(ev, _)| ev)
        .collect();
    stripped_actual.sort_by_key(event_sort_key);
    expected_events.sort_by_key(event_sort_key);
    assert_eq!(stripped_actual, expected_events);
    Ok(())
}

#[tokio::test]
async fn query10_without_output_path_emits_done_without_writing_files()
-> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::LogToShardedFiles),
        num_events: 120,
        num_event_generators: 1,
        first_event_rate: 60,
        next_event_rate: 60,
        window_size_sec: 1,
        max_log_events: 40,
        ..base_streaming_config()
    };

    let p = streaming_pipeline();
    let done = p
        .apply(
            "ReadUnboundedNullSink",
            UnboundedEventSource::new(&config).with_batch_size(30),
        )
        .apply("Query10NullSink", Query10::from_config(&config));
    passert::that("Query10NullSinkDone", &done).satisfies(check_done_markers);

    let result = p.run().await?;
    let metrics = result.metrics().expect("Prism reports pipeline metrics");
    assert_eq!(metrics.counter("Query10", "writtenRecords"), None);
    assert!(metrics.counter("Query10", "savedFile").unwrap_or(0) > 0);
    assert!(metrics.counter("Query10", "indexed").unwrap_or(0) > 0);
    Ok(())
}
