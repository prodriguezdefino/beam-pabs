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

//! Unit tests for BeamFnLogging layer, client micro-batching, and tracing integration.

use std::time::Duration;

use harness::logging::{
    BeamFnLoggingHandle, BeamFnLoggingLayer, LOG_QUEUE_BATCHES, LOG_QUEUE_ENTRIES, LoggingClient,
};
use model::fn_execution::{LogEntry, log_entry};
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;

mod common;
use common::within;

#[tokio::test]
async fn test_logging_client_micro_batching() {
    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(tx);

    (0..150).for_each(|i| {
        let _ = client.info(format!("Batch message {i}"));
    });

    // All 150 messages arrive, split across batches. A full batch holds 100 entries.
    let first_batch = within("the first batch", rx.recv())
        .await
        .expect("expected first batch of 100");
    let mut total_received = first_batch.log_entries.len();
    assert_eq!(first_batch.log_entries[0].message, "Batch message 0");

    client.flush().await;
    while total_received < 150 {
        if let Ok(Some(batch)) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            total_received += batch.log_entries.len();
        } else {
            break;
        }
    }
    assert_eq!(total_received, 150);
}

#[tokio::test]
async fn test_startup_buffering_and_drain() {
    let handle = BeamFnLoggingHandle::new();
    assert_eq!(handle.buffered_count(), 0);
    assert!(!handle.has_client());

    let entry = LogEntry {
        severity: log_entry::severity::Enum::Info as i32,
        message: "Early startup message".to_string(),
        ..Default::default()
    };
    handle.send(entry);
    assert_eq!(handle.buffered_count(), 1);

    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(tx);
    handle.set_client(client.clone());
    assert!(handle.has_client());
    assert_eq!(handle.buffered_count(), 0);

    client.flush().await;
    let batch = within("the drained startup batch", rx.recv())
        .await
        .expect("expected drained startup batch");
    assert_eq!(batch.log_entries.len(), 1);
    assert_eq!(batch.log_entries[0].message, "Early startup message");
}

#[tokio::test]
async fn test_beam_fn_logging_layer_with_tracing() {
    let handle = BeamFnLoggingHandle::new();
    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(tx);
    handle.set_client(client.clone());

    let layer = BeamFnLoggingLayer::new(handle);
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!("Test info message");
        tracing::warn!("Test warn message");
        tracing::error!("Test error message");

        // An event in a span takes instruction_id and transform_id from the span.
        let span = tracing::info_span!(
            "bundle_test",
            instruction_id = "test-instruction-999",
            transform_id = "test-transform-888"
        );
        let _enter = span.enter();
        tracing::info!("Inside bundle span");
    });

    client.flush().await;
    let batch = within("the batch of emitted events", rx.recv())
        .await
        .expect("expected batch with emitted events");
    assert_eq!(batch.log_entries.len(), 4);

    assert_eq!(
        batch.log_entries[0].severity,
        log_entry::severity::Enum::Info as i32
    );
    assert_eq!(batch.log_entries[0].message, "Test info message");
    assert!(batch.log_entries[0].instruction_id.is_empty());

    assert_eq!(
        batch.log_entries[1].severity,
        log_entry::severity::Enum::Warn as i32
    );
    assert_eq!(batch.log_entries[1].message, "Test warn message");

    assert_eq!(
        batch.log_entries[2].severity,
        log_entry::severity::Enum::Error as i32
    );
    assert_eq!(batch.log_entries[2].message, "Test error message");

    assert_eq!(batch.log_entries[3].message, "Inside bundle span");
    assert_eq!(batch.log_entries[3].instruction_id, "test-instruction-999");
    assert_eq!(batch.log_entries[3].transform_id, "test-transform-888");
}

#[tokio::test]
async fn test_transport_filtering() {
    let handle = BeamFnLoggingHandle::new();
    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(tx);
    handle.set_client(client.clone());

    let layer = BeamFnLoggingLayer::new(handle);
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        // The layer drops events from internal transport targets.
        tracing::event!(
            target: "tonic::transport::channel",
            tracing::Level::INFO,
            "gRPC connect"
        );
        tracing::event!(
            target: "hyper::proto::h2",
            tracing::Level::DEBUG,
            "HTTP/2 frame"
        );
        tracing::event!(
            target: "harness::logging",
            tracing::Level::INFO,
            "Internal logging msg"
        );

        // The layer keeps events from SDK targets.
        tracing::info!("Valid SDK event");
    });

    client.flush().await;
    let batch = within("the filtered batch", rx.recv())
        .await
        .expect("expected batch");
    assert_eq!(batch.log_entries.len(), 1);
    assert_eq!(batch.log_entries[0].message, "Valid SDK event");
}

/// Runs `body` under a subscriber wired to a fresh layer and returns the entries it produced.
/// `with_default` installs the subscriber for this thread only, so tests stay independent.
async fn capture_entries(body: impl FnOnce()) -> Vec<LogEntry> {
    let handle = BeamFnLoggingHandle::new();
    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(tx);
    handle.set_client(client.clone());

    let subscriber = tracing_subscriber::registry().with(BeamFnLoggingLayer::new(handle));
    tracing::subscriber::with_default(subscriber, body);

    client.flush().await;
    within("a batch of captured entries", rx.recv())
        .await
        .expect("expected a batch of captured entries")
        .log_entries
}

#[tokio::test]
async fn debug_and_trace_events_map_to_their_own_severities() {
    // The harness logs routine per-bundle messages at DEBUG, so levels must not collapse to INFO.
    let entries = capture_entries(|| {
        tracing::debug!("a debug message");
        tracing::trace!("a trace message");
    })
    .await;

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].severity, log_entry::severity::Enum::Debug as i32);
    assert_eq!(entries[0].message, "a debug message");
    assert_eq!(entries[1].severity, log_entry::severity::Enum::Trace as i32);
    assert_eq!(entries[1].message, "a trace message");
}

#[tokio::test]
async fn metadata_recorded_after_span_creation_is_picked_up() {
    // Fields declared as `Empty` and filled in later go through `Layer::on_record`.
    let entries = capture_entries(|| {
        let span = tracing::info_span!(
            "deferred",
            instruction_id = tracing::field::Empty,
            transform_id = tracing::field::Empty
        );
        span.record("instruction_id", "instr-late");
        span.record("transform_id", "transform-late");

        let _enter = span.enter();
        tracing::info!("after record");
    })
    .await;

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].instruction_id, "instr-late");
    assert_eq!(entries[0].transform_id, "transform-late");
}

#[tokio::test]
async fn typed_fields_become_typed_custom_data() {
    use prost_types::value::Kind;

    let entries = capture_entries(|| {
        tracing::info!(
            count = 7i64,
            size = 9u64,
            enabled = true,
            name = "bundle",
            "with fields"
        );
    })
    .await;

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].message, "with fields");

    let fields = &entries[0]
        .custom_data
        .as_ref()
        .expect("custom fields must be attached")
        .fields;

    assert_eq!(fields["count"].kind, Some(Kind::NumberValue(7.0)));
    assert_eq!(fields["size"].kind, Some(Kind::NumberValue(9.0)));
    assert_eq!(fields["enabled"].kind, Some(Kind::BoolValue(true)));
    assert_eq!(
        fields["name"].kind,
        Some(Kind::StringValue("bundle".to_string()))
    );
}

#[tokio::test]
async fn entries_carry_their_source_location_and_thread() {
    let entries = capture_entries(|| tracing::info!("located")).await;

    assert_eq!(entries.len(), 1);
    assert!(
        entries[0].log_location.contains("logging_test.rs:"),
        "expected a file:line location, got '{}'",
        entries[0].log_location
    );
    assert!(
        !entries[0].thread.is_empty(),
        "every entry should name the thread that produced it"
    );
}

#[tokio::test]
async fn every_transport_target_prefix_is_filtered() {
    // Logging the transport that ships the logs would recurse, so all these prefixes are dropped.
    let entries = capture_entries(|| {
        tracing::event!(target: "h2::codec", tracing::Level::INFO, "framed write");
        tracing::event!(target: "rustls::session", tracing::Level::INFO, "handshake");
        tracing::event!(target: "tower::buffer", tracing::Level::INFO, "queued");
        tracing::info!("survives");
    })
    .await;

    let messages: Vec<&str> = entries.iter().map(|e| e.message.as_str()).collect();
    assert_eq!(
        messages,
        vec!["survives"],
        "only the SDK event should reach the handle"
    );
}

#[test]
fn the_startup_buffer_drops_the_oldest_entries_when_full() {
    // Without a bound, a worker that never connects would grow the buffer until it is
    // killed. The cap keeps the newest entries, which are the ones worth reporting.
    const CAPACITY: usize = 2048;

    let handle = BeamFnLoggingHandle::new();
    for i in 0..CAPACITY + 10 {
        handle.send(LogEntry {
            severity: log_entry::severity::Enum::Info as i32,
            message: format!("entry {i}"),
            ..Default::default()
        });
    }

    assert_eq!(handle.buffered_count(), CAPACITY);
    assert!(!handle.has_client());
}

#[tokio::test(flavor = "current_thread")]
async fn a_full_log_queue_drops_entries_and_reports_how_many() {
    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(tx);

    // The batching task cannot run until this task yields, so the queue fills up.
    (0..LOG_QUEUE_ENTRIES + 5).for_each(|i| {
        client.info(format!("entry {i}")).expect("never blocks");
    });
    client.flush().await;

    let mut entries = Vec::new();
    while entries.len() < LOG_QUEUE_ENTRIES + 1 {
        let batch = within("a batch of queued entries", rx.recv())
            .await
            .expect("batch");
        entries.extend(batch.log_entries);
    }
    let warning = entries
        .iter()
        .find(|e| e.message == "Dropped 5 log entries: the logging stream fell behind")
        .expect("the overflow is reported");
    // The runner files entries by severity and time, so the note needs both.
    assert_eq!(warning.severity, log_entry::severity::Enum::Warn as i32);
    assert!(
        warning.timestamp.is_some(),
        "the dropped-entries warning must carry a timestamp"
    );
    assert_eq!(
        entries
            .iter()
            .filter(|e| e.message.starts_with("entry "))
            .count(),
        LOG_QUEUE_ENTRIES
    );
}

#[tokio::test]
async fn warn_and_error_helpers_emit_entries_with_their_severity() {
    let (tx, mut rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(tx);

    client.warn("a warning").unwrap();
    client.error("an error").unwrap();
    client.flush().await;

    let mut entries = Vec::new();
    while entries.len() < 2 {
        let batch = within("the warn and error entries", rx.recv())
            .await
            .unwrap();
        entries.extend(batch.log_entries);
    }

    let got: Vec<(i32, &str)> = entries
        .iter()
        .map(|e| (e.severity, e.message.as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            (log_entry::severity::Enum::Warn as i32, "a warning"),
            (log_entry::severity::Enum::Error as i32, "an error"),
        ]
    );
    assert!(entries.iter().all(|e| e.timestamp.is_some()));
}

/// The stream holds one batch and has no reader, so a later full batch blocks the task
/// mid-send; it stays stalled past the batching tick while the flush is pending.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_flush_requested_while_the_batching_task_is_busy_waits_for_its_entries() {
    let (tx, mut rx) = mpsc::channel(1);
    let client = LoggingClient::new(tx);

    let mut expected: Vec<String> = (0..200).map(|i| format!("entry {i}")).collect();
    expected
        .iter()
        .for_each(|m| client.info(m.clone()).unwrap());
    // The paused clock advances only once all tasks are idle, so after this sleep the batching
    // task has filled the stream and is blocked sending another of the 200 entries' batches.
    tokio::time::sleep(Duration::from_millis(1)).await;

    client.info("last").unwrap();
    expected.push("last".to_string());
    let mut flush = tokio::spawn({
        let client = client.clone();
        async move { client.flush().await }
    });

    let mut received: Vec<String> = within("the first batch", rx.recv())
        .await
        .unwrap()
        .log_entries
        .into_iter()
        .map(|e| e.message)
        .collect();
    // The blocked batch now fills the stream, which stays stalled for several ticks.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !flush.is_finished(),
        "flush returned while entries were still queued behind a stalled stream"
    );

    // Read until the flush returns, keeping only what was sent by then.
    within("the flush", async {
        loop {
            tokio::select! {
                result = &mut flush => break result.unwrap(),
                batch = rx.recv() => received.extend(
                    batch.unwrap().log_entries.into_iter().map(|e| e.message),
                ),
            }
        }
    })
    .await;
    while let Ok(batch) = rx.try_recv() {
        received.extend(batch.log_entries.into_iter().map(|e| e.message));
    }

    assert_eq!(
        received, expected,
        "once flush returns, every entry logged before it has been sent, in order"
    );
}

#[tokio::test]
async fn beam_metadata_comes_from_the_event_then_the_innermost_span_declaring_it() {
    type Case = (&'static str, fn(), (&'static str, &'static str));
    let cases: [Case; 5] = [
        (
            "the event wins over the span",
            || {
                let span = tracing::info_span!(
                    "outer",
                    instruction_id = "from-span",
                    transform_id = "from-span"
                );
                let _enter = span.enter();
                tracing::info!(
                    instruction_id = "from-event",
                    transform_id = "from-event",
                    "explicit metadata"
                );
            },
            ("from-event", "from-event"),
        ),
        (
            "the event sets only instruction_id",
            || {
                let span = tracing::info_span!(
                    "bundle",
                    instruction_id = "from-span",
                    transform_id = "from-span"
                );
                let _enter = span.enter();
                tracing::info!(instruction_id = "from-event", "partial");
            },
            ("from-event", "from-span"),
        ),
        (
            "the event sets only transform_id",
            || {
                let span = tracing::info_span!(
                    "bundle",
                    instruction_id = "from-span",
                    transform_id = "from-span"
                );
                let _enter = span.enter();
                tracing::info!(transform_id = "from-event", "partial");
            },
            ("from-span", "from-event"),
        ),
        (
            // The layer walks outwards until both ids are found.
            "each id from a different span",
            || {
                let outer = tracing::info_span!("outer", instruction_id = "instr-1");
                let _outer = outer.enter();
                let inner = tracing::info_span!("inner", transform_id = "transform-1");
                let _inner = inner.enter();
                tracing::info!("nested");
            },
            ("instr-1", "transform-1"),
        ),
        (
            "the innermost span wins",
            || {
                let outer =
                    tracing::info_span!("outer", instruction_id = "outer", transform_id = "outer");
                let _outer = outer.enter();
                let inner =
                    tracing::info_span!("inner", instruction_id = "inner", transform_id = "inner");
                let _inner = inner.enter();
                tracing::info!("nested");
            },
            ("inner", "inner"),
        ),
    ];
    for (case, body, expected) in cases {
        let entries = capture_entries(body).await;
        assert_eq!(entries.len(), 1, "{case}");
        assert_eq!(
            (
                entries[0].instruction_id.as_str(),
                entries[0].transform_id.as_str()
            ),
            expected,
            "{case}"
        );
        // Beam metadata is promoted to dedicated fields, never duplicated into custom data.
        assert!(entries[0].custom_data.is_none(), "{case}");
    }
}
