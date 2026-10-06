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

//! Concurrency, order, headers, errors and abort of the `MapAsync` transform.

use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use beam_core::coders::{CausedByDrain, DefaultCoder, ElementMetadata, PaneInfo, WindowedHeader};
use beam_core::internals::{ElementSink, HandlerContext, HandlerInstance, PipelineInternals};
use beam_core::pipeline::{Pipeline, PipelineError};
use beam_core::transforms::{Create, MapAsync};
use beam_core::windowing::{BoundedWindow, IntervalWindow};
use beam_core::{Error, Instant as BeamInstant};

/// Records emitted elements along with their exact [`WindowedHeader`].
#[derive(Default)]
struct RecordingSink {
    items: Vec<(WindowedHeader, Vec<u8>)>,
}

impl ElementSink for RecordingSink {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        self.items.push((WindowedHeader::default(), element));
        Ok(())
    }

    fn push_windowed(&mut self, header: &WindowedHeader, element: Vec<u8>) -> Result<(), String> {
        self.items.push((header.clone(), element));
        Ok(())
    }
}

/// Records the number of received elements at each flush.
#[derive(Default)]
struct FlushRecordingSink {
    items: usize,
    flushed_at: Vec<usize>,
}

impl ElementSink for FlushRecordingSink {
    fn push(&mut self, _element: Vec<u8>) -> Result<(), String> {
        self.items += 1;
        Ok(())
    }

    fn push_windowed(&mut self, _header: &WindowedHeader, _element: Vec<u8>) -> Result<(), String> {
        self.items += 1;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        self.flushed_at.push(self.items);
        Ok(())
    }
}

/// Increments a counter when dropped so tests can check task cancellation.
struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn build_handler<In, Out, F, Fut>(transform: MapAsync<In, Out, F, Fut>) -> HandlerInstance
where
    In: DefaultCoder,
    Out: DefaultCoder,
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = beam_core::Result<Out>> + Send + 'static,
{
    let p = Pipeline::new();
    let out = p
        .apply("Create", Create::new(Vec::<In>::new()))
        .apply("MapAsync", transform);
    p.validate().expect("pipeline validates");
    let id = p
        .producer_transform_id(out.id())
        .expect("MapAsync transform must exist");
    let handler = p
        .transform_handlers()
        .remove(&id)
        .expect("MapAsync handler must be registered");
    handler.instantiate()
}

fn wait_for_count(counter: &AtomicUsize, expected: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while counter.load(Ordering::SeqCst) < expected {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for count {expected}, observed {}",
            counter.load(Ordering::SeqCst)
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn concurrent_execution_runs_faster_than_serial_and_respects_cap() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let active_cb = Arc::clone(&active);
    let peak_cb = Arc::clone(&peak);

    let mut stage = build_handler(
        MapAsync::new(move |value: i64| {
            let active = Arc::clone(&active_cb);
            let peak = Arc::clone(&peak_cb);
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(30)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(value * 10)
            }
        })
        .with_max_in_flight(8),
    );

    let mut sink = RecordingSink::default();
    let header = WindowedHeader::global(1_000, PaneInfo::NO_FIRING);
    stage.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
        for value in 0..16_i64 {
            let encoded = value.encode().expect("encode");
            stage.process(&encoded, &mut ctx).expect("process");
        }
    }
    stage
        .finish_bundle(&mut HandlerContext::new(&mut sink))
        .expect("finish_bundle");

    let outputs: Vec<i64> = sink
        .items
        .iter()
        .map(|(_, bytes)| i64::decode(bytes).expect("decode"))
        .collect();
    assert_eq!(outputs, (0..16_i64).map(|v| v * 10).collect::<Vec<_>>());

    let max_seen = peak.load(Ordering::SeqCst);
    assert!(
        (2..=8).contains(&max_seen),
        "expected peak concurrency in 2..=8, got {max_seen}"
    );
}

#[test]
fn preserves_exact_windowed_header_across_interleaved_completions() {
    // Earlier elements sleep longer than later elements so completions interleave.
    let mut stage = build_handler(
        MapAsync::new(|value: i64| async move {
            let delay_ms = u64::try_from((8 - value).max(0) * 6).unwrap_or(0);
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Ok(format!("item-{value}"))
        })
        .with_max_in_flight(4),
    );

    let inputs: Vec<(i64, WindowedHeader)> = (0..8_i64)
        .map(|i| {
            let ts = 1_000 + i * 100;
            let meta = ElementMetadata {
                drain: if i % 2 == 0 {
                    CausedByDrain::Normal
                } else {
                    CausedByDrain::CausedByDrain
                },
                traceparent: Some(format!("00-trace-{i:02}")),
                ..ElementMetadata::default()
            };
            let w1 = BoundedWindow::Interval(IntervalWindow::new(
                BeamInstant::from_millis(i * 1_000),
                BeamInstant::from_millis((i + 1) * 1_000),
            ));
            let w2 = BoundedWindow::Interval(IntervalWindow::new(
                BeamInstant::from_millis(i * 1_000 + 500),
                BeamInstant::from_millis((i + 1) * 1_000 + 500),
            ));
            let header = WindowedHeader::global_with_metadata(ts, PaneInfo::NO_FIRING, &meta)
                .with_windows(&[w1, w2])
                .expect("encode windows");
            (i, header)
        })
        .collect();

    let mut sink = RecordingSink::default();
    stage.start_bundle().expect("start_bundle");
    for (value, header) in &inputs {
        let encoded = value.encode().expect("encode");
        let mut ctx = HandlerContext::new(&mut sink).with_header(header);
        stage.process(&encoded, &mut ctx).expect("process");
    }
    stage
        .finish_bundle(&mut HandlerContext::new(&mut sink))
        .expect("finish_bundle");

    let decoded: Vec<(WindowedHeader, String)> = sink
        .items
        .into_iter()
        .map(|(header, bytes)| (header, String::decode(&bytes).expect("decode")))
        .collect();
    let expected: Vec<(WindowedHeader, String)> = inputs
        .into_iter()
        .map(|(i, header)| (header, format!("item-{i}")))
        .collect();
    assert_eq!(decoded, expected);
}

#[test]
fn propagates_error_and_aborts_remaining_in_flight_tasks() {
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let started_cb = Arc::clone(&started);
    let dropped_cb = Arc::clone(&dropped);

    let mut stage = build_handler(
        MapAsync::new(move |value: i64| {
            let started = Arc::clone(&started_cb);
            let dropped = Arc::clone(&dropped_cb);
            async move {
                if value == 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    return Err(Error::msg("rpc failure on element 0"));
                }
                let _guard = DropCounter(dropped);
                started.fetch_add(1, Ordering::SeqCst);
                pending::<()>().await;
                Ok(value)
            }
        })
        .with_max_in_flight(8),
    );

    let mut sink = RecordingSink::default();
    let header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
    stage.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
        for value in 0..4_i64 {
            let encoded = value.encode().expect("encode");
            stage.process(&encoded, &mut ctx).expect("process");
        }
    }
    wait_for_count(&started, 3, Duration::from_secs(2));

    let err = stage
        .finish_bundle(&mut HandlerContext::new(&mut sink))
        .unwrap_err();
    assert!(
        err.contains("rpc failure on element 0"),
        "unexpected error: {err}"
    );
    wait_for_count(&dropped, 3, Duration::from_secs(2));
}

#[test]
fn zero_max_in_flight_records_construction_error() {
    let p = Pipeline::new();
    let _ = p.apply("Create", Create::new(vec![1_i64])).apply(
        "AsyncStep",
        MapAsync::new(|x: i64| async move { Ok(x) }).with_max_in_flight(0),
    );
    let err = p.validate().unwrap_err();
    assert!(
        matches!(
            &err,
            PipelineError::InvalidConstruction { transform_id, reason }
                if transform_id == "AsyncStep" && reason.contains("max_in_flight must be >= 1")
        ),
        "unexpected validation error: {err}"
    );
}

/// A full window waits for its oldest task. The output that went out before goes to the
/// runner first.
#[test]
fn flushes_buffered_output_before_waiting_on_a_full_window() {
    let mut stage =
        build_handler(MapAsync::new(|value: i64| async move { Ok(value) }).with_max_in_flight(1));
    let mut sink = FlushRecordingSink::default();
    let header = WindowedHeader::global(1_000, PaneInfo::NO_FIRING);
    stage.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink).with_header(&header);
        for value in 0..3_i64 {
            let encoded = value.encode().expect("encode");
            stage.process(&encoded, &mut ctx).expect("process");
        }
    }
    stage
        .finish_bundle(&mut HandlerContext::new(&mut sink))
        .expect("finish_bundle");
    assert_eq!(sink.flushed_at, vec![0, 1, 2]);
    assert_eq!(sink.items, 3);
}
