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

//! Unit and streaming Prism tests for `GeneratorRestriction`, `GeneratorRestrictionTracker`, and `UnboundedEventSource`.

use beam::coders::{Coder, Context, DefaultCoder};
use beam::prelude::*;
use beam::testing::{TestPipelineError, passert};
use beam::transforms::sdf::{RestrictionError, RestrictionTracker};
use nexmark::config::{BASE_TIME_MS, NexmarkConfig, RateShape, RateUnit};
use nexmark::generator::{Generator, GeneratorConfig, NextEvent};
use nexmark::model::{Event, KnownSize};
use nexmark::models;
use nexmark::source::{GeneratorRestriction, GeneratorRestrictionTracker, UnboundedEventSource};

use crate::{base_streaming_config, stamp, streaming_pipeline};

fn sample_delayed_events() -> Vec<NextEvent> {
    let cfg = GeneratorConfig::from_nexmark_config(&NexmarkConfig {
        num_events: 10,
        num_event_generators: 1,
        ..base_streaming_config()
    });
    Generator::new(cfg)
        .take(3)
        .enumerate()
        .map(|(idx, ne)| {
            let delay = (3 - idx as i64) * 500;
            NextEvent {
                wallclock_timestamp: BASE_TIME_MS + delay,
                ..ne.with_delay(delay)
            }
        })
        .collect()
}

#[test]
fn generator_restriction_coder_round_trips_empty_and_delayed_queues() {
    let empty = GeneratorRestriction::new(0, 250);
    let populated = GeneratorRestriction::with_state(15, 90, BASE_TIME_MS, sample_delayed_events());

    assert_eq!(populated.held_back.len(), 3);
    assert!(
        populated.held_back.windows(2).all(|pair| (
            pair[0].wallclock_timestamp,
            pair[0].event_timestamp
        ) <= (
            pair[1].wallclock_timestamp,
            pair[1].event_timestamp
        )),
        "held_back must stay sorted by (wallclock_timestamp, event_timestamp)"
    );

    let coder = GeneratorRestriction::coder();
    for sample in [&empty, &populated] {
        let mut whole = Vec::new();
        coder
            .encode(sample, &mut whole, Context::WholeStream)
            .expect("whole-stream encode");
        let mut whole_slice = whole.as_slice();
        let decoded = coder
            .decode(&mut whole_slice, Context::WholeStream)
            .expect("whole-stream decode");
        assert!(whole_slice.is_empty());
        assert_eq!(&decoded, sample);
    }

    let mut nested = Vec::new();
    empty.encode_element(&mut nested).expect("nested empty");
    populated
        .encode_element(&mut nested)
        .expect("nested populated");
    let mut reader = nested.as_slice();
    let dec_empty = GeneratorRestriction::decode_element(&mut reader).expect("decode empty");
    let dec_populated =
        GeneratorRestriction::decode_element(&mut reader).expect("decode populated");
    assert!(reader.is_empty());
    assert_eq!(dec_empty, empty);
    assert_eq!(dec_populated, populated);
}

const AVG_EVENT_BYTES: f64 = 100.0;

#[test]
fn restriction_size_and_tracker_progress_are_estimated_bytes() {
    let delayed = sample_delayed_events();
    let delayed_bytes: f64 = delayed
        .iter()
        .map(|next| next.event.estimated_size_bytes() as f64)
        .sum();
    assert!(delayed_bytes > 0.0);

    let plain = GeneratorRestriction::new(10, 20);
    assert_eq!(plain.estimated_bytes(AVG_EVENT_BYTES), 1_000.0);
    let with_delayed = GeneratorRestriction::with_state(10, 20, BASE_TIME_MS, delayed.clone());
    assert_eq!(
        with_delayed.estimated_bytes(AVG_EVENT_BYTES),
        1_000.0 + delayed_bytes
    );

    let tracker = GeneratorRestrictionTracker::new(plain, AVG_EVENT_BYTES);
    let start = tracker.current_progress();
    assert_eq!((start.work_completed, start.work_remaining), (0.0, 1_000.0));

    (10..14).for_each(|pos| assert!(tracker.try_claim(&pos)));
    delayed
        .into_iter()
        .for_each(|next| tracker.defer_delayed(next));
    let claimed = tracker.current_progress();
    assert_eq!(claimed.work_completed, 400.0);
    assert_eq!(claimed.work_remaining, 600.0 + delayed_bytes);
}

#[test]
fn generator_restriction_tracker_splits_and_checkpoints_trailing_delayed_events() {
    let delayed = sample_delayed_events();
    let initial = GeneratorRestriction::new(10, 20);
    let tracker = GeneratorRestrictionTracker::new(initial, AVG_EVENT_BYTES);
    assert_eq!(tracker.wallclock_base_ms(|| BASE_TIME_MS), BASE_TIME_MS);

    assert!(tracker.try_claim(&10));
    assert!(tracker.try_claim(&11));
    tracker.defer_delayed(delayed[0].clone());
    tracker.defer_delayed(delayed[1].clone());

    let (primary, residual_half) = tracker.try_split(0.5).expect("dynamic split at 0.5");
    assert_eq!(primary.start, 10);
    assert_eq!(primary.end, 16);
    assert_eq!(primary.held_back.len(), 2);
    assert_eq!(
        residual_half,
        GeneratorRestriction::with_state(16, 20, BASE_TIME_MS, Vec::new())
    );

    for pos in 12..16 {
        assert!(tracker.try_claim(&pos));
    }
    assert!(!tracker.try_claim(&16));

    assert!(
        matches!(
            tracker.check_done(),
            Err(RestrictionError::IncompleteWork(_))
        ),
        "tracker must not report done while delayed events remain"
    );

    let trailing = tracker
        .try_checkpoint()
        .expect("checkpoint must emit residual when delayed events remain after end offset");
    assert_eq!(trailing.start, 16);
    assert_eq!(trailing.end, 16);
    assert_eq!(trailing.wallclock_base_ms, BASE_TIME_MS);
    assert_eq!(trailing.held_back.len(), 2);

    let resumed = GeneratorRestrictionTracker::new(trailing, AVG_EVENT_BYTES);
    assert!(resumed.has_remaining_work());
    let first = resumed.pop_due_delayed(i64::MAX).expect("first delayed");
    let second = resumed.pop_due_delayed(i64::MAX).expect("second delayed");
    assert!(first.wallclock_timestamp <= second.wallclock_timestamp);
    assert!(!resumed.has_remaining_work());
    assert!(resumed.check_done().is_ok());
    assert!(resumed.try_checkpoint().is_none());

    let err_tracker =
        GeneratorRestrictionTracker::new(GeneratorRestriction::new(5, 10), AVG_EVENT_BYTES);
    assert!(!err_tracker.try_claim(&4));
    assert!(matches!(
        err_tracker.check_done(),
        Err(RestrictionError::OutOfBounds(_))
    ));
}

#[tokio::test]
async fn unbounded_source_matches_model_across_splits_and_checkpoints()
-> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        num_events: 300,
        num_event_generators: 3,
        rate_shape: RateShape::Sine,
        first_event_rate: 60,
        next_event_rate: 140,
        rate_unit: RateUnit::PerSecond,
        rate_period_sec: 20,
        out_of_order_group_size: 5,
        ..base_streaming_config()
    };
    let expected = models::query0(&config);

    let p = streaming_pipeline();
    let stamped = stamp(&p.apply(
        "ReadUnbounded",
        UnboundedEventSource::new(&config).with_batch_size(25),
    ));
    passert::that("UnboundedSourceMatchesModel", &stamped).contains_in_any_order(expected);
    p.run().await?;
    Ok(())
}

#[tokio::test]
async fn unbounded_source_injects_deterministic_late_annotations_and_conserves_events()
-> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        num_events: 200,
        num_event_generators: 2,
        first_event_rate: 50,
        next_event_rate: 50,
        occasional_delay_sec: 3,
        prob_delayed_event: 0.25,
        // Only a paced source holds events back.
        is_rate_limited: true,
        ..base_streaming_config()
    };
    let expected_stripped = models::query0(&config);

    let p1 = streaming_pipeline();
    let raw1 = stamp(&p1.apply(
        "ReadDelayed1",
        UnboundedEventSource::new(&config).with_batch_size(20),
    ));
    let stripped1 = raw1.map("StripLate1", |(ev, ts): (Event, Instant)| {
        (ev.without_annotation("LATE"), ts)
    });
    passert::that("ConservesStrippedEvents", &stripped1).contains_in_any_order(expected_stripped);
    passert::that("HasBothLateAndOnTime", &raw1).satisfies(|actual| {
        let late_count = actual
            .iter()
            .filter(|(ev, _)| ev.has_annotation("LATE"))
            .count();
        if late_count > 0 && late_count < actual.len() {
            Ok(())
        } else {
            Err(format!(
                "expected 0 < late_count < {}, observed {late_count}",
                actual.len()
            )
            .into())
        }
    });
    p1.run().await?;
    Ok(())
}

#[tokio::test]
async fn unbounded_source_paces_rate_limited_stream() -> std::result::Result<(), TestPipelineError>
{
    let config = NexmarkConfig {
        num_events: 20,
        num_event_generators: 2,
        first_event_rate: 400,
        next_event_rate: 400,
        is_rate_limited: true,
        prob_delayed_event: 0.0,
        ..base_streaming_config()
    };
    let expected = models::query0(&config);

    let p = streaming_pipeline();
    let stamped = stamp(&p.apply(
        "ReadRateLimited",
        UnboundedEventSource::new(&config).with_batch_size(5),
    ));
    passert::that("RateLimitedSourceMatchesModel", &stamped).contains_in_any_order(expected);
    p.run().await?;
    Ok(())
}

#[tokio::test]
async fn unpaced_source_holds_no_events_back() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        num_events: 500,
        num_event_generators: 2,
        occasional_delay_sec: 3,
        prob_delayed_event: 0.5,
        ..base_streaming_config()
    };
    let p = streaming_pipeline();
    let stamped = stamp(&p.apply(
        "ReadUnpaced",
        UnboundedEventSource::new(&config).with_batch_size(50),
    ));
    passert::that("UnpacedSourceMatchesModel", &stamped)
        .contains_in_any_order(models::query0(&config));
    p.run().await?;
    Ok(())
}
