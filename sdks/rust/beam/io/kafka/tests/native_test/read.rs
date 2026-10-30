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

//! The options, the restriction, the tracker and the expansion of the read. No test reads a
//! record.

use beam::internals::PipelineInternals;
use std::iter;
use std::time::Duration;

use beam::Instant;
use beam::coders::DefaultCoder;
use beam::prelude::*;
use beam::transforms::sdf::RestrictionTracker;
use beam::values::IsBounded;
use kafka_io::native::client::MAX_CONSUMERS;
use kafka_io::native::codec::managed::Payload;
use kafka_io::native::codec::{Utf8, read_error_schema};
use kafka_io::native::read::restriction::{
    PartitionBacklogs, PartitionRestriction, PartitionTracker,
};
use kafka_io::native::read::{
    ConsumerPool, PartitionStart, PoolSize, Read, ReadOptions, StartOffset,
};
use prost::Message;

fn options() -> ReadOptions {
    ReadOptions::new("broker:9092", "events")
}

fn restriction(start: i64, end: i64) -> PartitionRestriction {
    PartitionRestriction {
        start,
        end,
        watermark: Instant::from_millis(1_000),
    }
}

#[test]
fn options_need_servers_a_topic_and_a_max_read_time_above_zero() {
    assert!(options().validate().is_ok());
    assert!(ReadOptions::new("", "events").validate().is_err());
    assert!(ReadOptions::new("broker:9092", " ").validate().is_err());
    assert!(
        options()
            .with_max_read_time(Duration::ZERO)
            .validate()
            .is_err()
    );
}

#[test]
fn a_call_reads_for_5_seconds_unless_the_options_set_another_time_above_zero() {
    assert_eq!(options().max_read_time_per_call, Duration::from_secs(5));
    let short = options().with_max_read_time_per_call(Duration::from_secs(1));
    assert_eq!(short.max_read_time_per_call, Duration::from_secs(1));
    assert!(short.validate().is_ok());
    let error = options()
        .with_max_read_time_per_call(Duration::ZERO)
        .validate()
        .unwrap_err()
        .to_string();
    assert!(error.contains("per call"), "{error}");
}

#[test]
fn a_call_waits_1_second_for_a_record_unless_the_options_set_another_time_above_zero() {
    assert_eq!(options().max_idle_per_call, Duration::from_secs(1));
    let long = options().with_max_idle_per_call(Duration::from_millis(2_500));
    assert_eq!(long.max_idle_per_call, Duration::from_millis(2_500));
    assert!(long.validate().is_ok());
    let error = options()
        .with_max_idle_per_call(Duration::ZERO)
        .validate()
        .unwrap_err()
        .to_string();
    assert!(error.contains("idle time per call"), "{error}");
}

#[test]
fn only_a_split_by_the_runner_marks_the_tracker_as_split() {
    let finished = PartitionTracker::new(restriction(0, i64::MAX), IsBounded::Bounded);
    assert!(finished.try_claim(&3));
    finished.finish();
    assert!(!finished.was_split());
    assert!(!finished.try_claim(&4));

    let checkpointed = PartitionTracker::new(restriction(0, i64::MAX), IsBounded::Unbounded);
    assert!(checkpointed.try_claim(&3));
    assert!(!checkpointed.was_split());
    assert!(checkpointed.try_split(0.0).is_some());
    assert!(checkpointed.was_split());
    assert!(!checkpointed.try_claim(&4));
}

#[test]
fn a_read_has_one_consumer_for_each_partition_up_to_a_limit_unless_the_options_set_a_pool() {
    assert_eq!(options().consumer_pool, ConsumerPool::OnePerPartition);
    assert_eq!(ConsumerPool::OnePerPartition.size(), MAX_CONSUMERS);
    let size = PoolSize::try_from(3).unwrap();
    assert_eq!(size.get(), 3);
    let pool = options().with_consumer_pool(ConsumerPool::Size(size));
    assert_eq!(pool.consumer_pool.size(), 3);
    assert!(pool.validate().is_ok());
    assert_eq!(
        PoolSize::try_from(MAX_CONSUMERS).unwrap().get(),
        MAX_CONSUMERS
    );
    for size in [0, MAX_CONSUMERS + 1, 300] {
        let error = PoolSize::try_from(size).unwrap_err().to_string();
        assert!(error.contains("consumer pool"), "{error}");
    }
}

#[test]
fn start_offsets_parse_the_values_of_auto_offset_reset() {
    let parsed: Vec<StartOffset> = ["earliest", "LATEST", "none"]
        .iter()
        .map(|name| name.parse().unwrap())
        .collect();
    assert_eq!(
        parsed,
        [
            StartOffset::Earliest,
            StartOffset::Latest,
            StartOffset::Committed
        ]
    );
    assert!(parsed.iter().all(|start| {
        start
            .to_string()
            .parse::<StartOffset>()
            .is_ok_and(|again| again == *start)
    }));
    assert!("smallest".parse::<StartOffset>().is_err());
    assert_eq!(StartOffset::default(), StartOffset::Latest);
}

#[test]
fn the_restriction_and_the_partition_round_trip_through_their_coders() {
    let value = restriction(5, i64::MAX);
    let bytes = DefaultCoder::encode(&value).unwrap();
    assert_eq!(PartitionRestriction::decode(&bytes).unwrap(), value);

    [None, Some(Instant::from_millis(42))]
        .into_iter()
        .for_each(|stop_at| {
            let start = PartitionStart {
                topic: "events".to_string(),
                partition: 3,
                offset: 17,
                stop_at,
            };
            let bytes = DefaultCoder::encode(&start).unwrap();
            assert_eq!(PartitionStart::decode(&bytes).unwrap(), start);
        });
}

#[test]
fn the_backlog_ends_at_the_high_watermark_or_the_restriction_end() {
    assert_eq!(restriction(10, i64::MAX).backlog(Some(25)), 15.0);
    assert_eq!(restriction(10, 20).backlog(Some(25)), 10.0);
    assert_eq!(restriction(10, 20).backlog(None), 10.0);
    assert_eq!(restriction(10, i64::MAX).backlog(None), 0.0);
    assert_eq!(restriction(30, i64::MAX).backlog(Some(25)), 0.0);
}

#[test]
fn the_size_is_unknown_until_the_high_watermark_is_known() {
    let sizes = PartitionBacklogs::default();
    // Dataflow ignores a backlog of 1 byte, so an unknown size is not reported.
    assert_eq!(sizes.restriction_size(0, &restriction(10, i64::MAX)), 1.0);
    // A bounded restriction has records left, but no record size yet.
    assert_eq!(sizes.restriction_size(0, &restriction(10, 20)), 1.0);
    sizes.observe_records(0, 500, 5);
    assert_eq!(sizes.restriction_size(0, &restriction(10, 20)), 1_000.0);
}

#[test]
fn the_size_is_the_record_backlog_times_the_average_record_bytes() {
    let sizes = PartitionBacklogs::default();
    sizes.set_high_watermark(3, 1_010);
    sizes.observe_records(3, 3_000, 10);
    assert_eq!(
        sizes.restriction_size(3, &restriction(10, i64::MAX)),
        300_000.0
    );
    // Another partition keeps its own average, and has none yet.
    sizes.set_high_watermark(4, 20);
    assert_eq!(sizes.restriction_size(4, &restriction(10, i64::MAX)), 1.0);
}

#[test]
fn the_average_record_bytes_move_toward_recent_batches() {
    let sizes = PartitionBacklogs::default();
    sizes.set_high_watermark(0, 1_000);
    sizes.observe_records(0, 100 * 1_000, 1_000);
    sizes.observe_records(0, 300 * 1_000, 1_000);
    let avg = sizes.restriction_size(0, &restriction(999, i64::MAX));
    assert!(avg > 100.0 && avg < 300.0, "average {avg}");
}

#[test]
fn a_caught_up_partition_has_a_size_of_zero() {
    let sizes = PartitionBacklogs::default();
    sizes.set_high_watermark(0, 500);
    // Zero, not 1: Dataflow keeps the last backlog when the source reports 1 byte. No record
    // size is needed when no record is left.
    assert_eq!(sizes.restriction_size(0, &restriction(500, i64::MAX)), 0.0);
}

#[test]
fn claims_accept_gaps_and_a_checkpoint_resumes_after_the_last_claim() {
    let tracker = PartitionTracker::new(restriction(10, i64::MAX), IsBounded::Unbounded);
    assert!(tracker.try_claim(&10));
    assert!(tracker.try_claim(&14), "an offset after a gap");
    assert!(!tracker.try_claim(&12), "an earlier offset");
    let tracker = PartitionTracker::new(restriction(10, i64::MAX), IsBounded::Unbounded);
    assert!(tracker.try_claim(&10) && tracker.try_claim(&14));
    tracker.advance_watermark(Instant::from_millis(5_000));
    let residual = tracker.try_checkpoint().expect("work is left");
    assert_eq!(
        residual,
        PartitionRestriction {
            start: 15,
            end: i64::MAX,
            watermark: Instant::from_millis(5_000),
        }
    );
    assert_eq!(tracker.current_restriction().end, 15);
    assert!(
        !tracker.try_claim(&15),
        "the primary ends at the checkpoint"
    );
    assert_eq!(tracker.last_claimed(), Some(14));
}

#[test]
fn only_checkpoints_split_a_partition() {
    let tracker = PartitionTracker::new(restriction(0, 100), IsBounded::Bounded);
    assert!(tracker.try_claim(&0));
    assert_eq!(tracker.try_split(0.5), None);
    assert_eq!(tracker.current_restriction().end, 100);
}

#[test]
fn the_watermark_starts_from_the_restriction_and_never_moves_back() {
    let tracker = PartitionTracker::new(restriction(0, i64::MAX), IsBounded::Unbounded);
    assert_eq!(
        tracker.current_watermark(),
        Some(Instant::from_millis(1_000))
    );
    tracker.advance_watermark(Instant::from_millis(500));
    assert_eq!(
        tracker.current_watermark(),
        Some(Instant::from_millis(1_000))
    );
    tracker.advance_watermark(Instant::from_millis(3_000));
    assert_eq!(
        tracker.current_watermark(),
        Some(Instant::from_millis(3_000))
    );
}

#[test]
fn progress_counts_the_records_left_to_the_high_watermark() {
    let tracker = PartitionTracker::new(restriction(10, i64::MAX), IsBounded::Unbounded);
    assert!(!tracker.is_caught_up(), "no high watermark yet");
    tracker.set_high_watermark(20);
    assert!(tracker.try_claim(&13));
    let progress = tracker.current_progress();
    assert_eq!(
        (progress.work_completed, progress.work_remaining),
        (4.0, 6.0)
    );
    assert!(!tracker.is_caught_up());
    assert!(tracker.try_claim(&19));
    assert!(tracker.is_caught_up());
}

#[test]
fn a_new_partition_has_no_watermark_until_it_gets_one() {
    let tracker = PartitionTracker::new(PartitionRestriction::from_offset(0), IsBounded::Unbounded);
    assert_eq!(tracker.current_watermark(), None);
    tracker.advance_watermark(Instant::from_millis(2_000));
    assert_eq!(
        tracker.current_watermark(),
        Some(Instant::from_millis(2_000))
    );
}

#[test]
fn a_caught_up_partition_has_work_until_its_restriction_ends() {
    let progress = |tracker: &PartitionTracker| {
        let progress = tracker.current_progress();
        (progress.work_completed, progress.work_remaining)
    };
    let untouched = PartitionTracker::new(restriction(10, i64::MAX), IsBounded::Unbounded);
    untouched.set_high_watermark(10);
    assert_eq!(progress(&untouched), (0.0, 1.0));
    assert_eq!(untouched.current_progress().fraction_completed(), 0.0);

    let caught_up = PartitionTracker::new(restriction(10, i64::MAX), IsBounded::Bounded);
    caught_up.set_high_watermark(13);
    assert!(caught_up.try_claim(&12));
    assert_eq!(progress(&caught_up), (3.0, 1.0));
    caught_up.finish();
    assert_eq!(
        progress(&caught_up),
        (3.0, 0.0),
        "the restriction ends at 13"
    );
}

#[test]
fn finish_ends_a_bounded_restriction_after_the_claimed_offsets() {
    let tracker = PartitionTracker::new(restriction(10, i64::MAX), IsBounded::Bounded);
    assert!(tracker.check_done().is_err());
    assert!(tracker.try_claim(&12));
    tracker.finish();
    assert!(tracker.check_done().is_ok());
    assert_eq!(tracker.current_restriction().end, 13);

    let untouched = PartitionTracker::new(restriction(10, i64::MAX), IsBounded::Bounded);
    untouched.finish();
    assert!(
        untouched.check_done().is_ok(),
        "nothing claimed, nothing left"
    );
    assert_eq!(untouched.boundedness(), IsBounded::Bounded);
}

/// Whether the reader of the read named `name` asks the runner to finalize its bundles.
fn reader_requests_finalization(pipeline: &Pipeline, name: &str) -> bool {
    let proto = pipeline.to_proto();
    let reader = format!("{name}/ReadPartitions");
    let transform = proto
        .components
        .as_ref()
        .expect("components")
        .transforms
        .values()
        .find(|t| t.unique_name == reader)
        .unwrap_or_else(|| panic!("no transform named {reader}"));
    let payload = &transform.spec.as_ref().expect("spec").payload;
    model::pipeline::ParDoPayload::decode(payload.as_slice())
        .expect("ParDo payload")
        .requests_finalization
}

#[test]
fn the_read_is_one_composite_with_the_elements_of_its_decoder() {
    let pipeline = Pipeline::new();
    let codec = Payload::string();
    let rows = pipeline
        .try_apply(
            "ReadEvents",
            Read::new(options()).with_decoder(codec.clone()),
        )
        .unwrap();
    assert_eq!(rows.row_schema().as_ref(), Some(codec.row_schema()));
    {
        let lock = pipeline.lock();
        let composite = lock
            .components
            .transforms
            .values()
            .find(|t| t.unique_name == "ReadEvents")
            .expect("the composite of the read");
        assert_eq!(composite.outputs.get("out"), Some(&rows.id().to_string()));
        assert!(!composite.subtransforms.is_empty());
    }
    assert!(!reader_requests_finalization(&pipeline, "ReadEvents"));
}

/// The names of the direct steps of the read named `name`, in the order of their data, without
/// the prefix of the read.
fn read_steps(pipeline: &Pipeline, name: &str) -> Vec<String> {
    let lock = pipeline.lock();
    let transforms = &lock.components.transforms;
    let composite = transforms
        .values()
        .find(|t| t.unique_name == name)
        .expect("the composite of the read");
    let steps: Vec<_> = composite
        .subtransforms
        .iter()
        .map(|id| &transforms[id])
        .collect();
    let first = steps
        .iter()
        .find(|step| step.inputs.is_empty())
        .expect("a step with no input");
    iter::successors(Some(*first), |step| {
        steps
            .iter()
            .find(|next| {
                next.inputs
                    .values()
                    .any(|input| step.outputs.values().any(|o| o == input))
            })
            .copied()
    })
    .take(steps.len())
    .map(|step| {
        step.unique_name
            .trim_start_matches(&format!("{name}/"))
            .to_string()
    })
    .collect()
}

#[test]
fn the_reader_reads_the_discovered_partitions_with_no_step_between_them() {
    let pipeline = Pipeline::new();
    pipeline
        .try_apply("ReadEvents", Read::new(options()))
        .unwrap();
    assert_eq!(
        read_steps(&pipeline, "ReadEvents"),
        ["Impulse", "DiscoverPartitions", "ReadPartitions"]
    );
}

#[test]
fn a_bounded_read_can_reshuffle_its_partitions_before_the_reader() {
    let pipeline = Pipeline::new();
    let bounded = options()
        .with_max_read_time(Duration::from_secs(60))
        .with_reshuffle_partitions(true);
    pipeline
        .try_apply("ReadEvents", Read::new(bounded).with_failures())
        .unwrap();
    assert_eq!(
        read_steps(&pipeline, "ReadEvents"),
        [
            "Impulse",
            "DiscoverPartitions",
            "Reshuffle",
            "ReadPartitions"
        ]
    );
}

#[test]
fn a_read_with_no_end_rejects_the_reshuffle_of_its_partitions() {
    assert!(!options().reshuffle_partitions);
    let unbounded = options().with_reshuffle_partitions(true);
    let error = unbounded.validate().unwrap_err().to_string();
    assert!(error.contains("max read time"), "{error}");
    let error = Pipeline::new()
        .try_apply("ReadEvents", Read::new(unbounded))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("ReadEvents") && error.contains("reshuffle"),
        "{error}"
    );
}

#[test]
fn two_reads_of_one_topic_expand_side_by_side() {
    let pipeline = Pipeline::new();
    let first = pipeline
        .try_apply("ReadEvents", Read::new(options()))
        .unwrap();
    let second = pipeline
        .try_apply("ReadEvents", Read::new(options()))
        .unwrap();
    assert_ne!(first.id(), second.id());
    ["ReadEvents", "ReadEvents_2"].iter().for_each(|name| {
        assert_eq!(
            read_steps(&pipeline, name),
            ["Impulse", "DiscoverPartitions", "ReadPartitions"],
            "{name}"
        );
    });
}

#[test]
fn a_consumer_group_makes_the_reader_finalize_its_bundles() {
    let pipeline = Pipeline::new();
    let grouped = options().with_consumer_properties([("group.id".to_string(), "g".to_string())]);
    let failures = pipeline
        .try_apply("ReadEvents", Read::new(grouped).with_failures())
        .unwrap()
        .failures;
    assert_eq!(failures.row_schema().as_deref(), Some(&read_error_schema()));
    assert!(reader_requests_finalization(&pipeline, "ReadEvents"));
}

#[test]
fn invalid_options_fail_the_expansion_with_the_transform_name() {
    let pipeline = Pipeline::new();
    let error = pipeline
        .try_apply("ReadEvents", Read::new(ReadOptions::new("broker:9092", "")))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("ReadEvents") && error.contains("topic"),
        "{error}"
    );
}

/// The coder URN of the PCollection `id`.
fn coder_urn(pipeline: &Pipeline, id: &str) -> String {
    let proto = pipeline.to_proto();
    let components = proto.components.as_ref().expect("components");
    let coder_id = &components.pcollections[id].coder_id;
    components.coders[coder_id]
        .spec
        .as_ref()
        .expect("coder spec")
        .urn
        .clone()
}

#[test]
fn a_read_gives_the_record_values_with_the_bytes_coder_by_default() {
    let pipeline = Pipeline::new();
    let values = pipeline
        .try_apply(
            "ReadValues",
            Read::new(ReadOptions::new("broker:9092", "events")),
        )
        .unwrap();
    assert_eq!(coder_urn(&pipeline, values.id()), "beam:coder:bytes:v1");
    {
        let lock = pipeline.lock();
        let composite = lock
            .components
            .transforms
            .values()
            .find(|t| t.unique_name == "ReadValues")
            .expect("the composite of the read");
        assert_eq!(composite.outputs.get("out"), Some(&values.id().to_string()));
    }
    assert_eq!(
        read_steps(&pipeline, "ReadValues"),
        ["Impulse", "DiscoverPartitions", "ReadPartitions"]
    );
    assert!(!reader_requests_finalization(&pipeline, "ReadValues"));
}

#[test]
fn a_read_of_values_with_a_consumer_group_finalizes_and_has_a_failures_output() {
    let pipeline = Pipeline::new();
    let grouped = ReadOptions::new("broker:9092", "events")
        .with_consumer_properties([("group.id".to_string(), "g".to_string())]);
    let read = pipeline
        .try_apply("ReadValues", Read::new(grouped).with_failures())
        .unwrap();
    assert_eq!(
        coder_urn(&pipeline, read.output.id()),
        "beam:coder:bytes:v1"
    );
    assert_eq!(
        read.failures.row_schema().as_deref(),
        Some(&read_error_schema())
    );
    assert!(reader_requests_finalization(&pipeline, "ReadValues"));
}

#[test]
fn the_decoder_sets_the_elements_and_their_coder() {
    let pipeline = Pipeline::new();
    let lines = pipeline
        .try_apply("ReadLines", Read::new(options()).with_decoder(Utf8))
        .unwrap();
    assert_eq!(
        coder_urn(&pipeline, lines.id()),
        "beam:coder:string_utf8:v1"
    );
}
