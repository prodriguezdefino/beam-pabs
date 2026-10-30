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

//! The confirmed and pending offset commits of each partition, and the broker callback.

use beam::Error;
use kafka_io::native::client::Auth;
use kafka_io::native::context::KafkaContext;
use kafka_io::native::read::commits::{CommitTracker, PartitionCommit};
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::{Offset, TopicPartitionList};

fn offsets(entries: &[(&str, i32, i64)]) -> TopicPartitionList {
    let mut list = TopicPartitionList::new();
    entries.iter().for_each(|(topic, partition, offset)| {
        list.add_partition_offset(topic, *partition, Offset::Offset(*offset))
            .expect("valid partition offset");
    });
    list
}

fn queue(tracker: &CommitTracker, topic: &str, partition: i32, offset: i64) -> bool {
    tracker
        .commit_with(topic, partition, offset, || Ok(()))
        .expect("commit succeeds")
}

#[test]
fn commit_with_skips_offsets_at_or_below_confirmed_or_pending() {
    let tracker = CommitTracker::default();
    assert!(queue(&tracker, "events", 0, 100));
    assert_eq!(
        tracker.get("events", 0),
        PartitionCommit {
            confirmed: None,
            pending: Some(100),
        }
    );
    assert!(!queue(&tracker, "events", 0, 100));
    assert!(!queue(&tracker, "events", 0, 90));
    assert!(queue(&tracker, "events", 0, 150));
    assert_eq!(tracker.get("events", 0).pending, Some(150));
    assert!(queue(&tracker, "events", 1, 50));
    assert!(queue(&tracker, "other", 0, 50));
}

#[test]
fn a_send_error_does_not_mark_the_offset_as_pending() {
    let tracker = CommitTracker::default();
    let result = tracker.commit_with("events", 0, 100, || Err(Error::msg("queue full")));
    assert!(result.is_err());
    assert_eq!(tracker.get("events", 0), PartitionCommit::default());
    assert!(queue(&tracker, "events", 0, 100));
}

#[test]
fn a_confirmation_raises_confirmed_and_clears_pending_up_to_that_offset() {
    let tracker = CommitTracker::default();
    assert!(queue(&tracker, "events", 0, 100));
    tracker.complete(&Ok(()), &offsets(&[("events", 0, 100)]));
    assert_eq!(
        tracker.get("events", 0),
        PartitionCommit {
            confirmed: Some(100),
            pending: None,
        }
    );
    assert!(!queue(&tracker, "events", 0, 100));
    assert!(!queue(&tracker, "events", 0, 80));

    // A later commit is sent before the earlier one is confirmed.
    assert!(queue(&tracker, "events", 0, 200));
    assert!(queue(&tracker, "events", 0, 300));
    tracker.complete(&Ok(()), &offsets(&[("events", 0, 200)]));
    assert_eq!(
        tracker.get("events", 0),
        PartitionCommit {
            confirmed: Some(200),
            pending: Some(300),
        }
    );
    tracker.complete(&Ok(()), &offsets(&[("events", 0, 300)]));
    assert_eq!(
        tracker.get("events", 0),
        PartitionCommit {
            confirmed: Some(300),
            pending: None,
        }
    );
    // An out-of-order confirmation of an earlier offset keeps the larger confirmed offset.
    tracker.complete(&Ok(()), &offsets(&[("events", 0, 250)]));
    assert_eq!(tracker.get("events", 0).confirmed, Some(300));
}

#[test]
fn a_failed_commit_clears_pending_so_the_same_offset_is_retried() {
    let tracker = CommitTracker::default();
    let timeout = Err(KafkaError::ConsumerCommit(
        RDKafkaErrorCode::RequestTimedOut,
    ));
    assert!(queue(&tracker, "events", 0, 100));
    tracker.complete(&timeout, &offsets(&[("events", 0, 100)]));
    assert_eq!(tracker.failures(), 1);
    assert_eq!(
        tracker.get("events", 0),
        PartitionCommit {
            confirmed: None,
            pending: None,
        }
    );
    assert!(queue(&tracker, "events", 0, 100));

    // A failure of an older commit keeps a newer pending commit in flight.
    assert!(queue(&tracker, "events", 0, 200));
    tracker.complete(&timeout, &offsets(&[("events", 0, 100)]));
    assert_eq!(tracker.failures(), 2);
    assert_eq!(tracker.get("events", 0).pending, Some(200));
}

#[test]
fn symbolic_offsets_are_not_confirmed() {
    let tracker = CommitTracker::default();
    let mut list = TopicPartitionList::new();
    for (partition, offset) in [
        (0, Offset::Beginning),
        (1, Offset::End),
        (2, Offset::Stored),
        (3, Offset::Invalid),
        (4, Offset::OffsetTail(5)),
    ] {
        list.add_partition_offset("events", partition, offset)
            .unwrap();
    }
    tracker.complete(&Ok(()), &list);
    for partition in 0..5 {
        assert_eq!(tracker.get("events", partition), PartitionCommit::default());
    }
}

#[test]
fn the_commit_callback_of_the_context_updates_the_tracker_and_counts_failures() {
    let context = KafkaContext::new(Auth::Properties);
    assert!(queue(context.commits(), "events", 2, 40));
    let failed = Err(KafkaError::ConsumerCommit(
        RDKafkaErrorCode::BrokerTransportFailure,
    ));
    context.commit_callback(failed, &offsets(&[("events", 2, 40)]));
    assert_eq!(context.commits().failures(), 1);
    assert!(queue(context.commits(), "events", 2, 40));

    context.commit_callback(Ok(()), &offsets(&[("events", 2, 40)]));
    assert_eq!(
        context.commits().get("events", 2),
        PartitionCommit {
            confirmed: Some(40),
            pending: None,
        }
    );
}
