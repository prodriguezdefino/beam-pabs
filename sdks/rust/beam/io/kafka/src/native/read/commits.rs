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

//! The offsets that a reader commits for each partition, and the broker confirmations.
//!
//! A bundle finalizer can run after the finalizers of later bundles of the same partition.
//! So the tracker skips an offset when at least that offset is confirmed or pending. If the
//! broker rejects a commit, the tracker clears the pending offset so the next finalizer
//! commits again.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use beam::Error;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::{Offset, TopicPartitionList};
use tracing::warn;

use crate::native::client::KafkaConsumer;

/// The offsets of one partition in [`CommitTracker`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartitionCommit {
    /// The largest offset that the broker confirmed.
    pub confirmed: Option<i64>,
    /// The largest offset that the consumer sent to the broker and that the broker did not
    /// confirm or reject yet.
    pub pending: Option<i64>,
}

impl PartitionCommit {
    /// Whether `offset` is larger than both the confirmed and the pending offset.
    fn should_commit(self, offset: i64) -> bool {
        self.confirmed.is_none_or(|confirmed| offset > confirmed)
            && self.pending.is_none_or(|pending| offset > pending)
    }
}

/// The commits of each partition, by topic. A lookup borrows the topic name.
type TopicCommits = HashMap<String, HashMap<i32, PartitionCommit>>;

/// Runs `change` on the commits of `partition` of `topic`. Copies the topic name only for a
/// topic that has no commit yet.
fn change_partition<R>(
    topics: &mut TopicCommits,
    topic: &str,
    partition: i32,
    change: impl FnOnce(&mut PartitionCommit) -> R,
) -> R {
    let partitions = match topics.get_mut(topic) {
        Some(partitions) => partitions,
        None => topics.entry(topic.to_string()).or_default(),
    };
    change(partitions.entry(partition).or_default())
}

/// The confirmed and pending offset commits of each topic and partition of a consumer.
#[derive(Debug, Default)]
pub struct CommitTracker {
    topics: Mutex<TopicCommits>,
    failures: AtomicU64,
}

impl CommitTracker {
    /// The confirmed and pending offsets of `partition` of `topic`.
    pub fn get(&self, topic: &str, partition: i32) -> PartitionCommit {
        self.lock()
            .get(topic)
            .and_then(|partitions| partitions.get(&partition).copied())
            .unwrap_or_default()
    }

    fn lock(&self) -> MutexGuard<'_, TopicCommits> {
        // Each change sets one offset, so the map stays consistent when a holder panics.
        self.topics.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The number of offset commits that the broker rejected.
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    /// Runs `send` and marks `offset` of `partition` of `topic` as pending if no commit of at
    /// least `offset` is confirmed or pending.
    ///
    /// Holds the lock while `send` queues the commit, so two finalizers of a partition queue
    /// their commits in offset order. Without the lock, a smaller offset can reach the broker
    /// last and move the committed offset back. `send` must only queue the commit and not
    /// wait for the broker. An asynchronous commit of librdkafka puts a request on the queue
    /// of the group thread and returns. Its result goes to the main queue, and the commit
    /// callback runs only when a thread polls that queue. So `send` never calls
    /// [`complete`](Self::complete), which takes the same lock.
    pub fn commit_with(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        send: impl FnOnce() -> beam::Result,
    ) -> beam::Result<bool> {
        change_partition(&mut self.lock(), topic, partition, |state| {
            match state.should_commit(offset) {
                false => Ok(false),
                true => send().map(|()| {
                    state.pending = Some(offset);
                    true
                }),
            }
        })
    }

    /// Applies the broker result of a commit. A confirmation raises the confirmed offset; a
    /// rejection clears the pending offset so a later finalizer commits again.
    pub fn complete(&self, result: &KafkaResult<()>, offsets: &TopicPartitionList) {
        let elements = offsets.elements();
        let failed = result
            .as_ref()
            .err()
            .cloned()
            .or_else(|| elements.iter().find_map(|element| element.error().err()));
        if let Some(error) = failed {
            self.failures.fetch_add(1, Ordering::Relaxed);
            warn!("Cannot commit Kafka offsets {offsets:?}: {error}");
        }
        let mut topics = self.lock();
        elements
            .iter()
            .filter_map(|element| match element.offset() {
                Offset::Offset(offset) => Some((element, offset)),
                Offset::Beginning
                | Offset::End
                | Offset::Stored
                | Offset::Invalid
                | Offset::OffsetTail(_) => None,
            })
            .for_each(|(element, offset)| {
                let confirmed = result.is_ok() && element.error().is_ok();
                change_partition(&mut topics, element.topic(), element.partition(), |state| {
                    if confirmed {
                        state.confirmed = Some(
                            state
                                .confirmed
                                .map_or(offset, |confirmed| confirmed.max(offset)),
                        );
                    }
                    if state.pending.is_some_and(|pending| pending <= offset) {
                        state.pending = None;
                    }
                });
            });
    }

    /// Commits `offset` of `partition` of `topic` on `consumer` if no commit of at least
    /// `offset` is confirmed or pending.
    pub fn commit(
        &self,
        consumer: &KafkaConsumer,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> beam::Result {
        let _sent = self.commit_with(topic, partition, offset, || {
            let mut list = TopicPartitionList::new();
            // Only an asynchronous commit is allowed under the lock of `commit_with`.
            list.add_partition_offset(topic, partition, Offset::Offset(offset))
                .and_then(|()| consumer.commit(&list, CommitMode::Async))
                .map_err(|e| {
                    Error::msg(format!(
                        "Cannot commit offset {offset} of partition {partition} of Kafka topic \
                         '{topic}': {e}"
                    ))
                })
        })?;
        Ok(())
    }
}
