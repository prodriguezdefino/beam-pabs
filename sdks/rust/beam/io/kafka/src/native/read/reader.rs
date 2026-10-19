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

//! The splittable DoFn that reads the records of one partition into rows.

use std::collections::HashMap;
use std::ffi::CString;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant as WallClock};

use beam::schema::{FieldValue, Row, RowElement, Schema};
use beam::transforms::sdf::{ProcessContinuation, RestrictionTracker, SplittableDoFn};
use beam::transforms::{Outputs, ProcessContext, WithFinalizer};
use beam::values::IsBounded;
use beam::{Error, Instant};
use rdkafka::bindings::rd_kafka_get_watermark_offsets;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::types::RDKafkaRespErr;
use rdkafka::{Offset, TopicPartitionList};
use tracing::{debug, warn};

use super::ConsumerPool;
use super::discover::PartitionStart;
use super::parking::Parking;
use super::restriction::{PartitionBacklogs, PartitionRestriction, PartitionTracker};
use crate::native::auth::KafkaContext;
use crate::native::client::{Connection, KafkaConsumer, ReadId, consumer_slot, shared_consumer};
use crate::native::format::ReadFormat;

/// The longest wait for the first record after the reader assigns a partition. The consumer
/// starts to fetch the partition at the assignment.
const FIRST_POLL_TIMEOUT: Duration = Duration::from_secs(2);

/// The longest wait for the next record after the first one. A partition that has no record
/// for this time ends the call.
const POLL_TIMEOUT: Duration = Duration::from_millis(100);

/// The longest time that one call reads a partition. The reader then checkpoints, so the
/// runner commits the bundle and the read commits its offsets.
pub(super) const MAX_READ_CALL: Duration = Duration::from_secs(5);

/// The longest wait for the high watermark of a partition.
const WATERMARKS_TIMEOUT: Duration = Duration::from_secs(5);

/// The longest time that an assignment waits for the next call of its partition. The runner
/// starts the next call soon after a checkpoint, unless the partition moved to another
/// worker. Then a background thread unassigns the partition.
const PARKING_EXPIRY: Duration = Duration::from_secs(15);

/// The parked assignments of this process. See [`Parking`].
static PARKED: LazyLock<Parking<PartitionKey, Assignment>> =
    LazyLock::new(|| Parking::new(PARKING_EXPIRY));

/// A partition of a read, through the consumer in `slot` of the pool of the read.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct PartitionKey {
    read: ReadId,
    slot: usize,
    partition: i32,
}

/// The topic, the format and the rows of a read. Every copy of the reader shares it.
#[derive(Debug)]
pub(super) struct Source {
    /// Keeps the consumers and parked assignments of this read apart from other reads.
    pub(super) read: ReadId,
    pub(super) connection: Connection,
    pub(super) topic: String,
    pub(super) format: ReadFormat,
    pub(super) schema: Arc<Schema>,
    pub(super) errors: Arc<Schema>,
    pub(super) bounded: bool,
    pub(super) commit_expiry: Duration,
    pub(super) max_read_time_per_call: Duration,
    pub(super) consumer_pool: ConsumerPool,
}

/// The elements that a reader emits for the records of a partition, and how it commits
/// their offsets.
pub(super) trait ReadOutput: Outputs + Sized {
    /// The output of a record whose value gave `row`, or the reason why it did not.
    /// `payload` is the record value.
    fn record(row: Result<Row, String>, payload: &[u8], errors: &Arc<Schema>)
    -> beam::Result<Self>;

    /// Commits `commit` when the runner confirms the bundle. Only outputs with a finalizer
    /// commit.
    fn commit(_ctx: &ProcessContext<'_, Self>, _commit: Commit, _expiry: Duration) -> beam::Result {
        Ok(())
    }
}

/// Fails the bundle on the first record that does not convert.
impl ReadOutput for RowElement {
    fn record(
        row: Result<Row, String>,
        _payload: &[u8],
        _errors: &Arc<Schema>,
    ) -> beam::Result<Self> {
        row.map(RowElement::Row)
            .map_err(|message| Error::msg(format!("A Kafka record does not convert: {message}")))
    }
}

/// Sends a record that does not convert to the error output, with its value and the reason.
impl ReadOutput for Result<RowElement, Row> {
    fn record(
        row: Result<Row, String>,
        payload: &[u8],
        errors: &Arc<Schema>,
    ) -> beam::Result<Self> {
        match row {
            Ok(row) => Ok(Ok(RowElement::Row(row))),
            Err(message) => Row::new(
                Arc::clone(errors),
                vec![
                    Some(FieldValue::Bytes(payload.to_vec())),
                    Some(FieldValue::String(message)),
                ],
            )
            .map(Err)
            .map_err(|e| Error::msg(e.to_string())),
        }
    }
}

/// Emits the outputs of `O` and commits offsets in a bundle finalizer.
impl<O: ReadOutput> ReadOutput for WithFinalizer<O> {
    fn record(
        row: Result<Row, String>,
        payload: &[u8],
        errors: &Arc<Schema>,
    ) -> beam::Result<Self> {
        O::record(row, payload, errors).map(WithFinalizer)
    }

    fn commit(ctx: &ProcessContext<'_, Self>, commit: Commit, expiry: Duration) -> beam::Result {
        ctx.finalizer(move || commit.run())
            .expire_after(expiry)
            .register()
    }
}

/// The offset after the last record of a bundle, to commit for the consumer group.
pub(super) struct Commit {
    consumer: Arc<KafkaConsumer>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl Commit {
    fn run(self) -> beam::Result {
        self.consumer.context().commits().commit(
            &self.consumer,
            &self.topic,
            self.partition,
            self.offset,
        )
    }
}

/// The high watermark of `partition` that librdkafka knows from its last fetch response, or
/// `None` before the first response. Asks nothing from the broker.
///
/// A request to the broker for the watermark waits behind the fetch request of the consumer
/// on the same connection. When the partition has no new record, the broker holds that fetch
/// request for up to `fetch.wait.max.ms` (500 ms), so a request can wait that long.
fn cached_high_watermark(consumer: &KafkaConsumer, topic: &str, partition: i32) -> Option<i64> {
    let topic = CString::new(topic).ok()?;
    let (mut low, mut high) = (-1_i64, -1_i64);
    // SAFETY: rdkafka 0.39 has no safe call for the cached offsets; `fetch_watermarks` asks
    // the broker. `native_ptr` is the `rd_kafka_t` of `consumer`, which lives until the call
    // returns. librdkafka allows this call from any thread. `topic` is a NUL-terminated
    // string that lives until the call returns, and librdkafka keeps no pointer to it. `low`
    // and `high` are local `i64` values, the Rust type of `int64_t`, and valid for writes.
    // librdkafka takes a reference to the partition, reads the two offsets under the lock of
    // the partition, and releases the reference. For an unknown partition it writes nothing
    // and returns an error.
    let error = unsafe {
        rd_kafka_get_watermark_offsets(
            consumer.client().native_ptr(),
            topic.as_ptr(),
            partition,
            &mut low,
            &mut high,
        )
    };
    (error == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR && high >= 0).then_some(high)
}

/// One partition assigned to the consumer, with its own queue. Dropping it unassigns the
/// partition, also when the read fails, so the next reader can assign it again.
struct Assignment {
    consumer: Arc<KafkaConsumer>,
    queue: PartitionQueue<KafkaContext>,
    list: TopicPartitionList,
}

impl Assignment {
    /// Assigns `partition` of `topic` from `offset`. The queue exists before the assignment,
    /// so no record of the partition goes to the queue of the consumer.
    fn new(
        consumer: &Arc<KafkaConsumer>,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> beam::Result<Self> {
        let failed = |reason: &dyn Display| {
            Error::msg(format!(
                "Cannot assign partition {partition} of Kafka topic '{topic}': {reason}"
            ))
        };
        let queue = consumer
            .split_partition_queue(topic, partition)
            .ok_or_else(|| failed(&"the topic name is not valid"))?;
        let mut list = TopicPartitionList::new();
        list.add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|e| failed(&e))?;
        consumer.incremental_assign(&list).map_err(|e| failed(&e))?;
        Ok(Self {
            consumer: Arc::clone(consumer),
            queue,
            list,
        })
    }
}

impl Drop for Assignment {
    fn drop(&mut self) {
        if let Err(e) = self.consumer.incremental_unassign(&self.list) {
            warn!("Cannot unassign Kafka partitions {:?}: {e}", self.list);
        }
    }
}

/// Reads one partition from the start offset of its restriction, and emits each record as
/// `O` at the timestamp of the record. Each call reads for at most the max read time per call
/// of the options ([`MAX_READ_CALL`] by default), or until the partition has no new record,
/// and then resumes. A call that resumes parks its assignment for the next call.
pub(super) struct ReadPartitionFn<O> {
    source: Arc<Source>,
    /// The consumers of the pool that this copy used, by slot.
    consumers: HashMap<usize, Arc<KafkaConsumer>>,
    backlogs: Arc<PartitionBacklogs>,
    output: PhantomData<fn() -> O>,
}

impl<O> ReadPartitionFn<O> {
    pub(super) fn new(source: Arc<Source>) -> Self {
        Self {
            source,
            consumers: HashMap::new(),
            backlogs: Arc::default(),
            output: PhantomData,
        }
    }

    /// The slot of the consumer of `partition`, and the consumer.
    fn consumer(&mut self, partition: i32) -> beam::Result<(usize, Arc<KafkaConsumer>)> {
        let Source {
            read,
            connection,
            consumer_pool,
            ..
        } = self.source.as_ref();
        let slot = consumer_slot(*read, partition, consumer_pool.size());
        match self.consumers.get(&slot) {
            Some(consumer) => Ok((slot, Arc::clone(consumer))),
            None => {
                let consumer = shared_consumer(connection, *read, slot)?;
                self.consumers.insert(slot, Arc::clone(&consumer));
                Ok((slot, consumer))
            }
        }
    }

    /// Gives `tracker` the high watermark of `partition` that librdkafka knows from its
    /// fetch responses. Before the first response, asks the broker only if `ask` is set or
    /// no watermark of the partition is known.
    fn high_watermark(
        &self,
        consumer: &KafkaConsumer,
        partition: i32,
        tracker: &PartitionTracker,
        ask: bool,
    ) {
        match (
            cached_high_watermark(consumer, &self.source.topic, partition),
            self.backlogs.high_watermark(partition),
        ) {
            (Some(high), _) => {
                tracker.set_high_watermark(high);
                self.backlogs.set_high_watermark(partition, high);
            }
            (None, Some(high)) if !ask => tracker.set_high_watermark(high),
            (None, _) => {
                let _answered = self.refresh_high_watermark(consumer, partition, tracker);
            }
        }
    }

    /// Asks the broker for the high watermark of `partition`, and returns whether it
    /// answered. A failed request keeps the last known value.
    fn refresh_high_watermark(
        &self,
        consumer: &KafkaConsumer,
        partition: i32,
        tracker: &PartitionTracker,
    ) -> bool {
        match consumer.fetch_watermarks(&self.source.topic, partition, WATERMARKS_TIMEOUT) {
            Ok((_, high)) => {
                tracker.set_high_watermark(high);
                self.backlogs.set_high_watermark(partition, high);
                true
            }
            Err(e) => {
                debug!(
                    "Cannot read the high watermark of partition {partition} of Kafka topic '{}': {e}",
                    self.source.topic
                );
                false
            }
        }
    }
}

/// Each copy gets the shared consumers when it reads their partitions, and shares the
/// watermarks.
impl<O> Clone for ReadPartitionFn<O> {
    fn clone(&self) -> Self {
        Self {
            source: Arc::clone(&self.source),
            consumers: HashMap::new(),
            backlogs: Arc::clone(&self.backlogs),
            output: PhantomData,
        }
    }
}

impl<O: ReadOutput> SplittableDoFn for ReadPartitionFn<O> {
    type In = PartitionStart;
    type Out = O;
    type Restriction = PartitionRestriction;
    type Tracker = PartitionTracker;

    fn boundedness(&self) -> IsBounded {
        match self.source.bounded {
            true => IsBounded::Bounded,
            false => IsBounded::Unbounded,
        }
    }

    fn initial_restriction(&self, element: &PartitionStart) -> PartitionRestriction {
        PartitionRestriction::from_offset(element.offset)
    }

    /// Estimates the remaining backlog in bytes up to the last known high watermark, so
    /// runners and autoscalers see the lag.
    fn restriction_size(
        &self,
        element: &PartitionStart,
        restriction: &PartitionRestriction,
    ) -> f64 {
        self.backlogs
            .restriction_size(element.partition, restriction)
    }

    fn create_tracker(&self, restriction: &PartitionRestriction) -> PartitionTracker {
        PartitionTracker::new(*restriction, self.boundedness())
    }

    fn process_element(
        &mut self,
        element: PartitionStart,
        tracker: &PartitionTracker,
        ctx: &mut ProcessContext<'_, O>,
    ) -> beam::Result<ProcessContinuation> {
        let PartitionStart {
            topic,
            partition,
            stop_at,
            ..
        } = element;
        let (slot, consumer) = self.consumer(partition)?;
        let start = tracker.current_restriction().start;
        let key = PartitionKey {
            read: self.source.read,
            slot,
            partition,
        };
        // The lease holds the partition until this call parks or drops its assignment, so
        // no other call assigns it before then.
        let mut lease = PARKED.take(key, start, WallClock::now()).map_err(|e| {
            Error::msg(format!(
                "Cannot assign partition {partition} of Kafka topic '{topic}': {e}"
            ))
        })?;
        // A parked assignment fetched during the wait, so its first poll needs no long wait.
        let mut timeout = match lease.value() {
            Some(_) => POLL_TIMEOUT,
            None => FIRST_POLL_TIMEOUT,
        };
        let assignment = lease
            .get_or_try_insert_with(|| Assignment::new(&consumer, &topic, partition, start))?;
        // At the start, the watermark only sizes the backlog, so a known one is enough.
        self.high_watermark(&consumer, partition, tracker, false);
        // The read time starts here, so a slow assignment does not end a call before its
        // first poll.
        let started = WallClock::now();
        let mut batch_bytes = 0usize;
        let mut batch_records = 0u64;
        let continuation = loop {
            if stop_at.is_some_and(|stop_at| Instant::now() >= stop_at) {
                tracker.finish();
                break ProcessContinuation::stop();
            }
            if started.elapsed() >= self.source.max_read_time_per_call {
                // The checkpoint sizes the residual with this watermark.
                self.high_watermark(&consumer, partition, tracker, false);
                break ProcessContinuation::resume();
            }
            let message = match assignment.queue.poll(timeout) {
                None => {
                    self.high_watermark(&consumer, partition, tracker, false);
                    // A poll with no record does not prove that a fetch response came, so
                    // the cached watermark can be older than this assignment. Only the
                    // broker proves that no record is left, before the watermark moves to
                    // the wall clock.
                    if tracker.is_caught_up()
                        && self.refresh_high_watermark(&consumer, partition, tracker)
                        && tracker.is_caught_up()
                    {
                        tracker.advance_watermark(Instant::now());
                    }
                    break ProcessContinuation::resume();
                }
                Some(Err(KafkaError::PartitionEOF(_))) => continue,
                Some(Err(e)) => {
                    return Err(Error::msg(format!(
                        "Cannot read partition {partition} of Kafka topic '{topic}': {e}"
                    )));
                }
                Some(Ok(message)) => message,
            };
            if !tracker.try_claim(&message.offset()) {
                break ProcessContinuation::stop();
            }
            let timestamp = message
                .timestamp()
                .to_millis()
                .map_or_else(Instant::now, Instant::from_millis);
            let payload = message.payload();
            let record_bytes =
                message.key().map_or(0, <[u8]>::len) + payload.map_or(0, <[u8]>::len);
            batch_bytes = batch_bytes.saturating_add(record_bytes);
            batch_records += 1;
            let row = match payload {
                Some(payload) => self.source.format.decode(&self.source.schema, payload),
                None => Err("The record has no value".to_string()),
            };
            let output = O::record(row, payload.unwrap_or_default(), &self.source.errors)?;
            ctx.output(output).at(timestamp).emit()?;
            tracker.advance_watermark(timestamp);
            timeout = POLL_TIMEOUT;
        };
        // A call that fails adds no sample. The runner retries its bundle.
        self.backlogs
            .observe_records(partition, batch_bytes, batch_records);
        // Only a call that resumes has a next call. Every record that the call took from the
        // queue is a claim attempt, so the next record of the queue is at the residual start.
        if lease.end_call(tracker, &continuation, WallClock::now()) {
            PARKED.sweep_in_background();
        }
        if let Some(last) = tracker.last_claimed() {
            let commit = Commit {
                consumer,
                topic,
                partition,
                offset: last + 1,
            };
            O::commit(ctx, commit, self.source.commit_expiry)?;
        }
        Ok(continuation)
    }
}
