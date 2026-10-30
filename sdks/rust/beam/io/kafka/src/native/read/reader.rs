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

//! The splittable DoFn that reads the records of one partition into the elements of a decoder.

use std::collections::HashMap;
use std::ffi::CString;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant as WallClock};

use beam::coders::DefaultCoder;
use beam::metrics::Metrics;
use beam::schema::{FieldValue, Row, Schema};
use beam::transforms::sdf::{ProcessContinuation, RestrictionTracker, SplittableDoFn};
use beam::transforms::{Outputs, ProcessContext, WithFinalizer};
use beam::values::IsBounded;
use beam::{Error, Instant};
use rdkafka::bindings::rd_kafka_get_watermark_offsets;
use rdkafka::consumer::Consumer;
use rdkafka::types::RDKafkaRespErr;
use rdkafka::{Offset, TopicPartitionList};
use tracing::{debug, warn};

use super::ConsumerPool;
use super::call::{
    CallEnd, CallLimits, Ended, IdleCheck, Polled, first_wait, idle_check, idle_watermark,
    read_loop,
};
use super::discover::PartitionStart;
use super::fetch::{FetchedRecord, PartitionQueue};
use super::parking::Parking;
use super::restriction::{PartitionBacklogs, PartitionRestriction, PartitionTracker};
use crate::native::client::{Connection, KafkaConsumer, ReadId, consumer_slot, shared_consumer};
use crate::native::codec::{CodecError, Decoder, RecordRef};

/// The longest time that one call reads a partition. The reader then checkpoints, so the
/// runner commits the bundle and the read commits its offsets.
pub(super) const MAX_READ_CALL: Duration = Duration::from_secs(5);

/// The longest time that one call waits for the next record. A partition that has no record
/// for this time ends the call.
pub(super) const MAX_IDLE_PER_CALL: Duration = Duration::from_secs(1);

/// The longest wait for the high watermark of a partition.
const WATERMARKS_TIMEOUT: Duration = Duration::from_secs(5);

/// The namespace of the source counters, the same as Java's `SourceMetrics`.
const SOURCE_METRICS: &str = "source";

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

/// The topic of a read and how it reads. Every copy of the reader shares it.
#[derive(Debug)]
pub(super) struct Source {
    /// Keeps the consumers and parked assignments of this read apart from other reads.
    pub(super) read: ReadId,
    pub(super) connection: Connection,
    pub(super) topic: String,
    /// The schema of the failure output.
    pub(super) errors: Arc<Schema>,
    pub(super) bounded: bool,
    pub(super) commit_expiry: Duration,
    pub(super) max_read_time_per_call: Duration,
    pub(super) max_idle_per_call: Duration,
    /// The fetch wait of the consumer (`fetch.wait.max.ms`). See [`idle_watermark`].
    pub(super) fetch_wait: Duration,
    pub(super) consumer_pool: ConsumerPool,
}

/// What a reader emits for the decoded records of a partition (of type `E`), and how it
/// commits their offsets.
pub(super) trait OutputMode<E>: 'static {
    type Out: Outputs;

    /// The output of a record whose value is `value` and that decoded into `decoded`.
    fn record(
        decoded: Result<E, CodecError>,
        value: Option<&[u8]>,
        source: &Source,
    ) -> beam::Result<Self::Out>;

    /// Commits `commit` when the runner confirms the bundle. Only outputs with a finalizer
    /// commit.
    fn commit(
        _ctx: &ProcessContext<'_, Self::Out>,
        _commit: Commit,
        _expiry: Duration,
    ) -> beam::Result {
        Ok(())
    }
}

/// Emits the elements, and fails the bundle on the first record that does not convert.
pub(super) struct Strict;

impl<E: DefaultCoder> OutputMode<E> for Strict {
    type Out = E;

    fn record(
        decoded: Result<E, CodecError>,
        _value: Option<&[u8]>,
        _source: &Source,
    ) -> beam::Result<E> {
        decoded.map_err(|e| Error::msg(format!("A Kafka record does not convert: {e}")))
    }
}

/// Emits the elements, and sends a record that does not convert to the failure output with
/// its value (empty for a record with no value) and the reason.
pub(super) struct Failures;

impl<E: DefaultCoder> OutputMode<E> for Failures {
    type Out = Result<E, Row>;

    fn record(
        decoded: Result<E, CodecError>,
        value: Option<&[u8]>,
        source: &Source,
    ) -> beam::Result<Result<E, Row>> {
        match decoded {
            Ok(element) => Ok(Ok(element)),
            Err(e) => Row::new(
                Arc::clone(&source.errors),
                vec![
                    Some(FieldValue::Bytes(value.unwrap_or_default().to_vec())),
                    Some(FieldValue::String(e.to_string())),
                ],
            )
            .map(Err)
            .map_err(|e| Error::msg(e.to_string())),
        }
    }
}

/// Emits the outputs of `M` and commits offsets in a bundle finalizer.
pub(super) struct Finalize<M>(PhantomData<M>);

impl<E, M: OutputMode<E>> OutputMode<E> for Finalize<M> {
    type Out = WithFinalizer<M::Out>;

    fn record(
        decoded: Result<E, CodecError>,
        value: Option<&[u8]>,
        source: &Source,
    ) -> beam::Result<Self::Out> {
        M::record(decoded, value, source).map(WithFinalizer)
    }

    fn commit(
        ctx: &ProcessContext<'_, Self::Out>,
        commit: Commit,
        expiry: Duration,
    ) -> beam::Result {
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
/// Each fetch response updates this value, also a response with no record. A request to the
/// broker for the watermark waits behind the fetch request of the consumer on the same
/// connection. When the partition has no new record, the broker holds that fetch request for
/// up to `fetch.wait.max.ms` (500 ms), so a request can wait that long.
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
    queue: PartitionQueue,
    list: TopicPartitionList,
    /// Whether a fetch response came for this assignment: a record or the end of the
    /// partition. Its fetch responses then keep the cached high watermark current.
    fetch_seen: bool,
    /// When an idle call last asked the broker for the high watermark of this assignment.
    broker_request: Option<WallClock>,
    /// The record whose claim failed after a split. The next call emits it first, so the
    /// assignment can park at the start of the residual.
    refused: Option<FetchedRecord>,
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
        let queue = PartitionQueue::new(consumer, topic, partition)
            .ok_or_else(|| failed(&"the topic name is not valid"))?;
        let mut list = TopicPartitionList::new();
        list.add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|e| failed(&e))?;
        consumer.incremental_assign(&list).map_err(|e| failed(&e))?;
        Ok(Self {
            consumer: Arc::clone(consumer),
            queue,
            list,
            fetch_seen: false,
            broker_request: None,
            refused: None,
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
/// the output of `M` for its element of `D`, at the timestamp of the record. Each call reads for at most the max read time per call
/// of the options ([`MAX_READ_CALL`] by default), or until no record came for the max idle
/// time per call ([`MAX_IDLE_PER_CALL`] by default), and then resumes. A call that resumes
/// parks its assignment for the next call. See [`CallEnd`].
pub(super) struct ReadPartitionFn<D, M> {
    source: Arc<Source>,
    decoder: Arc<D>,
    /// The consumers of the pool that this copy used, by slot.
    consumers: HashMap<usize, Arc<KafkaConsumer>>,
    backlogs: Arc<PartitionBacklogs>,
    mode: PhantomData<fn() -> M>,
}

impl<D: Decoder, M: OutputMode<D::Element>> ReadPartitionFn<D, M> {
    pub(super) fn new(source: Arc<Source>, decoder: Arc<D>) -> Self {
        Self {
            source,
            decoder,
            consumers: HashMap::new(),
            backlogs: Arc::default(),
            mode: PhantomData,
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

    /// Whether the partition has no record after the claimed ones, at the end of an idle
    /// call. Asks the broker only as [`idle_check`] says, and remembers when.
    fn caught_up_after_idle(
        &self,
        assignment: &mut Assignment,
        consumer: &KafkaConsumer,
        partition: i32,
        tracker: &PartitionTracker,
    ) -> bool {
        let now = WallClock::now();
        tracker.is_caught_up()
            && match idle_check(assignment.fetch_seen, assignment.broker_request, now) {
                IdleCheck::Cached => true,
                IdleCheck::Skip => false,
                IdleCheck::Broker => {
                    assignment.broker_request = Some(now);
                    self.refresh_high_watermark(consumer, partition, tracker)
                        && tracker.is_caught_up()
                }
            }
    }

    /// Emits the output of `record` of `topic` at its timestamp and adds it to `batch`.
    fn emit_record(
        &self,
        topic: &str,
        record: &FetchedRecord,
        tracker: &PartitionTracker,
        ctx: &mut ProcessContext<'_, M::Out>,
        batch: &mut Batch,
    ) -> beam::Result {
        let record_timestamp = record.timestamp_millis().map(Instant::from_millis);
        let timestamp = record_timestamp.unwrap_or_else(Instant::now);
        let key = record.key();
        let value = record.value();
        let record_bytes = key.map_or(0, <[u8]>::len) + value.map_or(0, <[u8]>::len);
        batch.bytes = batch.bytes.saturating_add(record_bytes);
        batch.records += 1;
        // The headers borrow from the record. librdkafka parses them on request, so only a
        // decoder that reads them gets them. An empty `Vec` allocates nothing.
        let headers = match self.decoder.reads_headers() {
            true => record.headers(),
            false => Ok(Vec::new()),
        };
        let decoded = headers.and_then(|headers| {
            self.decoder.decode(&RecordRef {
                topic,
                partition: record.partition(),
                offset: record.offset(),
                timestamp: record_timestamp,
                key,
                value,
                headers: &headers,
            })
        });
        let output = M::record(decoded, value, &self.source)?;
        ctx.output(output).at(timestamp).emit()?;
        tracker.advance_watermark(timestamp);
        Ok(())
    }

    /// One call: reads `element` until the call ends, and returns why it ended.
    fn read_call(
        &mut self,
        element: PartitionStart,
        tracker: &PartitionTracker,
        ctx: &mut ProcessContext<'_, M::Out>,
    ) -> beam::Result<CallEnd> {
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
        let first = first_wait(lease.value().is_some());
        let assignment = lease
            .get_or_try_insert_with(|| Assignment::new(&consumer, &topic, partition, start))?;
        // At the start, the watermark only sizes the backlog, so a known one is enough.
        self.high_watermark(&consumer, partition, tracker, false);
        // The read time starts here, so a slow assignment does not end a call before its
        // first poll.
        let mut limits = CallLimits::new(
            WallClock::now(),
            self.source.max_read_time_per_call,
            self.source.max_idle_per_call,
            first,
        );
        let mut batch = Batch::default();
        let source = format!("partition {partition} of Kafka topic '{topic}'");
        let Assignment { queue, refused, .. } = &mut *assignment;
        let Ended {
            end,
            refused: last,
            reached_end,
        } = read_loop(
            &source,
            &mut limits,
            tracker,
            stop_at,
            |timeout| match refused.take() {
                Some(record) => Ok(Polled::Record(record)),
                None => queue
                    .poll(timeout)
                    .map_err(|e| Error::msg(format!("Cannot read {source}: {e}"))),
            },
            FetchedRecord::offset,
            |record| self.emit_record(&topic, &record, tracker, ctx, &mut batch),
        )?;
        assignment.refused = last;
        assignment.fetch_seen |= batch.records > 0 || reached_end;
        // The checkpoint sizes the residual with this watermark. After an idle call that
        // read every record, the watermark moves close to the wall clock.
        self.high_watermark(&consumer, partition, tracker, false);
        if end == CallEnd::Idle
            && self.caught_up_after_idle(assignment, &consumer, partition, tracker)
        {
            tracker.advance_watermark(idle_watermark(Instant::now(), self.source.fetch_wait));
        }
        // A call that fails adds no sample. The runner retries its bundle.
        self.backlogs
            .observe_records(partition, batch.bytes, batch.records);
        // The raw bytes of the records (key and value), with the names of Java's
        // `SourceMetrics`, so the source throughput does not depend on the output type.
        Metrics::counter(SOURCE_METRICS, "bytes_read")
            .inc_by(i64::try_from(batch.bytes).unwrap_or(i64::MAX));
        Metrics::counter(SOURCE_METRICS, "elements_read")
            .inc_by(i64::try_from(batch.records).unwrap_or(i64::MAX));
        // A call with a residual parks its assignment at the start of the residual.
        if lease.end_call(end, tracker, WallClock::now()) {
            PARKED.sweep_in_background();
        }
        if let Some(last) = tracker.last_claimed() {
            let commit = Commit {
                consumer,
                topic,
                partition,
                offset: last + 1,
            };
            M::commit(ctx, commit, self.source.commit_expiry)?;
        }
        Ok(end)
    }
}

/// The records and the key and value bytes that one call read.
#[derive(Default)]
struct Batch {
    bytes: usize,
    records: u64,
}

/// Each copy gets the shared consumers when it reads their partitions, and shares the
/// watermarks.
impl<D, M> Clone for ReadPartitionFn<D, M> {
    fn clone(&self) -> Self {
        Self {
            source: Arc::clone(&self.source),
            decoder: Arc::clone(&self.decoder),
            consumers: HashMap::new(),
            backlogs: Arc::clone(&self.backlogs),
            mode: PhantomData,
        }
    }
}

impl<D: Decoder, M: OutputMode<D::Element>> SplittableDoFn for ReadPartitionFn<D, M> {
    type In = PartitionStart;
    type Out = M::Out;
    type Restriction = PartitionRestriction;
    type Tracker = PartitionTracker;

    fn boundedness(&self) -> IsBounded {
        match self.source.bounded {
            true => IsBounded::Bounded,
            false => IsBounded::Unbounded,
        }
    }

    fn initial_restriction(&self, element: &PartitionStart) -> beam::Result<PartitionRestriction> {
        Ok(PartitionRestriction::from_offset(element.offset))
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

    /// Reads until the call ends. See [`CallEnd`].
    fn process_element(
        &mut self,
        element: PartitionStart,
        tracker: &PartitionTracker,
        ctx: &mut ProcessContext<'_, M::Out>,
    ) -> beam::Result<ProcessContinuation> {
        self.read_call(element, tracker, ctx)
            .map(CallEnd::continuation)
    }
}
