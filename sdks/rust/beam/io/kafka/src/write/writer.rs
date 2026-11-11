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

//! The DoFn that sends the elements of each bundle to a topic, as the records of an encoder.

use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use beam::metrics::{Lineage, Metrics};
use beam::schema::{FieldValue, Row, Schema};
use beam::transforms::{BundleWindows, DoFn, FinishBundleContext, ProcessContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use super::delivery::{Deliveries, Delivery};
use super::produce::TopicProducer;
use super::{GLOBAL, emit_global};
use crate::METRICS;
use crate::client::{Connection, shared_producer};
use crate::codec::{CodecError, Encoder};

/// The most bytes that one DoFn copy keeps for the records that Kafka did not confirm: the
/// records, the elements for the failure output and the state of each record. With more, the
/// DoFn waits for its oldest records.
///
/// The DoFn sends at most this number of bytes in one acknowledgement time of the cluster.
/// A limit of one producer batch makes the DoFn wait for each batch, so a slow cluster limits
/// its throughput. The queue limits of a producer (`queue.buffering.max.messages`,
/// `queue.buffering.max.kbytes`) apply to all the copies that share it.
const MAX_IN_FLIGHT_BYTES: usize = 32 << 20;

/// The wait before the DoFn sends a record again after the producer queue was full, when
/// the DoFn has no record of its own to wait for.
const QUEUE_FULL_WAIT: Duration = Duration::from_millis(10);

/// Confirmed record and byte counts accumulated across one bundle.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Written {
    pub elements: u64,
    pub bytes: usize,
}

impl Written {
    /// Adds one confirmed record of `record_len` key and value bytes.
    pub fn observe(&mut self, record_len: usize) {
        self.elements += 1;
        self.bytes = self.bytes.saturating_add(record_len);
    }

    /// Reports sink lineage and the confirmed write counters of the bundle.
    pub fn record(self, bootstrap_servers: &str, topic: &str) {
        Lineage::sinks().add(METRICS, [bootstrap_servers, topic]);
        Metrics::counter(METRICS, "elements_written")
            .inc_by(i64::try_from(self.elements).unwrap_or(i64::MAX));
        Metrics::counter(METRICS, "bytes_written")
            .inc_by(i64::try_from(self.bytes).unwrap_or(i64::MAX));
    }
}

/// The topic of a write. Every copy of the DoFn shares it.
pub(super) struct Target {
    pub(super) connection: Connection,
    pub(super) topic: String,
    /// The schema of the failure output.
    pub(super) errors: Arc<Schema>,
}

/// A record that the DoFn sent, with its element for the case that Kafka rejects it.
struct InFlight<T> {
    element: T,
    /// Key and value bytes of this record.
    record_len: usize,
    /// The bytes that this record adds to the limit [`MAX_IN_FLIGHT_BYTES`].
    bytes: usize,
}

/// The records that one DoFn copy sent and that Kafka did not confirm yet, oldest first.
/// The record at position `i` has the entry `first + i` in `deliveries`.
struct Pending<T> {
    deliveries: Deliveries,
    records: VecDeque<InFlight<T>>,
    first: usize,
    bytes: usize,
}

impl<T> Default for Pending<T> {
    fn default() -> Self {
        Self {
            deliveries: Deliveries::default(),
            records: VecDeque::new(),
            first: 0,
            bytes: 0,
        }
    }
}

impl<T> Pending<T> {
    /// The entry of the next record.
    fn next_entry(&self) -> usize {
        self.first.wrapping_add(self.records.len())
    }

    /// Whether the next record must wait for the oldest one: all entries are in use.
    fn is_full(&self) -> bool {
        self.records.len() >= Deliveries::CAPACITY
    }

    fn push(&mut self, record: InFlight<T>) {
        self.bytes += record.bytes;
        self.records.push_back(record);
    }

    /// The oldest record and its result, if Kafka answered for it. With `wait`, waits for
    /// the answer.
    fn pop_answered(&mut self, wait: bool) -> Option<(T, usize, Delivery)> {
        let result = if wait {
            self.deliveries.wait_for_result(self.first)
        } else {
            self.deliveries.result(self.first)
        };
        match (result, self.records.front()) {
            (Delivery::Pending, _) | (_, None) => None,
            (result, Some(_)) => {
                let InFlight {
                    element,
                    record_len,
                    bytes,
                } = self.records.pop_front()?;
                self.bytes -= bytes;
                self.first = self.first.wrapping_add(1);
                Some((element, record_len, result))
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Drops the records with no answer. Their late answers go to the old `Deliveries`, so
    /// they do not change the entries of new records.
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Sends each element of a bundle as the record of the encoder `E`. `finish_bundle` waits
/// until Kafka has every record, so a bundle succeeds only when its records are written.
/// Emits the elements that do not convert and the elements of records that Kafka rejects,
/// with the reason. Any other failure fails the bundle, and the runner retries it, so the
/// write is at least once.
pub(super) struct WriteFn<E: Encoder> {
    target: Arc<Target>,
    encoder: Arc<E>,
    /// The encoder state of this copy.
    state: E::State,
    producer: Option<TopicProducer>,
    /// The buffer of the encoder. The producer copies each record, so each element reuses it.
    buffer: Vec<u8>,
    pending: Pending<E::Element>,
    written: Written,
    windows: BundleWindows,
}

impl<E: Encoder> WriteFn<E> {
    pub(super) fn new(target: Arc<Target>, encoder: Arc<E>) -> Self {
        Self {
            target,
            encoder,
            state: E::State::default(),
            producer: None,
            buffer: Vec::new(),
            pending: Pending::default(),
            written: Written::default(),
            windows: BundleWindows::new(),
        }
    }

    /// Encodes `element` and sends its record. An element that does not convert, or whose
    /// record the producer rejects at once, goes to `rejected`.
    fn send(&mut self, element: E::Element, rejected: &mut Vec<Row>) -> beam::Result {
        let Self {
            target,
            encoder,
            state,
            producer,
            buffer,
            pending,
            written,
            ..
        } = self;
        let producer = topic_producer(producer, target)?;
        let outgoing = match encoder.encode(state, &element, buffer) {
            Ok(outgoing) => outgoing,
            Err(e) => {
                rejected.push(failure(target, encoder.as_ref(), element, e.to_string())?);
                return Ok(());
            }
        };
        let record_len = outgoing.key.as_ref().map_or(0, |key| key.len())
            + outgoing.value.map_or(0, <[u8]>::len);
        let sent = loop {
            if pending.is_full() {
                wait_for_oldest(pending, target, encoder.as_ref(), written, rejected)?;
            }
            let entry = pending.deliveries.send_entry(pending.next_entry());
            match producer.send(&outgoing, entry) {
                Ok(()) => break Ok(()),
                Err(RDKafkaErrorCode::QueueFull) => {
                    if pending.is_empty() {
                        thread::sleep(QUEUE_FULL_WAIT);
                        // A full local queue is not a broker throttle signal.
                        Metrics::retry_wait_msecs(METRICS)
                            .inc_by(i64::try_from(QUEUE_FULL_WAIT.as_millis()).unwrap_or(i64::MAX));
                    } else {
                        wait_for_oldest(pending, target, encoder.as_ref(), written, rejected)?;
                    }
                }
                Err(code) => break Err(KafkaError::MessageProduction(code)),
            }
        };
        drop(outgoing);
        match sent {
            Ok(()) => {}
            Err(error) if is_record_error(&error) => {
                rejected.push(failure(
                    target,
                    encoder.as_ref(),
                    element,
                    error.to_string(),
                )?);
                return Ok(());
            }
            Err(error) => return Err(send_failed(&target.topic, &error)),
        }
        // The element is close to the size of its record.
        pending.push(InFlight {
            element,
            record_len,
            bytes: 2 * record_len + size_of::<InFlight<E::Element>>(),
        });
        while pending.bytes > MAX_IN_FLIGHT_BYTES {
            wait_for_oldest(pending, target, encoder.as_ref(), written, rejected)?;
        }
        // Frees the records that Kafka confirmed, with no wait.
        std::iter::from_fn(|| pending.pop_answered(false)).try_for_each(
            |(element, record_len, result)| {
                handle_result(
                    target,
                    encoder.as_ref(),
                    element,
                    record_len,
                    result,
                    written,
                    rejected,
                )
            },
        )
    }
}

/// The producer of `target`. Gets the shared producer at the first call.
fn topic_producer<'a>(
    producer: &'a mut Option<TopicProducer>,
    target: &Target,
) -> beam::Result<&'a TopicProducer> {
    match producer {
        Some(producer) => Ok(producer),
        None => {
            let shared = shared_producer(&target.connection)?;
            Ok(producer.insert(TopicProducer::new(shared, &target.topic)))
        }
    }
}

/// Waits for the oldest record. A record that Kafka rejects goes to `rejected`.
fn wait_for_oldest<E: Encoder>(
    pending: &mut Pending<E::Element>,
    target: &Target,
    encoder: &E,
    written: &mut Written,
    rejected: &mut Vec<Row>,
) -> beam::Result {
    match pending.pop_answered(true) {
        Some((element, record_len, result)) => handle_result(
            target, encoder, element, record_len, result, written, rejected,
        ),
        None => Ok(()),
    }
}

/// Handles the answer of Kafka for the record of `element`. A record that Kafka rejects goes
/// to `rejected`. Another failure fails the bundle.
fn handle_result<E: Encoder>(
    target: &Target,
    encoder: &E,
    element: E::Element,
    record_len: usize,
    result: Delivery,
    written: &mut Written,
    rejected: &mut Vec<Row>,
) -> beam::Result {
    match result {
        Delivery::Delivered => {
            written.observe(record_len);
            Ok(())
        }
        Delivery::Pending => Ok(()),
        Delivery::Failed(code) => match KafkaError::MessageProduction(code) {
            error if is_record_error(&error) => {
                rejected.push(failure(target, encoder, element, error.to_string())?);
                Ok(())
            }
            error => Err(send_failed(&target.topic, &error)),
        },
    }
}

/// The failure row of a rejected element: the element, as the encoder gives it, and
/// `message`.
fn failure<E: Encoder>(
    target: &Target,
    encoder: &E,
    element: E::Element,
    message: String,
) -> beam::Result<Row> {
    let failed = encoder
        .failed_value(element)
        .map_err(|e: CodecError| beam::Error::msg(format!("{message}, and then: {e}")))?;
    Row::new(
        Arc::clone(&target.errors),
        vec![Some(failed), Some(FieldValue::String(message))],
    )
    .map_err(|e| beam::Error::msg(e.to_string()))
}

/// Whether Kafka rejects the record itself, so that another attempt fails too.
fn is_record_error(error: &KafkaError) -> bool {
    matches!(
        error.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::MessageSizeTooLarge
                | RDKafkaErrorCode::InvalidMessageSize
                | RDKafkaErrorCode::InvalidMessage
                | RDKafkaErrorCode::InvalidRecord
        )
    )
}

fn send_failed(topic: &str, error: &KafkaError) -> beam::Error {
    beam::Error::msg(format!("A write to Kafka topic '{topic}' failed: {error}"))
}

/// Each copy gets the shared producer in `setup`, and starts with no record in flight.
impl<E: Encoder> Clone for WriteFn<E> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.target), Arc::clone(&self.encoder))
    }
}

impl<E: Encoder> DoFn for WriteFn<E> {
    type In = E::Element;
    type Out = Row;

    fn setup(&mut self) -> beam::Result {
        topic_producer(&mut self.producer, &self.target).map(|_| ())
    }

    fn start_bundle(&mut self) -> beam::Result {
        self.written = Written::default();
        self.windows = BundleWindows::new();
        Ok(())
    }

    fn process_element(
        &mut self,
        element: E::Element,
        ctx: &mut ProcessContext<'_, Row>,
    ) -> beam::Result {
        self.windows.add(GLOBAL, ctx.timestamp());
        let mut rejected = Vec::new();
        let sent = self.send(element, &mut rejected);
        ctx.emit_all(rejected)?;
        sent
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Row>) -> beam::Result {
        let Self {
            target,
            encoder,
            pending,
            written,
            windows,
            ..
        } = self;
        let mut rejected = Vec::new();
        let answered = std::iter::from_fn(|| {
            (!pending.is_empty())
                .then(|| wait_for_oldest(pending, target, encoder.as_ref(), written, &mut rejected))
        })
        .find(Result::is_err)
        .unwrap_or(Ok(()));
        // The runner retries a failed bundle as a whole, so its other records need no answer.
        if answered.is_err() {
            pending.reset();
            *written = Written::default();
        } else {
            std::mem::take(written).record(&target.connection.bootstrap_servers, &target.topic);
        }
        emit_global(ctx, windows, rejected)?;
        answered
    }
}
