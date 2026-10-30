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

use beam::schema::{FieldValue, Row, Schema};
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use super::delivery::{Deliveries, Delivery};
use super::emit_global;
use super::produce::TopicProducer;
use crate::native::client::{Connection, shared_producer};
use crate::native::codec::{CodecError, Encoder};

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
    fn pop_answered(&mut self, wait: bool) -> Option<(T, Delivery)> {
        let result = match wait {
            true => self.deliveries.wait_for_result(self.first),
            false => self.deliveries.result(self.first),
        };
        match (result, self.records.front()) {
            (Delivery::Pending, _) | (_, None) => None,
            (result, Some(_)) => {
                let InFlight { element, bytes } = self.records.pop_front()?;
                self.bytes -= bytes;
                self.first = self.first.wrapping_add(1);
                Some((element, result))
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
    producer: Option<TopicProducer>,
    /// The buffer of the encoder. The producer copies each record, so each element reuses it.
    buffer: Vec<u8>,
    pending: Pending<E::Element>,
}

impl<E: Encoder> WriteFn<E> {
    pub(super) fn new(target: Arc<Target>, encoder: Arc<E>) -> Self {
        Self {
            target,
            encoder,
            producer: None,
            buffer: Vec::new(),
            pending: Pending::default(),
        }
    }

    /// Encodes `element` and sends its record. An element that does not convert, or whose
    /// record the producer rejects at once, goes to `rejected`.
    fn send(&mut self, element: E::Element, rejected: &mut Vec<Row>) -> beam::Result {
        let Self {
            target,
            encoder,
            producer,
            buffer,
            pending,
        } = self;
        let producer = topic_producer(producer, target)?;
        let outgoing = match encoder.encode(&element, buffer) {
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
                wait_for_oldest(pending, target, encoder.as_ref(), rejected)?;
            }
            let entry = pending.deliveries.send_entry(pending.next_entry());
            match producer.send(&outgoing, entry) {
                Ok(()) => break Ok(()),
                Err(RDKafkaErrorCode::QueueFull) => match pending.is_empty() {
                    true => thread::sleep(QUEUE_FULL_WAIT),
                    false => wait_for_oldest(pending, target, encoder.as_ref(), rejected)?,
                },
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
            bytes: 2 * record_len + size_of::<InFlight<E::Element>>(),
        });
        while pending.bytes > MAX_IN_FLIGHT_BYTES {
            wait_for_oldest(pending, target, encoder.as_ref(), rejected)?;
        }
        // Frees the records that Kafka confirmed, with no wait.
        std::iter::from_fn(|| pending.pop_answered(false)).try_for_each(|(element, result)| {
            handle_result(target, encoder.as_ref(), element, result, rejected)
        })
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
    rejected: &mut Vec<Row>,
) -> beam::Result {
    match pending.pop_answered(true) {
        Some((element, result)) => handle_result(target, encoder, element, result, rejected),
        None => Ok(()),
    }
}

/// Handles the answer of Kafka for the record of `element`. A record that Kafka rejects goes
/// to `rejected`. Another failure fails the bundle.
fn handle_result<E: Encoder>(
    target: &Target,
    encoder: &E,
    element: E::Element,
    result: Delivery,
    rejected: &mut Vec<Row>,
) -> beam::Result {
    match result {
        Delivery::Delivered | Delivery::Pending => Ok(()),
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

    fn process_element(
        &mut self,
        element: E::Element,
        ctx: &mut ProcessContext<'_, Row>,
    ) -> beam::Result {
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
            ..
        } = self;
        let mut rejected = Vec::new();
        let answered = std::iter::from_fn(|| {
            (!pending.is_empty())
                .then(|| wait_for_oldest(pending, target, encoder.as_ref(), &mut rejected))
        })
        .find(Result::is_err)
        .unwrap_or(Ok(()));
        // The runner retries a failed bundle as a whole, so its other records need no answer.
        if answered.is_err() {
            pending.reset();
        }
        rejected
            .into_iter()
            .try_for_each(|row| emit_global(ctx, row))?;
        answered
    }
}
