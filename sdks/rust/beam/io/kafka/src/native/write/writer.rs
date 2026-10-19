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

//! The DoFn that sends the rows of each bundle to a topic.

use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use beam::schema::{FieldValue, Row, RowElement, Schema};
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};
use futures::executor::block_on;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryFuture, FutureRecord};

use super::emit_global;
use crate::native::client::{Connection, Producer, shared_producer};
use crate::native::format::Encoder;

/// The most bytes that one DoFn copy keeps for the records that Kafka did not confirm: the
/// record values, the inputs for the error output and the state of each record. With more,
/// the DoFn waits for its oldest records.
///
/// The DoFn sends at most this number of bytes in one acknowledgement time of the cluster.
/// A limit of one producer batch makes the DoFn wait for each batch, so a slow cluster limits
/// its throughput. The queue limits of a producer (`queue.buffering.max.messages`,
/// `queue.buffering.max.kbytes`) apply to all the copies that share it.
const MAX_IN_FLIGHT_BYTES: usize = 32 << 20;

/// The wait before the DoFn sends a record again after the producer queue was full, when
/// the DoFn has no record of its own to wait for.
const QUEUE_FULL_WAIT: Duration = Duration::from_millis(10);

/// The topic and the rows of a write. Every copy of the DoFn shares it.
pub(super) struct Target {
    pub(super) connection: Connection,
    pub(super) topic: String,
    pub(super) schema: Arc<Schema>,
    pub(super) errors: Arc<Schema>,
    pub(super) encoder: Encoder,
}

/// The input of a record, which the DoFn keeps for the error output until Kafka confirms
/// the record.
enum Input {
    /// The row bytes from the runner. The DoFn decodes them again only if Kafka rejects the
    /// record. The bytes use less memory than the row, and the DoFn can release the row
    /// immediately after it encodes the record value.
    Encoded(Vec<u8>),
    /// A row from a transform in the same process.
    Row(Row),
}

impl Input {
    /// The row of this input. The DoFn decoded the same bytes before, so the decode fails
    /// only if the bytes changed.
    fn into_row(self, schema: &Arc<Schema>) -> beam::Result<Row> {
        match self {
            Self::Encoded(bytes) => decode_row(schema, &bytes),
            Self::Row(row) => Ok(row),
        }
    }
}

/// A record that the DoFn sent, with its input for the case that Kafka rejects it.
struct InFlight {
    input: Input,
    /// The bytes that this record adds to the limit [`MAX_IN_FLIGHT_BYTES`].
    bytes: usize,
    delivery: DeliveryFuture,
}

/// The records that one DoFn copy sent and that Kafka did not confirm yet, oldest first.
#[derive(Default)]
struct Pending {
    records: VecDeque<InFlight>,
    bytes: usize,
}

impl Pending {
    fn push(&mut self, record: InFlight) {
        self.bytes += record.bytes;
        self.records.push_back(record);
    }

    fn pop(&mut self) -> Option<InFlight> {
        let record = self.records.pop_front()?;
        self.bytes -= record.bytes;
        Some(record)
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
    }
}

/// Sends each row of a bundle as one record. `finish_bundle` waits until Kafka has every
/// record, so a bundle succeeds only when its records are written. Emits the rows that do
/// not convert and the rows of records that Kafka rejects, with the reason. Any other
/// failure fails the bundle, and the runner retries it, so the write is at least once.
pub(super) struct WriteFn {
    target: Arc<Target>,
    producer: Option<Arc<Producer>>,
    /// The value of the current record. The producer copies it, so each row reuses it.
    buffer: Vec<u8>,
    pending: Pending,
}

impl WriteFn {
    pub(super) fn new(target: Arc<Target>) -> Self {
        Self {
            target,
            producer: None,
            buffer: Vec::new(),
            pending: Pending::default(),
        }
    }

    fn producer(&mut self) -> beam::Result<Arc<Producer>> {
        match &self.producer {
            Some(producer) => Ok(Arc::clone(producer)),
            None => {
                let producer = shared_producer(&self.target.connection)?;
                self.producer = Some(Arc::clone(&producer));
                Ok(producer)
            }
        }
    }

    /// Converts `row` and sends its record. `encoded` holds the bytes of `row` when the row
    /// came from the runner. A row that does not convert, or whose record the producer
    /// rejects at once, goes to `rejected`.
    fn send(
        &mut self,
        row: Row,
        encoded: Option<Vec<u8>>,
        rejected: &mut Vec<Row>,
    ) -> beam::Result {
        let producer = self.producer()?;
        let Self {
            target,
            buffer,
            pending,
            ..
        } = self;
        let payload = match target.encoder.encode(&row, buffer) {
            Ok(payload) => payload,
            Err(message) => {
                rejected.push(error_row(&target.errors, row, message)?);
                return Ok(());
            }
        };
        let payload_len = payload.len();
        let mut record = FutureRecord::<(), [u8]>::to(&target.topic).payload(payload);
        let sent = loop {
            match producer.send_result(record) {
                Ok(delivery) => break Ok(delivery),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), back)) => {
                    record = back;
                    match pending.is_empty() {
                        true => thread::sleep(QUEUE_FULL_WAIT),
                        false => settle_oldest(pending, target, rejected)?,
                    }
                }
                Err((error, _)) => break Err(error),
            }
        };
        let delivery = match sent {
            Ok(delivery) => delivery,
            Err(error) if is_record_error(&error) => {
                rejected.push(error_row(&target.errors, row, error.to_string())?);
                return Ok(());
            }
            Err(error) => return Err(send_failed(&target.topic, &error)),
        };
        // A row from the runner is released here. Its bytes are smaller, and they are
        // decoded again only for the error output.
        let (input, input_len) = match encoded {
            Some(bytes) => {
                let len = bytes.len();
                (Input::Encoded(bytes), len)
            }
            // The record value is close to the size of the row that it holds.
            None => (Input::Row(row), payload_len),
        };
        pending.push(InFlight {
            input,
            bytes: payload_len + input_len + size_of::<InFlight>(),
            delivery,
        });
        while pending.bytes > MAX_IN_FLIGHT_BYTES {
            settle_oldest(pending, target, rejected)?;
        }
        Ok(())
    }
}

/// Waits for the oldest record. A record that Kafka rejects goes to `rejected`.
fn settle_oldest(pending: &mut Pending, target: &Target, rejected: &mut Vec<Row>) -> beam::Result {
    let Some(InFlight {
        input, delivery, ..
    }) = pending.pop()
    else {
        return Ok(());
    };
    match block_on(delivery) {
        Ok(Ok(_)) => Ok(()),
        Ok(Err((error, _))) if is_record_error(&error) => {
            let row = input.into_row(&target.schema)?;
            rejected.push(error_row(&target.errors, row, error.to_string())?);
            Ok(())
        }
        Ok(Err((error, _))) => Err(send_failed(&target.topic, &error)),
        Err(_) => Err(beam::Error::msg(format!(
            "The Kafka producer of topic '{}' closed before it confirmed a record",
            target.topic
        ))),
    }
}

fn decode_row(schema: &Arc<Schema>, bytes: &[u8]) -> beam::Result<Row> {
    Row::from_row_bytes(schema, bytes)
        .map_err(|e| beam::Error::msg(format!("Cannot decode an input row: {e}")))
}

/// The error row of `row`: the row and `message`.
fn error_row(errors: &Arc<Schema>, row: Row, message: String) -> beam::Result<Row> {
    Row::new(
        Arc::clone(errors),
        vec![
            Some(FieldValue::Row(row)),
            Some(FieldValue::String(message)),
        ],
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
impl Clone for WriteFn {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.target))
    }
}

impl DoFn for WriteFn {
    type In = RowElement;
    type Out = Row;

    fn setup(&mut self) -> beam::Result {
        self.producer().map(|_| ())
    }

    fn process_element(
        &mut self,
        row: RowElement,
        ctx: &mut ProcessContext<'_, Row>,
    ) -> beam::Result {
        let (row, encoded) = match row {
            RowElement::Row(row) => (row, None),
            RowElement::Encoded(bytes) => (decode_row(&self.target.schema, &bytes)?, Some(bytes)),
        };
        let mut rejected = Vec::new();
        let sent = self.send(row, encoded, &mut rejected);
        ctx.emit_all(rejected)?;
        sent
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Row>) -> beam::Result {
        let Self {
            target, pending, ..
        } = self;
        let mut rejected = Vec::new();
        let settled = std::iter::from_fn(|| {
            (!pending.is_empty()).then(|| settle_oldest(pending, target, &mut rejected))
        })
        .find(Result::is_err)
        .unwrap_or(Ok(()));
        // The runner retries a failed bundle as a whole, so its other records need no answer.
        pending.clear();
        rejected
            .into_iter()
            .try_for_each(|row| emit_global(ctx, row))?;
        settled
    }
}
