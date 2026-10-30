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

//! The queue of one assigned partition, which the reader takes in batches.
//!
//! One call of `rd_kafka_consume_batch_queue` takes all the records in the queue, up to
//! [`MAX_BATCH`]. A poll of rdkafka's `PartitionQueue` reads the clock two times and
//! allocates an event for each record. In a local profile, that is about one fifth of the
//! CPU time of the reader. rdkafka has no safe batch call, so this module calls librdkafka.
//! It is the only module of the connector with `unsafe` code for records.

use std::collections::VecDeque;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::bindings::{
    rd_kafka_consume_batch_queue, rd_kafka_header_get_all, rd_kafka_headers_t, rd_kafka_last_error,
    rd_kafka_message_destroy, rd_kafka_message_headers, rd_kafka_message_t,
    rd_kafka_message_timestamp, rd_kafka_queue_destroy, rd_kafka_queue_forward,
    rd_kafka_queue_get_partition, rd_kafka_queue_t, rd_kafka_timestamp_type_t,
};
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaRespErr;

use super::call::{MAX_BATCH, Polled};
use crate::native::client::KafkaConsumer;
use crate::native::codec::CodecError;

/// The headers of one record, in their order. A header can have no value.
pub type RecordHeaders<'a> = Vec<(&'a str, Option<&'a [u8]>)>;

/// The queue of one partition of a consumer. From the creation of this queue until it drops,
/// the consumer sends the records of the partition here, not to its main queue.
pub struct PartitionQueue {
    queue: NonNull<rd_kafka_queue_t>,
    /// The records of the last batch that the reader did not poll yet, oldest first.
    fetched: VecDeque<FetchedRecord>,
    /// Whether the last batch had the end of the partition after its records.
    end_after_fetched: bool,
    /// Room for the pointers of one batch, used again for each batch.
    batch: Vec<*mut rd_kafka_message_t>,
    /// Keeps the consumer until the queue and its records are freed.
    _consumer: Arc<KafkaConsumer>,
}

// SAFETY: librdkafka lets any thread use a queue and its messages. The queue has one owner,
// which uses it from one thread at a time.
unsafe impl Send for PartitionQueue {}

/// How one message of a batch changes the queue.
enum BatchMessage {
    Record(FetchedRecord),
    EndOfPartition,
    Error(RDKafkaRespErr),
}

impl PartitionQueue {
    /// The queue of `partition` of `topic`, or `None` if the topic name has a NUL byte.
    pub fn new(consumer: &Arc<KafkaConsumer>, topic: &str, partition: i32) -> Option<Self> {
        let topic = CString::new(topic).ok()?;
        // SAFETY: `native_ptr` is the `rd_kafka_t` of `consumer`, which `_consumer` keeps
        // until the queue is destroyed. `topic` is a NUL-terminated string that lives until
        // the call returns, and librdkafka copies it. The call returns a new reference to
        // the queue of the partition, which `Drop` releases.
        let queue = unsafe {
            rd_kafka_queue_get_partition(consumer.client().native_ptr(), topic.as_ptr(), partition)
        };
        let queue = NonNull::new(queue)?;
        // SAFETY: `queue` is a valid queue. A forward to NULL stops the forward of the
        // partition queue to the queue of the consumer, so its records stay here.
        unsafe { rd_kafka_queue_forward(queue.as_ptr(), ptr::null_mut()) };
        Some(Self {
            queue,
            fetched: VecDeque::new(),
            end_after_fetched: false,
            batch: Vec::with_capacity(MAX_BATCH),
            _consumer: Arc::clone(consumer),
        })
    }

    /// The next record. With no record of the last batch left, takes the next batch and
    /// waits at most `timeout` for its first record.
    ///
    /// # Errors
    ///
    /// Fails when librdkafka gives an error for the partition, other than its end.
    pub fn poll(&mut self, timeout: Duration) -> Result<Polled<FetchedRecord>, KafkaError> {
        match self.fetched.pop_front() {
            Some(record) => Ok(Polled::Record(record)),
            None if std::mem::take(&mut self.end_after_fetched) => Ok(Polled::EndOfPartition),
            None => {
                let reached_end = self.fetch_records(timeout)?;
                match (self.fetched.pop_front(), reached_end) {
                    (Some(record), _) => {
                        self.end_after_fetched = reached_end;
                        Ok(Polled::Record(record))
                    }
                    (None, true) => Ok(Polled::EndOfPartition),
                    (None, false) => Ok(Polled::Nothing),
                }
            }
        }
    }

    /// Takes the records in the queue. With none, waits at most `timeout` for the first
    /// record and then takes the rest. Returns whether the queue gave the end of the
    /// partition, which is not a record.
    fn fetch_records(&mut self, timeout: Duration) -> Result<bool, KafkaError> {
        // `rd_kafka_consume_batch_queue` waits until it has the full batch or its timeout
        // passes. So only the call for one record waits.
        let reached_end = self.consume_batch(Duration::ZERO, MAX_BATCH)?;
        match (reached_end, self.fetched.is_empty(), timeout.is_zero()) {
            (false, true, false) => {
                let reached_end = self.consume_batch(timeout, 1)?;
                match self.fetched.is_empty() {
                    true => Ok(reached_end),
                    false => Ok(self.consume_batch(Duration::ZERO, MAX_BATCH - 1)? || reached_end),
                }
            }
            _ => Ok(reached_end),
        }
    }

    /// Takes at most `max` messages, and waits at most `timeout` for all of them.
    fn consume_batch(&mut self, timeout: Duration, max: usize) -> Result<bool, KafkaError> {
        let Self {
            queue,
            fetched,
            batch,
            ..
        } = self;
        let max = max.min(batch.capacity());
        let timeout_ms = c_int::try_from(timeout.as_millis()).unwrap_or(c_int::MAX);
        batch.clear();
        // SAFETY: `queue` is valid until `Drop`. `batch` has room for `max` pointers.
        // librdkafka writes at most `max` of them, each a message that the caller owns.
        let count = unsafe {
            rd_kafka_consume_batch_queue(queue.as_ptr(), timeout_ms, batch.as_mut_ptr(), max)
        };
        let Ok(count) = usize::try_from(count) else {
            // SAFETY: Reads the error of the last librdkafka call of this thread.
            let error = unsafe { rd_kafka_last_error() };
            return Err(KafkaError::MessageConsumption(error.into()));
        };
        // SAFETY: librdkafka wrote the first `count` pointers, and `count <= max`.
        unsafe { batch.set_len(count.min(max)) };
        // Each message is freed when its record drops, also an error or the end.
        let (reached_end, error) = batch
            .drain(..)
            .filter_map(NonNull::new)
            .map(|message| FetchedRecord(message).into())
            .fold((false, None), |(end, error), message| match message {
                BatchMessage::Record(record) => {
                    fetched.push_back(record);
                    (end, error)
                }
                BatchMessage::EndOfPartition => (true, error),
                BatchMessage::Error(code) => (end, error.or(Some(code))),
            });
        match error {
            Some(code) => Err(KafkaError::MessageConsumption(code.into())),
            None => Ok(reached_end),
        }
    }
}

impl Drop for PartitionQueue {
    fn drop(&mut self) {
        self.fetched.clear();
        // SAFETY: Releases the reference of `new`. No record of this queue is left.
        unsafe { rd_kafka_queue_destroy(self.queue.as_ptr()) };
    }
}

/// A record that the reader took from the queue of its partition. Every field borrows from
/// the fetch buffer of librdkafka.
pub struct FetchedRecord(NonNull<rd_kafka_message_t>);

// SAFETY: A message has one owner, and librdkafka lets any thread read and free it.
unsafe impl Send for FetchedRecord {}

impl From<FetchedRecord> for BatchMessage {
    fn from(record: FetchedRecord) -> Self {
        match record.message().err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR => Self::Record(record),
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF => Self::EndOfPartition,
            code => Self::Error(code),
        }
    }
}

impl FetchedRecord {
    fn message(&self) -> &rd_kafka_message_t {
        // SAFETY: The message is valid until `Drop`, and librdkafka does not change it.
        unsafe { self.0.as_ref() }
    }

    pub fn offset(&self) -> i64 {
        self.message().offset
    }

    pub fn partition(&self) -> i32 {
        self.message().partition
    }

    pub fn key(&self) -> Option<&[u8]> {
        let &rd_kafka_message_t { key, key_len, .. } = self.message();
        // SAFETY: librdkafka sets `key` to `key_len` bytes of the message, or to NULL.
        unsafe { bytes(key, key_len) }
    }

    pub fn value(&self) -> Option<&[u8]> {
        let &rd_kafka_message_t { payload, len, .. } = self.message();
        // SAFETY: librdkafka sets `payload` to `len` bytes of the message, or to NULL.
        unsafe { bytes(payload, len) }
    }

    /// The timestamp of the record in milliseconds, if it has one.
    pub fn timestamp_millis(&self) -> Option<i64> {
        let mut kind = rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
        // SAFETY: The message is valid, and `kind` is valid for a write.
        let timestamp = unsafe { rd_kafka_message_timestamp(self.0.as_ptr(), &mut kind) };
        match kind {
            rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE => None,
            rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_CREATE_TIME
            | rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME => {
                (timestamp != -1).then_some(timestamp)
            }
        }
    }

    /// The headers of the record. librdkafka parses them at the first call for the record.
    ///
    /// # Errors
    ///
    /// Fails when a header name is not UTF-8.
    pub fn headers(&self) -> Result<RecordHeaders<'_>, CodecError> {
        let mut native: *mut rd_kafka_headers_t = ptr::null_mut();
        // SAFETY: The message is valid, and `native` is valid for a write. The headers
        // belong to the message.
        let error = unsafe { rd_kafka_message_headers(self.0.as_ptr(), &mut native) };
        match (error, native.is_null()) {
            (RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR, false) => (0..)
                // SAFETY: `native` is a valid header list of the message, which lives as
                // long as `self`.
                .map_while(|index| unsafe { header(native, index) })
                .map(|(name, value)| {
                    name.to_str()
                        .map(|name| (name, value))
                        .map_err(|e| CodecError::new(format!("A header name is not UTF-8: {e}")))
                })
                .collect(),
            _ => Ok(Vec::new()),
        }
    }
}

impl Drop for FetchedRecord {
    fn drop(&mut self) {
        // SAFETY: The record owns the message, which `rd_kafka_consume_batch_queue` gave.
        unsafe { rd_kafka_message_destroy(self.0.as_ptr()) };
    }
}

/// The name and the value of the header at `index`, or `None` after the last header.
///
/// # Safety
///
/// `headers` must be a valid header list that outlives `'a` and does not change.
unsafe fn header<'a>(
    headers: *const rd_kafka_headers_t,
    index: usize,
) -> Option<(&'a CStr, Option<&'a [u8]>)> {
    let mut name: *const c_char = ptr::null();
    let mut value: *const c_void = ptr::null();
    let mut size = 0_usize;
    // SAFETY: The caller guarantees `headers`. The outputs are valid for writes. librdkafka
    // sets them to the name and the value of the header, or returns an error after the last
    // header.
    let error =
        unsafe { rd_kafka_header_get_all(headers, index, &mut name, &mut value, &mut size) };
    match (error, name.is_null()) {
        // SAFETY: librdkafka gives a NUL-terminated name and a value of `size` bytes (NULL
        // for a header with no value). Both live as long as the header list.
        (RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR, false) => {
            Some(unsafe { (CStr::from_ptr(name), bytes(value.cast_mut(), size)) })
        }
        _ => None,
    }
}

/// The `len` bytes at `data`, or `None` for NULL.
///
/// # Safety
///
/// `data` must be NULL or point to `len` bytes that outlive `'a` and do not change.
unsafe fn bytes<'a>(data: *mut c_void, len: usize) -> Option<&'a [u8]> {
    match data.is_null() {
        true => None,
        // SAFETY: The caller guarantees `len` valid bytes at `data`.
        false => Some(unsafe { slice::from_raw_parts(data.cast::<u8>(), len) }),
    }
}
