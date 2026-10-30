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

//! Sends records to one topic through the producer of the connector.

use std::sync::Arc;

use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::BaseRecord;

use super::delivery::Entry;
use crate::native::client::Producer;
use crate::native::codec::Outgoing;

/// A producer and the topic that it writes.
pub struct TopicProducer {
    producer: Arc<Producer>,
    topic: String,
}

impl TopicProducer {
    /// Sends to `topic` through `producer`.
    pub fn new(producer: Arc<Producer>, topic: &str) -> Self {
        Self {
            producer,
            topic: topic.to_string(),
        }
    }

    /// Sends `record`. The delivery callback of the producer gets `entry` with the result.
    ///
    /// # Errors
    ///
    /// Fails with the code of librdkafka when it refuses the record at once, for example
    /// when its queue is full. The record is not sent, and `entry` drops.
    pub fn send(&self, record: &Outgoing<'_>, entry: Arc<Entry>) -> Result<(), RDKafkaErrorCode> {
        let Outgoing {
            key,
            value,
            headers,
            timestamp,
        } = record;
        let base = BaseRecord::<[u8], [u8], Arc<Entry>>::with_opaque_to(&self.topic, entry);
        let base = match value {
            Some(value) => base.payload(*value),
            None => base,
        };
        let base = match key {
            Some(key) => base.key(key.as_ref()),
            None => base,
        };
        let base = match timestamp {
            Some(timestamp) => base.timestamp(timestamp.as_millis()),
            None => base,
        };
        let base = match headers.is_empty() {
            true => base,
            false => base.headers(headers.iter().fold(
                OwnedHeaders::new_with_capacity(headers.len()),
                |list, (key, value)| list.insert(Header { key, value: *value }),
            )),
        };
        self.producer.send(base).map_err(|(error, _record)| {
            error.rdkafka_error_code().unwrap_or(RDKafkaErrorCode::Fail)
        })
    }
}
