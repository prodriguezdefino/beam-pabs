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

//! The DoFn that lists the partitions of a topic and resolves where each one starts.

use std::collections::HashMap;
use std::time::Duration;

use beam::Instant;
use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use beam::transforms::{DoFn, ProcessContext};
use rdkafka::consumer::Consumer;
use rdkafka::{Offset, TopicPartitionList};

use super::StartOffset;
use crate::native::client::{self, Connection, KafkaConsumer};

/// The longest wait for each metadata, committed offset and watermark request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One partition of a topic, the offset where its read starts, and the time when a bounded
/// read stops.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionStart {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub stop_at: Option<Instant>,
}

/// Wire format: a KV of `(topic, partition)` and `(offset, stop_at)`, with `stop_at` in
/// milliseconds and nullable.
#[derive(Clone, Copy, Debug, Default)]
pub struct PartitionStartCoder;

impl Coder<PartitionStart> for PartitionStartCoder {
    fn urn(&self) -> &'static str {
        beam::coders::URN_KV
    }

    fn encode(
        &self,
        value: &PartitionStart,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<PartitionStart, CoderError> {
        PartitionStart::decode_element(reader)
    }
}

impl DefaultCoder for PartitionStart {
    type Coder = PartitionStartCoder;

    fn coder() -> Self::Coder {
        PartitionStartCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        // A KV writes its key and then its value, so the fields go out one by one.
        self.topic.encode_element(writer)?;
        self.partition.encode_element(writer)?;
        (self.offset, self.stop_at.map(Instant::as_millis)).encode_element(writer)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let (topic, partition) = <(String, i32)>::decode_element(reader)?;
        let (offset, stop_at) = <(i64, Option<i64>)>::decode_element(reader)?;
        Ok(Self {
            topic,
            partition,
            offset,
            stop_at: stop_at.map(Instant::from_millis),
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <((String, i32), (i64, Option<i64>))>::register_coder(registry)
    }
}

/// Emits one [`PartitionStart`] for each partition of the topic. A partition starts at the
/// committed offset of the consumer group, or else where `start_offset` says.
#[derive(Clone)]
pub(super) struct DiscoverPartitionsFn {
    pub(super) connection: Connection,
    pub(super) topic: String,
    pub(super) start_offset: StartOffset,
    pub(super) max_read_time: Option<Duration>,
}

impl DiscoverPartitionsFn {
    fn starts(&self, consumer: &KafkaConsumer) -> beam::Result<Vec<(i32, i64)>> {
        let topic = self.topic.as_str();
        let metadata = consumer
            .fetch_metadata(Some(topic), REQUEST_TIMEOUT)
            .map_err(|e| failed(topic, "list the partitions", &e))?;
        let partitions: Vec<i32> = metadata
            .topics()
            .iter()
            .filter(|t| t.name() == topic && t.error().is_none())
            .flat_map(|t| t.partitions().iter().map(|p| p.id()))
            .collect();
        if partitions.is_empty() {
            return Err(beam::Error::msg(format!(
                "Kafka topic '{topic}' does not exist or has no partitions"
            )));
        }
        let committed = self.committed(consumer, &partitions)?;
        partitions
            .into_iter()
            .map(|partition| {
                let offset = match committed.get(&partition) {
                    Some(offset) => *offset,
                    None => self.reset(consumer, partition)?,
                };
                Ok((partition, offset))
            })
            .collect()
    }

    /// The committed offset of each partition that has one. Empty with no consumer group.
    fn committed(
        &self,
        consumer: &KafkaConsumer,
        partitions: &[i32],
    ) -> beam::Result<HashMap<i32, i64>> {
        if self.connection.group_id().is_none() {
            return Ok(HashMap::new());
        }
        let topic = self.topic.as_str();
        let mut list = TopicPartitionList::new();
        partitions.iter().for_each(|partition| {
            list.add_partition(topic, *partition);
        });
        let committed = consumer
            .committed_offsets(list, REQUEST_TIMEOUT)
            .map_err(|e| failed(topic, "read the committed offsets", &e))?;
        Ok(committed
            .elements_for_topic(topic)
            .iter()
            .filter_map(|element| match element.offset() {
                Offset::Offset(offset) => Some((element.partition(), offset)),
                _ => None,
            })
            .collect())
    }

    /// The start of a partition with no committed offset.
    fn reset(&self, consumer: &KafkaConsumer, partition: i32) -> beam::Result<i64> {
        let topic = self.topic.as_str();
        let watermarks = || {
            consumer
                .fetch_watermarks(topic, partition, REQUEST_TIMEOUT)
                .map_err(|e| failed(topic, "read the offsets of a partition", &e))
        };
        match self.start_offset {
            StartOffset::Earliest => watermarks().map(|(low, _)| low),
            StartOffset::Latest => watermarks().map(|(_, high)| high),
            StartOffset::Committed => Err(beam::Error::msg(format!(
                "Partition {partition} of Kafka topic '{topic}' has no committed offset, and the \
                 start offset is none"
            ))),
        }
    }
}

fn failed(topic: &str, action: &str, error: &rdkafka::error::KafkaError) -> beam::Error {
    beam::Error::msg(format!("Cannot {action} of Kafka topic '{topic}': {error}"))
}

impl DoFn for DiscoverPartitionsFn {
    type In = Vec<u8>;
    type Out = PartitionStart;

    fn process_element(
        &mut self,
        _impulse: Vec<u8>,
        ctx: &mut ProcessContext<'_, PartitionStart>,
    ) -> beam::Result {
        let consumer = client::consumer(&self.connection)?;
        let stop_at = self.max_read_time.map(|time| Instant::now() + time);
        self.starts(&consumer)?
            .into_iter()
            .try_for_each(|(partition, offset)| {
                ctx.emit(PartitionStart {
                    topic: self.topic.clone(),
                    partition,
                    offset,
                    stop_at,
                })
            })
    }
}
