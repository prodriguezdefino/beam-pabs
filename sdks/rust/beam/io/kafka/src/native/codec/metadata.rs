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

//! Records with their key, headers and timestamp ([`WithMetadata`], [`KafkaRecord`]).

use std::borrow::Cow;

use beam::Instant;
use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use beam::schema::{FieldType, FieldValue};
use beam::values::PCollection;

use super::{CodecError, Decoder, Encoder, Outgoing, Raw, RecordRef};

/// A Kafka record with a key of type `K` and a value of type `V`, as Java's `KafkaRecord`.
///
/// The read fills every field. The write uses the key, the value, the headers and the
/// timestamp, and sends to the topic of its options; it does not use the topic, the partition
/// or the offset.
#[derive(Clone, Debug, PartialEq)]
pub struct KafkaRecord<K, V> {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// The timestamp of the record, if it has one. On write, `None` lets the producer set the
    /// time of the send.
    pub timestamp: Option<Instant>,
    pub key: Option<K>,
    /// The value. `None` is a tombstone.
    pub value: Option<V>,
    /// The headers, in their order. A header can have no value.
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

impl<K, V> KafkaRecord<K, V> {
    /// A record to write with this value and nothing else.
    pub fn of_value(value: V) -> Self {
        Self {
            topic: String::new(),
            partition: -1,
            offset: -1,
            timestamp: None,
            key: None,
            value: Some(value),
            headers: Vec::new(),
        }
    }

    pub fn with_key(mut self, key: K) -> Self {
        self.key = Some(key);
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: Option<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value));
        self
    }

    pub fn with_timestamp(mut self, timestamp: Instant) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

/// The records of a read or a write with their key, headers and timestamp, as
/// [`KafkaRecord`]s. The value codec `V` converts the values and the key codec `K` (by default
/// [`Raw`]) the keys. A record with no key or no value (a tombstone) has `None` there, and the
/// codecs see only the records that have one.
///
/// The key codec gets the record with the key in place of the value, so any codec of values
/// can convert keys.
///
/// ```no_run
/// use kafka_io::native::codec::{Raw, Utf8, WithMetadata};
/// use kafka_io::native::read::{Read, ReadOptions};
///
/// // KafkaRecord<String, Vec<u8>>: UTF-8 keys and raw values.
/// let read = Read::new(ReadOptions::new("broker:9092", "events"))
///     .with_decoder(WithMetadata::new(Raw).with_key(Utf8));
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct WithMetadata<V, K = Raw> {
    key: K,
    value: V,
}

impl<V> WithMetadata<V, Raw> {
    /// The records with values of `value` and raw keys.
    pub fn new(value: V) -> Self {
        Self { key: Raw, value }
    }
}

impl<V, K> WithMetadata<V, K> {
    /// The same records, with keys of `key`.
    pub fn with_key<K2>(self, key: K2) -> WithMetadata<V, K2> {
        WithMetadata {
            key,
            value: self.value,
        }
    }
}

impl<V, K, VE, KE> Decoder for WithMetadata<V, K>
where
    V: Decoder<Element = VE, Output = VE>,
    K: Decoder<Element = KE, Output = KE>,
    VE: DefaultCoder,
    KE: DefaultCoder,
{
    type Element = KafkaRecord<KE, VE>;
    type Output = KafkaRecord<KE, VE>;

    fn validate(&self) -> beam::Result {
        self.key.validate()?;
        self.value.validate()
    }

    fn decode(&self, record: &RecordRef<'_>) -> Result<Self::Element, CodecError> {
        let key = match record.key {
            Some(key) => {
                let key_record = RecordRef {
                    value: Some(key),
                    ..*record
                };
                Some(
                    self.key
                        .decode(&key_record)
                        .map_err(|e| CodecError::new(format!("The key does not convert: {e}")))?,
                )
            }
            None => None,
        };
        let value = match record.value {
            Some(_) => Some(self.value.decode(record)?),
            None => None,
        };
        Ok(KafkaRecord {
            topic: record.topic.to_string(),
            partition: record.partition,
            offset: record.offset,
            timestamp: record.timestamp,
            key,
            value,
            headers: record
                .headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.map(<[u8]>::to_vec)))
                .collect(),
        })
    }

    fn reads_headers(&self) -> bool {
        true
    }

    fn output(&self, elements: PCollection<Self::Element>) -> PCollection<Self::Output> {
        elements
    }
}

impl<V, K, VE, KE> Encoder for WithMetadata<V, K>
where
    V: Encoder<Input = VE, Element = VE>,
    K: Encoder<Input = KE, Element = KE>,
    VE: DefaultCoder,
    KE: DefaultCoder,
{
    type Input = KafkaRecord<KE, VE>;
    type Element = KafkaRecord<KE, VE>;

    fn elements(
        &mut self,
        input: &PCollection<Self::Input>,
    ) -> beam::Result<PCollection<Self::Element>> {
        Ok(input.clone())
    }

    fn encode<'a>(
        &self,
        element: &'a Self::Element,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        // Keys are small: the key codec writes into its own buffer, and the record owns a copy.
        let key = match &element.key {
            Some(key) => {
                let mut key_buffer = Vec::new();
                self.key
                    .encode(key, &mut key_buffer)
                    .map_err(|e| CodecError::new(format!("The key does not convert: {e}")))?
                    .value
                    .map(|bytes| Cow::Owned(bytes.to_vec()))
            }
            None => None,
        };
        let value = match &element.value {
            Some(value) => self.value.encode(value, buffer)?.value,
            None => None,
        };
        Ok(Outgoing {
            key,
            value,
            headers: element
                .headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_deref()))
                .collect(),
            timestamp: element.timestamp,
        })
    }

    fn failed_type(&self) -> FieldType {
        self.value.failed_type()
    }

    /// The value of the rejected record, or null for a tombstone.
    fn failed_value(&self, element: Self::Element) -> Result<FieldValue, CodecError> {
        match element.value {
            Some(value) => self.value.failed_value(value),
            None => Err(CodecError::new("The rejected record has no value")),
        }
    }
}

/// Wire format: the nested KV
/// `((topic, (partition, offset)), ((timestamp_ms, headers), (key, value)))`, with the
/// standard coders of each part, so any SDK can decode it.
#[derive(Clone, Copy, Debug, Default)]
pub struct KafkaRecordCoder;

/// The type whose coder has the wire format of [`KafkaRecordCoder`].
type WireFormat<K, V> = (
    (String, (i32, i64)),
    (
        (Option<i64>, Vec<(String, Option<Vec<u8>>)>),
        (Option<K>, Option<V>),
    ),
);

impl<K: DefaultCoder, V: DefaultCoder> Coder<KafkaRecord<K, V>> for KafkaRecordCoder {
    fn urn(&self) -> &'static str {
        beam::coders::URN_KV
    }

    fn encode(
        &self,
        record: &KafkaRecord<K, V>,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        // Every part is nested, except the last one, which takes the enclosing context, as in
        // the KV coder.
        String::coder().encode(&record.topic, writer, Context::Nested)?;
        i32::coder().encode(&record.partition, writer, Context::Nested)?;
        i64::coder().encode(&record.offset, writer, Context::Nested)?;
        let timestamp = record.timestamp.map(|t| t.as_millis());
        <Option<i64>>::coder().encode(&timestamp, writer, Context::Nested)?;
        <Vec<(String, Option<Vec<u8>>)>>::coder().encode(
            &record.headers,
            writer,
            Context::Nested,
        )?;
        <Option<K>>::coder().encode(&record.key, writer, Context::Nested)?;
        <Option<V>>::coder().encode(&record.value, writer, context)
    }

    fn decode(
        &self,
        reader: &mut &[u8],
        context: Context,
    ) -> Result<KafkaRecord<K, V>, CoderError> {
        Ok(KafkaRecord {
            topic: String::coder().decode(reader, Context::Nested)?,
            partition: i32::coder().decode(reader, Context::Nested)?,
            offset: i64::coder().decode(reader, Context::Nested)?,
            timestamp: <Option<i64>>::coder()
                .decode(reader, Context::Nested)?
                .map(Instant::from_millis),
            headers: <Vec<(String, Option<Vec<u8>>)>>::coder().decode(reader, Context::Nested)?,
            key: <Option<K>>::coder().decode(reader, Context::Nested)?,
            value: <Option<V>>::coder().decode(reader, context)?,
        })
    }
}

impl<K: DefaultCoder, V: DefaultCoder> DefaultCoder for KafkaRecord<K, V> {
    type Coder = KafkaRecordCoder;

    fn coder() -> Self::Coder {
        KafkaRecordCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        KafkaRecordCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        KafkaRecordCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <WireFormat<K, V>>::register_coder(registry)
    }
}
