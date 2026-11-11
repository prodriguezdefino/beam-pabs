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

//! How Kafka records become elements, and elements become Kafka records.
//!
//! A [`Decoder`] turns each fetched record into an element of the read, and an [`Encoder`]
//! turns each element of the write into a record. Each codec is named after the wire format
//! of the record value; the table gives the Beam element that it reads and writes. The read
//! and the write take the value bytes as they are ([`Raw`]) unless they get another codec.
//!
//! | Codec | Record value (wire format) | Beam element |
//! |---|---|---|
//! | [`Raw`] (default) | Any bytes | `Vec<u8>` |
//! | [`Utf8`] | UTF-8 text | `String` |
//! | [`Json`] | A JSON object | `Row` |
//! | [`Avro`] | An Avro binary datum, alone or in the Confluent wire format | `Row` |
//! | [`Proto`] | A Protocol Buffers message | `Row` |
//! | [`BeamRow`] | A Beam row (`beam:coder:row:v1`) | `Row`, passed on still encoded |
//! | [`WithMetadata`] | The format of another codec | [`KafkaRecord`]: the element of that codec with the key, the headers and the timestamp |
//!
//! [`SchemaRegistry`] gives the [`Avro`] codec the schemas of a Confluent Schema Registry.
//! [`Payload`] gives the elements of [`Raw`] or [`Utf8`] as rows of one field.
//!
//! [`Typed::new`] adapts a `Row` codec to a `#[derive(BeamRow)]` type. A codec of your own
//! works with [`Typed`] when it implements [`RowEncoder`] or a `Decoder` of `RowElement`s.
//!
//! A codec of its own implements [`Decoder`], [`Encoder`] or both:
//!
//! ```
//! use beam::values::PCollection;
//! use beam_io_kafka::codec::{CodecError, Decoder, RecordRef};
//!
//! /// Values that are a big-endian `i64`.
//! struct Counter;
//!
//! impl Decoder for Counter {
//!     type Element = i64;
//!     type Output = i64;
//!     type State = ();
//!
//!     fn decode(&self, _state: &mut (), record: &RecordRef<'_>) -> Result<i64, CodecError> {
//!         let bytes: [u8; 8] = record.value_or_err()?.try_into().map_err(|_| "not 8 bytes")?;
//!         Ok(i64::from_be_bytes(bytes))
//!     }
//!
//!     fn output(&self, elements: PCollection<i64>) -> PCollection<i64> {
//!         elements
//!     }
//! }
//! ```

mod avro;
mod beam_row;
mod json;
mod metadata;
mod payload;
mod proto;
mod raw;
mod registry;
mod row_input;
mod typed;

use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use beam::Instant;
use beam::coders::DefaultCoder;
use beam::schema::{
    BeamRow as BeamRowValue, Field, FieldType, FieldValue, Row, RowElement, Schema,
};
use beam::values::PCollection;

pub use avro::{Avro, Framing};
pub use beam_row::BeamRow;
pub use json::Json;
pub use metadata::{KafkaRecord, WithMetadata};
pub use payload::{PAYLOAD_FIELD, Payload, PayloadField};
pub use proto::Proto;
pub use raw::{Raw, Utf8};
pub use registry::{RegisteredSchema, SchemaRegistry};
pub use typed::Typed;

/// A fetched record. Every field borrows from the buffer of librdkafka, so a decoder that
/// keeps nothing copies nothing.
#[derive(Clone, Copy, Debug)]
pub struct RecordRef<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    /// The timestamp of the record, if it has one.
    pub timestamp: Option<Instant>,
    pub key: Option<&'a [u8]>,
    /// The value of the record. `None` for a tombstone.
    pub value: Option<&'a [u8]>,
    /// The headers of the record, in their order. A header can have no value.
    pub headers: &'a [(&'a str, Option<&'a [u8]>)],
}

impl<'a> RecordRef<'a> {
    /// The value of the record, or an error for a record with no value (a tombstone).
    pub fn value_or_err(&self) -> Result<&'a [u8], CodecError> {
        self.value
            .ok_or_else(|| CodecError::new("The record has no value"))
    }
}

/// A record to send. Each part borrows from the element or from the buffer of the writer.
#[derive(Clone, Debug, Default)]
pub struct Outgoing<'a> {
    /// The key of the record. `Cow` lets an encoder that writes the key into a buffer of its
    /// own (as [`WithMetadata`] does) hand it over.
    pub key: Option<Cow<'a, [u8]>>,
    /// The value of the record. `None` sends a tombstone.
    pub value: Option<&'a [u8]>,
    pub headers: Vec<(&'a str, Option<&'a [u8]>)>,
    /// The timestamp of the record. `None` lets the producer set the time of the send.
    pub timestamp: Option<Instant>,
}

impl<'a> Outgoing<'a> {
    /// A record with this value and nothing else.
    pub fn value(value: &'a [u8]) -> Self {
        Self {
            value: Some(value),
            ..Self::default()
        }
    }
}

/// Why a record or an element does not convert. The read and the write send the record or
/// the element to their failure output with this message, or fail the bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecError(String);

impl CodecError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for CodecError {}

impl From<String> for CodecError {
    fn from(message: String) -> Self {
        Self(message)
    }
}

impl From<&str> for CodecError {
    fn from(message: &str) -> Self {
        Self(message.to_string())
    }
}

/// Turns fetched records into the elements of a read.
pub trait Decoder: Send + Sync + 'static {
    /// What the reader emits for each record.
    type Element: DefaultCoder;
    /// The elements of the read: the same type, or `Row` for a decoder that emits
    /// `RowElement`s.
    type Output: 'static;
    /// What the decoder keeps between calls, for example the decoder of a format library.
    /// Each copy of the reader owns one, from `Default`. A decoder that keeps nothing uses `()`.
    type State: Default + Send + Sync + 'static;

    /// Fails if the decoder cannot read records, for example a decoder of rows with no
    /// schema. The read calls it when it expands, so the pipeline fails before it runs.
    ///
    /// # Errors
    ///
    /// Fails with the reason.
    fn validate(&self) -> beam::Result {
        Ok(())
    }

    /// The element of `record`.
    fn decode(
        &self,
        state: &mut Self::State,
        record: &RecordRef<'_>,
    ) -> Result<Self::Element, CodecError>;

    /// Appends the element of each of `records` to `out`, in their order. The reader calls it
    /// with the records of one poll. The default calls [`Decoder::decode`] for each record.
    /// A decoder that reads a batch faster, such as [`Avro`], replaces it. A record that does
    /// not convert gets its own error, so the other records of the batch are not lost.
    fn decode_batch(
        &self,
        state: &mut Self::State,
        records: &[RecordRef<'_>],
        out: &mut Vec<Result<Self::Element, CodecError>>,
    ) {
        out.extend(records.iter().map(|record| self.decode(state, record)));
    }

    /// The schema of the `RowElement::Encoded` elements that `decode` gives. [`Typed`] decodes
    /// them with it. The default is `None`, for a decoder that gives no encoded rows.
    fn encoded_row_schema(&self) -> Option<&Arc<Schema>> {
        None
    }

    /// Whether `decode` uses the headers of a record. librdkafka parses headers only on
    /// request. So a decoder that returns `false` (the default) gets records with no headers.
    fn reads_headers(&self) -> bool {
        false
    }

    /// Whether [`Decoder::decode_batch`] is faster than one `decode` for each record. The
    /// reader collects the records of a poll only for a decoder that returns `true`. The
    /// default is `false`.
    fn decodes_batches(&self) -> bool {
        false
    }

    /// The output of the read from the PCollection that the reader emits, for example with
    /// its row schema.
    fn output(&self, elements: PCollection<Self::Element>) -> PCollection<Self::Output>;
}

/// Turns the elements of a write into records.
pub trait Encoder: Clone + Send + Sync + 'static {
    /// The elements that the write takes.
    type Input: 'static;
    /// What the writer gets for each input: the same type, or `RowElement` for `Row` inputs,
    /// so rows from another SDK reach the encoder still encoded.
    type Element: DefaultCoder;
    /// What the encoder keeps between calls. Each copy of the writer owns one, from
    /// `Default`. An encoder that keeps nothing uses `()`.
    type State: Default + Send + Sync + 'static;

    /// The PCollection that the writer gets from `input`. An encoder that needs the schema of
    /// the input keeps it here.
    ///
    /// # Errors
    ///
    /// Fails if the encoder cannot write the elements of `input`.
    fn elements(
        &mut self,
        input: &PCollection<Self::Input>,
    ) -> beam::Result<PCollection<Self::Element>>;

    /// Prepares an encoder that encodes a part of an element, such as the key or the value of
    /// [`WithMetadata`]. The parent calls it in place of `elements`. The default does nothing.
    ///
    /// # Errors
    ///
    /// Fails if the encoder cannot write its elements.
    fn prepare(&mut self) -> beam::Result {
        Ok(())
    }

    /// The record of `element`. The parts borrow from `element` or are written into `buffer`,
    /// which the writer reuses for each element.
    fn encode<'a>(
        &self,
        state: &mut Self::State,
        element: &'a Self::Element,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError>;

    /// The type of the `failed_row` field of the failure output.
    fn failed_type(&self) -> FieldType;

    /// The `failed_row` value of an element that the write rejects.
    fn failed_value(&self, element: Self::Element) -> Result<FieldValue, CodecError>;
}

/// An [`Encoder`] of `Row` inputs that [`Typed`] adapts to a `#[derive(BeamRow)]` type.
pub trait RowEncoder: Encoder<Input = Row, Element = RowElement> {
    /// Keeps `schema`, the schema of the rows to write. `elements` calls it with the schema of
    /// its input, and [`Typed`] with the schema of its type.
    ///
    /// # Errors
    ///
    /// Fails if the encoder cannot write rows of `schema`.
    fn bind_schema(&mut self, schema: Arc<Schema>) -> beam::Result;

    /// The record of `row`. The parts borrow only from `buffer`.
    fn encode_row<'a>(
        &self,
        state: &mut Self::State,
        row: &Row,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError>;

    /// The record of `element`. The default encodes the `Row` of `element`.
    fn encode_typed<'a, T: BeamRowValue>(
        &self,
        state: &mut Self::State,
        element: &T,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        let row = element
            .to_row()
            .map_err(|e| CodecError::new(format!("Cannot convert the value to a row: {e}")))?;
        self.encode_row(state, &row, buffer)
    }
}

/// The schema of the failure output of a read: the record value (empty for a record with no
/// value) and the reason.
pub fn read_error_schema() -> Schema {
    failure_schema(FieldType::bytes())
}

/// The schema of the failure output of a write whose encoder rejects elements as
/// `failed_type`: the element and the reason.
pub fn write_error_schema(failed_type: FieldType) -> Schema {
    failure_schema(failed_type)
}

fn failure_schema(failed_type: FieldType) -> Schema {
    Schema::new(vec![
        Field::new("failed_row", failed_type),
        Field::new("error_message", FieldType::string()),
    ])
}
