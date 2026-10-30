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

//! The record formats of the Kafka transforms of the Managed API, on top of the native codecs.
//!
//! Cross-language callers exchange rows, and name the format of the record values:
//!
//! | Managed format | Native codec | Rows |
//! |---|---|---|
//! | `RAW` | [`Payload`]`<`[`Raw`]`>` | `(payload: BYTES)` |
//! | `STRING` | [`Payload`]`<`[`Utf8`]`>` | `(payload: STRING)` (read only in Java) |
//! | `JSON` | [`Json`] | The fields of the JSON Schema |
//! | `AVRO` | [`Avro`] | The fields of the Avro schema |
//!
//! `JSON` and `AVRO` are the native codecs, which read and write rows. `RAW` and `STRING`
//! decorate the native codec: the read moves its element into the one field of the row, and
//! the write lends the field of the row to it, so neither copies the value.

use std::sync::Arc;

use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam::values::PCollection;

use super::row_input::RowInput;
use super::{Avro, CodecError, Decoder, Encoder, Json, Outgoing, Raw, RecordRef, Utf8};

/// The name of the one field of the rows of [`Payload`].
pub const PAYLOAD_FIELD: &str = "payload";

/// An element that can be the one field of a [`Payload`] row: `Vec<u8>` (`BYTES`) or `String`
/// (`STRING`).
pub trait PayloadField: beam::coders::DefaultCoder {
    /// The type of the field.
    fn field_type() -> FieldType;
    /// The field value that holds `self`, with no copy.
    fn into_field(self) -> FieldValue;
    /// The element inside `value`, if `value` holds one.
    fn from_field(value: &FieldValue) -> Option<&Self>;
}

impl PayloadField for Vec<u8> {
    fn field_type() -> FieldType {
        FieldType::bytes()
    }

    fn into_field(self) -> FieldValue {
        FieldValue::Bytes(self)
    }

    fn from_field(value: &FieldValue) -> Option<&Self> {
        match value {
            FieldValue::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }
}

impl PayloadField for String {
    fn field_type() -> FieldType {
        FieldType::string()
    }

    fn into_field(self) -> FieldValue {
        FieldValue::String(self)
    }

    fn from_field(value: &FieldValue) -> Option<&Self> {
        match value {
            FieldValue::String(text) => Some(text),
            _ => None,
        }
    }
}

/// The elements of the codec `C` as rows of one field, `payload`. `Payload<Raw>` is the
/// Managed `RAW` format and `Payload<Utf8>` the Managed `STRING` format.
#[derive(Clone)]
pub struct Payload<C> {
    codec: C,
    schema: Arc<Schema>,
    input: RowInput,
}

impl<C, T> Payload<C>
where
    C: Decoder<Element = T, Output = T>,
    T: PayloadField,
{
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            schema: Arc::new(Schema::new(vec![Field::new(
                PAYLOAD_FIELD,
                T::field_type(),
            )])),
            input: RowInput::default(),
        }
    }

    /// The schema of the rows: `(payload: <type of T>)`.
    pub fn row_schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

impl Payload<Raw> {
    /// The Managed `RAW` format: rows `(payload: BYTES)`.
    pub fn raw() -> Self {
        Self::new(Raw)
    }
}

impl Payload<Utf8> {
    /// The Managed `STRING` format: rows `(payload: STRING)`.
    pub fn string() -> Self {
        Self::new(Utf8)
    }
}

impl<C, T> Decoder for Payload<C>
where
    C: Decoder<Element = T, Output = T>,
    T: PayloadField,
{
    type Element = RowElement;
    type Output = Row;

    fn validate(&self) -> beam::Result {
        self.codec.validate()
    }

    fn decode(&self, record: &RecordRef<'_>) -> Result<RowElement, CodecError> {
        let value = self.codec.decode(record)?.into_field();
        Row::new(Arc::clone(&self.schema), vec![Some(value)])
            .map(RowElement::Row)
            .map_err(|e| CodecError::new(e.to_string()))
    }

    fn reads_headers(&self) -> bool {
        self.codec.reads_headers()
    }

    fn output(&self, elements: PCollection<RowElement>) -> PCollection<Row> {
        elements.as_rows().with_row_schema(&self.schema)
    }
}

impl<C, T> Encoder for Payload<C>
where
    C: Encoder<Input = T, Element = T>,
    T: PayloadField,
{
    type Input = Row;
    type Element = RowElement;

    fn elements(&mut self, input: &PCollection<Row>) -> beam::Result<PCollection<RowElement>> {
        let (schema, elements) = self.input.bind(input)?;
        match schema.fields.as_slice() {
            [field] if field.field_type.type_info == T::field_type().type_info => Ok(elements),
            [field] => Err(beam::Error::msg(format!(
                "The rows need one {} field, but '{}' is {}",
                T::field_type(),
                field.name,
                field.field_type
            ))),
            fields => Err(beam::Error::msg(format!(
                "The rows need exactly one {} field, found {} fields",
                T::field_type(),
                fields.len()
            ))),
        }
    }

    fn encode<'a>(
        &self,
        element: &'a RowElement,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        match element {
            // The field of the row goes to the codec as it is.
            RowElement::Row(row) => self.codec.encode(payload_of(row)?, buffer),
            // A row from another SDK: decode it, and keep the value in the buffer.
            RowElement::Encoded(_) => {
                let row = self.input.row(element)?.into_owned();
                let mut scratch = Vec::new();
                let value = self
                    .codec
                    .encode(payload_of(&row)?, &mut scratch)?
                    .value
                    .map(<[u8]>::to_vec);
                match value {
                    Some(value) => {
                        *buffer = value;
                        Ok(Outgoing::value(buffer))
                    }
                    None => Ok(Outgoing::default()),
                }
            }
        }
    }

    fn failed_type(&self) -> FieldType {
        self.input.failed_type()
    }

    fn failed_value(&self, element: RowElement) -> Result<FieldValue, CodecError> {
        self.input.failed_value(element)
    }
}

/// The element in the one field of `row`.
fn payload_of<T: PayloadField>(row: &Row) -> Result<&T, CodecError> {
    match row.get_value_by_index(0) {
        Some(Some(value)) => T::from_field(value)
            .ok_or_else(|| CodecError::new(format!("The payload is not {}", T::field_type()))),
        Some(None) => Err(CodecError::new("The payload is null")),
        None => Err(CodecError::new("The row has no payload")),
    }
}

/// A codec of a Managed format, for the decoders and encoders that the expansion service
/// builds from a configuration.
#[derive(Clone)]
pub enum Format {
    Raw(Payload<Raw>),
    String(Payload<Utf8>),
    Json(Json),
    Avro(Avro),
}

impl std::fmt::Debug for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Raw(_) => "RAW",
            Self::String(_) => "STRING",
            Self::Json(_) => "JSON",
            Self::Avro(_) => "AVRO",
        })
    }
}

impl Format {
    /// The codec of the Managed format `name` (any case). `JSON` and `AVRO` reads need
    /// `schema`: a JSON Schema or an Avro schema.
    ///
    /// # Errors
    ///
    /// Fails for an unknown format, a format that the connector does not support, or a
    /// missing or invalid schema.
    pub fn parse(name: &str, schema: Option<&str>) -> beam::Result<Self> {
        match (name.to_ascii_uppercase().as_str(), schema) {
            ("RAW", _) => Ok(Self::Raw(Payload::raw())),
            ("STRING", _) => Ok(Self::String(Payload::string())),
            ("JSON", Some(schema)) => Json::schema(schema).map(Self::Json),
            ("JSON", None) => Ok(Self::Json(Json::new())),
            ("AVRO", Some(schema)) => Avro::schema(schema).map(Self::Avro),
            ("AVRO", None) => Ok(Self::Avro(Avro::new())),
            ("PROTO", _) => Err(beam::Error::msg(
                "The native Kafka connector does not support the PROTO format yet",
            )),
            _ => Err(beam::Error::msg(format!(
                "Unknown format '{name}': expected RAW, STRING, JSON or AVRO"
            ))),
        }
    }

    /// Fails if a read cannot use this format: `JSON` and `AVRO` reads need a schema.
    ///
    /// # Errors
    ///
    /// Fails with the reason.
    pub fn validate_read(&self) -> beam::Result {
        match self {
            Self::Raw(codec) => codec.validate(),
            Self::String(codec) => codec.validate(),
            Self::Json(codec) => codec.validate(),
            Self::Avro(codec) => codec.validate(),
        }
    }
}
