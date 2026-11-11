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

//! Record values as rows of one field ([`Payload`]).
//!
//! [`Payload`] decorates a codec of `Vec<u8>` or `String` elements: each element is a `Row`
//! with one field, `payload`. The read moves its element into the field, and the write lends
//! the field to the codec, so neither copies the value. The field name is the name of the row
//! shape that other SDKs use for raw Kafka values.

use std::sync::Arc;

use beam::coders::DefaultCoder;
use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam::values::PCollection;

use super::row_input::{RowInput, bind_rows};
use super::{CodecError, Decoder, Encoder, Outgoing, Raw, RecordRef, RowEncoder, Utf8};

/// The name of the one field of the rows of [`Payload`].
pub const PAYLOAD_FIELD: &str = "payload";

/// An element that can be the one field of a [`Payload`] row: `Vec<u8>` (`BYTES`) or `String`
/// (`STRING`).
pub trait PayloadField: DefaultCoder {
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

/// The elements of the codec `C` as rows of one field, `payload`. `Payload<Raw>` gives rows
/// `(payload: BYTES)` and `Payload<Utf8>` rows `(payload: STRING)`.
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

    /// The row that holds `value` in its one field.
    fn row(&self, value: T) -> Result<RowElement, CodecError> {
        Row::new(Arc::clone(&self.schema), vec![Some(value.into_field())])
            .map(RowElement::Row)
            .map_err(|e| CodecError::new(e.to_string()))
    }
}

impl Payload<Raw> {
    /// Rows `(payload: BYTES)` of the record values.
    pub fn raw() -> Self {
        Self::new(Raw)
    }
}

impl Payload<Utf8> {
    /// Rows `(payload: STRING)` of the record values, which must be UTF-8.
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
    type State = C::State;

    fn validate(&self) -> beam::Result {
        self.codec.validate()
    }

    fn decode(
        &self,
        state: &mut C::State,
        record: &RecordRef<'_>,
    ) -> Result<RowElement, CodecError> {
        self.row(self.codec.decode(state, record)?)
    }

    fn decode_batch(
        &self,
        state: &mut C::State,
        records: &[RecordRef<'_>],
        out: &mut Vec<Result<RowElement, CodecError>>,
    ) {
        let mut values = Vec::with_capacity(records.len());
        self.codec.decode_batch(state, records, &mut values);
        out.extend(
            values
                .into_iter()
                .map(|value| value.and_then(|value| self.row(value))),
        );
    }

    fn reads_headers(&self) -> bool {
        self.codec.reads_headers()
    }

    fn decodes_batches(&self) -> bool {
        self.codec.decodes_batches()
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
    type State = (C::State, Vec<u8>);

    fn elements(&mut self, input: &PCollection<Row>) -> beam::Result<PCollection<RowElement>> {
        bind_rows(self, input)
    }

    fn encode<'a>(
        &self,
        state: &mut Self::State,
        element: &'a RowElement,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        match element {
            // The field of the row goes to the codec as it is.
            RowElement::Row(row) => self.codec.encode(&mut state.0, payload_of(row)?, buffer),
            // A row from another SDK: decode it, and keep the value in the buffer.
            RowElement::Encoded(_) => {
                let row = self.input.row(element)?;
                self.encode_row(state, &row, buffer)
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

impl<C, T> RowEncoder for Payload<C>
where
    C: Encoder<Input = T, Element = T>,
    T: PayloadField,
{
    fn bind_schema(&mut self, schema: Arc<Schema>) -> beam::Result {
        match schema.fields.as_slice() {
            [field] if field.field_type.type_info == T::field_type().type_info => Ok(()),
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
        }?;
        self.input.bind(schema);
        Ok(())
    }

    /// Copies the value of the inner codec into `buffer`, because the inner codec can lend
    /// the field of `row`, which does not live long enough.
    fn encode_row<'a>(
        &self,
        (inner, scratch): &mut Self::State,
        row: &Row,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        let Outgoing {
            value, timestamp, ..
        } = self.codec.encode(inner, payload_of(row)?, scratch)?;
        buffer.clear();
        Ok(match value {
            Some(value) => {
                buffer.extend_from_slice(value);
                Outgoing {
                    value: Some(buffer),
                    timestamp,
                    ..Outgoing::default()
                }
            }
            None => Outgoing {
                timestamp,
                ..Outgoing::default()
            },
        })
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
