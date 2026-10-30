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

//! Record values that are Beam rows ([`BeamRow`]).

use std::sync::Arc;

use beam::schema::{FieldType, FieldValue, Row, RowElement, Schema};
use beam::values::PCollection;

use super::row_input::RowInput;
use super::{CodecError, Decoder, Encoder, Outgoing, RecordRef};

/// Why a read of this codec with no schema fails.
const NO_SCHEMA: &str = "A BeamRow read needs a schema: use BeamRow::schema";

/// Record values in the encoding of the Beam row coder (`beam:coder:row:v1`), read into and
/// written from `Row`s of one schema.
///
/// The value bytes are already the bytes of an element, so the codec passes them on: the read
/// emits each value still encoded, and a later step decodes it only if it reads its fields.
/// The write sends a row that reaches it still encoded (from another SDK) as it is, and
/// encodes the others. The read does not check the values: a value that is not a row of the
/// schema fails the step that decodes it.
#[derive(Clone, Default)]
pub struct BeamRow {
    schema: Option<Arc<Schema>>,
    input: RowInput,
}

impl BeamRow {
    /// The codec of a write: rows of any schema.
    pub fn new() -> Self {
        Self::default()
    }

    /// The codec of a read of rows of `schema`.
    pub fn schema(schema: impl Into<Arc<Schema>>) -> Self {
        Self {
            schema: Some(schema.into()),
            input: RowInput::default(),
        }
    }
}

impl Decoder for BeamRow {
    type Element = RowElement;
    type Output = Row;

    fn validate(&self) -> beam::Result {
        match self.schema {
            Some(_) => Ok(()),
            None => Err(beam::Error::msg(NO_SCHEMA)),
        }
    }

    fn decode(&self, record: &RecordRef<'_>) -> Result<RowElement, CodecError> {
        if self.schema.is_none() {
            return Err(CodecError::new(NO_SCHEMA));
        }
        record
            .value_or_err()
            .map(|value| RowElement::Encoded(value.to_vec()))
    }

    fn output(&self, elements: PCollection<RowElement>) -> PCollection<Row> {
        let rows = elements.as_rows();
        match &self.schema {
            Some(schema) => rows.with_row_schema(schema),
            None => rows,
        }
    }
}

impl Encoder for BeamRow {
    type Input = Row;
    type Element = RowElement;

    fn elements(&mut self, input: &PCollection<Row>) -> beam::Result<PCollection<RowElement>> {
        self.input.bind(input).map(|(_, elements)| elements)
    }

    fn encode<'a>(
        &self,
        element: &'a RowElement,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        match element {
            RowElement::Encoded(bytes) => Ok(Outgoing::value(bytes)),
            RowElement::Row(row) => {
                *buffer = row
                    .to_row_bytes()
                    .map_err(|e| CodecError::new(format!("Cannot encode the row: {e}")))?;
                Ok(Outgoing::value(buffer))
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
