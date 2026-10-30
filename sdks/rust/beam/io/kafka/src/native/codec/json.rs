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

//! Record values that are JSON objects ([`Json`]).

use std::sync::Arc;

use beam::schema::{FieldType, FieldValue, Row, RowElement};
use beam::values::PCollection;
use json_io::{JsonReader, schema_from_json_schema, write_json};

use super::row_input::RowInput;
use super::{CodecError, Decoder, Encoder, Outgoing, RecordRef};

/// Why a read of this codec with no schema fails.
const NO_SCHEMA: &str = "A JSON read needs a JSON Schema: use Json::schema";

/// Record values that are JSON objects, read into and written from `Row`s.
///
/// The read needs the JSON Schema of the objects ([`Json::schema`]); the rows have the Beam
/// schema of that JSON Schema. The write takes rows of any schema ([`Json::new`]) and writes
/// each one as an object with a property for each field.
#[derive(Clone, Default)]
pub struct Json {
    reader: Option<Arc<JsonReader>>,
    input: RowInput,
}

impl Json {
    /// The codec of a write: rows of any schema.
    pub fn new() -> Self {
        Self::default()
    }

    /// The codec of a read of the objects that the JSON Schema `json_schema` describes.
    ///
    /// # Errors
    ///
    /// Fails if `json_schema` is not a JSON Schema that maps to a Beam schema.
    pub fn schema(json_schema: &str) -> beam::Result<Self> {
        let reader = schema_from_json_schema(json_schema)
            .and_then(|schema| JsonReader::new(Arc::new(schema)))
            .map_err(beam::Error::msg)?;
        Ok(Self {
            reader: Some(Arc::new(reader)),
            input: RowInput::default(),
        })
    }

    /// The Beam schema of the rows that the read gives, if the codec has a JSON Schema.
    pub fn row_schema(&self) -> Option<Arc<beam::schema::Schema>> {
        self.reader
            .as_ref()
            .map(|reader| Arc::clone(reader.schema()))
    }
}

impl Decoder for Json {
    type Element = RowElement;
    type Output = Row;

    fn validate(&self) -> beam::Result {
        match self.reader {
            Some(_) => Ok(()),
            None => Err(beam::Error::msg(NO_SCHEMA)),
        }
    }

    fn decode(&self, record: &RecordRef<'_>) -> Result<RowElement, CodecError> {
        let reader = self
            .reader
            .as_ref()
            .ok_or_else(|| CodecError::new(NO_SCHEMA))?;
        reader
            .read(record.value_or_err()?)
            .map(RowElement::Row)
            .map_err(CodecError::from)
    }

    fn output(&self, elements: PCollection<RowElement>) -> PCollection<Row> {
        let rows = elements.as_rows();
        match self.row_schema() {
            Some(schema) => rows.with_row_schema(&schema),
            None => rows,
        }
    }
}

impl Encoder for Json {
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
        let row = self.input.row(element)?;
        write_json(&row, buffer)?;
        Ok(Outgoing::value(buffer))
    }

    fn failed_type(&self) -> FieldType {
        self.input.failed_type()
    }

    fn failed_value(&self, element: RowElement) -> Result<FieldValue, CodecError> {
        self.input.failed_value(element)
    }
}
