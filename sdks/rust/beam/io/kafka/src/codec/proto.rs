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

//! Record values that are Protocol Buffers messages ([`Proto`]), on the `proto_io` crate.

use std::path::Path;
use std::sync::Arc;

use beam::schema::{FieldType, FieldValue, Row, RowElement, Schema};
use beam::values::PCollection;
use beam_io_proto::ProtoSchema;

use super::row_input::{RowInput, bind_rows};
use super::{CodecError, Decoder, Encoder, Outgoing, RecordRef, RowEncoder};

/// Record values that are the messages of one Protocol Buffers message type, read into and
/// written from `Row`s of the schema of that type. See `proto_io` for the schema mapping and
/// what it does not support.
#[derive(Clone, Debug)]
pub struct Proto {
    message: ProtoSchema,
    input: RowInput,
}

impl Proto {
    /// The codec of the message `message_name` of `descriptor_set`, a serialized
    /// `google.protobuf.FileDescriptorSet`. The name is the full name, with the package.
    ///
    /// # Errors
    ///
    /// Fails as [`ProtoSchema::new`] fails.
    pub fn new(descriptor_set: &[u8], message_name: &str) -> beam::Result<Self> {
        ProtoSchema::new(descriptor_set, message_name)
            .map(Self::from)
            .map_err(beam::Error::msg)
    }

    /// The codec of the message `message_name` of the file descriptor set in the file `path`.
    ///
    /// # Errors
    ///
    /// Fails as [`ProtoSchema::from_file`] fails.
    pub fn from_file(path: impl AsRef<Path>, message_name: &str) -> beam::Result<Self> {
        ProtoSchema::from_file(path, message_name)
            .map(Self::from)
            .map_err(beam::Error::msg)
    }

    /// The Beam schema of the rows.
    pub fn row_schema(&self) -> &Arc<Schema> {
        self.message.schema()
    }
}

impl From<ProtoSchema> for Proto {
    fn from(message: ProtoSchema) -> Self {
        Self {
            message,
            input: RowInput::default(),
        }
    }
}

impl Decoder for Proto {
    type Element = RowElement;
    type Output = Row;
    type State = ();

    fn decode(&self, _state: &mut (), record: &RecordRef<'_>) -> Result<RowElement, CodecError> {
        self.message
            .decode(record.value_or_err()?)
            .map(RowElement::Row)
            .map_err(CodecError::from)
    }

    fn output(&self, elements: PCollection<RowElement>) -> PCollection<Row> {
        elements.as_rows().with_row_schema(self.message.schema())
    }
}

impl Encoder for Proto {
    type Input = Row;
    type Element = RowElement;
    type State = ();

    fn elements(&mut self, input: &PCollection<Row>) -> beam::Result<PCollection<RowElement>> {
        bind_rows(self, input)
    }

    fn encode<'a>(
        &self,
        state: &mut (),
        element: &'a RowElement,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        let row = self.input.row(element)?;
        self.encode_row(state, &row, buffer)
    }

    fn failed_type(&self) -> FieldType {
        self.input.failed_type()
    }

    fn failed_value(&self, element: RowElement) -> Result<FieldValue, CodecError> {
        self.input.failed_value(element)
    }
}

impl RowEncoder for Proto {
    fn bind_schema(&mut self, schema: Arc<Schema>) -> beam::Result {
        self.message
            .check_fields(&schema)
            .map_err(beam::Error::msg)?;
        self.input.bind(schema);
        Ok(())
    }

    fn encode_row<'a>(
        &self,
        _state: &mut (),
        row: &Row,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        buffer.clear();
        self.message.encode(row, buffer)?;
        Ok(Outgoing::value(buffer))
    }
}
