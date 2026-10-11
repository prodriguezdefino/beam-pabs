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

//! Row coders for [`RowElement`] and for structs that implement
//! [`BeamRow`](crate::schema::BeamRow).

use std::io::Write;
use std::sync::Arc;

use super::RowCoder;
use crate::coders::URN_ROW;
use crate::coders::iterable::StateStreamReader;
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use crate::schema::{Row, RowElement, Schema};

/// Coder for a [`RowElement`]: the standard Beam row coder. A whole element decodes into its
/// bytes, with no [`Row`]. An element inside another value decodes into a [`Row`], because
/// only the schema tells where its bytes end.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowElementCoder {
    schema: Option<Arc<Schema>>,
}

impl RowElementCoder {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema: Some(schema),
        }
    }
}

impl Coder<RowElement> for RowElementCoder {
    fn urn(&self) -> &'static str {
        URN_ROW
    }

    fn encode(
        &self,
        value: &RowElement,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<RowElement, CoderError> {
        RowElement::decode_element_with_schema(reader, self.schema.as_ref())
    }
}

impl RowElement {
    /// Returns the row. Decodes the bytes with `schema` if the element holds bytes.
    pub fn into_row(self, schema: &Arc<Schema>) -> Result<Row, CoderError> {
        match self {
            Self::Encoded(bytes) => RowCoder::decode_row(schema, &mut bytes.as_slice()),
            Self::Row(row) => Ok(row),
        }
    }
}

impl DefaultCoder for RowElement {
    type Coder = RowElementCoder;

    fn coder() -> Self::Coder {
        RowElementCoder::default()
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        match self {
            Self::Encoded(bytes) => Ok(writer.write_all(bytes)?),
            Self::Row(row) => RowCoder::encode_row(row, writer),
        }
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        Self::decode_element_with_schema(reader, None)
    }

    /// Decodes a row inside another value, such as a key-value pair, into a [`Row`].
    fn decode_element_with_schema(
        reader: &mut &[u8],
        schema: Option<&Arc<Schema>>,
    ) -> Result<Self, CoderError> {
        Row::decode_element_with_schema(reader, schema).map(Self::Row)
    }

    /// `bytes` are one whole element, so they are the row coder bytes of the row.
    fn decode_with_schema(bytes: &[u8], _schema: Option<&Arc<Schema>>) -> Result<Self, CoderError> {
        Ok(Self::Encoded(bytes.to_vec()))
    }

    /// `bytes` are one whole element, so they are the row coder bytes of the row.
    fn decode_with_context(
        bytes: &[u8],
        _schema: Option<&Arc<Schema>>,
        _state_reader: Option<&Arc<dyn StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        Ok(Self::Encoded(bytes.to_vec()))
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        Row::register_coder(registry)
    }

    fn register_value_coder<R: CoderRegistry + ?Sized>(&self, registry: &R) -> String {
        match self {
            Self::Row(row) => row.register_value_coder(registry),
            Self::Encoded(_) => Self::register_coder(registry),
        }
    }
}

/// Coder for user structs that implement [`crate::schema::BeamRow`], as standard Beam rows with
/// the schema derived from the struct at compile time.
#[derive(Clone, Debug)]
pub struct RowStructCoder<T> {
    schema: Arc<Schema>,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: crate::schema::BeamRow> Default for RowStructCoder<T> {
    fn default() -> Self {
        Self {
            schema: Arc::clone(T::beam_schema()),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: crate::schema::BeamRow> RowStructCoder<T> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T: crate::schema::BeamRow + Send + Sync + 'static> Coder<T> for RowStructCoder<T> {
    fn urn(&self) -> &'static str {
        URN_ROW
    }

    fn encode(&self, value: &T, writer: &mut Vec<u8>, _context: Context) -> Result<(), CoderError> {
        let row = value.to_row()?;
        RowCoder::encode_row(&row, writer)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<T, CoderError> {
        let row = RowCoder::decode_row(&self.schema, reader)?;
        Ok(T::from_row(&row)?)
    }
}
