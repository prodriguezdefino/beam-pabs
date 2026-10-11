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
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, FieldMap};
use crate::schema::{BeamRow, Row, RowElement, Schema, SchemaError};

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
        Self::decode_element_with_context(reader, None, None, None)
    }

    /// Decodes a row inside another value, such as a key-value pair, into a [`Row`].
    fn decode_element_with_context(
        reader: &mut &[u8],
        schema: Option<&Arc<Schema>>,
        field_map: Option<&FieldMap>,
        state_reader: Option<&Arc<dyn StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        Row::decode_element_with_context(reader, schema, field_map, state_reader).map(Self::Row)
    }

    /// `bytes` are one whole element, so they are the row coder bytes of the row.
    fn decode_with_context(
        bytes: &[u8],
        _schema: Option<&Arc<Schema>>,
        _field_map: Option<&FieldMap>,
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

/// Coder for a struct that implements [`BeamRow`]. It reads and writes rows in the layout of
/// a writer schema, which is the schema of `T` unless [`Self::with_writer_schema`] sets
/// another one.
#[derive(Clone, Debug)]
pub struct RowStructCoder<T> {
    schema: Arc<Schema>,
    field_map: FieldMap,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: BeamRow> Default for RowStructCoder<T> {
    fn default() -> Self {
        Self {
            schema: T::beam_schema(),
            field_map: FieldMap::Same,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: BeamRow> RowStructCoder<T> {
    /// Creates a coder for rows in the layout of the schema of `T`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a coder for rows in the layout of `writer_schema`. Fields are matched by name.
    /// The coder skips writer fields that `T` does not have on decode, and writes them as null
    /// on encode.
    ///
    /// # Errors
    ///
    /// [`SchemaError::FieldNotFound`] if `writer_schema` has no field for a required field
    /// of `T`.
    pub fn with_writer_schema(writer_schema: Arc<Schema>) -> Result<Self, SchemaError> {
        let field_map = T::field_map(&writer_schema)?;
        Ok(Self {
            schema: writer_schema,
            field_map,
            _marker: std::marker::PhantomData,
        })
    }

    /// Moves the values of `row`, which has the schema of `T`, to the writer field positions.
    fn writer_row(&self, row: Row, indices: &[Option<usize>]) -> Result<Row, SchemaError> {
        let slots = vec![None; self.schema.num_fields()];
        let values =
            row.into_values()
                .into_iter()
                .zip(indices)
                .fold(slots, |mut slots, (value, index)| {
                    if let Some(index) = index {
                        slots[*index] = value;
                    }
                    slots
                });
        let missing = values
            .iter()
            .zip(&self.schema.fields)
            .find(|(value, field)| value.is_none() && !field.field_type.nullable);
        if let Some((_, field)) = missing {
            return Err(SchemaError::UnexpectedNull {
                expected: format!("writer field '{}' of type {}", field.name, field.field_type),
            });
        }
        Row::new(Arc::clone(&self.schema), values)
    }
}

impl<T: BeamRow + Send + Sync + 'static> Coder<T> for RowStructCoder<T> {
    fn urn(&self) -> &'static str {
        URN_ROW
    }

    fn encode(&self, value: &T, writer: &mut Vec<u8>, _context: Context) -> Result<(), CoderError> {
        match &self.field_map {
            FieldMap::Same => value.encode_row_bytes(writer),
            FieldMap::ByName(indices) => {
                let row = self.writer_row(value.to_row()?, indices)?;
                RowCoder::encode_row(&row, writer)
            }
        }
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<T, CoderError> {
        match &self.field_map {
            FieldMap::Same => T::decode_row_bytes(reader),
            FieldMap::ByName(indices) => {
                let row = RowCoder::decode_row(&self.schema, reader)?;
                Ok(T::from_row_mapped(&row, indices)?)
            }
        }
    }
}
