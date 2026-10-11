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

//! Standard Row coder (`beam:coder:row:v1`) for a Beam [`Row`] with its [`Schema`]. The wire
//! format has three parts, in this order:
//!
//! - The number of fields, as a `beam:coder:varint:v1`.
//! - The null bitmask: a packed byte array with a varint length prefix. A 1 bit means that
//!   the field at that position is null. If no field is null, the length is 0.
//! - The encodings of all non-null field values, one after the other.
//!
//! The bitmask and the values use the *encoding position* order (see [`encoding_order`]).
//! Two logical types do not encode as their representation type: see
//! [`encode_logical_value`](encode::encode_logical_value).

mod decode;
mod element;
mod encode;
pub(crate) mod header;
mod skip;

pub use element::{RowElementCoder, RowStructCoder};
pub(crate) use {decode::decode_field_value, encode::encode_field_value};

use std::io::{self, Error as IoError, ErrorKind};
use std::sync::Arc;

use crate::coders::URN_ROW;
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, FieldMap};
use crate::schema::{Row, Schema};
use header::{bit_is_null, read_row_header, write_row_header};
use skip::skip_field_value;

/// Coder for Beam [`Row`] instances.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowCoder {
    schema: Option<Arc<Schema>>,
}

impl RowCoder {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema: Some(schema),
        }
    }

    pub fn schema(&self) -> Option<&Arc<Schema>> {
        self.schema.as_ref()
    }

    /// Encodes a row with the schema of the row.
    pub fn encode_row(row: &Row, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        let schema = row.schema();
        let num_fields = schema.num_fields();
        let order = encoding_order(schema);
        let field_at = |position: usize| order.as_ref().map_or(position, |o| o[position]);
        let values = row.values();

        write_row_header(
            num_fields,
            |position| values[field_at(position)].is_none(),
            writer,
        );
        (0..num_fields)
            .map(field_at)
            .filter_map(|index| values[index].as_ref().map(|v| (index, v)))
            .try_for_each(|(index, field_val)| {
                encode_field_value(field_val, &schema.fields[index].field_type, writer)
            })
    }

    /// Decodes a row with `schema`. Returns an error if the field count does not match.
    pub fn decode_row(schema: &Arc<Schema>, reader: &mut &[u8]) -> Result<Row, CoderError> {
        let num_fields = schema.num_fields();
        let null_bits = read_row_header(num_fields, reader)?;
        let order = encoding_order(schema);
        let field_at = |position: usize| order.as_ref().map_or(position, |o| o[position]);

        let mut values = vec![None; num_fields];
        (0..num_fields)
            .filter(|&position| !bit_is_null(null_bits, position))
            .try_for_each(|position| -> Result<(), CoderError> {
                let index = field_at(position);
                values[index] = Some(decode_field_value(
                    &schema.fields[index].field_type,
                    reader,
                )?);
                Ok(())
            })?;

        Row::new(Arc::clone(schema), values).map_err(CoderError::Schema)
    }

    /// Moves `cursor` past one row encoded with `schema`, with no [`Row`] and no copy of the
    /// field values. Measures a row where the stream has no length prefix for it.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::UnexpectedEof`] if the bytes stop before the end of the row, so a caller
    /// can wait for the next chunk. [`ErrorKind::InvalidData`] if the field count is not the
    /// field count of `schema`, or an element count is negative.
    pub fn skip_row(schema: &Schema, cursor: &mut &[u8]) -> io::Result<()> {
        let null_bits = read_row_header(schema.num_fields(), cursor).map_err(into_io_error)?;
        let order = encoding_order(schema);
        let field_at = |position: usize| order.as_ref().map_or(position, |o| o[position]);
        (0..schema.num_fields())
            .filter(|&position| !bit_is_null(null_bits, position))
            .try_for_each(|position| {
                skip_field_value(&schema.fields[field_at(position)].field_type, cursor)
            })
    }
}

/// Keeps the kind of an I/O error, so that a caller sees `UnexpectedEof`. Other errors are
/// `InvalidData`.
fn into_io_error(error: CoderError) -> IoError {
    match error {
        CoderError::Io(error) => error,
        error @ (CoderError::Utf8(_)
        | CoderError::Schema(_)
        | CoderError::Format(_)
        | CoderError::StateStream(_)) => IoError::new(ErrorKind::InvalidData, error),
    }
}

/// Maps wire position to field index, or returns `None` if the two are the same. A schema can
/// set an `encoding_position` per field so that fields can be reordered, renamed, or appended
/// without changing the wire bytes. Both the null bitmask and the field values use this order.
fn encoding_order(schema: &Schema) -> Option<Vec<usize>> {
    // Custom wire order flags are rare so this branch almost never allocates.
    // Order compute per call supports field updates without cache synchronization.
    schema.encoding_positions_set.then(|| {
        let mut order: Vec<usize> = (0..schema.num_fields()).collect();
        order.sort_by_key(|&index| schema.fields[index].encoding_position.unwrap_or(0));
        order
    })
}

impl Coder<Row> for RowCoder {
    fn urn(&self) -> &'static str {
        URN_ROW
    }

    fn encode(
        &self,
        value: &Row,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        Self::encode_row(value, writer)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<Row, CoderError> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            CoderError::Format("Cannot decode Row without an associated Schema in RowCoder".into())
        })?;
        Self::decode_row(schema, reader)
    }
}

impl DefaultCoder for Row {
    type Coder = RowCoder;

    fn coder() -> Self::Coder {
        RowCoder::default()
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        RowCoder::encode_row(self, writer)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        Self::decode_element_with_context(reader, None, None, None)
    }

    fn decode_element_with_context(
        reader: &mut &[u8],
        schema: Option<&Arc<Schema>>,
        _field_map: Option<&FieldMap>,
        _state_reader: Option<&Arc<dyn crate::coders::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        let schema = schema.ok_or_else(|| {
            CoderError::Format(
                "Row cannot be decoded without a Schema; use decode_with_schema or RowCoder::new(schema)"
                    .into(),
            )
        })?;
        RowCoder::decode_row(schema, reader)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_ROW, vec![])
    }

    fn register_value_coder<R: CoderRegistry + ?Sized>(&self, registry: &R) -> String {
        registry.register_coder_with_payload(URN_ROW, vec![], self.schema().to_proto_bytes())
    }
}
