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
mod skip;

pub use element::{RowElementCoder, RowStructCoder};

use std::io::{self, Error as IoError, ErrorKind, Write};
use std::sync::Arc;

use crate::coders::URN_ROW;
use crate::coders::standard::{VarIntCoder, read_exact_vec, read_varint_len};
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use crate::coders::traversal::read_length_prefixed_slice;
use crate::schema::{Row, Schema};
use decode::decode_field_value;
use encode::encode_field_value;
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

        VarIntCoder::encode_varint(num_fields as i64, writer)?;

        let values = row.values();
        if values.iter().any(Option::is_none) {
            let bitmask_bytes = num_fields.div_ceil(8);
            VarIntCoder::encode_varint(bitmask_bytes as i64, writer)?;

            let bytes = (0..num_fields).fold(vec![0u8; bitmask_bytes], |mut bytes, position| {
                if values[field_at(position)].is_none() {
                    bytes[position / 8] |= 1 << (position % 8);
                }
                bytes
            });
            writer.write_all(&bytes)?;
        } else {
            // With no null field, the bitmask is empty.
            VarIntCoder::encode_varint(0, writer)?;
        }

        (0..num_fields)
            .map(field_at)
            .filter_map(|index| values[index].as_ref().map(|v| (index, v)))
            .try_for_each(|(index, field_val)| {
                encode_field_value(field_val, &schema.fields[index].field_type, writer)
            })
    }

    /// Decodes a row with `schema`. Returns an error if the field count does not match.
    pub fn decode_row(schema: &Arc<Schema>, reader: &mut &[u8]) -> Result<Row, CoderError> {
        let num_fields = read_varint_len(reader)?;
        if num_fields != schema.num_fields() {
            return Err(CoderError::Format(format!(
                "Row field count mismatch: expected {}, got {}",
                schema.num_fields(),
                num_fields
            )));
        }

        let bitmask_len = read_varint_len(reader)?;
        let null_bits = read_exact_vec(reader, bitmask_len)?;

        let is_null = |position: usize| -> bool {
            let byte_index = position / 8;
            byte_index < null_bits.len() && (null_bits[byte_index] & (1 << (position % 8))) != 0
        };

        let order = encoding_order(schema);
        let field_at = |position: usize| order.as_ref().map_or(position, |o| o[position]);

        let mut values = vec![None; num_fields];
        (0..num_fields).try_for_each(|position| -> Result<(), CoderError> {
            if !is_null(position) {
                let index = field_at(position);
                values[index] = Some(decode_field_value(
                    &schema.fields[index].field_type,
                    reader,
                )?);
            }
            Ok(())
        })?;

        Row::new(schema.clone(), values).map_err(CoderError::Schema)
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
        let num_fields = VarIntCoder::decode_varint(cursor)?;
        match usize::try_from(num_fields) {
            Ok(count) if count == schema.num_fields() => {}
            _ => {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "Row field count mismatch: expected {}, got {num_fields}",
                        schema.num_fields()
                    ),
                ));
            }
        }
        let null_bits = read_length_prefixed_slice(cursor)?;
        let is_null = |position: usize| {
            null_bits
                .get(position / 8)
                .is_some_and(|byte| byte & (1 << (position % 8)) != 0)
        };
        let order = encoding_order(schema);
        let field_at = |position: usize| order.as_ref().map_or(position, |o| o[position]);
        (0..schema.num_fields())
            .filter(|&position| !is_null(position))
            .try_for_each(|position| {
                skip_field_value(&schema.fields[field_at(position)].field_type, cursor)
            })
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
        Self::decode_element_with_schema(reader, None)
    }

    fn decode_element_with_schema(
        reader: &mut &[u8],
        schema: Option<&Arc<Schema>>,
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
