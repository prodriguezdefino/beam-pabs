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
//! Two logical types do not encode as their representation type: see [`encode_logical_value`].

use std::io::{Read, Write};
use std::sync::Arc;

use crate::coders::URN_ROW;
use crate::coders::standard::{VarIntCoder, read_array, read_be_i32, read_exact_vec};
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use crate::schema::{
    AtomicType, FieldType, FieldValue, Row, Schema, TypeInfo, URN_DECIMAL, URN_MILLIS_INSTANT,
};

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
    pub fn encode_row(row: &Row, writer: &mut dyn Write) -> Result<(), CoderError> {
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
    pub fn decode_row(schema: &Arc<Schema>, reader: &mut dyn Read) -> Result<Row, CoderError> {
        let num_fields = VarIntCoder::decode_varint(reader)? as usize;
        if num_fields != schema.num_fields() {
            return Err(CoderError::Format(format!(
                "Row field count mismatch: expected {}, got {}",
                schema.num_fields(),
                num_fields
            )));
        }

        let bitmask_len = VarIntCoder::decode_varint(reader)? as usize;
        let null_bits = read_exact_vec(reader, bitmask_len)?;

        let is_null = |position: usize| -> bool {
            let byte_index = position / 8;
            byte_index < null_bits.len() && (null_bits[byte_index] & (1 << (position % 8))) != 0
        };

        let order = encoding_order(schema);
        let field_at = |position: usize| order.as_ref().map_or(position, |o| o[position]);

        let mut values = vec![None; num_fields];
        for position in 0..num_fields {
            if !is_null(position) {
                let index = field_at(position);
                values[index] = Some(decode_field_value(
                    &schema.fields[index].field_type,
                    reader,
                )?);
            }
        }

        Row::new(schema.clone(), values).map_err(CoderError::Schema)
    }
}

/// Maps wire position to field index, or returns `None` if the two are the same. A schema can
/// set an `encoding_position` per field so that fields can be reordered, renamed, or appended
/// without changing the wire bytes. Both the null bitmask and the field values use this order.
fn encoding_order(schema: &Schema) -> Option<Vec<usize>> {
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
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        Self::encode_row(value, writer)
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<Row, CoderError> {
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

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        RowCoder::encode_row(self, writer)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        Self::decode_element_with_schema(reader, None)
    }

    fn decode_element_with_schema(
        reader: &mut dyn Read,
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

    fn encode(
        &self,
        value: &T,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        let row = value.to_row()?;
        RowCoder::encode_row(&row, writer)
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<T, CoderError> {
        let row = RowCoder::decode_row(&self.schema, reader)?;
        Ok(T::from_row(&row)?)
    }
}

fn encode_field_value(
    val: &FieldValue,
    field_type: &FieldType,
    writer: &mut dyn Write,
) -> Result<(), CoderError> {
    match (&field_type.type_info, val) {
        (TypeInfo::Atomic(atomic), _) => encode_atomic_value(val, *atomic, writer),
        (TypeInfo::Array(elem_type), FieldValue::Array(elements))
        | (TypeInfo::Iterable(elem_type), FieldValue::Array(elements)) => {
            // Known-length iterable: a 32-bit big-endian element count comes first.
            writer.write_all(&(elements.len() as i32).to_be_bytes())?;
            elements
                .iter()
                .try_for_each(|elem| encode_container_element(elem.as_ref(), elem_type, writer))
        }
        (TypeInfo::Map(k_type, v_type), FieldValue::Map(entries)) => {
            // A 32-bit big-endian entry count comes first.
            writer.write_all(&(entries.len() as i32).to_be_bytes())?;
            entries.iter().try_for_each(|(k, v)| {
                encode_field_value(k, k_type, writer)?;
                encode_container_element(v.as_ref(), v_type, writer)
            })
        }
        (TypeInfo::Row(_), FieldValue::Row(inner_row)) => RowCoder::encode_row(inner_row, writer),
        (
            TypeInfo::Logical {
                urn,
                representation,
                ..
            },
            _,
        ) => encode_logical_value(urn, val, representation, writer),
        (expected, actual) => Err(CoderError::Format(format!(
            "Mismatched field value: expected type info {expected:?}, got value {actual:?}"
        ))),
    }
}

/// Encodes the value of a logical type. A logical type usually encodes as its representation
/// type, so an SDK that does not know the URN can still read the field. All SDKs must encode
/// two portable types as follows:
///
/// - `decimal` is a varint scale and then a length-prefixed big integer. This payload
///   delimits itself, so it has no outer length prefix as a plain `BYTES` value has.
/// - `millis_instant` is a fixed-width big-endian `INT64`, not a varint. The fixed width
///   keeps the byte order the same as the value order.
fn encode_logical_value(
    urn: &str,
    val: &FieldValue,
    representation: &FieldType,
    writer: &mut dyn Write,
) -> Result<(), CoderError> {
    match (urn, val) {
        (URN_DECIMAL, FieldValue::Bytes(payload)) => {
            writer.write_all(payload)?;
            Ok(())
        }
        (URN_MILLIS_INSTANT, FieldValue::Int64(millis)) => {
            writer.write_all(&millis.to_be_bytes())?;
            Ok(())
        }
        _ => encode_field_value(val, representation, writer),
    }
}

/// Encodes one element of an array, iterable or map. A nullable element has a presence byte
/// first, as in `beam:coder:nullable:v1`: 1 means *present*, the opposite polarity of the row
/// null bitmask.
fn encode_container_element(
    val: Option<&FieldValue>,
    elem_type: &FieldType,
    writer: &mut dyn Write,
) -> Result<(), CoderError> {
    if elem_type.nullable {
        match val {
            None => {
                writer.write_all(&[0u8])?;
                Ok(())
            }
            Some(v) => {
                writer.write_all(&[1u8])?;
                encode_field_value(v, elem_type, writer)
            }
        }
    } else {
        let v = val.ok_or_else(|| {
            CoderError::Format("Null element found in non-nullable collection".into())
        })?;
        encode_field_value(v, elem_type, writer)
    }
}

fn encode_atomic_value(
    val: &FieldValue,
    atomic: AtomicType,
    writer: &mut dyn Write,
) -> Result<(), CoderError> {
    match (atomic, val) {
        (AtomicType::Byte, FieldValue::Byte(b)) => {
            writer.write_all(&[*b as u8])?;
            Ok(())
        }
        // INT16 is the only integral type that the row format writes with a fixed width.
        (AtomicType::Int16, FieldValue::Int16(n)) => {
            writer.write_all(&n.to_be_bytes())?;
            Ok(())
        }
        (AtomicType::Int32, FieldValue::Int32(n)) => {
            VarIntCoder::encode_varint(*n as i64, writer)?;
            Ok(())
        }
        (AtomicType::Int64, FieldValue::Int64(n)) => {
            VarIntCoder::encode_varint(*n, writer)?;
            Ok(())
        }
        (AtomicType::Float, FieldValue::Float(f)) => {
            writer.write_all(&f.to_be_bytes())?;
            Ok(())
        }
        (AtomicType::Double, FieldValue::Double(d)) => {
            writer.write_all(&d.to_be_bytes())?;
            Ok(())
        }
        (AtomicType::String, FieldValue::String(s)) => {
            VarIntCoder::encode_varint(s.len() as i64, writer)?;
            writer.write_all(s.as_bytes())?;
            Ok(())
        }
        (AtomicType::Boolean, FieldValue::Boolean(b)) => {
            writer.write_all(&[if *b { 1 } else { 0 }])?;
            Ok(())
        }
        (AtomicType::Bytes, FieldValue::Bytes(bytes)) => {
            VarIntCoder::encode_varint(bytes.len() as i64, writer)?;
            writer.write_all(bytes)?;
            Ok(())
        }
        (expected, actual) => Err(CoderError::Format(format!(
            "Atomic type mismatch: expected {expected:?}, got {actual:?}"
        ))),
    }
}

fn decode_field_value(
    field_type: &FieldType,
    reader: &mut dyn Read,
) -> Result<FieldValue, CoderError> {
    match &field_type.type_info {
        TypeInfo::Atomic(atomic) => decode_atomic_value(*atomic, reader),
        TypeInfo::Array(elem_type) | TypeInfo::Iterable(elem_type) => {
            let count = read_be_i32(reader)?;
            let mut elements = Vec::with_capacity(super::preallocation_for(count));
            for _ in 0..count {
                elements.push(decode_container_element(elem_type, reader)?);
            }
            Ok(FieldValue::Array(elements))
        }
        TypeInfo::Map(k_type, v_type) => {
            let count = read_be_i32(reader)?;
            let mut entries = Vec::with_capacity(super::preallocation_for(count));
            for _ in 0..count {
                let key = decode_field_value(k_type, reader)?;
                let val = decode_container_element(v_type, reader)?;
                entries.push((key, val));
            }
            Ok(FieldValue::Map(entries))
        }
        TypeInfo::Row(inner_schema) => {
            let inner_arc = Arc::new(inner_schema.clone());
            let row = RowCoder::decode_row(&inner_arc, reader)?;
            Ok(FieldValue::Row(row))
        }
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => decode_logical_value(urn, representation, reader),
    }
}

/// Decodes the value of a logical type. This is the inverse of [`encode_logical_value`].
fn decode_logical_value(
    urn: &str,
    representation: &FieldType,
    reader: &mut dyn Read,
) -> Result<FieldValue, CoderError> {
    match urn {
        URN_DECIMAL => decode_decimal(reader),
        URN_MILLIS_INSTANT => {
            let bytes = read_array::<8>(reader)?;
            Ok(FieldValue::Int64(i64::from_be_bytes(bytes)))
        }
        _ => decode_field_value(representation, reader),
    }
}

/// Reads a `decimal` payload into the self-delimiting form that `Decimal::from_field_value`
/// expects.
fn decode_decimal(reader: &mut dyn Read) -> Result<FieldValue, CoderError> {
    let scale = VarIntCoder::decode_varint(reader)?;
    let length = VarIntCoder::decode_varint(reader)?;
    let unscaled = read_exact_vec(
        reader,
        usize::try_from(length)
            .map_err(|_| CoderError::Format(format!("Negative decimal length {length}")))?,
    )?;

    let mut payload = Vec::with_capacity(unscaled.len() + 2);
    VarIntCoder::encode_varint(scale, &mut payload)?;
    VarIntCoder::encode_varint(length, &mut payload)?;
    payload.extend_from_slice(&unscaled);

    Ok(FieldValue::Bytes(payload))
}

fn decode_container_element(
    elem_type: &FieldType,
    reader: &mut dyn Read,
) -> Result<Option<FieldValue>, CoderError> {
    if elem_type.nullable && read_array::<1>(reader)?[0] == 0 {
        return Ok(None);
    }
    decode_field_value(elem_type, reader).map(Some)
}

fn decode_atomic_value(
    atomic: AtomicType,
    reader: &mut dyn Read,
) -> Result<FieldValue, CoderError> {
    match atomic {
        AtomicType::Byte => {
            let [b] = read_array::<1>(reader)?;
            Ok(FieldValue::Byte(b as i8))
        }
        AtomicType::Int16 => {
            let bytes = read_array::<2>(reader)?;
            Ok(FieldValue::Int16(i16::from_be_bytes(bytes)))
        }
        AtomicType::Int32 => {
            let n = VarIntCoder::decode_varint(reader)?;
            Ok(FieldValue::Int32(n as i32))
        }
        AtomicType::Int64 => {
            let n = VarIntCoder::decode_varint(reader)?;
            Ok(FieldValue::Int64(n))
        }
        AtomicType::Float => {
            let bytes = read_array::<4>(reader)?;
            Ok(FieldValue::Float(f32::from_be_bytes(bytes)))
        }
        AtomicType::Double => {
            let bytes = read_array::<8>(reader)?;
            Ok(FieldValue::Double(f64::from_be_bytes(bytes)))
        }
        AtomicType::String => {
            let len = VarIntCoder::decode_varint(reader)? as usize;
            let s = String::from_utf8(read_exact_vec(reader, len)?)?;
            Ok(FieldValue::String(s))
        }
        AtomicType::Boolean => {
            let [b] = read_array::<1>(reader)?;
            Ok(FieldValue::Boolean(b != 0))
        }
        AtomicType::Bytes => {
            let len = VarIntCoder::decode_varint(reader)? as usize;
            Ok(FieldValue::Bytes(read_exact_vec(reader, len)?))
        }
    }
}
