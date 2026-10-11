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

use std::io::{self, Cursor, Error as IoError, ErrorKind, Read, Write};
use std::sync::Arc;

use crate::coders::URN_ROW;
use crate::coders::standard::{
    VarIntCoder, read_array, read_be_i32, read_count, read_exact_vec, read_varint_len, write_count,
};
use crate::coders::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use crate::coders::traversal::{read_length_prefixed_slice, skip_length_prefixed};
use crate::schema::{
    AtomicType, FieldType, FieldValue, Row, RowElement, Schema, TypeInfo, URN_DECIMAL,
    URN_MILLIS_INSTANT,
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
    pub fn skip_row(schema: &Schema, cursor: &mut Cursor<&[u8]>) -> io::Result<()> {
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

/// Moves `cursor` past one value of `field_type`. This is the inverse of the length that
/// [`encode_field_value`] writes.
fn skip_field_value(field_type: &FieldType, cursor: &mut Cursor<&[u8]>) -> io::Result<()> {
    match &field_type.type_info {
        TypeInfo::Atomic(atomic) => skip_atomic_value(*atomic, cursor),
        TypeInfo::Array(elem_type) | TypeInfo::Iterable(elem_type) => {
            let count = read_element_count(cursor)?;
            (0..count).try_for_each(|_| skip_container_element(elem_type, cursor))
        }
        TypeInfo::Map(k_type, v_type) => {
            let count = read_element_count(cursor)?;
            (0..count).try_for_each(|_| {
                skip_field_value(k_type, cursor)?;
                skip_container_element(v_type, cursor)
            })
        }
        TypeInfo::Row(inner_schema) => RowCoder::skip_row(inner_schema, cursor),
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => match urn.as_str() {
            URN_DECIMAL => {
                VarIntCoder::decode_varint(cursor)?;
                skip_length_prefixed(cursor)
            }
            URN_MILLIS_INSTANT => skip_bytes(cursor, 8),
            _ => skip_field_value(representation, cursor),
        },
    }
}

/// Moves `cursor` past one element of an array, iterable or map. A nullable element has a
/// presence byte first: see [`encode_container_element`].
fn skip_container_element(elem_type: &FieldType, cursor: &mut Cursor<&[u8]>) -> io::Result<()> {
    match elem_type.nullable {
        true => match read_array::<1>(cursor)? {
            [0] => Ok(()),
            [1] => skip_field_value(elem_type, cursor),
            [b] => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("Invalid nullable presence byte: {b}"),
            )),
        },
        false => skip_field_value(elem_type, cursor),
    }
}

fn skip_atomic_value(atomic: AtomicType, cursor: &mut Cursor<&[u8]>) -> io::Result<()> {
    match atomic {
        AtomicType::Byte | AtomicType::Boolean => skip_bytes(cursor, 1),
        AtomicType::Int16 => skip_bytes(cursor, 2),
        AtomicType::Float => skip_bytes(cursor, 4),
        AtomicType::Double => skip_bytes(cursor, 8),
        AtomicType::Int32 | AtomicType::Int64 => VarIntCoder::decode_varint(cursor).map(|_| ()),
        AtomicType::String | AtomicType::Bytes => skip_length_prefixed(cursor),
    }
}

/// Moves `cursor` forward by `len` bytes, or fails with `UnexpectedEof` if fewer remain.
fn skip_bytes(cursor: &mut Cursor<&[u8]>, len: usize) -> io::Result<()> {
    let end = usize::try_from(cursor.position())
        .ok()
        .and_then(|position| position.checked_add(len))
        .filter(|&end| end <= cursor.get_ref().len())
        .ok_or_else(|| IoError::new(ErrorKind::UnexpectedEof, "the row ends before its field"))?;
    cursor.set_position(end as u64);
    Ok(())
}

/// Reads the 32-bit big-endian element count of an array or map.
fn read_element_count(cursor: &mut Cursor<&[u8]>) -> io::Result<usize> {
    let count = read_be_i32(cursor)?;
    usize::try_from(count).map_err(|_| {
        IoError::new(
            ErrorKind::InvalidData,
            format!("Negative element count: {count}"),
        )
    })
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

    fn register_value_coder<R: CoderRegistry + ?Sized>(&self, registry: &R) -> String {
        registry.register_coder_with_payload(URN_ROW, vec![], self.schema().to_proto_bytes())
    }
}

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
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<RowElement, CoderError> {
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

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        match self {
            Self::Encoded(bytes) => Ok(writer.write_all(bytes)?),
            Self::Row(row) => RowCoder::encode_row(row, writer),
        }
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        Self::decode_element_with_schema(reader, None)
    }

    /// Decodes a row inside another value, such as a key-value pair, into a [`Row`].
    fn decode_element_with_schema(
        reader: &mut dyn Read,
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
        _state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
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
            write_count(elements.len(), writer)?;
            elements
                .iter()
                .try_for_each(|elem| encode_container_element(elem.as_ref(), elem_type, writer))
        }
        (TypeInfo::Map(k_type, v_type), FieldValue::Map(entries)) => {
            // A 32-bit big-endian entry count comes first.
            write_count(entries.len(), writer)?;
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
            // A negative count would decode an empty array and misalign the reader.
            let count = read_count(reader)?;
            let elements = (0..count)
                .map(|_| decode_container_element(elem_type, reader))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FieldValue::Array(elements))
        }
        TypeInfo::Map(k_type, v_type) => {
            let count = read_count(reader)?;
            let entries = (0..count)
                .map(|_| {
                    let key = decode_field_value(k_type, reader)?;
                    Ok((key, decode_container_element(v_type, reader)?))
                })
                .collect::<Result<Vec<_>, CoderError>>()?;
            Ok(FieldValue::Map(entries))
        }
        TypeInfo::Row(inner_schema) => {
            let row = RowCoder::decode_row(inner_schema, reader)?;
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
    let len = usize::try_from(length)
        .map_err(|_| CoderError::Format(format!("Negative decimal length {length}")))?;

    // Encode varints first and stream bytes into payload to avoid double allocation.
    let mut payload = Vec::with_capacity(len.min(64 * 1024) + 20);
    VarIntCoder::encode_varint(scale, &mut payload)?;
    VarIntCoder::encode_varint(length, &mut payload)?;
    let prefix_len = payload.len();
    Read::take(&mut *reader, len as u64).read_to_end(&mut payload)?;
    if payload.len() - prefix_len < len {
        return Err(CoderError::Io(IoError::new(
            ErrorKind::UnexpectedEof,
            format!(
                "expected {len} decimal bytes, stream ended after {}",
                payload.len() - prefix_len
            ),
        )));
    }

    Ok(FieldValue::Bytes(payload))
}

fn decode_container_element(
    elem_type: &FieldType,
    reader: &mut dyn Read,
) -> Result<Option<FieldValue>, CoderError> {
    if elem_type.nullable {
        let [b] = read_array::<1>(reader)?;
        match b {
            0 => return Ok(None),
            1 => {}
            other => {
                return Err(CoderError::Format(format!(
                    "Invalid nullable presence byte: {other}"
                )));
            }
        }
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
            i32::try_from(n)
                .map(FieldValue::Int32)
                .map_err(|_| CoderError::Format(format!("Int32 field value {n} is out of range")))
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
            let len = read_varint_len(reader)?;
            let s = String::from_utf8(read_exact_vec(reader, len)?)?;
            Ok(FieldValue::String(s))
        }
        AtomicType::Boolean => {
            let [b] = read_array::<1>(reader)?;
            match b {
                0 => Ok(FieldValue::Boolean(false)),
                1 => Ok(FieldValue::Boolean(true)),
                other => Err(CoderError::Format(format!("Invalid boolean byte: {other}"))),
            }
        }
        AtomicType::Bytes => {
            let len = read_varint_len(reader)?;
            Ok(FieldValue::Bytes(read_exact_vec(reader, len)?))
        }
    }
}
