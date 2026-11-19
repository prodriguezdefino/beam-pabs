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

//! Moves a cursor past encoded row field values without decoding them.

use std::io::{self, Error as IoError, ErrorKind};
use std::sync::Arc;

use super::RowCoder;
use crate::coders::standard::{VarIntCoder, read_array, read_be_i32, take_bytes};
use crate::coders::traversal::{skip_iterable_elements, skip_length_prefixed};
use crate::schema::{AtomicType, FieldType, Schema, TypeInfo, URN_DECIMAL, URN_MILLIS_INSTANT};

/// Wire primitive that encodes one row field value, after resolving logical types.
pub(crate) enum FieldWire<'a> {
    Fixed(usize),
    VarInt,
    LengthPrefixed,
    Decimal,
    Iterable(&'a FieldType),
    Map(&'a FieldType, &'a FieldType),
    Row(&'a Arc<Schema>),
}

/// Maps `field_type` to the wire primitive that encodes its value. Both [`skip_field_value`]
/// and [`SkipPlan`](crate::coders::SkipPlan) use this mapping so row wire rules stay in one
/// place.
pub(crate) fn field_wire(field_type: &FieldType) -> FieldWire<'_> {
    match &field_type.type_info {
        TypeInfo::Atomic(atomic) => match atomic {
            AtomicType::Byte | AtomicType::Boolean => FieldWire::Fixed(1),
            AtomicType::Int16 => FieldWire::Fixed(2),
            AtomicType::Float => FieldWire::Fixed(4),
            AtomicType::Double => FieldWire::Fixed(8),
            AtomicType::Int32 | AtomicType::Int64 => FieldWire::VarInt,
            AtomicType::String | AtomicType::Bytes => FieldWire::LengthPrefixed,
        },
        TypeInfo::Array(elem_type) | TypeInfo::Iterable(elem_type) => {
            FieldWire::Iterable(elem_type)
        }
        TypeInfo::Map(k_type, v_type) => FieldWire::Map(k_type, v_type),
        TypeInfo::Row(inner_schema) => FieldWire::Row(inner_schema),
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => match urn.as_str() {
            URN_DECIMAL => FieldWire::Decimal,
            URN_MILLIS_INSTANT => FieldWire::Fixed(8),
            _ => field_wire(representation),
        },
    }
}

/// Moves `cursor` past one value of `field_type`. This is the inverse of the length that
/// [`encode_field_value`](super::encode::encode_field_value) writes.
pub(crate) fn skip_field_value(field_type: &FieldType, cursor: &mut &[u8]) -> io::Result<()> {
    match field_wire(field_type) {
        FieldWire::Fixed(len) => skip_bytes(cursor, len),
        FieldWire::VarInt => VarIntCoder::decode_varint(cursor).map(|_| ()),
        FieldWire::LengthPrefixed => skip_length_prefixed(cursor),
        FieldWire::Decimal => {
            VarIntCoder::decode_varint(cursor)?;
            skip_length_prefixed(cursor)
        }
        FieldWire::Iterable(elem_type) => {
            skip_iterable_elements(cursor, |c| skip_container_element(elem_type, c))
        }
        FieldWire::Map(k_type, v_type) => {
            let count = read_element_count(cursor)?;
            (0..count).try_for_each(|_| {
                skip_field_value(k_type, cursor)?;
                skip_container_element(v_type, cursor)
            })
        }
        FieldWire::Row(inner_schema) => RowCoder::skip_row(inner_schema, cursor),
    }
}

/// Moves `cursor` past one element of an array, iterable or map. A nullable element has a
/// presence byte first: see
/// [`encode_container_element`](super::encode::encode_container_element).
fn skip_container_element(elem_type: &FieldType, cursor: &mut &[u8]) -> io::Result<()> {
    if elem_type.nullable {
        match read_array::<1>(cursor)? {
            [0] => Ok(()),
            [1] => skip_field_value(elem_type, cursor),
            [b] => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("Invalid nullable presence byte: {b}"),
            )),
        }
    } else {
        skip_field_value(elem_type, cursor)
    }
}

/// Moves `cursor` forward by `len` bytes, or fails with `UnexpectedEof` if fewer remain.
fn skip_bytes(cursor: &mut &[u8], len: usize) -> io::Result<()> {
    take_bytes(cursor, len)
        .map(|_| ())
        .map_err(|_| IoError::new(ErrorKind::UnexpectedEof, "the row ends before its field"))
}

/// Reads the 32-bit big-endian element count of an array or map.
fn read_element_count(cursor: &mut &[u8]) -> io::Result<usize> {
    let count = read_be_i32(cursor)?;
    usize::try_from(count).map_err(|_| {
        IoError::new(
            ErrorKind::InvalidData,
            format!("Negative element count: {count}"),
        )
    })
}
