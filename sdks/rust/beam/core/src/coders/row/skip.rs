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

use super::RowCoder;
use crate::coders::standard::{VarIntCoder, read_array, read_be_i32, take_bytes};
use crate::coders::traversal::skip_length_prefixed;
use crate::schema::{AtomicType, FieldType, TypeInfo, URN_DECIMAL, URN_MILLIS_INSTANT};

/// Moves `cursor` past one value of `field_type`. This is the inverse of the length that
/// [`encode_field_value`](super::encode::encode_field_value) writes.
pub(super) fn skip_field_value(field_type: &FieldType, cursor: &mut &[u8]) -> io::Result<()> {
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

fn skip_atomic_value(atomic: AtomicType, cursor: &mut &[u8]) -> io::Result<()> {
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
