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

//! Decodes row field values with the field types of the schema.

use std::io::{Error as IoError, ErrorKind, Read};

use super::RowCoder;
use crate::coders::standard::{
    VarIntCoder, read_array, read_count, read_exact_vec, read_varint_len,
};
use crate::coders::traits::CoderError;
use crate::schema::{AtomicType, FieldType, FieldValue, TypeInfo, URN_DECIMAL, URN_MILLIS_INSTANT};

pub(super) fn decode_field_value(
    field_type: &FieldType,
    reader: &mut &[u8],
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

/// Decodes the value of a logical type. This is the inverse of
/// [`encode_logical_value`](super::encode::encode_logical_value).
fn decode_logical_value(
    urn: &str,
    representation: &FieldType,
    reader: &mut &[u8],
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
fn decode_decimal(reader: &mut &[u8]) -> Result<FieldValue, CoderError> {
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
    reader: &mut &[u8],
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

fn decode_atomic_value(atomic: AtomicType, reader: &mut &[u8]) -> Result<FieldValue, CoderError> {
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
