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

//! Encodes row field values with the field types of the schema.

use std::io::Write;

use super::RowCoder;
use crate::coders::standard::{VarIntCoder, write_count};
use crate::coders::traits::CoderError;
use crate::schema::{AtomicType, FieldType, FieldValue, TypeInfo, URN_DECIMAL, URN_MILLIS_INSTANT};

pub(super) fn encode_field_value(
    val: &FieldValue,
    field_type: &FieldType,
    writer: &mut Vec<u8>,
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
    writer: &mut Vec<u8>,
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
    writer: &mut Vec<u8>,
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
    writer: &mut Vec<u8>,
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
