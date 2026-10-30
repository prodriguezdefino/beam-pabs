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

//! Wire primitives of encoded row field values.

use std::io;
use std::sync::Arc;

use crate::coders::traversal::skip_row_field;
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

/// Maps `field_type` to the wire primitive that encodes its value.
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
    skip_row_field(field_type, cursor)
}
