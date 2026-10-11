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

//! Steps over the row coder bytes of a value that a projection does not convert.

use beam::schema::{
    AtomicType, FieldType, Schema, SchemaError, TypeInfo, URN_DATE, URN_DECIMAL,
    URN_MICROS_INSTANT, URN_MILLIS_INSTANT,
};

use super::columns::encoding_order;
use super::encoded::{read_count, read_length, read_varint, take};
use crate::error::Result;

/// The layout of an encoded value. [`Skip::new`] gives the layout that `read_value` in
/// `encoded.rs` reads for the same type, so a skipped value takes the same bytes.
#[derive(Debug)]
pub(super) enum Skip {
    /// A fixed number of bytes.
    Fixed(usize),
    Varint,
    /// A varint length, then that number of bytes.
    LengthPrefixed,
    /// A varint scale, then a length-prefixed unscaled value.
    Decimal,
    /// A 32-bit count, then the elements. A nullable element starts with a presence byte.
    List {
        nullable: bool,
        items: Box<Skip>,
    },
    /// A 32-bit count, then a key and a value for each entry. Keys are never null.
    Map {
        keys: Box<Skip>,
        nullable: bool,
        values: Box<Skip>,
    },
    /// A row: the field count, the null bitmask, then the values that are not null. The
    /// fields are in encoding order.
    Row(Vec<Skip>),
}

impl Skip {
    pub(super) fn new(field_type: &FieldType) -> Self {
        match &field_type.type_info {
            TypeInfo::Atomic(atomic) => match atomic {
                AtomicType::Byte | AtomicType::Boolean => Self::Fixed(1),
                AtomicType::Int16 => Self::Fixed(2),
                AtomicType::Float => Self::Fixed(4),
                AtomicType::Double => Self::Fixed(8),
                AtomicType::Int32 | AtomicType::Int64 => Self::Varint,
                AtomicType::String | AtomicType::Bytes => Self::LengthPrefixed,
            },
            TypeInfo::Array(item) | TypeInfo::Iterable(item) => Self::List {
                nullable: item.nullable,
                items: Box::new(Self::new(item)),
            },
            TypeInfo::Map(key, value) => Self::Map {
                keys: Box::new(Self::new(key)),
                nullable: value.nullable,
                values: Box::new(Self::new(value)),
            },
            TypeInfo::Row(schema) => Self::row(schema),
            TypeInfo::Logical {
                urn,
                representation,
                ..
            } => match urn.as_str() {
                // `read_value` reads a row of two varints for any representation.
                URN_MICROS_INSTANT => Self::Row(vec![Self::Varint, Self::Varint]),
                URN_MILLIS_INSTANT => Self::Fixed(8),
                URN_DATE => Self::Varint,
                URN_DECIMAL => Self::Decimal,
                _ => Self::new(representation),
            },
        }
    }

    fn row(schema: &Schema) -> Self {
        let field = |index: usize| Self::new(&schema.fields[index].field_type);
        Self::Row(match encoding_order(schema) {
            Some(order) => order.into_iter().map(field).collect(),
            None => (0..schema.fields.len()).map(field).collect(),
        })
    }

    /// Moves `input` past one value that is not null. `name` names the field in an error.
    pub(super) fn skip(&self, input: &mut &[u8], name: &str) -> Result<()> {
        match self {
            Self::Fixed(length) => {
                take(input, *length, name)?;
            }
            Self::Varint => {
                read_varint(input, name)?;
            }
            Self::LengthPrefixed => {
                let length = read_length(input, name)?;
                take(input, length, name)?;
            }
            Self::Decimal => {
                read_varint(input, name)?;
                let length = read_length(input, name)?;
                take(input, length, name)?;
            }
            Self::List { nullable, items } => {
                let count = read_count(input, name)?;
                (0..count).try_for_each(|_| skip_element(*nullable, items, input, name))?;
            }
            Self::Map {
                keys,
                nullable,
                values,
            } => {
                let count = read_count(input, name)?;
                (0..count).try_for_each(|_| {
                    keys.skip(input, name)?;
                    skip_element(*nullable, values, input, name)
                })?;
            }
            Self::Row(fields) => {
                let count = read_varint(input, name)?;
                if usize::try_from(count).ok() != Some(fields.len()) {
                    return Err(SchemaError::ValueCountMismatch {
                        expected: fields.len(),
                        actual: usize::try_from(count).unwrap_or_default(),
                    }
                    .into());
                }
                let bitmask = read_length(input, name)?;
                let nulls = take(input, bitmask, name)?;
                fields
                    .iter()
                    .enumerate()
                    .filter(|(position, _)| {
                        !nulls
                            .get(position / 8)
                            .is_some_and(|byte| byte & (1 << (position % 8)) != 0)
                    })
                    .try_for_each(|(_, field)| field.skip(input, name))?;
            }
        }
        Ok(())
    }
}

/// Steps over an element of a list or a map value. A nullable element starts with a presence
/// byte: 1 means present.
fn skip_element(nullable: bool, skip: &Skip, input: &mut &[u8], name: &str) -> Result<()> {
    if nullable && take(input, 1, name)?[0] == 0 {
        return Ok(());
    }
    skip.skip(input, name)
}
