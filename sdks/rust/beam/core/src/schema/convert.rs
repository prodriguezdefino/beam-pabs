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

//! Type-driven conversion between Rust types and Beam schema values.
//!
//! All elements of a `PCollection` share one schema, so the schema depends on the Rust type
//! only: [`BeamField::beam_field_type`] and [`BeamRow::beam_schema`] take no `self`.
//!
//! # Unmapped types
//!
//! Beam has no unsigned types and no integer wider than 64 bits, so `u8`, `u64`, `i128` and
//! `u128` have no [`BeamField`] implementation: their use is a compile error, not a truncation.
//! So a bare `Vec<u8>` does not compile, and coherence does not let it override the blanket
//! `Vec<T>` implementation. Select the type explicitly:
//!
//! | Intent | Write |
//! |---|---|
//! | `BYTES` | [`bytes::Bytes`], or `Vec<u8>` with `#[beam(bytes)]` |
//! | `ARRAY<BYTE>` | `Vec<i8>` |

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;

use super::error::SchemaError;
use super::row::{FieldValue, Row};
use super::types::{AtomicType, FieldType, Schema, TypeInfo};
use crate::coders::standard::{
    read_array, read_bool, read_bytes, read_count, read_f32, read_f64, read_i8, read_i16,
    read_i32_varint, write_bool, write_bytes, write_count, write_f32, write_f64, write_i8,
    write_i16, write_i32_varint,
};
use crate::coders::{
    CoderError, Context, FieldMap, RowCoder, VarIntCoder, decode_field_value, encode_field_value,
};

/// Marker for types that are not already nullable. Beam nullability is a flag on
/// [`FieldType`], so `Option<Option<T>>` would collapse to one level; the `Option<T>`
/// implementation requires this marker to make that a compile error. Mapped types and
/// `#[derive(BeamRow)]` implement it. Do not implement it by hand.
pub trait NotNullable {}

/// A Rust type with a statically known Beam field type. Values convert to
/// `Option<FieldValue>` because Beam stores nullability in the enclosing slot (see
/// [`Row::values`], [`FieldValue::Array`], [`FieldValue::Map`]), so `Option<T>` uses this trait.
pub trait BeamField: Sized {
    /// True when this Rust type encodes a nullable schema field (`Option<T>`).
    const NULLABLE: bool = false;

    /// Returns the Beam field type for this Rust type. It depends on the type only.
    fn beam_field_type() -> FieldType;

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError>;

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError>;

    /// Returns true when this value is null (`Option::None`).
    fn is_null(&self) -> bool {
        false
    }

    /// Reconstructs a null field value (`None` for `Option<T>`, error otherwise).
    fn decode_null() -> Result<Self, CoderError> {
        Self::from_field_value(None).map_err(CoderError::Schema)
    }

    /// Writes a non-null field payload in `beam:coder:row:v1` format.
    fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        let field_type = Self::beam_field_type();
        let value = self.to_field_value()?.ok_or_else(|| {
            CoderError::Schema(SchemaError::UnexpectedNull {
                expected: field_type.to_string(),
            })
        })?;
        encode_field_value(&value, &field_type, writer)
    }

    /// Reads a non-null field payload in `beam:coder:row:v1` format.
    fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let field_type = Self::beam_field_type();
        let value = decode_field_value(&field_type, reader)?;
        Self::from_field_value(Some(&value)).map_err(CoderError::Schema)
    }

    /// Returns true when `type_info` has the wire layout of `Self` ignoring nullability.
    fn matches_type_info(type_info: &TypeInfo) -> bool {
        *type_info == Self::beam_field_type().type_info
    }

    /// Returns true when `field_type` has the wire layout and nullability of `Self`.
    fn matches_field_type(field_type: &FieldType) -> bool {
        field_type.nullable == Self::NULLABLE && Self::matches_type_info(&field_type.type_info)
    }
}

/// A Rust struct that maps to a Beam [`Row`]. `#[derive(BeamRow)]` also emits a [`BeamField`]
/// implementation so the type can nest. A blanket `impl<T: BeamRow> BeamField for T` would
/// overlap each primitive implementation.
///
/// ```
/// #[derive(beam::schema::BeamRow)]
/// struct Event {
///     #[beam(encoding_position = 1)]
///     id: i64,
///     #[beam(rename = "kind", encoding_position = 0)]
///     name: String,
/// }
///
/// #[derive(beam::schema::BeamRow)]
/// struct Pair<T> {
///     key: String,
///     value: T,
/// }
/// ```
///
/// The derive rejects lifetime and const generic parameters because decoded rows own their
/// values:
///
/// ```compile_fail
/// #[derive(beam::schema::BeamRow)]
/// struct Borrowed<'a> {
///     id: &'a str,
/// }
/// ```
///
/// ```compile_fail
/// #[derive(beam::schema::BeamRow)]
/// struct Fixed<const N: usize> {
///     id: i64,
/// }
/// ```
///
/// It rejects two fields with the same Beam name, also after `rename`:
///
/// ```compile_fail
/// #[derive(beam::schema::BeamRow)]
/// struct Event {
///     id: i64,
///     #[beam(rename = "id")]
///     name: String,
/// }
/// ```
///
/// It rejects an `encoding_position` on some fields only, a duplicate position, and a
/// position outside `0..n`:
///
/// ```compile_fail
/// #[derive(beam::schema::BeamRow)]
/// struct Event {
///     #[beam(encoding_position = 0)]
///     id: i64,
///     name: String,
/// }
/// ```
///
/// ```compile_fail
/// #[derive(beam::schema::BeamRow)]
/// struct Event {
///     #[beam(encoding_position = 0)]
///     id: i64,
///     #[beam(encoding_position = 0)]
///     name: String,
/// }
/// ```
///
/// ```compile_fail
/// #[derive(beam::schema::BeamRow)]
/// struct Event {
///     #[beam(encoding_position = 0)]
///     id: i64,
///     #[beam(encoding_position = 2)]
///     name: String,
/// }
/// ```
pub trait BeamRow: Sized {
    /// Returns the schema for this type. A non-generic derived type builds it once. A generic
    /// derived type builds it, and the schemas of its nested generic types, on each call. So
    /// `to_row` and `beam_field_type` of a generic type allocate a schema for each value.
    fn beam_schema() -> Arc<Schema>;

    fn to_row(&self) -> Result<Row, SchemaError>;

    fn from_row(row: &Row) -> Result<Self, SchemaError>;

    /// Returns true when `schema` has the wire layout of `Self`.
    fn matches_schema(schema: &Schema) -> bool {
        let expected = Self::beam_schema();
        schema.fields == expected.fields
            && schema.encoding_positions_set == expected.encoding_positions_set
    }

    /// Builds a [`FieldMap`] from `writer` to `Self`.
    fn field_map(writer: &Schema) -> Result<FieldMap, SchemaError> {
        if Self::matches_schema(writer) {
            return Ok(FieldMap::Same);
        }
        let expected = Self::beam_schema();
        let indices = expected
            .fields
            .iter()
            .map(|field| match writer.field_index(&field.name) {
                Some(index) => Ok(Some(index)),
                None if field.field_type.nullable => Ok(None),
                None => Err(SchemaError::FieldNotFound(field.name.clone())),
            })
            .collect::<Result<_, _>>()?;
        Ok(FieldMap::ByName(indices))
    }

    /// Converts `row` using writer field indices from [`Self::field_map`].
    fn from_row_mapped(row: &Row, _indices: &[Option<usize>]) -> Result<Self, SchemaError> {
        Self::from_row(row)
    }

    /// Appends the `beam:coder:row:v1` encoding of `self` to `writer`.
    fn encode_row_bytes(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        let row = self.to_row()?;
        RowCoder::encode_row(&row, writer)
    }

    /// Reads `Self` from `beam:coder:row:v1` bytes encoded with the schema of `Self`.
    fn decode_row_bytes(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let row = RowCoder::decode_row(&Self::beam_schema(), reader)?;
        Self::from_row(&row).map_err(CoderError::Schema)
    }
}

/// Builds the `Some`/`None`/mismatch arms shared by every implementation.
macro_rules! from_field_value_arms {
    ($value:expr, $variant:ident, $name:literal, $extract:expr) => {
        match $value {
            Some(FieldValue::$variant(v)) => $extract(v),
            Some(other) => Err(SchemaError::ValueTypeMismatch {
                expected: $name.to_string(),
                actual: format!("{other:?}"),
            }),
            None => Err(SchemaError::UnexpectedNull {
                expected: $name.to_string(),
            }),
        }
    };
}

/// Implements [`BeamField`] for a leaf type that maps to a `FieldValue` variant with no
/// conversion.
macro_rules! beam_field_copy {
    ($rust:ty, $variant:ident, $atomic:ident, $ctor:ident, $name:literal, $enc:expr, $dec:expr) => {
        impl NotNullable for $rust {}

        impl BeamField for $rust {
            fn beam_field_type() -> FieldType {
                FieldType::$ctor()
            }

            fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
                Ok(Some(FieldValue::$variant(*self)))
            }

            fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
                from_field_value_arms!(value, $variant, $name, |v: &$rust| Ok(*v))
            }

            fn decode_null() -> Result<Self, CoderError> {
                Err(CoderError::Schema(SchemaError::UnexpectedNull {
                    expected: $name.to_string(),
                }))
            }

            fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
                ($enc)(*self, writer)
            }

            fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
                ($dec)(reader)
            }

            fn matches_type_info(type_info: &TypeInfo) -> bool {
                matches!(type_info, TypeInfo::Atomic(AtomicType::$atomic))
            }
        }
    };
}

/// Implements [`BeamField`] for an unsigned type that widens losslessly into a larger signed
/// Beam type. Decoding narrows with a check: a value from another SDK that does not fit returns
/// [`SchemaError::ValueOutOfRange`] and does not wrap.
macro_rules! beam_field_widening {
    ($rust:ty, $variant:ident, $wide:ty, $atomic:ident, $ctor:ident, $name:literal) => {
        impl NotNullable for $rust {}

        impl BeamField for $rust {
            fn beam_field_type() -> FieldType {
                FieldType::$ctor()
            }

            fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
                Ok(Some(FieldValue::$variant(<$wide>::from(*self))))
            }

            fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
                from_field_value_arms!(value, $variant, $name, |v: &$wide| {
                    <$rust>::try_from(*v).map_err(|_| SchemaError::ValueOutOfRange {
                        value: v.to_string(),
                        target: stringify!($rust).to_string(),
                    })
                })
            }

            fn decode_null() -> Result<Self, CoderError> {
                Err(CoderError::Schema(SchemaError::UnexpectedNull {
                    expected: $name.to_string(),
                }))
            }

            fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
                VarIntCoder::encode_varint_to_vec(i64::from(*self), writer);
                Ok(())
            }

            fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
                let wide = <$wide as BeamField>::decode_field(reader)?;
                <$rust>::try_from(wide).map_err(|_| {
                    CoderError::Schema(SchemaError::ValueOutOfRange {
                        value: wide.to_string(),
                        target: stringify!($rust).to_string(),
                    })
                })
            }

            fn matches_type_info(type_info: &TypeInfo) -> bool {
                matches!(type_info, TypeInfo::Atomic(AtomicType::$atomic))
            }
        }
    };
}

beam_field_copy!(
    bool,
    Boolean,
    Boolean,
    boolean,
    "BOOLEAN",
    |v: bool, w: &mut Vec<u8>| {
        write_bool(v, w);
        Ok(())
    },
    read_bool
);
beam_field_copy!(
    i8,
    Byte,
    Byte,
    byte,
    "BYTE",
    |v: i8, w: &mut Vec<u8>| {
        write_i8(v, w);
        Ok(())
    },
    |r: &mut &[u8]| Ok(read_i8(r)?)
);
beam_field_copy!(
    i16,
    Int16,
    Int16,
    int16,
    "INT16",
    |v: i16, w: &mut Vec<u8>| {
        write_i16(v, w);
        Ok(())
    },
    |r: &mut &[u8]| Ok(read_i16(r)?)
);
beam_field_copy!(
    i32,
    Int32,
    Int32,
    int32,
    "INT32",
    |v: i32, w: &mut Vec<u8>| {
        write_i32_varint(v, w);
        Ok(())
    },
    read_i32_varint
);
beam_field_copy!(
    i64,
    Int64,
    Int64,
    int64,
    "INT64",
    |v: i64, w: &mut Vec<u8>| {
        VarIntCoder::encode_varint_to_vec(v, w);
        Ok(())
    },
    |r: &mut &[u8]| Ok(VarIntCoder::decode_varint(r)?)
);
beam_field_copy!(
    f32,
    Float,
    Float,
    float,
    "FLOAT",
    |v: f32, w: &mut Vec<u8>| {
        write_f32(v, w);
        Ok(())
    },
    |r: &mut &[u8]| Ok(read_f32(r)?)
);
beam_field_copy!(
    f64,
    Double,
    Double,
    double,
    "DOUBLE",
    |v: f64, w: &mut Vec<u8>| {
        write_f64(v, w);
        Ok(())
    },
    |r: &mut &[u8]| Ok(read_f64(r)?)
);

beam_field_widening!(u16, Int32, i32, Int32, int32, "INT32");
beam_field_widening!(u32, Int64, i64, Int64, int64, "INT64");

impl NotNullable for String {}

impl BeamField for String {
    fn beam_field_type() -> FieldType {
        FieldType::string()
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        Ok(Some(FieldValue::String(self.clone())))
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        from_field_value_arms!(value, String, "STRING", |v: &Self| Ok(v.clone()))
    }

    fn decode_null() -> Result<Self, CoderError> {
        Err(CoderError::Schema(SchemaError::UnexpectedNull {
            expected: "STRING".to_string(),
        }))
    }

    fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        write_bytes(self.as_bytes(), writer, Context::Nested)
    }

    fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
        Ok(Self::from_utf8(read_bytes(reader, Context::Nested)?)?)
    }

    fn matches_type_info(type_info: &TypeInfo) -> bool {
        matches!(type_info, TypeInfo::Atomic(AtomicType::String))
    }
}

impl NotNullable for Bytes {}

impl BeamField for Bytes {
    fn beam_field_type() -> FieldType {
        FieldType::bytes()
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        Ok(Some(FieldValue::Bytes(self.to_vec())))
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        from_field_value_arms!(value, Bytes, "BYTES", |v: &Vec<u8>| Ok(
            Self::copy_from_slice(v)
        ))
    }

    fn decode_null() -> Result<Self, CoderError> {
        Err(CoderError::Schema(SchemaError::UnexpectedNull {
            expected: "BYTES".to_string(),
        }))
    }

    fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        write_bytes(self, writer, Context::Nested)
    }

    fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
        read_bytes(reader, Context::Nested).map(Self::from)
    }

    fn matches_type_info(type_info: &TypeInfo) -> bool {
        matches!(type_info, TypeInfo::Atomic(AtomicType::Bytes))
    }
}

/// `Option<T>` sets the nullable flag on `T`'s field type. [`NotNullable`] rejects
/// `Option<Option<T>>`.
impl<T: BeamField + NotNullable> BeamField for Option<T> {
    const NULLABLE: bool = true;

    fn beam_field_type() -> FieldType {
        T::beam_field_type().with_nullable(true)
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        self.as_ref().map_or(Ok(None), T::to_field_value)
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        value.map(|v| T::from_field_value(Some(v))).transpose()
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }

    fn decode_null() -> Result<Self, CoderError> {
        Ok(None)
    }

    fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        match self {
            Some(value) => value.encode_field(writer),
            None => Err(CoderError::Schema(SchemaError::UnexpectedNull {
                expected: T::beam_field_type().to_string(),
            })),
        }
    }

    fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
        T::decode_field(reader).map(Some)
    }

    fn matches_type_info(type_info: &TypeInfo) -> bool {
        T::matches_type_info(type_info)
    }
}

fn encode_container_item<T: BeamField>(item: &T, writer: &mut Vec<u8>) -> Result<(), CoderError> {
    if T::NULLABLE {
        if item.is_null() {
            writer.push(0);
            Ok(())
        } else {
            writer.push(1);
            item.encode_field(writer)
        }
    } else {
        item.encode_field(writer)
    }
}

fn decode_container_item<T: BeamField>(reader: &mut &[u8]) -> Result<T, CoderError> {
    if T::NULLABLE {
        match read_array::<1>(reader)? {
            [0] => T::decode_null(),
            [1] => T::decode_field(reader),
            [other] => Err(CoderError::Format(format!(
                "Invalid nullable presence byte: {other}"
            ))),
        }
    } else {
        T::decode_field(reader)
    }
}

impl<T: BeamField> NotNullable for Vec<T> {}

impl<T: BeamField> BeamField for Vec<T> {
    fn beam_field_type() -> FieldType {
        FieldType::array(T::beam_field_type())
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        self.iter()
            .map(T::to_field_value)
            .collect::<Result<Vec<_>, _>>()
            .map(|items| Some(FieldValue::Array(items)))
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        from_field_value_arms!(value, Array, "ARRAY", |items: &Vec<Option<FieldValue>>| {
            items
                .iter()
                .map(|item| T::from_field_value(item.as_ref()))
                .collect()
        })
    }

    fn decode_null() -> Result<Self, CoderError> {
        Err(CoderError::Schema(SchemaError::UnexpectedNull {
            expected: "ARRAY".to_string(),
        }))
    }

    fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        write_count(self.len(), writer)?;
        self.iter()
            .try_for_each(|item| encode_container_item(item, writer))
    }

    fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let count = read_count(reader)?;
        (0..count)
            .map(|_| decode_container_item::<T>(reader))
            .collect()
    }

    fn matches_type_info(type_info: &TypeInfo) -> bool {
        matches!(
            type_info,
            TypeInfo::Array(elem) | TypeInfo::Iterable(elem) if T::matches_field_type(elem)
        )
    }
}

/// Implements [`BeamField`] for `HashMap` and `BTreeMap`, which differ only in the bounds on `K`.
macro_rules! beam_field_map {
    ($map:ident, $($key_bound:tt)+) => {
        impl<K: BeamField + $($key_bound)+, V: BeamField> NotNullable for $map<K, V> {}

        impl<K: BeamField + $($key_bound)+, V: BeamField> BeamField for $map<K, V> {
            fn beam_field_type() -> FieldType {
                FieldType::map(K::beam_field_type(), V::beam_field_type())
            }

            fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
                self.iter()
                    .map(|(key, value)| {
                        let key = key.to_field_value()?.ok_or(SchemaError::NullMapKey)?;
                        Ok((key, value.to_field_value()?))
                    })
                    .collect::<Result<Vec<_>, SchemaError>>()
                    .map(|entries| Some(FieldValue::Map(entries)))
            }

            fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
                from_field_value_arms!(
                    value,
                    Map,
                    "MAP",
                    |entries: &Vec<(FieldValue, Option<FieldValue>)>| {
                        entries
                            .iter()
                            .map(|(key, value)| {
                                Ok((
                                    K::from_field_value(Some(key))?,
                                    V::from_field_value(value.as_ref())?,
                                ))
                            })
                            .collect()
                    }
                )
            }

            fn decode_null() -> Result<Self, CoderError> {
                Err(CoderError::Schema(SchemaError::UnexpectedNull {
                    expected: "MAP".to_string(),
                }))
            }

            fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
                write_count(self.len(), writer)?;
                self.iter().try_for_each(|(key, value)| {
                    if key.is_null() {
                        return Err(CoderError::Schema(SchemaError::NullMapKey));
                    }
                    key.encode_field(writer)?;
                    encode_container_item(value, writer)
                })
            }

            fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
                let count = read_count(reader)?;
                (0..count)
                    .map(|_| {
                        let key = K::decode_field(reader)?;
                        let value = decode_container_item::<V>(reader)?;
                        Ok((key, value))
                    })
                    .collect()
            }

            fn matches_type_info(type_info: &TypeInfo) -> bool {
                matches!(
                    type_info,
                    TypeInfo::Map(k, v) if K::matches_field_type(k) && V::matches_field_type(v)
                )
            }
        }
    };
}

beam_field_map!(HashMap, Eq + Hash);
beam_field_map!(BTreeMap, Ord);

/// Support functions for code that `#[derive(BeamRow)]` and `#[derive(BeamEnum)]` generate.
/// Not part of the stable public API: it changes together with the derive macros.
#[doc(hidden)]
pub mod derive_support {
    use super::{AtomicType, CoderError, Context, FieldType, FieldValue, SchemaError, TypeInfo};
    pub use crate::coders::row::header::{bit_is_null, read_row_header, write_row_header};
    use crate::coders::standard::{read_bytes, read_nested_bytes, write_bytes};

    /// Field type for a `Vec<u8>` field annotated `#[beam(bytes)]`. `Vec<u8>` has no
    /// [`BeamField`](super::BeamField) implementation (see the module docs).
    pub fn bytes_field_type() -> FieldType {
        FieldType::bytes()
    }

    pub fn bytes_matches_field_type(field_type: &FieldType) -> bool {
        !field_type.nullable && matches!(field_type.type_info, TypeInfo::Atomic(AtomicType::Bytes))
    }

    pub fn bytes_to_field_value(value: &[u8]) -> Result<Option<FieldValue>, SchemaError> {
        Ok(Some(FieldValue::Bytes(value.to_vec())))
    }

    pub fn bytes_from_field_value(value: Option<&FieldValue>) -> Result<Vec<u8>, SchemaError> {
        from_field_value_arms!(value, Bytes, "BYTES", |v: &Vec<u8>| Ok(v.clone()))
    }

    pub fn bytes_encode_field(value: &[u8], writer: &mut Vec<u8>) -> Result<(), CoderError> {
        write_bytes(value, writer, Context::Nested)
    }

    pub fn bytes_decode_field(reader: &mut &[u8]) -> Result<Vec<u8>, CoderError> {
        read_bytes(reader, Context::Nested)
    }

    pub fn bytes_decode_null() -> Result<Vec<u8>, CoderError> {
        Err(CoderError::Schema(SchemaError::UnexpectedNull {
            expected: "BYTES".to_string(),
        }))
    }

    pub fn encode_str_field(label: &str, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        write_bytes(label.as_bytes(), writer, Context::Nested)
    }

    pub fn read_str_field<'a>(reader: &mut &'a [u8]) -> Result<&'a str, CoderError> {
        std::str::from_utf8(read_nested_bytes(reader)?)
            .map_err(|e| CoderError::Format(format!("Invalid UTF-8 in a STRING field: {e}")))
    }

    /// Returns the value at `index` of a row.
    pub fn row_slot(
        values: &[Option<FieldValue>],
        index: usize,
    ) -> Result<Option<&FieldValue>, SchemaError> {
        values
            .get(index)
            .map(Option::as_ref)
            .ok_or(SchemaError::IndexOutOfBounds(index, values.len()))
    }

    /// Returns the value of the field `name` at `writer_index` of a row. A field that the row
    /// does not have is null if `nullable`, and an error if not.
    pub fn mapped_slot<'a>(
        values: &'a [Option<FieldValue>],
        writer_index: Option<usize>,
        nullable: bool,
        name: &str,
    ) -> Result<Option<&'a FieldValue>, SchemaError> {
        match writer_index {
            Some(index) => row_slot(values, index),
            None if nullable => Ok(None),
            None => Err(SchemaError::FieldNotFound(name.to_string())),
        }
    }
}
