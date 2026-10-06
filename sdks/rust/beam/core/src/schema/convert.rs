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
use super::types::{FieldType, Schema};
use crate::coders::CoderError;

/// Marker for types that are not already nullable. Beam nullability is a flag on
/// [`FieldType`], so `Option<Option<T>>` would collapse to one level; the `Option<T>`
/// implementation requires this marker to make that a compile error. Mapped types and
/// `#[derive(BeamRow)]` implement it. Do not implement it by hand.
pub trait NotNullable {}

/// A Rust type with a statically known Beam field type. Values convert to
/// `Option<FieldValue>` because Beam stores nullability in the enclosing slot (see
/// [`Row::values`], [`FieldValue::Array`], [`FieldValue::Map`]), so `Option<T>` uses this trait.
pub trait BeamField: Sized {
    /// Returns the Beam field type for this Rust type. It depends on the type only.
    fn beam_field_type() -> FieldType;

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError>;

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError>;
}

/// A Rust struct that maps to a Beam [`Row`]. `#[derive(BeamRow)]` also emits a [`BeamField`]
/// implementation so the type can nest. A blanket `impl<T: BeamRow> BeamField for T` would
/// overlap each primitive implementation.
pub trait BeamRow: Sized {
    /// Returns the schema for this type, built once and cached. The `&'static Arc` lets
    /// [`Row::new`] increment a refcount instead of deep-cloning the schema.
    fn beam_schema() -> &'static Arc<Schema>;

    fn to_row(&self) -> Result<Row, SchemaError>;

    fn from_row(row: &Row) -> Result<Self, SchemaError>;

    /// Encodes directly to the portable `beam:coder:row:v1` wire format. Returns
    /// [`CoderError::Schema`] if the value does not convert to a row.
    fn to_row_bytes(&self) -> Result<Vec<u8>, CoderError> {
        self.to_row()
            .map_err(CoderError::Schema)
            .and_then(|row| row.to_row_bytes())
    }

    /// Decodes from the portable `beam:coder:row:v1` wire format with the schema of `Self`, so
    /// encoder and decoder always agree. Returns [`CoderError::Schema`] if the row does not
    /// convert to `Self`.
    fn from_row_bytes(bytes: &[u8]) -> Result<Self, CoderError> {
        Row::from_row_bytes(Self::beam_schema(), bytes)
            .and_then(|row| Self::from_row(&row).map_err(CoderError::Schema))
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
    ($rust:ty, $variant:ident, $ctor:ident, $name:literal) => {
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
        }
    };
}

/// Implements [`BeamField`] for an unsigned type that widens losslessly into a larger signed
/// Beam type. Decoding narrows with a check: a value from another SDK that does not fit returns
/// [`SchemaError::ValueOutOfRange`] and does not wrap.
macro_rules! beam_field_widening {
    ($rust:ty, $variant:ident, $wide:ty, $ctor:ident, $name:literal) => {
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
        }
    };
}

beam_field_copy!(bool, Boolean, boolean, "BOOLEAN");
beam_field_copy!(i8, Byte, byte, "BYTE");
beam_field_copy!(i16, Int16, int16, "INT16");
beam_field_copy!(i32, Int32, int32, "INT32");
beam_field_copy!(i64, Int64, int64, "INT64");
beam_field_copy!(f32, Float, float, "FLOAT");
beam_field_copy!(f64, Double, double, "DOUBLE");

beam_field_widening!(u16, Int32, i32, int32, "INT32");
beam_field_widening!(u32, Int64, i64, int64, "INT64");

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
}

/// `Option<T>` sets the nullable flag on `T`'s field type. [`NotNullable`] rejects
/// `Option<Option<T>>`.
impl<T: BeamField + NotNullable> BeamField for Option<T> {
    fn beam_field_type() -> FieldType {
        T::beam_field_type().with_nullable(true)
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        self.as_ref().map_or(Ok(None), T::to_field_value)
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        value.map(|v| T::from_field_value(Some(v))).transpose()
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
        }
    };
}

beam_field_map!(HashMap, Eq + Hash);
beam_field_map!(BTreeMap, Ord);

/// Support functions for code that `#[derive(BeamRow)]` generates. Not part of the stable
/// public API: it changes together with the derive macro.
#[doc(hidden)]
pub mod derive_support {
    use super::{FieldType, FieldValue, SchemaError};

    /// Field type for a `Vec<u8>` field annotated `#[beam(bytes)]`. `Vec<u8>` has no
    /// [`BeamField`](super::BeamField) implementation (see the module docs).
    pub fn bytes_field_type() -> FieldType {
        FieldType::bytes()
    }

    pub fn bytes_to_field_value(value: &[u8]) -> Result<Option<FieldValue>, SchemaError> {
        Ok(Some(FieldValue::Bytes(value.to_vec())))
    }

    pub fn bytes_from_field_value(value: Option<&FieldValue>) -> Result<Vec<u8>, SchemaError> {
        from_field_value_arms!(value, Bytes, "BYTES", |v: &Vec<u8>| Ok(v.clone()))
    }
}
