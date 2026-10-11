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

//! Readers of the values of Arrow arrays as the types of a Beam schema.
//!
//! A [`Reader`] holds one Arrow array and the Beam type of its values. It accepts the Arrow
//! types that convert with no loss: any integer width (range checked), `Float32` for
//! `DOUBLE`, large and view strings, binaries and lists, dictionary columns, any timestamp
//! unit and `Date64`. Columns and struct children match **by name**. The conversion to Beam
//! rows and the conversion to row coder bytes read the same readers, so the two give the same
//! values.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};
use beam::coders::VarIntCoder;
use beam::schema::{
    AtomicType, FieldType, Schema, TypeInfo, URN_DATE, URN_DECIMAL, URN_MICROS_INSTANT,
    URN_MILLIS_INSTANT,
};

use super::columns::encoding_order;
use super::{MICROS_PER_SECOND, out_of_range};
use crate::error::{ArrowBridgeError, Result};

const MILLIS_PER_SECOND: i64 = 1_000;
const MICROS_PER_MILLI: i64 = 1_000;
const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const MILLIS_PER_DAY: i64 = 86_400_000;

/// The readers of the fields of one schema, over the columns of a batch or a struct array.
pub(super) struct RowReaders<'a> {
    pub(super) schema: Arc<Schema>,
    pub(super) fields: Vec<Reader<'a>>,
    /// The field index at each position of the row coder encoding, if the two are not the
    /// same.
    pub(super) order: Option<Vec<usize>>,
}

impl<'a> RowReaders<'a> {
    /// The readers of the columns of `batch`, by the names of the fields of `schema`.
    pub(super) fn for_batch(batch: &'a RecordBatch, schema: &Arc<Schema>) -> Result<Self> {
        Self::new(None, schema, |name| {
            batch.column_by_name(name).map(AsRef::as_ref)
        })
    }

    fn for_struct(name: &str, schema: &Schema, array: &'a StructArray) -> Result<Self> {
        Self::new(Some(name), &Arc::new(schema.clone()), |child| {
            array.column_by_name(child).map(AsRef::as_ref)
        })
    }

    fn new(
        parent: Option<&str>,
        schema: &Arc<Schema>,
        column: impl Fn(&str) -> Option<&'a dyn Array>,
    ) -> Result<Self> {
        let fields = schema
            .fields
            .iter()
            .map(|field| {
                let name = parent.map_or_else(
                    || field.name.clone(),
                    |parent| format!("{parent}.{}", field.name),
                );
                match column(&field.name) {
                    Some(array) => Reader::new(name, &field.field_type, array),
                    None if field.field_type.nullable => Ok(Reader::missing(name)),
                    None => Err(ArrowBridgeError::MissingField(name)),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema: Arc::clone(schema),
            fields,
            order: encoding_order(schema),
        })
    }
}

/// How the row coder writes a value of a reader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Wire {
    /// As the Beam type of the reader.
    Plain,
    /// `millis_instant`: a fixed-width big-endian `INT64`.
    FixedInt64,
    /// `decimal`: the payload, with no length prefix.
    Payload,
}

/// One Arrow array and the Beam type of its values.
pub(super) struct Reader<'a> {
    pub(super) name: String,
    pub(super) nullable: bool,
    pub(super) wire: Wire,
    pub(super) kind: ReadKind<'a>,
    array: Option<&'a dyn Array>,
}

pub(super) enum ReadKind<'a> {
    /// A nullable field with no column: every value is null.
    Missing,
    Dictionary {
        keys: Vec<usize>,
        values: Box<Reader<'a>>,
    },
    Byte(Integers<'a>),
    Int16(Integers<'a>),
    Int32(Integers<'a>),
    Int64(Integers<'a>),
    Float(&'a Float32Array),
    Double(Doubles<'a>),
    Boolean(&'a BooleanArray),
    String(Texts<'a>),
    Bytes(Binaries<'a>),
    /// A timestamp as a `micros_instant` row of seconds and micros. `order` is the encoding
    /// order of that row.
    MicrosInstant {
        values: Timestamps<'a>,
        representation: Arc<Schema>,
        order: Option<Vec<usize>>,
    },
    MillisInstant(Timestamps<'a>),
    Date(Dates<'a>),
    Decimal(Decimals<'a>),
    List {
        ranges: Ranges<'a>,
        items: Box<Reader<'a>>,
    },
    Map {
        offsets: &'a [i32],
        keys: Box<Reader<'a>>,
        values: Box<Reader<'a>>,
    },
    Struct(RowReaders<'a>),
}

impl<'a> Reader<'a> {
    fn missing(name: String) -> Self {
        Self {
            name,
            nullable: true,
            wire: Wire::Plain,
            kind: ReadKind::Missing,
            array: None,
        }
    }

    /// A reader of `array` as values of `field_type`. Fails if the Arrow type does not
    /// convert.
    pub(super) fn new(name: String, field_type: &FieldType, array: &'a dyn Array) -> Result<Self> {
        let (kind, wire) = match array.as_any_dictionary_opt() {
            Some(dictionary) => {
                let values = Reader::new(name.clone(), field_type, dictionary.values().as_ref())?;
                let wire = values.wire;
                let kind = ReadKind::Dictionary {
                    keys: dictionary.normalized_keys(),
                    values: Box::new(values),
                };
                (kind, wire)
            }
            None => read_kind(&name, &field_type.type_info, array)?,
        };
        Ok(Self {
            name,
            nullable: field_type.nullable,
            wire,
            kind,
            array: Some(array),
        })
    }

    /// Whether the value at `index` is null.
    pub(super) fn is_null(&self, index: usize) -> Result<bool> {
        match (&self.kind, self.array) {
            (ReadKind::Missing, _) | (_, None) => Ok(true),
            (_, Some(array)) if array.is_null(index) => Ok(true),
            (ReadKind::Dictionary { keys, values }, _) => {
                values.is_null(dictionary_key(keys, index, &self.name)?)
            }
            _ => Ok(false),
        }
    }
}

/// The key of a dictionary value at `index`.
pub(super) fn dictionary_key(keys: &[usize], index: usize, name: &str) -> Result<usize> {
    keys.get(index)
        .copied()
        .ok_or_else(|| out_of_range(name, index, "dictionary index"))
}

fn read_kind<'a>(
    name: &str,
    type_info: &TypeInfo,
    array: &'a dyn Array,
) -> Result<(ReadKind<'a>, Wire)> {
    let mismatch = |expected: &str| arrow_mismatch(name, expected, array.data_type());
    let kind = match type_info {
        TypeInfo::Atomic(atomic) => match atomic {
            AtomicType::Byte => {
                ReadKind::Byte(Integers::new(array).ok_or_else(|| mismatch("BYTE"))?)
            }
            AtomicType::Int16 => {
                ReadKind::Int16(Integers::new(array).ok_or_else(|| mismatch("INT16"))?)
            }
            AtomicType::Int32 => {
                ReadKind::Int32(Integers::new(array).ok_or_else(|| mismatch("INT32"))?)
            }
            AtomicType::Int64 => {
                ReadKind::Int64(Integers::new(array).ok_or_else(|| mismatch("INT64"))?)
            }
            AtomicType::Float => {
                ReadKind::Float(array.as_primitive_opt().ok_or_else(|| mismatch("FLOAT"))?)
            }
            AtomicType::Double => {
                ReadKind::Double(Doubles::new(array).ok_or_else(|| mismatch("DOUBLE"))?)
            }
            AtomicType::Boolean => {
                ReadKind::Boolean(array.as_boolean_opt().ok_or_else(|| mismatch("BOOLEAN"))?)
            }
            AtomicType::String => {
                ReadKind::String(Texts::new(array).ok_or_else(|| mismatch("STRING"))?)
            }
            AtomicType::Bytes => {
                ReadKind::Bytes(Binaries::new(array).ok_or_else(|| mismatch("BYTES"))?)
            }
        },
        TypeInfo::Array(item) | TypeInfo::Iterable(item) => {
            let (ranges, values) = Ranges::new(array).ok_or_else(|| mismatch("ARRAY"))?;
            ReadKind::List {
                ranges,
                items: Box::new(Reader::new(format!("{name}[]"), item, values)?),
            }
        }
        TypeInfo::Map(key, value) => {
            let map = array.as_map_opt().ok_or_else(|| mismatch("MAP"))?;
            ReadKind::Map {
                offsets: map.value_offsets(),
                keys: Box::new(Reader::new(
                    format!("{name}.key"),
                    key,
                    map.keys().as_ref(),
                )?),
                values: Box::new(Reader::new(
                    format!("{name}.value"),
                    value,
                    map.values().as_ref(),
                )?),
            }
        }
        TypeInfo::Row(schema) => {
            let structs = array.as_struct_opt().ok_or_else(|| mismatch("ROW"))?;
            ReadKind::Struct(RowReaders::for_struct(name, schema, structs)?)
        }
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => {
            let timestamp = matches!(array.data_type(), DataType::Timestamp(..));
            let decimal = matches!(
                array.data_type(),
                DataType::Decimal128(..) | DataType::Decimal256(..)
            );
            match urn.as_str() {
                URN_MICROS_INSTANT if timestamp => {
                    let TypeInfo::Row(representation) = &representation.type_info else {
                        return Err(ArrowBridgeError::UnsupportedType {
                            field: name.to_string(),
                            detail: "micros_instant must be represented as a ROW".to_string(),
                        });
                    };
                    ReadKind::MicrosInstant {
                        values: Timestamps::new(name, array)?,
                        order: encoding_order(representation),
                        representation: Arc::new(representation.clone()),
                    }
                }
                URN_MILLIS_INSTANT if timestamp => {
                    ReadKind::MillisInstant(Timestamps::new(name, array)?)
                }
                URN_DATE => ReadKind::Date(Dates::new(array).ok_or_else(|| mismatch("DATE"))?),
                URN_DECIMAL if decimal => ReadKind::Decimal(Decimals::new(name, array)?),
                _ => {
                    let (kind, _) = read_kind(name, &representation.type_info, array)?;
                    let wire = match urn.as_str() {
                        URN_MILLIS_INSTANT => Wire::FixedInt64,
                        URN_DECIMAL => Wire::Payload,
                        _ => Wire::Plain,
                    };
                    return Ok((kind, wire));
                }
            }
        }
    };
    let wire = match &kind {
        ReadKind::MillisInstant(_) => Wire::FixedInt64,
        ReadKind::Decimal(_) => Wire::Payload,
        _ => Wire::Plain,
    };
    Ok((kind, wire))
}

fn arrow_mismatch(name: &str, expected: &str, data_type: &DataType) -> ArrowBridgeError {
    ArrowBridgeError::TypeMismatch {
        field: name.to_string(),
        expected: expected.to_string(),
        actual: format!("Arrow {data_type}"),
    }
}

/// Any Arrow integer array, read as `i64`.
pub(super) enum Integers<'a> {
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
}

impl<'a> Integers<'a> {
    fn new(array: &'a dyn Array) -> Option<Self> {
        Some(match array.data_type() {
            DataType::Int8 => Self::Int8(array.as_primitive::<Int8Type>()),
            DataType::Int16 => Self::Int16(array.as_primitive::<Int16Type>()),
            DataType::Int32 => Self::Int32(array.as_primitive::<Int32Type>()),
            DataType::Int64 => Self::Int64(array.as_primitive::<Int64Type>()),
            DataType::UInt8 => Self::UInt8(array.as_primitive::<UInt8Type>()),
            DataType::UInt16 => Self::UInt16(array.as_primitive::<UInt16Type>()),
            DataType::UInt32 => Self::UInt32(array.as_primitive::<UInt32Type>()),
            DataType::UInt64 => Self::UInt64(array.as_primitive::<UInt64Type>()),
            _ => return None,
        })
    }

    /// The value at `index`, if `T` holds it. `expected` names the Beam type for the error.
    pub(super) fn get<T: TryFrom<i64>>(
        &self,
        index: usize,
        name: &str,
        expected: &str,
    ) -> Result<T> {
        let value = match self {
            Self::Int8(array) => i64::from(array.value(index)),
            Self::Int16(array) => i64::from(array.value(index)),
            Self::Int32(array) => i64::from(array.value(index)),
            Self::Int64(array) => array.value(index),
            Self::UInt8(array) => i64::from(array.value(index)),
            Self::UInt16(array) => i64::from(array.value(index)),
            Self::UInt32(array) => i64::from(array.value(index)),
            Self::UInt64(array) => {
                let value = array.value(index);
                i64::try_from(value).map_err(|_| out_of_range(name, value, "INT64"))?
            }
        };
        T::try_from(value).map_err(|_| out_of_range(name, value, expected))
    }
}

/// A `Float64` array, or a `Float32` array read as `f64`.
pub(super) enum Doubles<'a> {
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
}

impl<'a> Doubles<'a> {
    fn new(array: &'a dyn Array) -> Option<Self> {
        match array.data_type() {
            DataType::Float32 => Some(Self::Float32(array.as_primitive::<Float32Type>())),
            _ => array.as_primitive_opt::<Float64Type>().map(Self::Float64),
        }
    }

    pub(super) fn get(&self, index: usize) -> f64 {
        match self {
            Self::Float32(array) => f64::from(array.value(index)),
            Self::Float64(array) => array.value(index),
        }
    }
}

/// Any Arrow string array.
pub(super) enum Texts<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl<'a> Texts<'a> {
    fn new(array: &'a dyn Array) -> Option<Self> {
        Some(match array.data_type() {
            DataType::Utf8 => Self::Utf8(array.as_string::<i32>()),
            DataType::LargeUtf8 => Self::LargeUtf8(array.as_string::<i64>()),
            DataType::Utf8View => Self::View(array.as_string_view()),
            _ => return None,
        })
    }

    pub(super) fn get(&self, index: usize) -> &'a str {
        match self {
            Self::Utf8(array) => array.value(index),
            Self::LargeUtf8(array) => array.value(index),
            Self::View(array) => array.value(index),
        }
    }
}

/// Any Arrow binary array.
pub(super) enum Binaries<'a> {
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    View(&'a BinaryViewArray),
    FixedSize(&'a FixedSizeBinaryArray),
}

impl<'a> Binaries<'a> {
    fn new(array: &'a dyn Array) -> Option<Self> {
        Some(match array.data_type() {
            DataType::Binary => Self::Binary(array.as_binary::<i32>()),
            DataType::LargeBinary => Self::LargeBinary(array.as_binary::<i64>()),
            DataType::BinaryView => Self::View(array.as_binary_view()),
            DataType::FixedSizeBinary(_) => Self::FixedSize(array.as_fixed_size_binary()),
            _ => return None,
        })
    }

    pub(super) fn get(&self, index: usize) -> &'a [u8] {
        match self {
            Self::Binary(array) => array.value(index),
            Self::LargeBinary(array) => array.value(index),
            Self::View(array) => array.value(index),
            Self::FixedSize(array) => array.value(index),
        }
    }
}

/// Any Arrow timestamp array.
pub(super) enum Timestamps<'a> {
    Second(&'a TimestampSecondArray),
    Millisecond(&'a TimestampMillisecondArray),
    Microsecond(&'a TimestampMicrosecondArray),
    Nanosecond(&'a TimestampNanosecondArray),
}

impl<'a> Timestamps<'a> {
    fn new(name: &str, array: &'a dyn Array) -> Result<Self> {
        Ok(match array.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => {
                Self::Second(array.as_primitive::<TimestampSecondType>())
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Self::Millisecond(array.as_primitive::<TimestampMillisecondType>())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Self::Microsecond(array.as_primitive::<TimestampMicrosecondType>())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::Nanosecond(array.as_primitive::<TimestampNanosecondType>())
            }
            other => return Err(arrow_mismatch(name, "TIMESTAMP", other)),
        })
    }

    /// The value at `index` in `unit`. A lower resolution rounds toward negative infinity.
    pub(super) fn get(&self, index: usize, unit: TimeUnit, name: &str) -> Result<i64> {
        let (value, scaled) = match self {
            Self::Second(array) => {
                let v = array.value(index);
                let scaled = match unit {
                    TimeUnit::Microsecond => v.checked_mul(MICROS_PER_SECOND),
                    TimeUnit::Millisecond => v.checked_mul(MILLIS_PER_SECOND),
                    TimeUnit::Second => Some(v),
                    TimeUnit::Nanosecond => None,
                };
                (v, scaled)
            }
            Self::Millisecond(array) => {
                let v = array.value(index);
                let scaled = match unit {
                    TimeUnit::Microsecond => v.checked_mul(MICROS_PER_MILLI),
                    TimeUnit::Millisecond => Some(v),
                    TimeUnit::Second | TimeUnit::Nanosecond => None,
                };
                (v, scaled)
            }
            Self::Microsecond(array) => {
                let v = array.value(index);
                let scaled = match unit {
                    TimeUnit::Microsecond => Some(v),
                    TimeUnit::Millisecond => Some(v.div_euclid(MICROS_PER_MILLI)),
                    TimeUnit::Second | TimeUnit::Nanosecond => None,
                };
                (v, scaled)
            }
            Self::Nanosecond(array) => {
                let v = array.value(index);
                let scaled = match unit {
                    TimeUnit::Microsecond => Some(v.div_euclid(NANOS_PER_MICRO)),
                    TimeUnit::Millisecond => Some(v.div_euclid(NANOS_PER_MILLI)),
                    TimeUnit::Nanosecond => Some(v),
                    TimeUnit::Second => None,
                };
                (v, scaled)
            }
        };
        scaled.ok_or_else(|| out_of_range(name, value, &format!("timestamp {unit:?}")))
    }
}

/// The seconds and the micros of a `micros_instant` value of `micros` microseconds.
pub(super) fn seconds_and_micros(micros: i64) -> (i64, i64) {
    (
        micros.div_euclid(MICROS_PER_SECOND),
        micros.rem_euclid(MICROS_PER_SECOND),
    )
}

/// An Arrow date array, or an integer day count, read as days since the epoch.
pub(super) enum Dates<'a> {
    Date32(&'a Date32Array),
    Date64(&'a Date64Array),
    Days(Integers<'a>),
}

impl<'a> Dates<'a> {
    fn new(array: &'a dyn Array) -> Option<Self> {
        match array.data_type() {
            DataType::Date32 => Some(Self::Date32(array.as_primitive::<Date32Type>())),
            DataType::Date64 => Some(Self::Date64(array.as_primitive::<Date64Type>())),
            _ => Integers::new(array).map(Self::Days),
        }
    }

    pub(super) fn get(&self, index: usize, name: &str) -> Result<i64> {
        Ok(match self {
            Self::Date32(array) => i64::from(array.value(index)),
            Self::Date64(array) => array.value(index).div_euclid(MILLIS_PER_DAY),
            Self::Days(days) => days.get(index, name, "DATE")?,
        })
    }
}

/// An Arrow `Decimal128` or `Decimal256` array, read as Beam `decimal` payloads.
pub(super) enum Decimals<'a> {
    Decimal128(&'a Decimal128Array, i8),
    Decimal256(&'a Decimal256Array, i8),
}

impl<'a> Decimals<'a> {
    fn new(name: &str, array: &'a dyn Array) -> Result<Self> {
        Ok(match array.data_type() {
            DataType::Decimal128(_, scale) => {
                Self::Decimal128(array.as_primitive::<Decimal128Type>(), *scale)
            }
            DataType::Decimal256(_, scale) => {
                Self::Decimal256(array.as_primitive::<Decimal256Type>(), *scale)
            }
            other => return Err(arrow_mismatch(name, "DECIMAL", other)),
        })
    }

    /// The `decimal` payload of the value at `index`: the varint scale, the varint length
    /// and the big-endian two's complement of the unscaled value.
    pub(super) fn payload(&self, index: usize, name: &str) -> Result<Vec<u8>> {
        match self {
            Self::Decimal128(array, scale) => {
                decimal_payload(name, *scale, &array.value(index).to_be_bytes())
            }
            Self::Decimal256(array, scale) => {
                decimal_payload(name, *scale, &array.value(index).to_be_bytes())
            }
        }
    }
}

fn decimal_payload(name: &str, scale: i8, unscaled: &[u8]) -> Result<Vec<u8>> {
    let digits = minimal_twos_complement(unscaled);
    let mut buffer = Vec::with_capacity(digits.len() + 4);
    VarIntCoder::encode_varint(i64::from(scale), &mut buffer)
        .and_then(|()| VarIntCoder::encode_varint(digits.len() as i64, &mut buffer))
        .map_err(|e| out_of_range(name, e, "decimal encoding"))?;
    buffer.extend_from_slice(digits);
    Ok(buffer)
}

/// The shortest suffix of the big-endian two's-complement bytes `full` that keeps the value.
fn minimal_twos_complement(full: &[u8]) -> &[u8] {
    let start = (0..full.len().saturating_sub(1))
        .take_while(|&i| {
            let next_negative = full[i + 1] & 0x80 != 0;
            (full[i] == 0x00 && !next_negative) || (full[i] == 0xFF && next_negative)
        })
        .count();
    &full[start..]
}

/// The child ranges of the values of a list array, and its child array.
pub(super) enum Ranges<'a> {
    Small(&'a [i32]),
    Large(&'a [i64]),
    /// A fixed-size list: the size of each list and the offset of the array.
    Fixed(usize, usize),
}

impl<'a> Ranges<'a> {
    fn new(array: &'a dyn Array) -> Option<(Self, &'a dyn Array)> {
        if let Some(list) = array.as_list_opt::<i32>() {
            Some((Self::Small(list.value_offsets()), list.values().as_ref()))
        } else if let Some(list) = array.as_list_opt::<i64>() {
            Some((Self::Large(list.value_offsets()), list.values().as_ref()))
        } else {
            array.as_fixed_size_list_opt().map(|list| {
                let size = usize::try_from(list.value_length()).unwrap_or(0);
                (Self::Fixed(size, list.offset()), list.values().as_ref())
            })
        }
    }

    /// The range of child elements of the list at `index`.
    pub(super) fn get(&self, index: usize) -> Range<usize> {
        match self {
            Self::Small(offsets) => offset_range(offsets, index),
            Self::Large(offsets) => offset_range(offsets, index),
            Self::Fixed(size, offset) => (offset + index) * size..(offset + index + 1) * size,
        }
    }
}

/// The range between the offsets at `index` and `index + 1`.
pub(super) fn offset_range<O: Copy + TryInto<usize>>(offsets: &[O], index: usize) -> Range<usize> {
    let at = |i: usize| {
        offsets
            .get(i)
            .and_then(|offset| (*offset).try_into().ok())
            .unwrap_or(0)
    };
    let (start, end) = (at(index), at(index + 1));
    start..end.max(start)
}
