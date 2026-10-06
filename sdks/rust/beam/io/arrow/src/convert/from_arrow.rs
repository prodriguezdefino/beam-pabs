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

//! An Arrow record batch to Beam rows of a requested schema.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, MapArray, RecordBatch, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, TimeUnit};
use beam::coders::VarIntCoder;
use beam::schema::{
    AtomicType, BeamRow, FieldType, FieldValue, Row, Schema, SchemaError, TypeInfo, URN_DATE,
    URN_DECIMAL, URN_MICROS_INSTANT, URN_MILLIS_INSTANT,
};

use super::{MICROS_PER_SECOND, out_of_range};
use crate::error::{ArrowBridgeError, Result};

const MILLIS_PER_SECOND: i64 = 1_000;
const MICROS_PER_MILLI: i64 = 1_000;
const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const MILLIS_PER_DAY: i64 = 86_400_000;

/// Converts an Arrow record batch into Beam rows of `schema`.
///
/// Columns match fields by name. A missing column gives nulls for a nullable field and an
/// error otherwise; extra columns are ignored.
pub fn record_batch_to_rows(batch: &RecordBatch, schema: &Arc<Schema>) -> Result<Vec<Row>> {
    let columns = schema
        .fields
        .iter()
        .map(|field| match batch.column_by_name(&field.name) {
            Some(column) => array_to_values(&field.name, &field.field_type, column.as_ref()),
            None if field.field_type.nullable => Ok(vec![None; batch.num_rows()]),
            None => Err(ArrowBridgeError::MissingField(field.name.clone())),
        })
        .collect::<Result<Vec<_>>>()?;
    assemble_rows(schema, columns, batch.num_rows(), None)
}

/// Converts an Arrow record batch into `#[derive(BeamRow)]` values.
pub fn record_batch_to_beam_rows<T: BeamRow>(batch: &RecordBatch) -> Result<Vec<T>> {
    record_batch_to_rows(batch, T::beam_schema())?
        .iter()
        .map(|row| T::from_row(row).map_err(ArrowBridgeError::from))
        .collect()
}

/// Transposes columns into rows; checks non-nullable fields on every valid row.
fn assemble_rows(
    schema: &Arc<Schema>,
    columns: Vec<Vec<Option<FieldValue>>>,
    len: usize,
    nulls: Option<&NullBuffer>,
) -> Result<Vec<Row>> {
    let mut iters: Vec<_> = columns.into_iter().map(Vec::into_iter).collect();
    (0..len)
        .filter_map(|i| {
            let values: Vec<Option<FieldValue>> =
                iters.iter_mut().map(|it| it.next().flatten()).collect();
            if nulls.is_some_and(|n| n.is_null(i)) {
                return None;
            }
            Some(
                schema
                    .fields
                    .iter()
                    .zip(&values)
                    .try_for_each(|(field, value)| {
                        if value.is_none() && !field.field_type.nullable {
                            Err(ArrowBridgeError::UnexpectedNull {
                                field: field.name.clone(),
                            })
                        } else {
                            Ok(())
                        }
                    })
                    .and_then(|()| Row::new(Arc::clone(schema), values).map_err(Into::into)),
            )
        })
        .collect()
}

fn arrow_mismatch(name: &str, expected: &str, data_type: &DataType) -> ArrowBridgeError {
    ArrowBridgeError::TypeMismatch {
        field: name.to_string(),
        expected: expected.to_string(),
        actual: format!("Arrow {data_type}"),
    }
}

/// Converts an Arrow array into one optional Beam value per element.
pub fn array_to_values(
    name: &str,
    ft: &FieldType,
    array: &dyn Array,
) -> Result<Vec<Option<FieldValue>>> {
    if let Some(dict) = array.as_any_dictionary_opt() {
        let values = array_to_values(name, ft, dict.values().as_ref())?;
        return dict
            .normalized_keys()
            .into_iter()
            .enumerate()
            .map(|(i, key)| {
                if array.is_null(i) {
                    Ok(None)
                } else {
                    values
                        .get(key)
                        .cloned()
                        .ok_or_else(|| out_of_range(name, key, "dictionary index"))
                }
            })
            .collect();
    }
    convert_type(name, &ft.type_info, array)
}

fn convert_type(
    name: &str,
    type_info: &TypeInfo,
    array: &dyn Array,
) -> Result<Vec<Option<FieldValue>>> {
    match type_info {
        TypeInfo::Atomic(atomic) => convert_atomic(name, *atomic, array),
        TypeInfo::Array(elem) | TypeInfo::Iterable(elem) => convert_list(name, elem, array),
        TypeInfo::Map(key, value) => convert_map(name, key, value, array),
        TypeInfo::Row(schema) => convert_struct(name, schema, array),
        TypeInfo::Logical {
            urn,
            representation,
            ..
        } => match urn.as_str() {
            URN_MICROS_INSTANT if matches!(array.data_type(), DataType::Timestamp(..)) => {
                let TypeInfo::Row(repr) = &representation.type_info else {
                    return Err(ArrowBridgeError::UnsupportedType {
                        field: name.to_string(),
                        detail: "micros_instant must be represented as a ROW".to_string(),
                    });
                };
                let repr = Arc::new(repr.clone());
                timestamps(name, array, TimeUnit::Microsecond)?
                    .into_iter()
                    .map(|micros| {
                        micros
                            .map(|m| {
                                Row::new(
                                    Arc::clone(&repr),
                                    vec![
                                        Some(FieldValue::Int64(m.div_euclid(MICROS_PER_SECOND))),
                                        Some(FieldValue::Int64(m.rem_euclid(MICROS_PER_SECOND))),
                                    ],
                                )
                                .map(FieldValue::Row)
                                .map_err(ArrowBridgeError::from)
                            })
                            .transpose()
                    })
                    .collect()
            }
            URN_MILLIS_INSTANT if matches!(array.data_type(), DataType::Timestamp(..)) => {
                Ok(timestamps(name, array, TimeUnit::Millisecond)?
                    .into_iter()
                    .map(|v| v.map(FieldValue::Int64))
                    .collect())
            }
            URN_DATE => Ok(dates(name, array)?
                .into_iter()
                .map(|v| v.map(FieldValue::Int64))
                .collect()),
            URN_DECIMAL
                if matches!(
                    array.data_type(),
                    DataType::Decimal128(..) | DataType::Decimal256(..)
                ) =>
            {
                decimals(name, array)
            }
            _ => array_to_values(name, representation, array),
        },
    }
}

/// Reads any Arrow integer array as `i64`, or returns `None` for non-integer arrays.
fn integers(name: &str, array: &dyn Array) -> Result<Option<Vec<Option<i64>>>> {
    macro_rules! widen {
        ($t:ty) => {
            array
                .as_primitive::<$t>()
                .iter()
                .map(|v| v.map(i64::from))
                .collect()
        };
    }
    let values = match array.data_type() {
        DataType::Int8 => widen!(Int8Type),
        DataType::Int16 => widen!(Int16Type),
        DataType::Int32 => widen!(Int32Type),
        DataType::Int64 => array.as_primitive::<Int64Type>().iter().collect(),
        DataType::UInt8 => widen!(UInt8Type),
        DataType::UInt16 => widen!(UInt16Type),
        DataType::UInt32 => widen!(UInt32Type),
        DataType::UInt64 => array
            .as_primitive::<UInt64Type>()
            .iter()
            .map(|v| {
                v.map(|v| i64::try_from(v).map_err(|_| out_of_range(name, v, "INT64")))
                    .transpose()
            })
            .collect::<Result<_>>()?,
        _ => return Ok(None),
    };
    Ok(Some(values))
}

fn narrow<T: TryFrom<i64>>(
    name: &str,
    array: &dyn Array,
    expected: &str,
    wrap: impl Fn(T) -> FieldValue,
) -> Result<Vec<Option<FieldValue>>> {
    integers(name, array)?
        .ok_or_else(|| arrow_mismatch(name, expected, array.data_type()))?
        .into_iter()
        .map(|v| {
            v.map(|v| {
                T::try_from(v)
                    .map(&wrap)
                    .map_err(|_| out_of_range(name, v, expected))
            })
            .transpose()
        })
        .collect()
}

fn convert_atomic(
    name: &str,
    atomic: AtomicType,
    array: &dyn Array,
) -> Result<Vec<Option<FieldValue>>> {
    let mismatch = |expected: &str| arrow_mismatch(name, expected, array.data_type());
    let values = match atomic {
        AtomicType::Byte => narrow(name, array, "BYTE", FieldValue::Byte)?,
        AtomicType::Int16 => narrow(name, array, "INT16", FieldValue::Int16)?,
        AtomicType::Int32 => narrow(name, array, "INT32", FieldValue::Int32)?,
        AtomicType::Int64 => narrow(name, array, "INT64", FieldValue::Int64)?,
        AtomicType::Float => array
            .as_primitive_opt::<Float32Type>()
            .ok_or_else(|| mismatch("FLOAT"))?
            .iter()
            .map(|v| v.map(FieldValue::Float))
            .collect(),
        AtomicType::Double => match array.data_type() {
            DataType::Float32 => array
                .as_primitive::<Float32Type>()
                .iter()
                .map(|v| v.map(|v| FieldValue::Double(f64::from(v))))
                .collect(),
            _ => array
                .as_primitive_opt::<Float64Type>()
                .ok_or_else(|| mismatch("DOUBLE"))?
                .iter()
                .map(|v| v.map(FieldValue::Double))
                .collect(),
        },
        AtomicType::Boolean => array
            .as_boolean_opt()
            .ok_or_else(|| mismatch("BOOLEAN"))?
            .iter()
            .map(|v| v.map(FieldValue::Boolean))
            .collect(),
        AtomicType::String => {
            let to_value = |v: Option<&str>| v.map(|s| FieldValue::String(s.to_string()));
            match array.data_type() {
                DataType::Utf8 => array.as_string::<i32>().iter().map(to_value).collect(),
                DataType::LargeUtf8 => array.as_string::<i64>().iter().map(to_value).collect(),
                DataType::Utf8View => array.as_string_view().iter().map(to_value).collect(),
                _ => return Err(mismatch("STRING")),
            }
        }
        AtomicType::Bytes => {
            let to_value = |v: Option<&[u8]>| v.map(|b| FieldValue::Bytes(b.to_vec()));
            match array.data_type() {
                DataType::Binary => array.as_binary::<i32>().iter().map(to_value).collect(),
                DataType::LargeBinary => array.as_binary::<i64>().iter().map(to_value).collect(),
                DataType::BinaryView => array.as_binary_view().iter().map(to_value).collect(),
                DataType::FixedSizeBinary(_) => {
                    array.as_fixed_size_binary().iter().map(to_value).collect()
                }
                _ => return Err(mismatch("BYTES")),
            }
        }
    };
    Ok(values)
}

/// Splits converted child values into one Beam array per parent element.
fn gather_lists(
    name: &str,
    elem: &FieldType,
    child: &dyn Array,
    parent: &dyn Array,
    ranges: impl Iterator<Item = Range<usize>>,
) -> Result<Vec<Option<FieldValue>>> {
    let item_name = format!("{name}[]");
    let mut children = array_to_values(&item_name, elem, child)?;
    ranges
        .enumerate()
        .map(|(i, range)| {
            if parent.is_null(i) {
                return Ok(None);
            }
            let items = children
                .get_mut(range.clone())
                .ok_or_else(|| out_of_range(name, format!("{range:?}"), "list child range"))?;
            if !elem.nullable && items.iter().any(Option::is_none) {
                return Err(ArrowBridgeError::UnexpectedNull {
                    field: item_name.clone(),
                });
            }
            Ok(Some(FieldValue::Array(
                items.iter_mut().map(Option::take).collect(),
            )))
        })
        .collect()
}

fn offset_ranges<O: Copy + TryInto<usize>>(offsets: &[O]) -> impl Iterator<Item = Range<usize>> {
    offsets.windows(2).map(|w| {
        let start = w[0].try_into().unwrap_or(0);
        let end = w[1].try_into().unwrap_or(0);
        start..end.max(start)
    })
}

fn convert_list(
    name: &str,
    elem: &FieldType,
    array: &dyn Array,
) -> Result<Vec<Option<FieldValue>>> {
    if let Some(list) = array.as_list_opt::<i32>() {
        gather_lists(
            name,
            elem,
            list.values().as_ref(),
            array,
            offset_ranges(list.value_offsets()),
        )
    } else if let Some(list) = array.as_list_opt::<i64>() {
        gather_lists(
            name,
            elem,
            list.values().as_ref(),
            array,
            offset_ranges(list.value_offsets()),
        )
    } else if let Some(list) = array.as_fixed_size_list_opt() {
        let size = usize::try_from(list.value_length()).unwrap_or(0);
        let start = list.offset();
        gather_lists(
            name,
            elem,
            list.values().as_ref(),
            array,
            (0..list.len()).map(|i| (start + i) * size..(start + i + 1) * size),
        )
    } else {
        Err(arrow_mismatch(name, "ARRAY", array.data_type()))
    }
}

fn convert_map(
    name: &str,
    key: &FieldType,
    value: &FieldType,
    array: &dyn Array,
) -> Result<Vec<Option<FieldValue>>> {
    let map: &MapArray = array
        .as_map_opt()
        .ok_or_else(|| arrow_mismatch(name, "MAP", array.data_type()))?;
    let key_name = format!("{name}.key");
    let keys = array_to_values(&key_name, key, map.keys().as_ref())?;
    let mut values = array_to_values(&format!("{name}.value"), value, map.values().as_ref())?;
    let mut keys = keys.into_iter().map(Some).collect::<Vec<_>>();
    offset_ranges(map.value_offsets())
        .enumerate()
        .map(|(i, range)| {
            if map.is_null(i) {
                return Ok(None);
            }
            range
                .map(|j| {
                    let k = keys
                        .get_mut(j)
                        .and_then(Option::take)
                        .flatten()
                        .ok_or(ArrowBridgeError::Schema(SchemaError::NullMapKey))?;
                    let v = values.get_mut(j).and_then(Option::take);
                    if v.is_none() && !value.nullable {
                        return Err(ArrowBridgeError::UnexpectedNull {
                            field: format!("{name}.value"),
                        });
                    }
                    Ok((k, v))
                })
                .collect::<Result<Vec<_>>>()
                .map(|entries| Some(FieldValue::Map(entries)))
        })
        .collect()
}

fn convert_struct(
    name: &str,
    schema: &Schema,
    array: &dyn Array,
) -> Result<Vec<Option<FieldValue>>> {
    let array: &StructArray = array
        .as_struct_opt()
        .ok_or_else(|| arrow_mismatch(name, "ROW", array.data_type()))?;
    let schema = Arc::new(schema.clone());
    let columns = schema
        .fields
        .iter()
        .map(|field| {
            let child_name = format!("{name}.{}", field.name);
            match array.column_by_name(&field.name) {
                Some(column) => array_to_values(&child_name, &field.field_type, column.as_ref()),
                None if field.field_type.nullable => Ok(vec![None; array.len()]),
                None => Err(ArrowBridgeError::MissingField(child_name)),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let nulls = array.logical_nulls();
    let mut rows = assemble_rows(&schema, columns, array.len(), nulls.as_ref())?.into_iter();
    Ok((0..array.len())
        .map(|i| {
            if array.is_null(i) {
                None
            } else {
                rows.next().map(FieldValue::Row)
            }
        })
        .collect())
}

/// Reads any Arrow timestamp array in `unit`, flooring when reducing resolution.
fn timestamps(name: &str, array: &dyn Array, unit: TimeUnit) -> Result<Vec<Option<i64>>> {
    let DataType::Timestamp(source, _) = array.data_type() else {
        return Err(arrow_mismatch(name, "TIMESTAMP", array.data_type()));
    };
    let raw: Vec<Option<i64>> = match source {
        TimeUnit::Second => array.as_primitive::<TimestampSecondType>().iter().collect(),
        TimeUnit::Millisecond => array
            .as_primitive::<TimestampMillisecondType>()
            .iter()
            .collect(),
        TimeUnit::Microsecond => array
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .collect(),
        TimeUnit::Nanosecond => array
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .collect(),
    };
    let scale = |v: i64| -> Option<i64> {
        match (source, unit) {
            (TimeUnit::Second, TimeUnit::Microsecond) => v.checked_mul(MICROS_PER_SECOND),
            (TimeUnit::Second, TimeUnit::Millisecond) => v.checked_mul(MILLIS_PER_SECOND),
            (TimeUnit::Millisecond, TimeUnit::Microsecond) => v.checked_mul(MICROS_PER_MILLI),
            (TimeUnit::Microsecond, TimeUnit::Millisecond) => Some(v.div_euclid(MICROS_PER_MILLI)),
            (TimeUnit::Nanosecond, TimeUnit::Microsecond) => Some(v.div_euclid(NANOS_PER_MICRO)),
            (TimeUnit::Nanosecond, TimeUnit::Millisecond) => Some(v.div_euclid(NANOS_PER_MILLI)),
            (s, u) if s == &u => Some(v),
            _ => None,
        }
    };
    raw.into_iter()
        .map(|v| {
            v.map(|v| scale(v).ok_or_else(|| out_of_range(name, v, &format!("timestamp {unit:?}"))))
                .transpose()
        })
        .collect()
}

/// Reads an Arrow date (or integer day count) as days since the epoch.
fn dates(name: &str, array: &dyn Array) -> Result<Vec<Option<i64>>> {
    match array.data_type() {
        DataType::Date32 => Ok(array
            .as_primitive::<Date32Type>()
            .iter()
            .map(|v| v.map(i64::from))
            .collect()),
        DataType::Date64 => Ok(array
            .as_primitive::<Date64Type>()
            .iter()
            .map(|v| v.map(|ms| ms.div_euclid(MILLIS_PER_DAY)))
            .collect()),
        _ => integers(name, array)?.ok_or_else(|| arrow_mismatch(name, "DATE", array.data_type())),
    }
}

/// Encodes Arrow `Decimal128` and `Decimal256` values as Beam `decimal` payloads:
/// VarInt scale, VarInt length, big-endian two's complement.
fn decimals(name: &str, array: &dyn Array) -> Result<Vec<Option<FieldValue>>> {
    match array.data_type() {
        DataType::Decimal128(_, scale) => array
            .as_primitive::<arrow_array::types::Decimal128Type>()
            .iter()
            .map(|v| decimal_payload(name, *scale, v.map(i128::to_be_bytes).as_ref()))
            .collect(),
        DataType::Decimal256(_, scale) => array
            .as_primitive::<arrow_array::types::Decimal256Type>()
            .iter()
            .map(|v| decimal_payload(name, *scale, v.map(|u| u.to_be_bytes()).as_ref()))
            .collect(),
        other => Err(arrow_mismatch(name, "DECIMAL", other)),
    }
}

/// Builds one `decimal` payload from the big-endian bytes of an unscaled value.
fn decimal_payload(
    name: &str,
    scale: i8,
    unscaled: Option<&impl AsRef<[u8]>>,
) -> Result<Option<FieldValue>> {
    unscaled
        .map(|unscaled| {
            let digits = minimal_twos_complement(unscaled.as_ref());
            let mut buffer = Vec::with_capacity(digits.len() + 4);
            VarIntCoder::encode_varint(i64::from(scale), &mut buffer)
                .and_then(|()| VarIntCoder::encode_varint(digits.len() as i64, &mut buffer))
                .map_err(|e| out_of_range(name, e, "decimal encoding"))?;
            buffer.extend_from_slice(digits);
            Ok(FieldValue::Bytes(buffer))
        })
        .transpose()
}

/// The shortest suffix of the big-endian two's-complement bytes `full` that keeps the value.
fn minimal_twos_complement(full: &[u8]) -> &[u8] {
    let start = (0..full.len() - 1)
        .take_while(|&i| {
            let next_negative = full[i + 1] & 0x80 != 0;
            (full[i] == 0x00 && !next_negative) || (full[i] == 0xFF && next_negative)
        })
        .count();
    &full[start..]
}
