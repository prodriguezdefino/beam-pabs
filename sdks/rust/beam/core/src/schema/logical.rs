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

//! Portable Beam logical types.
//!
//! Only the logical-type URNs in `LogicalTypes.Enum` in `schema.proto` are portable across
//! Beam SDKs. This SDK supports [`URN_MICROS_INSTANT`], [`URN_MILLIS_INSTANT`], [`URN_DATE`],
//! and [`URN_DECIMAL`].
//!
//! # `beam:logical_type:decimal:v1` wire format
//!
//! This SDK encodes the `BYTES` representation of `beam:logical_type:decimal:v1` as
//! `VarInt(scale) || VarInt(len) || two's-complement big-endian unscaled` (`BigDecimalCoder`
//! layout, compatible with the Java expansion service). The Python SDK encodes this logical
//! type as UTF-8 text; use a `STRING` field when sharing decimal values with Python transforms.

use std::sync::{Arc, LazyLock};

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rust_decimal::Decimal;

use crate::Instant;
use crate::coders::standard::{read_array, take_bytes};
use crate::coders::{CoderError, VarIntCoder};

use super::convert::{BeamField, NotNullable, derive_support};
use super::error::SchemaError;
use super::row::{FieldValue, Row};
use super::types::{Field, FieldType, Schema, TypeInfo};

/// Timestamp as seconds + microseconds since the epoch.
pub const URN_MICROS_INSTANT: &str = "beam:logical_type:micros_instant:v1";

/// Timestamp as milliseconds since the epoch (encoded as 8-byte big-endian `INT64` in row wire
/// payloads).
pub const URN_MILLIS_INSTANT: &str = "beam:logical_type:millis_instant:v1";

/// Date as days since the epoch.
pub const URN_DATE: &str = "beam:logical_type:date:v1";

/// Arbitrary-scale decimal.
pub const URN_DECIMAL: &str = "beam:logical_type:decimal:v1";

const MICROS_PER_SECOND: i64 = 1_000_000;
const NANOS_PER_MICRO: u32 = 1_000;

fn extract_required<'a, T>(
    value: Option<&'a FieldValue>,
    expected: &'static str,
    extract: impl FnOnce(&'a FieldValue) -> Option<T>,
) -> Result<T, SchemaError> {
    match value {
        Some(v) => extract(v).ok_or_else(|| SchemaError::ValueTypeMismatch {
            expected: expected.to_string(),
            actual: format!("{v:?}"),
        }),
        None => Err(SchemaError::UnexpectedNull {
            expected: expected.to_string(),
        }),
    }
}

/// Returns `ROW<seconds: INT64, micros: INT64>`, the required representation of
/// `micros_instant`.
fn micros_instant_representation() -> &'static Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("seconds", FieldType::int64()),
            Field::new("micros", FieldType::int64()),
        ]))
    });
    &SCHEMA
}

/// Reads a non-null `INT64` field by name from a representation row.
fn read_i64(row: &Row, name: &str) -> Result<i64, SchemaError> {
    row.get_i64(name)?
        .ok_or_else(|| SchemaError::UnexpectedNull {
            expected: format!("INT64 field '{name}'"),
        })
}

fn datetime_from_seconds_micros(seconds: i64, micros: i64) -> Result<DateTime<Utc>, SchemaError> {
    let nanos = u32::try_from(micros)
        .map_err(|_| SchemaError::ValueOutOfRange {
            value: micros.to_string(),
            target: "micros (0..1_000_000)".to_string(),
        })?
        .checked_mul(NANOS_PER_MICRO)
        .ok_or_else(|| SchemaError::ValueOutOfRange {
            value: micros.to_string(),
            target: "micros (0..1_000_000)".to_string(),
        })?;

    DateTime::from_timestamp(seconds, nanos).ok_or_else(|| SchemaError::ValueOutOfRange {
        value: format!("{seconds}s + {micros}us"),
        target: "DateTime<Utc>".to_string(),
    })
}

fn naive_date_to_days(date: NaiveDate) -> i64 {
    // `num_days_from_ce` has a constant offset. Subtract the epoch value to get
    // days since the epoch without a Duration.
    let epoch_days = DateTime::UNIX_EPOCH.date_naive().num_days_from_ce();
    i64::from(date.num_days_from_ce() - epoch_days)
}

fn naive_date_from_days(days: i64) -> Result<NaiveDate, SchemaError> {
    let out_of_range = || SchemaError::ValueOutOfRange {
        value: days.to_string(),
        target: "NaiveDate".to_string(),
    };

    let offset = i32::try_from(days).map_err(|_| out_of_range())?;
    let epoch_days = DateTime::UNIX_EPOCH.date_naive().num_days_from_ce();

    NaiveDate::from_num_days_from_ce_opt(epoch_days.checked_add(offset).ok_or_else(out_of_range)?)
        .ok_or_else(out_of_range)
}

impl NotNullable for DateTime<Utc> {}

impl BeamField for DateTime<Utc> {
    fn beam_field_type() -> FieldType {
        FieldType::logical(
            URN_MICROS_INSTANT,
            Vec::new(),
            FieldType::row(Arc::clone(micros_instant_representation())),
        )
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        // The specification requires a non-negative `micros`, also before the
        // epoch. Euclidean division gives that result.
        let total = self.timestamp_micros();
        let row = Row::new(
            Arc::clone(micros_instant_representation()),
            vec![
                Some(FieldValue::Int64(total.div_euclid(MICROS_PER_SECOND))),
                Some(FieldValue::Int64(total.rem_euclid(MICROS_PER_SECOND))),
            ],
        )?;

        Ok(Some(FieldValue::Row(row)))
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        let row = extract_required(value, "ROW<seconds: INT64, micros: INT64>", |v| match v {
            FieldValue::Row(r) => Some(r),
            _ => None,
        })?;

        let seconds = read_i64(row, "seconds")?;
        let micros = read_i64(row, "micros")?;
        datetime_from_seconds_micros(seconds, micros)
    }

    fn decode_null() -> Result<Self, CoderError> {
        Err(CoderError::Schema(SchemaError::UnexpectedNull {
            expected: "ROW<seconds: INT64, micros: INT64>".to_string(),
        }))
    }

    fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        let total = self.timestamp_micros();
        // Representation row has 2 non-null INT64 fields: VarInt(2) and VarInt(0) mask.
        writer.extend_from_slice(&[2, 0]);
        VarIntCoder::encode_varint_to_vec(total.div_euclid(MICROS_PER_SECOND), writer);
        VarIntCoder::encode_varint_to_vec(total.rem_euclid(MICROS_PER_SECOND), writer);
        Ok(())
    }

    fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let null_bits = derive_support::read_row_header(2, reader)?;
        if derive_support::bit_is_null(null_bits, 0) {
            return Err(CoderError::Schema(SchemaError::UnexpectedNull {
                expected: "INT64 field 'seconds'".to_string(),
            }));
        }
        if derive_support::bit_is_null(null_bits, 1) {
            return Err(CoderError::Schema(SchemaError::UnexpectedNull {
                expected: "INT64 field 'micros'".to_string(),
            }));
        }
        let seconds = VarIntCoder::decode_varint(reader)?;
        let micros = VarIntCoder::decode_varint(reader)?;
        datetime_from_seconds_micros(seconds, micros).map_err(CoderError::Schema)
    }

    fn matches_type_info(type_info: &TypeInfo) -> bool {
        matches!(
            type_info,
            TypeInfo::Logical { urn, representation, .. }
                if urn == URN_MICROS_INSTANT
                    && !representation.nullable
                    && matches!(
                        &representation.type_info,
                        TypeInfo::Row(s)
                            if !s.encoding_positions_set
                                && s.fields == micros_instant_representation().fields
                    )
        )
    }
}

impl NotNullable for NaiveDate {}

impl BeamField for NaiveDate {
    fn beam_field_type() -> FieldType {
        FieldType::logical(URN_DATE, Vec::new(), FieldType::int64())
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        Ok(Some(FieldValue::Int64(naive_date_to_days(*self))))
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        let days = extract_required(value, "INT64 (days since epoch)", |v| match v {
            FieldValue::Int64(d) => Some(*d),
            _ => None,
        })?;
        naive_date_from_days(days)
    }

    fn decode_null() -> Result<Self, CoderError> {
        Err(CoderError::Schema(SchemaError::UnexpectedNull {
            expected: "INT64 (days since epoch)".to_string(),
        }))
    }

    fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        VarIntCoder::encode_varint_to_vec(naive_date_to_days(*self), writer);
        Ok(())
    }

    fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let days = VarIntCoder::decode_varint(reader)?;
        naive_date_from_days(days).map_err(CoderError::Schema)
    }

    fn matches_type_info(type_info: &TypeInfo) -> bool {
        matches!(
            type_info,
            TypeInfo::Logical { urn, representation, .. }
                if urn == URN_DATE && <i64 as BeamField>::matches_field_type(representation)
        )
    }
}

/// Returns the minimal big-endian two's-complement slice of `full` (`value.to_be_bytes()`).
fn minimal_twos_complement_slice(full: &[u8; 16]) -> &[u8] {
    let redundant = |index: usize| {
        let byte = full[index];
        let next_is_negative = full[index + 1] & 0x80 != 0;
        (byte == 0x00 && !next_is_negative) || (byte == 0xFF && next_is_negative)
    };
    let start = (0..full.len() - 1)
        .take_while(|&index| redundant(index))
        .count();
    &full[start..]
}

/// Returns the minimal big-endian two's-complement bytes of `value` as the decimal wire format
/// requires. Redundant leading sign bytes are removed, but one sign byte always stays so the
/// sign survives the round trip.
pub(crate) fn to_minimal_twos_complement(value: i128) -> Vec<u8> {
    minimal_twos_complement_slice(&value.to_be_bytes()).to_vec()
}

/// Inverse of [`to_minimal_twos_complement`], sign-extending to 128 bits.
pub(crate) fn from_minimal_twos_complement(bytes: &[u8]) -> Result<i128, SchemaError> {
    let out_of_range = || SchemaError::ValueOutOfRange {
        value: format!("{} byte big integer", bytes.len()),
        target: "i128".to_string(),
    };

    let (&first, _) = bytes.split_first().ok_or_else(out_of_range)?;
    if bytes.len() > 16 {
        return Err(out_of_range());
    }

    // Fill with the sign first, so that a shorter slice widens correctly.
    let fill = if first & 0x80 != 0 { 0xFF } else { 0x00 };
    let mut buffer = [fill; 16];
    buffer[16 - bytes.len()..].copy_from_slice(bytes);

    Ok(i128::from_be_bytes(buffer))
}

fn decimal_from_parts(scale: i64, unscaled: &[u8]) -> Result<Decimal, SchemaError> {
    let mantissa = from_minimal_twos_complement(unscaled)?;
    let scale = u32::try_from(scale).map_err(|_| SchemaError::ValueOutOfRange {
        value: scale.to_string(),
        target: "decimal scale".to_string(),
    })?;

    Decimal::try_from_i128_with_scale(mantissa, scale).map_err(|e| SchemaError::ValueOutOfRange {
        value: format!("{mantissa}e-{scale} ({e})"),
        target: "Decimal".to_string(),
    })
}

/// Appends the decimal payload: the scale, the length and the minimal unscaled bytes.
fn write_decimal(value: &Decimal, writer: &mut Vec<u8>) {
    VarIntCoder::encode_varint_to_vec(i64::from(value.scale()), writer);
    let full = value.mantissa().to_be_bytes();
    let unscaled = minimal_twos_complement_slice(&full);
    VarIntCoder::encode_varint_to_vec(unscaled.len() as i64, writer);
    writer.extend_from_slice(unscaled);
}

impl NotNullable for Decimal {}

impl BeamField for Decimal {
    fn beam_field_type() -> FieldType {
        FieldType::logical(URN_DECIMAL, Vec::new(), FieldType::bytes())
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        let mut buffer = Vec::new();
        write_decimal(self, &mut buffer);
        Ok(Some(FieldValue::Bytes(buffer)))
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        let bytes = extract_required(value, "BYTES (decimal)", |v| match v {
            FieldValue::Bytes(b) => Some(b),
            _ => None,
        })?;

        let encoding_error = |e: std::io::Error| SchemaError::LogicalTypeEncoding(e.to_string());

        let mut cursor = bytes.as_slice();
        let scale = VarIntCoder::decode_varint(&mut cursor).map_err(encoding_error)?;
        let length = VarIntCoder::decode_varint(&mut cursor).map_err(encoding_error)?;

        let length = usize::try_from(length).map_err(|_| {
            SchemaError::LogicalTypeEncoding(format!("negative decimal length {length}"))
        })?;

        let unscaled = cursor.get(..length).ok_or_else(|| {
            SchemaError::LogicalTypeEncoding(format!(
                "decimal payload declares {length} bytes but only {} remain",
                cursor.len()
            ))
        })?;

        decimal_from_parts(scale, unscaled)
    }

    fn decode_null() -> Result<Self, CoderError> {
        Err(CoderError::Schema(SchemaError::UnexpectedNull {
            expected: "BYTES (decimal)".to_string(),
        }))
    }

    fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        write_decimal(self, writer);
        Ok(())
    }

    fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let scale = VarIntCoder::decode_varint(reader)?;
        let length = VarIntCoder::decode_varint(reader)?;
        let len = usize::try_from(length)
            .map_err(|_| CoderError::Format(format!("Negative decimal length {length}")))?;
        let unscaled = take_bytes(reader, len)?;
        decimal_from_parts(scale, unscaled).map_err(CoderError::Schema)
    }

    fn matches_type_info(type_info: &TypeInfo) -> bool {
        matches!(type_info, TypeInfo::Logical { urn, .. } if urn == URN_DECIMAL)
    }
}

impl NotNullable for Instant {}

/// `millis_instant`: the milliseconds since the epoch, which is the precision of [`Instant`].
impl BeamField for Instant {
    fn beam_field_type() -> FieldType {
        FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64())
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        Ok(Some(FieldValue::Int64(self.as_millis())))
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        extract_required(value, "INT64 (millis_instant)", |v| match v {
            FieldValue::Int64(millis) => Some(Self::from_millis(*millis)),
            _ => None,
        })
    }

    fn decode_null() -> Result<Self, CoderError> {
        Err(CoderError::Schema(SchemaError::UnexpectedNull {
            expected: "INT64 (millis_instant)".to_string(),
        }))
    }

    fn encode_field(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        writer.extend_from_slice(&self.as_millis().to_be_bytes());
        Ok(())
    }

    fn decode_field(reader: &mut &[u8]) -> Result<Self, CoderError> {
        Ok(Self::from_millis(i64::from_be_bytes(read_array::<8>(
            reader,
        )?)))
    }

    fn matches_type_info(type_info: &TypeInfo) -> bool {
        matches!(type_info, TypeInfo::Logical { urn, .. } if urn == URN_MILLIS_INSTANT)
    }
}
