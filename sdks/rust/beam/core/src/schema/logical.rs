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
//! Beam SDKs. This SDK supports [`URN_MICROS_INSTANT`], [`URN_DATE`], and [`URN_DECIMAL`].
//!
//! # `beam:logical_type:decimal:v1` wire format
//!
//! This SDK encodes the `BYTES` representation of `beam:logical_type:decimal:v1` as
//! `VarInt(scale) || VarInt(len) || two's-complement big-endian unscaled` (`BigDecimalCoder`
//! layout, compatible with the Java expansion service). The Python SDK encodes this logical
//! type as UTF-8 text; use a `STRING` field when sharing decimal values with Python transforms.

use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rust_decimal::Decimal;

use crate::coders::VarIntCoder;

use super::convert::{BeamField, NotNullable};
use super::error::SchemaError;
use super::row::{FieldValue, Row};
use super::types::{Field, FieldType, Schema};

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
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Arc::new(Schema::new(vec![
            Field::new("seconds", FieldType::int64()),
            Field::new("micros", FieldType::int64()),
        ]))
    })
}

/// Reads a non-null `INT64` field by name from a representation row.
fn read_i64(row: &Row, name: &str) -> Result<i64, SchemaError> {
    row.get_i64(name)?
        .ok_or_else(|| SchemaError::UnexpectedNull {
            expected: format!("INT64 field '{name}'"),
        })
}

impl NotNullable for DateTime<Utc> {}

impl BeamField for DateTime<Utc> {
    fn beam_field_type() -> FieldType {
        FieldType::logical(
            URN_MICROS_INSTANT,
            Vec::new(),
            FieldType::row((**micros_instant_representation()).clone()),
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

        Self::from_timestamp(seconds, nanos).ok_or_else(|| SchemaError::ValueOutOfRange {
            value: format!("{seconds}s + {micros}us"),
            target: "DateTime<Utc>".to_string(),
        })
    }
}

impl NotNullable for NaiveDate {}

impl BeamField for NaiveDate {
    fn beam_field_type() -> FieldType {
        FieldType::logical(URN_DATE, Vec::new(), FieldType::int64())
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        // `num_days_from_ce` has a constant offset. Subtract the epoch value to get
        // days since the epoch without a Duration.
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a valid date");
        let days = i64::from(self.num_days_from_ce() - epoch.num_days_from_ce());

        Ok(Some(FieldValue::Int64(days)))
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        let days = extract_required(value, "INT64 (days since epoch)", |v| match v {
            FieldValue::Int64(d) => Some(*d),
            _ => None,
        })?;

        let out_of_range = || SchemaError::ValueOutOfRange {
            value: days.to_string(),
            target: "NaiveDate".to_string(),
        };

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a valid date");
        let offset = i32::try_from(days).map_err(|_| out_of_range())?;

        NaiveDate::from_num_days_from_ce_opt(
            epoch
                .num_days_from_ce()
                .checked_add(offset)
                .ok_or_else(out_of_range)?,
        )
        .ok_or_else(out_of_range)
    }
}

/// Returns the minimal big-endian two's-complement bytes of `value` as the decimal wire format
/// requires. Redundant leading sign bytes are removed, but one sign byte always stays so the
/// sign survives the round trip.
fn to_minimal_twos_complement(value: i128) -> Vec<u8> {
    let full = value.to_be_bytes();

    let redundant = |index: usize| {
        let byte = full[index];
        let next_is_negative = full[index + 1] & 0x80 != 0;
        (byte == 0x00 && !next_is_negative) || (byte == 0xFF && next_is_negative)
    };

    let start = (0..full.len() - 1)
        .take_while(|&index| redundant(index))
        .count();

    full[start..].to_vec()
}

/// Inverse of [`to_minimal_twos_complement`], sign-extending to 128 bits.
fn from_minimal_twos_complement(bytes: &[u8]) -> Result<i128, SchemaError> {
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

impl NotNullable for Decimal {}

impl BeamField for Decimal {
    fn beam_field_type() -> FieldType {
        FieldType::logical(URN_DECIMAL, Vec::new(), FieldType::bytes())
    }

    fn to_field_value(&self) -> Result<Option<FieldValue>, SchemaError> {
        let encoding_error = |e: std::io::Error| SchemaError::LogicalTypeEncoding(e.to_string());

        // Layout: VarInt(scale), then VarInt(length), then the two's-complement bytes.
        let mut buffer = Vec::new();
        VarIntCoder::encode_varint(i64::from(self.scale()), &mut buffer).map_err(encoding_error)?;

        let unscaled = to_minimal_twos_complement(self.mantissa());
        let length = i64::try_from(unscaled.len()).map_err(|_| SchemaError::ValueOutOfRange {
            value: unscaled.len().to_string(),
            target: "i64 length prefix".to_string(),
        })?;
        VarIntCoder::encode_varint(length, &mut buffer).map_err(encoding_error)?;
        buffer.extend_from_slice(&unscaled);

        Ok(Some(FieldValue::Bytes(buffer)))
    }

    fn from_field_value(value: Option<&FieldValue>) -> Result<Self, SchemaError> {
        let bytes = extract_required(value, "BYTES (decimal)", |v| match v {
            FieldValue::Bytes(b) => Some(b),
            _ => None,
        })?;

        let encoding_error = |e: std::io::Error| SchemaError::LogicalTypeEncoding(e.to_string());

        let mut cursor = std::io::Cursor::new(bytes.as_slice());
        let scale = VarIntCoder::decode_varint(&mut cursor).map_err(encoding_error)?;
        let length = VarIntCoder::decode_varint(&mut cursor).map_err(encoding_error)?;

        let start = usize::try_from(cursor.position()).map_err(|_| {
            SchemaError::LogicalTypeEncoding("decimal cursor overflowed".to_string())
        })?;
        let length = usize::try_from(length).map_err(|_| {
            SchemaError::LogicalTypeEncoding(format!("negative decimal length {length}"))
        })?;

        let unscaled = bytes.get(start..start + length).ok_or_else(|| {
            SchemaError::LogicalTypeEncoding(format!(
                "decimal payload declares {length} bytes but only {} remain",
                bytes.len().saturating_sub(start)
            ))
        })?;

        let mantissa = from_minimal_twos_complement(unscaled)?;
        let scale = u32::try_from(scale).map_err(|_| SchemaError::ValueOutOfRange {
            value: scale.to_string(),
            target: "decimal scale".to_string(),
        })?;

        Self::try_from_i128_with_scale(mantissa, scale).map_err(|e| SchemaError::ValueOutOfRange {
            value: format!("{mantissa}e-{scale} ({e})"),
            target: "Decimal".to_string(),
        })
    }
}
