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

//! Type mapping between BigQuery and Beam for the Storage API connectors.
//!
//! Column types of a read map to Beam types as follows. Nested columns follow the same rules.
//!
//! | BigQuery | Beam |
//! |---|---|
//! | `INT64`, `FLOAT64`, `BOOL`, `STRING`, `BYTES` | `INT64`, `DOUBLE`, `BOOLEAN`, `STRING`, `BYTES` |
//! | `NUMERIC`, `BIGNUMERIC` | `decimal` |
//! | `DATE` | `date` |
//! | `TIMESTAMP` | `micros_instant` |
//! | `DATETIME`, `TIME` | `STRING`, as `2024-05-01T12:30:00.000001` and `12:30:00.000001` |
//! | `GEOGRAPHY`, `JSON` | `STRING`, as WKT and JSON text |
//! | `STRUCT`, `ARRAY` | `ROW`, `ARRAY` |
//!
//! Beam has no standard logical type for a date and time without a time zone, or for a time
//! of day, that all SDKs decode. Strings keep the value in every SDK.
//!
//! A write maps the other way. A column of the written rows takes the Arrow type that a read
//! session gives for the table column with the same name, so the write accepts what the read
//! returns:
//!
//! | Beam | BigQuery column |
//! |---|---|
//! | `BYTE`, `INT16`, `INT32`, `INT64` | `INT64` |
//! | `FLOAT`, `DOUBLE` | `FLOAT64` |
//! | `decimal` | `NUMERIC` or `BIGNUMERIC`, at the scale of the column |
//! | `micros_instant`, `millis_instant` | `TIMESTAMP` or `DATETIME` |
//! | `STRING` | `STRING`, `JSON`, `GEOGRAPHY`, and `DATETIME` and `TIME` in the read format |
//! | `ROW`, `ARRAY`, `ITERABLE` | `STRUCT`, `REPEATED` |
//!
//! A table that the write creates gets the first column type in the row of each field type.
//! `MAP` fields and arrays of arrays have no BigQuery column type.

use std::sync::Arc;

use arrow_io::arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, Decimal128Array, Decimal256Array, ListArray,
    PrimitiveArray, RecordBatch, StringArray, StructArray,
    cast::AsArray,
    new_empty_array,
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Time64MicrosecondType,
        TimestampMicrosecondType, TimestampMillisecondType,
    },
};
use arrow_io::arrow_buffer::i256;
use arrow_io::arrow_schema::{
    ArrowError, DataType, Field as ArrowField, Fields, Schema as ArrowSchema, TimeUnit,
};
use beam::schema::{
    AtomicType, FieldType, Row, Schema, TypeInfo, URN_DATE, URN_DECIMAL, URN_MICROS_INSTANT,
    URN_MILLIS_INSTANT,
};
use chrono::{DateTime, NaiveDateTime, NaiveTime, Timelike};
use google_cloud_bigquery_v2::model::TableFieldSchema;

/// Returns the Beam schema for the Arrow schema of a read session.
pub fn beam_schema(arrow_schema: &ArrowSchema) -> beam::Result<Schema> {
    let fields: Fields = arrow_schema
        .fields()
        .iter()
        .map(|f| Arc::new(beam_field(f)))
        .collect();
    arrow_io::arrow_to_beam_schema(&ArrowSchema::new(fields)).map_err(beam::Error::msg)
}

/// Returns the Beam schema for the fields of a BigQuery table schema, as a dry-run job gives
/// them. The fields take the Arrow types of a read session first, so a query read and a table
/// read map types the same way.
pub fn table_schema_to_beam_schema(fields: &[TableFieldSchema]) -> beam::Result<Schema> {
    let fields = fields
        .iter()
        .map(session_field)
        .collect::<beam::Result<Vec<_>>>()?;
    beam_schema(&ArrowSchema::new(fields))
}

/// The Arrow field that a read session gives for a column.
fn session_field(field: &TableFieldSchema) -> beam::Result<ArrowField> {
    let data_type = match field.r#type.as_str() {
        "INTEGER" | "INT64" => DataType::Int64,
        "FLOAT" | "FLOAT64" => DataType::Float64,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "STRING" | "GEOGRAPHY" | "JSON" => DataType::Utf8,
        "BYTES" => DataType::Binary,
        "NUMERIC" => DataType::Decimal128(38, 9),
        "BIGNUMERIC" => DataType::Decimal256(76, 38),
        "DATE" => DataType::Date32,
        "DATETIME" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIME" => DataType::Time64(TimeUnit::Microsecond),
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "RECORD" | "STRUCT" => DataType::Struct(
            field
                .fields
                .iter()
                .map(session_field)
                .collect::<beam::Result<Fields>>()?,
        ),
        other => {
            return Err(beam::Error::msg(format!(
                "Column '{}' has the BigQuery type {other}, which the read does not support",
                field.name
            )));
        }
    };
    Ok(match field.mode.as_str() {
        "REPEATED" => ArrowField::new(
            &field.name,
            DataType::List(Arc::new(ArrowField::new("item", data_type, false))),
            false,
        ),
        "REQUIRED" => ArrowField::new(&field.name, data_type, false),
        _ => ArrowField::new(&field.name, data_type, true),
    })
}

/// The field with the Arrow type that [`beam_array`] produces for it.
fn beam_field(field: &ArrowField) -> ArrowField {
    let data_type = match field.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None)
        | DataType::Time64(TimeUnit::Microsecond) => DataType::Utf8,
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|f| Arc::new(beam_field(f))).collect())
        }
        DataType::List(item) => DataType::List(Arc::new(beam_field(item))),
        other => other.clone(),
    };
    field.clone().with_data_type(data_type)
}

/// Rewrites the columns that have no Beam type as strings: `DATETIME` arrives as a timestamp
/// without a time zone, and `TIME` as microseconds since midnight.
fn beam_array(array: &ArrayRef) -> beam::Result<ArrayRef> {
    Ok(match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None) => Arc::new(strings(
            array.as_primitive::<TimestampMicrosecondType>().iter(),
            |micros| {
                DateTime::from_timestamp_micros(micros)
                    .map(|t| t.naive_utc().format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
            },
        )?),
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(strings(
            array.as_primitive::<Time64MicrosecondType>().iter(),
            |micros| {
                let secs = u32::try_from(micros.div_euclid(1_000_000)).ok()?;
                let nanos = u32::try_from(micros.rem_euclid(1_000_000) * 1_000).ok()?;
                NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .map(|t| t.format("%H:%M:%S%.6f").to_string())
            },
        )?),
        DataType::Struct(fields) => {
            let structs = array.as_struct();
            let columns = structs
                .columns()
                .iter()
                .map(beam_array)
                .collect::<beam::Result<Vec<_>>>()?;
            let fields: Fields = fields.iter().map(|f| Arc::new(beam_field(f))).collect();
            Arc::new(
                StructArray::try_new(fields, columns, structs.nulls().cloned())
                    .map_err(beam::Error::msg)?,
            )
        }
        DataType::List(item) => {
            let lists = array.as_list::<i32>();
            Arc::new(
                ListArray::try_new(
                    Arc::new(beam_field(item)),
                    lists.offsets().clone(),
                    beam_array(lists.values())?,
                    lists.nulls().cloned(),
                )
                .map_err(beam::Error::msg)?,
            )
        }
        _ => Arc::clone(array),
    })
}

fn strings(
    values: impl Iterator<Item = Option<i64>>,
    format: impl Fn(i64) -> Option<String>,
) -> beam::Result<StringArray> {
    values
        .map(|value| {
            value
                .map(|v| {
                    format(v).ok_or_else(|| beam::Error::msg(format!("Value {v} is out of range")))
                })
                .transpose()
        })
        .collect()
}

/// Converts a record batch of a read session into Beam rows of `schema`.
pub fn batch_to_rows(batch: &RecordBatch, schema: &Arc<Schema>) -> beam::Result<Vec<Row>> {
    arrow_io::record_batch_to_rows(&beam_batch(batch)?, schema).map_err(beam::Error::msg)
}

/// Converts a record batch of a read session into the row coder bytes of each row, as rows
/// of `schema`. The values are the values of [`batch_to_rows`], with no [`Row`] between.
pub fn batch_to_encoded_rows(
    batch: &RecordBatch,
    schema: &Arc<Schema>,
) -> beam::Result<Vec<Vec<u8>>> {
    arrow_io::record_batch_to_encoded_rows(&beam_batch(batch)?, schema).map_err(beam::Error::msg)
}

/// The batch with the columns that have no Beam type rewritten, as [`beam_array`] does.
fn beam_batch(batch: &RecordBatch) -> beam::Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(beam_array)
        .collect::<beam::Result<Vec<_>>>()?;
    let fields: Fields = batch
        .schema()
        .fields()
        .iter()
        .map(|f| Arc::new(beam_field(f)))
        .collect();
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).map_err(beam::Error::msg)
}

/// Returns the Arrow schema that a read session gives for the columns of a table.
pub fn table_arrow_schema(fields: &[TableFieldSchema]) -> beam::Result<ArrowSchema> {
    fields
        .iter()
        .map(session_field)
        .collect::<beam::Result<Vec<_>>>()
        .map(ArrowSchema::new)
}

/// Returns the columns of a new table for rows of `schema`.
pub fn table_fields(schema: &Schema) -> beam::Result<Vec<TableFieldSchema>> {
    schema
        .fields
        .iter()
        .map(|field| table_field(&field.name, &field.field_type))
        .collect()
}

fn table_field(name: &str, field_type: &FieldType) -> beam::Result<TableFieldSchema> {
    let (element, mode) = match &field_type.type_info {
        TypeInfo::Array(element) | TypeInfo::Iterable(element) => (element.as_ref(), "REPEATED"),
        _ if field_type.nullable => (field_type, "NULLABLE"),
        _ => (field_type, "REQUIRED"),
    };
    let (column_type, fields) = column_type(name, element)?;
    Ok(TableFieldSchema::new()
        .set_name(name)
        .set_type(column_type)
        .set_mode(mode)
        .set_fields(fields))
}

fn column_type(
    name: &str,
    field_type: &FieldType,
) -> beam::Result<(&'static str, Vec<TableFieldSchema>)> {
    let column = match &field_type.type_info {
        TypeInfo::Atomic(
            AtomicType::Byte | AtomicType::Int16 | AtomicType::Int32 | AtomicType::Int64,
        ) => "INT64",
        TypeInfo::Atomic(AtomicType::Float | AtomicType::Double) => "FLOAT64",
        TypeInfo::Atomic(AtomicType::String) => "STRING",
        TypeInfo::Atomic(AtomicType::Boolean) => "BOOL",
        TypeInfo::Atomic(AtomicType::Bytes) => "BYTES",
        TypeInfo::Row(schema) => return Ok(("STRUCT", table_fields(schema)?)),
        TypeInfo::Logical { urn, .. } if urn == URN_MICROS_INSTANT || urn == URN_MILLIS_INSTANT => {
            "TIMESTAMP"
        }
        TypeInfo::Logical { urn, .. } if urn == URN_DATE => "DATE",
        TypeInfo::Logical { urn, .. } if urn == URN_DECIMAL => "NUMERIC",
        TypeInfo::Logical { representation, .. } => return column_type(name, representation),
        TypeInfo::Array(_) | TypeInfo::Iterable(_) | TypeInfo::Map(..) => {
            return Err(beam::Error::msg(format!(
                "Field '{name}' has the type {field_type}, which no BigQuery column holds"
            )));
        }
    };
    Ok((column, Vec::new()))
}

/// Returns the field that a write sends for `source`, a field of the written rows, into the
/// table column whose read session field is `column`. Fails if the write cannot convert the
/// type, so a bad pair fails once, before any row.
pub fn write_field(source: &ArrowField, column: &ArrowField) -> beam::Result<ArrowField> {
    let data_type = match (source.data_type(), column.data_type()) {
        (DataType::Struct(sources), DataType::Struct(columns)) => DataType::Struct(
            sources
                .iter()
                .map(|source| {
                    let column = find_field(columns, source.name())?;
                    write_field(source, column).map(Arc::new)
                })
                .collect::<beam::Result<Fields>>()?,
        ),
        (DataType::List(source), DataType::List(column)) => {
            DataType::List(Arc::new(write_field(source, column)?))
        }
        (_, column_type) => column_type.clone(),
    };
    write_array(&new_empty_array(source.data_type()), &data_type)
        .map_err(|e| e.context(format!("Field '{}'", source.name())))?;
    Ok(ArrowField::new(
        column.name(),
        data_type,
        column.is_nullable(),
    ))
}

/// Finds a field by name. BigQuery column names do not depend on case.
pub fn find_field<'a>(fields: &'a Fields, name: &str) -> beam::Result<&'a ArrowField> {
    fields
        .iter()
        .find(|field| field.name().eq_ignore_ascii_case(name))
        .map(AsRef::as_ref)
        .ok_or_else(|| beam::Error::msg(format!("The table has no column '{name}'")))
}

/// Converts a column of the written rows into `target`, the type of [`write_field`].
pub fn write_array(array: &ArrayRef, target: &DataType) -> beam::Result<ArrayRef> {
    Ok(match (array.data_type(), target) {
        (source, target) if source == target => Arc::clone(array),
        (DataType::Int8, DataType::Int64) => Arc::new(
            array
                .as_primitive::<Int8Type>()
                .unary::<_, Int64Type>(i64::from),
        ),
        (DataType::Int16, DataType::Int64) => Arc::new(
            array
                .as_primitive::<Int16Type>()
                .unary::<_, Int64Type>(i64::from),
        ),
        (DataType::Int32, DataType::Int64) => Arc::new(
            array
                .as_primitive::<Int32Type>()
                .unary::<_, Int64Type>(i64::from),
        ),
        (DataType::Float32, DataType::Float64) => Arc::new(
            array
                .as_primitive::<Float32Type>()
                .unary::<_, Float64Type>(f64::from),
        ),
        (DataType::Binary, &DataType::Decimal128(precision, scale)) => {
            let values = decimals(array.as_binary::<i32>(), precision, scale)?
                .into_iter()
                .map(|value| value.and_then(|v| v.to_i128()))
                .collect::<Vec<_>>();
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .map_err(beam::Error::msg)?,
            )
        }
        (DataType::Binary, &DataType::Decimal256(precision, scale)) => Arc::new(
            Decimal256Array::from(decimals(array.as_binary::<i32>(), precision, scale)?)
                .with_precision_and_scale(precision, scale)
                .map_err(beam::Error::msg)?,
        ),
        (DataType::Utf8, DataType::Timestamp(TimeUnit::Microsecond, None)) => {
            Arc::new(parse::<TimestampMicrosecondType>(
                array.as_string::<i32>(),
                datetime_micros,
            )?)
        }
        (DataType::Utf8, DataType::Time64(TimeUnit::Microsecond)) => {
            Arc::new(parse::<Time64MicrosecondType>(
                array.as_string::<i32>(),
                time_micros,
            )?)
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, zone),
        ) => Arc::new(
            // A clone of an Arrow array shares its buffers.
            array
                .as_primitive::<TimestampMicrosecondType>()
                .clone()
                .with_timezone_opt(zone.clone()),
        ),
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, zone),
        ) => Arc::new(
            array
                .as_primitive::<TimestampMillisecondType>()
                .try_unary::<_, TimestampMicrosecondType, _>(|millis| {
                    millis.checked_mul(1_000).ok_or_else(|| {
                        ArrowError::ComputeError(format!("Timestamp {millis} is out of range"))
                    })
                })
                .map_err(beam::Error::msg)?
                .with_timezone_opt(zone.clone()),
        ),
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let structs = array.as_struct();
            let columns = structs
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(column, field)| write_array(column, field.data_type()))
                .collect::<beam::Result<Vec<_>>>()?;
            Arc::new(
                StructArray::try_new(fields.clone(), columns, structs.nulls().cloned())
                    .map_err(beam::Error::msg)?,
            )
        }
        (DataType::List(_), DataType::List(item)) => {
            let lists = array.as_list::<i32>();
            Arc::new(
                ListArray::try_new(
                    Arc::clone(item),
                    lists.offsets().clone(),
                    write_array(lists.values(), item.data_type())?,
                    lists.nulls().cloned(),
                )
                .map_err(beam::Error::msg)?,
            )
        }
        (source, target) => {
            return Err(beam::Error::msg(format!(
                "The write cannot convert Arrow type {source} into the column type {target}"
            )));
        }
    })
}

/// Converts columns that [`write_array`] wrote back into columns of `source`, the Arrow
/// types of the written rows, and then into Beam rows of `schema`. Rows that BigQuery rejects
/// go back to the caller this way.
pub fn write_batch_to_rows(batch: &RecordBatch, schema: &Arc<Schema>) -> beam::Result<Vec<Row>> {
    let source = arrow_io::beam_to_arrow_schema(schema).map_err(beam::Error::msg)?;
    let columns = batch
        .columns()
        .iter()
        .zip(source.fields())
        .map(|(column, field)| restore_array(column, field.data_type()))
        .collect::<beam::Result<Vec<_>>>()?;
    let fields: Fields = columns
        .iter()
        .zip(source.fields())
        .map(|(column, field)| ArrowField::new(field.name(), column.data_type().clone(), true))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
        .map_err(beam::Error::msg)?;
    arrow_io::record_batch_to_rows(&batch, schema).map_err(beam::Error::msg)
}

/// The inverse of [`write_array`]. Decimals stay decimal arrays, which the row conversion
/// reads for `decimal` fields.
fn restore_array(array: &ArrayRef, source: &DataType) -> beam::Result<ArrayRef> {
    let narrow = |array: &ArrayRef| array.as_primitive::<Int64Type>().clone();
    Ok(match (array.data_type(), source) {
        (written, source) if written == source => Arc::clone(array),
        // The values came from the narrower type, so `as` keeps them.
        (DataType::Int64, DataType::Int8) => {
            Arc::new(narrow(array).unary::<_, Int8Type>(|v| v as i8))
        }
        (DataType::Int64, DataType::Int16) => {
            Arc::new(narrow(array).unary::<_, Int16Type>(|v| v as i16))
        }
        (DataType::Int64, DataType::Int32) => {
            Arc::new(narrow(array).unary::<_, Int32Type>(|v| v as i32))
        }
        (DataType::Float64, DataType::Float32) => Arc::new(
            array
                .as_primitive::<Float64Type>()
                .unary::<_, Float32Type>(|v| v as f32),
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None)
            | DataType::Time64(TimeUnit::Microsecond),
            DataType::Utf8,
        ) => beam_array(array)?,
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, zone),
        ) => Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .clone()
                .with_timezone_opt(zone.clone()),
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Millisecond, zone),
        ) => Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .unary::<_, TimestampMillisecondType>(|micros| micros.div_euclid(1_000))
                .with_timezone_opt(zone.clone()),
        ),
        (DataType::Struct(_), DataType::Struct(sources)) => {
            let structs = array.as_struct();
            let columns = structs
                .columns()
                .iter()
                .zip(sources.iter())
                .map(|(column, source)| restore_array(column, source.data_type()))
                .collect::<beam::Result<Vec<_>>>()?;
            let fields: Fields = columns
                .iter()
                .zip(sources.iter())
                .map(|(column, source)| {
                    ArrowField::new(source.name(), column.data_type().clone(), true)
                })
                .collect();
            Arc::new(
                StructArray::try_new(fields, columns, structs.nulls().cloned())
                    .map_err(beam::Error::msg)?,
            )
        }
        (DataType::List(_), DataType::List(source)) => {
            let lists = array.as_list::<i32>();
            let values = restore_array(lists.values(), source.data_type())?;
            Arc::new(
                ListArray::try_new(
                    Arc::new(ArrowField::new(
                        source.name(),
                        values.data_type().clone(),
                        true,
                    )),
                    lists.offsets().clone(),
                    values,
                    lists.nulls().cloned(),
                )
                .map_err(beam::Error::msg)?,
            )
        }
        _ => Arc::clone(array),
    })
}

fn parse<T: ArrowPrimitiveType<Native = i64>>(
    strings: &StringArray,
    value: impl Fn(&str) -> Option<i64>,
) -> beam::Result<PrimitiveArray<T>> {
    strings
        .iter()
        .map(|text| {
            text.map(|text| {
                value(text).ok_or_else(|| beam::Error::msg(format!("Invalid value '{text}'")))
            })
            .transpose()
        })
        .collect()
}

/// Parses a `DATETIME` in the format of the read, or with a space before the time.
fn datetime_micros(text: &str) -> Option<i64> {
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
        .iter()
        .find_map(|format| NaiveDateTime::parse_from_str(text, format).ok())
        .map(|time| time.and_utc().timestamp_micros())
}

fn time_micros(text: &str) -> Option<i64> {
    NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .ok()
        .map(|time| {
            i64::from(time.num_seconds_from_midnight()) * 1_000_000
                + i64::from(time.nanosecond() / 1_000)
        })
}

/// Rescales Beam `decimal` payloads to `scale`. Fails for a value that loses digits or does
/// not fit in `precision` digits.
fn decimals(payloads: &BinaryArray, precision: u8, scale: i8) -> beam::Result<Vec<Option<i256>>> {
    let limit = pow10(i64::from(precision))
        .ok_or_else(|| beam::Error::msg(format!("Invalid precision {precision}")))?;
    payloads
        .iter()
        .map(|payload| {
            payload
                .map(|payload| {
                    decimal(payload, scale)
                        .filter(|value| *value < limit && *value > limit.wrapping_neg())
                        .ok_or_else(|| {
                            beam::Error::msg(format!(
                                "A decimal does not fit in NUMERIC({precision}, {scale})"
                            ))
                        })
                })
                .transpose()
        })
        .collect()
}

/// Reads a `decimal` payload (VarInt scale, VarInt length, big-endian two's complement) as
/// an unscaled value at `scale`.
fn decimal(payload: &[u8], scale: i8) -> Option<i256> {
    let (value_scale, rest) = varint(payload)?;
    let (length, rest) = varint(rest)?;
    let digits = rest.get(..usize::try_from(length).ok()?)?;
    let fill = if digits.first().is_some_and(|b| b & 0x80 != 0) {
        0xFF
    } else {
        0x00
    };
    let mut bytes = [fill; 32];
    bytes
        .get_mut(32usize.checked_sub(digits.len())?..)?
        .copy_from_slice(digits);
    let unscaled = i256::from_be_bytes(bytes);
    match i64::from(scale) - value_scale {
        shift if shift >= 0 => unscaled.checked_mul(pow10(shift)?),
        shift => {
            let divisor = pow10(-shift)?;
            (unscaled.checked_rem(divisor)? == i256::ZERO)
                .then(|| unscaled.checked_div(divisor))
                .flatten()
        }
    }
}

fn pow10(exponent: i64) -> Option<i256> {
    i256::from_i128(10).checked_pow(u32::try_from(exponent).ok()?)
}

/// Reads an unsigned LEB128 varint as the `i64` that Beam encodes.
fn varint(bytes: &[u8]) -> Option<(i64, &[u8])> {
    let end = bytes.iter().take(10).position(|b| b & 0x80 == 0)?;
    let value = bytes[..=end]
        .iter()
        .rev()
        .fold(0u64, |value, b| (value << 7) | u64::from(b & 0x7F));
    Some((value as i64, &bytes[end + 1..]))
}
