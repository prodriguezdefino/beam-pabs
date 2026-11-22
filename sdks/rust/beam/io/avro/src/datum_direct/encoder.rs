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

//! Writes the values of a [`Row`] as an Avro datum, and the Avro primitives that the walk
//! over row coder bytes also uses.

use std::collections::HashMap;

use beam::schema::{FieldValue, Row};

use super::schema::{AvroNull, ValueKind, ValuePlan, WriteOp};

const MICROS_PER_SECOND: i64 = 1_000_000;

/// Writes `row` with the record plan `plan`.
pub(super) fn write_row(plan: &ValuePlan, row: &Row, out: &mut Vec<u8>) -> Result<(), String> {
    let fields = &plan.children;
    if row.values().len() != fields.len() {
        return Err(format!(
            "Row value count mismatch for '{}': expected {}, got {}",
            plan.op.name,
            fields.len(),
            row.values().len(),
        ));
    }
    fields
        .iter()
        .zip(row.values())
        .try_for_each(|(field, value)| write_field(field, value.as_ref(), out))
}

#[inline]
fn write_field(
    plan: &ValuePlan,
    value: Option<&FieldValue>,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    match value {
        None => write_null(&plan.op, out),
        Some(value) => {
            write_present_tag(plan.op.avro_null, out);
            write_value(plan, value, out)
        }
    }
}

fn write_value(plan: &ValuePlan, value: &FieldValue, out: &mut Vec<u8>) -> Result<(), String> {
    let name = plan.op.name.as_str();
    match &plan.op.kind {
        ValueKind::Boolean => match value {
            FieldValue::Boolean(v) => out.push(u8::from(*v)),
            other => return Err(type_mismatch(name, "BOOLEAN", other)),
        },
        ValueKind::ByteToInt => match value {
            FieldValue::Byte(v) => write_zigzag(out, i64::from(*v)),
            other => return Err(type_mismatch(name, "BYTE", other)),
        },
        ValueKind::Int16ToInt => match value {
            FieldValue::Int16(v) => write_zigzag(out, i64::from(*v)),
            other => return Err(type_mismatch(name, "INT16", other)),
        },
        ValueKind::Int32ToInt => match value {
            FieldValue::Int32(v) => write_zigzag(out, i64::from(*v)),
            other => return Err(type_mismatch(name, "INT32", other)),
        },
        ValueKind::Int64ToLong => match value {
            FieldValue::Int64(v) => write_zigzag(out, *v),
            other => return Err(type_mismatch(name, "INT64", other)),
        },
        ValueKind::Float => match value {
            FieldValue::Float(v) => out.extend_from_slice(&v.to_le_bytes()),
            other => return Err(type_mismatch(name, "FLOAT", other)),
        },
        ValueKind::FloatToDouble => match value {
            FieldValue::Float(v) => out.extend_from_slice(&f64::from(*v).to_le_bytes()),
            other => return Err(type_mismatch(name, "FLOAT", other)),
        },
        ValueKind::Double => match value {
            FieldValue::Double(v) => out.extend_from_slice(&v.to_le_bytes()),
            other => return Err(type_mismatch(name, "DOUBLE", other)),
        },
        ValueKind::String => match value {
            FieldValue::String(v) => write_avro_bytes(out, v.as_bytes()),
            other => return Err(type_mismatch(name, "STRING", other)),
        },
        ValueKind::Bytes => match value {
            FieldValue::Bytes(v) => write_avro_bytes(out, v),
            other => return Err(type_mismatch(name, "BYTES", other)),
        },
        ValueKind::Enum(symbols) => match value {
            FieldValue::String(v) => write_enum(symbols, v, out)?,
            other => return Err(type_mismatch(name, "STRING", other)),
        },
        ValueKind::Date => match value {
            FieldValue::Int64(days) => write_zigzag(out, i64::from(date_days(name, *days)?)),
            other => return Err(type_mismatch(name, "INT64 (date)", other)),
        },
        ValueKind::MillisInstant => match value {
            FieldValue::Int64(millis) => write_zigzag(out, *millis),
            other => return Err(type_mismatch(name, "INT64 (millis_instant)", other)),
        },
        ValueKind::MicrosInstant => match value {
            FieldValue::Row(row) => write_zigzag(out, row_instant_micros(name, row, value)?),
            other => return Err(type_mismatch(name, "micros_instant row", other)),
        },
        ValueKind::InstantPart(_) => {
            return Err(type_mismatch(name, "micros_instant row", value));
        }
        ValueKind::Array => match value {
            FieldValue::Array(items) => {
                let [item_plan] = &*plan.children else {
                    return Err(format!("Field '{name}' has no item plan"));
                };
                if !items.is_empty() {
                    write_zigzag(out, avro_count(name, items.len())?);
                    items
                        .iter()
                        .try_for_each(|item| write_field(item_plan, item.as_ref(), out))?;
                }
                out.push(0x00);
            }
            other => return Err(type_mismatch(name, "ARRAY", other)),
        },
        ValueKind::Map => match value {
            FieldValue::Map(entries) => {
                let [_, value_plan] = &*plan.children else {
                    return Err(format!("Field '{name}' has no value plan"));
                };
                if !entries.is_empty() {
                    write_zigzag(out, avro_count(name, entries.len())?);
                    for (key, entry) in entries {
                        let FieldValue::String(key) = key else {
                            return Err(type_mismatch(name, "STRING map key", key));
                        };
                        write_avro_bytes(out, key.as_bytes());
                        write_field(value_plan, entry.as_ref(), out)?;
                    }
                }
                out.push(0x00);
            }
            other => return Err(type_mismatch(name, "MAP", other)),
        },
        ValueKind::Record => match value {
            FieldValue::Row(row) => write_row(plan, row, out)?,
            other => return Err(type_mismatch(name, "ROW", other)),
        },
    }
    Ok(())
}

fn row_instant_micros(name: &str, row: &Row, value: &FieldValue) -> Result<i64, String> {
    match row.values() {
        [
            Some(FieldValue::Int64(seconds)),
            Some(FieldValue::Int64(micros)),
        ] => instant_micros(name, *seconds, *micros),
        [
            Some(FieldValue::Int64(seconds)),
            Some(FieldValue::Int32(micros)),
        ] => instant_micros(name, *seconds, i64::from(*micros)),
        _ => Err(type_mismatch(name, "micros_instant row", value)),
    }
}

#[inline]
pub(super) fn write_null(plan: &WriteOp, out: &mut Vec<u8>) -> Result<(), String> {
    if !plan.beam_nullable {
        return Err(format!("Field '{}' is not nullable", plan.name));
    }
    match plan.avro_null {
        AvroNull::None => Err(format!("Avro field '{}' is not nullable", plan.name)),
        AvroNull::NullFirst => {
            out.push(0x00);
            Ok(())
        }
        AvroNull::NullSecond => {
            out.push(0x02);
            Ok(())
        }
    }
}

#[inline]
pub(super) fn write_present_tag(avro_null: AvroNull, out: &mut Vec<u8>) {
    match avro_null {
        AvroNull::None => {}
        AvroNull::NullFirst => out.push(0x02),
        AvroNull::NullSecond => out.push(0x00),
    }
}

/// Writes `value` in the Avro zig-zag base-128 varint encoding.
#[inline]
pub(super) fn write_zigzag(out: &mut Vec<u8>, value: i64) {
    let n = zigzag(value);
    if n < 0x80 {
        out.push(n as u8);
    } else {
        let (buf, len) = varint_bytes(n);
        out.extend_from_slice(&buf[..len]);
    }
}

/// Inserts `value` at `start` in the Avro zig-zag varint encoding.
pub(super) fn insert_zigzag(out: &mut Vec<u8>, start: usize, value: i64) {
    let (buf, len) = varint_bytes(zigzag(value));
    out.splice(start..start, buf[..len].iter().copied());
}

#[inline]
fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

#[inline]
fn varint_bytes(mut n: u64) -> ([u8; 10], usize) {
    let mut buf = [0u8; 10];
    let mut len = 0;
    while n >= 0x80 {
        buf[len] = (n as u8) | 0x80;
        n >>= 7;
        len += 1;
    }
    buf[len] = n as u8;
    (buf, len + 1)
}

#[inline]
pub(super) fn write_avro_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    write_zigzag(out, bytes.len() as i64);
    out.extend_from_slice(bytes);
}

/// Converts the element count of an array or map block.
pub(super) fn avro_count(name: &str, count: usize) -> Result<i64, String> {
    i64::try_from(count).map_err(|_| format!("Field '{name}' element count {count} exceeds i64"))
}

pub(super) fn write_enum(
    symbols: &HashMap<String, i32>,
    value: &str,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let index = symbols
        .get(value)
        .copied()
        .ok_or_else(|| format!("'{value}' is not a symbol of the enum"))?;
    write_zigzag(out, i64::from(index));
    Ok(())
}

pub(super) fn date_days(name: &str, days: i64) -> Result<i32, String> {
    i32::try_from(days).map_err(|_| format!("Field '{name}' date {days} is outside i32 range"))
}

pub(super) fn instant_micros(name: &str, seconds: i64, micros: i64) -> Result<i64, String> {
    seconds
        .checked_mul(MICROS_PER_SECOND)
        .and_then(|s| s.checked_add(micros))
        .ok_or_else(|| format!("Field '{name}' timestamp {seconds}s + {micros}us is out of range"))
}

fn type_mismatch(name: &str, expected: &str, actual: &FieldValue) -> String {
    format!("Field '{name}': expected {expected}, got {actual:?}")
}
