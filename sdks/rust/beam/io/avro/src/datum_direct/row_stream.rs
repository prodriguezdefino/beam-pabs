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

//! Writes an Avro datum from Beam row coder bytes. The core [`RowWalk`] reads the bytes, and
//! the write operation of each value writes it.

use beam::coders::CoderError;
use beam::internals::{RowVisitor, RowWalk, WireValue};

use super::encoder::{
    avro_count, date_days, insert_zigzag, instant_micros, write_avro_bytes, write_enum, write_null,
    write_present_tag, write_zigzag,
};
use super::schema::{InstantPart, ValueKind, WriteOp};

/// Writes the row in `bytes` with the compiled `walk`.
pub(super) fn stream_row(
    walk: &RowWalk<WriteOp>,
    mut bytes: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let mut writer = AvroWriter {
        out: std::mem::take(out),
        instant: None,
    };
    let result = walk.walk(&mut bytes, &mut writer);
    *out = writer.out;
    result.map_err(|StreamError(message)| message)
}

/// An error message. The walk returns it at each level, so it is smaller than a `CoderError`.
pub(super) struct StreamError(String);

impl From<CoderError> for StreamError {
    fn from(error: CoderError) -> Self {
        match error {
            CoderError::Format(message) => Self(message),
            error @ (CoderError::Io(_)
            | CoderError::Utf8(_)
            | CoderError::Schema(_)
            | CoderError::StateStream(_)) => Self(error.to_string()),
        }
    }
}

/// The Avro output, and the first field of the `micros_instant` value that the walk is in.
struct AvroWriter {
    out: Vec<u8>,
    instant: Option<(InstantPart, i64)>,
}

impl RowVisitor for AvroWriter {
    type Op = WriteOp;
    /// The output position of a block count that is known only at the end.
    type Mark = Option<usize>;
    type Error = StreamError;

    #[inline]
    fn null(&mut self, op: &WriteOp) -> Result<(), StreamError> {
        write_null(op, &mut self.out).map_err(StreamError)
    }

    #[inline]
    fn present(&mut self, op: &WriteOp) {
        write_present_tag(op.avro_null, &mut self.out);
    }

    #[inline]
    fn value(&mut self, op: &WriteOp, value: WireValue<'_>) -> Result<(), StreamError> {
        self.write_value(op, value).map_err(StreamError)
    }

    fn begin_container(
        &mut self,
        op: &WriteOp,
        count: Option<usize>,
    ) -> Result<Option<usize>, StreamError> {
        match count {
            Some(0) => Ok(None),
            Some(count) => {
                let count = avro_count(&op.name, count).map_err(StreamError)?;
                write_zigzag(&mut self.out, count);
                Ok(None)
            }
            None => Ok(Some(self.out.len())),
        }
    }

    fn end_container(
        &mut self,
        op: &WriteOp,
        mark: Option<usize>,
        count: usize,
    ) -> Result<(), StreamError> {
        // A chunked iterable writes one Avro block, as a counted one does.
        if let Some(start) = mark
            && count > 0
        {
            let count = avro_count(&op.name, count).map_err(StreamError)?;
            insert_zigzag(&mut self.out, start, count);
        }
        self.out.push(0x00);
        Ok(())
    }
}

impl AvroWriter {
    #[inline]
    fn write_value(&mut self, op: &WriteOp, value: WireValue<'_>) -> Result<(), String> {
        let name = op.name.as_str();
        let out = &mut self.out;
        let mismatch =
            |expected: &str| format!("Field '{name}': expected {expected}, got {value:?}");
        match &op.kind {
            ValueKind::Boolean => {
                let WireValue::Boolean(v) = value else {
                    return Err(mismatch("BOOLEAN"));
                };
                out.push(u8::from(v));
            }
            ValueKind::ByteToInt => {
                let WireValue::Byte(v) = value else {
                    return Err(mismatch("BYTE"));
                };
                write_zigzag(out, i64::from(v));
            }
            ValueKind::Int16ToInt => {
                let WireValue::Int16(v) = value else {
                    return Err(mismatch("INT16"));
                };
                write_zigzag(out, i64::from(v));
            }
            ValueKind::Int32ToInt => {
                let WireValue::Int32(v) = value else {
                    return Err(mismatch("INT32"));
                };
                write_zigzag(out, i64::from(v));
            }
            ValueKind::Int64ToLong => {
                let WireValue::Int64(v) = value else {
                    return Err(mismatch("INT64"));
                };
                write_zigzag(out, v);
            }
            ValueKind::Float => {
                let WireValue::Float(v) = value else {
                    return Err(mismatch("FLOAT"));
                };
                out.extend_from_slice(&v.to_le_bytes());
            }
            ValueKind::FloatToDouble => {
                let WireValue::Float(v) = value else {
                    return Err(mismatch("FLOAT"));
                };
                out.extend_from_slice(&f64::from(v).to_le_bytes());
            }
            ValueKind::Double => {
                let WireValue::Double(v) = value else {
                    return Err(mismatch("DOUBLE"));
                };
                out.extend_from_slice(&v.to_le_bytes());
            }
            ValueKind::String => {
                let WireValue::String(v) = value else {
                    return Err(mismatch("STRING"));
                };
                write_avro_bytes(out, v.as_bytes());
            }
            ValueKind::Bytes => {
                let WireValue::Bytes(v) = value else {
                    return Err(mismatch("BYTES"));
                };
                write_avro_bytes(out, v);
            }
            ValueKind::Enum(symbols) => {
                let WireValue::String(v) = value else {
                    return Err(mismatch("STRING"));
                };
                write_enum(symbols, v, out)?;
            }
            ValueKind::Date => {
                let WireValue::Int64(days) = value else {
                    return Err(mismatch("INT64 (date)"));
                };
                write_zigzag(out, i64::from(date_days(name, days)?));
            }
            ValueKind::MillisInstant => {
                let WireValue::Int64(millis) = value else {
                    return Err(mismatch("INT64 (millis_instant)"));
                };
                write_zigzag(out, millis);
            }
            ValueKind::InstantPart(part) => {
                let part_value = match value {
                    WireValue::Int64(v) => v,
                    WireValue::Int32(v) if *part == InstantPart::Micros => i64::from(v),
                    WireValue::Boolean(_)
                    | WireValue::Byte(_)
                    | WireValue::Int16(_)
                    | WireValue::Int32(_)
                    | WireValue::Float(_)
                    | WireValue::Double(_)
                    | WireValue::String(_)
                    | WireValue::Bytes(_)
                    | WireValue::Decimal { .. } => {
                        return Err(mismatch("INT64 (micros_instant part)"));
                    }
                };
                match (self.instant.take(), part) {
                    (None, part) => self.instant = Some((*part, part_value)),
                    (Some((InstantPart::Seconds, seconds)), InstantPart::Micros) => {
                        write_zigzag(out, instant_micros(name, seconds, part_value)?);
                    }
                    (Some((InstantPart::Micros, micros)), InstantPart::Seconds) => {
                        write_zigzag(out, instant_micros(name, part_value, micros)?);
                    }
                    (Some((InstantPart::Seconds, _)), InstantPart::Seconds)
                    | (Some((InstantPart::Micros, _)), InstantPart::Micros) => {
                        return Err(mismatch("the other micros_instant part"));
                    }
                }
            }
            ValueKind::MicrosInstant => return Err(mismatch("micros_instant row")),
            ValueKind::Array => return Err(mismatch("ARRAY")),
            ValueKind::Map => return Err(mismatch("MAP")),
            ValueKind::Record => return Err(mismatch("ROW")),
        }
        Ok(())
    }
}
