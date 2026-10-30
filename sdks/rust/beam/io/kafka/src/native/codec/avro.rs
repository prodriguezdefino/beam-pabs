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

//! Record values that are Avro binary datums ([`Avro`]).
//!
//! The codec reads a datum straight into a `Row`, and writes a `Row` straight into a datum,
//! with no intermediate record or batch. The Avro schema maps to the Beam schema as Java's
//! `AvroUtils` maps it:
//!
//! | Avro | Beam |
//! |---|---|
//! | `boolean`, `int`, `long`, `float`, `double`, `string`, `bytes` | `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `STRING`, `BYTES` |
//! | `fixed` | `BYTES` |
//! | `enum` | `STRING` (the symbol) |
//! | `array`, `map` | `ARRAY`, `MAP<STRING, _>` |
//! | `record` | `ROW` |
//! | `["null", T]` or `[T, "null"]` | `T`, nullable |
//! | `date` | `beam:logical_type:date:v1` (days, `INT64`) |
//! | `timestamp-millis` | `beam:logical_type:millis_instant:v1` (`INT64`) |
//! | `timestamp-micros` | `INT64` (microseconds) |
//! | other logical types | their Avro type |
//!
//! Other unions, and recursive types, are not supported.

use std::collections::HashMap;
use std::sync::Arc;

use beam::schema::{AtomicType, Field, FieldType, FieldValue, Row, RowElement, Schema, TypeInfo};
use beam::values::PCollection;
use serde_json::Value as Json;

use super::row_input::RowInput;
use super::{CodecError, Decoder, Encoder, Outgoing, RecordRef};

const URN_DATE: &str = "beam:logical_type:date:v1";
const URN_MILLIS_INSTANT: &str = "beam:logical_type:millis_instant:v1";

/// Why a read of this codec with no schema fails.
const NO_SCHEMA: &str = "An Avro read needs an Avro schema: use Avro::schema";

/// The first byte of a value in the Confluent wire format, before the 4-byte schema ID.
const CONFLUENT_MAGIC: u8 = 0;

/// How the datum sits in the record value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Framing {
    /// The datum alone, as the Managed `AVRO` format writes it.
    #[default]
    Plain,
    /// The Confluent wire format: byte `0`, the schema ID (4 bytes, big-endian), the datum.
    /// The read skips the header (and checks the ID if it has one); the write puts it in front.
    Confluent { schema_id: Option<u32> },
}

/// Record values that are Avro binary datums, read into and written from `Row`s.
///
/// The read needs the Avro schema of the datums ([`Avro::schema`]). The write takes rows of
/// any schema ([`Avro::new`]) and writes the datum of the Avro schema of their Beam schema, or
/// of a given Avro schema ([`Avro::schema`]) whose fields match the fields of the rows.
#[derive(Clone, Debug, Default)]
pub struct Avro {
    /// The type of the datums, from the Avro schema or from the schema of the input.
    record: Option<Arc<RecordType>>,
    framing: Framing,
    input: RowInput,
}

impl Avro {
    /// The codec of a write: rows of any schema, as datums of the Avro schema of their Beam
    /// schema (a nullable field is the union `["null", T]`).
    pub fn new() -> Self {
        Self::default()
    }

    /// The codec of the datums of the Avro schema `avro_schema`, a record schema in JSON.
    ///
    /// # Errors
    ///
    /// Fails if `avro_schema` is not a record schema that maps to a Beam schema.
    pub fn schema(avro_schema: &str) -> beam::Result<Self> {
        let json: Json = serde_json::from_str(avro_schema)
            .map_err(|e| beam::Error::msg(format!("The Avro schema is not JSON: {e}")))?;
        let mut names = HashMap::new();
        match parse(&json, &mut names).map_err(beam::Error::msg)? {
            AvroType::Record(record) => Ok(Self {
                record: Some(record),
                framing: Framing::Plain,
                input: RowInput::default(),
            }),
            _ => Err(beam::Error::msg("The Avro schema must be a record")),
        }
    }

    /// The same codec, with the datums in the record values as `framing` says.
    pub fn with_framing(mut self, framing: Framing) -> Self {
        self.framing = framing;
        self
    }

    /// The Beam schema of the rows, if the codec has an Avro schema.
    pub fn row_schema(&self) -> Option<Arc<Schema>> {
        self.record
            .as_ref()
            .map(|record| Arc::clone(&record.schema))
    }

    /// The datum of the record value `value`, without the header of its framing.
    fn datum<'a>(&self, value: &'a [u8]) -> Result<&'a [u8], CodecError> {
        match self.framing {
            Framing::Plain => Ok(value),
            Framing::Confluent { schema_id } => match value {
                [CONFLUENT_MAGIC, id @ ..] if id.len() >= 4 => {
                    let found = u32::from_be_bytes([id[0], id[1], id[2], id[3]]);
                    match schema_id {
                        Some(expected) if expected != found => Err(CodecError::new(format!(
                            "The value has the schema ID {found}, expected {expected}"
                        ))),
                        _ => Ok(&value[5..]),
                    }
                }
                _ => Err(CodecError::new(
                    "The value is not in the Confluent wire format (magic byte 0 and a schema ID)",
                )),
            },
        }
    }
}

impl Decoder for Avro {
    type Element = RowElement;
    type Output = Row;

    fn validate(&self) -> beam::Result {
        match self.record {
            Some(_) => Ok(()),
            None => Err(beam::Error::msg(NO_SCHEMA)),
        }
    }

    fn decode(&self, record: &RecordRef<'_>) -> Result<RowElement, CodecError> {
        let record_type = self
            .record
            .as_ref()
            .ok_or_else(|| CodecError::new(NO_SCHEMA))?;
        let mut reader = self.datum(record.value_or_err()?)?;
        let row = read_record(record_type, &mut reader)?;
        match reader.is_empty() {
            true => Ok(RowElement::Row(row)),
            false => Err(CodecError::new(format!(
                "The value has {} bytes after the Avro datum",
                reader.len()
            ))),
        }
    }

    fn output(&self, elements: PCollection<RowElement>) -> PCollection<Row> {
        let rows = elements.as_rows();
        match self.row_schema() {
            Some(schema) => rows.with_row_schema(&schema),
            None => rows,
        }
    }
}

impl Encoder for Avro {
    type Input = Row;
    type Element = RowElement;

    fn elements(&mut self, input: &PCollection<Row>) -> beam::Result<PCollection<RowElement>> {
        let (schema, elements) = self.input.bind(input)?;
        match &self.record {
            Some(record) => check_fields(record, &schema).map_err(beam::Error::msg)?,
            None => {
                self.record = Some(Arc::new(
                    record_of_schema(&schema).map_err(beam::Error::msg)?,
                ));
            }
        }
        Ok(elements)
    }

    fn encode<'a>(
        &self,
        element: &'a RowElement,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        let record = self
            .record
            .as_ref()
            .ok_or_else(|| CodecError::new("The encoder has no Avro schema yet"))?;
        let row = self.input.row(element)?;
        buffer.clear();
        if let Framing::Confluent { schema_id } = self.framing {
            let id = schema_id.ok_or_else(|| {
                CodecError::new("A write in the Confluent wire format needs the schema ID")
            })?;
            buffer.push(CONFLUENT_MAGIC);
            buffer.extend_from_slice(&id.to_be_bytes());
        }
        write_record(record, &row, buffer)?;
        Ok(Outgoing::value(buffer))
    }

    fn failed_type(&self) -> FieldType {
        self.input.failed_type()
    }

    fn failed_value(&self, element: RowElement) -> Result<FieldValue, CodecError> {
        self.input.failed_value(element)
    }
}

/// An Avro type, compiled for reading and writing.
#[derive(Clone, Debug)]
enum AvroType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Fixed(usize),
    Enum(Arc<[String]>),
    Array(Box<AvroType>),
    Map(Box<AvroType>),
    Record(Arc<RecordType>),
    /// A union of `null` and one type. `null_index` is the branch of `null` (0 or 1).
    Nullable {
        null_index: i64,
        inner: Box<AvroType>,
    },
    Date,
    TimestampMillis,
    TimestampMicros,
}

/// A record type: its fields, and the Beam schema of its rows.
#[derive(Debug)]
struct RecordType {
    fields: Vec<AvroType>,
    names: Vec<String>,
    schema: Arc<Schema>,
}

impl AvroType {
    /// The Beam type of the values of this type.
    fn beam_type(&self) -> FieldType {
        match self {
            Self::Null => FieldType::nullable_atomic(AtomicType::Bytes),
            Self::Boolean => FieldType::boolean(),
            Self::Int => FieldType::int32(),
            Self::Long | Self::TimestampMicros => FieldType::int64(),
            Self::Float => FieldType::float(),
            Self::Double => FieldType::double(),
            Self::Bytes | Self::Fixed(_) => FieldType::bytes(),
            Self::String | Self::Enum(_) => FieldType::string(),
            Self::Array(items) => FieldType::array(items.beam_type()),
            Self::Map(values) => FieldType::map(FieldType::string(), values.beam_type()),
            Self::Record(record) => FieldType::row(Arc::clone(&record.schema)),
            Self::Nullable { inner, .. } => inner.beam_type().with_nullable(true),
            Self::Date => FieldType::logical(URN_DATE, Vec::new(), FieldType::int64()),
            Self::TimestampMillis => {
                FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64())
            }
        }
    }
}

/// Compiles the Avro schema `json`. `names` holds the named types defined so far.
fn parse(json: &Json, names: &mut HashMap<String, AvroType>) -> Result<AvroType, String> {
    match json {
        Json::String(name) => primitive(name)
            .or_else(|| names.get(name.as_str()).cloned())
            .ok_or_else(|| format!("Unknown Avro type '{name}'")),
        Json::Array(branches) => {
            let null = |b: &Json| b.as_str() == Some("null");
            match branches.as_slice() {
                [a, b] if null(a) && !null(b) => Ok(AvroType::Nullable {
                    null_index: 0,
                    inner: Box::new(parse(b, names)?),
                }),
                [a, b] if null(b) && !null(a) => Ok(AvroType::Nullable {
                    null_index: 1,
                    inner: Box::new(parse(a, names)?),
                }),
                _ => Err(format!(
                    "Only unions of null and one type are supported, not {json}"
                )),
            }
        }
        Json::Object(object) => {
            let type_name = object
                .get("type")
                .ok_or_else(|| format!("The Avro type {json} has no 'type'"))?;
            if let Some(logical) = object.get("logicalType").and_then(Json::as_str) {
                match (logical, type_name.as_str()) {
                    ("date", Some("int")) => return Ok(AvroType::Date),
                    ("timestamp-millis", Some("long")) => return Ok(AvroType::TimestampMillis),
                    ("timestamp-micros", Some("long")) => return Ok(AvroType::TimestampMicros),
                    _ => {}
                }
            }
            let full_name = || -> Result<String, String> {
                let name = object
                    .get("name")
                    .and_then(Json::as_str)
                    .ok_or_else(|| format!("The named Avro type {json} has no 'name'"))?;
                Ok(match object.get("namespace").and_then(Json::as_str) {
                    Some(namespace) if !name.contains('.') => format!("{namespace}.{name}"),
                    _ => name.to_string(),
                })
            };
            let named = match type_name.as_str() {
                Some("record") => {
                    let name = full_name()?;
                    let fields = object
                        .get("fields")
                        .and_then(Json::as_array)
                        .ok_or_else(|| format!("The record '{name}' has no 'fields'"))?;
                    let mut types = Vec::with_capacity(fields.len());
                    let mut field_names = Vec::with_capacity(fields.len());
                    for field in fields {
                        let field_name = field
                            .get("name")
                            .and_then(Json::as_str)
                            .ok_or_else(|| format!("A field of '{name}' has no 'name'"))?;
                        let field_type = field
                            .get("type")
                            .ok_or_else(|| format!("The field '{field_name}' has no 'type'"))?;
                        types.push(parse(field_type, names)?);
                        field_names.push(field_name.to_string());
                    }
                    let record = record_type(types, field_names);
                    (name, AvroType::Record(Arc::new(record)))
                }
                Some("enum") => {
                    let symbols = object
                        .get("symbols")
                        .and_then(Json::as_array)
                        .ok_or_else(|| format!("The enum {json} has no 'symbols'"))?
                        .iter()
                        .map(|s| s.as_str().map(str::to_string))
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| format!("The symbols of {json} are not strings"))?;
                    (full_name()?, AvroType::Enum(symbols.into()))
                }
                Some("fixed") => {
                    let size = object
                        .get("size")
                        .and_then(Json::as_u64)
                        .ok_or_else(|| format!("The fixed type {json} has no 'size'"))?;
                    (full_name()?, AvroType::Fixed(size as usize))
                }
                Some("array") => {
                    let items = object
                        .get("items")
                        .ok_or_else(|| format!("The array {json} has no 'items'"))?;
                    return Ok(AvroType::Array(Box::new(parse(items, names)?)));
                }
                Some("map") => {
                    let values = object
                        .get("values")
                        .ok_or_else(|| format!("The map {json} has no 'values'"))?;
                    return Ok(AvroType::Map(Box::new(parse(values, names)?)));
                }
                // A primitive in an object, such as {"type": "long", "logicalType": "..."}.
                _ => return parse(type_name, names),
            };
            let (name, avro_type) = named;
            names.insert(name, avro_type.clone());
            Ok(avro_type)
        }
        _ => Err(format!("{json} is not an Avro schema")),
    }
}

fn primitive(name: &str) -> Option<AvroType> {
    Some(match name {
        "null" => AvroType::Null,
        "boolean" => AvroType::Boolean,
        "int" => AvroType::Int,
        "long" => AvroType::Long,
        "float" => AvroType::Float,
        "double" => AvroType::Double,
        "bytes" => AvroType::Bytes,
        "string" => AvroType::String,
        _ => return None,
    })
}

fn record_type(fields: Vec<AvroType>, names: Vec<String>) -> RecordType {
    let schema = Schema::new(
        fields
            .iter()
            .zip(&names)
            .map(|(field, name)| Field::new(name.clone(), field.beam_type()))
            .collect(),
    );
    RecordType {
        fields,
        names,
        schema: Arc::new(schema),
    }
}

/// The record type of the rows of `schema`, for a write with no Avro schema.
fn record_of_schema(schema: &Schema) -> Result<RecordType, String> {
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            avro_of_beam(&field.field_type).map_err(|e| format!("Field '{}': {e}", field.name))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let names = schema.fields.iter().map(|f| f.name.clone()).collect();
    Ok(RecordType {
        fields,
        names,
        schema: Arc::new(schema.clone()),
    })
}

/// The Avro type of the Beam type `field_type`.
fn avro_of_beam(field_type: &FieldType) -> Result<AvroType, String> {
    let avro_type = match &field_type.type_info {
        TypeInfo::Atomic(AtomicType::Boolean) => AvroType::Boolean,
        TypeInfo::Atomic(AtomicType::Byte | AtomicType::Int16 | AtomicType::Int32) => AvroType::Int,
        TypeInfo::Atomic(AtomicType::Int64) => AvroType::Long,
        TypeInfo::Atomic(AtomicType::Float) => AvroType::Float,
        TypeInfo::Atomic(AtomicType::Double) => AvroType::Double,
        TypeInfo::Atomic(AtomicType::String) => AvroType::String,
        TypeInfo::Atomic(AtomicType::Bytes) => AvroType::Bytes,
        TypeInfo::Array(items) | TypeInfo::Iterable(items) => {
            AvroType::Array(Box::new(avro_of_beam(items)?))
        }
        TypeInfo::Map(key, value) => match key.type_info {
            TypeInfo::Atomic(AtomicType::String) => AvroType::Map(Box::new(avro_of_beam(value)?)),
            _ => return Err(format!("Avro maps have STRING keys, not {key}")),
        },
        TypeInfo::Row(schema) => AvroType::Record(Arc::new(record_of_schema(schema)?)),
        TypeInfo::Logical { urn, .. } if urn == URN_DATE => AvroType::Date,
        TypeInfo::Logical { urn, .. } if urn == URN_MILLIS_INSTANT => AvroType::TimestampMillis,
        TypeInfo::Logical { representation, .. } => avro_of_beam(representation)?,
    };
    Ok(match field_type.nullable {
        true => AvroType::Nullable {
            null_index: 0,
            inner: Box::new(avro_type),
        },
        false => avro_type,
    })
}

/// Checks that the rows of `schema` have the fields of `record`, by name and in order.
fn check_fields(record: &RecordType, schema: &Schema) -> Result<(), String> {
    let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    match names == record.names.iter().map(String::as_str).collect::<Vec<_>>() {
        true => Ok(()),
        false => Err(format!(
            "The rows have the fields {names:?}, but the Avro schema has {:?}",
            record.names
        )),
    }
}

// Reading.

fn read_record(record: &RecordType, reader: &mut &[u8]) -> Result<Row, CodecError> {
    let values = record
        .fields
        .iter()
        .zip(&record.names)
        .map(|(field, name)| {
            read_value(field, reader).map_err(|e| CodecError::new(format!("Field '{name}': {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Row::new(Arc::clone(&record.schema), values).map_err(|e| CodecError::new(e.to_string()))
}

fn read_value(avro_type: &AvroType, reader: &mut &[u8]) -> Result<Option<FieldValue>, CodecError> {
    Ok(Some(match avro_type {
        AvroType::Null => return Ok(None),
        AvroType::Boolean => FieldValue::Boolean(take(reader, 1)?[0] != 0),
        AvroType::Int => FieldValue::Int32(read_int(reader)?),
        AvroType::Long | AvroType::TimestampMillis | AvroType::TimestampMicros => {
            FieldValue::Int64(read_long(reader)?)
        }
        AvroType::Date => FieldValue::Int64(i64::from(read_int(reader)?)),
        AvroType::Float => FieldValue::Float(f32::from_le_bytes(
            take(reader, 4)?.try_into().unwrap_or_default(),
        )),
        AvroType::Double => FieldValue::Double(f64::from_le_bytes(
            take(reader, 8)?.try_into().unwrap_or_default(),
        )),
        AvroType::Bytes => FieldValue::Bytes(read_bytes(reader)?.to_vec()),
        AvroType::String => FieldValue::String(read_string(reader)?),
        AvroType::Fixed(size) => FieldValue::Bytes(take(reader, *size)?.to_vec()),
        AvroType::Enum(symbols) => {
            let index = read_int(reader)?;
            let symbol = usize::try_from(index)
                .ok()
                .and_then(|i| symbols.get(i))
                .ok_or_else(|| CodecError::new(format!("No enum symbol at index {index}")))?;
            FieldValue::String(symbol.clone())
        }
        AvroType::Array(items) => {
            let mut values = Vec::new();
            read_blocks(reader, |reader| {
                values.push(read_value(items, reader)?);
                Ok(())
            })?;
            FieldValue::Array(values)
        }
        AvroType::Map(value_type) => {
            let mut entries = Vec::new();
            read_blocks(reader, |reader| {
                let key = FieldValue::String(read_string(reader)?);
                entries.push((key, read_value(value_type, reader)?));
                Ok(())
            })?;
            FieldValue::Map(entries)
        }
        AvroType::Record(record) => FieldValue::Row(read_record(record, reader)?),
        AvroType::Nullable { null_index, inner } => {
            let branch = read_long(reader)?;
            return match branch {
                b if b == *null_index => Ok(None),
                b if b == 1 - *null_index => read_value(inner, reader),
                b => Err(CodecError::new(format!("No union branch {b}"))),
            };
        }
    }))
}

/// Reads the blocks of an array or a map, and calls `item` for each item.
fn read_blocks(
    reader: &mut &[u8],
    mut item: impl FnMut(&mut &[u8]) -> Result<(), CodecError>,
) -> Result<(), CodecError> {
    loop {
        let count = read_long(reader)?;
        if count == 0 {
            return Ok(());
        }
        if count < 0 {
            // A negative count is followed by the size of the block in bytes.
            let _size = read_long(reader)?;
        }
        for _ in 0..count.unsigned_abs() {
            item(reader)?;
        }
    }
}

fn take<'a>(reader: &mut &'a [u8], len: usize) -> Result<&'a [u8], CodecError> {
    if reader.len() < len {
        return Err(CodecError::new("The Avro datum ends too early"));
    }
    let (head, tail) = reader.split_at(len);
    *reader = tail;
    Ok(head)
}

fn read_long(reader: &mut &[u8]) -> Result<i64, CodecError> {
    let mut value: u64 = 0;
    for shift in (0..64).step_by(7) {
        let byte = take(reader, 1)?[0];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            // Zigzag decoding.
            return Ok((value >> 1) as i64 ^ -((value & 1) as i64));
        }
    }
    Err(CodecError::new("An Avro long is longer than 10 bytes"))
}

fn read_int(reader: &mut &[u8]) -> Result<i32, CodecError> {
    let value = read_long(reader)?;
    i32::try_from(value).map_err(|_| CodecError::new(format!("{value} is not an Avro int")))
}

fn read_bytes<'a>(reader: &mut &'a [u8]) -> Result<&'a [u8], CodecError> {
    let len = read_long(reader)?;
    let len =
        usize::try_from(len).map_err(|_| CodecError::new(format!("Negative length {len}")))?;
    take(reader, len)
}

fn read_string(reader: &mut &[u8]) -> Result<String, CodecError> {
    let bytes = read_bytes(reader)?;
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|e| CodecError::new(format!("An Avro string is not UTF-8: {e}")))
}

// Writing.

fn write_record(record: &RecordType, row: &Row, buffer: &mut Vec<u8>) -> Result<(), CodecError> {
    let values = row.values();
    if values.len() != record.fields.len() {
        return Err(CodecError::new(format!(
            "The row has {} fields, the Avro record {}",
            values.len(),
            record.fields.len()
        )));
    }
    for ((field, name), value) in record.fields.iter().zip(&record.names).zip(values) {
        write_value(field, value.as_ref(), buffer)
            .map_err(|e| CodecError::new(format!("Field '{name}': {e}")))?;
    }
    Ok(())
}

fn write_value(
    avro_type: &AvroType,
    value: Option<&FieldValue>,
    buffer: &mut Vec<u8>,
) -> Result<(), CodecError> {
    let mismatch = |value: &FieldValue| {
        CodecError::new(format!(
            "The value {value} does not fit the Avro type {avro_type:?}"
        ))
    };
    match (avro_type, value) {
        (AvroType::Null, None) => Ok(()),
        (AvroType::Nullable { null_index, .. }, None) => {
            write_long(*null_index, buffer);
            Ok(())
        }
        (AvroType::Nullable { null_index, inner }, Some(value)) => {
            write_long(1 - *null_index, buffer);
            write_value(inner, Some(value), buffer)
        }
        (_, None) => Err(CodecError::new(
            "The value is null, and the Avro type is not nullable",
        )),
        (AvroType::Boolean, Some(FieldValue::Boolean(v))) => {
            buffer.push(u8::from(*v));
            Ok(())
        }
        (AvroType::Int | AvroType::Date, Some(value)) => {
            let v = match value {
                FieldValue::Byte(v) => i64::from(*v),
                FieldValue::Int16(v) => i64::from(*v),
                FieldValue::Int32(v) => i64::from(*v),
                FieldValue::Int64(v) if i32::try_from(*v).is_ok() => *v,
                other => return Err(mismatch(other)),
            };
            write_long(v, buffer);
            Ok(())
        }
        (AvroType::Long | AvroType::TimestampMillis | AvroType::TimestampMicros, Some(value)) => {
            let v = match value {
                FieldValue::Int64(v) => *v,
                FieldValue::Int32(v) => i64::from(*v),
                other => return Err(mismatch(other)),
            };
            write_long(v, buffer);
            Ok(())
        }
        (AvroType::Float, Some(FieldValue::Float(v))) => {
            buffer.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (AvroType::Double, Some(FieldValue::Double(v))) => {
            buffer.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (AvroType::Bytes, Some(FieldValue::Bytes(v))) => {
            write_bytes(v, buffer);
            Ok(())
        }
        (AvroType::String, Some(FieldValue::String(v))) => {
            write_bytes(v.as_bytes(), buffer);
            Ok(())
        }
        (AvroType::Fixed(size), Some(FieldValue::Bytes(v))) if v.len() == *size => {
            buffer.extend_from_slice(v);
            Ok(())
        }
        (AvroType::Enum(symbols), Some(FieldValue::String(v))) => {
            let index = symbols
                .iter()
                .position(|symbol| symbol == v)
                .ok_or_else(|| CodecError::new(format!("'{v}' is not a symbol of the enum")))?;
            write_long(index as i64, buffer);
            Ok(())
        }
        (AvroType::Array(items), Some(FieldValue::Array(values))) => {
            if !values.is_empty() {
                write_long(values.len() as i64, buffer);
                for value in values {
                    write_value(items, value.as_ref(), buffer)?;
                }
            }
            write_long(0, buffer);
            Ok(())
        }
        (AvroType::Map(value_type), Some(FieldValue::Map(entries))) => {
            if !entries.is_empty() {
                write_long(entries.len() as i64, buffer);
                for (key, value) in entries {
                    match key {
                        FieldValue::String(key) => write_bytes(key.as_bytes(), buffer),
                        other => return Err(mismatch(other)),
                    }
                    write_value(value_type, value.as_ref(), buffer)?;
                }
            }
            write_long(0, buffer);
            Ok(())
        }
        (AvroType::Record(record), Some(FieldValue::Row(row))) => write_record(record, row, buffer),
        (_, Some(value)) => Err(mismatch(value)),
    }
}

fn write_long(value: i64, buffer: &mut Vec<u8>) {
    // Zigzag encoding, then a varint.
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    while v >= 0x80 {
        buffer.push((v as u8) | 0x80);
        v >>= 7;
    }
    buffer.push(v as u8);
}

fn write_bytes(bytes: &[u8], buffer: &mut Vec<u8>) {
    write_long(bytes.len() as i64, buffer);
    buffer.extend_from_slice(bytes);
}
